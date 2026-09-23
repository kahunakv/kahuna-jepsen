(ns kahuna.workload.append
  "Elle list-append workload over Kahuna's interactive transactions.

  Each Jepsen key is one Kahuna key holding a comma-separated list of integers.
  A transaction is an interactive session:

    start-tx-session  →  try-get / try-set (carrying the session's HLC
                         transactionId and coordinatorKey)  →  commit-tx-session

  Elle infers the dependency graph from what each transaction read and
  appended, and reports the strongest consistency model the history is
  compatible with. Sessions are opened with `TrackAndValidate` read validation
  and pessimistic locking — the combination Kahuna's docs credit for
  serializable behaviour — so the default check is serializability.

  Kahuna has no native list-append, so an append is a read-modify-write inside
  the session. The read half is deliberate: it puts the key in the
  transaction's read set, which is exactly the conflict the isolation level is
  supposed to police.

  ## Read locks

  `--read-lock shared` takes a Shared point range lock over the key before
  every read and holds it to commit — strict two-phase locking on the read
  side, which is exactly what CamusDB's serializable transactions do for every
  row they read. The default (`none`) reads bare, as above.

  The two modes exercise different server paths, and that is the point. A bare
  read is protected by the per-key lock and by commit-time read-set validation.
  A Shared range lock goes through `TryAcquireExclusiveRangeLockHandler`, whose
  Shared mode used to defer every conflict to the *writer's* commit-time probe.
  A reader whose lock landed after that probe and before the writer's decision
  was never checked by anyone: it kept its lock, read the pre-write value, and
  the writer committed anyway — a G2 cycle Elle found in CamusDB (Caraxes
  `append-rw-8keys`) that this suite could not see, because nothing here ever
  acquired a range lock. Kahuna `b4823b8` closes that window; this mode is
  what keeps it closed.

  A refused acquire (`:already-locked`) is resolved the way CamusDB resolves
  it, by wait-die on the holder's transaction id: an older requester waits for
  the younger holder up to a deadline, a younger requester aborts at once. That
  ordering cannot form a wait cycle. An aborted transaction applied nothing
  and is reported `:fail`."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]]
            [jepsen.tests.cycle.append :as append]
            [kahuna.client :as kc]
            [slingshot.slingshot :refer [try+ throw+]]))

;; KeyValueTransactionLocking
(def pessimistic 0)
(def optimistic  1)
;; ReadValidation
(def no-validation     0)
(def track-and-validate 1)
;; DecisionDurability
(def best-effort 0)
(def durable     1)
;; TransactionPriority/Normal
(def priority-normal 2)
;; RangeLockMode (Kahuna.Shared/KeyValue/RangeLockMode.cs)
(def range-lock-exclusive 0)
(def range-lock-shared    1)

(def key-space
  "The Kahuna key space these lists live in. `--key-range` registers this exact
  string, so it is derived from the same place the keys are.

  Note the transaction coordinator keys (`jepsen/tx/…`) are a *different* key
  space and stay hash-routed: a coordinator anchor is not data anyone scans by
  range, and range-routing it would move 2PC bookkeeping around under a split
  for no reason anyone is testing."
  "jepsen/append")

(defn- kv-key [k] (str key-space "/" k))

(defn op-id
  "A fresh 128-bit operation id for one micro-op.

  Every transaction-scoped request must carry *both* a coordinator key and a
  non-zero operation id. `KeyValuesManager.ClassifyRegistration` treats a
  request carrying exactly one of the pair as `Malformed` and answers
  `InvalidInput`, on the grounds that applying it would mutate a participant
  outside the finalize fence. Omitting the id therefore fails every micro-op —
  it does not silently degrade to the unregistered path.

  The id is the coordinator's dedup key: resubmitting the same id replays the
  cached response instead of applying the operation twice. This client never
  retries a micro-op, so a fresh random id per call is correct. The one
  retrying call is the Shared point lock under `--read-lock shared`, and it
  keeps its id across transient retries for exactly that reason. Both halves are
  masked to 63 bits, since these land in C# `ulong` fields and a negative
  Clojure long would not deserialize."
  []
  (let [u (java.util.UUID/randomUUID)]
    {:operationIdHigh (bit-and (.getMostSignificantBits u) Long/MAX_VALUE)
     :operationIdLow  (bit-or 1 (bit-and (.getLeastSignificantBits u)
                                         Long/MAX_VALUE))}))

(defn- parse-list
  "Kahuna stores opaque bytes; a list is stored as \"1,2,3\". An absent key
  reads as nil, which Elle treats as the empty list."
  [s]
  (when (and s (seq s))
    (mapv #(Long/parseLong %) (str/split s #","))))

(defn- render-list [xs] (str/join "," xs))

(defn wait-for-holder?
  "Wait-die, as CamusDB's `KvRangeLockManager` applies it to a refused range
  lock: the requester waits only when it is *older* than the holder (a smaller
  transaction id), so every wait points older → younger and no wait cycle can
  form. A younger requester gives up at once. With no holder reported there is
  nothing to order against, so the answer is to give up."
  [tx-id holder]
  (boolean
    (and holder
         (not (kc/hlc-zero? holder))
         (neg? (kc/hlc-compare tx-id holder)))))

(defn- backoff-ms
  "Delay before the next acquire attempt. Short and bounded: the point is to
  let the holder finish, not to model a client library's retry curve."
  [attempt]
  (min 250 (* 25 (inc attempt))))

(defn- acquire-shared-point-lock!
  "Takes a Shared range lock whose start and end are both `key` — one point —
  via POST /v1/kv/try-acquire-range-lock, and returns nil once it is held.

  The prefix is the key space, and the bounds are the full key: keys route by
  their parent bucket, so the lock lands on the partition that serves the key.
  The lock is registered with the coordinator (`coordinatorKey` plus an
  operation id), which renews its lease and releases it on finalize — there
  is no explicit release, exactly as CamusDB does it.

  Transient answers (`:must-retry`, `:waiting-for-replication`) are retried
  under the *same* operation id until `wait-ms` runs out, so a lost ack is
  replayed rather than double-registered. A refused acquire is retried under a
  *fresh* id only when `wait-for-holder?` says this transaction is the older
  one. Anything else, or the deadline, throws the same `:kahuna/abort` the
  micro-ops throw, tagged `:stage :range-lock` so the history tells a refused
  lock apart from a failed read.

  The lease (`lease-ms`) is the session timeout: the coordinator renews it on
  its own cadence, and a lease shorter than that cadence would lapse between
  renewals and silently unlock the key."
  [node coordinator tx-id key http wait-ms lease-ms mop]
  (let [deadline (+ (System/nanoTime) (* (long wait-ms) 1000000))
        request  (fn [id]
                   (kc/post! node "/v1/kv/try-acquire-range-lock"
                             (merge {:transactionId  tx-id
                                     :prefix         key-space
                                     :startKey       key
                                     :startInclusive true
                                     :endKey         key
                                     :endInclusive   true
                                     :expiresMs      lease-ms
                                     :durability     kc/persistent
                                     :mode           range-lock-shared
                                     :coordinatorKey coordinator}
                                    id)
                             http))]
    (loop [attempt 0
           id      (op-id)]
      (let [r        (request id)
            t        (kc/kv-response-type (:type r))
            in-time? (< (System/nanoTime) deadline)]
        (cond
          (= :locked t)
          nil

          (and in-time? (#{:must-retry :waiting-for-replication} t))
          (do (Thread/sleep (backoff-ms attempt))
              (recur (inc attempt) id))

          (and in-time?
               (= :already-locked t)
               (wait-for-holder? tx-id (:holderTransactionId r)))
          (do (Thread/sleep (backoff-ms attempt))
              (recur (inc attempt) (op-id)))

          :else
          (throw+ {:kahuna/abort t :mop mop :stage :range-lock}))))))

(defn- lock-for-read!
  "Under `:shared` read locking, takes the point lock for `k` unless this
  transaction already holds it — a lock held to commit covers every later read
  of the same key, so re-asserting it only spends a round trip (CamusDB's
  'already covered' rule). Returns the updated set of locked keys."
  [read-lock node coordinator tx-id k key http wait-ms lease-ms mop locked]
  (if (or (not= :shared read-lock) (contains? locked k))
    locked
    (do (acquire-shared-point-lock! node coordinator tx-id key http
                                    wait-ms lease-ms mop)
        (conj locked k))))

(defn rollback!
  "Best-effort abort. Failure here changes nothing about the transaction's
  outcome — it only means locks and write intents linger until they time out."
  [node coordinator tx-id http]
  (try+
    (kc/post! node "/v1/kv/rollback-tx-session"
              {:coordinatorKey coordinator :transactionId tx-id}
              http)
    (catch Object _ nil)))

(defmacro with-errors
  "A transaction that fails to start, or that we abort ourselves, definitely
  had no effect (:fail). A commit whose outcome we never learned is :info —
  Kahuna may still decide it, and calling that a failure would invent
  anomalies Elle would faithfully report."
  [op & body]
  `(try+
     ~@body
     (catch java.net.ConnectException e#
       (assoc ~op :type :fail, :error :connection-refused))
     (catch java.net.SocketTimeoutException e#
       (assoc ~op :type :info, :error :timeout))
     (catch java.net.UnknownHostException e#
       (assoc ~op :type :fail, :error :unknown-host))
     (catch org.apache.http.NoHttpResponseException e#
       (assoc ~op :type :info, :error :no-http-response))
     (catch java.io.IOException e#
       (assoc ~op :type :info, :error [:io (.getMessage e#)]))))

(defrecord AppendClient [node locking read-lock lock-wait-ms tx-timeout-ms timeout]
  client/Client
  (open! [this test n] (assoc this :node n))

  (setup! [this test])

  (invoke! [this test op]
    (let [coordinator (str "jepsen/tx/" (java.util.UUID/randomUUID))
          http        {:timeout timeout}]
      (with-errors op
        (let [start (kc/post! node "/v1/kv/start-tx-session"
                              {:coordinatorKey     coordinator
                               :timeout            tx-timeout-ms
                               :lockingType        locking
                               :asyncRelease       false
                               :autoCommit         false
                               :readValidation     track-and-validate
                               :decisionDurability durable
                               :priority           priority-normal
                               :readTimestamp      kc/hlc-zero}
                              http)
              ;; TransactionCoordinator.StartTransaction signals success with
              ;; KeyValueResponseType.Set — not an obvious name, hence the note.
              start-type (kc/kv-response-type (:type start))]
          (if-not (= :set start-type)
            ;; Couldn't open a session: nothing happened.
            ;; Couldn't open a session, so no micro-op was ever sent and the
            ;; transaction definitely had no effect — :fail even when the type
            ;; is nil (an unparseable body). The HTTP status rides along
            ;; because a nil type is otherwise undiagnosable from the history.
            (assoc op :type (if (kc/indeterminate-types start-type) :info :fail)
                      :error [:start start-type (:status start)])
            (let [tx-id (:transactionId start)]
              (try+
                (loop [mops   (seq (:value op))
                       result []
                       cache  {}    ; key -> list as this txn has left it
                       locked #{}]  ; keys under this txn's Shared point lock
                  (if-not mops
                    ;; All micro-ops applied; ask for a decision.
                    (let [c (kc/post! node "/v1/kv/commit-tx-session"
                                      {:coordinatorKey  coordinator
                                       :transactionId   tx-id
                                       :recordAnchorKey (:recordAnchorKey start)}
                                      http)
                          t (kc/kv-response-type (:type c))]
                      (case t
                        :committed (assoc op :type :ok :value result)
                        ;; A rolled-back or aborted transaction definitely
                        ;; applied nothing.
                        (:aborted :rolled-back)
                        (assoc op :type :fail :error t)
                        (assoc op :type (kc/response-class t) :error [:commit t])))

                    (let [[f k v] (first mops)
                          key     (kv-key k)
                          ;; Every read is locked under `:shared`, including
                          ;; the read half of an append that has to go to the
                          ;; server. An append whose list is already cached
                          ;; reads nothing, and the key is then already locked
                          ;; from the earlier read or write.
                          locked  (if (and (= :append f) (contains? cache k))
                                    locked
                                    (lock-for-read! read-lock node coordinator
                                                    tx-id k key http
                                                    lock-wait-ms tx-timeout-ms
                                                    [f k v] locked))]
                      (case f
                        :r
                        (let [r (kc/post! node "/v1/kv/try-get"
                                          (merge {:transactionId  tx-id
                                                  :key            key
                                                  :revision       -1
                                                  :readTimestamp  kc/hlc-zero
                                                  ;; `durability`, not `value` —
                                                  ;; see the note in kc/kv-get.
                                                  :durability     kc/persistent
                                                  :coordinatorKey coordinator}
                                                 (op-id))
                                          http)
                              t (kc/kv-response-type (:type r))]
                          (case t
                            (:get :exists)
                            (let [xs (parse-list (kc/b64->str (:value r)))]
                              (recur (next mops) (conj result [:r k xs])
                                     (assoc cache k xs) locked))

                            :does-not-exist
                            (recur (next mops) (conj result [:r k nil])
                                   (assoc cache k nil) locked)

                            (throw+ {:kahuna/abort t :mop [f k v]})))

                        :append
                        ;; Read-modify-write: prefer what this txn already
                        ;; wrote, else read the committed list.
                        (let [current
                              (if (contains? cache k)
                                (get cache k)
                                (let [r (kc/post! node "/v1/kv/try-get"
                                                  (merge {:transactionId  tx-id
                                                          :key            key
                                                          :revision       -1
                                                          :readTimestamp  kc/hlc-zero
                                                          :durability     kc/persistent
                                                          :coordinatorKey coordinator}
                                                         (op-id))
                                                  http)
                                      t (kc/kv-response-type (:type r))]
                                  (case t
                                    (:get :exists) (parse-list (kc/b64->str (:value r)))
                                    :does-not-exist nil
                                    (throw+ {:kahuna/abort t :mop [f k v]}))))
                              updated (conj (vec current) v)
                              w (kc/post! node "/v1/kv/try-set"
                                          (merge {:transactionId   tx-id
                                                  :key             key
                                                  :value           (kc/->b64 (render-list updated))
                                                  :compareValue    nil
                                                  :compareRevision 0
                                                  :expiresMs       0
                                                  :flags           kc/flag-set
                                                  :durability      kc/persistent
                                                  :coordinatorKey  coordinator}
                                                 (op-id))
                                          http)
                              t (kc/kv-response-type (:type w))]
                          (if (= :set t)
                            (recur (next mops) (conj result [:append k v])
                                   (assoc cache k updated) locked)
                            (throw+ {:kahuna/abort t :mop [f k v]})))))))

                ;; A micro-op failed. Roll the session back so its locks and
                ;; write intents are released rather than left to time out,
                ;; then report according to how definite the failure was.
                (catch [:kahuna/abort :must-retry] {:keys [kahuna/abort mop stage]}
                  (rollback! node coordinator tx-id http)
                  (assoc op :type :info :error [(or stage :mop) mop abort]))
                ;; `map?` first: slingshot tries each selector against every
                ;; thrown object, including the SocketTimeoutExceptions this
                ;; body raises under partition. `contains?` throws on those,
                ;; which escapes as an unhandled IllegalArgumentException and
                ;; skips the rollback below.
                (catch (and (map? %) (contains? % :kahuna/abort))
                       {:keys [kahuna/abort mop stage]}
                  (rollback! node coordinator tx-id http)
                  (assoc op :type (kc/response-class abort)
                            :error [(or stage :mop) mop abort])))))))))

  (teardown! [this test])

  (close! [this test]))

(defn check-cpus!
  "Elle's analysis deadlocks on a single-CPU machine, so refuse to start rather
  than hang.

  `elle.core/combine` launches `jepsen.history.task`s that await other tasks.
  That executor is sized from `availableProcessors`; with one worker, a task
  blocks forever on a subtask that can never be scheduled. The symptom is
  brutal to diagnose from the outside — the run reaches 'Analyzing...' and sits
  at 0% CPU indefinitely, looking like a slow check rather than a deadlock.

  Docker Desktop defaults can hand a VM a single CPU even on an 8-core host;
  raise it under Settings → Resources."
  []
  (let [cores (.availableProcessors (Runtime/getRuntime))]
    (when (< cores 2)
      (throw (ex-info
               (str "The append workload needs at least 2 CPUs; this JVM sees "
                    cores ". Elle's checker would deadlock during analysis "
                    "after the run completes. Raise the CPU allocation "
                    "(Docker Desktop → Settings → Resources) and retry.")
               {:cores cores})))
    cores))

(defn workload
  "Options:
    :consistency-model  Elle model to demand (default :serializable)
    :key-count          keys in play at once
    :max-txn-length     micro-ops per transaction
    :locking            :pessimistic (default) or :optimistic
    :read-lock          :none (default) or :shared — a Shared point range lock
                        before every read, held to commit (see the namespace
                        docstring)
    :lock-wait-ms       how long an older transaction waits for a younger
                        holder before it gives up (default 2000)"
  [opts]
  (check-cpus!)
  (let [model (:consistency-model opts :serializable)]
    (merge (append/test {:key-count       (:key-count opts 5)
                         :max-txn-length  (:max-txn-length opts 4)
                         :max-writes-per-key (:max-writes-per-key opts 32)
                         :consistency-models [model]})
           {:key-space key-space
            :client (AppendClient. nil
                                   (if (= :optimistic (:locking opts))
                                     optimistic
                                     pessimistic)
                                   (:read-lock opts :none)
                                   (:lock-wait-ms opts 2000)
                                   (:tx-timeout-ms opts 10000)
                                   (:tx-http-timeout-ms opts 10000))})))
