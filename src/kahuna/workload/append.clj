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
  supposed to police."
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
  retries a micro-op, so a fresh random id per call is correct. Both halves are
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

(defrecord AppendClient [node locking tx-timeout-ms timeout]
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
                       cache  {}]   ; key -> list as this txn has left it
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
                          key     (kv-key k)]
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
                                     (assoc cache k xs)))

                            :does-not-exist
                            (recur (next mops) (conj result [:r k nil])
                                   (assoc cache k nil))

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
                                   (assoc cache k updated))
                            (throw+ {:kahuna/abort t :mop [f k v]})))))))

                ;; A micro-op failed. Roll the session back so its locks and
                ;; write intents are released rather than left to time out,
                ;; then report according to how definite the failure was.
                (catch [:kahuna/abort :must-retry] {:keys [kahuna/abort mop]}
                  (rollback! node coordinator tx-id http)
                  (assoc op :type :info :error [:mop mop abort]))
                ;; `map?` first: slingshot tries each selector against every
                ;; thrown object, including the SocketTimeoutExceptions this
                ;; body raises under partition. `contains?` throws on those,
                ;; which escapes as an unhandled IllegalArgumentException and
                ;; skips the rollback below.
                (catch (and (map? %) (contains? % :kahuna/abort))
                       {:keys [kahuna/abort mop]}
                  (rollback! node coordinator tx-id http)
                  (assoc op :type (kc/response-class abort)
                            :error [:mop mop abort])))))))))

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
    :locking            :pessimistic (default) or :optimistic"
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
                                   (:tx-timeout-ms opts 10000)
                                   (:tx-http-timeout-ms opts 10000))})))
