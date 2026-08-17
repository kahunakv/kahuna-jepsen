(ns kahuna.workload.snapshot
  "Snapshot-read workload: a pinned snapshot never changes its answer.

  Kahuna keeps MVCC revisions per key and lets a read name the version it
  wants with `readTimestamp`. Retention is bounded — 16 in-memory revisions by
  default — so old versions are reclaimed as writes pile up. A *snapshot hold*
  is the client's claim on a point in time: while a hold at timestamp T is
  live, the effective floor stops reclamation from dropping the revision that
  was current at T, on every read path.

  The property under test:

    For a key k and a timestamp T protected by a live hold, every read of k as
    of T returns the same value — no matter when it is issued, which node
    serves it, or how much has been written to k since.

  Five operations:

    :write    overwrite a key, pushing older revisions toward the cutoff
    :pin      read a key at latest, then hold a snapshot at that value's
              commit timestamp
    :read     re-read a pinned key as of its pinned timestamp
    :renew    extend a hold's lease
    :release  drop a hold
    :floor    read the cluster-wide effective floor and live-hold count

  ## What is a violation, and what is not

  Like the sequencer workload, this one deliberately checks less than it
  could, because the interesting asymmetry is what the server *promised*:

  * **Reclaiming unprotected history is legal.** After a hold is released — or
    once its lease lapses — the revision at T may be dropped, and a read as of
    T may then legitimately answer with an older version or with
    `does-not-exist`. Reads outside a proven protection window are recorded but
    never asserted on. This is the single largest source of false positives
    available here, which is why protection windows are reconstructed from the
    history rather than trusted from the client's own bookkeeping.
  * **Gaps in revision numbers are legal.** Retention is a cache policy, not a
    contract, and nothing here depends on which revisions survive — only on the
    answer at a held timestamp.
  * **A changed answer at a held timestamp is never legal.** Under any
    interleaving, on any node, at any time.

  `does-not-exist` from a protected read is therefore an *observation with the
  value nil*, not a failure. Recording it as a failed operation would discard
  the single clearest symptom of history lost from under a hold, and the run
  would come back clean.

  ## The pin has to verify itself

  A pin reads the latest value at T and then acquires the hold — two calls,
  with a window between them in which reclamation could drop the very revision
  the hold is about to protect. `SnapshotFloorStore.AcquireAsync` fails closed
  for a prune that *overlaps* the acquire (answering `:must-retry`), but not
  for one that finished just before it. So every pin immediately re-reads as of
  T and keeps the pin only if the answer still matches. A pin that lost that
  race is discarded and counted, never registered — otherwise the checker would
  inherit a premise the server never agreed to and report the test's own race
  as a server violation."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [kahuna.client :as kc]
            [slingshot.slingshot :refer [try+]]))

(def key-space
  "The Kahuna key space these keys live in. `--key-range` registers this exact
  string, so it is derived from the same place the keys are."
  "jepsen/snapshot")

(defn- kv-key [k] (str key-space "/" k))

(defmacro with-errors
  "Transport failures become Jepsen ops.

  A write that times out is `:info`: it may still land, and the checker treats
  every *attempted* write as a value that could legitimately be read back. A
  read that times out is `:info` too, but harmlessly — an unanswered read
  contributes no observation at all.

  `op-box` is a volatile holding the op as currently known, not a plain op. A
  branch that has already chosen which hold it is acting on records that in the
  box *before* making the request, so the op still names the hold when the
  request throws. This is load-bearing for `:release`: `hold-windows` ends a
  hold's protection window on an indeterminate release, but only when it can
  see a `:hold-id`. Assoc'ing onto the original invocation instead dropped the
  id on every transport-level failure, leaving the window open to the lease
  bound — and every later `live-holds 0` inside it was reported as a floor
  violation. That produced ten violations in a run where the server was right."
  [op-box & body]
  `(try+
     ~@body
     (catch java.net.ConnectException e#
       (assoc @~op-box :type :fail, :error :connection-refused))
     (catch java.net.SocketTimeoutException e#
       (assoc @~op-box :type :info, :error :timeout))
     (catch java.net.UnknownHostException e#
       (assoc @~op-box :type :fail, :error :unknown-host))
     (catch org.apache.http.NoHttpResponseException e#
       (assoc @~op-box :type :info, :error :no-http-response))
     (catch java.io.IOException e#
       (assoc @~op-box :type :info, :error [:io (.getMessage e#)]))))

;; ---------------------------------------------------------------------------
;; Client
;; ---------------------------------------------------------------------------

(defn- pick-pin
  "A random registered pin, or nil. Pins live in an atom shared by every client
  process — `open!` copies the record, so the atom (not its contents) is the
  shared thing."
  [pins]
  (let [all (vec (mapcat val @pins))]
    (when (seq all) (rand-nth all))))

(defn- register-pin! [pins pin]
  (swap! pins update (:key pin) (fnil conj []) pin))

(defn- forget-pin!
  "Drops a pin from the registry so no further :read op selects it. Called
  *before* a release is sent, and after a renewal proves the hold is gone."
  [pins pin]
  (swap! pins update (:key pin)
         (fn [ps] (vec (remove #(= (:hold-id %) (:hold-id pin)) ps)))))

(defrecord SnapshotClient [node lease-ms timeout pins]
  client/Client
  (open! [this test n] (assoc this :node n))

  (setup! [this test])

  (invoke! [this test op]
    (let [http   {:timeout timeout}
          op-box (volatile! op)]
      (with-errors op-box
        (case (:f op)
          :write
          (let [{:keys [key value]} (:value op)
                r (kc/kv-set node (kv-key key) value http)]
            (if (= :set (:type r))
              (assoc op :type :ok :value {:key key :value value
                                          :revision (:revision r) :node node})
              (assoc op :type (kc/response-class (:type r))
                        :error [:write (:type r)])))

          :pin
          (let [k    (get-in op [:value :key])
                kk   (kv-key k)
                r    (kc/kv-get node kk http)]
            (cond
              (not (#{:get :exists} (:type r)))
              (assoc op :type (kc/response-class (:type r))
                        :error [:read-latest (:type r)])

              ;; No timestamp, no snapshot: reading as of zero is the "latest"
              ;; sentinel, so a pin built on one would assert nothing while
              ;; looking exactly like a pin that worked.
              ;; Carries the revision and served-from node, not just the fact:
              ;; a value that exists at a revision but reports no commit
              ;; timestamp cannot be pinned at all, and which read path
              ;; produced it is the whole question.
              (kc/hlc-zero? (:last-modified r))
              (assoc op :type :fail
                        :error [:no-timestamp k node (:revision r)])

              :else
              (let [t      (:last-modified r)
                    anchor (:value r)
                    ;; Unique per pin: acquire is idempotent by
                    ;; (holderId, timestamp), so a shared holder would collapse
                    ;; two pins into one hold that either could release.
                    holder (str "jepsen/snapshot/" (java.util.UUID/randomUUID))
                    a      (kc/snapshot-hold-acquire! node holder t lease-ms http)]
                (if-not (= :set (:type a))
                  (assoc op :type (kc/response-class (:type a))
                            :error [:acquire (:type a)])
                  ;; Confirm the hold actually protects what we think it does
                  ;; before anyone can read against it.
                  (let [v (kc/kv-get node kk (assoc http :read-timestamp t))]
                    (if-not (and (#{:get :exists} (:type v))
                                 (= anchor (:value v)))
                      (do (kc/snapshot-hold-release! node (:hold-id a) http)
                          (assoc op :type :fail
                                    :error [:pin-lost-race (:type v)]))
                      (let [pin {:hold-id (:hold-id a) :key k :t t :value anchor}]
                        (register-pin! pins pin)
                        (assoc op :type :ok
                                  :value (assoc pin :revision (:revision r)
                                                    :lease-expiry (:lease-expiry a)
                                                    :node node)))))))))

          :read
          (if-let [pin (pick-pin pins)]
            (let [r (kc/kv-get node (kv-key (:key pin))
                               (assoc http :read-timestamp (:t pin)))
                  base {:key (:key pin) :hold-id (:hold-id pin) :t (:t pin)
                        :node node}]
              (case (:type r)
                (:get :exists)
                (assoc op :type :ok
                          :value (assoc base :value (:value r)
                                             :revision (:revision r)
                                             :last-modified (:last-modified r)))

                ;; An observation, not a failure: history lost from under a
                ;; live hold surfaces exactly here, and calling it a failed
                ;; operation would delete the evidence.
                :does-not-exist
                (assoc op :type :ok :value (assoc base :value nil))

                (assoc op :type (kc/response-class (:type r))
                          :error [:read (:type r)])))
            (assoc op :type :fail :error :no-pins))

          :renew
          (if-let [pin (pick-pin pins)]
            (let [r (kc/snapshot-hold-renew! node (:hold-id pin) lease-ms http)]
              (case (:type r)
                :set
                (assoc op :type :ok :value {:hold-id (:hold-id pin)
                                            :lease-expiry (:lease-expiry r)
                                            :node node})

                ;; The hold is gone — expired or never registered; the server
                ;; does not say which. Either way it protects nothing now, so
                ;; stop reading against it.
                :does-not-exist
                (do (forget-pin! pins pin)
                    (assoc op :type :fail :error [:renew :does-not-exist]))

                (assoc op :type (kc/response-class (:type r))
                          :error [:renew (:type r)])))
            (assoc op :type :fail :error :no-pins))

          :release
          (if-let [pin (pick-pin pins)]
            ;; Deregister first: protection ends the moment this request is
            ;; sent, so a read that started afterwards must not be asserted on.
            (do (forget-pin! pins pin)
                ;; Into the box before the call, not onto the result after it:
                ;; a release that throws is still a release the server may have
                ;; performed, and the checker can only end the window for a
                ;; hold it can name.
                (vswap! op-box assoc :value {:hold-id (:hold-id pin)
                                             :key (:key pin)
                                             :t (:t pin) :node node})
                (let [r (kc/snapshot-hold-release! node (:hold-id pin) http)]
                  (assoc @op-box :type (if (= :set (:type r)) :ok
                                           (kc/response-class (:type r)))
                                 :error (when-not (= :set (:type r)) (:type r)))))
            (assoc op :type :fail :error :no-pins))

          :floor
          (let [r (kc/snapshot-floor node http)]
            ;; A 200 is not enough. When the request fails retryably the
            ;; middleware rewrites the body to a KeyValue MustRetry and leaves
            ;; the status at 200, so `liveHolds` is simply absent — and a nil
            ;; reaching the checker would be read as "the cluster reports no
            ;; live holds", which is precisely the violation it exists to
            ;; flag. No count, no observation.
            ;;
            ;; Both conditions, not either: the type is the contract's own
            ;; answer to "did this reach the handler", and a numeric count is
            ;; what the checker actually consumes. Requiring the count alone
            ;; would still be correct today, but it infers from a field's
            ;; presence what the server now states outright.
            (if (and (= :get (:type r)) (number? (:live-holds r)))
              (assoc op :type :ok :value {:floor (:floor r)
                                          :live-holds (:live-holds r)
                                          :node node})
              (assoc op :type (if (:type r) (kc/response-class (:type r)) :fail)
                        :error [:floor (or (:type r) (:status r)) node])))))))

  (teardown! [this test])

  (close! [this test]))

;; ---------------------------------------------------------------------------
;; Reconstructing protection windows
;; ---------------------------------------------------------------------------

(defn- with-invoke-times
  "Pairs each completion with the :time of its own invocation.

  Jepsen alternates invoke/complete strictly per process, so the last invoke
  seen on a process is the one a completion belongs to. The pairing matters
  because the two ends of an operation bound protection in opposite
  directions: a hold is certainly established by the *completion* of its pin,
  and certainly at risk from the *invocation* of its release. Using completion
  times for both would credit protection to a window the client had already
  asked the server to end."
  [history]
  (:ops (reduce (fn [{:keys [pending ops]} op]
                  (if (= :invoke (:type op))
                    {:pending (assoc pending (:process op) (:time op)) :ops ops}
                    {:pending pending
                     :ops     (conj ops (assoc op ::invoke-time
                                               (get pending (:process op) (:time op))))}))
                {:pending {} :ops []}
                history)))

(defn hold-windows
  "hold-id → {:key :t :anchor :from :until}, the interval over which a hold is
  *provably* protecting its timestamp.

    :from   completion of the pin — protection is established by then
    :until  the earliest of
              - the invocation of the release, and
              - the lease bound: the last invocation that (re)set the lease,
                plus the lease, minus a margin

  Both ends are deliberately pessimistic. The lease bound is the important one:
  a hold whose lease quietly lapses stops protecting anything, and every read
  after that point could legitimately return a different answer. Without this
  bound a long run would accumulate expired holds and then report the server's
  correct reclamation as a stability violation.

  The margin absorbs the difference between the control node's clock, which
  timestamps the history, and the cluster HLC that decides when a lease
  actually expires."
  [history lease-ms margin-ms]
  (let [ops       (with-invoke-times history)
        lease-ns  (* 1e6 (- lease-ms margin-ms))
        pins      (->> ops
                       (filter #(and (= :pin (:f %)) (= :ok (:type %))))
                       (reduce (fn [m op]
                                 (let [v (:value op)]
                                   (assoc m (:hold-id v)
                                          {:key    (:key v)
                                           :t      (:t v)
                                           :anchor (:value v)
                                           :from   (:time op)
                                           :leased (::invoke-time op)})))
                               {}))
        ;; A renewal extends the bound from when it was *sent*, which is no
        ;; later than when the server reset the expiry.
        renewed   (->> ops
                       (filter #(and (= :renew (:f %)) (= :ok (:type %))))
                       (reduce (fn [m op]
                                 (let [h (get-in op [:value :hold-id])]
                                   (if (contains? pins h)
                                     (update m h (fnil max 0) (::invoke-time op))
                                     m)))
                               {}))
        ;; An indeterminate release may well have taken effect, so it ends the
        ;; window exactly as a confirmed one does.
        released  (->> ops
                       (filter #(and (= :release (:f %))
                                     (#{:ok :info} (:type %))
                                     (get-in % [:value :hold-id])))
                       (reduce (fn [m op]
                                 (let [h (get-in op [:value :hold-id])]
                                   (if (contains? m h)
                                     m
                                     (assoc m h (::invoke-time op)))))
                               {}))]
    (into {}
          (map (fn [[h {:keys [key t anchor from leased]}]]
                 (let [lease-until (+ (max leased (get renewed h 0)) lease-ns)
                       until       (min lease-until
                                        (get released h Double/POSITIVE_INFINITY))]
                   [h {:key key :t t :anchor anchor
                       :from from :until until}])))
          pins)))

(defn read-depths
  "Pairs every ok :read with how far below the key's newest known revision the
  revision it was served actually sat.

  This is the measure of whether a run tested anything. A read as of T is
  trivially satisfied while T is still the key's current version — the server
  need not consult history at all, and a run whose reads all sit at head has
  verified nothing about retention, holds or the floor while looking exactly
  like a run that did. Only writes that were acknowledged contribute a
  revision, so the depth is a lower bound.

  Returns [{:op op :depth n}], skipping reads on keys with no acknowledged
  write yet."
  [history]
  (:reads
    (reduce
      (fn [{:keys [head] :as acc} op]
        (let [v (:value op)]
          (cond
            (and (= :write (:f op)) (= :ok (:type op)) (:revision v))
            (update-in acc [:head (:key v)] (fnil max -1) (:revision v))

            (and (= :read (:f op)) (= :ok (:type op)) (:revision v))
            (if-let [h (get head (:key v))]
              (update acc :reads conj {:op op :depth (- h (:revision v))})
              acc)

            :else acc)))
      {:head {} :reads []}
      history)))

(defn- protected?
  [window op]
  (and window
       (<= (:from window) (:time op))
       (< (:time op) (:until window))))

;; ---------------------------------------------------------------------------
;; Checkers
;; ---------------------------------------------------------------------------

(def min-protected-reads
  "Below this many reads inside a proven protection window, the run has not
  exercised snapshot stability and the checker reports :valid? :unknown.

  A history with no protected reads satisfies 'no pinned snapshot ever
  changed' perfectly. Without this floor a run in which every pin failed —
  a cluster that never elected, an endpoint that 404s, a lease too short to
  cover anything — is indistinguishable from a clean one. This suite has
  already been bitten by that shape twice; see the `:empty-transaction-graph`
  washouts in FINDINGS.md."
  25)

(defn stability-checker
  "Fails if a pinned snapshot ever changed its answer.

  Compares every protected read against the value the pin itself observed at
  that timestamp, rather than only comparing reads to each other: a history in
  which every read agrees with the others but all of them disagree with the
  pinned value is still a snapshot that moved."
  [lease-ms margin-ms]
  (reify checker/Checker
    (check [this test history opts]
      (let [windows    (hold-windows history lease-ms margin-ms)
            reads      (->> history
                            (filter #(and (= :read (:f %)) (= :ok (:type %))
                                          (:value %)))
                            (into []))
            {prot true unprot false}
            (group-by #(boolean (protected? (windows (get-in % [:value :hold-id])) %))
                      reads)
            violations (->> prot
                            (keep (fn [op]
                                    (let [v (:value op)
                                          w (windows (:hold-id v))]
                                      (when (not= (:anchor w) (:value v))
                                        {:key      (:key v)
                                         :t        (:t v)
                                         :hold-id  (:hold-id v)
                                         :pinned   (:anchor w)
                                         :read     (:value v)
                                         :revision (:revision v)
                                         :node     (:node v)
                                         :process  (:process op)
                                         :time     (:time op)}))))
                            (into []))
            vanished   (count (filter #(nil? (:read %)) violations))
            ;; Depth is computed over protected reads only: a deep read
            ;; outside any protection window proves nothing was held.
            protected-ids (into #{} (map :index) prot)
            depths     (->> (read-depths history)
                            (filter #(protected-ids (:index (:op %))))
                            (map :depth)
                            (into []))
            below-head (count (filter pos? depths))]
        {:valid?            (cond
                              (seq violations)                     false
                              (< (count prot) min-protected-reads) :unknown
                              ;; Every protected read was served by the key's
                              ;; current version, so none of them needed the
                              ;; history a hold exists to protect.
                              (and (seq depths) (zero? below-head)) :unknown
                              :else                                true)
         :holds             (count windows)
         :protected-reads   (count prot)
         :unprotected-reads (count unprot)
         :reads-below-head  below-head
         :max-depth         (when (seq depths) (apply max depths))
         :median-depth      (when (seq depths)
                              (nth (sort depths) (quot (count depths) 2)))
         :insufficient-data (cond
                              (< (count prot) min-protected-reads)
                              (str "only " (count prot) " reads fell inside a "
                                   "proven protection window; need "
                                   min-protected-reads " to call a run clean")

                              (and (seq depths) (zero? below-head))
                              (str "all " (count depths) " protected reads were "
                                   "served by the key's current revision, so no "
                                   "read exercised retained history"))
         :history-vanished  vanished
         :violations        (take 10 violations)}))))

(defn leakage-checker
  "Fails if a snapshot read served a revision committed *after* its snapshot.

  A read as of T comes back with the commit timestamp of the version served,
  and that timestamp must be at or before T on every code path — the snapshot
  path returns the revision it selected, and the fall-through path only runs
  when the current value is already at or before T.

  This needs no protection window and no model of what was written: it is the
  server disagreeing with itself in a single response, so a hold that expired
  or a snapshot that was never protected cannot explain it away."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [bad (->> history
                     (filter #(and (= :read (:f %)) (= :ok (:type %))
                                   (get-in % [:value :last-modified])))
                     (keep (fn [op]
                             (let [v  (:value op)
                                   lm (:last-modified v)]
                               (when (pos? (kc/hlc-compare lm (:t v)))
                                 {:key (:key v) :t (:t v) :last-modified lm
                                  :value (:value v) :revision (:revision v)
                                  :node (:node v) :process (:process op)
                                  :time (:time op)}))))
                     (into []))]
        {:valid?     (empty? bad)
         :violations (take 10 bad)}))))

(defn provenance-checker
  "Fails if a read returned a value nobody ever wrote.

  Every attempted write counts, including `:info` ones — a write whose
  acknowledgement was lost may well have landed, and treating its value as
  never-written would turn a correct read into a violation. That makes this a
  corruption check rather than a consistency check, which is the point: it is
  the one assertion here that survives regardless of holds, leases and
  retention."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [written (->> history
                         (filter #(and (= :write (:f %)) (= :invoke (:type %))))
                         (keep #(get-in % [:value :value]))
                         (into #{}))
            bad     (->> history
                         (filter #(and (= :read (:f %)) (= :ok (:type %))
                                       (get-in % [:value :value])))
                         (keep (fn [op]
                                 (let [v (:value op)]
                                   (when-not (written (:value v))
                                     {:key (:key v) :t (:t v) :value (:value v)
                                      :revision (:revision v) :node (:node v)
                                      :process (:process op) :time (:time op)}))))
                         (into []))]
        {:valid?        (empty? bad)
         :values-written (count written)
         :violations    (take 10 bad)}))))

(defn floor-checker
  "Fails if the cluster reported no live holds while this test provably held
  one.

  `/v1/kv/snapshot-floor` routes to the meta-partition leader rather than
  answering from local state, so a zero here is the authoritative registry
  saying the hold is gone — not a stale follower. Since the protection windows
  are already pessimistic at both ends, a hold being established or released
  concurrently cannot produce this.

  Only the zero case is asserted. The count is cluster-wide and this test is
  not the only thing that can hold a snapshot, so 'fewer than we expected' has
  innocent explanations that 'none at all, while we hold one' does not."
  [lease-ms margin-ms]
  (reify checker/Checker
    (check [this test history opts]
      (let [windows (hold-windows history lease-ms margin-ms)
            bad     (->> history
                         ;; `= 0`, not `zero?`: a nil that reached here would
                         ;; throw rather than be ignored, and the checker must
                         ;; never crash on a malformed observation.
                         (filter #(and (= :floor (:f %)) (= :ok (:type %))
                                       (= 0 (get-in % [:value :live-holds]))))
                         (keep (fn [op]
                                 (let [held (->> (vals windows)
                                                 (filter #(and (<= (:from %) (:time op))
                                                               (< (:time op) (:until %))))
                                                 (into []))]
                                   (when (seq held)
                                     {:time (:time op)
                                      :node (get-in op [:value :node])
                                      :expected-live (count held)
                                      :holds (take 3 (map :t held))}))))
                         (into []))]
        {:valid?     (empty? bad)
         :violations (take 10 bad)}))))

;; ---------------------------------------------------------------------------
;; Workload
;; ---------------------------------------------------------------------------

(defn- generator
  "Phase 1 writes every key once so a pin always finds a value; phase 2 mixes.

  The mix is write-heavy on purpose. Retention keeps the newest 16 in-memory
  revisions per key, so a pinned revision only becomes interesting once enough
  writes have pushed it below that cutoff — a read-heavy mix would spend the
  run reading versions that were never in danger of being reclaimed."
  [key-count]
  (let [ctr (atom 0)
        wr  (fn [_ _] {:type :invoke :f :write
                       :value {:key   (rand-int key-count)
                               :value (str "v" (swap! ctr inc))}})]
    (gen/phases
      (mapv (fn [k] {:type :invoke :f :write
                     :value {:key k :value (str "seed-" k)}})
            (range key-count))
      (gen/mix
        [wr wr wr wr
         (repeat {:type :invoke :f :read})
         (repeat {:type :invoke :f :read})
         (repeat {:type :invoke :f :read})
         (fn [_ _] {:type :invoke :f :pin :value {:key (rand-int key-count)}})
         (repeat {:type :invoke :f :renew})
         (repeat {:type :invoke :f :release})
         (repeat {:type :invoke :f :floor})]))))

(defn workload
  "Options:
    :key-count           keys in play (default 5)
    :snapshot-lease-ms   hold lease. Defaults to comfortably longer than the
                         run, so a lapsed lease is not the thing under test —
                         :renew exercises renewal, but no verdict depends on it
    :lease-margin-ms     slack subtracted from every protection window"
  [opts]
  (let [key-count (:key-count opts 5)
        margin    (:lease-margin-ms opts 500)
        ;; A lease shorter than the run silently shrinks every protection
        ;; window, and the checker would then report :unknown for want of
        ;; protected reads rather than failing — a quiet no-op of exactly the
        ;; kind this suite keeps rediscovering. Default well past the horizon.
        ;; `or`, not a lookup default: an option that is present-and-nil would
        ;; otherwise sail past the default and set a nil lease. That exact
        ;; shape silently disabled health sampling earlier in this suite.
        lease     (or (:snapshot-lease-ms opts)
                      (max 60000 (* 1000 (+ 300 (or (:time-limit opts) 60)))))]
    {:client    (SnapshotClient. nil lease (:snapshot-timeout-ms opts 5000)
                                 (atom {}))
     :key-space key-space
     :checker   (checker/compose
                  {:stability  (stability-checker lease margin)
                   :leakage    (leakage-checker)
                   :provenance (provenance-checker)
                   :floor      (floor-checker lease margin)
                   :timeline   (timeline/html)})
     :generator (generator key-count)}))
