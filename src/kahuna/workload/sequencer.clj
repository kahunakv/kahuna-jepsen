(ns kahuna.workload.sequencer
  "Sequencer workload: no id is ever handed out twice.

  Every client hammers ONE sequence, so all traffic lands on a single Raft
  partition — the place where leader changes can lose or replay state. Spreading
  across many sequences would dilute exactly the contention we are hunting.

  Three operations:

    :next        take one value
    :reserve     take a contiguous run of n values
    :next-twice  send the SAME idempotency key twice and compare the answers

  ## What is a violation, and what is not

  This workload deliberately checks *less* than it could. Kahuna's sequencer
  hands each node an exclusively reserved block and lets it drain that block
  locally (`SequenceActor.TryPlanFromBlock`), so:

  * **Gaps are legal.** A leader change surrenders the rest of a block. So does
    an allocation whose acknowledgement was lost. Both burn values that no
    client ever sees, and neither is a bug.
  * **Out-of-order values are legal.** Node A holding [100,200) can serve 105
    *after* node B served 205. There is no global ordering to violate, so a
    monotonicity check would manufacture failures the protocol never promised.
  * **Duplicates are never legal.** Two clients receiving the same value is a
    safety violation under any interleaving, on any node, at any time.

  That asymmetry is what makes this workload robust to indeterminacy. A `:next`
  that times out may or may not have consumed a value, but either way the result
  is a gap, never a duplicate — so unlike the register workload, no verdict here
  depends on classifying an ambiguous response correctly. An `:info` op simply
  contributes nothing to the analysis.

  The one thing indeterminacy *does* threaten is replay safety, which is what
  `:next-twice` exists to test: the documented way to retry a timed-out `next`
  is to resend it with the same idempotency key, and that must return the
  original allocation rather than burning a second value. Under partition+kill
  the replay may land on a different leader than the original — precisely where
  an idempotency record could go missing."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [kahuna.client :as kc]
            [slingshot.slingshot :refer [try+]]))

(defmacro with-errors
  "Transport failures become Jepsen ops. A timeout on an allocating call is
  indeterminate (:info): the value may have been consumed with the ack lost.
  That costs us nothing here — an unobserved allocation is a gap, and gaps are
  legal — so there is no incentive to guess."
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

(defn- idem-key
  "A fresh idempotency key. Random rather than derived from the process id: a
  crashed process is renumbered and reused, and a key colliding with an earlier
  op's would make the server correctly replay an allocation that this op never
  made, which reads as a duplicate."
  []
  (str (java.util.UUID/randomUUID)))

(defrecord SequencerClient [node seq-name reserve-max]
  client/Client
  (open! [this test n]
    (assoc this :node n))

  (setup! [this test]
    ;; Every client races to create the sequence; one wins and the rest get
    ;; :already-exists, which is success for our purposes.
    ;;
    ;; This MUST retry. `setup!` runs as soon as the DB is up, which is before
    ;; the cluster has elected leaders for every partition — the first attempts
    ;; come back "node has not completed cluster initialization". A single-shot
    ;; create leaves the sequence absent, every subsequent op answers
    ;; :not-found, and the run produces zero allocations while the checkers,
    ;; having nothing to examine, report success. That is the washout trap this
    ;; project has already been bitten by; the fix belongs here rather than in
    ;; a checker heuristic.
    (let [deadline (+ (System/currentTimeMillis) 60000)]
      (loop [attempt 1]
        (let [r (try+ (kc/seq-create! node seq-name {:initial-value 0
                                                    :increment     1
                                                    :timeout       5000})
                      (catch Object _ {:type :unreachable}))]
          (cond
            (#{:success :already-exists} (:type r))
            (info "sequence" seq-name "ready on" node "after" attempt "attempt(s)")

            (< deadline (System/currentTimeMillis))
            (warn "sequence create on" node "gave up after" attempt
                  "attempts; last response" (:type r))

            :else
            (do (Thread/sleep 1000)
                (recur (inc attempt))))))))

  (invoke! [this test op]
    (let [opts {:timeout 5000}]
      (with-errors op
        (case (:f op)
          :next
          (let [r (kc/seq-next! node seq-name (idem-key) opts)]
            (if (= :success (:type r))
              (assoc op :type :ok :value {:allocation (:allocation r)
                                          :node       node})
              (assoc op :type (kc/sequence-class (:type r)) :error (:type r))))

          :reserve
          (let [n (inc (rand-int reserve-max))
                r (kc/seq-reserve! node seq-name n (idem-key) opts)]
            (if (= :success (:type r))
              (assoc op :type :ok :value {:allocation (:allocation r)
                                          :requested  n
                                          :node       node})
              (assoc op :type (kc/sequence-class (:type r)) :error (:type r))))

          :next-twice
          ;; Both calls carry one key, so the second is a replay of the first.
          ;; Sent to the same node; a partition between them may still route the
          ;; replay to a new leader, which is the interesting case.
          (let [k  (idem-key)
                r1 (kc/seq-next! node seq-name k opts)]
            (if-not (= :success (:type r1))
              (assoc op :type (kc/sequence-class (:type r1)) :error [:first (:type r1)])
              (let [r2 (kc/seq-next! node seq-name k opts)]
                (if-not (= :success (:type r2))
                  ;; The first call DID allocate, so this op is still evidence
                  ;; for the duplicate checker even though the replay failed.
                  (assoc op :type :ok :value {:allocation (:allocation r1)
                                              :replay-error (:type r2)
                                              :node node})
                  (assoc op :type :ok :value {:allocation (:allocation r1)
                                              :replay     (:allocation r2)
                                              :node       node})))))))))

  (teardown! [this test])

  (close! [this test]))

;; ---------------------------------------------------------------------------
;; Checkers
;; ---------------------------------------------------------------------------

(defn- allocations
  "Every allocation this history observed, as inclusive [start end] intervals.

  A `:next-twice` whose replay agreed contributes ONE interval — counting the
  replay separately would report the server's correct behaviour as a duplicate.
  A replay that disagreed contributes both, because two distinct values really
  were consumed."
  [history]
  (->> history
       (filter #(and (= :ok (:type %)) (:value %)))
       (mapcat (fn [op]
                 (let [{:keys [allocation replay node]} (:value op)
                       base (when allocation
                              [{:start (:start allocation)
                                :end   (:end allocation)
                                :count (:count allocation)
                                :node  node
                                :f     (:f op)
                                :process (:process op)
                                :time  (:time op)}])]
                   (if (and replay allocation (not= replay allocation))
                     (conj (vec base)
                           {:start (:start replay) :end (:end replay)
                            :count (:count replay) :node node
                            :f :next-twice-replay
                            :process (:process op) :time (:time op)})
                     base))))
       (remove #(or (nil? (:start %)) (nil? (:end %))))
       (sort-by :start)
       vec))

(def min-allocations
  "Below this many observed allocations a run has not exercised the sequencer
  meaningfully, and the checkers report :valid? :unknown rather than success.

  An empty history satisfies 'no duplicates' perfectly. Without this floor, a
  run in which every operation failed — a botched setup, a cluster that never
  elected, a wrong sequence name — is indistinguishable from a clean run, and
  reads as a pass. See the `:empty-transaction-graph` washouts in FINDINGS.md
  for the same failure mode in the append workload."
  25)

(defn duplicate-checker
  "Fails if any value was handed out more than once.

  Gaps between allocations are expected and reported as statistics only."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [as   (allocations history)
            dups (->> (partition 2 1 as)
                      (keep (fn [[a b]]
                              ;; Sorted by start, so an overlap means b begins
                              ;; at or before a's inclusive end.
                              (when (<= (:start b) (:end a))
                                {:a a :b b
                                 :overlap [(:start b) (min (:end a) (:end b))]})))
                      (into []))
            values (reduce + 0 (map #(inc (- (:end %) (:start %))) as))
            gaps   (->> (partition 2 1 as)
                        (keep (fn [[a b]]
                                (when (> (:start b) (inc (:end a)))
                                  (- (:start b) (inc (:end a))))))
                        (reduce + 0))]
        {:valid?           (cond
                             (seq dups)                        false
                             (< (count as) min-allocations)    :unknown
                             :else                             true)
         :allocation-count (count as)
         :values-allocated values
         :max-value        (when (seq as) (apply max (map :end as)))
         :gap-total        gaps
         :insufficient-data (when (< (count as) min-allocations)
                              (str "only " (count as) " allocations observed; "
                                   "need " min-allocations " to call a run clean"))
         :duplicates       (take 10 dups)}))))

(defn range-integrity-checker
  "Fails if an allocation's own arithmetic is inconsistent — `end - start + 1`
  must equal the `count` the server reported, and a reserve must return the
  number of values it was asked for.

  This is cheap and local, but it is the difference between 'the range is
  plausible' and 'the range is what the caller will actually use': a client
  trusting `count` while iterating `start..end` would silently skip or reuse
  ids if they disagree."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [bad (->> history
                     (filter #(and (= :ok (:type %)) (:value %)))
                     (keep (fn [op]
                             (let [{:keys [allocation requested]} (:value op)]
                               (when allocation
                                 (let [{:keys [start end count]} allocation
                                       span (inc (- end start))]
                                   (cond
                                     (not= span count)
                                     {:type :count-mismatch :allocation allocation
                                      :span span :process (:process op)}

                                     (and requested (not= requested count))
                                     {:type :wrong-size :allocation allocation
                                      :requested requested :process (:process op)}

                                     (neg? span)
                                     {:type :inverted-range :allocation allocation
                                      :process (:process op)}))))))
                     (into []))]
        {:valid?     (empty? bad)
         :violations (take 10 bad)}))))

(defn idempotency-checker
  "Fails if replaying an idempotency key returned a different allocation.

  A mismatch means the replay consumed a *second* value, which is the failure
  mode that makes retrying a timed-out `next` unsafe. Replays that errored are
  counted but not failed: an error is a retryable answer, whereas a different
  value is a wrong one."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [pairs (->> history
                       (filter #(and (= :ok (:type %))
                                     (= :next-twice (:f %))
                                     (:value %)))
                       (into []))
            mismatches (->> pairs
                            (keep (fn [op]
                                    (let [{:keys [allocation replay]} (:value op)]
                                      (when (and replay (not= allocation replay))
                                        {:first allocation :replay replay
                                         :process (:process op) :time (:time op)}))))
                            (into []))
            errored (count (filter #(get-in % [:value :replay-error]) pairs))]
        {:valid?        (cond
                          (seq mismatches)                        false
                          ;; Every replay erroring means idempotency was never
                          ;; actually exercised, however many ops ran.
                          (zero? (- (count pairs) errored))       :unknown
                          :else                                   true)
         :replays       (- (count pairs) errored)
         :replay-errors errored
         :mismatches    (take 10 mismatches)}))))

;; ---------------------------------------------------------------------------
;; Workload
;; ---------------------------------------------------------------------------

(defn workload
  "Options:
    :sequence-name        sequence to hammer (default \"jepsen/sequencer/s1\")
    :sequence-reserve-max largest run a :reserve op may request"
  [opts]
  (let [seq-name    (:sequence-name opts "jepsen/sequencer/s1")
        reserve-max (:sequence-reserve-max opts 10)]
    {:client    (SequencerClient. nil seq-name reserve-max)
     :checker   (checker/compose
                  {:duplicates  (duplicate-checker)
                   :integrity   (range-integrity-checker)
                   :idempotency (idempotency-checker)
                   :timeline    (timeline/html)})
     ;; Weighted toward :next — single-value allocation is the common path and
     ;; the one that drains a block fastest, forcing the block bumps that make
     ;; leader changes interesting.
     :generator (gen/mix [(repeat {:type :invoke, :f :next})
                          (repeat {:type :invoke, :f :next})
                          (repeat {:type :invoke, :f :next-twice})
                          (repeat {:type :invoke, :f :reserve})])}))
