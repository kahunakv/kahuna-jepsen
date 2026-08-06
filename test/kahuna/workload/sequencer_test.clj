(ns kahuna.workload.sequencer-test
  "Negative controls for the sequencer checkers.

  Each checker gets a history containing a known violation (it must fail) AND a
  history whose shape looks suspicious but is legal (it must pass). The second
  half matters more than the first: this workload's whole design rests on gaps
  and out-of-order values being acceptable, so a checker that rejects them would
  turn correct server behaviour into a red run."
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [kahuna.workload.sequencer :as seqr]))

(def test-map {:concurrency 2})

(defn alloc-op
  "One :ok allocation op. `end` is inclusive."
  ([process f start end] (alloc-op process f start end (inc (- end start))))
  ([process f start end cnt]
   {:type :ok :f f :process process :time (* 1000000 start)
    :value {:allocation {:start start :end end :count cnt :revision 1}
            :node "n1"}}))

(defn check* [c history]
  (checker/check c test-map history {}))

;; ---------------------------------------------------------------------------
;; Duplicates — the core safety property
;; ---------------------------------------------------------------------------

(deftest duplicates-caught-when-same-value-issued-twice
  (let [history [(alloc-op 0 :next 5 5)
                 (alloc-op 1 :next 5 5)]
        r (check* (seqr/duplicate-checker) history)]
    (is (false? (:valid? r)))
    (is (= 1 (count (:duplicates r))))))

(deftest duplicates-caught-when-ranges-overlap
  ;; Reserve [10,19] then [15,24]: 15..19 handed out twice.
  (let [history [(alloc-op 0 :reserve 10 19)
                 (alloc-op 1 :reserve 15 24)]
        r (check* (seqr/duplicate-checker) history)]
    (is (false? (:valid? r)))
    (is (= [15 19] (:overlap (first (:duplicates r)))))))

(deftest duplicates-caught-when-a-range-contains-a-single-value
  ;; A block replay after a leader change: [100,109] reserved, then 105 served
  ;; again on its own. Sorting by start puts the range first, so this only
  ;; passes if the check compares against the inclusive END, not the next start.
  (let [history [(alloc-op 0 :reserve 100 109)
                 (alloc-op 1 :next 105 105)]
        r (check* (seqr/duplicate-checker) history)]
    (is (false? (:valid? r)))))

(deftest gaps-are-legal
  ;; A leader change surrenders the rest of a block, so 6..99 are simply burned.
  ;; This MUST pass — treating it as lost ids would fail nearly every run with a
  ;; nemesis.
  (let [history [(alloc-op 0 :next 5 5)
                 (alloc-op 1 :next 100 100)]
        r (check* (seqr/duplicate-checker) history)]
    (is (empty? (:duplicates r)))
    (is (= 94 (:gap-total r)))))

(deftest out-of-order-allocation-is-legal
  ;; Node B (block [200,…]) answers at t=1, node A (block [100,…]) answers at
  ;; t=2. Values go "backwards" in time with no duplicate. Legal by design.
  (let [history [{:type :ok :f :next :process 1 :time 1000
                  :value {:allocation {:start 205 :end 205 :count 1} :node "n2"}}
                 {:type :ok :f :next :process 0 :time 2000
                  :value {:allocation {:start 105 :end 105 :count 1} :node "n1"}}]
        r (check* (seqr/duplicate-checker) history)]
    (is (empty? (:duplicates r)))))

(deftest indeterminate-and-failed-ops-are-ignored
  ;; An :info next may have consumed a value; we never saw which, so it is a
  ;; gap. It must not contribute an interval, and must not crash the checker.
  (let [history [(alloc-op 0 :next 1 1)
                 {:type :info :f :next :process 1 :time 100 :error :timeout}
                 {:type :fail :f :next :process 2 :time 200 :error :not-found}
                 (alloc-op 3 :next 2 2)]
        r (check* (seqr/duplicate-checker) history)]
    (is (empty? (:duplicates r)))
    (is (= 2 (:allocation-count r)))))

(deftest empty-history-is-unknown-not-clean
  ;; The washout guard. A run where every op failed satisfies "no duplicates"
  ;; vacuously; reporting that as success is how a broken setup masquerades as
  ;; a green run. Caught exactly this way during the workload's first smoke run.
  (let [r (check* (seqr/duplicate-checker) [])]
    (is (= :unknown (:valid? r)))
    (is (some? (:insufficient-data r)))))

(deftest thin-history-is-unknown-not-clean
  (let [history (map #(alloc-op 0 :next % %) (range 1 5))
        r (check* (seqr/duplicate-checker) history)]
    (is (= :unknown (:valid? r)))))

(deftest a-duplicate-in-a-thin-history-still-fails
  ;; The floor must not swallow a real violation: too little data is only
  ;; "unknown" when nothing is actually wrong.
  (let [history [(alloc-op 0 :next 5 5)
                 (alloc-op 1 :next 5 5)]
        r (check* (seqr/duplicate-checker) history)]
    (is (false? (:valid? r)))))

(deftest sufficient-history-is-clean
  (let [history (map #(alloc-op 0 :next % %) (range 1 (+ 1 seqr/min-allocations)))
        r (check* (seqr/duplicate-checker) history)]
    (is (true? (:valid? r)))))

(deftest idempotency-unknown-when-every-replay-errored
  ;; Ops ran, but none actually tested a replay.
  (let [history [{:type :ok :f :next-twice :process 0 :time 1
                  :value {:allocation {:start 5 :end 5 :count 1}
                          :replay-error :must-retry :node "n1"}}]
        r (check* (seqr/idempotency-checker) history)]
    (is (= :unknown (:valid? r)))))

(deftest adjacent-allocations-are-not-duplicates
  ;; [1,5] then [6,10] share no value. An off-by-one in the overlap test would
  ;; report these, and they are the single most common shape in a real history.
  (let [history [(alloc-op 0 :reserve 1 5)
                 (alloc-op 1 :reserve 6 10)]
        r (check* (seqr/duplicate-checker) history)]
    (is (empty? (:duplicates r)))
    (is (zero? (:gap-total r)))
    (is (= 10 (:values-allocated r)))))

;; ---------------------------------------------------------------------------
;; Range integrity
;; ---------------------------------------------------------------------------

(deftest integrity-catches-count-disagreeing-with-span
  ;; Server says 3 values but the range spans 5.
  (let [history [(alloc-op 0 :reserve 10 14 3)]
        r (check* (seqr/range-integrity-checker) history)]
    (is (false? (:valid? r)))
    (is (= :count-mismatch (:type (first (:violations r)))))))

(deftest integrity-catches-reserve-returning-wrong-size
  (let [history [{:type :ok :f :reserve :process 0 :time 1
                  :value {:allocation {:start 1 :end 3 :count 3}
                          :requested 7 :node "n1"}}]
        r (check* (seqr/range-integrity-checker) history)]
    (is (false? (:valid? r)))
    (is (= :wrong-size (:type (first (:violations r)))))))

(deftest integrity-accepts-consistent-allocations
  (let [history [(alloc-op 0 :next 7 7)
                 {:type :ok :f :reserve :process 1 :time 2
                  :value {:allocation {:start 8 :end 12 :count 5}
                          :requested 5 :node "n1"}}]
        r (check* (seqr/range-integrity-checker) history)]
    (is (true? (:valid? r)))))

;; ---------------------------------------------------------------------------
;; Idempotency
;; ---------------------------------------------------------------------------

(deftest idempotency-catches-replay-returning-a-different-value
  ;; The failure that makes retrying a timed-out `next` unsafe: the same key
  ;; came back with a second, distinct value.
  (let [history [{:type :ok :f :next-twice :process 0 :time 1
                  :value {:allocation {:start 5 :end 5 :count 1}
                          :replay     {:start 6 :end 6 :count 1}
                          :node "n1"}}]
        r (check* (seqr/idempotency-checker) history)]
    (is (false? (:valid? r)))
    (is (= 1 (count (:mismatches r))))))

(deftest idempotency-accepts-identical-replay
  (let [history [{:type :ok :f :next-twice :process 0 :time 1
                  :value {:allocation {:start 5 :end 5 :count 1}
                          :replay     {:start 5 :end 5 :count 1}
                          :node "n1"}}]
        r (check* (seqr/idempotency-checker) history)]
    (is (true? (:valid? r)))
    (is (= 1 (:replays r)))))

(deftest idempotency-tolerates-a-failed-replay
  ;; An errored replay is a retryable answer, not a wrong one.
  (let [history [{:type :ok :f :next-twice :process 0 :time 1
                  :value {:allocation {:start 5 :end 5 :count 1}
                          :replay-error :must-retry
                          :node "n1"}}]
        r (check* (seqr/idempotency-checker) history)]
    ;; No mismatch is reported — but with every replay errored, idempotency was
    ;; never actually exercised, so the verdict is :unknown rather than clean.
    (is (empty? (:mismatches r)))
    (is (= 1 (:replay-errors r)))))

(deftest divergent-replay-also-counts-as-two-allocations
  ;; A replay that returned a different value consumed a second id, so the
  ;; duplicate checker must see BOTH intervals — otherwise a server that
  ;; double-allocates on replay could still look gap-clean.
  (let [history [{:type :ok :f :next-twice :process 0 :time 1
                  :value {:allocation {:start 5 :end 5 :count 1}
                          :replay     {:start 6 :end 6 :count 1}
                          :node "n1"}}]
        r (check* (seqr/duplicate-checker) history)]
    (is (= 2 (:allocation-count r)))))

(deftest agreeing-replay-counts-once
  ;; The dual: a correct replay must NOT be counted twice, or every honest
  ;; :next-twice would self-report as a duplicate.
  (let [history [{:type :ok :f :next-twice :process 0 :time 1
                  :value {:allocation {:start 5 :end 5 :count 1}
                          :replay     {:start 5 :end 5 :count 1}
                          :node "n1"}}]
        r (check* (seqr/duplicate-checker) history)]
    (is (empty? (:duplicates r)))
    (is (= 1 (:allocation-count r)))))
