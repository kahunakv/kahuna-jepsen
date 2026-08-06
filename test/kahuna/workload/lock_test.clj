(ns kahuna.workload.lock-test
  "Negative controls for the lock checkers.

  A checker that cannot fail is worse than no checker: it converts 'we did not
  look' into a green tick. These tests feed hand-built histories containing
  known violations and assert the checkers reject them — and, just as
  importantly, feed histories whose overlap is explained by lease expiry and
  assert they are accepted."
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [kahuna.workload.lock :as lock]))

(def ms 1000000)                      ; ns per ms
(def test-map {:concurrency 2})
(def expires-ms 10000)                ; 10s lease
(def margin-ms 0)                     ; no slack, so the arithmetic is exact

(defn acquire-ops
  "An invoke/ok acquire pair for one thread."
  [process owner token invoke-t ok-t]
  [{:type :invoke :f :acquire :process process :time (* ms invoke-t) :value nil}
   {:type :ok :f :acquire :process process :time (* ms ok-t)
    :value {:owner owner :token token}}])

(defn check* [c history]
  (checker/check c test-map history {}))

(deftest exclusion-catches-genuine-overlap
  ;; A holds from t=1000ms with a lease to t=10000ms; B acquires at t=3000ms,
  ;; well inside A's lease. Nothing about expiry explains this.
  (let [history (concat (acquire-ops 0 "A" 1 0    1000)
                        (acquire-ops 1 "B" 2 2000 3000))
        r (check* (lock/exclusion-checker expires-ms margin-ms) history)]
    (is (false? (:valid? r)))
    (is (= 1 (count (:violations r))))))

(deftest exclusion-allows-expiry-explained-overlap
  ;; A's lease can lapse at t=10000ms (invoke 0 + 10s). B acquires at
  ;; t=12000ms, after that. This is Kahuna working as designed, not a bug.
  (let [history (concat (acquire-ops 0 "A" 1 0     1000)
                        (acquire-ops 1 "B" 2 11000 12000))
        r (check* (lock/exclusion-checker expires-ms margin-ms) history)]
    (is (true? (:valid? r)))))

(deftest exclusion-allows-indeterminate-release-then-retry
  ;; A acquires, invokes a release at t=2000ms that times out (:info), and the
  ;; renumbered process retries the release much later, which fails. B acquires
  ;; at t=3000ms — AFTER A's first release attempt, so A's hold is no longer
  ;; definite (the first release may have committed server-side with its ack
  ;; lost to a partition). The definite-hold window must end at the FIRST
  ;; release invoke, not be extended by the retry's invoke time — extending it
  ;; invented multi-second overlaps the server never produced.
  (let [history (concat (acquire-ops 0 "A" 1 0 1000)
                        [{:type :invoke :f :release :process 0 :time (* ms 2000) :value nil}
                         {:type :info   :f :release :process 0 :time (* ms 2500) :value nil}]
                        (acquire-ops 1 "B" 2 2600 3000)
                        ;; Jepsen renumbers the crashed process: 0 + concurrency = 2,
                        ;; same thread. Its retried release fails after B acquired.
                        [{:type :invoke :f :release :process 2 :time (* ms 9000) :value nil}
                         {:type :fail   :f :release :process 2 :time (* ms 9500) :value nil}])
        r (check* (lock/exclusion-checker expires-ms margin-ms) history)]
    (is (true? (:valid? r)))))

(deftest exclusion-still-catches-overlap-before-any-release
  ;; The dual control: B acquires strictly BEFORE A's first release attempt and
  ;; well inside A's lease. Keeping the earliest release-start must not weaken
  ;; this detection.
  (let [history (concat (acquire-ops 0 "A" 1 0 1000)
                        (acquire-ops 1 "B" 2 1500 2000)
                        [{:type :invoke :f :release :process 0 :time (* ms 3000) :value nil}
                         {:type :ok     :f :release :process 0 :time (* ms 3200) :value nil}])
        r (check* (lock/exclusion-checker expires-ms margin-ms) history)]
    (is (false? (:valid? r)))
    (is (= 1 (count (:violations r))))))

(deftest fencing-catches-token-going-backwards
  ;; A completes with token 5 at t=1000ms; B *starts* at t=2000ms — strictly
  ;; after — and comes back with a lower token.
  (let [history (concat (acquire-ops 0 "A" 5 0    1000)
                        (acquire-ops 1 "B" 3 2000 3000))
        r (check* (lock/fencing-checker) history)]
    (is (false? (:valid? r)))
    (is (= :token-went-backwards (:type (first (:violations r)))))))

(deftest fencing-catches-token-reuse-by-another-owner
  (let [history (concat (acquire-ops 0 "A" 5 0    1000)
                        (acquire-ops 1 "B" 5 2000 3000))
        r (check* (lock/fencing-checker) history)]
    (is (false? (:valid? r)))
    (is (= :token-reused-by-other-owner
           (:type (first (:violations r)))))))

(deftest fencing-allows-same-owner-reacquire
  ;; LockActor returns the existing FencingToken unchanged when the current
  ;; holder re-acquires. That must not be reported.
  (let [history (concat (acquire-ops 0 "A" 5 0    1000)
                        (acquire-ops 0 "A" 5 2000 3000))
        r (check* (lock/fencing-checker) history)]
    (is (true? (:valid? r)))))

(deftest fencing-allows-concurrent-acquires-out-of-order
  ;; B is invoked *before* A completes, so the two are concurrent and the
  ;; protocol promises no ordering between them. Flagging this would be a
  ;; false positive.
  (let [history (concat (acquire-ops 0 "A" 5 0   3000)
                        (acquire-ops 1 "B" 4 500 1000))
        r (check* (lock/fencing-checker) history)]
    (is (true? (:valid? r)))))
