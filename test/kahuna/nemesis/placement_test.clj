(ns kahuna.nemesis.placement-test
  "Controls for what the placement nemesis actually emits.

  The generator is the whole fault: whatever it does not emit is not tested,
  however healthy the run looks afterwards. `--placement-nodes-out 0` exists so
  a profile can keep the replication-factor overrides — which still drive add,
  seed, promote and retire — while dropping the decommission, and the thing to
  guard is that 'drop the decommission' does not quietly become 'drop the
  fault'."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.context :as context]
            [kahuna.nemesis.placement :as placement]))

(def ^:private gen #'placement/fault-generator)

(defn- ops
  "The first n op names the generator would emit."
  [nodes-out n]
  (mapv :f (take n (gen nodes-out))))

(deftest zero-nodes-out-drops-the-roster-churn
  (let [fs (set (ops 0 40))]
    (testing "no node ever leaves the roster"
      (is (not (contains? fs :decommission)))
      (is (not (contains? fs :recommission))))

    (testing "the overrides still run, so the fault is not a no-op"
      ;; This is the half that matters. A profile that drops the drain and
      ;; emits nothing at all would pass its vacuity gate on a technicality
      ;; and prove nothing about replica movement.
      (is (contains? fs :set-rf))
      (is (contains? fs :clear-rf)))))

(deftest zero-nodes-out-alternates-raise-and-clear
  (testing "an override is always cleared before the next is raised"
    ;; Two :set-rf in a row would leave the second with nothing to do: the
    ;; nemesis holds one override per partition.
    (is (= [:set-rf :clear-rf :set-rf :clear-rf :set-rf :clear-rf]
           (ops 0 6)))))

(deftest one-node-out-is-the-leave-rejoin-cycle
  (testing "unchanged from before the zero case existed"
    (is (= [:decommission :set-rf :clear-rf :recommission]
           (ops 1 4)))))

(deftest three-nodes-out-walks-the-roster-down-and-back
  (testing "three leaves before anything rejoins — the scale-down scenario"
    (let [fs (ops 3 12)]
      (is (= [:decommission :set-rf :clear-rf
              :decommission :set-rf :clear-rf
              :decommission :set-rf :clear-rf
              :recommission :recommission :recommission]
             fs))
      (testing "every node that left comes back in the same pass"
        (is (= (count (filter #{:decommission} fs))
               (count (filter #{:recommission} fs))))))))

(deftest negative-nodes-out-is-treated-as-zero
  (testing "a nonsense value degrades to overrides-only, not to a crash"
    ;; The CLI validates this, but the generator is called from tests and from
    ;; a test map that may have been merged from several sources.
    (is (= [:set-rf :clear-rf] (ops -2 2)))))

;; ---------------------------------------------------------------------------
;; Where a decommission is aimed, and when the first one may fire
;; ---------------------------------------------------------------------------

(def ^:private test-map {:nodes ["n1" "n2" "n3" "n4" "n5" "n6"]})

(defn- replicas
  "A placement view with the given endpoints as voters (plus optional
  learners) of one partition, other partitions elsewhere."
  [pid voters & [learners]]
  {:partitions {pid {:replicas (concat (map (fn [n] {:endpoint (str n ":8082") :role :voter}) voters)
                                       (map (fn [n] {:endpoint (str n ":8082") :role :learner}) learners))}
                9   {:replicas [{:endpoint "n6:8082" :role :voter}]}}})

(deftest decommission-prefers-a-voter-of-the-written-partition
  (let [placement (replicas 3 ["n1" "n2" "n3"])]
    (testing "every draw is one of the partition's voters"
      (dotimes [_ 50]
        (let [t (placement/decommission-target test-map placement #{3} #{})]
          (is (contains? #{"n1" "n2" "n3"} (:node t)))
          (is (= 3 (:partition t)))
          (is (= :written-partition-host (:reason t))))))))

(deftest decommission-skips-nodes-already-out-and-learners
  (let [placement (replicas 3 ["n1" "n2"] ["n5"])]
    (dotimes [_ 50]
      (let [t (placement/decommission-target test-map placement #{3} #{"n1"})]
        (testing "n1 is out, n5 is only a learner: n2 is the one voter left"
          (is (= "n2" (:node t)))
          (is (= :written-partition-host (:reason t))))))))

(deftest decommission-falls-back-to-any-node-with-a-reason
  (testing "no written partition known: random, and says so"
    (dotimes [_ 20]
      (let [t (placement/decommission-target test-map (replicas 3 ["n1"]) nil #{"n1"})]
        (is (contains? #{"n2" "n3" "n4" "n5" "n6"} (:node t)))
        (is (= :written-partition-unknown (:reason t)))
        (is (nil? (:partition t))))))

  (testing "written partition known but every voter is out: random, and says so"
    (dotimes [_ 20]
      (let [t (placement/decommission-target test-map (replicas 3 ["n1" "n2"]) #{3} #{"n1" "n2"})]
        (is (contains? #{"n3" "n4" "n5" "n6"} (:node t)))
        (is (= :no-eligible-host (:reason t))))))

  (testing "no placement view at all behaves like no written partition"
    (let [t (placement/decommission-target test-map nil #{3} #{})]
      (is (some? (:node t)))
      (is (= :no-eligible-host (:reason t)))))

  (testing "nothing eligible at all: nil, not an exception"
    (is (nil? (placement/decommission-target test-map (replicas 3 ["n1"]) #{3}
                                             (set (:nodes test-map)))))))

(deftest decommission-covers-every-written-partition
  (testing "a key-range space spread over two partitions draws from both"
    (let [placement {:partitions {2 {:replicas [{:endpoint "n1:8082" :role :voter}]}
                                  5 {:replicas [{:endpoint "n4:8082" :role :voter}]}}}
          drawn     (set (repeatedly 100 #(:node (placement/decommission-target
                                                   test-map placement #{2 5} #{}))))]
      (is (= #{"n1" "n4"} drawn)))))

(defn- first-op-time
  "The :time, in seconds, of the first op a generator schedules at ctx time 0."
  [g]
  (let [test (assoc test-map :concurrency 2)
        ctx  (context/context test)
        [op] (gen/op g test ctx)]
    (/ (:time op) 1e9)))

(deftest warmup-delays-the-first-op-without-blocking
  (testing "the first fault is scheduled at the warm-up, not at zero"
    (is (= 30.0 (first-op-time (placement/not-before 30 (gen 3))))))

  (testing "zero is exactly the old behaviour"
    (is (= 0.0 (first-op-time (placement/not-before 0 (gen 3))))))

  (testing "an op already later than the warm-up keeps its own time"
    (let [test (assoc test-map :concurrency 2)
          ctx  (assoc (context/context test) :time (long 90e9))
          [op] (gen/op (placement/not-before 30 (gen 3)) test ctx)]
      (is (= 90.0 (/ (:time op) 1e9)))))

  (testing "the delayed op is still the decommission — nothing is dropped"
    (let [test (assoc test-map :concurrency 2)
          ctx  (context/context test)
          [op] (gen/op (placement/not-before 30 (gen 3)) test ctx)]
      (is (= :decommission (:f op))))))
