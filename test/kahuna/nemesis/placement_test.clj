(ns kahuna.nemesis.placement-test
  "Controls for what the placement nemesis actually emits.

  The generator is the whole fault: whatever it does not emit is not tested,
  however healthy the run looks afterwards. `--placement-nodes-out 0` exists so
  a profile can keep the replication-factor overrides — which still drive add,
  seed, promote and retire — while dropping the decommission, and the thing to
  guard is that 'drop the decommission' does not quietly become 'drop the
  fault'."
  (:require [clojure.test :refer [deftest is testing]]
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
