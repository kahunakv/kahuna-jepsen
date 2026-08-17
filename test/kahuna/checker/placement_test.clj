(ns kahuna.checker.placement-test
  "Negative controls for the placement checker.

  This checker's most important behaviour is not a failure, it is a refusal: a
  run that never moved a replica must come back `:unknown`, not `true`. That is
  the one verdict nothing else in the suite would notice being wrong — a vacuous
  pass looks exactly like a real one, and this repository has twice shipped
  weeks of runs that proved nothing while reporting health.

  So each test fixes a sample stream whose answer is known by construction, and
  the pairs matter more than the individual cases: for every history the checker
  must reject there is a neighbouring one it must accept, so a checker that
  simply failed everything could not pass this file.

  Log-derived evidence is switched off here by requiring only `:move`, which is
  measurable from samples alone — these tests have no store directory and no
  server logs to read."
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [kahuna.checker.placement :as placement]))

(def nodes ["n1" "n2" "n3" "n4"])

(def base-test
  "A four-node test at RF 3, requiring only sample-derived evidence."
  {:nodes nodes
   :replication-factor 3
   :require-placement-evidence #{:move}})

(defn ep [n] (str n ":8082"))

(defn view
  "One partition's placement view. `replicas` is a seq of [node role]."
  [generation replicas]
  {:state "Active"
   :generation generation
   :effective-rf 3
   :hosted-locally false
   :replicas (mapv (fn [[n role]] {:endpoint (ep n) :role role}) replicas)})

(defn placement
  "One node's whole placement table."
  [node partitions]
  {:replication-factor 3
   :rebalancer true
   :initialized true
   :endpoint (ep node)
   :hosted-count 0
   :partitions partitions})

(defn sample
  "A placement sample op at `ms`, where every node reports `partitions`.

  The sampler's ops are :type :info both on invocation and on completion, so the
  invocation carries no value — included here because the checker has to filter
  it out, and a checker that counted both would double every observation."
  [ms partitions]
  [{:type :info :process :nemesis :f :placement-sample
    :time (* 1000000 ms) :value nil}
   {:type :info :process :nemesis :f :placement-sample
    :time (* 1000000 (inc ms))
    :value (into (sorted-map)
                 (map (fn [n] [n (placement n partitions)]))
                 nodes)}])

(defn check* [t samples]
  (checker/check (placement/checker) t (vec (apply concat samples)) {}))

;; A range that never moves: the same three voters at generation 1 throughout.
(def steady {1 (view 1 [["n1" :voter] ["n2" :voter] ["n3" :voter]])})

;; The same range after a completed move from n3 to n4.
(def moved {1 (view 3 [["n1" :voter] ["n2" :voter] ["n4" :voter]])})

;; Mid-move: n4 is a Learner alongside the three voters.
(def moving {1 (view 2 [["n1" :voter] ["n2" :voter] ["n3" :voter] ["n4" :learner]])})

;; ---------------------------------------------------------------------------
;; The refusal: a run that exercised nothing is not a pass
;; ---------------------------------------------------------------------------

(deftest a-run-that-never-moved-a-replica-is-unknown
  ;; Nothing is wrong with this cluster. Every invariant holds, every node
  ;; agrees, no fault broke anything -- and the run proves nothing at all,
  ;; because the code under test never ran. :valid? true here would be the
  ;; single most misleading result this checker could produce.
  (let [r (check* base-test [(sample 0 steady)
                             (sample 1000 steady)
                             (sample 2000 steady)])]
    (is (= :unknown (:valid? r)))
    (is (= :vacuous (:cause r)))
    (is (= [:move] (:missing r)))))

(deftest a-run-that-moved-a-replica-is-valid
  ;; The neighbouring history: identical except that the range actually moved
  ;; from n3 to n4. Same invariants, same agreement -- the only difference is
  ;; that there is now something to have checked.
  (let [r (check* base-test [(sample 0 steady)
                             (sample 1000 moving)
                             (sample 2000 moved)])]
    (is (true? (:valid? r)))
    (is (= 1 (get-in r [:movement :replicas-gained])))
    (is (= 1 (get-in r [:movement :replicas-lost])))
    (is (= [{:partition 1 :generation 3 :gained [(ep "n4")] :lost [(ep "n3")]}]
           (:transitions r)))))

(deftest evidence-absent-and-evidence-unmeasurable-are-different-answers
  ;; With no samples and no logs there is nothing to read, and the checker must
  ;; say so rather than concluding "no moves happened". The distinction is the
  ;; whole reason `:measured?` exists: :vacuous invites re-running with the
  ;; nemesis on, :unmeasured invites fixing the observability.
  (let [r (check* base-test [])]
    (is (= :unknown (:valid? r)))
    (is (= :unmeasured (:cause r)))
    (is (= [:move] (:unmeasured r)))))

(deftest requiring-no-evidence-lets-a-quiet-run-pass
  ;; The escape hatch has to work, or a run deliberately configured without the
  ;; placement nemesis could never be green.
  (let [r (check* (assoc base-test :require-placement-evidence #{})
                  [(sample 0 steady)])]
    (is (true? (:valid? r)))))

(deftest full-replication-makes-no-claim
  ;; At RF 0 there are no replica sets, so every property here is vacuously
  ;; true. Returning :unknown for a legacy run would make the whole existing
  ;; nightly matrix inconclusive overnight.
  (let [r (check* (assoc base-test :replication-factor 0) [])]
    (is (true? (:valid? r)))
    (is (= 0 (:replication-factor r)))))

;; ---------------------------------------------------------------------------
;; Safety
;; ---------------------------------------------------------------------------

(deftest two-movers-on-one-range-is-a-violation
  ;; Kommander refuses to start a second move while a range has a transitional
  ;; replica, precisely so successive configurations overlap by a quorum. Two at
  ;; once means that discipline broke.
  (let [two {1 (view 2 [["n1" :voter] ["n2" :voter]
                        ["n3" :learner] ["n4" :learner]])}
        r   (check* base-test [(sample 0 steady) (sample 1000 two)])]
    (is (false? (:valid? r)))
    (is (= #{:multiple-movers} (set (map :type (:violations r)))))))

(deftest one-mover-with-a-removing-replica-is-fine
  ;; The accepting twin: a Learner promoted and the outgoing replica marked
  ;; Removing is one mover, not two, and is the ordinary shape of a move. A
  ;; checker that counted transitional replicas without distinguishing this
  ;; would fail every healthy run.
  (let [one {1 (view 2 [["n1" :voter] ["n2" :voter] ["n4" :voter]
                        ["n3" :removing]])}
        r   (check* base-test [(sample 0 steady) (sample 1000 one)])]
    (is (true? (:valid? r)))))

(deftest nodes-disagreeing-at-one-generation-is-a-violation
  ;; The map is committed on the meta partition, so a generation names exactly
  ;; one replica set. Two nodes describing generation 1 differently means they
  ;; are routing from different maps while believing they agree.
  (let [ops [{:type :info :process :nemesis :f :placement-sample :time 0 :value nil}
             {:type :info :process :nemesis :f :placement-sample :time 1000000
              :value {"n1" (placement "n1" steady)
                      ;; Same generation, different replica set.
                      "n2" (placement "n2" {1 (view 1 [["n1" :voter]
                                                       ["n2" :voter]
                                                       ["n4" :voter]])})}}]
        r   (checker/check (placement/checker) base-test ops {})]
    (is (false? (:valid? r)))
    (is (= 1 (count (:disagreements r))))
    (is (= 1 (:partition (first (:disagreements r)))))))

(deftest lagging-views-are-not-disagreement-and-not-movement
  ;; n2 is a generation behind -- a normal, expected state, since a view that
  ;; lags fails closed to MustRetry rather than serving wrong data. Two things
  ;; must follow: it is not a disagreement (the generations differ), and the
  ;; move is counted once rather than once forward and once back. Ordering
  ;; observations by arrival instead of by generation would report three moves
  ;; here.
  (let [ops [{:type :info :process :nemesis :f :placement-sample :time 0 :value nil}
             {:type :info :process :nemesis :f :placement-sample :time 1000000
              :value {"n1" (placement "n1" moved)
                      "n2" (placement "n2" steady)}}
             {:type :info :process :nemesis :f :placement-sample :time 2000000
              :value nil}
             {:type :info :process :nemesis :f :placement-sample :time 3000000
              :value {"n1" (placement "n1" moved)
                      "n2" (placement "n2" moved)}}]
        r   (checker/check (placement/checker) base-test ops {})]
    (is (true? (:valid? r)))
    (is (empty? (:disagreements r)))
    (is (= 1 (get-in r [:movement :replicas-gained])))
    (is (= 1 (get-in r [:movement :replicas-lost])))))

(deftest a-placed-range-with-no-voter-is-a-violation
  ;; A replica set of learners alone can commit nothing: there is no quorum to
  ;; elect a leader and no way back without operator intervention.
  (let [none {1 (view 2 [["n1" :learner]])}
        r    (check* base-test [(sample 0 steady) (sample 1000 none)])]
    (is (false? (:valid? r)))
    (is (contains? (set (map :type (:violations r))) :no-voters))))

(deftest an-empty-replica-set-is-full-replication-not-a-missing-voter
  ;; The accepting twin of the test above. An empty replica set is the legacy
  ;; mode -- every roster voter hosts the range -- and reading it as "no voters"
  ;; would fail every mixed-mode cluster, which the operations guide documents
  ;; as safe.
  (let [legacy {1 (view 1 [])}
        r      (check* (assoc base-test :require-placement-evidence #{})
                       [(sample 0 legacy)])]
    (is (true? (:valid? r)))))

(deftest a-replica-on-an-unknown-endpoint-is-a-violation
  ;; A map naming a node the roster does not contain has outlived its cluster:
  ;; whatever quorum arithmetic is being done on it counts a replica nobody can
  ;; reach.
  (let [ghost {1 (view 2 [["n1" :voter] ["n2" :voter] ["n9" :voter]])}
        r     (check* base-test [(sample 0 steady) (sample 1000 ghost)])]
    (is (false? (:valid? r)))
    (is (contains? (set (map :type (:violations r))) :unknown-endpoint))))

;; ---------------------------------------------------------------------------
;; Reporting
;; ---------------------------------------------------------------------------

(deftest learners-are-counted-when-they-are-seen
  ;; A Learner exists only between the add and the promotion. Counting them is
  ;; how a run distinguishes "the sampler was too slow to see the move" from
  ;; "the move skipped the state", which are very different bugs.
  (let [r (check* base-test [(sample 0 steady) (sample 1000 moving) (sample 2000 moved)])]
    (is (= 1 (get-in r [:transitional :learners])))
    (is (= 0 (get-in r [:transitional :removings])))))
