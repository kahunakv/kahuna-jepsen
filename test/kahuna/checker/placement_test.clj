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
  server logs to read.

  Every history that expects a verdict other than `:unmeasured` opens with a
  `settle` marker: the checker's stalled-move property reads only the samples
  after the final heal, and a history with no heal recorded cannot answer it."
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [kahuna.checker.placement :as placement]))

(def nodes ["n1" "n2" "n3" "n4"])

(def base-test
  "A four-node test at RF 3, requiring only sample-derived evidence.

  `:faults` is load-bearing, not decoration: the vacuity gate applies only to
  runs carrying a fault that could have moved a replica, so a test map without
  one would quietly switch the gate off and every refusal test below would pass
  for the wrong reason.

  `:placement-pass-interval` is 100 ms so the stalled-move bound is ten passes
  = 1 s, which the seconds-long fixtures below can span. At the server default
  of 5 s every settle window here would be too short to measure and every
  verdict would be `:unmeasured`."
  {:nodes nodes
   :replication-factor 3
   :faults #{:placement}
   :placement-pass-interval 100
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

(defn sample-views
  "A placement sample op at `ms` where each node reports its own table:
  `views` is {node partitions-or-nil}. Nodes absent from `views` answer nil,
  as an unreachable node does.

  The sampler's ops are :type :info both on invocation and on completion, so the
  invocation carries no value — included here because the checker has to filter
  it out, and a checker that counted both would double every observation."
  [ms views]
  [{:type :info :process :nemesis :f :placement-sample
    :time (* 1000000 ms) :value nil}
   {:type :info :process :nemesis :f :placement-sample
    :time (* 1000000 (inc ms))
    :value (into (sorted-map)
                 (map (fn [n] [n (some->> (get views n) (placement n))]))
                 nodes)}])

(defn sample
  "A placement sample op at `ms`, where every node reports `partitions`."
  [ms partitions]
  (sample-views ms (zipmap nodes (repeat partitions))))

(defn settle
  "The sampler's settle marker at `ms`: the final heal is over and every sample
  after this one describes a healed cluster. Completes at exactly `ms`, so a
  `sample` at the same `ms` (which completes at `ms` + 1) is already settled."
  [ms]
  [{:type :info :process :nemesis :f :placement-settle
    :time (* 1000000 ms) :value nil}
   {:type :info :process :nemesis :f :placement-settle
    :time (* 1000000 ms) :value {:bound-secs 1.0 :window-secs 1.4}}])

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
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady)
                             (sample 1000 steady)
                             (sample 2000 steady)])]
    (is (= :unknown (:valid? r)))
    (is (= :vacuous (:cause r)))
    (is (= [:move] (:missing r)))))

(deftest a-run-that-moved-a-replica-is-valid
  ;; The neighbouring history: identical except that the range actually moved
  ;; from n3 to n4. Same invariants, same agreement -- the only difference is
  ;; that there is now something to have checked.
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady)
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
  ;; nemesis on, :unmeasured invites fixing the observability. The stall check
  ;; is in the same position -- no settle window, nothing to read -- and is
  ;; named alongside the evidence it could not find.
  (let [r (check* base-test [])]
    (is (= :unknown (:valid? r)))
    (is (= :unmeasured (:cause r)))
    (is (= [:move :stalled-move] (:unmeasured r)))))

(deftest requiring-no-evidence-lets-a-quiet-run-pass
  ;; The escape hatch has to work, or a run deliberately configured without the
  ;; placement nemesis could never be green.
  (let [r (check* (assoc base-test :require-placement-evidence #{})
                  [(settle 0) (sample 0 steady) (sample 1000 steady)])]
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
        r   (check* base-test [(settle 0) (sample 0 steady) (sample 1000 two)])]
    (is (false? (:valid? r)))
    (is (= #{:multiple-movers} (set (map :type (:violations r)))))))

(deftest one-mover-with-a-removing-replica-is-fine
  ;; The accepting twin: a Learner promoted and the outgoing replica marked
  ;; Removing is one mover, not two, and is the ordinary shape of a move. A
  ;; checker that counted transitional replicas without distinguishing this
  ;; would fail every healthy run.
  (let [one {1 (view 2 [["n1" :voter] ["n2" :voter] ["n4" :voter]
                        ["n3" :removing]])}
        r   (check* base-test [(settle 0) (sample 0 steady) (sample 1000 one)])]
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
  (let [r (check* base-test [(settle 0)
                             (sample-views 0 {"n1" moved "n2" steady})
                             (sample-views 2000 {"n1" moved "n2" moved})])]
    (is (true? (:valid? r)))
    (is (empty? (:disagreements r)))
    (is (= 1 (get-in r [:movement :replicas-gained])))
    (is (= 1 (get-in r [:movement :replicas-lost])))))

(deftest a-placed-range-with-no-voter-is-a-violation
  ;; A replica set of learners alone can commit nothing: there is no quorum to
  ;; elect a leader and no way back without operator intervention.
  (let [none {1 (view 2 [["n1" :learner]])}
        r    (check* base-test [(settle 0) (sample 0 steady) (sample 1000 none)])]
    (is (false? (:valid? r)))
    (is (contains? (set (map :type (:violations r))) :no-voters))))

(deftest an-empty-replica-set-is-full-replication-not-a-missing-voter
  ;; The accepting twin of the test above. An empty replica set is the legacy
  ;; mode -- every roster voter hosts the range -- and reading it as "no voters"
  ;; would fail every mixed-mode cluster, which the operations guide documents
  ;; as safe.
  (let [legacy {1 (view 1 [])}
        r      (check* (assoc base-test :require-placement-evidence #{})
                       [(settle 0) (sample 0 legacy) (sample 1000 legacy)])]
    (is (true? (:valid? r)))))

(deftest a-replica-on-an-unknown-endpoint-is-a-violation
  ;; A map naming a node the roster does not contain has outlived its cluster:
  ;; whatever quorum arithmetic is being done on it counts a replica nobody can
  ;; reach.
  (let [ghost {1 (view 2 [["n1" :voter] ["n2" :voter] ["n9" :voter]])}
        r     (check* base-test [(settle 0) (sample 0 steady) (sample 1000 ghost)])]
    (is (false? (:valid? r)))
    (is (contains? (set (map :type (:violations r))) :unknown-endpoint))))

;; ---------------------------------------------------------------------------
;; Reporting
;; ---------------------------------------------------------------------------

(deftest learners-are-counted-when-they-are-seen
  ;; A Learner exists only between the add and the promotion. Counting them is
  ;; how a run distinguishes "the sampler was too slow to see the move" from
  ;; "the move skipped the state", which are very different bugs.
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady) (sample 1000 moving) (sample 2000 moved)])]
    (is (= 1 (get-in r [:transitional :learners])))
    (is (= 0 (get-in r [:transitional :removings])))))

;; ---------------------------------------------------------------------------
;; The gate is scoped to runs that could have moved something
;; ---------------------------------------------------------------------------

(deftest a-partition-only-run-is-not-asked-to-prove-a-move
  ;; A replication factor with nothing but `partition` is a legitimate profile:
  ;; it tests that placed routing survives a network fault. No replica was ever
  ;; going to move, so "nothing moved" is the expected outcome and not a vacuous
  ;; result. Failing it would punish the run for not doing something it was
  ;; never asked to do — and a gate that fires on correct configurations is one
  ;; that gets switched off wholesale.
  (let [r (check* (assoc base-test :faults #{:partition})
                  [(settle 0) (sample 0 steady) (sample 1000 steady)])]
    (is (true? (:valid? r)))
    (is (= :off (:gate r)))
    (is (= :no-replica-moving-fault (:gate-reason r)))
    ;; Still measured and still reported — switching the gate off must not
    ;; switch off the observation, or a reader loses the ability to notice that
    ;; a run they expected to move things did not.
    (is (= 0 (get-in r [:evidence :move :count])))))

(deftest kill-alone-is-still-asked-to-prove-a-move
  ;; The accepting twin, and the reason the set is not just #{:placement}: a
  ;; dead node's ranges have to be repaired onto the survivors, so a kill run
  ;; that moved nothing is exactly as vacuous as a placement run that did.
  (let [r (check* (assoc base-test :faults #{:kill})
                  [(settle 0) (sample 0 steady) (sample 1000 steady)])]
    (is (= :unknown (:valid? r)))
    (is (= :vacuous (:cause r)))
    (is (nil? (:gate r)))))

(deftest membership-alone-is-still-gated
  (let [r (check* (assoc base-test :faults #{:membership})
                  [(settle 0) (sample 0 steady) (sample 1000 steady)])]
    (is (= :unknown (:valid? r)))
    (is (= :vacuous (:cause r)))))

(deftest an-ungated-run-still-fails-on-a-safety-violation
  ;; The scoping governs the vacuity gate and nothing else. A partition-only run
  ;; that shows two movers on one range is still broken, and a checker that
  ;; stopped looking because it had stopped demanding would be worse than one
  ;; that never gated at all.
  (let [two {1 (view 2 [["n1" :voter] ["n2" :voter] ["n3" :learner] ["n4" :learner]])}
        r   (check* (assoc base-test :faults #{:partition})
                    [(settle 0) (sample 0 steady) (sample 1000 two)])]
    (is (false? (:valid? r)))
    (is (contains? (set (map :type (:violations r))) :multiple-movers))))

(deftest a-run-that-started-moves-and-finished-none-is-stalled-not-vacuous
  ;; Learners appear and no voter set ever changes. That is not an idle
  ;; nemesis, it is moves that begin and never complete — the shape an RF 1
  ;; kill-heavy run produces, because a range with a single voter has no
  ;; surviving copy to seed a replacement from while that voter is down.
  ;; Reporting it as :vacuous would file "the planner is stuck" under "the
  ;; nemesis did nothing".
  ;; `moving` is the mid-move fixture: generation 2, the three original voters
  ;; plus n4 as a Learner. Ending the history there — rather than following it
  ;; with `moved` as the passing test does — is a move that began and never
  ;; finished. It must carry a *new* generation: reusing generation 1 with a
  ;; different replica set is a map disagreement, which fails the run before the
  ;; vacuity gate is ever consulted.
  ;; The learner is seen once, in the last sample, so it has not yet sat for
  ;; the stalled-move bound: this is the gate's answer, not the stall check's.
  (let [r (check* base-test [(settle 0) (sample 0 steady) (sample 1000 moving)])]
    (is (= :unknown (:valid? r)))
    (is (= :stalled (:cause r)))
    (is (= [:move] (:missing r)))
    (is (= 1 (get-in r [:transitional :learners])))))

(deftest a-run-with-no-transitional-replica-at-all-is-still-vacuous
  ;; The accepting twin for the distinction: nothing was ever attempted, so
  ;; :vacuous remains the right word and :stalled must not swallow it.
  (let [r (check* base-test [(settle 0) (sample 0 steady) (sample 1000 steady)])]
    (is (= :unknown (:valid? r)))
    (is (= :vacuous (:cause r)))))

;; ---------------------------------------------------------------------------
;; Stalled moves: a move that started on a healed cluster must finish
;; ---------------------------------------------------------------------------
;;
;; The vacuity gate proves one move finished. It cannot see a move that began
;; and never ended, which is exactly what the RF-1 election bug produced:
;; learners added and never promoted. These histories fix the settle window
;; (everything after `settle`) and the bound (1 s, from base-test) so the
;; question "did this learner sit for the bound with no fault in the way?" has
;; an answer by construction.

(deftest a-move-still-open-for-the-bound-after-the-heal-is-a-violation
  ;; n4 is a Learner on every settled sample for two seconds, twice the bound,
  ;; and is still one at the end. Nothing was in its way. That is a move the
  ;; planner started and never finished, and it fails the run outright --
  ;; even though a different range completing a move would have satisfied the
  ;; gate.
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady)
                             (sample 1000 moving)
                             (sample 2000 moving)
                             (sample 3000 moving)])
        v (first (filter #(= :stalled-move (:type %)) (:violations r)))]
    (is (false? (:valid? r)))
    (is (some? v))
    (is (= 1 (:partition v)))
    (is (= (ep "n4") (:endpoint v)))
    (is (= :learner (:role v)))
    (is (= 2.0 (:duration-secs v)))
    (is (= 1.0 (:bound-secs v)))
    (is (true? (get-in r [:settle :measured?])))))

(deftest the-same-move-that-finishes-inside-the-window-is-fine
  ;; The accepting twin: identical until the last sample, where n4 has been
  ;; promoted and n3 retired. The learner was seen at 1 s and 2 s and was gone
  ;; by 3 s, which is a slow move, not a stuck one. The longest span is still
  ;; reported so a run can show how close it came -- measured between the
  ;; first and last sighting (1 s here), never up to the sample that closed it.
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady)
                             (sample 1000 moving)
                             (sample 2000 moving)
                             (sample 3000 moved)])]
    (is (true? (:valid? r)))
    (is (= 1.0 (get-in r [:settle :longest-secs])))))

(deftest a-removing-replica-that-never-retires-is-stalled-too
  ;; The other transitional role. A replica promoted but never dropped is a
  ;; move whose last step never ran, and the planner refuses to start the next
  ;; move on that range while it is there -- so it blocks exactly like a stuck
  ;; learner does.
  (let [one {1 (view 2 [["n1" :voter] ["n2" :voter] ["n4" :voter] ["n3" :removing]])}
        r   (check* base-test [(settle 0)
                               (sample 0 steady)
                               (sample 1000 one)
                               (sample 2000 one)
                               (sample 3000 one)])
        v   (first (filter #(= :stalled-move (:type %)) (:violations r)))]
    (is (false? (:valid? r)))
    (is (= :removing (:role v)))
    (is (= (ep "n3") (:endpoint v)))))

(deftest time-before-the-heal-does-not-count-toward-the-bound
  ;; n4 was a Learner for three seconds before the heal -- three times the
  ;; bound -- and the move completes half a second into the settle window.
  ;; Mid-run that learner may have been waiting on a killed voter, which at RF
  ;; 1 is the only copy it can seed from, so the pre-heal time says nothing
  ;; about the planner. Only the settled span counts, and it is shorter than
  ;; the bound.
  (let [r (check* base-test [(sample 0 moving)
                             (sample 1000 moving)
                             (sample 2000 moving)
                             (settle 2500)
                             (sample 3000 moving)
                             (sample 3500 moved)
                             (sample 4000 moved)
                             (sample 4500 moved)])]
    (is (true? (:valid? r)))
    (is (true? (get-in r [:settle :measured?])))
    (is (= 4 (get-in r [:settle :samples])))))

(deftest a-lagging-node-does-not-keep-a-finished-move-open
  ;; In the last two samples n1 has applied the promotion (generation 3) while
  ;; n2 still reports generation 2 with n4 as a Learner. The committed map is
  ;; the newest view, so the move is closed when n1 says so; a checker that
  ;; read n2's stale table would call this move stalled for as long as one
  ;; node lagged.
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady)
                             (sample 1000 moving)
                             (sample 2000 moving)
                             (sample-views 3000 {"n1" moved "n2" moving})
                             (sample-views 4000 {"n1" moved "n2" moving})])]
    (is (true? (:valid? r)))))

(deftest a-sample-nobody-answered-keeps-the-span-open
  ;; The middle sample reached no node at all. That is not evidence the move
  ;; finished, so the span continues through it: n4 was a Learner at 1 s and
  ;; still at 3 s, which is the bound twice over. Closing the span on a blank
  ;; sample would reset the clock every time the control node lost a round
  ;; trip, and a stall would only ever be visible between two such gaps.
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady)
                             (sample 1000 moving)
                             (sample-views 2000 {})
                             (sample 3000 moving)])
        v (first (filter #(= :stalled-move (:type %)) (:violations r)))]
    (is (false? (:valid? r)))
    (is (= 2.0 (:duration-secs v)))))

(deftest without-a-settle-marker-the-stall-check-is-unmeasured
  ;; The samples show a clean completed move, but nothing says when the final
  ;; heal happened, so there is no settled span to measure a stall in. The
  ;; honest answer is :unmeasured with the check named -- an older history, or
  ;; one from a run whose sampler was switched off, must not read as "no move
  ;; stalled".
  (let [r (check* base-test [(sample 0 steady)
                             (sample 1000 moving)
                             (sample 2000 moved)])]
    (is (= :unknown (:valid? r)))
    (is (= :unmeasured (:cause r)))
    (is (= [:stalled-move] (:unmeasured r)))
    (is (false? (get-in r [:settle :measured?])))
    (is (nil? (get-in r [:settle :marker])))))

(deftest a-settle-window-shorter-than-the-bound-is-unmeasured
  ;; The marker is there but the settled samples span half a second against a
  ;; one-second bound: a learner could not have been seen sitting for the
  ;; bound even if it was. Reporting the window lets a reader see that the
  ;; run was cut short, rather than that the planner was fine.
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady)
                             (sample 500 moved)])]
    (is (= :unknown (:valid? r)))
    (is (= :unmeasured (:cause r)))
    (is (= [:stalled-move] (:unmeasured r)))
    (is (= 0.5 (get-in r [:settle :window-secs])))
    (is (= 1.0 (get-in r [:settle :bound-secs])))))

(deftest the-bound-follows-the-placement-pass-interval
  ;; Ten passes at the server's 5 s default is 50 s; a profile that slows the
  ;; pass to 10 s moves the bound to 100 s with it, and a pass interval of 0
  ;; (timer off, commit-driven passes only) falls back to the default rather
  ;; than to a bound of zero that would flag every move in flight.
  (is (= 50.0 (placement/stalled-move-bound {})))
  (is (= 100.0 (placement/stalled-move-bound {:placement-pass-interval 10000})))
  (is (= 50.0 (placement/stalled-move-bound {:placement-pass-interval 0})))
  (is (= 2.0 (placement/stalled-move-bound {:placement-pass-interval 1000
                                             :stalled-move-passes 2}))))

;; ---------------------------------------------------------------------------
;; Narrowing the gate for a profile that cannot produce every kind
;; ---------------------------------------------------------------------------
;;
;; `--require-placement-evidence` lets a job ask for less than the default
;; set. The RF-1 job used it to drop `seeding` and `purge`, and for a month to
;; drop `move` for the weaker `transfer`, while a Kommander election bug kept
;; learners from ever being promoted. `move` is back there now; `transfer`
;; stays available for the next profile that needs it.
;;
;; Narrowing a vacuity gate is the move this suite has been burned by before, so
;; the two properties worth pinning are both about it *not* becoming a way to
;; buy a green.

(deftest the-configured-evidence-set-is-honoured-exactly
  ;; A narrowed requirement must neither reinstate a kind that was dropped nor
  ;; quietly drop one that was asked for. If `:seeding` ever creeps back into
  ;; `:evidence-required` for a job that excluded it, that job goes red for a
  ;; reason its configuration says it should not.
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady)
                             (sample 1000 moving)
                             (sample 2000 moved)])]
    (is (= [:move] (:evidence-required r)))
    (is (not (contains? (set (:evidence-required r)) :seeding)))
    (is (true? (:valid? r)))))

(deftest naming-an-unmeasurable-kind-is-unknown-not-a-pass
  ;; The abuse this must not permit: narrowing the set to kinds the harness
  ;; cannot read would turn every run green while proving nothing. `:purge` is
  ;; log-derived and there are no logs here, so the honest answer is :unknown
  ;; with the kind named -- :unmeasured invites fixing the observability, where
  ;; a pass would invite nothing at all.
  (let [r (check* (assoc base-test :require-placement-evidence #{:move :purge})
                  [(settle 0)
                   (sample 0 steady)
                   (sample 1000 moving)
                   (sample 2000 moved)])]
    (is (= :unknown (:valid? r)))
    (is (= :unmeasured (:cause r)))
    (is (= [:purge] (:unmeasured r)))))

(deftest requiring-transfer-accepts-a-move-that-only-started
  ;; The weakest requirement in the set. This is the exact history that `move`
  ;; refuses -- a Learner appeared and the voter set never changed -- so the
  ;; two requirements must disagree about it, or `transfer` is not doing
  ;; anything `move` was not already doing.
  (let [r (check* (assoc base-test :require-placement-evidence #{:transfer})
                  [(settle 0) (sample 0 steady) (sample 1000 moving)])]
    (is (true? (:valid? r)))
    (is (= [:transfer] (:evidence-required r)))
    (is (= 1 (get-in r [:evidence :transfer :count])))
    (is (= 0 (get-in r [:evidence :move :count])))))

(deftest requiring-transfer-does-not-excuse-a-stalled-move
  ;; `transfer` narrows what the *gate* asks for. The stall check is a safety
  ;; property and is not narrowed with it: a job that only has to show a move
  ;; started still fails when that move sits unfinished for the bound on a
  ;; healed cluster. Otherwise `transfer` would be the one setting under which
  ;; the RF-1 stall could never be seen -- the setting that job was running.
  (let [r (check* (assoc base-test :require-placement-evidence #{:transfer})
                  [(settle 0)
                   (sample 0 steady)
                   (sample 1000 moving)
                   (sample 2000 moving)
                   (sample 3000 moving)])]
    (is (false? (:valid? r)))
    (is (contains? (set (map :type (:violations r))) :stalled-move))))

(deftest requiring-transfer-still-refuses-a-run-that-attempted-nothing
  ;; The accepting twin's refusal, and the property that makes the narrowing
  ;; safe to ship: `transfer` must not be a way to buy a green from an idle
  ;; nemesis. No transitional replica was ever sampled here, so the gate has to
  ;; hold even at its weakest setting.
  (let [r (check* (assoc base-test :require-placement-evidence #{:transfer})
                  [(settle 0) (sample 0 steady) (sample 1000 steady)])]
    (is (= :unknown (:valid? r)))
    (is (= :vacuous (:cause r)))
    (is (= [:transfer] (:missing r)))))

(deftest transfer-is-unmeasurable-without-samples
  ;; Same distinction `:move` draws, and it matters more here: `transfer` is
  ;; read from the samples alone, so an absent sampler must report :unmeasured
  ;; rather than let a zero stand for "the planner attempted nothing".
  (let [r (check* (assoc base-test :require-placement-evidence #{:transfer}) [])]
    (is (= :unknown (:valid? r)))
    (is (= :unmeasured (:cause r)))
    (is (= [:transfer :stalled-move] (:unmeasured r)))))

;; ---------------------------------------------------------------------------
;; A node outside the roster has no view
;; ---------------------------------------------------------------------------

(defn stale
  "A node's table as a node *outside the roster* reports it. The sampler stamps
  every table with the node's own roster role, and `NotMember` is the one that
  marks a table nobody updates any more."
  [node partitions]
  (assoc (placement node partitions) :roster-role "NotMember" :roster-version 6))

(defn sample-tables
  "A placement sample op at `ms` built from whole tables: `tables` is
  {node table-or-nil}. Like `sample-views`, but the caller builds each table,
  so it can carry a roster stamp."
  [ms tables]
  [{:type :info :process :nemesis :f :placement-sample
    :time (* 1000000 ms) :value nil}
   {:type :info :process :nemesis :f :placement-sample
    :time (* 1000000 (inc ms))
    :value (into (sorted-map) (map (fn [n] [n (get tables n)])) nodes)}])

;; Partition 1 after a merge took it off the map: a live node lists nothing.
(def gone {})

(defn- after-the-merge
  "A settled sample in which the three live nodes list no partition 1 and n4
  still shows the Learner at generation 2. `stamp` builds n4's table."
  [stamp ms]
  (sample-tables ms {"n1" (placement "n1" gone)
                     "n2" (placement "n2" gone)
                     "n3" (placement "n3" gone)
                     "n4" (stamp "n4" moving)}))

(deftest a-node-outside-the-roster-cannot-keep-a-move-open
  ;; The 2026-10-30 nightly. n4 left the roster between the add and the
  ;; promotion, so its table still shows the Learner at generation 2. The live
  ;; nodes promoted the Learner, retired it, and then merged the partition
  ;; away, so they list no partition 1 at all. The highest-generation rule
  ;; alone would make n4's frozen table the freshest view -- it is the only one
  ;; -- and call a finished move stalled for the whole window.
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady)
                             (sample 500 moved)
                             (after-the-merge stale 1000)
                             (after-the-merge stale 2000)
                             (after-the-merge stale 3000)])]
    (is (true? (:valid? r)))
    (testing "the verdict says how often a table was set aside"
      (is (= 3 (:non-member-views r))))))

(deftest the-same-frozen-table-from-a-member-is-still-a-stall
  ;; The twin. With no stamp, n4 is a member as far as the checker can tell,
  ;; and a member that still lists a partition nobody else has is behind the
  ;; committed map by the whole window. That is the lag the stall check
  ;; measures, and it must keep measuring it -- a filter that dropped every
  ;; lone view would blind the check to a planner that stops short.
  (let [r (check* base-test [(settle 0)
                             (sample 0 steady)
                             (sample 500 moved)
                             (after-the-merge placement 1000)
                             (after-the-merge placement 2000)
                             (after-the-merge placement 3000)])
        v (first (filter #(= :stalled-move (:type %)) (:violations r)))]
    (is (false? (:valid? r)))
    (is (= "n4:8082" (:endpoint v)))
    (is (= 0 (:non-member-views r)))))

(deftest only-a-not-member-stamp-sets-a-table-aside
  ;; An older history has no stamp; a node that answered the placement read but
  ;; not the membership read has none either. Both must read as members: the
  ;; stamp proves a node out, and its absence proves nothing.
  (is (placement/member-view? {}))
  (is (placement/member-view? {:roster-role "Voter"}))
  (is (placement/member-view? {:roster-role "Learner"}))
  (is (placement/member-view? {:roster-role "Leaving"}))
  (is (not (placement/member-view? {:roster-role "NotMember"}))))
