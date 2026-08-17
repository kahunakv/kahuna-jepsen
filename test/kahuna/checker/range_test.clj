(ns kahuna.checker.range-test
  "Controls for the key-range checker.

  Two behaviours are load-bearing and neither would be noticed if it broke.

  The first is the **overlap**. A gap is loud — a key routes nowhere and the
  workload sees it immediately — but an overlap looks exactly like the database
  working, right up until two clients disagree about a value they both wrote
  successfully. Every coverage test here therefore pins which of the two was
  reported, not merely that something was.

  The second is the **refusal**: a run whose key space never left one descriptor
  split nothing, and must come back `:unknown` rather than `true`. This suite has
  twice shipped weeks of runs that proved nothing while reporting health.

  Every rejecting case is paired with an accepting one, so a checker that simply
  failed everything could not get through this file. The pairs are the point."
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [kahuna.checker.range :as range-checker]))

(def nodes ["n1" "n2" "n3"])

(def space "jepsen/register")

(def base-test
  "A three-node test with the register workload's key space range-routed."
  {:nodes nodes
   :key-range true
   :key-space space
   :require-range-evidence #{:split}})

(defn d
  "One range descriptor. nil bounds are ∓infinity within the key space."
  [start end pid gen]
  {:start start :end end :partition pid :generation gen})

(defn view
  "One node's range map, reporting `descriptors` for the workload's key space."
  ([descriptors] (view descriptors "KeyRange"))
  ([descriptors mode]
   {:initialized true
    :endpoint    "n?:8082"
    :key-spaces  {space {:routing-mode mode :descriptors descriptors}}}))

(defn sample
  "A range-sample op at `ms`, where each node reports its own view.

  The sampler's ops are :type :info both on invocation and on completion, so the
  invocation carries no value — included here because the checker has to filter
  it out, and one that counted both would double every observation."
  [ms node->view]
  [{:type :info :process :nemesis :f :range-sample
    :time (* 1000000 ms) :value nil}
   {:type :info :process :nemesis :f :range-sample
    :time (* 1000000 (inc ms))
    :value (into (sorted-map) node->view)}])

(defn uniform
  "A sample in which every node reports the same view — the ordinary case, and
  the one every coverage test wants, since coverage is a property of a single
  view."
  [ms v]
  (sample ms (zipmap nodes (repeat v))))

(defn split-op [ms value]
  [{:type :info :process :nemesis :f :split-range :time (* 1000000 ms) :value nil}
   {:type :info :process :nemesis :f :split-range :time (* 1000000 (inc ms)) :value value}])

(defn merge-op [ms value]
  [{:type :info :process :nemesis :f :merge-ranges :time (* 1000000 ms) :value nil}
   {:type :info :process :nemesis :f :merge-ranges :time (* 1000000 (inc ms)) :value value}])

(def acked-split
  {:attempted true :success true :determinate true
   :outcome "Succeeded" :new-partition 3 :new-generation 2})

(def refused-split
  {:attempted true :success false :determinate true :outcome "BelowMinRangeSize"})

(def indeterminate-split
  {:attempted true :success false :determinate false :outcome "CutoverFailed"})

(defn check*
  ([parts] (check* base-test parts))
  ([t parts] (checker/check (range-checker/checker) t (vec (apply concat parts)) {})))

(defn types
  "The violation types a result reported, as a set."
  [r]
  (set (map :type (:violations r))))

;; Layouts used throughout. `whole` is the seeded state; `split` is the same
;; space after one clean cutover.
(def whole [(d nil nil 1 1)])
(def split [(d nil "jepsen/register/5" 1 2)
            (d "jepsen/register/5" nil 3 2)])

;; ---------------------------------------------------------------------------
;; Coverage: the descriptors must tile the key space exactly once
;; ---------------------------------------------------------------------------

(deftest a-clean-split-tiles-the-key-space
  ;; The accepting twin for every coverage test below: two adjacent half-open
  ;; ranges meeting at exactly one key, reaching ∓infinity at the ends.
  (let [r (check* [(uniform 0 (view whole))
                   (uniform 10 (view split))])]
    (is (true? (:valid? r)))
    (is (empty? (:violations r)))
    (is (= 2 (:max-descriptors r)))
    (is (= [1 3] (:partitions r)))))

(deftest a-gap-between-descriptors-is-caught
  ;; Keys in ["…/4", "…/5") route nowhere.
  (let [r (check* [(uniform 0 (view [(d nil "jepsen/register/4" 1 2)
                                     (d "jepsen/register/5" nil 3 2)]))])]
    (is (false? (:valid? r)))
    (is (= #{:gap} (types r)))))

(deftest an-overlap-between-descriptors-is-caught
  ;; The dangerous one. Keys in ["…/5", "…/6") are claimed by two partitions,
  ;; and two partitions each holding a version of a key is a lost update
  ;; wearing a successful write's clothes.
  (let [r (check* [(uniform 0 (view [(d nil "jepsen/register/6" 1 2)
                                     (d "jepsen/register/5" nil 3 2)]))])]
    (is (false? (:valid? r)))
    (is (= #{:overlap} (types r)))))

(deftest a-non-final-descriptor-claiming-infinity-is-an-overlap
  ;; A nil end is legitimate only on the last descriptor. In the middle it
  ;; swallows every range after it — reported as the overlap it is, and not as
  ;; a gap, because the two failure modes want opposite fixes.
  (let [r (check* [(uniform 0 (view [(d nil nil 1 2)
                                     (d "jepsen/register/5" nil 3 2)]))])]
    (is (false? (:valid? r)))
    (is (= #{:overlap} (types r)))))

(deftest a-set-that-does-not-reach-the-ends-is-caught
  (let [below (check* [(uniform 0 (view [(d "jepsen/register/1" nil 1 2)]))])
        above (check* [(uniform 0 (view [(d nil "jepsen/register/9" 1 2)]))])]
    (is (false? (:valid? below)))
    (is (= #{:uncovered-below} (types below)))
    (is (false? (:valid? above)))
    (is (= #{:uncovered-above} (types above)))))

(deftest a-descriptor-that-can-hold-no-key-is-caught
  ;; start >= end. Routing will never select it, so whatever was meant to live
  ;; there lives nowhere.
  (let [r (check* [(uniform 0 (view [(d "jepsen/register/b" "jepsen/register/a" 1 2)]))])]
    (is (false? (:valid? r)))
    (is (contains? (types r) :empty-range))))

(deftest bounds-are-compared-ordinally-not-numerically
  ;; "…/10" sorts before "…/2" ordinally, which is the comparison the server's
  ;; router uses. A checker that ordered these any other way — by length, or by
  ;; the trailing integer — would read two gaps in a set that tiles perfectly.
  (let [r (check* [(uniform 0 (view [(d nil "jepsen/register/10" 1 2)
                                     (d "jepsen/register/10" "jepsen/register/2" 3 2)
                                     (d "jepsen/register/2" nil 4 2)]))
                   (split-op 5 acked-split)])]
    (is (true? (:valid? r)))
    (is (empty? (:violations r)))))

;; ---------------------------------------------------------------------------
;; Routing mode has to follow the descriptors
;; ---------------------------------------------------------------------------

(deftest a-node-hashing-a-space-it-holds-descriptors-for-is-caught
  ;; Kahuna reconciles each node's routing registry against the range map inside
  ;; the same call that applies a descriptor, so this state is not reachable by
  ;; lag. A node in it hash-routes a space the rest of the cluster range-routes,
  ;; which serves one key from two partitions.
  (let [r (check* [(sample 0 {"n1" (view split)
                              "n2" (view split)
                              "n3" (view split "Hash")})])]
    (is (false? (:valid? r)))
    (is (= #{:routing-mode-mismatch} (types r)))
    (is (= "n3" (:node (first (:violations r)))))))

(deftest a-node-with-no-descriptors-yet-says-nothing
  ;; The accepting twin, and the false positive this checker most had to avoid.
  ;; The placement nemesis wipes and rejoins nodes; a node that has not applied
  ;; the meta partition honestly reports the space as hash-routed and empty.
  ;; Failing on that would be reporting the harness's own teardown as a Kahuna
  ;; bug — and it would fire on every single run that churns the roster.
  (let [r (check* [(sample 0 {"n1" (view split)
                              "n2" (view split)
                              "n3" (view [] "Hash")})
                   (split-op 5 acked-split)])]
    (is (true? (:valid? r)))
    (is (empty? (:violations r)))))

;; ---------------------------------------------------------------------------
;; Cross-node agreement
;; ---------------------------------------------------------------------------

(deftest two-nodes-describing-one-descriptor-differently-is-caught
  ;; Descriptors are replicated, so one (key space, partition, generation) has
  ;; exactly one set of bounds. Two answers means every routing decision made
  ;; from the map was made against a map somebody else did not have.
  (let [r (check* [(sample 0 {"n1" (view [(d nil "jepsen/register/5" 1 2)
                                          (d "jepsen/register/5" nil 3 2)])
                              "n2" (view [(d nil "jepsen/register/7" 1 2)
                                          (d "jepsen/register/7" nil 3 2)])
                              "n3" (view whole)})])]
    (is (false? (:valid? r)))
    ;; Both descriptors of the split disagree — n1 cut at "…/5" and n2 at "…/7",
    ;; so partition 1's upper bound and partition 3's lower bound are each
    ;; reported two ways. Every disagreeing descriptor is named rather than only
    ;; the first, because "which partition" is the first thing a reader needs.
    (is (= #{:descriptor-disagreement} (set (map :type (:disagreements r)))))
    (is (= #{1 3} (set (map :partition (:disagreements r)))))
    (is (= 2 (:generation (first (:disagreements r)))))))

(deftest one-partition-holding-two-ranges-is-not-a-disagreement-with-itself
  ;; A generation is `descriptor.Generation + 1` on the descriptor being changed,
  ;; not a counter over the whole map, so nothing promises that one (key space,
  ;; partition, generation) names exactly one descriptor. Here P1 holds two
  ;; disjoint ranges at generation 2 and every node says so. Comparing bounds
  ;; without grouping by node first would report that as a node disagreeing with
  ;; itself — a violation invented out of the checker's own bookkeeping.
  (let [ds [(d nil "jepsen/register/3" 1 2)
            (d "jepsen/register/3" "jepsen/register/6" 3 2)
            (d "jepsen/register/6" nil 1 2)]
        r  (check* [(uniform 0 (view ds))
                    (split-op 5 acked-split)])]
    (is (true? (:valid? r)))
    (is (empty? (:disagreements r)))
    (is (empty? (:violations r)))))

(deftest a-lagging-node-is-not-a-disagreement
  ;; The accepting twin. n3 has not applied the cutover yet, so it answers with
  ;; the *previous* generation — an older complete map, not a conflicting one.
  ;; A checker that compared views rather than generations would fail this, and
  ;; it would fail it on nearly every real run.
  (let [r (check* [(sample 0 {"n1" (view split)
                              "n2" (view split)
                              "n3" (view whole)})
                   (split-op 5 acked-split)])]
    (is (true? (:valid? r)))
    (is (empty? (:disagreements r)))))

;; ---------------------------------------------------------------------------
;; Destination partition ids are allocated, never recycled
;; ---------------------------------------------------------------------------

(defn- split-to [pid]
  {:attempted true :success true :determinate true
   :outcome "Succeeded" :new-partition pid :new-generation 2})

(deftest a-destination-partition-id-handed-out-twice-is-caught
  ;; The regression this guards: the next id used to be derived from the range
  ;; map, which forgets retired partitions, while Kommander keeps them. After a
  ;; merge the id looked free — and on a cluster with several initial partitions
  ;; the same arithmetic could land inside the hash pool, where
  ;; CreatePartitionAsync answers idempotent success and ranged data silently
  ;; joins hash-routed data on one partition. Two acknowledged splits naming one
  ;; id is the only unambiguous evidence of it.
  (let [r (check* [(uniform 0 (view whole))
                   (split-op 3 (split-to 9))
                   (merge-op 5 {:attempted true :success true :determinate true
                                :outcome "Completed" :merges 1})
                   (split-op 7 (split-to 9))])]
    (is (false? (:valid? r)))
    (is (= #{:partition-id-reused} (types r)))
    (is (= 9 (:partition (first (:violations r)))))
    (is (= 2 (:splits (first (:violations r)))))))

(deftest successive-splits-onto-fresh-ids-are-fine
  ;; The accepting twin, and the ordinary case: every split gets a new id, and
  ;; a merge in between does not free one for reuse. A check that fired here
  ;; would fail every key-range run that splits more than once.
  (let [r (check* [(uniform 0 (view whole))
                   (split-op 3 (split-to 9))
                   (merge-op 5 {:attempted true :success true :determinate true
                                :outcome "Completed" :merges 1})
                   (split-op 7 (split-to 10))
                   (split-op 9 (split-to 11))])]
    (is (true? (:valid? r)))
    (is (empty? (:violations r)))
    (is (= [9 10 11] (get-in r [:splits :destinations])))))

(deftest a-failed-split-does-not-reserve-an-id
  ;; Only *successful* splits name a destination. A refusal reports 0, and
  ;; counting those would report every run with two refusals as reusing
  ;; partition 0 — a violation manufactured out of an absent field.
  (let [r (check* [(uniform 0 (view whole))
                   (split-op 3 (split-to 9))
                   (split-op 5 (assoc refused-split :new-partition 0))
                   (split-op 7 (assoc refused-split :new-partition 0))])]
    (is (true? (:valid? r)))
    (is (empty? (:violations r)))
    (is (= [9] (get-in r [:splits :destinations])))))

(deftest creation-failures-are-reported-apart-from-other-refusals
  ;; Not a violation on its own — leadership can move between the gate and the
  ;; call — but it is what a reused id looks like to a caller, so a run full of
  ;; them is worth seeing without digging through :outcomes.
  (let [r (check* [(uniform 0 (view whole))
                   (split-op 3 (split-to 9))
                   (split-op 5 {:attempted true :success false :determinate true
                                :outcome "PartitionCreationFailed"})])]
    (is (true? (:valid? r)))
    (is (= 1 (get-in r [:splits :creation-failed])))
    (is (= 1 (get-in r [:splits :refused])))))

;; ---------------------------------------------------------------------------
;; The refusal: a run that split nothing is not a pass
;; ---------------------------------------------------------------------------

(deftest a-run-whose-key-space-never-split-is-unknown
  (let [r (check* [(uniform 0 (view whole))
                   (uniform 10 (view whole))
                   (split-op 5 refused-split)])]
    (is (= :unknown (:valid? r)))
    (is (= :vacuous (:cause r)))
    (is (= [:split] (:missing r)))
    (is (= 1 (:max-descriptors r)))))

(deftest an-acknowledged-split-is-enough-on-its-own
  ;; The sampler can miss the two-descriptor state entirely — a merge pass can
  ;; fold the range back between two samples — so a cutover the server confirmed
  ;; is evidence in its own right. Note the samples here never show a split.
  (let [r (check* [(uniform 0 (view whole))
                   (split-op 5 acked-split)])]
    (is (true? (:valid? r)))
    (is (= [:split] (:shown r)))
    (is (= 1 (get-in r [:evidence :split :count])))))

(deftest a-sampled-split-is-enough-on-its-own
  ;; And the other direction: the auto-splitter can move a boundary with no
  ;; nemesis operation behind it at all. Requiring an acked op would report that
  ;; run as vacuous while the map was demonstrably carved in two.
  (let [r (check* [(uniform 0 (view whole))
                   (uniform 10 (view split))])]
    (is (true? (:valid? r)))
    (is (true? (get-in r [:evidence :split :observed?])))
    (is (zero? (get-in r [:evidence :split :count])))))

(deftest a-run-with-nothing-to-look-at-is-unmeasured
  ;; No samples and no operations. "The machinery did not run" and "nobody
  ;; looked" license completely different conclusions, and this must not report
  ;; the second as the first.
  (let [r (check* [])]
    (is (= :unknown (:valid? r)))
    (is (= :unmeasured (:cause r)))
    (is (= [:split] (:unmeasured r)))))

(deftest an-indeterminate-split-is-not-counted-as-a-refusal
  ;; A split that failed after the transaction was under way may still have
  ;; changed the map. Filing it as a refusal would let a report say the run did
  ;; nothing while the boundaries moved underneath it.
  (let [r (check* [(uniform 0 (view whole))
                   (split-op 5 indeterminate-split)
                   (split-op 7 refused-split)])]
    (is (= :unknown (:valid? r)))
    (is (= 1 (get-in r [:splits :indeterminate])))
    (is (= 1 (get-in r [:splits :refused])))
    (is (zero? (get-in r [:splits :succeeded])))))

(deftest skipped-operations-are-reported-not-counted-as-attempts
  ;; The nemesis skips when a range holds too few keys to bisect. That is a fact
  ;; about the run, not an attempt, and burying it would make a nemesis that
  ;; never managed to aim look like one whose splits were all refused.
  (let [r (check* [(uniform 0 (view whole))
                   (split-op 5 {:skipped :too-few-keys :scanned 1})])]
    (is (= :unknown (:valid? r)))
    (is (zero? (get-in r [:splits :attempted])))
    (is (= {:too-few-keys 1} (get-in r [:splits :skipped])))))

;; ---------------------------------------------------------------------------
;; Merges
;; ---------------------------------------------------------------------------

(deftest a-leader-that-merged-nothing-is-not-evidence-of-a-merge
  ;; The merge pass succeeds with 0 when nothing is eligible. Counting the
  ;; success rather than the merges would let a run demanding merge evidence
  ;; pass having folded nothing.
  (let [t (assoc base-test :require-range-evidence #{:split :merge})
        r (check* t [(uniform 0 (view whole))
                     (split-op 3 acked-split)
                     (merge-op 5 {:attempted true :success true :determinate true
                                  :outcome "Completed" :merges 0})])]
    (is (= :unknown (:valid? r)))
    (is (= [:merge] (:missing r)))
    (is (= [:split] (:shown r)))))

(deftest a-pass-that-folded-a-pair-shows-a-merge
  (let [t (assoc base-test :require-range-evidence #{:split :merge})
        r (check* t [(uniform 0 (view whole))
                     (split-op 3 acked-split)
                     (merge-op 5 {:attempted true :success true :determinate true
                                  :outcome "Completed" :merges 1})])]
    (is (true? (:valid? r)))
    (is (= 1 (get-in r [:merges :merges])))))

;; ---------------------------------------------------------------------------
;; Off by default
;; ---------------------------------------------------------------------------

(deftest without-key-range-the-checker-makes-no-claim
  ;; A hash-routed run has no descriptors, so every property above is vacuously
  ;; true of a map that does not exist. It must not be reported as though the
  ;; run demonstrated something.
  (let [r (check* (dissoc base-test :key-range) [])]
    (is (true? (:valid? r)))
    (is (false? (:key-range r)))
    (is (nil? (:evidence r)))))
