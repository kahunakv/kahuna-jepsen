(ns kahuna.checker.placement
  "Checks per-partition replica placement, and refuses to call a run clean when
  it never exercised placement at all.

  ## The vacuous pass is the thing to be afraid of here

  Every other checker in this suite can fail. This one's first job is to stop a
  *pass* from being meaningless. Placement's interesting code — snapshot seeding
  of a new replica, the un-host purge of a lost one, the promotion of a Learner
  to Voter — runs only when the map actually changes. A run whose planner never
  moved anything is not evidence that moving things is safe; it is evidence that
  nothing was tested. The suite has already been burned by exactly this shape
  twice (a membership nemesis that silently removed nobody, and a whole day of
  nightlies whose reads returned nothing and were certified valid), so the
  verdict here is `:unknown` unless the run can show its work.

  What counts as showing its work is `--require-placement-evidence`, and each
  kind is read from a different place:

  | evidence | where it comes from |
  |---|---|
  | `move` | the sampled placement tables: a partition's committed replica set changed |
  | `seeding` | `Imported whole-partition state of partition #N` in a node log |
  | `purge` | `Stopped hosting N partition(s)` in a node log |
  | `split` | `RangeSplitTrigger: split` in a node log |

  Node logs are downloaded before the checker runs (jepsen captures them in step
  6 and checks in step 9), so reading them here is sound. When they are missing
  the evidence is reported as *unmeasured* rather than absent — a checker that
  could not look must not answer.

  `split` is deliberately **not** in the default requirement, and the reason has
  changed. It used to be that no run this harness could launch was *able* to
  split a range: the thresholds had no command-line flag and the force-split
  primitive was an internal test seam. Both are now reachable, so a split is
  producible — but it is producible only for a key space that has been
  registered for key-range routing, which is `--key-range`, not a property of
  running under a replication factor. Requiring it here would turn every ordinary
  RF run `:unknown` for want of something it never asked for.

  Splits are gated instead by `kahuna.checker.range`, which measures them from
  the *range map* rather than from a log line — it can tell a split that
  committed from one that was attempted, and it can say which partitions the
  space ended up on. `split` stays available here as a log-side cross-check for
  anyone who wants both.

  ## The safety properties, and the ones deliberately not checked

  Checked, because Kommander states them as invariants:

  * **Single mover per range.** `ReplicaPlacementService` refuses to add or
    remove a replica while the range already has a transitional one, so
    successive committed configurations always overlap by a quorum. Two Learners
    (or a Learner and a Removing) on one range at one instant means that
    discipline broke, and the overlap guarantee with it.
  * **The committed map is the same everywhere.** Placement is committed on the
    meta partition and every node answers from that commit, so two nodes
    reporting one partition at the same `generation` must report an identical
    replica set. Views may lag — that is routing's problem and it fails closed to
    MustRetry — but they may not *differ at the same generation*. Only the
    replica set is compared, never the effective factor: `TrySetReplicationFactor`
    bumps the map version and deliberately leaves `Generation` alone, because
    an override changes a target and moves nothing, so two nodes may honestly
    report different factors at one generation.
  * **A placed range always has at least one voter.** A replica set with no
    voter can commit nothing; the range is offline and no quorum can restore it.
  * **Replicas live on nodes the test knows about.** An endpoint belonging to no
    node in the roster is a map that has outlived its cluster.

  Not checked, on purpose:

  * **An upper bound on voters.** The obvious rule — voters ≤ effective RF + 1,
    allowing for one promoted-but-not-yet-retired replica — is wrong the moment
    a per-range override *lowers* the target: the range is legitimately
    over-replicated until the planner trims it, which is precisely what
    `:set-rf` asks for. A check that fires on the nemesis's own intended effect
    is a bug in the checker.
  * **Hosting matching the committed map.** A node keeps serving a partition for
    a moment after the map drops it, and purges afterwards; that gap is the
    design, not a violation. It is reported as a count so a purge that never
    finishes is visible, and it never fails a run."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :refer [warn]]
            [jepsen [checker :as checker]
                    [history :as h]
                    [store :as store]]
            [kahuna.client :as kc]
            [kahuna.db :as kdb]))

;; ---------------------------------------------------------------------------
;; Reading samples out of the history
;; ---------------------------------------------------------------------------

(defn samples
  "The placement samples in `history`, oldest first, as
  `{:time nanos :views {node placement-or-nil}}`.

  A sampler op appears twice — invocation and completion both carry `:type
  :info` — and only the completion has a value, so filtering on a map value
  keeps exactly one of each pair."
  [history]
  (->> (h/history history)
       (h/remove h/client-op?)
       (filter #(and (= :placement-sample (:f %)) (map? (:value %))))
       (mapv (fn [op] {:time (:time op) :views (:value op)}))))

(defn- replica-set
  "A partition view's replica set in a comparable form: endpoints paired with
  roles, ordered, so two views can be compared without caring about wire order."
  [view]
  (->> (:replicas view)
       (map (juxt :endpoint :role))
       (sort-by first)
       vec))

(defn- voter-set
  "Just the voting endpoints of a view, sorted. This is what 'where does this
  range live' means for movement accounting: a Learner is not yet part of the
  range's quorum and a Removing replica is already out of it, so counting either
  as a member would report a move twice — once when it appeared and again when
  its role changed."
  [view]
  (->> (:replicas view)
       (filter #(= :voter (:role %)))
       (map :endpoint)
       sort
       vec))

;; ---------------------------------------------------------------------------
;; Safety
;; ---------------------------------------------------------------------------

(defn violations
  "Every safety violation visible in `samples`, as a vector of maps. Empty is
  the expected result; the namespace docstring says which properties these are
  and why the tempting fifth one is absent."
  [test samples]
  (let [known (set (map kdb/endpoint (:nodes test)))]
    (persistent!
      (reduce
        (fn [acc {:keys [time views]}]
          (reduce
            (fn [acc [node placement]]
              (reduce
                (fn [acc [pid view]]
                  (let [replicas (:replicas view)
                        trans    (filter #(kc/transitional-roles (:role %)) replicas)
                        voters   (filter #(= :voter (:role %)) replicas)
                        unknown  (remove #(known (:endpoint %)) replicas)]
                    (cond-> acc
                      (< 1 (count trans))
                      (conj! {:type :multiple-movers :time time :node node
                              :partition pid :generation (:generation view)
                              :replicas (replica-set view)})

                      ;; Only for a *placed* range: an empty replica set is
                      ;; legacy full replication, where every voter hosts it and
                      ;; there is nothing here to be missing.
                      (and (seq replicas) (empty? voters))
                      (conj! {:type :no-voters :time time :node node
                              :partition pid :generation (:generation view)
                              :replicas (replica-set view)})

                      (seq unknown)
                      (conj! {:type :unknown-endpoint :time time :node node
                              :partition pid :generation (:generation view)
                              :endpoints (mapv :endpoint unknown)}))))
                acc
                (:partitions placement)))
            acc
            (filter (comp map? val) views)))
        (transient [])
        samples))))

(defn disagreements
  "Partitions where two nodes described the same generation differently.

  Built by folding every (partition, generation) pair seen anywhere in the run
  into the set of replica sets reported for it. More than one set means the
  committed map was not, in fact, common — which would undermine every routing
  decision made from it."
  [samples]
  (let [seen (reduce
               (fn [acc {:keys [views]}]
                 (reduce
                   (fn [acc [node placement]]
                     (reduce
                       (fn [acc [pid view]]
                         (update-in acc [[pid (:generation view)] (replica-set view)]
                                    (fnil conj #{}) node))
                       acc
                       (:partitions placement)))
                   acc
                   (filter (comp map? val) views)))
               {}
               samples)]
    (->> seen
         (keep (fn [[[pid gen] by-set]]
                 (when (< 1 (count by-set))
                   {:type :map-disagreement
                    :partition pid
                    :generation gen
                    :views (into {} (map (fn [[rs nodes]] [(sort nodes) rs]) by-set))})))
         vec)))

;; ---------------------------------------------------------------------------
;; Movement
;; ---------------------------------------------------------------------------

(defn movement
  "What the placement table did over the run.

  For each partition, its committed voter sets **ordered by generation**;
  consecutive pairs give the endpoints gained and lost.

  Ordering by generation rather than by when a sample arrived is the whole
  correctness of this function. Views lag: a node that has not yet applied the
  newest map answers with an older generation, so a node-ordered walk would see
  the map appear to move forward and then back, and would report two spurious
  moves for every one real one. Generations bump on every placement change and
  never go backwards, so they are the map's own order — and a generation
  reported with two different replica sets is caught by `disagreements`, not
  papered over here.

  `:transitions` is what makes this a real observation rather than a summary —
  it names the partition, the generation, and the endpoints that came and went,
  so a run's history can be read against the server logs line by line."
  [samples]
  (let [;; partition -> {generation -> voter-set}, first observation wins
        seqs (reduce
               (fn [acc {:keys [views]}]
                 (reduce
                   (fn [acc [_node placement]]
                     (reduce
                       (fn [acc [pid view]]
                         (let [gen (:generation view)]
                           (if (get-in acc [pid gen])
                             acc
                             (assoc-in acc [pid gen] (voter-set view)))))
                       acc
                       (:partitions placement)))
                   acc
                   (filter (comp map? val) views)))
               (sorted-map)
               samples)
        seqs (into (sorted-map)
                   (map (fn [[pid by-gen]] [pid (vec (sort-by key by-gen))]))
                   seqs)
        transitions
        (vec (mapcat
               (fn [[pid steps]]
                 (keep (fn [[[_g0 a] [g1 b]]]
                         (let [gained (vec (remove (set a) b))
                               lost   (vec (remove (set b) a))]
                           (when (or (seq gained) (seq lost))
                             {:partition pid :generation g1
                              :gained gained :lost lost})))
                       (partition 2 1 steps)))
               seqs))]
    {:transitions      transitions
     :replicas-gained  (reduce + 0 (map (comp count :gained) transitions))
     :replicas-lost    (reduce + 0 (map (comp count :lost) transitions))
     :partitions-moved (count (distinct (map :partition transitions)))}))

(defn roles-observed
  "How many distinct (partition, endpoint) pairs were ever seen in each
  transitional role.

  Seeing a Learner at all is the sharpest cheap signal that a move really
  happened: a learner exists only between the add and the promotion, a window
  bounded by `--raft-learner-promotion-stable-window`. Zero learners across a
  run whose replica sets changed means the sampler was too slow to see them, not
  that the moves skipped the state."
  [samples]
  (let [pairs (reduce
                (fn [acc {:keys [views]}]
                  (reduce
                    (fn [acc [_node placement]]
                      (reduce
                        (fn [acc [pid view]]
                          (reduce (fn [acc r]
                                    (if (kc/transitional-roles (:role r))
                                      (update acc (:role r) (fnil conj #{})
                                              [pid (:endpoint r)])
                                      acc))
                                  acc
                                  (:replicas view)))
                        acc
                        (:partitions placement)))
                    acc
                    (filter (comp map? val) views)))
                {}
                samples)]
    {:learners  (count (get pairs :learner #{}))
     :removings (count (get pairs :removing #{}))}))

(defn hosting-lag
  "Partitions a node reported hosting locally while the committed map did not
  list it as a replica, in the *final* sample.

  Transiently this is normal — a node keeps serving a range for a moment after
  the map drops it, and purges afterwards. At the end of a run, after the final
  generator has put everything back, an entry here is a purge that did not
  finish. Reported, never failed: the last sample can land mid-move, and a
  checker that failed on that would be reporting its own sampling cadence."
  [samples]
  (when-let [{:keys [views]} (last samples)]
    (vec (for [[node placement] views
               :when (map? placement)
               [pid view] (:partitions placement)
               :when (and (:hosted-locally view)
                          (seq (:replicas view))
                          (not-any? #(= (:endpoint placement) (:endpoint %))
                                    (:replicas view)))]
           {:node node :partition pid :generation (:generation view)}))))

;; ---------------------------------------------------------------------------
;; Evidence from the server logs
;; ---------------------------------------------------------------------------

(def log-markers
  "Information-level lines that prove a piece of placement machinery ran.

  These are matched as plain substrings of the rendered message, so they must
  stay in step with `KahunaLoggerExtensions` in the Kahuna repo. They were
  chosen because each names the machinery uniquely: nothing else logs
  'Imported whole-partition state', and that message comes from
  `PartitionStateTransfer.ImportAsync` — the snapshot path — and not from log
  backfill, which is the distinction the whole compaction-forcing profile
  exists to create."
  {:banner   "Partition placement: replication factor"
   :hosted   "Started hosting"
   :unhosted "Stopped hosting"
   :imported "Imported whole-partition state of partition"
   :exported "Exported whole-partition state of partition"
   ;; The trailing space is load-bearing and must not be tidied away.
   ;; `RangeSplitTrigger` logs two messages: \"RangeSplitTrigger: splitting
   ;; {Space} [{Start},{End}) at {Key}\" before it starts, and
   ;; \"RangeSplitTrigger: split {Space} at {Key} → P{Id}\" once the cutover has
   ;; committed. Without the space this substring matches both, and the evidence
   ;; would be satisfied by a split that was attempted and failed — which is the
   ;; one outcome a split gate exists to exclude.
   :split    "RangeSplitTrigger: split "})

(defn- count-markers
  "Counts marker occurrences in one log file, one pass, line by line. The files
  run to tens of megabytes because Kahuna logs at debug level, so this never
  slurps and never regexes."
  [file]
  (with-open [r (io/reader file)]
    (reduce (fn [acc ^String line]
              (reduce-kv (fn [acc k ^String m]
                           (if (.contains line m) (update acc k inc) acc))
                         acc
                         log-markers))
            (zipmap (keys log-markers) (repeat 0))
            (line-seq r))))

(defn log-evidence
  "Per-node marker counts from the downloaded server logs, or nil if they are
  not there.

  nil is a distinct answer from all-zeros and the caller must keep it that way:
  'the machinery did not run' and 'nobody looked' license completely different
  conclusions, and collapsing them is how a run with no logs would certify
  itself as having exercised nothing."
  [test]
  ;; `jepsen.store/path` asserts on both of these, and an assertion is an Error
  ;; rather than an Exception — so a test map without a store (every unit test
  ;; here, and any REPL session holding a hand-built map) has to be recognised
  ;; before asking, not caught afterwards.
  (when (and (:name test) (:start-time test))
    (try
      (let [per-node (into (sorted-map)
                           (keep (fn [node]
                                   (let [f (store/path test (name node) "kahuna.log")]
                                     (when (.exists ^java.io.File f)
                                       [node (count-markers f)]))))
                           (:nodes test))]
        (when (seq per-node)
          {:per-node per-node
           :totals   (apply merge-with + (vals per-node))}))
      (catch Throwable t
        (warn t "placement checker could not read server logs")
        nil))))

;; ---------------------------------------------------------------------------
;; The vacuity gate
;; ---------------------------------------------------------------------------

(def evidence-kinds
  "The kinds of placement activity a run can be required to demonstrate, in the
  order they are reported."
  [:move :seeding :purge :split])

(def default-required-evidence
  "What a placement run must show before its pass means anything.

  `:split` is absent for the reason given in full in the namespace docstring: a
  split needs a key space registered for key-range routing, which is what
  `--key-range` does and what `kahuna.checker.range` gates on. Requiring it of
  every replication-factor run would fail runs that never asked to be
  range-routed."
  #{:move :seeding :purge})

(defn evidence
  "Measures each kind of placement activity: `{:measured? bool :count n}`.

  `:measured? false` means this run had no way to tell — the samples or the logs
  it would have been read from are absent — and is never treated as zero."
  [samples logs]
  (let [t (:totals logs)
        m (movement samples)]
    ;; `:move` comes from the samples alone, never from the logs, and the
    ;; tempting shortcut is worth naming: `Started hosting` looks like a move
    ;; signal and is not. It fires whenever a node's hosted set grows against
    ;; its *local* state, which includes every boot — a fresh join, and a
    ;; restart after a kill, both materialize their partitions and log it. A
    ;; gate built on it would be satisfied by starting the cluster, which is the
    ;; exact failure mode this checker exists to prevent. `Stopped hosting` has
    ;; no such twin: it fires only when the committed map drops this node as a
    ;; replica, and it schedules the purge on the next line.
    {:move    (if (seq samples)
                {:measured? true
                 :count (+ (:replicas-gained m) (:replicas-lost m))}
                {:measured? false})
     :seeding (if t {:measured? true :count (:imported t)} {:measured? false})
     :purge   (if t {:measured? true :count (:unhosted t)} {:measured? false})
     :split   (if t {:measured? true :count (:split t)}    {:measured? false})}))

(defn- gate
  "Splits the required evidence into what was shown, what was measured and
  absent, and what could not be measured at all."
  [required ev]
  (reduce (fn [acc k]
            (let [e (get ev k)]
              (cond
                (not (:measured? e)) (update acc :unmeasured conj k)
                (pos? (:count e 0))  (update acc :shown conj k)
                :else                (update acc :missing conj k))))
          {:shown [] :missing [] :unmeasured []}
          (filter required evidence-kinds)))

;; ---------------------------------------------------------------------------
;; The checker
;; ---------------------------------------------------------------------------

(defn checker
  "A jepsen checker for per-partition replica placement.

  * `:valid? true` — placement behaved, and the run demonstrated it did
    something.
  * `:valid? false` — a safety property broke. `:violations` and
    `:disagreements` say which and where.
  * `:valid? :unknown` — nothing was proven. Either the run never moved a
    replica (`:cause :vacuous`) or it could not be observed (`:cause
    :unmeasured`). Read this as 'run it again with the placement nemesis on',
    never as a pass.

  At replication factor 0 this returns `:valid? true` and makes no claim:
  full replication has no replica sets to place, and every property above is
  vacuously true of a map that never changes."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (if-not (kdb/placed? test)
        {:valid? true :replication-factor 0
         :note "full replication; placement not under test"}
        (let [ss       (samples history)
              logs     (log-evidence test)
              vs       (into (violations test ss) (disagreements ss))
              move     (movement ss)
              roles    (roles-observed ss)
              ev       (evidence ss logs)
              required (set (:require-placement-evidence test
                                                         default-required-evidence))
              {:keys [shown missing unmeasured]} (gate required ev)
              base     {:replication-factor (kdb/replication-factor test)
                        :samples            (count ss)
                        :evidence           ev
                        :evidence-required  (vec (sort required))
                        :movement           (dissoc move :transitions)
                        :transitional       roles
                        ;; Informational: see `hosting-lag`. A non-empty list at
                        ;; the end of a run is a purge that did not finish.
                        :hosting-lag        (hosting-lag ss)
                        :transitions        (:transitions move)}
              base     (cond-> base
                         logs (assoc :log-markers (:totals logs)))]
          (cond
            (seq vs)
            (assoc base :valid? false
                        :violations    (vec (remove #(= :map-disagreement (:type %)) vs))
                        :disagreements (vec (filter #(= :map-disagreement (:type %)) vs)))

            (seq unmeasured)
            (assoc base :valid? :unknown :cause :unmeasured :unmeasured unmeasured
                        :missing missing)

            (seq missing)
            (assoc base :valid? :unknown :cause :vacuous :missing missing)

            :else
            (assoc base :valid? true :shown shown)))))))
