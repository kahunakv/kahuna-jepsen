(ns kahuna.nemesis.placement
  "Placement nemesis: forces replica moves while the workload runs, and samples
  the committed placement table so a checker can see what moved.

  ## Why this fault exists at all

  Under a replication factor each partition has an explicit replica set, and the
  machinery that maintains it — adding a Learner, seeding it (by log backfill or
  by whole-partition snapshot), promoting it to Voter, marking the outgoing
  replica Removing, dropping it, then purging the partition's key-values, locks,
  transaction bookkeeping and durability floors from the node that lost it —
  runs *only* when something makes placement change. `partition`, `kill` and
  `pause` never do. A run with all three faults and no placement change
  exercises none of that code and passes cleanly having tested nothing about
  placement at all.

  So this nemesis's whole job is to make the planner move replicas:

  * `:set-rf` / `:clear-rf` — a per-range replication-factor override. Raising
    it forces adds, lowering it forces removals, clearing it makes the range
    converge back on the global factor. This is the cheapest way to move a
    replica without touching the roster, so it is the one that runs against an
    otherwise healthy cluster.
  * `:decommission` / `:recommission` — a node commits its own removal through
    `POST /v1/cluster/leave` and is then stopped and wiped; the planner must
    re-replicate its ranges onto the survivors. Rejoining it fresh makes the
    planner donate ranges back. This is the path that exercises repair, and with
    `--placement-nodes-out` above 1 it walks the roster down toward the
    replication factor, where the planner must degrade to fuller replication
    rather than lose ranges.

  ## Why a leave goes through the API here and not through SIGTERM

  `kahuna.nemesis.membership` stops a node and waits for its shutdown hook to
  commit `RemoveMember(self)`. That works, but it can only *infer* whether the
  removal happened, which is why it has to refuse to act unless the cluster is
  fully formed — and that refusal is what makes it close to a no-op when
  combined with any other fault.

  `POST /v1/cluster/leave` commits the removal synchronously and answers with
  the consensus outcome (`Committed`, `NoLeader`, `RefusedInsufficientVoters`,
  `Timeout`, …). A nemesis that can read *why* a removal did not happen does not
  need a healthy-cluster precondition, so this one runs alongside `partition`
  and `kill` — which is the entire point, since 'leadership moves between the
  add and the promote' and 'a node dies mid-purge' are the scenarios worth
  hunting.

  The process is stopped only when the removal actually committed. Killing a
  node that is still in the roster is a `kill` fault wearing a leave's name, and
  a history that recorded it as a leave would be lying.

  ## Sampling, and why it is separate

  `sampler-package` records every node's view of the placement table on a timer.
  It is not a fault; it is the only way to see a Learner exist at all. A learner
  is promoted after `--raft-learner-promotion-stable-window` (3 s by default), so
  a move is visible as a *state* for a few seconds and then only as a changed
  replica set. Sampling at a few seconds catches the transitions; asking once at
  the end sees nothing but the final layout.

  Every node is asked, not one. The map is committed on the meta partition, so
  two nodes reporting the same partition at the same generation must report an
  identical replica set — a disagreement is a finding no single-node read could
  ever surface. See `kahuna.checker.placement`."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as n]]
            [kahuna.client :as kc]
            [kahuna.db :as kdb]))

(defmacro ^:private attempt
  "Evaluates `body`, returning `fallback` on any throwable. A nemesis that dies
  because one node refused a connection takes the whole run's history with it."
  [body fallback]
  `(try ~body (catch Throwable _# ~fallback)))

(defn- act!
  "Runs `f` on `node` in that node's SSH context, returning its result."
  [test node f]
  (-> (c/on-nodes test [node] (fn [test node] (f test node)))
      (get node)))

;; ---------------------------------------------------------------------------
;; Reading the map
;; ---------------------------------------------------------------------------

(defn- any-placement
  "The placement table from whichever node answers first. Enough for *choosing*
  a partition to act on; never enough for *checking* anything, because one
  node's view cannot show two nodes disagreeing."
  [test]
  (some kdb/placement (shuffle (vec (:nodes test)))))

(defn digest
  "A compact summary of a placement table, small enough to sit in the history on
  every operation: partition → [generation, voter count, transitional count].

  Recorded before and after each fault so a run carries its own evidence of
  whether the map moved. Do not read a `:moved? false` as a broken nemesis: a
  replica move is several committed steps and a seed in between, so the map
  usually has not changed by the time the op completes. The sampler is what
  observes convergence; this is what proves the op was aimed at a real map."
  [placement]
  (when placement
    (into (sorted-map)
          (map (fn [[pid p]]
                 (let [roles (frequencies (map :role (:replicas p)))]
                   [pid [(:generation p)
                         (get roles :voter 0)
                         (reduce + 0 (map #(get roles % 0) kc/transitional-roles))]])))
          (:partitions placement))))

(defn- movable-partitions
  "Data partitions that a replication-factor override can actually move.

  Filters out three things, each of which would make the op a silent no-op:
  partition ids that are not data partitions, ranges that are not Active, and
  ranges with an *empty* replica set. The last is the legacy full-replication
  case — Kommander refuses replica mutations on those outright (\"assign an
  initial placement first\"), so an override aimed at one changes a target that
  nothing will ever converge to."
  [placement]
  (->> (:partitions placement)
       (filter (fn [[pid p]]
                 (and (pos? pid)
                      (= "Active" (:state p))
                      (seq (:replicas p)))))
       (mapv key)))

;; ---------------------------------------------------------------------------
;; Operations
;; ---------------------------------------------------------------------------

(defn- override-targets
  "Candidate override factors for a cluster of `node-count` nodes at global
  factor `rf`, excluding whatever the range already has.

  Both directions are wanted. Raising the factor adds a replica (exercising the
  add → seed → promote path); lowering it removes one (exercising Removing, the
  final drop, and the un-host purge). 1 is always a candidate because at a
  single replica per range every placement bug is data loss rather than
  degradation."
  [rf node-count current]
  (->> [1 (min node-count (+ rf 2))]
       distinct
       (remove #(= % current))
       vec))

(defn- set-rf!
  "Commits a per-range override, trying endpoints until one accepts.

  Only the meta-partition leader may mutate the map; every other node answers
  409 with the reason. Rather than track leadership, this asks nodes in random
  order and stops at the first success — the same thing `kahuna.control` does.
  A run where *no* node accepts is recorded as such, because 'the override never
  committed' and 'the override committed and nothing moved' are different
  findings and must not be confused."
  [test partition-id rf]
  (loop [[node & more] (shuffle (vec (:nodes test)))
         refusals      []]
    (if-not node
      {:committed false :refusals refusals}
      (let [r (attempt (kc/set-replication-factor! node partition-id rf {})
                       {:success false :outcome :unreachable})]
        (if (:success r)
          {:committed   true
           :accepted-by node
           :generation  (:generation r)
           :refusals    refusals}
          (recur more (conj refusals [node (or (:outcome r) (:reason r))])))))))

(defn- roster-floor
  "The smallest roster this nemesis will shrink to. Never below three voters —
  a two-node roster tolerates no failure at all and every other fault in the
  test would then be reporting quorum loss rather than anything about placement
  — and never below the replication factor, since a range cannot hold more
  replicas than there are nodes."
  [test]
  (max 3 (kdb/replication-factor test)))

(defn max-nodes-out
  "How many nodes may be out of the roster at once, given the request and the
  floor. Clamped rather than validated so an over-ambitious `--placement-nodes-out`
  degrades to the largest safe value instead of refusing to start a run."
  [test]
  (max 0 (min (:placement-nodes-out test 1)
              (- (count (:nodes test)) (roster-floor test)))))

(defn nemesis
  "The placement nemesis. State — which nodes are currently out and which
  partitions carry an override — lives here rather than in the generator: the
  generator is replayed and its choices must stay reproducible, whereas this is
  the single actor that knows what it actually did."
  [opts]
  (let [out       (atom #{})        ; nodes currently removed from the roster
        overrides (atom {})]        ; partition-id → the override we committed
    (reify n/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (let [before (digest (any-placement test))]
          (letfn [(done [value]
                    (assoc op :value (assoc value
                                            :placement-before before
                                            :placement-after  (digest (any-placement test)))))]
            (case (:f op)
              :set-rf
              (if-let [p (any-placement test)]
                (let [candidates (remove @overrides (movable-partitions p))]
                  (if-not (seq candidates)
                    (done {:skipped :no-movable-partition})
                    (let [pid     (rand-nth (vec candidates))
                          current (get-in p [:partitions pid :effective-rf])
                          targets (override-targets (kdb/replication-factor test)
                                                    (count (:nodes test))
                                                    current)]
                      (if-not (seq targets)
                        (done {:skipped :no-distinct-target :partition pid})
                        (let [rf  (rand-nth targets)
                              _   (info "placement: setting partition" pid "replication factor to" rf)
                              res (set-rf! test pid rf)]
                          (when (:committed res) (swap! overrides assoc pid rf))
                          (done (merge {:partition pid :from current :to rf} res)))))))
                (done {:skipped :no-placement-view}))

              :clear-rf
              (if-let [[pid rf] (first @overrides)]
                (let [_   (info "placement: clearing partition" pid "override of" rf)
                      res (set-rf! test pid 0)]
                  (when (:committed res) (swap! overrides dissoc pid))
                  (done (merge {:partition pid :was rf :to 0} res)))
                (done {:skipped :no-override-set}))

              :decommission
              (let [cap    (max-nodes-out test)
                    floor  (roster-floor test)
                    ;; The *witnessed* roster, not this nemesis's bookkeeping.
                    ;; `out` counts only the nodes this nemesis removed, and it
                    ;; is not the only thing that shrinks a roster: the
                    ;; membership nemesis removes one of its own, and a node
                    ;; evicted after a long enough outage leaves too. Two
                    ;; independent actors each respecting their own bound can
                    ;; still walk a cluster past quorum between them, and every
                    ;; workload would then be reporting quorum loss while the
                    ;; run's name claimed it was testing placement.
                    roster (some #(some-> (kdb/membership %) :members count)
                                 (shuffle (vec (remove @out (:nodes test)))))]
                (cond
                  (zero? cap)
                  (done {:skipped :roster-at-floor :floor floor})

                  (<= cap (count @out))
                  (done {:skipped :max-nodes-out :out (sort @out) :cap cap})

                  (and roster (<= roster floor))
                  (done {:skipped :roster-at-floor :roster roster :floor floor})

                  :else
                  (let [node (rand-nth (vec (remove @out (:nodes test))))
                        _    (info "placement: decommissioning" node)
                        res  (act! test node kdb/decommission!)]
                    ;; Only a committed removal counts. A node that refused to
                    ;; leave is still a member and is still running, so adding
                    ;; it to `out` would make the next :recommission restart a
                    ;; node that never stopped.
                    (when (:left res) (swap! out conj node))
                    (done (assoc res :node node :out (sort @out))))))

              :recommission
              (if-let [node (first (sort @out))]
                (let [_   (info "placement: rejoining" node)
                      res (attempt (act! test node kdb/join!)
                                   :join-failed)]
                  (when (= :joined res) (swap! out disj node))
                  (done {:node node :join res :out (sort @out)}))
                (done {:skipped :nobody-out}))))))

      (teardown! [this test])

      n/Reflection
      (fs [this] #{:set-rf :clear-rf :decommission :recommission}))))

(defn- fault-generator
  "Cycles overrides and roster churn, sized so the roster actually reaches its
  floor when `--placement-nodes-out` asks for it.

  Each pass takes one more node out and works the overrides while it is gone,
  then puts every node back before starting again. With the default of one node
  out this is the obvious leave/rejoin cycle; at three it walks a six-node
  cluster down to three and back, which is the scale-down scenario — the planner
  must degrade toward fuller replication rather than drop ranges on the floor."
  [nodes-out]
  (let [n (max 1 nodes-out)]
    (cycle
      (concat
        (mapcat (fn [_] [{:type :info, :f :decommission}
                         {:type :info, :f :set-rf}
                         {:type :info, :f :clear-rf}])
                (range n))
        (repeat n {:type :info, :f :recommission})))))

(defn package
  "A nemesis package for placement churn, shaped like the ones
  `jepsen.nemesis.combined` returns.

  Returns the no-op package unless `:placement` is in `:faults` *and* the test
  runs with a replication factor. At RF 0 there are no replica sets to move:
  every range is hosted by every voter, replica mutations are refused, and the
  fault would do nothing while the run's name claimed otherwise.

  The interval defaults longer than the other faults' because a replica move is
  not instantaneous: an add, a seed (a whole-partition snapshot when the
  learner starts below the WAL compaction floor), a promotion that waits out
  `--raft-learner-promotion-stable-window`, and two more committed map changes to
  retire the outgoing replica. Firing faster than that just queues moves the
  planner refuses — it allows only `--raft-max-concurrent-replica-transfers`
  ranges in flight at a time."
  [opts]
  (if-not (and (some #{:placement} (:faults opts))
               (pos? (or (:replication-factor opts) 0)))
    (do (when (some #{:placement} (:faults opts))
          (warn "placement fault requested without --replication-factor; disabled"))
        {:generator nil :final-generator nil :nemesis nil :perf #{}})
    {:generator       (->> (fault-generator (:placement-nodes-out opts 1))
                           (gen/stagger (:placement-interval opts 30)))
     ;; Everything comes back before the final read, so the last generator phase
     ;; runs against a whole roster. One :recommission per node that could be
     ;; out — the nemesis answers :nobody-out for the extras, which is cheap.
     :final-generator (repeat (max 1 (:placement-nodes-out opts 1))
                              {:type :info, :f :recommission})
     :nemesis         (nemesis opts)
     :perf            #{{:name  "placement"
                         :start #{:decommission}
                         :stop  #{:recommission}
                         :color "#C7B3E5"}}}))

;; ---------------------------------------------------------------------------
;; Sampling
;; ---------------------------------------------------------------------------

(defn sampler
  "A nemesis whose only operation records every node's view of the placement
  table. Never waits on anything: an unreachable node contributes nil, and a
  sample is one HTTP round trip per node from the control node."
  []
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc op :value (kdb/cluster-placement test)))

    (teardown! [this test])

    n/Reflection
    (fs [this] #{:placement-sample})))

(defn sampler-package
  "A placement-sampling package, shaped for `nc/compose-packages`.

  Enabled whenever the test runs with a replication factor, whether or not the
  placement *fault* is on: `kill` and `membership` move replicas too (a dead
  node's ranges get repaired onto survivors), and a run that cannot see that
  happen cannot claim it did.

  Deliberately absent from `:perf` — these are observations, not faults, and a
  stripe every few seconds on the latency plot would bury the faults that
  matter."
  [opts]
  (if-not (and (pos? (or (:replication-factor opts) 0))
               (:placement-sampling opts true))
    {:generator nil :final-generator nil :nemesis nil :perf #{}}
    {:generator       (->> (repeat {:type :info, :f :placement-sample})
                           (gen/stagger (:placement-sample-interval opts 3)))
     :final-generator nil
     :nemesis         (sampler)
     :perf            #{}}))
