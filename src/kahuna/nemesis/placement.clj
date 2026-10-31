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
    planner donate ranges back. The node drained is a voter of the partition
    the workload writes when one is eligible (see `decommission-target`), and
    the first drain waits out `--placement-warmup` so that partition has a
    compacted WAL by the time a learner joins it — the two conditions a
    snapshot seed needs. This is the path that exercises repair, and with
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
  `RefusedDrainInProgress`, `DrainTimedOut`, `Timeout`, …). A nemesis that can
  read *why* a removal did not happen does not need a healthy-cluster
  precondition, so this one runs alongside `partition` and `kill` — which is the
  entire point, since 'leadership moves between the add and the promote' and 'a
  node dies mid-purge' are the scenarios worth hunting.

  A graceful leave now **drains** first: the node's replicas are evacuated onto
  survivors before its removal commits, so the departure preserves durability
  rather than leaving it to post-hoc repair. That makes this the slowest
  operation here — the call blocks for the whole drain — and it adds an outcome
  that must not be mistaken for a departure. `DrainTimedOut` means the evacuation
  did not finish, so the node is *restored to a voter* and keeps serving;
  replicas already moved stay moved, so a later attempt resumes rather than
  restarts.

  The process is stopped only when the removal actually committed. Killing a
  node that is still in the roster is a `kill` fault wearing a leave's name, and
  a history that recorded it as a leave would be lying — which is precisely what
  a `DrainTimedOut` would become if this keyed off the call returning instead of
  off `:left`.

  `Timeout` is the one answer that settles nothing either way. The server says
  so itself (`It may still commit — re-read the membership roster before
  deciding`), and in this suite it often does commit: the decommission is issued
  as a partition starts, so the drain's first commit misses its deadline and
  lands seconds later. A `Timeout` leave is therefore kept **pending** and the
  roster is re-read before each later `:decommission` and `:recommission`
  (`settle-outcome`). A pending node the roster has since dropped, and that
  hosts nothing, had its leave complete late: it is stopped and wiped then —
  the half of the leave the HTTP call never reached — and rejoins with the
  others. A pending node the roster dropped while it *still hosts ranges* was
  evicted, not drained; it is recorded as `:stranded` and left running exactly
  as found, because stopping it would hand its ranges a new leader and hide the
  very defect the checker's stalled-move property exists to catch (Kommander
  `a2233ad0`). Before this, a late-committed leave left a node running outside
  the roster for the rest of the run with a placement table frozen at the moment
  it left, which is how the 2026-10-30 nightly went red on a partition the live
  cluster had merged away.

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
  ever surface. See `kahuna.checker.placement`.

  The sampler also owns the **settle window**: after every fault package has
  put the cluster back, it records a `:placement-settle` marker and keeps
  sampling for a bounded time. Those samples are the only ones that can show
  whether a move finishes on a cluster with nothing wrong with it, which is
  what the checker's `:stalled-move` property needs — see `settle-window`."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as n]]
            [jepsen.util :as util]
            [kahuna.checker.placement :as placement-checker]
            [kahuna.client :as kc]
            [kahuna.db :as kdb]
            [kahuna.hash :as hash]))

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
  "The placement table from whichever node answers first, skipping `excluded`.
  Enough for *choosing* a partition to act on; never enough for *checking*
  anything, because one node's view cannot show two nodes disagreeing.

  `excluded` is the set of nodes this nemesis knows to be out of the roster or
  on their way out (see `nemesis`). A node out of the roster receives no
  committed map and hands back a table frozen at the moment it left, so a
  partition chosen from it may no longer exist."
  ([test] (any-placement test #{}))
  ([test excluded]
   (some kdb/placement (shuffle (vec (remove excluded (:nodes test)))))))

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
;; Aiming at the partition the workload writes
;; ---------------------------------------------------------------------------

(defn written-partitions
  "The data partitions the workload's writes land on, read from the routing
  rule the cluster publishes, or nil when that cannot be known.

  Under hash routing a whole key space lands on one partition, so a register
  or append run writes exactly one of its eight partitions and the other seven
  never compact. The snapshot-seeding path this suite exists to attack runs
  only when a learner joins a partition whose WAL *has* compacted, so a
  decommission that never touches a host of the written partition cannot
  produce a seed no matter how many replicas it moves. Two nightlies went red
  on `:missing [:seeding]` for exactly that: the nemesis drew n6, n6, n4, n6, n4
  while partition 3 sat on n1, n2, n3.

  `/v1/cluster/routing` gives the descriptors of a key-range-routed space
  outright. For a hash-routed space it gives only the rule, and `kahuna.hash`
  reproduces it — after checking the rule's identifier, so a server that
  changes its hash yields *no* answer rather than a wrong one. Returns
  `{:partitions #{pid …} :via :ranges | :hash}`."
  [test]
  (when-let [key-space (:key-space test)]
    (when-let [routing (some #(kdb/routing % key-space)
                             (shuffle (vec (:nodes test))))]
      (let [ranged (->> (:key-spaces routing)
                        (filter #(= key-space (:key-space %)))
                        (mapcat :ranges)
                        (keep :partition-id)
                        set)]
        (if (seq ranged)
          {:partitions ranged :via :ranges}
          (when-let [pid (hash/hash-partition routing key-space)]
            {:partitions #{pid} :via :hash}))))))

(defn decommission-target
  "Which node the next `:decommission` drains.

  A voter of a written partition, when one is eligible: draining it forces the
  planner to add a learner to the one partition that can seed by snapshot.
  Learners are not candidates — a replica still catching up is not yet what
  the drain must re-home — and neither is a node already out. When no written
  partition is known, or none of its voters is eligible, any eligible node is
  drawn at random exactly as before, and the reason is recorded so a run can
  show which case it was in.

  `written` is `(:partitions (written-partitions test))`. Returns
  `{:node n :reason kw}` plus `:partition` when a written partition chose it."
  [test placement written out]
  (let [eligible (vec (remove out (:nodes test)))
        hosts    (vec (for [pid (sort written)
                            r   (get-in placement [:partitions pid :replicas])
                            :when (= :voter (:role r))
                            :let [node (kdb/endpoint->node test (:endpoint r))]
                            :when (and node (not (contains? out node)))]
                        {:node node :partition pid}))]
    (cond
      (seq hosts)     (assoc (rand-nth hosts) :reason :written-partition-host)
      (empty? eligible) nil
      (seq written)   {:node (rand-nth eligible) :reason :no-eligible-host}
      :else           {:node (rand-nth eligible) :reason :written-partition-unknown})))

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

(defn- hosting?
  "Does this node's own table say it still hosts a partition? Both fields are
  the answering node's own: `:hosted-count` is its summary, `:hosted-locally`
  its per-partition flag, and either one is enough."
  [placement]
  (or (pos? (or (:hosted-count placement) 0))
      (boolean (some :hosted-locally (vals (:partitions placement))))))

(defn settle-outcome
  "What became of a leave whose HTTP call answered `Timeout`, read from the
  cluster afterwards. `since` is the roster version the `Timeout` answer
  carried; `roster` is a *member's* view of the committed roster
  (`kdb/membership`), `self` the departing node's own view of it, `placement`
  the departing node's table; each of the last three is nil when it could not
  be read.

  * `:left-late` — the roster no longer names the node and the node hosts no
    partition. The removal committed after the caller gave up, which is the
    outcome the server's answer warns about. The second half of the leave —
    stop and wipe — is now due.
  * `:stranded` — the roster no longer names the node but the node still hosts
    partitions. A drain evacuates a node *before* its removal commits, so a
    node that is out while still hosting was evicted, not drained: it is the
    `NotMember` range leader of Kommander `a2233ad0`, and the thing the
    stalled-move check exists to catch. It is left exactly as found. Stopping
    it would give its ranges a new leader and hide the defect from the run.
  * `:rolled-back` — a roster *newer than the one the answer carried* still
    names the node, and the node calls itself a Voter. Nothing is in flight; it
    is an ordinary member again. The version test is the same watermark rule
    Kommander applies to its own leave latch, and it is load-bearing: on
    2026-10-30 n1's `Leaving` commit landed 40 s after the `Timeout`, so for
    40 s the roster named it Voter at the very version the answer carried. A
    Voter at that version proves nothing; a Voter in a later roster proves the
    drain request died.
  * `:pending` — the roster still names the node: as `Leaving`, with no
    readable role, or as a Voter at a version no newer than `since`. The drain
    may yet commit; ask again later.
  * `:unknown` — no member answered for the roster, or the node is out of the
    roster but its own table could not be read, so hosting cannot be judged.
    Ask again later rather than guess: a wrong `:left-late` is a kill wearing a
    leave's name."
  [node since roster self placement]
  (let [ep (kdb/endpoint node)]
    (cond
      (nil? roster)                    :unknown
      (contains? (:members roster) ep) (if (and (= "Voter" (:role self))
                                                since (:version roster)
                                                (> (:version roster) since))
                                         :rolled-back
                                         :pending)
      (nil? placement)                 :unknown
      (hosting? placement)             :stranded
      :else                            :left-late)))

(defn- settle-pending!
  "Settles every leave in `pending` whose fate the cluster can now tell, and
  returns what it found as `[{:node :outcome :roster-version} …]`.

  A `:left-late` node is stopped and wiped here and moves to `out`, so the next
  `:recommission` brings it back the same way as any committed leave. A
  `:stranded` node moves to `stranded` and is never touched again. A
  `:rolled-back` node is simply forgotten. `:pending` and `:unknown` stay where
  they are and are not reported.

  The roster is read from a node this nemesis believes to be a member — never
  from a pending or stranded node, whose own roster can only repeat what gossip
  told it."
  [test pending out stranded]
  (if (empty? @pending)
    []
    (let [excluded (into (into @out @stranded) (keys @pending))
          roster   (some kdb/membership (shuffle (vec (remove excluded (:nodes test)))))]
      (vec
        (for [node (sort (keys @pending))
              :let [outcome (settle-outcome node
                                            (:membership-version (get @pending node))
                                            roster
                                            (kdb/membership node)
                                            (kdb/placement node))]
              :when (not (contains? #{:pending :unknown} outcome))]
          (do
            (case outcome
              :left-late
              (do (info "placement:" node "left the roster after its leave timed out;"
                        "stopping and wiping it")
                  (attempt (act! test node (fn [test node]
                                             (kdb/stop! test node)
                                             (kdb/wipe-data! test node)))
                           nil)
                  (swap! out conj node))

              :stranded
              (do (warn "placement:" node "is out of the roster but still hosts partitions;"
                        "left running as found (see settle-outcome)")
                  (swap! stranded conj node))

              :rolled-back
              (info "placement:" node "is a Voter again after its leave timed out"))
            (swap! pending dissoc node)
            {:node node :outcome outcome :roster-version (:version roster)}))))))

(defn nemesis
  "The placement nemesis. State — which nodes are currently out, which are
  waiting on the roster's verdict after a `Timeout` leave, which were dropped
  by the roster while still hosting ranges, and which partitions carry an
  override — lives here rather than in the generator: the generator is
  replayed and its choices must stay reproducible, whereas this is the single
  actor that knows what it actually did.

  Every `:decommission` and `:recommission` op first settles the pending leaves
  (`settle-pending!`) and records the result under `:settled`; `:out`,
  `:pending` and `:stranded` on the op's value are this nemesis's bookkeeping
  after the op."
  [opts]
  (let [out       (atom #{})        ; nodes currently removed from the roster
        pending   (atom {})         ; node → the Timeout answer, awaiting the roster's verdict
        stranded  (atom #{})        ; nodes the roster dropped while they still hosted ranges
        overrides (atom {})]        ; partition-id → the override we committed
    (reify n/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (let [;; Nodes whose table is known to be frozen or about to be: out,
              ;; pending, or stranded. Read fresh on each call — settling moves
              ;; nodes between these sets mid-op.
              excluded (fn [] (into (into @out @stranded) (keys @pending)))
              before   (digest (any-placement test (excluded)))]
          (letfn [(done [value]
                    (assoc op :value (assoc value
                                            :placement-before before
                                            :placement-after  (digest (any-placement test (excluded))))))
                  (state []
                    (cond-> {:out (sort @out)}
                      (seq @pending)  (assoc :pending (sort (keys @pending)))
                      (seq @stranded) (assoc :stranded (sort @stranded))))]
            (case (:f op)
              :set-rf
              (if-let [p (any-placement test (excluded))]
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
              (let [settled (settle-pending! test pending out stranded)
                    finish  (fn [value]
                              (done (merge (state) value
                                           (when (seq settled) {:settled settled}))))
                    cap     (max-nodes-out test)
                    floor   (roster-floor test)
                    ;; The *witnessed* roster, not this nemesis's bookkeeping.
                    ;; `out` counts only the nodes this nemesis removed, and it
                    ;; is not the only thing that shrinks a roster: the
                    ;; membership nemesis removes one of its own, and a node
                    ;; evicted after a long enough outage leaves too. Two
                    ;; independent actors each respecting their own bound can
                    ;; still walk a cluster past quorum between them, and every
                    ;; workload would then be reporting quorum loss while the
                    ;; run's name claimed it was testing placement.
                    ;;
                    ;; Read from a node believed to be a member. A node out of
                    ;; the roster, or one whose leave is pending, can only
                    ;; repeat what gossip has told it.
                    roster  (some #(some-> (kdb/membership %) :members count)
                                  (shuffle (vec (remove (excluded) (:nodes test)))))]
                (cond
                  (zero? cap)
                  (finish {:skipped :roster-at-floor :floor floor})

                  (<= cap (count @out))
                  (finish {:skipped :max-nodes-out :cap cap})

                  (and roster (<= roster floor))
                  (finish {:skipped :roster-at-floor :roster roster :floor floor})

                  :else
                  (let [written (written-partitions test)
                        ;; Neither a node already out nor one pending or
                        ;; stranded is a candidate: the first is stopped, the
                        ;; second is still being decided, the third is left
                        ;; alone on purpose.
                        target  (decommission-target test
                                                     (any-placement test (excluded))
                                                     (:partitions written)
                                                     (excluded))]
                    (if-not target
                      (finish {:skipped :no-eligible-node})
                      (let [node (:node target)
                            _    (info "placement: decommissioning" node
                                       (name (:reason target))
                                       (or (:partition target) ""))
                            res  (act! test node kdb/decommission!)]
                        ;; Only a committed removal counts. A node that refused
                        ;; to leave is still a member and is still running, so
                        ;; adding it to `out` would make the next :recommission
                        ;; restart a node that never stopped. A `Timeout` is
                        ;; neither: the removal may still commit, so the node
                        ;; goes to `pending` and the roster decides it later
                        ;; (`settle-outcome`).
                        (cond
                          (:left res)
                          (swap! out conj node)

                          (= "Timeout" (:outcome res))
                          (swap! pending assoc node
                                 (select-keys res [:membership-version :http-status :reason])))
                        (finish (assoc res
                                       :node   node
                                       :target (merge (dissoc target :node)
                                                      (when written
                                                        {:written (sort (:partitions written))
                                                         :via     (:via written)})))))))))

              :recommission
              (let [settled (settle-pending! test pending out stranded)
                    finish  (fn [value]
                              (done (merge (state) value
                                           (when (seq settled) {:settled settled}))))]
                (if-let [node (first (sort @out))]
                  (let [_   (info "placement: rejoining" node)
                        res (attempt (act! test node kdb/join!)
                                     :join-failed)]
                    (when (= :joined res) (swap! out disj node))
                    (finish {:node node :join res}))
                  (finish {:skipped :nobody-out})))))))

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
  must degrade toward fuller replication rather than drop ranges on the floor.

  At zero the roster never changes and only the overrides run. Raising and
  clearing a range's replication factor still makes the planner add, seed,
  promote and retire replicas, so the fault keeps its point; what it drops is
  the drain, which is not a supported operation on every profile."
  [nodes-out]
  (let [n (max 0 nodes-out)]
    (if (zero? n)
      (cycle [{:type :info, :f :set-rf}
              {:type :info, :f :clear-rf}])
      (cycle
        (concat
          (mapcat (fn [_] [{:type :info, :f :decommission}
                           {:type :info, :f :set-rf}
                           {:type :info, :f :clear-rf}])
                  (range n))
          (repeat n {:type :info, :f :recommission}))))))

(defrecord NotBefore [t gen]
  gen/Generator
  (op [this test ctx]
    (when-let [[op gen'] (gen/op gen test ctx)]
      (if (= :pending op)
        [op (NotBefore. t gen')]
        [(assoc op :time (max (:time op) t)) (NotBefore. t gen')])))
  (update [this test ctx event]
    (NotBefore. t (gen/update gen test ctx event))))

(defn not-before
  "Wraps `gen` so nothing it emits is scheduled before `secs` into the run.

  Only the schedule moves: the op is handed back with a later `:time`, and the
  interpreter sleeps until then. Nothing blocks — the sampler and the other
  nemeses in the same `gen/any` keep running, which `(gen/sleep secs)` on the
  nemesis thread would not allow.

  Why the placement fault needs a head start and the others do not: the first
  decommission used to fire in the same millisecond as the first client write,
  so a learner added by that drain joined a partition whose log was empty and
  caught up from it — no snapshot, no `seeding` evidence — and the drain then
  left the partition over-replicated, so no later drain needed a learner there
  either. And it is not enough for writes to have merely started: the WAL is
  trimmed only once a checkpoint has advanced the retention floor, and the
  first one lands about 30 s after the first write (`--checkpoint-interval`).
  A 20 s head start was measured to backfill 67 entries from index 1 — no
  seed; 60 s put the learner below the floor and seeded it by snapshot."
  [secs gen]
  (NotBefore. (long (util/secs->nanos secs)) gen))

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
                           (not-before (:placement-warmup opts 60))
                           (gen/stagger (:placement-interval opts 30)))
     ;; Everything comes back before the final read, so the last generator phase
     ;; runs against a whole roster. One :recommission per node that could be
     ;; out — the nemesis answers :nobody-out for the extras, which is cheap.
     ;; Nothing to put back when roster churn is off, and an empty final
     ;; generator is the honest way to say so.
     :final-generator (repeat (max 0 (:placement-nodes-out opts 1))
                              {:type :info, :f :recommission})
     :nemesis         (nemesis opts)
     :perf            #{{:name  "placement"
                         :start #{:decommission}
                         :stop  #{:recommission}
                         :color "#C7B3E5"}}}))

;; ---------------------------------------------------------------------------
;; Sampling
;; ---------------------------------------------------------------------------

(defn settle-window
  "Seconds the sampler keeps reading the table after the final heal.

  The checker calls a move stalled when a replica sits in a transitional role
  for `stalled-move-bound` seconds of settled time, and it can only measure
  what the samples span. The window is the bound plus four sample intervals,
  so a replica mid-move for the whole window is observed for at least the
  bound even after the first and last samples are discounted. At the defaults
  (ten passes of 5 s, sampling every 3 s) that is 62 s on the end of each
  placed run — paid once, and it is what turns 'some move finished' into
  'every move that started on a healthy cluster finished'."
  [opts]
  (+ (placement-checker/stalled-move-bound opts)
     (* 4 (:placement-sample-interval opts 3))))

(defn sampler
  "A nemesis that records every node's view of the placement table, each
  stamped with that node's own roster role, and marks the start of the settle
  window. Never waits on anything: an unreachable node contributes nil, and a
  sample is two HTTP round trips per node from the control node — the table,
  and the membership read that stamps it (`kdb/placement-sample`). The stamp
  is what lets the checker set aside the frozen table of a node that is no
  longer in the roster; see `kahuna.checker.placement/member-view?`.

  `:placement-settle` records nothing about the cluster. Its completion time
  is the fact: every fault package's final generator has run by then, so the
  samples after it describe a healed cluster. The value carries the bound and
  window in force so a history explains its own verdict."
  [opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (case (:f op)
        :placement-sample
        (assoc op :value (kdb/placement-sample test))

        :placement-settle
        (let [bound  (placement-checker/stalled-move-bound opts)
              window (settle-window opts)]
          (info "placement: cluster healed; sampling for" window
                "s to catch a move that does not finish within" bound "s")
          (assoc op :value {:bound-secs bound :window-secs window}))))

    (teardown! [this test])

    n/Reflection
    (fs [this] #{:placement-sample :placement-settle})))

(defn sampler-package
  "A placement-sampling package, shaped for `nc/compose-packages`.

  Enabled whenever the test runs with a replication factor, whether or not the
  placement *fault* is on: `kill` and `membership` move replicas too (a dead
  node's ranges get repaired onto survivors), and a run that cannot see that
  happen cannot claim it did.

  The final generator is the settle window. `compose-packages` runs final
  generators in package order, and this package is composed after every fault
  package, so the `:placement-settle` marker lands once the killed nodes are
  restarted, the partitions healed and the decommissioned nodes rejoined. The
  samples that follow are taken at a fixed cadence (`gen/delay`, not
  `gen/stagger`) so the window's span is predictable: a staggered window could
  come up short of the bound by chance and turn a clean run `:unmeasured`.

  Deliberately absent from `:perf` — these are observations, not faults, and a
  stripe every few seconds on the latency plot would bury the faults that
  matter."
  [opts]
  (if-not (and (pos? (or (:replication-factor opts) 0))
               (:placement-sampling opts true))
    {:generator nil :final-generator nil :nemesis nil :perf #{}}
    (let [interval (:placement-sample-interval opts 3)]
      {:generator       (->> (repeat {:type :info, :f :placement-sample})
                             (gen/stagger interval))
       :final-generator [{:type :info, :f :placement-settle}
                         (->> (repeat {:type :info, :f :placement-sample})
                              (gen/delay interval)
                              (gen/time-limit (settle-window opts)))]
       :nemesis         (sampler opts)
       :perf            #{}})))
