(ns kahuna.nemesis.membership
  "Membership nemesis: takes a node out of the voting roster and puts it back.

  ## How membership is actually changed

  Not through the API. `/v1/cluster/membership` is GET-only
  (`ClusterHandlers.MapClusterRoutes` maps exactly one route), so there is no
  add-node or remove-node call to make. Kommander exposes `JoinCluster` /
  `LeaveCluster` on `IRaft`, and Kahuna drives both from the process lifecycle:

  * `--join-existing` makes a starting node request membership from the seeds in
    `--initial-cluster`. It is admitted as a Learner, backfilled, and promoted to
    Voter once its log is caught up (`ReplicationService.ExecuteAsync`).
  * `--graceful-leave-on-shutdown` makes a node commit `RemoveMember(self)` from
    its shutdown hook (`ReplicationService.StopAsync`).

  So a leave is SIGTERM-and-wait, and a join is a restart with an extra flag.
  Both are slow — tens of seconds — which is why this fault runs on a longer
  interval than the others.

  ## Two ways this nemesis silently did nothing

  Both were caught by comparing the roster before and after, and both produced
  a *clean* register verdict while changing no membership at all. That check is
  now part of every `:leave`, and its result is in the history as `:removed`.

  1. **Firing before the cluster formed.** The first `:leave` landed 3 s into a
     test, while the target was still logging `JoinCluster: waiting for
     initialization ... systemLeader=none`. There was no leader to commit a
     RemoveMember, so the node just died. Hence the `cluster-formed?` gate.
  2. **SIGKILLing before the leave finished.** A 20 s grace period is shorter
     than .NET's 30 s default host shutdown budget, so the departing node was
     killed mid-leave every time and the roster never moved. See
     `kahuna.db/graceful-stop-timeout-s`.

  ## Why only one node leaves at a time

  Five voters tolerate two failures. This nemesis removes exactly one, taking
  the cluster to four voters (quorum 3, one failure tolerated), and puts it back
  before removing another. Combined with `partition` or `kill` that is already
  enough to reach the edge; removing two would make quorum loss the expected
  outcome and every workload would grind to `:fail` for reasons no checker
  could distinguish from a real bug.

  Kommander refuses a removal that would leave zero voters
  (`InsufficientVoters`, a terminal status), but 5 -> 1 is permitted, so the
  bound has to come from here.

  `cluster-formed?` also means that when this fault is combined with
  `partition` or `kill`, most membership operations will decline: a cluster
  missing a node is not fully formed. That is the conservative choice —
  membership churn is only exercised from a healthy state — and it makes the
  combination weaker than it looks. Read the `:leave` values before concluding
  a combined run tested anything.

  ## Why a departing node is wiped

  A node that leaves and comes back is a *new* member, and is started that way:
  its RocksDB data and WAL are deleted so it joins as a Learner and is
  backfilled from scratch. Rejoining while carrying a log from a membership
  epoch it no longer belongs to is a different scenario, and one the Kommander
  guide never claims to support — testing it here by accident would produce
  findings this suite could not defend.

  ## Interaction with `kill`

  None, and that is worth knowing. Automatic eviction of a crashed node is a
  SWIM job, and SWIM is wired only on Kommander's in-memory transport — on the
  gRPC transport Kahuna uses, the failure detector is disabled
  (`PingInterval = TimeSpan.Zero`). A SIGKILLed node therefore stays in the
  roster indefinitely, so `kill` never changes membership behind this nemesis's
  back, and a node this nemesis removed never comes back on its own."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as n]]
            [kahuna.db :as kdb]))

(defn- act!
  "Runs `f` on `node` in that node's SSH context, returning its result."
  [test node f]
  (-> (c/on-nodes test [node] (fn [test node] (f test node)))
      (get node)))

(defn nemesis
  "A nemesis that removes one node from the roster and adds it back.

  `:leave` picks a node that is currently in, `:join` returns whichever node
  this nemesis last removed. The node is held in an atom here rather than
  chosen by the generator on purpose: the generator is replayed and its choices
  must stay reproducible, whereas the nemesis is a single stateful actor that
  is the only thing that knows what it actually did."
  [opts]
  (let [;; The node currently out of the roster, or nil.
        out (atom nil)]
    (reify n/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (case (:f op)
          :leave
          (cond
            ;; Refusing rather than removing a second node is the quorum bound
            ;; described in the namespace docstring.
            @out
            (assoc op :value [:already-out @out])

            ;; A cluster that has not finished forming has no leader to commit
            ;; a RemoveMember, so stopping a node here removes nothing and the
            ;; fault degenerates into a slow `kill`. This is not hypothetical:
            ;; it is what the first version of this nemesis did for a whole
            ;; run, and the run passed.
            (not (kdb/cluster-formed? test))
            (assoc op :value :cluster-not-formed)

            :else
            (let [node (rand-nth (vec (:nodes test)))
                  ;; A survivor's view is the only trustworthy one: the
                  ;; departing node is about to be stopped and wiped.
                  witness (first (remove #{node} (:nodes test)))
                  before  (:members (kdb/membership witness))]
              (info "membership: removing" node)
              (let [stopped (act! test node kdb/leave!)
                    after   (:members (kdb/membership witness))
                    shrank? (boolean (and before after (< (count after)
                                                          (count before))))]
                (reset! out node)
                (assoc op :value {:node    node
                                  :stop    stopped
                                  :roster  [(count before) (count after)]
                                  ;; The whole point of the operation. A false
                                  ;; here means the node went away without
                                  ;; leaving the roster — worth seeing in the
                                  ;; history rather than inferring later.
                                  :removed shrank?}))))

          :join
          (if-let [node @out]
            (do (info "membership: rejoining" node)
                (let [res (try
                            (act! test node kdb/join!)
                            (catch Exception e
                              ;; A join that fails leaves the node out of the
                              ;; roster; say so rather than clearing `out` and
                              ;; letting the next :leave drop a second node.
                              (warn e "membership: rejoin of" node "failed")
                              :join-failed))]
                  (when (= :joined res)
                    (reset! out nil))
                  (assoc op :value {:node node :join res})))
            (assoc op :value :nobody-out))))

      (teardown! [this test])

      n/Reflection
      (fs [this] #{:leave :join}))))

(defn package
  "A nemesis package for membership changes, shaped like the ones
  jepsen.nemesis.combined returns so it can be handed to `nc/compose-packages`.

  Returns the no-op package unless :membership is in :faults.

  The interval is deliberately independent of the other faults': a leave waits
  for the departing node to commit its removal and a join waits for a Learner to
  catch up, so at the 15 s default the two operations would overlap each other
  and the roster would spend the whole test mid-change."
  [opts]
  (if-not (some #{:membership} (:faults opts))
    {:generator nil :final-generator nil :nemesis nil :perf #{}}
    (let [interval (:membership-interval opts 30)]
      {:generator       (->> [{:type :info, :f :leave}
                              {:type :info, :f :join}]
                             cycle
                             (gen/stagger interval))
       ;; Whatever is out at the end comes back, so the final read runs against
       ;; a whole cluster.
       :final-generator {:type :info, :f :join}
       :nemesis         (nemesis opts)
       :perf            #{{:name  "membership"
                           :start #{:leave}
                           :stop  #{:join}
                           :color "#B8E9A0"}}})))
