(ns kahuna.db
  "Installs, starts, stops, kills and pauses Kahuna.Server on each node.

  Kahuna is a .NET application. Rather than install a .NET runtime on every
  Jepsen node, we ship a *self-contained* publish as a tarball built on the
  host (see scripts/build-tarball.sh) and upload it. The tarball must contain a
  `Kahuna.Server` executable and `certificate.pfx` at its root."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [kahuna.client :as kc]
            [slingshot.slingshot :refer [try+ throw+]]))

(def dir      "/opt/kahuna")
(def binary   (str dir "/Kahuna.Server"))
(def logfile  (str dir "/kahuna.log"))
(def pidfile  (str dir "/kahuna.pid"))
(def data-dir (str dir "/storage/data"))
(def wal-dir  (str dir "/storage/wal"))

(def raft-port
  "Raft/replication port. Also the HTTPS API port — Kahuna binds both from the
  same --https-ports/--raft-port pair, which is why the nemesis's network
  faults hit replication and client HTTPS traffic together. Client traffic in
  these tests goes over plain HTTP on kahuna.client/http-port instead."
  8082)

(defn node-id
  "Kahuna wants a small integer node id. Jepsen node names are conventionally
  n1..n5, so derive the id from the trailing digits, falling back to position in
  the node list."
  [test node]
  (if-let [n (re-find #"\d+$" (name node))]
    (Long/parseLong n)
    (inc (.indexOf ^java.util.List (vec (:nodes test)) node))))

(defn endpoint
  "This node's Raft endpoint, `host:port` — the identity every placement replica
  and roster member is named by. The placement table speaks endpoints, the rest
  of the harness speaks node names, and `endpoint->node` maps back."
  [node]
  (str (name node) ":" raft-port))

(defn endpoint->node
  "Inverse of `endpoint` over a test's node list, or nil for an endpoint that
  belongs to no known node. Returns nil rather than guessing: a replica on an
  endpoint this test does not know about is a finding, not a parsing problem."
  [test ep]
  (first (filter #(= ep (endpoint %)) (:nodes test))))

(defn peers
  "The --initial-cluster seed list: every node *except* this one, as host:port.
  docker/local.yml in the Kahuna repo excludes self, so we do too."
  [test node]
  (->> (:nodes test)
       (remove #(= % node))
       (map endpoint)))

(defn replication-factor
  "The test's configured replication factor, or 0 for full replication. 0 is not
  merely 'off': it is the legacy mode in which every voter hosts every range and
  replica sets are empty, and it must stay byte-identical to the pre-placement
  behaviour."
  [test]
  (or (:replication-factor test) 0))

(defn placed?
  "Is this test running with per-partition placement? Everything placement-shaped
  — the RF server flags, the placement nemesis, the placement checker's claims —
  keys off this and degrades to a no-op when it is false, so an RF-0 run is
  unchanged by any of it."
  [test]
  (pos? (replication-factor test)))

(defn zone
  "This node's zone, or nil when `--zones` is 0. Zones are assigned round-robin
  over the node list so consecutive nodes land in different zones — with 6 nodes
  and 3 zones that is two nodes per zone, and the planner then spreads each
  range's three replicas one per zone, which is the placement a zone outage is
  supposed to survive."
  [test node]
  (let [zones (:zones test 0)]
    (when (pos? zones)
      (str "z" (mod (.indexOf ^java.util.List (vec (:nodes test)) node) zones)))))

(defn placement-args
  "Replication-factor flags for one node's command line.

  Empty at RF 0, which is the point: a run that does not ask for placement must
  produce exactly the command line it produced before placement existed.

  Booleans are passed as bare switches rather than `--flag true`. CommandLineParser
  treats a `bool` option as a switch, and every placement flag defaults to false,
  so presence means on and absence means off. The operations guide writes
  `--raft-enable-placement-rebalancer true`; the switch form is what the rest of
  this file already uses (`--raft-allow-insecure-certificate-validation`) and it
  cannot be mis-tokenized."
  [test node]
  (when (placed? test)
    (concat
      [:--raft-replication-factor (replication-factor test)]
      ;; On unless explicitly disabled. Initial placement lands either way; this
      ;; is the switch for *ongoing* moves — repairing under-replicated ranges,
      ;; trimming over-replication, smoothing skew. Without it the placement
      ;; nemesis can change targets all day and nothing will ever move, which is
      ;; the quietest possible way for this whole suite to test nothing.
      (when (:rebalancer test true) [:--raft-enable-placement-rebalancer])
      ;; Load reports are enabled automatically whenever a factor is set; asking
      ;; for them explicitly costs nothing and makes the intent legible in `ps`.
      [:--raft-enable-load-reports]
      (when-let [n (:max-replica-moves-per-pass test)]
        [:--raft-max-replica-moves-per-pass n])
      (when-let [n (:max-concurrent-replica-transfers test)]
        [:--raft-max-concurrent-replica-transfers n])
      (when-let [n (:replica-count-deadband test)]
        [:--raft-replica-count-deadband n])
      (when-let [z (zone test node)]
        [:--raft-zone z]))))

(def compaction-args
  "Forces the Raft WAL floor to advance fast enough that a replica added
  mid-run is *below* it and must be seeded by a whole-partition snapshot rather
  than by log backfill.

  Three knobs, and all three are needed. `--raft-compact-every-operations`
  decides how often compaction runs, but compaction cannot discard anything the
  PITR floor still protects, and that floor sits at `now − pitr-window −
  base-snapshot-interval` — an hour and a half back by default, which no Jepsen
  run ever reaches. At the defaults, therefore, every learner catches up from
  the log and `PartitionStateTransfer` is never entered: a profile whose
  checkpoints never fire silently validates nothing about snapshot seeding.

  The cost is that these settings destroy the point-in-time recovery window, so
  they belong to a chaos profile and nowhere near a real deployment."
  [:--raft-compact-every-operations 200
   :--raft-compact-number-entries   100
   :--pitr-window                   1
   :--base-snapshot-interval        1])

;; ---------------------------------------------------------------------------
;; Key-range routing
;; ---------------------------------------------------------------------------

(defn key-space
  "The key space this test's workload stores its keys in, or nil for a workload
  whose state is not key/value keys at all (locks, sequences).

  A *key space* is a key's prefix up to — and excluding — its last `/`, which is
  the same boundary Kommander hashes on. So `jepsen/register/4` lives in
  `jepsen/register`. It is the unit key-range routing is opted into, so a
  workload that cannot name one cannot be range-routed."
  [test]
  (:key-space test))

(defn key-ranged?
  "Is this test running its workload's key space under key-range routing?

  Everything range-shaped — the registration step, the range nemesis, the range
  checker's claims — keys off this and degrades to a no-op when it is false, so
  a run without `--key-range` is unchanged by any of it."
  [test]
  (boolean (and (:key-range test) (key-space test))))

(defn range-args
  "Split/merge policy flags for one node's command line, rendered only for the
  knobs a run actually set.

  All four exist on the server with defaults that are right for production and
  wrong for a 300 s chaos run: auto-split fires at 1000 sampled keys and the
  sampler runs once a minute, so nothing splits by itself inside a run. The
  nemesis does not need any of them — it forces splits through
  `POST /v1/ranges/split`, which is deterministic — but a profile that wants to
  exercise the *automatic* splitter has to be able to reach them.

  `when-let` rather than `when`, because 0 is a meaningful value for two of
  these (it disables the checker outright) and 0 is truthy in Clojure, so it
  renders correctly. `--range-collection-interval` is shared with the key-value
  collector, prepared-intent recovery and range-lock renewal — the server floors
  it at the phase-two commit timeout and refuses to start below that, so
  lowering it to make splits fire sooner is not free."
  [test]
  (concat
    (when-let [n (:range-split-threshold test)]      [:--range-split-threshold n])
    (when-let [n (:range-split-min-range-size test)] [:--range-split-min-range-size n])
    (when-let [n (:range-merge-min-size test)]       [:--range-merge-min-size n])
    (when-let [n (:range-collection-interval test)]  [:--range-collection-interval n])))

(defn membership-faults?
  "Is the membership nemesis enabled for this test? Governs whether nodes are
  started with --graceful-leave-on-shutdown, which is a *start* flag: a node
  booted without it will never shrink the roster no matter how politely it is
  asked to stop."
  [test]
  (boolean (some #{:membership} (:faults test))))

(defn start-args
  "Mirrors docker/DockerfileLocal's entrypoint from the Kahuna repo, with paths
  and cluster wiring substituted for this node.

  `join?` starts the node with --join-existing, which makes it request
  membership from the seeds in --initial-cluster (as a Learner, auto-promoted
  to Voter once caught up) instead of booting as a static-discovery member.
  Only meaningful for a node that has *left* the roster."
  ([test node] (start-args test node false))
  ([test node join?]
   (concat
    [:--raft-nodename    (name node)
     :--raft-nodeid      (node-id test node)
     :--raft-host        (name node)
     :--raft-port        raft-port
     :--http-ports       kc/http-port
     :--https-ports      raft-port
     :--https-certificate (str dir "/certificate.pfx")
     :--initial-cluster-partitions (:partitions test 3)]
    [:--initial-cluster] (peers test node)
    [:--storage          :rocksdb
     :--storage-path     data-dir
     :--storage-revision :v1
     :--wal-storage      :rocksdb
     :--wal-path         wal-dir
     :--wal-revision     :v3
     :--raft-allow-insecure-certificate-validation]
    ;; Durability knob: with --disable-wal-sync-writes a node that is SIGKILLed
    ;; may lose acknowledged writes, which is a legitimate finding only if you
    ;; are testing that configuration on purpose. Default here is to fsync.
    (when (:disable-wal-sync-writes test)
      [:--disable-wal-sync-writes])
    ;; Persisted MVCC revision retention. The server default is 0 — keep every
    ;; revision forever — which means a snapshot read that misses the bounded
    ;; in-memory archive is always rescued from disk. That makes a snapshot
    ;; hold nearly unfalsifiable: history it failed to protect would still be
    ;; there. Setting a finite retention (and a cleanup interval short enough
    ;; that a sweep runs during the test, rather than the 300 s default that a
    ;; 300 s run never reaches) is what turns reclamation into a real force for
    ;; the floor to hold back. See src/kahuna/workload/snapshot.clj.
    (when-let [n (:revision-retention test)]
      (when (pos? n) [:--persistent-revision-retention-count n]))
    (when-let [s (:revision-cleanup-interval test)]
      [:--persistent-revision-cleanup-interval s])
    ;; Safe to set unconditionally alongside the other faults: it only fires
    ;; from StopAsync, which a SIGKILL never reaches. The :kill fault therefore
    ;; still models a crash, not a polite departure.
    (when (membership-faults? test)
      [:--graceful-leave-on-shutdown])
    (placement-args test node)
    (range-args test)
    (when (:force-compaction test) compaction-args)
    (when join?
      [:--join-existing]))))

(defn start!
  ([test node] (start! test node false))
  ([test node join?]
   (c/su
     (cu/start-daemon!
       {:chdir   dir
        :logfile logfile
        :pidfile pidfile
        :env     {:DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS 1}}
       binary
       (start-args test node join?)))))

(def process-pattern
  "pgrep -f pattern matching the server process — and deliberately NOT matching
  the kill pipeline that carries it.

  `grepkill!` expands to `pgrep -f <pattern> | xargs kill -SIG`, so the
  pipeline's own command line contains the pattern verbatim. With a plain
  \"Kahuna.Server\" the pipeline matches itself: `kill -kill` SIGKILLs its own
  xargs and exits 137 (which crashed a CI run after the history was complete
  but before analysis), and `kill -stop` would SIGSTOP the pipeline and hang.

  The bracket makes the two strings differ while matching the same process:
  the regex `Kahuna[.]Server` matches the real process's `Kahuna.Server`, but
  the literal text `Kahuna[.]Server` in the pipeline's own command line does
  not match it."
  "Kahuna[.]Server")

(defn kill-stragglers!
  "SIGKILLs any Kahuna.Server left running. Tolerates failure: the nemesis
  cheerfully kills a node that is already dead, and that must never abort a
  test that has a complete history waiting to be analyzed."
  []
  (try+
    (c/su (cu/grepkill! :kill process-pattern))
    (catch Object _ nil)))

(defn stop!
  [test node]
  (c/su (cu/stop-daemon! binary pidfile))
  (kill-stragglers!))

(def graceful-stop-timeout-s
  "Seconds to wait for a SIGTERMed node to exit before SIGKILLing it.

  Must clear the *host's* shutdown budget, not just the leave's. Kahuna sets no
  HostOptions.ShutdownTimeout, so .NET's 30 s default applies to all hosted
  services; ReplicationService.StopAsync spends up to 10 s of it inside
  LeaveCluster. A SIGKILL before that budget is exhausted truncates the leave
  and the roster never shrinks — which is what a 20 s timeout did here, and it
  looked exactly like a server-side bug."
  45)

(defn graceful-stop!
  "SIGTERMs the server and waits for it to exit, so its shutdown hook can run.

  `jepsen.control.util/stop-daemon!` cannot be used here: it sends SIGKILL
  outright, which skips StopAsync entirely — the very hook that commits
  RemoveMember. The kill -9 at the end is only a backstop for a node that hangs
  past the timeout; reaching it means the leave did not commit.

  Returns :left if the process exited on its own, :killed if it had to be
  SIGKILLed, and :not-running if there was nothing to stop."
  [_test _node]
  (c/su
    (keyword
      (c/exec :bash :-c
        (str "pid=$(cat " pidfile " 2>/dev/null); "
             "if [ -z \"$pid\" ] || ! kill -0 \"$pid\" 2>/dev/null; then "
             "  echo not-running; exit 0; fi; "
             "kill -TERM \"$pid\" 2>/dev/null; "
             "for _ in $(seq 1 " graceful-stop-timeout-s "); do "
             "  kill -0 \"$pid\" 2>/dev/null || break; sleep 1; done; "
             "if kill -0 \"$pid\" 2>/dev/null; then "
             "  kill -9 \"$pid\" 2>/dev/null; echo killed; "
             "else echo left; fi; "
             "rm -f " pidfile)))))

(defn wipe-data!
  "Deletes this node's RocksDB data and WAL, so the next start is a genuinely
  fresh member rather than one carrying a log from a membership epoch it is no
  longer part of.

  Rejoining *with* the old log is a different scenario, and not obviously a
  supported one — see FINDINGS.md. Testing it by accident would manufacture
  findings this suite could not defend."
  [_test _node]
  (c/su
    (c/exec :rm :-rf data-dir wal-dir)
    (c/exec :mkdir :-p data-dir wal-dir)))

(defn up?
  "Is this node answering API requests? Checks the status explicitly: the client
  returns a map even for error responses, so `some?` would always be true."
  [node]
  (try+
    (= 200 (:status (kc/cluster-membership node)))
    (catch Object _ false)))

(defn await-cluster!
  "Blocks until `node` serves the membership endpoint. Cluster formation is
  asynchronous (static discovery plus a Raft election per partition), so tests
  that start hammering immediately will otherwise see a burst of
  leader-not-found responses that says nothing interesting."
  [node]
  (util/await-fn (fn [] (or (up? node)
                            (throw (RuntimeException. "not up yet"))))
                 {:log-message (str "Waiting for Kahuna on " node)
                  :timeout     120000
                  :interval    1000}))

(def start-ready-timeout-ms
  "How long a restarted node gets to open its HTTP port before the nemesis
  stops waiting.

  Kept short on purpose. Whatever the nemesis waits for here it is *not*
  applying faults, so a long wait quietly rewrites the fault schedule — at 60 s
  a 300 s run managed 5 nemesis windows instead of ~20. The port opens in 1-3 s
  in practice; this only needs to cover a slow start, not a broken one."
  15000)

(defn ready?
  "Can `node` serve requests? Reads /v1/cluster/health.

  This is the signal the harness spent three wrong attempts approximating:
  `up?` answers a second after launch and means only 'listening'; a real KV
  probe conflates this node being initialised with the whole cluster being
  serviceable. The server now reports it directly — initialization complete
  *and* a serving role in the roster."
  [node]
  (try+
    (true? (:ready (kc/cluster-health node)))
    (catch Object _ false)))

(defn await-up!
  "Blocks until `node`'s HTTP port answers, up to `timeout` ms. Returns true, or
  false on timeout — never throws, because a node that fails to come back is a
  result the nemesis records, not an error that should abort a test whose
  history is still worth analysing.

  Note this waits for the port, NOT for readiness, even though `ready?` now
  exists. Readiness depends on the cluster: `IsInitialized` requires the
  partition map from the P0 leader, so a node cannot become ready while a
  partition or a majority kill is still in force. Since the nemesis calls
  `start!` with other faults still active, waiting for readiness here would
  block each start until those cleared — which is exactly what happened when
  this waited on a KV probe: a 300 s run fell from ~20 nemesis windows to 5,
  none recovered. A nemesis that is waiting is a nemesis that is not applying
  faults.

  Readiness is instead *sampled* — see `kahuna.nemesis.health` — so it is
  observed without being waited on."
  ([node] (await-up! node start-ready-timeout-ms))
  ([node timeout]
   (try+
     (util/await-fn (fn [] (or (up? node)
                               (throw (RuntimeException. "not up yet"))))
                    {:log-message (str "Waiting for Kahuna on " node)
                     :timeout     timeout
                     :interval    500})
     true
     (catch Object _ false))))

(defn membership
  "The roster as seen by `node`: {:version long :members #{endpoint} :role str},
  or nil if the node cannot be reached.

  Read from /v1/cluster/membership, which is GET-only — the roster is *changed*
  through the process lifecycle (--join-existing,
  --graceful-leave-on-shutdown), never through this endpoint."
  [node]
  (try+
    ;; `normalize` flattens the JSON body to the top level and adds :status, so
    ;; the fields are at the top, not under a :body key.
    (let [r (kc/cluster-membership node)]
      (when (= 200 (:status r))
        {:version (:membershipVersion r)
         :members (set (map :endpoint (:members r)))
         :role    (:localRole r)}))
    (catch Object _ nil)))

(defn voter?
  "Has `node` been committed to the roster as a Voter? This is the readiness
  signal membership operations need, and it is strictly stronger than `up?`:
  a node answers HTTP while it is still catching up as a Learner, and for
  several seconds at boot before the system partition even has a leader."
  [node]
  (= "Voter" (:role (membership node))))

(defn await-voter!
  "Blocks until `node` is a committed Voter. Returns true, or false on timeout —
  never throws, because a join that does not finish is a result the nemesis
  must record, not an error that aborts a test mid-history."
  ([node] (await-voter! node 90000))
  ([node timeout]
   (try+
     (util/await-fn (fn [] (or (voter? node)
                               (throw (RuntimeException. "not a voter yet"))))
                    {:log-message (str "Waiting for " node " to become a Voter")
                     :timeout     timeout
                     :interval    1000})
     true
     (catch Object _ false))))

(defn cluster-formed?
  "Does the whole cluster agree on a roster containing every node, with this
  node a Voter? Cheap precondition for a membership operation: removing a node
  from a cluster that has not finished forming tests nothing, and the departing
  node cannot commit its own removal because there is no leader to commit it."
  [test]
  (let [expected (count (:nodes test))]
    (every? (fn [node]
              (when-let [m (membership node)]
                (and (= "Voter" (:role m))
                     (= expected (count (:members m))))))
            (:nodes test))))

(defn leave!
  "Removes `node` from the cluster: stop it gracefully so its shutdown hook
  commits RemoveMember(self), then wipe its state so the next start is a fresh
  member. Runs in the caller's c/on-nodes context.

  Returns the graceful-stop outcome (:left, :killed or :not-running). Whether
  the roster actually shrank is checked by the caller against a *surviving*
  node — the departing node's own opinion is worthless, and an unverified
  :left is exactly how a no-op nemesis passes for a working one."
  [test node]
  (let [outcome (graceful-stop! test node)]
    (kill-stragglers!)
    (wipe-data! test node)
    outcome))

(defn placement
  "The committed placement table as seen by `node`, or nil if it cannot answer.
  A thin pass-through to the client so nemeses and checkers have one place to
  reach for it."
  [node]
  (try+
    (kc/cluster-placement node)
    (catch Object _ nil)))

(defn cluster-placement
  "Every node's view of the placement table: {node → placement-or-nil}.

  All of them are asked, not one: the committed map is cluster-wide, so
  disagreement between two nodes at the same generation is a finding, and a
  single-node read could never see it. Probes go out from the control node over
  HTTP, so a node that is down contributes nil rather than an exception."
  [test]
  (into (sorted-map)
        (map (fn [node] [node (placement node)]))
        (:nodes test)))

(defn ranges
  "The range-descriptor map as seen by `node`, or nil if it cannot answer. A
  thin pass-through to the client so the range nemesis and checker have one
  place to reach for it."
  [node]
  (try+
    (kc/range-map node)
    (catch Object _ nil)))

(defn cluster-ranges
  "Every node's view of the range map: {node → range-map-or-nil}.

  All of them are asked for the same reason `cluster-placement` asks all of
  them: the descriptors are replicated, so two nodes describing one descriptor
  differently is a finding a single-node read could never see. A node that is
  down contributes nil rather than an exception."
  [test]
  (into (sorted-map)
        (map (fn [node] [node (ranges node)]))
        (:nodes test)))

(defn key-space-view
  "What one node's range map says about `space`: `{:routing-mode :descriptors}`,
  or nil when the node has never heard of it."
  [view space]
  (get-in view [:key-spaces space]))

(defn key-range-live?
  "Does `node` route `space` by key range *and* have somewhere to route to?

  Both halves are needed and neither implies the other. A registered space with
  zero descriptors routes by key range with nothing underneath it, and every
  write to it throws; a space with descriptors on a node that has not yet
  reconciled its registry still hashes. Only the conjunction means the node can
  serve the space."
  [node space]
  (let [v (key-space-view (ranges node) space)]
    (boolean (and (= "KeyRange" (:routing-mode v))
                  (seq (:descriptors v))))))

(defn register-key-range!
  "Registers `space` for key-range routing on **every** node, returning
  {node → result}.

  Every node, not one, and the reason is in the server's own contract: the seed
  descriptor is a single replicated meta-partition write, but the routing-mode
  flip is node-local. Seeding via one node does converge everywhere — each node
  reconciles its registry against the descriptors it applies — but only after
  the descriptor replicates. Registering everywhere collapses that window
  instead of racing it, and each call is idempotent (`AlreadySeeded`).

  Never throws: a node that refuses or cannot be reached is recorded, and
  `await-key-range!` is what decides whether the cluster ended up usable."
  [test space]
  (into (sorted-map)
        (map (fn [node]
               [node (try+
                       (kc/register-key-range! node space {})
                       (catch Object o
                         {:success false :outcome :unreachable :reason (str o)}))]))
        (:nodes test)))

(defn await-key-range!
  "Blocks until every node routes `space` by key range with at least one
  descriptor. Returns true, or false on timeout.

  This is a precondition, not an observation, and it is worth waiting for: a
  cluster where some nodes range-route a space and others hash it serves one key
  from two partitions. Writes would then diverge silently, and the *workload's*
  checker would report the result — a linearizability violation manufactured
  entirely by the harness's own setup."
  ([test space] (await-key-range! test space 60000))
  ([test space timeout]
   (try+
     (util/await-fn
       (fn []
         (or (every? #(key-range-live? % space) (:nodes test))
             (throw (RuntimeException. "key range not established on every node"))))
       {:log-message (str "Waiting for key space " space " to be key-range routed")
        :timeout     timeout
        :interval    1000})
     true
     (catch Object _ false))))

(defn establish-key-range!
  "Puts `space` under key-range routing cluster-wide, or fails the DB setup.

  Aborting is deliberate, and it is the opposite of how this suite treats a
  mid-run failure — there, a node that does not come back is a result the
  nemesis records. Setup is different: a run that could not establish key-range
  routing cannot split a range, so its history would be labelled `-kr` while
  testing hash routing. `:jepsen.db/setup-failed` is jepsen's own signal for
  this and buys three whole teardown/setup cycles before the test gives up,
  which is a plausible fix for a cluster that merely formed slowly."
  [test space]
  (let [results (register-key-range! test space)]
    (info "key-range registration for" space ":"
          (into (sorted-map) (map (fn [[n r]] [n (or (:outcome r) :no-answer)])) results))
    (when-not (await-key-range! test space)
      (throw+ {:type         :jepsen.db/setup-failed
               :message      (str "key space " space
                                  " is not key-range routed on every node")
               :key-space    space
               :registration results
               :ranges       (cluster-ranges test)}))
    (info "key space" space "is key-range routed cluster-wide")
    true))

(defn decommission!
  "Removes `node` from the roster the way an operator would: commit the removal
  through `POST /v1/cluster/leave`, *then* stop the process and wipe its state.

  This is strictly better than the SIGTERM-and-wait leave in
  `kahuna.nemesis.membership`, and the difference is diagnostic rather than
  cosmetic. The endpoint answers with the consensus outcome, so a removal that
  did not happen says `NoLeader` or `RefusedInsufficientVoters` instead of
  looking exactly like one that did. That is what lets this run *combined* with
  `partition` and `kill`: it does not need a fully-formed cluster as a
  precondition, because it can tell afterwards whether it got one.

  The process is stopped only when the removal actually committed. A node still
  in the roster that has been killed is a `kill` fault wearing a leave's name,
  and this nemesis would then be silently doing something other than what its
  history says.

  Returns {:left bool :outcome str ...} — the leave result, plus :stopped and
  :wiped when the process was taken down. Runs in the caller's c/on-nodes
  context, but talks to the node over HTTP."
  [test node]
  (let [result (try+
                 (kc/cluster-leave! node {})
                 (catch Object o
                   {:left false :outcome :unreachable :reason (str o)}))]
    (if (:left result)
      (do (stop! test node)
          (wipe-data! test node)
          (assoc result :stopped true :wiped true))
      (assoc result :stopped false :wiped false))))

(defn join!
  "Adds `node` back: start it with --join-existing so it requests membership
  from its seeds as a Learner and is promoted once caught up. Runs in the
  caller's c/on-nodes context.

  Waits for promotion to Voter, not merely for the HTTP port to answer. A node
  answers /v1/cluster/membership within a second of starting and can spend a
  further minute as a Learner; returning at the first 200 would report a join
  as complete while the cluster was still one voter short."
  [test node]
  (start! test node true)
  (if (await-voter! node)
    :joined
    :join-timeout))

(defn db
  "A Kahuna DB. `tarball` is a path on the *control node* to the self-contained
  publish produced by scripts/build-tarball.sh."
  [tarball]
  (reify
    db/DB
    (setup! [this test node]
      (info node "installing Kahuna from" tarball)
      (c/su
        (c/exec :mkdir :-p dir data-dir wal-dir)
        (c/upload tarball "/tmp/kahuna.tar.gz")
        (c/exec :tar :xzf "/tmp/kahuna.tar.gz" :-C dir)
        (c/exec :chmod :+x binary))
      (start! test node)
      (await-cluster! node))

    (teardown! [this test node]
      (info node "tearing down Kahuna")
      (stop! test node)
      (c/su (c/exec :rm :-rf dir)))

    ;; One-time, cluster-wide setup, run on the first node after *every* node's
    ;; setup! has returned. Key-range registration has to happen here and not in
    ;; setup!: setup! runs concurrently on all nodes, and seeding a whole-space
    ;; descriptor from six nodes at once against a cluster that has not finished
    ;; electing a meta-partition leader is a race with nothing to gain.
    db/Primary
    (primaries [this test]
      [(first (:nodes test))])

    (setup-primary! [this test node]
      (when (key-ranged? test)
        (establish-key-range! test (key-space test))))

    db/LogFiles
    (log-files [this test node]
      {logfile "kahuna.log"})

    ;; Required by jepsen.nemesis.combined's :kill fault
    db/Process
    (start! [this test node]
      ;; Waits for the HTTP port, so the :start op's COMPLETION means "this
      ;; node is at least listening" rather than "the daemon was spawned".
      ;;
      ;; :port-open-ms is named for exactly what it measures and no more. It is
      ;; NOT time-to-ready — see `await-up!` for why that is not observable
      ;; from here — so nothing downstream may subtract it from recovery and
      ;; call the remainder consensus latency.
      (let [t0    (System/nanoTime)
            _     (start! test node)
            open? (await-up! node)
            ms    (long (/ (- (System/nanoTime) t0) 1e6))]
        (when-not open?
          (warn node "did not answer within" start-ready-timeout-ms "ms of starting"))
        {:started      true
         :port-open    open?
         :port-open-ms ms}))

    (kill! [this test node]
      ;; Same self-kill hazard as stop!: the nemesis happily kills a node it
      ;; already killed, and that must not crash the test.
      (kill-stragglers!)
      :killed)

    ;; Required by jepsen.nemesis.combined's :pause fault
    db/Pause
    (pause! [this test node]
      (c/su (cu/grepkill! :stop process-pattern))
      :paused)

    (resume! [this test node]
      (c/su (cu/grepkill! :cont process-pattern))
      :resumed)))
