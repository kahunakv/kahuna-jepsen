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
            [slingshot.slingshot :refer [try+]]))

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

(defn peers
  "The --initial-cluster seed list: every node *except* this one, as host:port.
  docker/local.yml in the Kahuna repo excludes self, so we do too."
  [test node]
  (->> (:nodes test)
       (remove #(= % node))
       (map #(str (name %) ":" raft-port))))

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
    ;; Safe to set unconditionally alongside the other faults: it only fires
    ;; from StopAsync, which a SIGKILL never reaches. The :kill fault therefore
    ;; still models a crash, not a polite departure.
    (when (membership-faults? test)
      [:--graceful-leave-on-shutdown])
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

    db/LogFiles
    (log-files [this test node]
      {logfile "kahuna.log"})

    ;; Required by jepsen.nemesis.combined's :kill fault
    db/Process
    (start! [this test node]
      (start! test node)
      :started)

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
