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

(defn start-args
  "Mirrors docker/DockerfileLocal's entrypoint from the Kahuna repo, with paths
  and cluster wiring substituted for this node."
  [test node]
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
      [:--disable-wal-sync-writes])))

(defn start!
  [test node]
  (c/su
    (cu/start-daemon!
      {:chdir   dir
       :logfile logfile
       :pidfile pidfile
       :env     {:DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS 1}}
      binary
      (start-args test node))))

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
