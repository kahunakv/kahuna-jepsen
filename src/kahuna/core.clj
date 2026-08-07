(ns kahuna.core
  "Entry point. `lein run test --help` for options."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [kahuna.checker.recovery :as recovery]
            [kahuna.client :as kc]
            [kahuna.db :as kdb]
            [kahuna.nemesis.health :as health]
            [kahuna.nemesis.membership :as membership]
            [kahuna.workload.append :as append]
            [kahuna.workload.lock :as lock]
            [kahuna.workload.register :as register]
            [kahuna.workload.sequencer :as sequencer]))

(def workloads
  {:register  register/workload
   :lock      lock/workload
   :append    append/workload
   :sequencer sequencer/workload})

(def all-faults
  "Clock faults are NOT in the default set on purpose: settimeofday inside a
  container moves the shared kernel clock, which on Docker Desktop means the
  whole VM. Enable :clock only on a disposable Linux host — but do enable it
  there, because Kahuna's MVCC and lease logic ride on an HLC and that is where
  the interesting bugs live."
  #{:partition :kill :pause :membership})

(defn parse-faults [s]
  (if (= s "all")
    all-faults
    (set (map keyword (str/split s #",")))))

(defn kahuna-test
  "Builds a test map from CLI options."
  [opts]
  (let [workload-fn (workloads (:workload opts))
        workload    (workload-fn opts)
        faults      (:faults opts all-faults)
        db          (kdb/db (:tarball opts))
        nemesis-opts {:db        db
                      :nodes     (:nodes opts)
                      :faults    faults
                      :partition {:targets [:one :majority :majorities-ring]}
                      :kill      {:targets [:one :majority :all]}
                      :pause     {:targets [:one :majority]}
                      :interval  (:nemesis-interval opts 15)
                      :membership-interval (:membership-interval opts 30)
                      ;; `get` with a default, not `(:health-sampling opts)`:
                      ;; the key is present-and-nil unless --no-health-sampling
                      ;; assoc'd it, and a nil value defeats the package's own
                      ;; default, silently disabling sampling.
                      :health-sampling (get opts :health-sampling true)
                      :health-interval (:health-interval opts 2)}
        ;; :membership is ours, not jepsen.nemesis.combined's — it would ignore
        ;; the fault silently and the test would run with no membership churn
        ;; at all, which is exactly the kind of quiet no-op that reads as a
        ;; clean pass. Composed in explicitly instead.
        nemesis     (nc/compose-packages
                      (conj (nc/nemesis-packages nemesis-opts)
                            (membership/package nemesis-opts)
                            ;; Not a fault — it samples readiness so recovery
                            ;; latency can be split into initialisation and
                            ;; consensus. Composed here so its samples share the
                            ;; nemesis process and land in the same history.
                            (health/package nemesis-opts)))]
    (merge tests/noop-test
           opts
           {:name       (str "kahuna-" (name (:workload opts))
                             "-" (str/join "," (map name (sort faults))))
            :os         debian/os
            :db         db
            :client     (:client workload)
            :nemesis    (:nemesis nemesis)
            :checker    (checker/compose
                          {:perf     (checker/perf {:nemeses (:perf nemesis)})
                           :stats    (checker/stats)
                           :exceptions (checker/unhandled-exceptions)
                           ;; Measurement, not a property: always :valid? true.
                           ;; Present on every run so a history that commits
                           ;; nothing carries its own explanation.
                           :recovery (recovery/checker)
                           :workload (:checker workload)})
            :generator  (gen/phases
                          (->> (:generator workload)
                               (gen/stagger (/ (:rate opts 50)))
                               (gen/nemesis (:generator nemesis))
                               (gen/time-limit (:time-limit opts)))
                          (gen/log "Healing cluster")
                          (gen/nemesis (:final-generator nemesis))
                          (gen/log "Waiting for recovery")
                          (gen/sleep 10)
                          (gen/clients (:final-generator workload)))})))

(def cli-opts
  [["-w" "--workload NAME" "Workload to run"
    :default :register
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--tarball PATH" "Path (on the control node) to the Kahuna tarball"
    :default "target/kahuna.tar.gz"]

   [nil "--faults FAULTS" "Comma-separated nemesis faults, or 'all'"
    :default all-faults
    :parse-fn parse-faults]

   [nil "--nemesis-interval SECONDS" "Seconds between nemesis operations"
    :default 15
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--health-interval SECONDS" "Seconds between readiness samples. Bounds
                                    the resolution of the :init-ms /
                                    :consensus-ms split in the recovery
                                    checker."
    :default 2
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--no-health-sampling" "Stop sampling /v1/cluster/health. Recovery
                               latency is then reported as an upper bound only,
                               with no initialisation/consensus split."
    :default false
    :assoc-fn (fn [m _ _] (assoc m :health-sampling false))]

   [nil "--membership-interval SECONDS" "Seconds between membership operations.
                                        Longer than --nemesis-interval on
                                        purpose: a leave waits for the departing
                                        node to commit its own removal, and a
                                        join waits for a Learner to catch up and
                                        be promoted."
    :default 30
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   ["-r" "--rate HZ" "Approximate request rate per client"
    :default 50
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--partitions COUNT" "Kahuna partitions (Raft groups) per cluster"
    :default 3
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--ephemeral" "Use Ephemeral durability instead of Persistent"
    :default false
    :assoc-fn (fn [m _ _] (assoc m :durability kc/ephemeral))]

   [nil "--disable-wal-sync-writes" "Run Kahuna without WAL fsync (expect data loss on kill)"
    :default false]

   [nil "--consistency-model NAME" "Elle model to demand of the append
                                   workload: serializable (default),
                                   snapshot-isolation, strict-serializable, …"
    :default :serializable
    :parse-fn keyword]

   [nil "--locking NAME" "Transaction locking for the append workload:
                         pessimistic (default) or optimistic."
    :default :pessimistic
    :parse-fn keyword
    :validate [#{:pessimistic :optimistic} "must be pessimistic or optimistic"]]

   [nil "--key-count COUNT" "Keys in play at once (append workload)"
    :default 5
    :parse-fn read-string]

   [nil "--max-txn-length COUNT" "Micro-ops per transaction (append workload)"
    :default 4
    :parse-fn read-string]

   [nil "--lock-expires-ms MS" "Lock lease length (lock workload). Long enough
                               that a healthy holder keeps the lock, short
                               enough that a killed one frees it."
    :default 10000
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--lease-margin-ms MS" "Slack subtracted from hold windows to absorb
                               clock-rate differences between the control node
                               and the servers."
    :default 500
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   [nil "--linearizable-algorithm NAME" "Knossos search: wgl (lower memory,
                                        default) or linear."
    :default :wgl
    :parse-fn keyword
    :validate [#{:wgl :linear} "must be wgl or linear"]]

   [nil "--concurrency-per-key COUNT" "Processes hammering one register at once.
                                      Drives Knossos's search cost hardest.
                                      MUST divide --concurrency evenly, or
                                      jepsen.independent asserts at start-up."
    :default 3
    :parse-fn read-string]

   [nil "--ops-per-key COUNT" "Operations per register before rotating keys.
                              Higher values make Knossos's search exponentially
                              more expensive; 100-200 is the practical ceiling."
    :default 100
    :parse-fn read-string]

   [nil "--sequence-name NAME" "Sequence hammered by the sequencer workload.
                               One sequence on purpose: all traffic lands on a
                               single Raft partition, which is where leader
                               changes can lose or replay allocator state."
    :default "jepsen/sequencer/s1"]

   [nil "--sequence-reserve-max COUNT" "Largest run a :reserve op may request
                                       (sequencer workload)."
    :default 10
    :parse-fn read-string
    :validate [pos? "must be positive"]]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   kahuna-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
