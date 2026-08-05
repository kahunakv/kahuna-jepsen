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
            [kahuna.client :as kc]
            [kahuna.db :as kdb]
            [kahuna.workload.register :as register]))

(def workloads
  {:register register/workload})

(def all-faults
  "Clock faults are NOT in the default set on purpose: settimeofday inside a
  container moves the shared kernel clock, which on Docker Desktop means the
  whole VM. Enable :clock only on a disposable Linux host — but do enable it
  there, because Kahuna's MVCC and lease logic ride on an HLC and that is where
  the interesting bugs live."
  #{:partition :kill :pause})

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
        nemesis     (nc/nemesis-package
                      {:db        db
                       :nodes     (:nodes opts)
                       :faults    faults
                       :partition {:targets [:one :majority :majorities-ring]}
                       :kill      {:targets [:one :majority :all]}
                       :pause     {:targets [:one :majority]}
                       :interval  (:nemesis-interval opts 15)})]
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
    :parse-fn read-string]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   kahuna-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
