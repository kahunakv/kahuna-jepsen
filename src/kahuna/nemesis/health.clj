(ns kahuna.nemesis.health
  "Samples every node's readiness into the history, so recovery can be split
  into 'still initialising' and 'initialised but not yet serving'.

  ## Why sampling rather than waiting

  Readiness is not a node-local property. `IsInitialized` requires the partition
  map from the P0 leader, so a node cannot become ready while a partition or a
  majority kill is still in force. Anything that *waits* for readiness inside a
  nemesis operation therefore waits out unrelated faults — and since the nemesis
  is a single sequential process, a nemesis that is waiting is a nemesis that is
  not applying faults. Doing that turned a 300 s run from ~20 nemesis windows
  into 5, none of which recovered.

  So readiness is observed on a timer and never blocked on. Each sample is one
  op in the history:

      {:f :health, :value {\"n1\" true, \"n2\" false, ...}}

  which `kahuna.checker.recovery` reads to find the moment every node became
  ready inside a recovery window.

  ## Resolution caveat

  Jepsen runs one nemesis process, so a sample cannot be taken while a fault
  operation is in progress — `:start` can block for seconds waiting on a port.
  Samples are therefore evenly spaced only when the nemesis is idle, and a
  readiness transition is located to within one sample interval *plus* the
  duration of any fault op that happened to be running. Treat the derived
  initialisation times as lower-bounded by the sample interval, not exact."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as n]]
            [kahuna.db :as kdb]))

(defn nemesis
  "A nemesis whose only operation samples /v1/cluster/health on every node.

  Probes are issued from the control node over HTTP rather than through
  `c/on-nodes`, so a sample costs one round trip per node and does not depend on
  SSH to a node that may be down."
  []
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc op :value
             (into (sorted-map)
                   (map (fn [node] [node (kdb/ready? node)]))
                   (:nodes test))))

    (teardown! [this test])

    n/Reflection
    (fs [this] #{:health})))

(defn package
  "A health-sampling package, shaped for `nc/compose-packages`.

  Returns the no-op package when sampling is disabled. Enabled by default: the
  samples are what make `:recovery-ms` decomposable, and a run without them can
  only report an upper bound."
  [opts]
  (if-not (:health-sampling opts true)
    {:generator nil :final-generator nil :nemesis nil :perf #{}}
    (let [interval (:health-interval opts 2)]
      {:generator (->> (repeat {:type :info, :f :health})
                       (gen/stagger interval))
       ;; No final generator: this observes, it never needs to put anything back.
       :final-generator nil
       :nemesis   (nemesis)
       ;; Deliberately absent from :perf. These are not faults, and drawing them
       ;; on the latency plot would bury the faults that matter under a stripe
       ;; every couple of seconds.
       :perf      #{}})))
