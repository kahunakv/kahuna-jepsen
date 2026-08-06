(ns kahuna.checker.recovery
  "Measures how long the cluster takes to serve a request again after a fault
  ends.

  ## The question this exists to answer

  Roughly one append run in ten commits *nothing* and yields
  `:empty-transaction-graph` — a verdict that reads like a pass and is not one
  (FINDINGS.md). The interesting instance was a run with no `:kill :all` at
  all: just repeated `:kill :majority` plus partitions, 761 `must-retry`
  responses, 559 connection refusals, zero commits. Nodes were up and
  answering; no leader was ever resolvable.

  That leaves two possibilities, and the history alone cannot distinguish them:

  1. Kahuna *can* form a quorum in the 11-16 s the nemesis leaves between a
     restart and the next majority kill, and something else prevented commits.
  2. Recovery is simply slower than the fault schedule, and the run never had a
     chance.

  Measuring the gap from 'fault ended' to 'first successful operation' settles
  it. If that gap routinely exceeds the nemesis interval, it is (2), the fault
  schedule is too aggressive for the cluster, and those runs are a harness
  configuration problem rather than a Kahuna bug.

  ## Why a checker and not a probe

  A probe issuing its own requests would perturb the thing it measures — extra
  load precisely during recovery, when the cluster is least able to absorb it —
  and could only ever measure runs written after it existed. This reads the
  history every run already produces, so it costs nothing, cannot distort the
  result, and can be pointed at a stored history from a run that happened
  months ago.

  ## This checker never fails a test

  It reports `:valid? true` unconditionally. It is an instrument, not a
  property: 'recovery took 30 s' is not a safety violation, and wiring it to
  fail runs would make every slow-but-correct run red. Read the numbers."
  (:require [clojure.core.reducers :as r]
            [jepsen [checker :as checker]
                    [history :as h]]))

(def fault-end-fs
  "Nemesis :fs that *end* a fault, and so open a recovery window.

  `:start` is jepsen.nemesis.combined's db-nemesis restarting killed nodes,
  `:resume` un-SIGSTOPs a paused one, `:stop-partition` heals the network, and
  `:join` is this suite's membership nemesis putting a node back in the roster."
  #{:start :resume :stop-partition :join})

(def fault-start-fs
  "Nemesis :fs that *begin* a fault, and so close any open recovery window.

  A window that closes this way never saw a successful operation — the cluster
  was hit again before it recovered. Those are the interesting ones."
  #{:kill :pause :start-partition :leave})

(defn nemesis-ops
  "The nemesis ops split into {:invokes [...] :completions [...]}, in time order.

  Nemesis invocations and their completions BOTH have :type :info, so the usual
  invoke?/ok? predicates cannot tell them apart. The nemesis is a single
  sequential process, so its ops strictly alternate: even-indexed are
  invocations, odd-indexed are completions.

  Both halves are needed, and for opposite reasons:

  * A recovery window OPENS at the completion of the fault-ending op. `:start`
    invokes when jepsen begins restarting nodes and completes when they are up;
    measuring from the invocation would charge the restart itself to recovery.
  * A recovery window CLOSES at the invocation of the next fault. That is when
    the cluster stops being left alone — measuring to the completion would
    credit it with time it spent under attack."
  [history]
  (let [ops (->> history (h/remove h/client-op?) vec)
        by  (fn [pred] (keep-indexed (fn [i op] (when (pred i) op)) ops))]
    {:invokes     (vec (by even?))
     :completions (vec (by odd?))}))

(defn ms
  "Jepsen records :time in nanoseconds since the start of the test."
  [nanos]
  (when nanos (long (/ nanos 1e6))))

(defn windows
  "Pairs each fault-ending nemesis op with the first client :ok that follows it.

  A window is closed by whichever comes first: a successful operation
  (`:recovered? true`), the next fault (`:recovered? false`), or the end of the
  history (`:recovered? false`, `:end :history`). Windows cut short by the end
  of the history are reported but excluded from the latency summary — they are
  censored observations, not fast or slow ones."
  [history]
  (let [;; jepsen hands the checker a History; tests hand it a plain vector of
        ;; op maps. `h/history` accepts either and is what the h/* filters need.
        history (h/history history)
        ;; Realised once: the history may be large and we scan it repeatedly.
        oks     (vec (->> history (h/filter h/client-op?) (h/filter h/ok?)))
        {:keys [invokes completions]} (nemesis-ops history)
        ok-time (fn [after]
                  (->> oks
                       (drop-while #(<= (:time %) after))
                       first
                       :time))
        ;; Invocations, not completions — see `nemesis-ops`.
        next-fault-time (fn [after]
                          (->> invokes
                               (drop-while #(<= (:time %) after))
                               (filter #(contains? fault-start-fs (:f %)))
                               first
                               :time))]
    (->> completions
         (filter #(contains? fault-end-fs (:f %)))
         (map (fn [op]
                (let [t      (:time op)
                      ok-t   (ok-time t)
                      next-t (next-fault-time t)]
                  (cond
                    ;; Recovered before anything else happened.
                    (and ok-t (or (nil? next-t) (< ok-t next-t)))
                    {:f (:f op) :at-ms (ms t) :recovered? true
                     :recovered-after-ms (ms (- ok-t t))}

                    ;; Hit again before a single request succeeded. This is the
                    ;; measurement that answers the open question.
                    next-t
                    {:f (:f op) :at-ms (ms t) :recovered? false :end :next-fault
                     :window-ms (ms (- next-t t))}

                    :else
                    {:f (:f op) :at-ms (ms t) :recovered? false :end :history}))))
         vec)))

(defn- percentile
  [sorted p]
  (when (seq sorted)
    (nth sorted (min (dec (count sorted))
                     (long (* p (count sorted)))))))

(defn checker
  "A jepsen checker reporting recovery latency. Always :valid? true — see the
  namespace docstring."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [ws        (windows history)
            recovered (filter :recovered? ws)
            latencies (sort (keep :recovered-after-ms recovered))
            ;; Only windows closed by a *new fault* count as failures to
            ;; recover. One closed by the end of the history is censored: the
            ;; test simply stopped watching.
            starved   (filter #(= :next-fault (:end %)) ws)]
        (cond-> {:valid?          true
                 :windows         (count ws)
                 :recovered       (count recovered)
                 ;; Windows where a fault landed before any request succeeded.
                 ;; Consistently high here means the nemesis is cycling faster
                 ;; than the cluster can recover, and runs that commit nothing
                 ;; are explained by the fault schedule.
                 :never-recovered (count starved)
                 :detail          ws}
          (seq latencies)
          (assoc :recovery-ms {:min    (first latencies)
                               :median (percentile latencies 0.5)
                               :p95    (percentile latencies 0.95)
                               :max    (last latencies)})

          (seq starved)
          (assoc :starved-window-ms (sort (keep :window-ms starved))))))))
