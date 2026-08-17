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

(def fault-pairs
  "Each fault's beginning :f mapped to the :f that ends it.

  `:kill`/`:start` is jepsen.nemesis.combined's db-nemesis, `:pause`/`:resume`
  is SIGSTOP/SIGCONT, `:start-partition`/`:stop-partition` is the network,
  `:leave`/`:join` is this suite's membership nemesis, and
  `:decommission`/`:recommission` is the placement nemesis taking a node out of
  the roster and putting it back.

  The placement nemesis's `:set-rf`/`:clear-rf` are deliberately absent. They
  change a placement *target* without removing any capacity — the cluster is
  fully available throughout — so counting them as faults would close recovery
  windows that never needed to recover and drag the median toward zero.

  The range nemesis's `:split-range`/`:merge-ranges` are absent for a weaker
  reason, and it is a caveat rather than a decision: a split does quiesce its
  source range for the cutover, so a window measured across one carries that
  pause. But a quiesce is bounded and range-local, it has no ending op to pair
  with, and treating it as a fault would open a recovery window per split on a
  cluster that never lost capacity. The number this checker reports is already
  documented as an upper bound; on key-range runs it is slightly looser."
  {:kill            :start
   :pause           :resume
   :start-partition :stop-partition
   :leave           :join
   :decommission    :recommission})

(def fault-start-fs (set (keys fault-pairs)))
(def fault-end-fs   (set (vals fault-pairs)))

(def ^:private end->start
  (into {} (map (fn [[a b]] [b a]) fault-pairs)))

(defn nemesis-ops
  "The nemesis ops, in order, each tagged ::role with :invoke or :complete.

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
    credit it with time it spent under attack.

  Tagging positionally rather than by :index keeps this usable on a plain
  vector of op maps, which is what the tests pass."
  [history]
  ;; jepsen hands the checker a History; tests hand it a plain vector of op
  ;; maps. `h/history` accepts either, and the h/* filters require it.
  (->> (h/history history)
       (h/remove h/client-op?)
       (map-indexed (fn [i op] (assoc op ::role (if (even? i) :invoke :complete))))
       vec))

(defn ms
  "Jepsen records :time in nanoseconds since the start of the test."
  [nanos]
  (when nanos (long (/ nanos 1e6))))

(defn windows
  "Recovery windows: periods when NO fault is active, paired with the first
  client :ok in each.

  A window opens only when the *last* outstanding fault ends, not when any
  single fault-ending op fires. Faults overlap — under `partition,kill` the
  nemesis will heal a partition while nodes are still killed — and timing
  'recovery' from that heal measures a cluster that is still under attack. The
  first version of this checker did exactly that, and produced a confident,
  entirely unsound table of per-fault recovery latencies.

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
        nem     (nemesis-ops history)
        ;; Readiness samples from kahuna.nemesis.health, if it was enabled.
        healths (->> nem
                     (filter #(and (= :complete (::role %))
                                   (= :health (:f %))
                                   (map? (:value %))))
                     vec)
        ;; First :ok strictly inside (from, to). `to` nil means "to the end".
        ok-in   (fn [from to]
                  (->> oks
                       (drop-while #(<= (:time %) from))
                       (take-while #(or (nil? to) (< (:time %) to)))
                       first
                       :time))
        ;; First moment inside the window at which every node reported ready.
        ;; Resolution is one sample interval — see kahuna.nemesis.health.
        all-ready-in (fn [from to]
                       (->> healths
                            (drop-while #(<= (:time %) from))
                            (take-while #(or (nil? to) (< (:time %) to)))
                            (filter #(and (seq (:value %))
                                          (every? true? (vals (:value %)))))
                            first
                            :time))
        ;; Resolves an open window against the moment it closed.
        close   (fn [opened closed-at end-tag]
                  (if-let [ok-t (ok-in opened closed-at)]
                    (let [ready-t (all-ready-in opened ok-t)]
                      (cond-> {:at-ms (ms opened) :recovered? true
                               :recovered-after-ms (ms (- ok-t opened))}
                        ;; Split the window at the moment every node reported
                        ;; ready: before it the cluster was still initialising,
                        ;; after it the remaining wait is consensus.
                        ready-t
                        (assoc :init-ms      (ms (- ready-t opened))
                               :consensus-ms (ms (- ok-t ready-t)))))
                    (merge {:at-ms (ms opened) :recovered? false :end end-tag}
                           (when closed-at
                             {:window-ms (ms (- closed-at opened))}))))
        final
        (reduce
          (fn [{:keys [active opened] :as st} op]
            (let [t (:time op)
                  f (:f op)
                  r (::role op)]
              (cond
                ;; A fault begins, closing any window that was open.
                (and (= :invoke r) (contains? fault-start-fs f))
                (cond-> (assoc st :active (conj active f) :opened nil)
                  opened (update :windows conj (close opened t :next-fault)))

                ;; A fault ends. A window opens only once nothing is left
                ;; active — see the docstring.
                (and (= :complete r) (contains? fault-end-fs f))
                (let [active' (disj active (end->start f))]
                  (assoc st :active active'
                            :opened (if (seq active') opened t)))

                :else st)))
          {:active #{} :opened nil :windows []}
          nem)]
    ;; A window still open when the history ends is censored, not starved: the
    ;; test simply stopped watching. It still counts as recovered if something
    ;; succeeded inside it.
    (cond-> (:windows final)
      (:opened final) (conj (close (:opened final) nil :history)))))

(defn- percentile
  [sorted p]
  (when (seq sorted)
    (nth sorted (min (dec (count sorted))
                     (long (* p (count sorted)))))))

(defn port-open-ms
  "Per-node time from launch to the HTTP port answering, from the :start ops.

  Reported for context only. It is emphatically NOT time-to-ready, and
  :recovery-ms is NOT 'recovery minus this': Kahuna answers HTTP about a second
  after launch and then refuses every KV request for as long as it takes to
  finish initialising, with no signal a client can observe. So an unknown and
  possibly dominant share of every :recovery-ms figure below is a node that was
  listening but not yet initialised, and this number does not bound it.

  Making that separation real needs a readiness signal from the server. Filed
  against Kahuna; until it exists, treat :recovery-ms as an upper bound on
  consensus recovery and nothing more precise."
  [history]
  (->> (nemesis-ops history)
       (filter #(and (= :complete (::role %)) (= :start (:f %))))
       (mapcat (fn [op] (let [v (:value op)] (when (map? v) (vals v)))))
       (keep #(when (map? %) (:port-open-ms %)))
       sort))

(defn checker
  "A jepsen checker reporting recovery latency. Always :valid? true — see the
  namespace docstring."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [ws        (windows history)
            recovered (filter :recovered? ws)
            latencies (sort (keep :recovered-after-ms recovered))
            boots     (port-open-ms history)
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
          (assoc :starved-window-ms (sort (keep :window-ms starved)))

          ;; Context only — not subtractable from :recovery-ms. See port-open-ms.
          (seq boots)
          (assoc :port-open-ms {:median (percentile boots 0.5)
                                :max    (last boots)})

          ;; The decomposition, available only when health sampling ran. These
          ;; DO sum to :recovery-ms, unlike :port-open-ms: :init-ms is the wait
          ;; for every node to report ready, :consensus-ms is what the cluster
          ;; spent after that before serving a request.
          (seq (keep :init-ms recovered))
          (assoc :init-ms
                 (let [v (sort (keep :init-ms recovered))]
                   {:median (percentile v 0.5) :max (last v)})
                 :consensus-ms
                 (let [v (sort (keep :consensus-ms recovered))]
                   {:median (percentile v 0.5) :max (last v)})))))))
