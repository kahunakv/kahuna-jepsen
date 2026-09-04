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
            [kahuna.checker.placement :as placement-checker]
            [kahuna.checker.range :as range-checker]
            [kahuna.checker.recovery :as recovery]
            [kahuna.client :as kc]
            [kahuna.db :as kdb]
            [kahuna.nemesis.health :as health]
            [kahuna.nemesis.membership :as membership]
            [kahuna.nemesis.placement :as placement]
            [kahuna.nemesis.range :as range-nemesis]
            [kahuna.workload.append :as append]
            [kahuna.workload.lock :as lock]
            [kahuna.workload.register :as register]
            [kahuna.workload.sequencer :as sequencer]
            [kahuna.workload.snapshot :as snapshot]))

(def workloads
  {:register  register/workload
   :lock      lock/workload
   :append    append/workload
   :sequencer sequencer/workload
   :snapshot  snapshot/workload})

(def all-faults
  "Clock faults are NOT in the default set on purpose: settimeofday inside a
  container moves the shared kernel clock, which on Docker Desktop means the
  whole VM. Enable :clock only on a disposable Linux host — but do enable it
  there, because Kahuna's MVCC and lease logic ride on an HLC and that is where
  the interesting bugs live.

  :placement is in the default set but is inert without --replication-factor:
  at RF 0 every voter hosts every range, there are no replica sets to move, and
  the fault would add a name to the test without adding a fault. See
  kahuna.nemesis.placement.

  :range is the same shape one level up — inert without --key-range, because a
  hash-routed key space has no range descriptors to split. See
  kahuna.nemesis.range."
  #{:partition :kill :pause :membership :placement :range})

(defn parse-faults
  "'all' is every fault above; 'none' is the fault-free control. 'none' is
  spelled out rather than accepting an empty string for the same reason
  --require-placement-evidence does: running with no nemesis at all is a
  decision worth seeing on the command line, not something a stray comma
  should produce."
  [s]
  (case s
    "all"  all-faults
    "none" #{}
    (set (map keyword (str/split s #",")))))

(def kill-targets
  "What `:kill` may aim at. jepsen.nemesis.combined also accepts :primaries,
  which is deliberately absent from the default: Kahuna spreads leadership
  across partitions, so \"the primaries\" is most of the cluster and the fault
  degenerates into :all."
  #{:one :minority :majority :all})

(def default-kill-targets [:one :majority :all])

(defn parse-kill-targets [s]
  (mapv keyword (str/split s #",")))

(defn parse-evidence
  "Parses --require-placement-evidence and --require-range-evidence. 'none' is
  spelled out rather than accepting an empty string: switching a vacuity gate
  off is a decision worth seeing on the command line, not something a stray
  comma should do."
  [s]
  (if (= s "none")
    #{}
    (set (map keyword (str/split s #",")))))

(defn kahuna-test
  "Builds a test map from CLI options."
  [opts]
  (let [workload-fn (workloads (:workload opts))
        workload    (workload-fn opts)
        requested   (set (:faults opts all-faults))
        ;; The workload names the key space it stores keys in; a workload whose
        ;; state is locks or sequences names none, and cannot be range-routed.
        ;; Asking for --key-range there is a mistake worth saying out loud
        ;; rather than silently honouring half of.
        key-space   (:key-space workload)
        key-range?  (boolean (and (:key-range opts) key-space))
        _           (when (and (:key-range opts) (not key-range?))
                      (info "--key-range ignored:" (name (:workload opts))
                            "stores no key/value key space to route"))
        ;; :placement needs replica sets to move, and at RF 0 there are none.
        ;; :range needs range descriptors, and a hash-routed key space has none.
        ;; Dropping them here rather than letting the packages no-op keeps the
        ;; test's *name* honest: store/ is keyed by that string, and a run
        ;; called "…-placement" that applied no placement fault is the kind of
        ;; label this suite has already been misled by once.
        faults      (cond-> requested
                      (not (pos? (:replication-factor opts 0))) (disj :placement)
                      (not key-range?)                          (disj :range))
        _           (when (and (contains? requested :placement)
                               (not (contains? faults :placement)))
                      (info "placement fault dropped: it needs --replication-factor"))
        _           (when (and (contains? requested :range)
                               (not (contains? faults :range)))
                      (info "range fault dropped: it needs --key-range"))
        db          (kdb/db (:tarball opts))
        nemesis-opts {:db        db
                      :nodes     (:nodes opts)
                      :faults    faults
                      :partition {:targets [:one :majority :majorities-ring]}
                      :kill      {:targets (:kill-targets opts default-kill-targets)}
                      :pause     {:targets [:one :majority]}
                      :interval  (:nemesis-interval opts 15)
                      :membership-interval (:membership-interval opts 30)
                      ;; `get` with a default, not `(:health-sampling opts)`:
                      ;; the key is present-and-nil unless --no-health-sampling
                      ;; assoc'd it, and a nil value defeats the package's own
                      ;; default, silently disabling sampling.
                      :health-sampling (get opts :health-sampling true)
                      :health-interval (:health-interval opts 2)
                      ;; Placement knobs. The nemesis and the sampler both key
                      ;; off :replication-factor and disable themselves at 0, so
                      ;; passing these through unconditionally leaves an RF-0 run
                      ;; exactly as it was.
                      :replication-factor        (:replication-factor opts 0)
                      :placement-interval        (:placement-interval opts 30)
                      :placement-nodes-out       (:placement-nodes-out opts 1)
                      :placement-sampling        (get opts :placement-sampling true)
                      :placement-sample-interval (:placement-sample-interval opts 3)
                      ;; Range knobs. Both the nemesis and its sampler refuse to
                      ;; run without a key-range-routed key space, so an
                      ;; unregistered run is left exactly as it was.
                      :key-range             key-range?
                      :key-space             key-space
                      :range-interval        (:range-interval opts 20)
                      :range-sampling        (get opts :range-sampling true)
                      :range-sample-interval (:range-sample-interval opts 3)}
        ;; :membership and :placement are ours, not jepsen.nemesis.combined's —
        ;; it would ignore those faults silently and the test would run with no
        ;; roster or replica churn at all, which is exactly the kind of quiet
        ;; no-op that reads as a clean pass. Composed in explicitly instead.
        nemesis     (nc/compose-packages
                      (conj (nc/nemesis-packages nemesis-opts)
                            (membership/package nemesis-opts)
                            (placement/package nemesis-opts)
                            (range-nemesis/package nemesis-opts)
                            ;; Not a fault — it samples readiness so recovery
                            ;; latency can be split into initialisation and
                            ;; consensus. Composed here so its samples share the
                            ;; nemesis process and land in the same history.
                            (health/package nemesis-opts)
                            ;; Likewise an observer, not a fault: the committed
                            ;; placement table, read from every node. A Learner
                            ;; exists for a few seconds between the add and the
                            ;; promotion, so a run that does not sample cannot
                            ;; show a move happened at all.
                            (placement/sampler-package nemesis-opts)
                            ;; Likewise an observer: every node's view of the
                            ;; range map. Coverage — that the descriptors tile
                            ;; the key space with no gap and no overlap — is a
                            ;; property of a whole view at one instant, and the
                            ;; window a split puts it at risk in is milliseconds
                            ;; wide, so a final read would never see it.
                            (range-nemesis/sampler-package nemesis-opts)))]
    (merge tests/noop-test
           opts
           {;; The replication factor is in the name because it changes what the
            ;; run means, not just how it is configured: an rf3 result says
            ;; nothing about full replication and vice versa, and store/ is
            ;; keyed by this string.
            :name       (str "kahuna-" (name (:workload opts))
                             ;; A fault-free control is named for what it is.
                             ;; The alternative is a trailing dash, and store/
                             ;; is keyed by this string.
                             "-" (if (seq faults)
                                   (str/join "," (map name (sort faults)))
                                   "nofaults")
                             (when (pos? (:replication-factor opts 0))
                               (str "-rf" (:replication-factor opts)))
                             ;; Same reasoning as the factor: key-range routing
                             ;; changes what the run means, not just how it is
                             ;; configured. A hash-routed result says nothing
                             ;; about ranges and vice versa.
                             (when key-range? "-kr"))
            ;; The DB's one-time setup registers this space for key-range
            ;; routing, and the range nemesis and checker read it from here.
            ;; nil for a workload that has none, which turns all of it off.
            :key-space  key-space
            :key-range  key-range?
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
                           ;; Unlike :recovery this one CAN fail, and can also
                           ;; return :unknown to refuse a vacuous pass. At RF 0
                           ;; it makes no claim at all.
                           :placement (placement-checker/checker)
                           ;; Also refuses a vacuous pass, one level up: a run
                           ;; whose key space never left one descriptor split
                           ;; nothing. Makes no claim without --key-range.
                           :range     (range-checker/checker)
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

   [nil "--faults FAULTS" "Comma-separated nemesis faults, 'all', or 'none' for
                          a fault-free control run"
    :default all-faults
    :parse-fn parse-faults]

   [nil "--kill-targets TARGETS" "Comma-separated targets the :kill fault may
                                 aim at: one, minority, majority, all. Drop
                                 'all' for workloads whose state lives in the
                                 cluster rather than in the client — a
                                 full-cluster kill destroys every live snapshot
                                 hold, so the workload spends the run
                                 re-acquiring instead of measuring."
    :default default-kill-targets
    :parse-fn parse-kill-targets
    :validate [#(and (seq %) (every? kill-targets %))
               (str "must be a comma-separated subset of "
                    (str/join ", " (map name (sort kill-targets))))]]

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

   ;; ── Replication factor / placement ──────────────────────────────────────
   ;;
   ;; All of these are inert at the default of 0, which is full replication and
   ;; must stay byte-identical to the pre-placement behaviour. Turning the
   ;; factor on is what enables the placement fault, the placement sampler and
   ;; the placement checker's claims.

   [nil "--replication-factor N" "Voter replicas per partition range. 0 (the
                                 default) is full replication: every voter hosts
                                 every range. 3 is the standard placed profile;
                                 1 is the highest-signal cheap one, because with
                                 a single replica per range every placement bug
                                 is data loss rather than degradation. Prefer
                                 odd values — an even factor tolerates no more
                                 failures than the odd one below it."
    :default 0
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   [nil "--no-rebalancer" "Leave initial placement in force but plan no ongoing
                          moves. Initial placement still applies; nothing
                          repairs an under-replicated range, and a per-range
                          override changes the target while nothing converges to
                          it. Useful to prove a finding needs the rebalancer,
                          and nearly useless otherwise."
    :default false
    :assoc-fn (fn [m _ _] (assoc m :rebalancer false))]

   [nil "--zones COUNT" "Spread nodes over this many zones (0 = none), assigned
                        round-robin. With zones set, the planner prefers putting
                        a range's replicas in distinct zones, so a zone outage
                        does not take out a whole quorum."
    :default 0
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   [nil "--placement-pass-interval MS" "How often the P0 leader runs a placement
                                       pass — driving in-flight transitions and
                                       planning moves. A relocation costs about
                                       three passes plus a trim, so this bounds
                                       convergence speed. Server default 5000;
                                       0 disables the timer and leaves only
                                       commit-driven passes."
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   [nil "--max-replica-moves-per-pass N" "New replica moves the placement
                                         controller may start per pass — the
                                         blast radius of a bad plan. Keep it at
                                         least repairs + transfers, or it binds
                                         first and starves repair behind
                                         cosmetic balancing."
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--max-concurrent-replica-transfers N" "Ranges allowed an in-flight
                                                Learner/Removing replica from a
                                                *balance* move at once. Raise it
                                                to converge faster at the cost of
                                                more concurrent backfill."
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--max-concurrent-replica-repairs N" "In-flight *repair* moves allowed at
                                              once — re-replicating
                                              under-replicated ranges and
                                              shedding replicas stranded on
                                              evicted nodes. Budgeted apart from
                                              transfers so restoring durability
                                              after a node loss is not
                                              serialized behind skew-spreading."
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--decommission-drain-timeout MS" "How long a graceful leave waits for
                                          this node's replicas to be evacuated
                                          onto survivors before giving up. On
                                          expiry the node is restored to a voter
                                          and the leave reports DrainTimedOut;
                                          replicas already moved stay moved, so a
                                          retry resumes the drain. Server default
                                          120000, which is a quarter of a 900 s
                                          run — the nemesis blocks on the call."
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--replica-count-deadband N" "Per-node replica imbalance tolerated above
                                      the even spread before balancing moves are
                                      planned. Under-replicated ranges bypass
                                      it."
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   [nil "--force-compaction" "Compact the Raft WAL aggressively and collapse the
                             PITR window, so a replica added mid-run starts
                             *below* the compaction floor and must be seeded by
                             a whole-partition snapshot instead of by log
                             backfill. Without this the defaults protect an hour
                             and a half of WAL, no Jepsen run ever compacts
                             anything, and the snapshot-seeding path this suite
                             exists to attack is never entered."
    :default false
    :assoc-fn (fn [m _ _] (assoc m :force-compaction true))]

   [nil "--placement-interval SECONDS" "Seconds between placement-nemesis
                                       operations. Longer than
                                       --nemesis-interval on purpose: a replica
                                       move is an add, a seed, a promotion that
                                       waits out the learner stable window, and
                                       two more map commits."
    :default 30
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--placement-nodes-out N" "How many nodes the placement nemesis may hold
                                  out of the roster at once. 1 is leave/rejoin
                                  churn; higher walks the roster down toward the
                                  replication factor, where the planner must
                                  degrade to fuller replication rather than lose
                                  ranges. Clamped so at least max(3, RF) nodes
                                  remain.

                                  0 turns roster churn off entirely: the nemesis
                                  works replication-factor overrides only, which
                                  still forces the planner to add and retire
                                  replicas. That is the setting for a profile
                                  where decommission is not a supported
                                  operation — RF 1, where a range has a single
                                  voter and a drain has nowhere to hand it off."
    :default 1
    :parse-fn read-string
    :validate [(complement neg?) "must be zero or positive"]]

   [nil "--no-placement-sampling" "Stop reading /v1/cluster/placement on a timer.
                                  The placement checker then cannot see a
                                  Learner or attribute a move to a partition,
                                  and falls back to log evidence alone."
    :default false
    :assoc-fn (fn [m _ _] (assoc m :placement-sampling false))]

   [nil "--placement-sample-interval SECONDS" "Seconds between placement samples.
                                              Bounds what can be seen: a Learner
                                              is promoted after ~3 s, so sampling
                                              slower than that observes moves
                                              only after the fact."
    :default 3
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--require-placement-evidence KINDS" "Comma-separated placement activity
                                             a run must demonstrate before its
                                             pass counts: transfer, move,
                                             seeding, purge, split (or 'none').
                                             A run that
                                             triggered none of the machinery
                                             under test is a vacuous pass, so
                                             the checker returns :unknown rather
                                             than success. Ignored unless the
                                             run carries a fault that could move
                                             a replica (placement, kill,
                                             membership) — a partition-only run
                                             at a replication factor is expected
                                             to move nothing. 'split' is not a
                                             default: it needs --key-range, and
                                             is gated by --require-range-evidence
                                             instead, which measures it from the
                                             range map rather than a log line.
                                             'transfer' is not a default either:
                                             it asks only that a move *started*,
                                             and is for a profile where a
                                             completed move is not reliably
                                             producible."
    :default placement-checker/default-required-evidence
    :parse-fn parse-evidence
    :validate [#(every? (set placement-checker/evidence-kinds) %)
               (str "must be 'none' or a comma-separated subset of "
                    (str/join ", " (map name placement-checker/evidence-kinds)))]]

   ;; ── Key-range routing ───────────────────────────────────────────────────
   ;;
   ;; Inert by default. Without --key-range the workload's key space is
   ;; hash-routed exactly as it was before any of this existed: no registration
   ;; happens, the range fault is stripped from the run's name, the sampler does
   ;; not run and the range checker makes no claim.

   [nil "--key-range" "Register the workload's key space for key-range routing
                       before the run starts, and enable the :range fault.
                       Keys are then served by ordered range descriptors — one
                       Raft partition each — instead of by hash, which is what
                       makes a split reachable at all. Registration is a
                       precondition, not a fault: if it cannot be established on
                       every node the DB setup fails rather than letting the run
                       proceed hash-routed under a name that says otherwise.
                       Ignored for workloads with no key/value key space (lock,
                       sequencer)."
    :default false
    :assoc-fn (fn [m _ _] (assoc m :key-range true))]

   [nil "--range-interval SECONDS" "Seconds between range-nemesis operations.
                                   Longer than --nemesis-interval on purpose: a
                                   split creates a partition, quiesces the
                                   source, copies the upper half through the
                                   destination's Raft log and commits a cutover.
                                   Firing faster than that collects
                                   ConcurrentSplit."
    :default 20
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--range-scan-limit COUNT" "Keys the nemesis scans from a range before
                                   picking its median as the split key. The
                                   median of two or more keys provably leaves
                                   both halves non-empty, which is what the
                                   server refuses a split for; a larger page
                                   only bisects more evenly."
    :default 64
    :parse-fn read-string
    :validate [#(< 1 %) "must be greater than 1"]]

   [nil "--no-range-sampling" "Stop reading /v1/ranges on a timer. The range
                              checker can then no longer see coverage at all —
                              a gap or an overlap between two descriptors is
                              only visible in a whole view at one instant — and
                              falls back to the split outcomes in the history."
    :default false
    :assoc-fn (fn [m _ _] (assoc m :range-sampling false))]

   [nil "--range-sample-interval SECONDS" "Seconds between range-map samples.
                                          Bounds what can be seen: the window in
                                          which a split has committed one half of
                                          the cutover and not the other is
                                          milliseconds wide."
    :default 3
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--require-range-evidence KINDS" "Comma-separated range activity a run
                                         must demonstrate before its pass
                                         counts: split, merge (or 'none'). A run
                                         whose key space never left one
                                         descriptor exercised no split
                                         transaction and no cutover, so the
                                         checker returns :unknown rather than
                                         success. 'merge' is not a default: a
                                         merge pass only folds ranges the
                                         running policy considers under-sized,
                                         so requiring one would make the verdict
                                         depend on how much the workload wrote."
    :default range-checker/default-required-evidence
    :parse-fn parse-evidence
    :validate [#(every? (set range-checker/evidence-kinds) %)
               (str "must be 'none' or a comma-separated subset of "
                    (str/join ", " (map name range-checker/evidence-kinds)))]]

   ;; The server's own split/merge policy. Rendered on the command line only
   ;; when set, so an unset run gets the server defaults byte for byte. The
   ;; nemesis needs none of these — it forces splits through the admin endpoint,
   ;; which is deterministic — but a profile that wants to exercise the
   ;; *automatic* splitter has to be able to reach them, and before they existed
   ;; nothing a command line could say made a split happen inside a 300 s run.

   [nil "--range-split-threshold COUNT" "Sampled keys above which a range splits
                                        automatically. 0 disables count-based
                                        auto-split entirely — the checker is not
                                        even started. Server default 1000, which
                                        no run this suite launches reaches."
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   [nil "--range-split-min-range-size COUNT" "Keys each half must hold for an
                                             *automatic* split to proceed. Does
                                             not gate the forced split the
                                             nemesis issues, which is refused
                                             only if a half would be empty."
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   [nil "--range-merge-min-size COUNT" "Keys below which two adjacent ranges
                                       become eligible to be folded back
                                       together. 0 disables auto-merge."
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   [nil "--range-collection-interval SECONDS" "Seconds between split/merge
                                              sampling passes. Shared with the
                                              key-value collector, prepared-intent
                                              recovery and range-lock renewal —
                                              whose lease is twice this — so
                                              lowering it to make auto-splits
                                              fire sooner also shortens those.
                                              The server floors it at the
                                              phase-two commit timeout — 5 s,
                                              which has no flag of its own — and
                                              refuses to start below it, so 5 is
                                              the lowest usable value."
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

   [nil "--revision-retention COUNT" "Persisted MVCC revisions to keep per key
                                     (0 = the server default, keep forever).
                                     Set this to give the snapshot workload
                                     something to hold back: with unlimited
                                     retention a snapshot hold cannot be shown
                                     to have failed, because history it did not
                                     protect survives on disk anyway."
    :parse-fn read-string
    :validate [(complement neg?) "must be non-negative"]]

   [nil "--revision-cleanup-interval SECONDS" "Minimum gap between persistent
                                              revision cleanup sweeps. The
                                              server default is 300 s, which a
                                              300 s run may never reach."
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--snapshot-lease-ms MS" "Snapshot-hold lease (snapshot workload).
                                 Defaults to the time limit plus 300 s, so a
                                 lapsed lease never truncates the protection
                                 windows the checker reasons over. Lower it
                                 only to test lease expiry on purpose."
    :parse-fn read-string
    :validate [pos? "must be positive"]]

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
