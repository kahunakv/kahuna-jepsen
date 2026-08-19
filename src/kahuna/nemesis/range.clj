(ns kahuna.nemesis.range
  "Range nemesis: splits and merges the workload's key space while the workload
  runs, and samples the range map so a checker can see what the boundaries did.

  ## Why this fault exists at all

  A key space under key-range routing is served by a set of half-open range
  descriptors, one Raft partition each. Splitting one is a multi-step
  transaction — allocate a destination partition, create it, quiesce the source
  range, copy the upper half through the destination's log, commit the cutover —
  and none of it runs unless something asks for a split. `partition`, `kill`,
  `pause` and even the placement fault never do. A run that never split a range
  says nothing about whether splitting one is safe.

  Until recently no run *could*: the thresholds had no server flag, the
  force-split primitive was an internal test seam, and the range map had no read
  surface, so the placement checker deliberately left `split` out of its default
  evidence — no run this harness could launch was able to produce one. All four
  of those are now reachable from the command line, which is what this namespace
  is here to use.

  ## Why the split key comes from a live scan

  The server refuses a split that would leave either half empty
  (`BelowMinRangeSize`), and refuses one whose key falls outside the covering
  range (`InvalidSplitKey`). A nemesis that guessed at keys would spend a run
  collecting refusals and report a vacuous pass with a straight face.

  So the split key is *measured*, not guessed: scan the chosen descriptor's own
  bounds for a page of live keys and cut at the page's median. The median of two
  or more sorted keys is strictly greater than the first and no greater than the
  last, so the left half provably holds a key, the right half holds the median
  itself, and both bounds checks pass by construction. A page that yields fewer
  than two keys is reported as a skip — the range is genuinely too small to
  split, which is a fact about the run rather than a failure.

  ## Retrying a split is not always safe, and the server says which

  `NotLeader` means the node did not attempt anything, so trying the next node
  is free. Everything else must stop there. `TransferFailed`, `QuiesceFailed`,
  `CutoverFailed` and `ConcurrentSplit` come back with `determinate` false: the
  split transaction was already under way and the map may still change after the
  call returns. Issuing a second split against another node in that state means
  two cutovers racing on one range, and the resulting map would be this
  nemesis's doing rather than Kahuna's. The rule this file follows is: hunt for
  the leader, then take the first real answer, whatever it is.

  ## …but abandoning every `TransferFailed` starves the run of splits

  That rule was too blunt in one direction. Kahuna's `cfbb55d2` fix changed what
  `TransferFailed` means: `ExportRangeAsync` used to read a refused scan page
  (`MustRetry` / `WaitingForReplication`) as \"no data\" and emit a clean
  terminal sentinel — a silently truncated copy reported as a *successful*
  split — and it now fails the export loudly instead. Under a write-heavy
  workload the refusals are routine, so a nemesis that gives up on the first one
  can spend a whole run without completing a single split. Run 32206164668 did
  exactly that: 11 attempts, 8 `TransferFailed`, 0 succeeded, and the range
  checker correctly refused to call it a pass.

  So a failed attempt is retried — but only when the first one provably left
  nothing behind, which is knowable two ways. Either the server says so
  (`determinate` true is the contract's final answer), or the range map is
  unchanged across the attempt, which is the re-read `split-range!`'s own
  docstring prescribes for an indeterminate answer. If the map moved, the split
  may have landed and the attempt is not reissued. The conservative case is
  still the default: no proof, no retry.

  ## Sampling, and why it is separate

  `sampler-package` records every node's view of the range map on a timer. It is
  not a fault. Descriptors are replicated, so two nodes describing one descriptor
  at one generation differently is a finding no single-node read could ever
  surface — and the coverage property (the descriptors tile the key space with
  no gap and no overlap) is only checkable against a whole view at one instant.
  See `kahuna.checker.range`."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [generator :as gen]
                    [nemesis :as n]]
            [kahuna.client :as kc]
            [kahuna.db :as kdb]))

(defmacro ^:private attempt
  "Evaluates `body`, returning `fallback` on any throwable. A nemesis that dies
  because one node refused a connection takes the whole run's history with it."
  [body fallback]
  `(try ~body (catch Throwable _# ~fallback)))

(defn- any-range-map
  "The range map from whichever node answers first. Enough for *choosing* a
  descriptor to act on; never enough for *checking* anything, because one node's
  view cannot show two nodes disagreeing."
  [test]
  (some kdb/ranges (shuffle (vec (:nodes test)))))

;; ---------------------------------------------------------------------------
;; Choosing where to cut
;; ---------------------------------------------------------------------------

(defn median-key
  "The middle key of a page of live keys, or nil when the page cannot yield a
  split point.

  Sorted and de-duplicated first: the scan returns keys in order already, but
  the guarantee this function makes has to rest on something the caller can see.
  With `n` distinct keys the index is `(quot n 2)`, which for n ≥ 2 is at least
  1 — so the result is strictly greater than the smallest key scanned and less
  than or equal to the largest. That is exactly the condition the server checks:
  the left half holds at least the first key, the right half holds the median
  itself, and the key falls strictly inside the covering range.

  Fewer than two distinct keys means there is nowhere to cut that leaves both
  halves non-empty, and nil says so rather than returning a bound."
  [ks]
  (let [v (vec (sort (distinct (remove nil? ks))))]
    (when (< 1 (count v))
      (nth v (quot (count v) 2)))))

(defn- split-candidate
  "Finds a split key inside `descriptor` by scanning its own bounds for live
  keys. Returns `{:split-key k :scanned n :scanned-from node}`, or `{:error …}`.

  The descriptor's nil bounds mean ∓infinity within the key space and are passed
  through unchanged — the scan's start/end are optional in exactly the same way,
  so a whole-space descriptor needs no special case."
  [test space descriptor limit]
  (let [node (rand-nth (vec (:nodes test)))
        r    (attempt (kc/scan-range node space (:start descriptor) (:end descriptor)
                                     limit {})
                      {:type :unreachable :keys []})]
    (cond
      (not= :get (:type r))
      {:error (:type r) :scanned-from node}

      :else
      (if-let [k (median-key (:keys r))]
        {:split-key k :scanned (count (:keys r)) :scanned-from node}
        {:error :too-few-keys :scanned (count (:keys r)) :scanned-from node}))))

;; ---------------------------------------------------------------------------
;; Operations
;; ---------------------------------------------------------------------------

(def ^:private not-attempted
  "Answers that mean the node did nothing, so another node may be tried.

  `NotLeader` is the server saying so explicitly; `:unreachable` is this
  harness's own name for a call that never produced a body. Every other status —
  including the indeterminate ones — means a split transaction may have started,
  and moving on to the next node would race it."
  #{"NotLeader" :unreachable})

(def ^:private retryable-outcomes
  "Failures worth a second attempt, *if* it can be shown the first left nothing
  behind. See `retry-safe?` — membership here is necessary, never sufficient.

  `TransferFailed` is the copy phase refusing a scan page rather than silently
  truncating it, which Kahuna's `cfbb55d2` made a loud, retryable failure. Under
  a write-heavy workload it is routine, and abandoning the attempt on it costs a
  run every split it would otherwise have completed."
  #{"TransferFailed"})

(def ^:private max-attempts
  "Total tries per operation, including the first. Three is enough to ride out
  the transient contention `TransferFailed` reports without letting one nemesis
  op eat a meaningful slice of the interval between faults."
  3)

(defn- retry-safe?
  "Whether a failed attempt may be reissued, given the range map digest either
  side of it.

  Two independent proofs that the first attempt landed nothing. The server
  saying so — `determinate` true is the contract's 'this is the final answer' —
  or the map being unchanged across the call, which is the re-read that
  `kc/split-range!`'s docstring prescribes for an indeterminate answer. Absent
  either, the split may still be in flight and a second one would race it, so
  this returns false: the harness must not be the thing that broke the map."
  [r before after]
  (boolean (and (retryable-outcomes (:outcome r))
                (or (:determinate r) (= before after)))))

(defn- hunt!
  "Calls `call!` on endpoints in turn until one does more than refuse for lack
  of leadership.

  Returns the accepting node's answer with `:attempted true`, or
  `{:attempted false}` when every node refused. 'Nobody would attempt it' and
  'somebody attempted it and it failed' are different findings and this must not
  collapse them."
  [test call!]
  (loop [[node & more] (shuffle (vec (:nodes test)))
         refusals      []]
    (if-not node
      {:attempted false :refusals refusals}
      (let [r (attempt (call! node)
                       {:success false :determinate false :outcome :unreachable})]
        (if (not-attempted (:outcome r))
          (recur more (conj refusals [node (:outcome r)]))
          (assoc r :attempted true :accepted-by node :refusals refusals))))))

(defn- with-retry
  "Runs a leader-hunt, retrying a provably-clean failure up to `max-attempts`.

  `map-digest` is read either side of every attempt, so a retry is only issued
  against a map that has not moved. Records the abandoned attempts under
  `:retries` — a run that needed three tries to split is a different fact from
  one that split first time, and the history should say which."
  [test call! map-digest]
  (loop [n 1, retries []]
    (let [before (map-digest)
          r      (hunt! test call!)
          after  (map-digest)]
      (if (or (:success r)
              (not (:attempted r))
              (>= n max-attempts)
              (not (retry-safe? r before after)))
        (cond-> r (seq retries) (assoc :retries retries))
        (do (warn "range: retrying after" (:outcome r) "— attempt" n "of" max-attempts)
            ;; The contention this reports is transient by nature; retrying into
            ;; the same write storm immediately would just spend the budget.
            (Thread/sleep (* 1000 n))
            (recur (inc n) (conj retries [(:accepted-by r) (:outcome r)])))))))

(defn- split!
  "Splits `space` at `split-key`, hunting for the leader and retrying a
  provably-clean failure."
  [test space split-key map-digest]
  (with-retry test #(kc/split-range! % space split-key {}) map-digest))

(defn- merge!
  "Runs the merge pass. Same leader-hunt, stopping rule and retry proof as
  `split!` — a merge copies through the destination's log too, so it refuses a
  contended scan page the same way."
  [test map-digest]
  (with-retry test #(kc/merge-ranges! % {}) map-digest))

(defn digest
  "A compact summary of one key space's descriptors, small enough to sit in the
  history on every operation: the ordered `[start end partition generation]`
  rows.

  Recorded before and after each fault so a run carries its own evidence of what
  the boundaries were when the op was aimed. The checker reasons over the
  sampler's views, not these — one node's opinion cannot show a disagreement —
  but a transition in the history is what makes a single op readable against the
  server logs."
  [view]
  (when view
    (mapv (juxt :start :end :partition :generation) (:descriptors view))))

(defn nemesis
  "The range nemesis. Reads the key space from the test map, so a workload whose
  state is not key/value keys (locks, sequences) is skipped rather than
  guessed at."
  [_opts]
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [space  (kdb/key-space test)
            limit  (:range-scan-limit test 64)
            view   (fn [] (kdb/key-space-view (any-range-map test) space))
            ;; Read once and use the same view to choose a descriptor and to
            ;; record what the map looked like when the op was aimed. Reading
            ;; twice would let the history claim the op targeted a layout it did
            ;; not target.
            v0     (view)
            before (digest v0)
            ;; Read fresh on every call: `with-retry` compares this either side
            ;; of an attempt to decide whether reissuing it is safe, so a cached
            ;; value would make every comparison trivially equal and turn the
            ;; safety check into a rubber stamp.
            digest-now (fn [] (digest (view)))]
        (letfn [(done [value]
                  (assoc op :value (assoc value
                                          :ranges-before before
                                          :ranges-after  (digest (view)))))]
          (case (:f op)
            :split-range
            (if-not space
              (done {:skipped :no-key-space})
              ;; This view can be stale by the time the server acts on it: an
              ;; auto-split may have cut the chosen descriptor already. That is
              ;; harmless — the server re-reads the map under its split lock and
              ;; splits whichever descriptor covers the key — except in one
              ;; narrow case, where the concurrent cut landed on exactly the key
              ;; this scan picked, and the answer comes back InvalidSplitKey.
              ;; Recorded as the refusal it is rather than retried.
              (let [ds (:descriptors v0)]
                (if-not (seq ds)
                  ;; No descriptor at all means the space is not range-routed
                  ;; here — either registration never landed or it was undone.
                  ;; Reported, not retried: this is a precondition the DB setup
                  ;; is responsible for, and papering over it here would hide
                  ;; a run that spent itself hash-routed.
                  (done {:skipped :no-descriptors})
                  (let [d (rand-nth ds)
                        c (split-candidate test space d limit)]
                    (if-not (:split-key c)
                      (done (assoc c :skipped :no-split-key :descriptor
                                   [(:start d) (:end d) (:partition d)]))
                      (let [_ (info "range: splitting" space "at" (:split-key c)
                                    "inside partition" (:partition d))
                            r (split! test space (:split-key c) digest-now)]
                        (done (merge {:key-space  space
                                      :descriptor [(:start d) (:end d) (:partition d)]
                                      :split-key  (:split-key c)
                                      :scanned    (:scanned c)}
                                     r))))))))

            :merge-ranges
            (let [_ (info "range: running the merge pass")
                  r (merge! test digest-now)]
              (done (assoc r :key-space space)))))))

    (teardown! [this test])

    n/Reflection
    (fs [this] #{:split-range :merge-ranges})))

(defn- fault-generator
  "Two splits for every merge.

  Splits are what the placement interaction hinges on — the copy goes through
  the destination partition's log, so a kill landing right after cutover is the
  test — and a merge that runs against a space with one descriptor does nothing
  at all. Merging at the same rate as splitting would spend half the run folding
  ranges back before anything could be aimed at them.

  The merge is worth keeping, and not only for symmetry: a merge retires a
  partition, and 'split, merge, split again' is the sequence that used to wedge
  the splitter for the rest of the run because the next partition id was derived
  from the range map, which forgets retired partitions."
  []
  (cycle [{:type :info, :f :split-range}
          {:type :info, :f :split-range}
          {:type :info, :f :merge-ranges}]))

(defn package
  "A nemesis package for range churn, shaped like the ones
  `jepsen.nemesis.combined` returns.

  Returns the no-op package unless `:range` is in `:faults` *and* the test runs
  key-range routed. Without registration the workload's key space is hash-routed
  and has no descriptors, so every split would answer `NoRange` while the run's
  name claimed a range fault was applied.

  The interval defaults longer than the other faults' because a split is not
  instantaneous: a partition creation, a quiesce, a bulk copy of the upper half
  through the destination's Raft log, and a cutover commit. Firing faster than
  that just collects `ConcurrentSplit`."
  [opts]
  (if-not (and (some #{:range} (:faults opts))
               (:key-range opts)
               (:key-space opts))
    (do (when (some #{:range} (:faults opts))
          (warn "range fault requested without a key-range-routed key space; disabled"))
        {:generator nil :final-generator nil :nemesis nil :perf #{}})
    {:generator (->> (fault-generator)
                     (gen/stagger (:range-interval opts 20)))
     ;; Nothing to undo. A split is not a fault the cluster recovers from — the
     ;; new boundary is the cluster's state now, and the final read is supposed
     ;; to run against a split key space. Folding the ranges back here would
     ;; discard the very layout the last phase exists to read.
     :final-generator nil
     :nemesis   (nemesis opts)
     :perf      #{{:name  "range"
                   :start #{:split-range}
                   :stop  #{:merge-ranges}
                   :color "#B3E5C7"}}}))

;; ---------------------------------------------------------------------------
;; Sampling
;; ---------------------------------------------------------------------------

(defn sampler
  "A nemesis whose only operation records every node's view of the range map.
  Never waits on anything: an unreachable node contributes nil, and a sample is
  one HTTP round trip per node from the control node."
  []
  (reify n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc op :value (kdb/cluster-ranges test)))

    (teardown! [this test])

    n/Reflection
    (fs [this] #{:range-sample})))

(defn sampler-package
  "A range-map sampling package, shaped for `nc/compose-packages`.

  Enabled whenever the test is key-range routed, whether or not the range
  *fault* is on: the automatic split and merge checkers move boundaries too when
  their thresholds are reachable, and a run that cannot see that happen cannot
  claim it did.

  Deliberately absent from `:perf` — these are observations, not faults, and a
  stripe every few seconds on the latency plot would bury the faults that
  matter."
  [opts]
  (if-not (and (:key-range opts)
               (:key-space opts)
               (:range-sampling opts true))
    {:generator nil :final-generator nil :nemesis nil :perf #{}}
    {:generator       (->> (repeat {:type :info, :f :range-sample})
                           (gen/stagger (:range-sample-interval opts 3)))
     :final-generator nil
     :nemesis         (sampler)
     :perf            #{}}))
