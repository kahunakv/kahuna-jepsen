(ns kahuna.checker.range
  "Checks key-range routing: that the descriptors covering a key space tile it
  exactly, that every node describes a given descriptor the same way, and that
  the run actually moved a boundary rather than passing vacuously.

  ## The property that matters

  A key space under key-range routing is served by a set of half-open
  descriptors `[start, end)`, one Raft partition each, with nil meaning ∓infinity
  *within the space*. Routing binary-searches them. So the set must **tile** the
  space:

  * no **gap** — a key falling in one would route nowhere, and every operation
    on it fails for a reason no client can act on;
  * no **overlap** — a key falling in one routes to two partitions, and two
    partitions each holding a version of it is a lost update dressed up as a
    successful write. This is the one to be afraid of: a gap is loud and a
    workload notices immediately, while an overlap looks exactly like the
    database working until two clients disagree about a value.

  A split is the moment the property is at risk: `[S, E)` becomes `[S, K)` and
  `[K, E)` in one committed cutover, and any window in which only half of that
  is visible is a gap or an overlap depending on which half. The whole reason
  this checker reads *samples* rather than a final state is that the interesting
  window is a few milliseconds wide in the middle of a run.

  ## What is checked, and what is deliberately not

  Checked:

  * **Coverage**, per node, per sample, per key space — the descriptors are a
    committed set, so each single view must tile the space on its own. A node
    that lags holds an *older complete* map, not a partial one, so lag cannot
    produce a gap. That is what makes this checkable against one view at a time.
  * **Cross-node agreement.** A descriptor is replicated, so one
    `(key space, partition, generation)` must have identical bounds everywhere.
    Views may lag; they may not differ at the same generation.
  * **A destination partition id is never handed out twice.** A split's
    destination is allocated one past every id Kommander has ever used, so a
    merged-away or rolled-back id never comes back. Checked from the server's own
    `Succeeded` answers — see `reused-partition-ids` for why the sampled route
    cannot be made sound.
  * **Routing mode follows the descriptors.** Kahuna reconciles each node's
    routing registry against the range map synchronously, inside the same call
    that applies a descriptor, so a node reporting descriptors for a space it
    still hashes is a projection that broke — and it would hash-route a space the
    rest of the cluster range-routes. (The one benign way to see this is a
    concurrent `unregister`, which this harness never issues.)

  Not checked, on purpose:

  * **A key space disappearing.** Tempting, and unsound here: the placement
    nemesis wipes and rejoins nodes, and a freshly rejoined node reports zero
    descriptors until it applies the meta partition. Failing on that would be
    reporting the harness's own teardown. Views with no descriptors are skipped
    entirely, and the descriptor counts are reported instead.
  * **Generations moving forward.** Same reason — a wiped node's view starts
    over, and a checker that could not tell that from a regression would be
    guessing.

  ## The vacuity gate

  As in `kahuna.checker.placement`, a pass means nothing if the machinery never
  ran. A run whose key space stayed on one descriptor exercised no split
  transaction, no cutover and no cross-partition routing change; it is
  `:unknown`, not success. Evidence comes from two independent places and either
  will do: the server's own `Succeeded` answers in the history, and the sampled
  range maps showing the space on more than one descriptor."
  (:require [jepsen [checker :as checker]
                    [history :as h]]
            [kahuna.db :as kdb]))

;; ---------------------------------------------------------------------------
;; Reading the history
;; ---------------------------------------------------------------------------

(defn samples
  "The range-map samples in `history`, oldest first, as
  `{:time nanos :views {node range-map-or-nil}}`.

  A sampler op appears twice — invocation and completion both carry `:type
  :info` — and only the completion has a value, so filtering on a map value
  keeps exactly one of each pair."
  [history]
  (->> (h/history history)
       (h/remove h/client-op?)
       (filter #(and (= :range-sample (:f %)) (map? (:value %))))
       (mapv (fn [op] {:time (:time op) :views (:value op)}))))

(defn operations
  "The range nemesis's completed operations, as their value maps. Same
  invocation/completion filter as `samples`."
  [history f]
  (->> (h/history history)
       (h/remove h/client-op?)
       (filter #(and (= f (:f %)) (map? (:value %))))
       (mapv :value)))

;; ---------------------------------------------------------------------------
;; Coverage
;; ---------------------------------------------------------------------------

(defn- start-order
  "Ordinal order on descriptor start bounds, nil (−infinity) first.

  `compare` on strings is `String/compareTo`, which is ordinal — the same
  comparison the server's router uses. A culture-aware one would order
  `\"jepsen/register/2\"` and `\"jepsen/register/10\"` differently from the
  server and read gaps that are not there."
  [a b]
  (cond (= a b) 0
        (nil? a) -1
        (nil? b) 1
        :else (compare a b)))

(defn- row
  "A descriptor in the compact form violations quote it by."
  [d]
  [(:start d) (:end d) (:partition d) (:generation d)])

(defn coverage-violations
  "Every way `descriptors` fails to tile its key space exactly once.

  Assumes a non-empty set — an empty one means the node has nothing to say, not
  that the space is uncovered, and callers filter those out before getting here.

  Returns maps with `:type` one of:

  * `:uncovered-below` / `:uncovered-above` — the set does not reach ∓infinity,
    so keys past the outermost bound route nowhere.
  * `:gap` — consecutive descriptors leave a hole between them.
  * `:overlap` — consecutive descriptors both claim a key. Includes the nil
    cases: a non-final descriptor claiming `+infinity`, or a second descriptor
    claiming `−infinity`, each of which swallows its neighbour whole.
  * `:empty-range` — `start >= end`, a descriptor that can never hold a key and
    that routing will never select."
  [descriptors]
  (let [sorted (vec (sort-by :start start-order descriptors))]
    (concat
      (when (some? (:start (first sorted)))
        [{:type :uncovered-below :descriptor (row (first sorted))}])

      (when (some? (:end (last sorted)))
        [{:type :uncovered-above :descriptor (row (last sorted))}])

      (keep (fn [d]
              (let [{:keys [start end]} d]
                (when (and (some? start) (some? end) (not (neg? (compare start end))))
                  {:type :empty-range :descriptor (row d)})))
            sorted)

      (keep (fn [[a b]]
              (let [ea (:end a)
                    sb (:start b)]
                (when-not (= ea sb)
                  {:type (cond
                           ;; A non-final descriptor running to +infinity, or a
                           ;; second one starting at −infinity: either way both
                           ;; claim every key between them.
                           (nil? ea) :overlap
                           (nil? sb) :overlap
                           (neg? (compare ea sb)) :gap
                           :else :overlap)
                   :left  (row a)
                   :right (row b)})))
            (partition 2 1 sorted)))))

(defn violations
  "Every safety violation visible in `samples`, as a vector of maps.

  Walks each node's view of each key space independently: coverage is a property
  of one committed map, so it is checkable one view at a time, and attributing a
  violation to the node and sample it was seen in is what makes it possible to
  read the server logs at that instant."
  [samples]
  (persistent!
    (reduce
      (fn [acc {:keys [time views]}]
        (reduce
          (fn [acc [node view]]
            (reduce
              (fn [acc [space {:keys [routing-mode descriptors]}]]
                ;; A view with no descriptors says nothing: a node that has not
                ;; applied the meta partition — a fresh join, a rejoin after a
                ;; wipe — honestly reports the space as hash-routed and empty.
                (if-not (seq descriptors)
                  acc
                  (let [acc (if (= "KeyRange" routing-mode)
                              acc
                              (conj! acc {:type :routing-mode-mismatch
                                          :time time :node node :key-space space
                                          :routing-mode routing-mode
                                          :descriptors (mapv row descriptors)}))]
                    (reduce (fn [acc v]
                              (conj! acc (assoc v :time time :node node
                                                  :key-space space)))
                            acc
                            (coverage-violations descriptors)))))
              acc
              (:key-spaces view)))
          acc
          (filter (comp map? val) views)))
      (transient [])
      samples)))

(defn disagreements
  "Descriptors two nodes described differently at the same generation.

  Built by folding every `(key space, partition, generation)` seen anywhere in
  the run into what *each node* answered for it — a **set** of bounds per node,
  not a single pair. Two nodes whose sets differ mean the replicated map was
  not, in fact, common, and every routing decision made from it was made against
  a map somebody else did not have.

  Collecting per node before comparing is what keeps this from inventing
  findings. A generation is `descriptor.Generation + 1` on the descriptor being
  changed, not a counter over the whole map, so nothing in the wire contract
  promises that one `(key space, partition, generation)` names exactly one
  descriptor. Folding straight into a set of bounds would report a partition
  that legitimately held two ranges of one space at one generation as a
  disagreement between a node and itself. Compared per node, every node holding
  the same two ranges agrees, and only a genuine difference fires."
  [samples]
  (let [seen (reduce
               (fn [acc {:keys [views]}]
                 (reduce
                   (fn [acc [node view]]
                     (reduce
                       (fn [acc [space {:keys [descriptors]}]]
                         (reduce
                           (fn [acc d]
                             (update-in acc
                                        [[space (:partition d) (:generation d)] node]
                                        (fnil conj #{}) [(:start d) (:end d)]))
                           acc
                           descriptors))
                       acc
                       (:key-spaces view)))
                   acc
                   (filter (comp map? val) views)))
               {}
               samples)]
    (->> seen
         (keep (fn [[[space pid gen] by-node]]
                 (let [by-answer (reduce (fn [m [node bounds]]
                                           (update m bounds (fnil conj #{}) node))
                                         {}
                                         by-node)]
                   (when (< 1 (count by-answer))
                     {:type :descriptor-disagreement
                      :key-space space
                      :partition pid
                      :generation gen
                      :views (into {}
                                   (map (fn [[bounds nodes]]
                                          [(sort nodes) (vec (sort-by first start-order bounds))]))
                                   by-answer)}))))
         vec)))

;; ---------------------------------------------------------------------------
;; What the boundaries did
;; ---------------------------------------------------------------------------

(defn boundaries
  "What `space`'s descriptor set did over the run, from the samples alone.

  `:layouts` is every distinct boundary set observed, in the order first seen,
  which is the run's own record of how the space was carved up — readable line
  by line against the server logs. `:max-descriptors` above 1 is the sampled
  proof that a split landed and was visible; `:partitions` names every partition
  that ever served part of the space, which is how a destination partition
  created by a split becomes nameable in a report."
  [samples space]
  (let [layouts (reduce
                  (fn [acc {:keys [views]}]
                    (reduce
                      (fn [acc [_node view]]
                        (let [ds (:descriptors (get-in view [:key-spaces space]))]
                          (if-not (seq ds)
                            acc
                            (let [layout (mapv row (sort-by :start start-order ds))]
                              (if (contains? (set acc) layout)
                                acc
                                (conj acc layout))))))
                      acc
                      (filter (comp map? val) views)))
                  []
                  samples)]
    {:layouts         layouts
     :max-descriptors (reduce max 0 (map count layouts))
     :partitions      (vec (sort (distinct (map #(nth % 2) (apply concat layouts)))))}))

(defn reused-partition-ids
  "Destination partition ids handed out by more than one successful split.

  Kahuna allocates a split's destination from Kommander's partition map — one
  past every id ever used — so an id is **never** reused, not even after the
  range that held it was merged away or its creation was rolled back. Before
  that, the id came from the range map, which forgets retired partitions: a
  merge freed an id that Kommander still held as `Removed`, and the next split
  died on it. On a cluster configured with several initial partitions the same
  arithmetic could also pick an id inside the hash pool, and
  `CreatePartitionAsync` answers idempotent success on a live partition — so
  ranged data landed on a partition already serving hash-routed data, silently.

  Checked from the server's own `Succeeded` answers and nowhere else. The
  tempting alternative — watching a partition id vanish from the sampled range
  maps and come back — cannot be made sound: nodes lag, so an id can leave every
  view and reappear from a node that had not caught up, and a run that churns
  the roster would produce that constantly. Two acknowledged splits naming one
  id is unambiguous and has no sampling exposure at all."
  [succeeded]
  (->> succeeded
       (keep :new-partition)
       (filter pos?)
       frequencies
       (keep (fn [[pid n]]
               (when (< 1 n)
                 {:type :partition-id-reused :partition pid :splits n})))
       vec))

(defn split-attempts
  "How the run's split operations were answered, and what that licenses.

  `:succeeded` is a server-side fact — a cutover committed and named the
  destination partition — so it is evidence in its own right, independent of
  whether the sampler happened to catch the two-descriptor state.

  `:indeterminate` is kept apart from `:refused` deliberately. A split that
  failed after the transaction was under way may still have changed the map, and
  a checker that counted those as refusals would report a run as having done
  nothing while the map moved underneath it.

  `:creation-failed` is pulled out of `:outcomes` because it is the signature of
  a specific regression rather than an ordinary refusal: it is what a split
  answers when the destination id cannot be created, which is how a reused id
  surfaces to a caller. Under a correct allocator a retry gets a fresh id, so a
  run full of these is worth reading even though none of them is a violation on
  its own."
  [ops]
  (let [attempted (filter :attempted ops)
        succeeded (filter :success attempted)]
    {:ops             (count ops)
     :attempted       (count attempted)
     :succeeded       (count succeeded)
     :indeterminate   (count (remove #(or (:success %) (:determinate %)) attempted))
     :refused         (count (filter #(and (not (:success %)) (:determinate %)) attempted))
     :creation-failed (count (filter #(= "PartitionCreationFailed" (:outcome %)) attempted))
     :destinations    (vec (sort (distinct (keep :new-partition succeeded))))
     :reused          (reused-partition-ids succeeded)
     :skipped         (frequencies (keep :skipped ops))
     :outcomes        (frequencies (keep :outcome attempted))}))

(defn merge-attempts
  "How the run's merge passes were answered. `:merges` is the total number of
  adjacent pairs folded, which is the only number here that says the machinery
  did anything: a leader that found nothing eligible succeeds with 0."
  [ops]
  (let [attempted (filter :attempted ops)]
    {:ops       (count ops)
     :attempted (count attempted)
     :succeeded (count (filter :success attempted))
     :merges    (reduce + 0 (keep :merges (filter :success attempted)))
     :outcomes  (frequencies (keep :outcome attempted))}))

;; ---------------------------------------------------------------------------
;; The vacuity gate
;; ---------------------------------------------------------------------------

(def evidence-kinds
  "The kinds of range activity a run can be required to demonstrate, in the
  order they are reported."
  [:split :merge])

(def default-required-evidence
  "What a key-range run must show before its pass means anything.

  `:merge` is not required by default: a merge pass only folds ranges the
  running policy considers under-sized, so a run whose ranges stay large enough
  legitimately merges nothing, and demanding one would make the verdict depend
  on how much data the workload happened to write."
  #{:split})

(defn evidence
  "Measures each kind of range activity.

  `:measured? false` means this run had no way to tell — no samples and no
  operations of that kind — and is never treated as zero. The distinction is the
  same one the placement checker draws: 'the machinery did not run' and 'nobody
  looked' license completely different conclusions."
  [bounds splits merges]
  {:split (if (or (seq (:layouts bounds)) (pos? (:ops splits 0)))
            {:measured?  true
             :count      (:succeeded splits)
             ;; The sampled half. A split that succeeded and was merged back
             ;; between two samples leaves this false while :count is positive —
             ;; which is why either one is enough.
             :observed?  (< 1 (:max-descriptors bounds 0))}
            {:measured? false})
   :merge (if (pos? (:ops merges 0))
            {:measured? true :count (:merges merges)}
            {:measured? false})})

(defn- shown?
  "Did this evidence kind actually happen? `:split` accepts either an
  acknowledged success or a sampled multi-descriptor layout."
  [e]
  (boolean (or (pos? (:count e 0)) (:observed? e))))

(defn- gate
  "Splits the required evidence into what was shown, what was measured and
  absent, and what could not be measured at all."
  [required ev]
  (reduce (fn [acc k]
            (let [e (get ev k)]
              (cond
                (not (:measured? e)) (update acc :unmeasured conj k)
                (shown? e)           (update acc :shown conj k)
                :else                (update acc :missing conj k))))
          {:shown [] :missing [] :unmeasured []}
          (filter required evidence-kinds)))

;; ---------------------------------------------------------------------------
;; The checker
;; ---------------------------------------------------------------------------

(defn checker
  "A jepsen checker for key-range routing.

  * `:valid? true` — the descriptors tiled the key space throughout, every node
    agreed, and the run demonstrated a boundary actually moved.
  * `:valid? false` — a gap, an overlap, an empty range, a node hashing a space
    it holds descriptors for, a destination partition id handed out twice, or
    two nodes describing one descriptor differently. `:violations` and
    `:disagreements` say which and where.
  * `:valid? :unknown` — nothing was proven. Either no split ever landed
    (`:cause :vacuous`) or the run could not be observed (`:cause :unmeasured`).
    Read this as 'run it again with the range nemesis on', never as a pass.

  Without `--key-range` this returns `:valid? true` and makes no claim: the
  workload's key space is hash-routed, there are no descriptors, and every
  property above is vacuously true of a map that does not exist."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (if-not (kdb/key-ranged? test)
        {:valid? true :key-range false
         :note "hash routing; key ranges not under test"}
        (let [space    (kdb/key-space test)
              ss       (samples history)
              splits   (split-attempts (operations history :split-range))
              merges   (merge-attempts (operations history :merge-ranges))
              bounds   (boundaries ss space)
              ;; Two sources, deliberately joined: the sampled views say whether
              ;; the map stayed coherent, and the split answers say whether the
              ;; destination ids were allocated correctly. Neither can see the
              ;; other's failure.
              vs       (into (violations ss) (:reused splits))
              ds       (disagreements ss)
              ev       (evidence bounds splits merges)
              required (set (:require-range-evidence test default-required-evidence))
              {:keys [shown missing unmeasured]} (gate required ev)
              base     {:key-space         space
                        :samples           (count ss)
                        :evidence          ev
                        :evidence-required (vec (sort required))
                        ;; Reported on every verdict, not only a passing one. A
                        ;; run that showed a split and no merge is a different
                        ;; report from one that showed neither, and a :missing
                        ;; list on its own cannot tell them apart.
                        :shown             shown
                        :splits            splits
                        :merges            merges
                        :max-descriptors   (:max-descriptors bounds)
                        :partitions        (:partitions bounds)
                        :layouts           (:layouts bounds)}]
          (cond
            (or (seq vs) (seq ds))
            (assoc base :valid? false :violations (vec vs) :disagreements ds)

            (seq unmeasured)
            (assoc base :valid? :unknown :cause :unmeasured
                        :unmeasured unmeasured :missing missing)

            (seq missing)
            (assoc base :valid? :unknown :cause :vacuous :missing missing)

            :else
            (assoc base :valid? true)))))))
