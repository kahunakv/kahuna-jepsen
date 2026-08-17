(ns kahuna.client
  "Thin Clojure wrapper over Kahuna's REST API.

  Kahuna serializes with System.Text.Json using a camelCase naming policy and
  numeric enums (see Kahuna.Shared/Communication/Rest/KahunaJsonContext.cs), so:

    * enum fields (`type`, `flags`, `durability`) are integers on the wire
    * `byte[]` fields (`value`, `compareValue`, `lockId`) are base64 strings
    * `HLCTimestamp` is an object {\"n\" int, \"l\" long, \"c\" uint}

  Every call returns the parsed body plus a `:status`; classification of a
  response into ok/fail/info lives in `response-class`, which is the single most
  important function in this suite: mislabelling an indeterminate write as a
  definite failure manufactures false linearizability violations, and the
  reverse hides real ones."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]])
  (:import (java.util Base64)))

;; ---------------------------------------------------------------------------
;; Wire encoding helpers
;; ---------------------------------------------------------------------------

(defn ->b64
  "Encodes a string (or byte array) as base64 for a Kahuna byte[] field."
  [x]
  (let [^bytes bs (if (bytes? x) x (.getBytes (str x) "UTF-8"))]
    (.encodeToString (Base64/getEncoder) bs)))

(defn b64->str
  "Decodes a base64 byte[] field back into a string. nil-safe."
  [s]
  (when s
    (String. (.decode (Base64/getDecoder) ^String s) "UTF-8")))

(def hlc-zero
  "HLCTimestamp.Zero — 'no transaction' / 'read the latest committed value'."
  {:n 0 :l 0 :c 0})

(defn hlc-compare
  "Total order on HLC timestamps: physical time, then counter, then node id.

  Mirrors Kommander's `HLCTimestamp.CompareTo` component for component,
  including the node-id tie-break. Getting this wrong would be quiet: nearly
  every pair of timestamps differs in `:l` already, so a comparator that
  ignored `:c` would agree with this one on almost all inputs and disagree
  exactly on the same-millisecond pairs a snapshot test is built to examine.

  Missing components read as 0, so `hlc-zero` and a nil-free partial map both
  order sensibly."
  [a b]
  (let [c (compare (long (:l a 0)) (long (:l b 0)))]
    (if-not (zero? c)
      c
      (let [c (compare (long (:c a 0)) (long (:c b 0)))]
        (if-not (zero? c)
          c
          (compare (long (:n a 0)) (long (:n b 0))))))))

(defn hlc-zero?
  "Is this HLCTimestamp.Zero? Zero is the 'read latest' sentinel, so a snapshot
  read must never be issued with one — it would silently become an ordinary
  read of the current value and every stability check would pass vacuously."
  [t]
  (or (nil? t)
      (= 0 (:n t 0) (:l t 0) (:c t 0))))

;; KeyValueFlags (Kahuna.Shared/KeyValue/KeyValueFlags.cs)
(def flag-set                     1)
(def flag-set-no-revision         2)
(def flag-set-if-exists           4)
(def flag-set-if-not-exists       8)
(def flag-set-if-equal-to-value  16)
(def flag-set-if-equal-to-rev    32)

;; KeyValueDurability / LockDurability
(def ephemeral  0)
(def persistent 1)

;; KeyValueResponseType (Kahuna.Shared/KeyValue/KeyValueResponseType.cs)
(def kv-response-type
  {0   :set
   1   :not-set
   2   :extended
   3   :get
   4   :deleted
   5   :locked
   6   :unlocked
   7   :prepared
   8   :committed
   9   :rolled-back
   10  :exists
   11  :waiting-for-replication
   99  :errored
   100 :invalid-input
   101 :must-retry
   102 :aborted
   103 :does-not-exist
   104 :already-locked
   105 :prefix-lock-unsupported
   106 :range-locks
   107 :safe-timestamp})

;; LockResponseType (Kahuna.Shared/Locks/LockResponseType.cs)
(def lock-response-type
  {0   :locked
   1   :busy
   2   :extended
   3   :unlocked
   4   :got
   10  :waiting-for-replication
   99  :errored
   100 :invalid-input
   101 :must-retry
   102 :lock-does-not-exist
   103 :invalid-owner
   104 :aborted})

(def indeterminate-types
  "Response types that leave the outcome of a write UNKNOWN. A write that
  returns one of these may still commit later, so it must be recorded as :info,
  never :fail.

  `:must-retry` is deliberately here even though a retry is usually safe: the
  KeyValueResponseType docs describe it as overloaded (stale route OR safe-time
  not reached), and on a write path the proposal may already be in flight. If
  you later prove a given call site can only produce the stale-route variant,
  narrow it there — not globally."
  #{:waiting-for-replication :must-retry :aborted :errored})

(defn response-class
  "Classifies a Kahuna response type keyword for a *write* operation:

    :ok    — the operation definitely took effect
    :fail  — the operation definitely did NOT take effect
    :info  — indeterminate; it may or may not apply later

  Read operations should use this too, but a failed read is harmless either way."
  [t]
  (cond
    (nil? t)                       :info
    (indeterminate-types t)        :info
    (#{:set :extended :get :deleted :locked :unlocked :committed
       :rolled-back :exists :prepared} t) :ok
    ;; :not-set, :does-not-exist, :already-locked, :busy, :invalid-owner,
    ;; :invalid-input, :lock-does-not-exist — the server made a decision.
    :else                          :fail))

;; ---------------------------------------------------------------------------
;; Transport
;; ---------------------------------------------------------------------------

(def http-port
  "Plain-HTTP API port. Kahuna also serves HTTPS (and Raft) on 8082, but the
  tests deliberately use cleartext HTTP so failures are transport failures and
  not TLS handshake noise."
  8081)

(defn base-url [node]
  (str "http://" node ":" http-port))

(defn- json-body
  "The response body as a map, whether or not clj-http decoded it — nil when it
  is not JSON at all.

  clj-http decodes `:as :json` only for *unexceptional* statuses: `:coerce`
  defaults to `:unexceptional`, and `can-parse-body?` forces everything else to
  a raw String. That is not a cosmetic detail here. Kahuna reports refusals as
  **409 with a body**, so its entire outcome vocabulary — `NotLeader`,
  `Indeterminate`, `BelowMinRangeSize`, and the `determinate` flag a
  fault-injection harness must read before deciding whether a retry is safe —
  lives exactly where clj-http stops parsing. Without this, every refusal in
  this suite arrives as all-nils and a caller cannot tell 'try the next node'
  from 'the map may still be changing'.

  Parsed here rather than by asking clj-http for `:coerce :always`, because that
  decodes unconditionally and *throws* on a body that is not JSON — and an
  unhandled ASP.NET 500 answers text/plain, which this suite has already been
  bitten by. A body that will not parse, or parses to something that is not a
  map, yields nil, which every caller already treats as 'nothing was learned'."
  [body]
  (cond
    (map? body)    body
    (string? body) (let [parsed (try (json/parse-string body true)
                                     (catch Exception _ nil))]
                     (when (map? parsed) parsed))
    :else          nil))

(defn- normalize
  "Flattens a response body to the top level and adds :status.

  A body that is not JSON at all — an empty body, or an ASP.NET text/plain 500 —
  becomes a map with no :type, which `response-class` treats as indeterminate.
  Assoc'ing onto the String directly would throw ClassCastException mid-history."
  [resp]
  (if-let [body (json-body (:body resp))]
    (assoc body :status (:status resp))
    {:type nil :status (:status resp) :body (:body resp)}))

(defn post*
  "POSTs `body` as JSON to `path` on `node`, returning {:status int :body parsed}
  *without* flattening the body to the top level.

  Use this — not `post!` — when the response carries a field of its own named
  `status`. `KahunaSetReplicationFactorResponse` does: it reports the commit
  outcome (`Success`, `Refused`, …) there, and flattening would replace it with
  the HTTP status code, so a refusal would read as the number 409 rather than as
  the reason it was refused.

  `:body` is nil when the response was not JSON. It is *not* nil merely because
  the call was refused: refusals are exactly where the interesting body is, and
  `json-body` exists because clj-http would otherwise hand back the raw string."
  ([node path body] (post* node path body {}))
  ([node path body opts]
   (let [timeout (:timeout opts 5000)
         resp    (http/post (str (base-url node) path)
                            {:body               (json/generate-string body)
                             :content-type       :json
                             :accept             :json
                             :socket-timeout     timeout
                             :connection-timeout timeout
                             :throw-exceptions   false
                             :as                 :json})]
     {:status (:status resp) :body (json-body (:body resp))})))

(defn post!
  "POSTs `body` as JSON to `path` on `node`. Returns the parsed body map with
  keyword keys, augmented with :status. Never throws on a non-2xx status —
  callers decide. Network-level failures still throw, and callers are expected
  to funnel those through `jepsen.client/with-errors`-style handling."
  ([node path body] (post! node path body {}))
  ([node path body opts]
   (let [timeout (:timeout opts 5000)]
     (normalize
       (http/post (str (base-url node) path)
                  {:body               (json/generate-string body)
                   :content-type       :json
                   :accept             :json
                   :socket-timeout     timeout
                   :connection-timeout timeout
                   :throw-exceptions   false
                   :as                 :json})))))

(defn get!
  "GETs `path` on `node`, parsed as JSON."
  ([node path] (get! node path {}))
  ([node path opts]
   (let [timeout (:timeout opts 5000)]
     (normalize
       (http/get (str (base-url node) path)
                 {:accept             :json
                  :socket-timeout     timeout
                  :connection-timeout timeout
                  :throw-exceptions   false
                  :as                 :json})))))

;; ---------------------------------------------------------------------------
;; Key/value operations
;; ---------------------------------------------------------------------------

(defn kv-get
  "Reads `key`. Returns
  {:type kw :value str-or-nil :revision long :last-modified hlc}.

  `:read-timestamp` selects MVCC snapshot visibility: the server serves the
  revision at-or-before it. It defaults to `hlc-zero`, which is the 'latest
  committed value' sentinel rather than a very old snapshot.

  `:last-modified` is the commit timestamp of the revision actually served, and
  Kahuna documents it as round-trippable — it is the only way a client obtains
  a real HLC to read as-of later. Two consequences the snapshot workload leans
  on: reading at latest yields a timestamp naming *this* value, and a snapshot
  read whose `:last-modified` exceeds the requested `:read-timestamp` has
  served a revision from the future of its own snapshot."
  [node key {:keys [durability timeout read-timestamp]
             :or   {durability persistent}}]
  (let [r (post! node "/v1/kv/try-get"
                 {:transactionId hlc-zero
                  :key           key
                  :revision      -1
                  :readTimestamp (or read-timestamp hlc-zero)
                  ;; Was `:value` until Kahuna 023bfb1 ("Fix REST JSON field
                  ;; names"), which corrected KahunaGetKeyValueRequest's
                  ;; [JsonPropertyName] from "value" to "durability". The old
                  ;; spelling does not fail loudly against a current server: an
                  ;; unmapped member is ignored, Durability defaults to
                  ;; Ephemeral, and every read of a persistent key comes back
                  ;; DoesNotExist. See FINDINGS.md.
                  :durability    durability}
                 {:timeout timeout})
        t (kv-response-type (:type r))]
    {:type          t
     :value         (b64->str (:value r))
     :revision      (:revision r)
     :last-modified (:lastModified r)
     :raw           r}))

(defn kv-set
  "Unconditional write. Returns {:type kw :revision long}."
  [node key value {:keys [durability expires-ms timeout]
                   :or   {durability persistent expires-ms 0}}]
  (let [r (post! node "/v1/kv/try-set"
                 {:transactionId hlc-zero
                  :key           key
                  :value         (->b64 value)
                  :compareValue  nil
                  :compareRevision 0
                  :expiresMs     expires-ms
                  :flags         flag-set
                  :durability    durability}
                 {:timeout timeout})]
    {:type (kv-response-type (:type r)) :revision (:revision r) :raw r}))

(defn kv-cas!
  "Compare-and-set on the *value*: writes `new-value` only if the current value
  equals `old-value`. Returns {:type :set} on success, {:type :not-set} when the
  comparison failed."
  [node key old-value new-value {:keys [durability expires-ms timeout]
                                 :or   {durability persistent expires-ms 0}}]
  (let [r (post! node "/v1/kv/try-set"
                 {:transactionId   hlc-zero
                  :key             key
                  :value           (->b64 new-value)
                  :compareValue    (->b64 old-value)
                  :compareRevision 0
                  :expiresMs       expires-ms
                  :flags           flag-set-if-equal-to-value
                  :durability      durability}
                 {:timeout timeout})]
    {:type (kv-response-type (:type r)) :revision (:revision r) :raw r}))

(defn kv-delete!
  [node key {:keys [durability timeout] :or {durability persistent}}]
  (let [r (post! node "/v1/kv/try-delete"
                 {:transactionId hlc-zero
                  :key           key
                  :durability    durability}
                 {:timeout timeout})]
    {:type (kv-response-type (:type r)) :revision (:revision r) :raw r}))

;; ---------------------------------------------------------------------------
;; Locks (used by the lock workload; see src/kahuna/workload/lock.clj)
;; ---------------------------------------------------------------------------

(defn try-lock!
  "Acquires `resource` for `owner` with a lease of `expires-ms`. Returns
  {:type kw :fencing-token long}."
  [node resource owner expires-ms {:keys [durability timeout]
                                   :or   {durability persistent}}]
  (let [r (post! node "/v1/locks/try-lock"
                 {:resource   resource
                  ;; `owner`, not `lockId`: renamed by Kahuna 023bfb1. A server
                  ;; on the new name binds no Owner from `lockId` and refuses
                  ;; with InvalidInput before doing any work.
                  :owner      (->b64 owner)
                  :expiresMs  expires-ms
                  :durability durability}
                 {:timeout timeout})]
    {:type (lock-response-type (:type r)) :fencing-token (:fencingToken r) :raw r}))

(defn try-unlock!
  [node resource owner {:keys [durability timeout] :or {durability persistent}}]
  (let [r (post! node "/v1/locks/try-unlock"
                 {:resource   resource
                  :owner      (->b64 owner)
                  :expiresMs  0
                  :durability durability}
                 {:timeout timeout})]
    {:type (lock-response-type (:type r)) :raw r}))

;; ---------------------------------------------------------------------------
;; Sequences (used by the sequencer workload; see src/kahuna/workload/sequencer.clj)
;; ---------------------------------------------------------------------------

;; SequenceResponseType (Kahuna.Shared/Sequences/SequenceResponseType.cs)
(def sequence-response-type
  {0  :success
   1  :not-found
   2  :already-exists
   3  :invalid-input
   4  :max-value-exceeded
   5  :must-retry
   6  :aborted
   99 :error})

;; SequenceDurability (Kahuna.Shared/Sequences/SequenceDurability.cs) has a
;; single member, Persistent = 1 — note it is NOT the 0/1 of KeyValueDurability,
;; so passing `kc/persistent` here would serialize as Ephemeral-that-does-not-exist.
(def sequence-persistent 1)

(defn sequence-class
  "Classifies a sequence response for an *allocating* call (`next` / `reserve`).

  `:must-retry`, `:aborted` and `:error` are indeterminate: the allocation may
  have been durably consumed with the acknowledgement lost, so the caller must
  record :info. The remaining non-success types are decisions the server made
  before consuming anything."
  [t]
  (cond
    (nil? t)                                    :info
    (= :success t)                              :ok
    (#{:must-retry :aborted :error} t)          :info
    :else                                       :fail))

(defn seq-create!
  "Creates a sequence. `:already-exists` is not an error for our purposes — it
  means some other client won the race."
  [node name {:keys [initial-value increment max-value timeout]
              :or   {initial-value 0 increment 1}}]
  (let [r (post! node "/v1/sequences/create"
                 {:name         name
                  :initialValue initial-value
                  :increment    increment
                  :maxValue     max-value
                  :durability   sequence-persistent}
                 {:timeout timeout})]
    {:type (sequence-response-type (:type r)) :revision (:revision r) :raw r}))

(defn- allocation
  "Unpacks a SequenceAllocation. `end` is INCLUSIVE: the server plans
  `start = current + increment`, `end = current + increment * count`, so an
  allocation covers exactly `count` values (see SequenceActor.TryPlanFromBlock)."
  [r]
  (let [a (:allocation r)]
    (when a
      {:start    (:start a)
       :end      (:end a)
       :count    (:count a)
       :revision (:revision a)})))

(defn seq-next!
  "Takes the next value. `idempotency-key` may be nil; when supplied, replaying
  the same key must return the *same* allocation rather than consuming another
  value. Returns {:type kw :allocation {...}}."
  [node name idempotency-key {:keys [timeout]}]
  (let [r (post! node "/v1/sequences/next"
                 {:name           name
                  :idempotencyKey idempotency-key
                  :durability     sequence-persistent}
                 {:timeout timeout})]
    {:type       (sequence-response-type (:type r))
     :allocation (allocation r)
     :served-from (:servedFrom r)
     :raw        r}))

(defn seq-reserve!
  "Reserves a contiguous run of `count` values. Returns {:type kw :allocation}."
  [node name count idempotency-key {:keys [timeout]}]
  (let [r (post! node "/v1/sequences/reserve"
                 {:name           name
                  :count          count
                  :idempotencyKey idempotency-key
                  :durability     sequence-persistent}
                 {:timeout timeout})]
    {:type       (sequence-response-type (:type r))
     :allocation (allocation r)
     :served-from (:servedFrom r)
     :raw        r}))

;; ---------------------------------------------------------------------------
;; MVCC snapshot holds (used by the snapshot workload; see
;; src/kahuna/workload/snapshot.clj)
;; ---------------------------------------------------------------------------

(defn snapshot-hold-acquire!
  "Pins every revision at-or-after `timestamp` against reclamation, for
  `lease-ms`. Returns {:type kw :hold-id str :lease-expiry hlc}.

  Success is `:set` — the same overloaded name `start-tx-session` uses.

  `:must-retry` here is emphatically NOT 'the hold exists, try later for the
  id'. `SnapshotFloorStore.AcquireAsync` fails closed when a prune-delete
  window overlapped the acquire: the hold *is* committed and durable, but the
  boundary revision it was meant to protect may already have been deleted, so
  the server refuses to claim the timestamp is readable. Treating that as a
  successful pin would let the workload assert stability over history the
  server never promised to keep, and the resulting 'violation' would be the
  test's fault. Only `:set` may become a pin.

  `holder-id` should be unique per pin: acquire is idempotent by
  (holderId, timestamp), so two pins sharing a holder at the same timestamp
  collapse into one hold, and releasing either drops the protection both
  believe they hold."
  [node holder-id timestamp lease-ms {:keys [timeout]}]
  (let [r (post! node "/v1/kv/snapshot-hold/acquire"
                 {:holderId  holder-id
                  :timestamp timestamp
                  :leaseMs   lease-ms}
                 {:timeout timeout})]
    {:type         (kv-response-type (:type r))
     :hold-id      (:holdId r)
     :lease-expiry (:leaseExpiry r)
     :raw          r}))

(defn snapshot-hold-renew!
  "Extends a hold's lease by `lease-ms` from now. `:does-not-exist` means the
  hold was never registered *or* had already expired — the server does not
  distinguish, so a renewal that comes back with it cannot be read as proof the
  protection was continuous."
  [node hold-id lease-ms {:keys [timeout]}]
  (let [r (post! node "/v1/kv/snapshot-hold/renew"
                 {:holdId hold-id :leaseMs lease-ms}
                 {:timeout timeout})]
    {:type (kv-response-type (:type r)) :lease-expiry (:leaseExpiry r) :raw r}))

(defn snapshot-hold-release!
  "Drops a hold. The effective floor rises when the lowest one goes."
  [node hold-id {:keys [timeout]}]
  (let [r (post! node "/v1/kv/snapshot-hold/release"
                 {:holdId hold-id}
                 {:timeout timeout})]
    {:type (kv-response-type (:type r)) :raw r}))

(defn snapshot-floor
  "Reads /v1/kv/snapshot-floor: {:floor hlc :live-holds int}. The floor is the
  minimum timestamp among live holds, and `hlc-zero` when there are none."
  [node {:keys [timeout]}]
  (let [r (get! node "/v1/kv/snapshot-floor" {:timeout (or timeout 5000)})]
    {:floor      (:effectiveFloor r)
     :live-holds (:liveHolds r)
     ;; `:get` on success. The type matters because
     ;; `RetryableExceptionMapping` catches retryable infrastructure exceptions
     ;; on /v1/kv/* and substitutes a KeyValue-shaped {"type":101} — MustRetry
     ;; — while leaving the status at 200. Callers must not read the resulting
     ;; absent `liveHolds` as "no holds are live"; it means nothing was
     ;; measured at all. The field was added to this DTO for exactly that
     ;; reason, so a refusal is now distinguishable from an empty success.
     :type       (kv-response-type (:type r))
     :status     (:status r)
     :raw        r}))

(defn cluster-membership
  "Reads /v1/cluster/membership — handy for debugging and for waiting until the
  cluster has actually formed before a test starts."
  [node]
  (get! node "/v1/cluster/membership" {:timeout 2000}))

(defn cluster-health
  "Reads /v1/cluster/health: {:ready bool :initialized bool :localRole str}.

  The status code carries the same answer as the body — 200 ready, 503 not —
  and `get!` does not throw on either, so callers read :ready rather than
  inspecting :status.

  This is the signal that distinguishes 'listening' from 'able to serve'. A
  node answers /v1/cluster/membership about a second after launch and then
  refuses every key/value request until initialization completes; before this
  endpoint existed there was no way to tell those apart without sending a real
  request and being refused."
  [node]
  (get! node "/v1/cluster/health" {:timeout 2000}))

;; ---------------------------------------------------------------------------
;; Per-partition replica placement (used by the placement nemesis and checker;
;; see src/kahuna/nemesis/placement.clj and src/kahuna/checker/placement.clj)
;; ---------------------------------------------------------------------------

(defn- replica-role
  "Normalizes a wire role name to a keyword: Voter/Learner/Removing become
  :voter/:learner/:removing. An unknown role keywordizes rather than being
  dropped — a role this harness has never seen is exactly the thing a placement
  checker must not silently ignore."
  [s]
  (when s (keyword (str/lower-case s))))

(def transitional-roles
  "Roles that mean a replica is mid-move. Kommander's placement service enforces
  at most one of these per range (`ReplicaPlacementService`, \"single mover per
  range\"), which is what makes successive committed configurations overlap by a
  quorum. Observing two at once on one range is a safety violation, not churn."
  #{:learner :removing})

(defn cluster-placement
  "Reads /v1/cluster/placement — the committed per-partition replica map, as seen
  by `node`. Returns nil when the node cannot answer.

  Every node returns the *same* committed map; only the `:hosted-locally` flags
  and `:hosted-count` describe the answering node. That is what makes this
  endpoint usable as a cross-node agreement check: two nodes reporting the same
  partition at the same `:generation` must report an identical replica set.

  An empty `:replicas` for a partition means legacy full replication (every
  roster voter hosts it), which is also what an RF-0 cluster reports for
  everything."
  [node]
  (let [r (get! node "/v1/cluster/placement" {:timeout 3000})]
    (when (= 200 (:status r))
      {:replication-factor (:replicationFactor r)
       :rebalancer         (true? (:rebalancerEnabled r))
       :initialized        (true? (:initialized r))
       :endpoint           (:localEndpoint r)
       :hosted-count       (:hostedPartitionCount r)
       :partitions
       (into (sorted-map)
             (map (fn [p]
                    [(:partitionId p)
                     {:state          (:state p)
                      :generation     (:generation p)
                      :effective-rf   (:effectiveReplicationFactor p)
                      :hosted-locally (true? (:hostedLocally p))
                      :replicas       (mapv (fn [x]
                                              {:endpoint (:endpoint x)
                                               :role     (replica-role (:role x))})
                                            (:replicas p))}]))
             (:partitions r))})))

(defn set-replication-factor!
  "Commits a per-partition replication-factor override (0 clears it, so the
  partition inherits the global factor).

  **Leader-only.** Like every partition-map mutation this is accepted by the
  meta-partition leader alone; a follower answers 409 with the refusing status
  in the body, so a caller that wants the change to land must try endpoints
  until one commits. `:success` is the only thing that means committed —
  `:status` carries the refusal reason and is read straight from the body, which
  is why this goes through `post*`.

  Success means the *target* moved, not the replicas: the rebalancer converges
  toward it on later passes, and with the rebalancer off nothing moves at all."
  [node partition-id rf {:keys [timeout]}]
  (let [{:keys [status body]} (post* node "/v1/cluster/replication-factor"
                                     {:partitionId       partition-id
                                      :replicationFactor rf}
                                     {:timeout (or timeout 10000)})
        body (when (map? body) body)]
    {:success     (true? (:success body))
     :outcome     (:status body)
     :generation  (:generation body)
     :reason      (:reason body)
     :http-status status}))

(defn cluster-leave!
  "Decommissions `node`: commits its removal from the roster now, rather than
  stopping the process and making the cluster infer the departure from silence.

  The node keeps serving its port afterwards so this answer can be read;
  stopping it is the caller's next step. `:outcome` is the consensus outcome
  name (`Committed`, `NotAMember`, `RefusedInsufficientVoters`, `NotInitialized`,
  `NoLeader`, `Timeout`), so — unlike a SIGTERM-and-hope leave — a nemesis can
  record *why* a removal did not happen instead of inferring it from a roster
  count."
  [node {:keys [timeout]}]
  (let [r (post! node "/v1/cluster/leave" {} {:timeout (or timeout 30000)})]
    {:left               (true? (:left r))
     :outcome            (:outcome r)
     :membership-version (:membershipVersion r)
     :retryable          (true? (:retryable r))
     :reason             (:reason r)
     :http-status        (:status r)}))

;; ---------------------------------------------------------------------------
;; Key ranges: the range map, and the split/merge admin surface
;; (used by kahuna.nemesis.range and kahuna.checker.range)
;;
;; Every POST here answers with a body carrying its own `status` field — the
;; outcome name, not the HTTP code — so all of them go through `post*`. Using
;; `post!` would flatten the body and overwrite that field with 200/400/409,
;; turning `"BelowMinRangeSize"` into the number 409 and making a refusal
;; indistinguishable from a mid-cutover failure. That distinction is the entire
;; reason the endpoints report `determinate` at all.
;; ---------------------------------------------------------------------------

(defn- range-descriptor
  "One wire descriptor as `{:start :end :partition :generation}`.

  `:start` and `:end` keep the server's nils, which mean ∓infinity *within the
  key space* rather than a missing field. Bounds are half-open — `[start, end)`
  — and compared **ordinally**; `clojure.core/compare` on strings is
  `String/compareTo`, which is ordinal, so coverage arithmetic on these is
  sound. A culture-aware comparison would read gaps that are not there."
  [d]
  {:start      (:startKey d)
   :end        (:endKey d)
   :partition  (:partitionId d)
   :generation (:generation d)})

(defn range-map
  "Reads /v1/ranges — the range-descriptor map as `node` has applied it. Returns
  nil when the node cannot answer.

  Two things travel side by side here and they are not the same kind of fact.
  The **descriptors** are replicated on the meta partition, so every node
  converges on them; the **routing mode** is node-local, derived by reconciling
  the registry against the descriptors this node has applied. So a node that has
  not applied the map yet honestly reports `Hash` with no descriptors — which is
  why a checker must ignore empty views rather than read them as a key space
  going missing.

  Deliberately unfiltered: the `?keySpace=` form reports one space even when
  nothing knows about it, which is useful for polling a registration, but a
  checker wants everything the node believes so an unexpected space is visible
  rather than invisible."
  [node]
  (let [r (get! node "/v1/ranges" {:timeout 3000})]
    (when (= 200 (:status r))
      {:initialized (true? (:initialized r))
       :endpoint    (:localEndpoint r)
       :key-spaces  (into (sorted-map)
                          (map (fn [ks]
                                 [(:keySpace ks)
                                  {:routing-mode (:routingMode ks)
                                   :descriptors  (mapv range-descriptor (:descriptors ks))}]))
                          (:keySpaces r))})))

(defn register-key-range!
  "Puts `key-space` under key-range routing: POST /v1/ranges/register.

  **Not leader-only** — the node forwards the whole-space seed descriptor to the
  meta-partition leader and waits for it to replicate back, so a follower
  legitimately succeeds. That wait is why the default timeout here is generous.

  `:seeded` is true only when *this* call committed the seed; `AlreadySeeded` is
  still a success, just not this call's doing. `Indeterminate` means the mode
  was flipped locally but no descriptor is visible here yet — it may still
  arrive, so it is not a failure and must not be retried as one. Read
  `range-map` before concluding anything."
  [node key-space {:keys [timeout]}]
  (let [{:keys [status body]} (post* node "/v1/ranges/register"
                                     {:keySpace key-space}
                                     {:timeout (or timeout 20000)})
        body (when (map? body) body)]
    {:success          (true? (:success body))
     :outcome          (:status body)
     :seeded           (true? (:seeded body))
     :routing-mode     (:routingMode body)
     :descriptor-count (:descriptorCount body)
     :reason           (:reason body)
     :http-status      status}))

(defn unregister-key-range!
  "Drops `key-space`'s descriptors from the replicated map: POST
  /v1/ranges/unregister. Every node's routing mode follows through the normal
  replication path. Same forwarding contract as `register-key-range!`."
  [node key-space {:keys [timeout]}]
  (let [{:keys [status body]} (post* node "/v1/ranges/unregister"
                                     {:keySpace key-space}
                                     {:timeout (or timeout 20000)})
        body (when (map? body) body)]
    {:success          (true? (:success body))
     :outcome          (:status body)
     :routing-mode     (:routingMode body)
     :descriptor-count (:descriptorCount body)
     :reason           (:reason body)
     :http-status      status}))

(defn split-range!
  "Splits the range covering `split-key` at exactly that key: POST
  /v1/ranges/split. `[S, E)` becomes `[S, split-key)` and `[split-key, E)`, so
  the key itself lands in the *upper* half.

  **Leader-only**: a node that does not lead the partition owning the range map
  answers `NotLeader` without attempting anything, which is safe to retry
  elsewhere.

  `:determinate` is the field a fault-injection harness must read, and it is
  not the same question as `:success`. A split is a multi-step transaction —
  create the destination partition, quiesce the source, copy, commit the
  cutover — and `TransferFailed` / `QuiesceFailed` / `CutoverFailed` /
  `ConcurrentSplit` leave the outcome genuinely unknown: the map may change
  moments after this returns. Retrying one of those against another node would
  be issuing a second mutation while the first is still in flight, which is
  precisely how a harness manufactures a finding. `:determinate false` means
  re-read `range-map`, never 'it failed'.

  An unparsable body reads as `:success false :determinate false` — unknown,
  which is the only safe default for a mutation whose answer was lost."
  [node key-space split-key {:keys [timeout]}]
  (let [{:keys [status body]} (post* node "/v1/ranges/split"
                                     {:keySpace key-space :splitKey split-key}
                                     {:timeout (or timeout 60000)})
        body (when (map? body) body)]
    {:success        (true? (:success body))
     :outcome        (:status body)
     :determinate    (true? (:determinate body))
     :new-partition  (:newPartitionId body)
     :new-generation (:newGeneration body)
     :leader-hint    (:leaderHint body)
     :reason         (:reason body)
     :http-status    status}))

(defn merge-ranges!
  "Runs the merge pass on demand: POST /v1/ranges/merge. Folds adjacent
  under-min ranges back into one — the same work the periodic checker does.

  Leader-only, and refusing rather than answering 0 is the point: the trigger
  underneath returns 0 on a non-leader, which reads exactly like 'nothing was
  eligible'. `NotLeader` here is that ambiguity removed.

  No key-space argument: the pass scans every key-range space and the minimum
  size it enforces is server configuration (`--range-merge-min-size`), not a
  request parameter."
  [node {:keys [timeout]}]
  (let [{:keys [status body]} (post* node "/v1/ranges/merge" {}
                                     {:timeout (or timeout 60000)})
        body (when (map? body) body)]
    {:success     (true? (:success body))
     :outcome     (:status body)
     :determinate (true? (:determinate body))
     :merges      (:merges body)
     :leader-hint (:leaderHint body)
     :reason      (:reason body)
     :http-status status}))

(defn scan-range
  "Pages up to `limit` live keys from `[start, end)` under `prefix`, via POST
  /v1/kv/get-by-range. Returns `{:type kw :keys [str] :has-more bool}`.

  This is how the range nemesis finds a split key that is *known* to have keys
  on both sides of it. `prefix` is required by the server; nil `start`/`end`
  mean unbounded, matching a descriptor's ∓infinity bounds exactly, so a
  descriptor can be handed to this function as-is.

  Reads at `hlc-zero`, the 'latest committed' sentinel — not a very old
  snapshot. A scan is routed through the covering partitions' leaders, so under
  a network fault it fails rather than answering from a stale local view."
  [node prefix start end limit {:keys [timeout]}]
  (let [r (post! node "/v1/kv/get-by-range"
                 (cond-> {:prefix         prefix
                          :limit          limit
                          :startInclusive true
                          :endInclusive   false
                          :transactionId  hlc-zero
                          :readTimestamp  hlc-zero
                          :durability     persistent}
                   start (assoc :startKey start)
                   end   (assoc :endKey   end))
                 {:timeout (or timeout 10000)})]
    {:type     (kv-response-type (:type r))
     :keys     (into [] (keep :key) (:items r))
     :has-more (true? (:hasMore r))
     :status   (:status r)}))
