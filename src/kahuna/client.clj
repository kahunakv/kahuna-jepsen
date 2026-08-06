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

(defn- normalize
  "clj-http's :as :json only parses when the response *is* JSON. A 500 with an
  ASP.NET text/plain body, or an empty body, yields a String — assoc'ing onto
  that throws ClassCastException mid-history. Non-map bodies become a map with
  no :type, which `response-class` treats as indeterminate."
  [resp]
  (let [body (:body resp)]
    (if (map? body)
      (assoc body :status (:status resp))
      {:type nil :status (:status resp) :body body})))

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
  "Reads the latest committed value of `key`. Returns
  {:type kw :value str-or-nil :revision long}."
  [node key {:keys [durability timeout] :or {durability persistent}}]
  (let [r (post! node "/v1/kv/try-get"
                 {:transactionId hlc-zero
                  :key           key
                  :revision      -1
                  :readTimestamp hlc-zero
                  ;; NOTE: KahunaGetKeyValueRequest tags Durability with
                  ;; [JsonPropertyName("value")] — that is not a typo here.
                  :value         durability}
                 {:timeout timeout})
        t (kv-response-type (:type r))]
    {:type     t
     :value    (b64->str (:value r))
     :revision (:revision r)
     :raw      r}))

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
                  :lockId     (->b64 owner)
                  :expiresMs  expires-ms
                  :durability durability}
                 {:timeout timeout})]
    {:type (lock-response-type (:type r)) :fencing-token (:fencingToken r) :raw r}))

(defn try-unlock!
  [node resource owner {:keys [durability timeout] :or {durability persistent}}]
  (let [r (post! node "/v1/locks/try-unlock"
                 {:resource   resource
                  :lockId     (->b64 owner)
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

(defn cluster-membership
  "Reads /v1/cluster/membership — handy for debugging and for waiting until the
  cluster has actually formed before a test starts."
  [node]
  (get! node "/v1/cluster/membership" {:timeout 2000}))
