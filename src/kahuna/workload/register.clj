(ns kahuna.workload.register
  "Linearizable-register workload over Kahuna's key/value store.

  Each Jepsen key is one Kahuna key holding a small integer. Operations are
  read / write / compare-and-set, where CAS is expressed with Kahuna's
  SetIfEqualToValue flag (compare on value, not revision — the CAS the register
  model actually needs).

  Analysis is Knossos linearizability over jepsen's cas-register model, run
  per-key via jepsen.independent so the search space stays tractable.

  ## Why there is a second checker

  Knossos alone will certify a run in which **not one read ever returned a
  value**. It did exactly that for a full day of nightlies: 1543 acknowledged
  reads across four jobs, every one of them empty, four green checkmarks, while
  the server's whole KV read path was pointed at the wrong keyspace (see
  FINDINGS.md).

  Two individually-correct decisions compose into the hole. This workload reports
  Kahuna's `DoesNotExist` as `:ok` with value `nil`, because an unwritten
  register *is* nil and downgrading it to `:fail` would throw away real
  information. And `knossos.model/cas-register` reads a `nil` read value as
  *unfilled* — a Jepsen idiom, since a client fills the value in on completion —
  so it matches against any state:

      :read (if (or (nil? (:value op)) (= value (:value op))) r (inconsistent …))

  A read this workload means as \"the register is empty\" therefore arrives at the
  model as \"we did not observe anything\", and every one of them is a free pass.
  `:stats` does not help: those reads all have `:type :ok`, so by its measure the
  read path was the healthiest thing in the run.

  The fix is not to change either decision — both are right — but to stop the two
  `nil`s being indistinguishable. `invoke!` now tags every acknowledged read with
  what it actually saw (`:observed`), leaving `:value` exactly as Knossos needs
  it, and `observation-checker` makes the floor a property of the run: a history
  with acknowledged writes and no observed value is `:unknown`, never `true`."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [kahuna.client :as kc]
            [slingshot.slingshot :refer [try+]]))

(def key-space
  "The Kahuna key space these registers live in — a key's prefix up to, and
  excluding, its last `/`. Defined here rather than spelled out at the call site
  because `--key-range` registers *this string* for key-range routing, and a key
  space that does not match the keys actually written would range-route an empty
  space while the workload carried on hashing."
  "jepsen/register")

(defn- parse-value
  "Kahuna stores opaque bytes; we store decimal integers as UTF-8."
  [s]
  (when (and s (seq s)) (Long/parseLong s)))

(defmacro with-errors
  "Turns transport-level failures into Jepsen ops. A timeout or connection reset
  on a write is *indeterminate* (:info) — the proposal may still commit — so
  only ops we know never reached the server may be downgraded to :fail. Reads
  are safe to fail either way, but we keep the same conservative mapping to
  avoid a read accidentally asserting something."
  [op & body]
  `(try+
     ~@body
     (catch java.net.ConnectException e#
       (assoc ~op :type :fail, :error :connection-refused))
     (catch java.net.SocketTimeoutException e#
       (assoc ~op :type :info, :error :timeout))
     (catch java.net.UnknownHostException e#
       (assoc ~op :type :fail, :error :unknown-host))
     (catch org.apache.http.NoHttpResponseException e#
       (assoc ~op :type :info, :error :no-http-response))
     (catch java.io.IOException e#
       (assoc ~op :type :info, :error [:io (.getMessage e#)]))))

(defrecord RegisterClient [node durability]
  client/Client
  (open! [this test n]
    (assoc this :node n))

  (setup! [this test])

  (invoke! [this test op]
    (let [[k v] (:value op)
          key   (str key-space "/" k)
          opts  {:durability durability :timeout 5000}]
      (with-errors op
        (case (:f op)
          ;; `:value` is what Knossos checks and is left exactly as the model
          ;; wants it — nil for an empty register. `:observed` is the extra
          ;; field that says which *kind* of nil this is, and nothing but
          ;; `observation-checker` reads it. Keeping them separate is the whole
          ;; point: encoding the distinction into `:value` (a sentinel like
          ;; :absent) would make `cas-register` compare :absent against nil and
          ;; report every legitimately-empty read as a violation.
          :read
          (let [r (kc/kv-get node key opts)]
            (case (:type r)
              :get
              (let [v (parse-value (:value r))]
                (assoc op :type :ok
                          :value (independent/tuple k v)
                          ;; A `Get` that yields no parsable integer is not an
                          ;; empty register: the server said the key is there
                          ;; and handed back something this workload cannot
                          ;; read. Counted apart from :absent because it is a
                          ;; wire or serialization symptom, and lumping it in
                          ;; with "the register is empty" is how it would go
                          ;; unnoticed.
                          :observed (if (nil? v) :blank :value)))

              :does-not-exist
              (assoc op :type :ok
                        :value (independent/tuple k nil)
                        :observed :absent)

              ;; Anything else (must-retry, errored, …) tells us nothing.
              (assoc op :type :fail :error (:type r))))

          :write
          (let [r (kc/kv-set node key (str v) opts)]
            (assoc op :type (kc/response-class (:type r))
                      :error (when-not (= :set (:type r)) (:type r))))

          :cas
          (let [[old new] v
                r (kc/kv-cas! node key (str old) (str new) opts)]
            (case (:type r)
              :set     (assoc op :type :ok)
              :not-set (assoc op :type :fail :error :cas-mismatch)
              ;; DoesNotExist means the key was absent, so the CAS definitely
              ;; did not apply.
              :does-not-exist (assoc op :type :fail :error :does-not-exist)
              (assoc op :type (kc/response-class (:type r)) :error (:type r))))))))

  (teardown! [this test])

  (close! [this test]))

;; ---------------------------------------------------------------------------
;; The observation floor
;; ---------------------------------------------------------------------------

(defn observations
  "Tallies what a run's acknowledged operations actually observed.

  Counts only `:ok` client ops — an `:info` write may or may not have landed and
  a `:fail` read saw nothing, so neither can testify either way.

  Returns `{:acked-writes, :ok-reads, :value-reads, :absent-reads,
  :blank-reads}`. The three read counts partition `:ok-reads`, so a run whose
  reads do not add up is carrying ops from an older harness that did not tag
  `:observed`, and `:untagged-reads` says so rather than being silently folded
  into one of the others."
  [history]
  (->> (h/history history)
       (h/filter h/client-op?)
       (h/filter h/ok?)
       (reduce (fn [acc op]
                 (case (:f op)
                   (:write :cas) (update acc :acked-writes inc)
                   :read (-> acc
                             (update :ok-reads inc)
                             (update (case (:observed op)
                                       :value  :value-reads
                                       :absent :absent-reads
                                       :blank  :blank-reads
                                       :untagged-reads)
                                     inc))
                   acc))
               {:acked-writes 0 :ok-reads 0 :value-reads 0
                :absent-reads 0 :blank-reads 0 :untagged-reads 0})))

(defn observation-checker
  "Refuses to certify a run that observed nothing.

  This checker cannot find a linearizability violation and does not try. It
  answers one question Knossos structurally cannot: *did the read path work at
  all?* A history of acknowledged writes and uniformly empty reads is
  indistinguishable, to `cas-register`, from a history of reads whose values were
  never filled in — so it passes. Here it is `:unknown`.

  Two shapes are refused, and both mean 'this run cannot settle anything':

  * **no acknowledged writes** — nothing was ever stored, so every read is
    correct by default and the linearizability verdict is vacuous. This is the
    washout shape already written up in FINDINGS.md, now visible from the
    verdict instead of by counting errors by hand.
  * **acknowledged writes, no observed value** — the read path returned nothing
    for the whole run. This is the 1543-blind-reads case.

  `min-value-reads` defaults to 1 deliberately. A higher floor would be guessing:
  the failure this guards against is *total* blindness — a renamed wire field, a
  misdirected keyspace — which takes the count to exactly zero. A run that reads
  10 values out of 500 is suspicious rather than broken, and the counts are in
  the result so a human can see the ratio; inventing a threshold for it would
  trade a real guard for a flaky one.

  Always reports the counts, pass or fail. The number that made the original bug
  diagnosable could only be got by hand-processing the history, which is most of
  why it took a day."
  ([] (observation-checker 1))
  ([min-value-reads]
   (reify checker/Checker
     (check [_ test history _opts]
       (let [{:keys [acked-writes value-reads] :as obs} (observations history)]
         (cond
           (zero? acked-writes)
           (assoc obs :valid? :unknown :cause :no-acked-writes)

           (< value-reads min-value-reads)
           (assoc obs :valid? :unknown
                      :cause :no-observed-reads
                      :min-value-reads min-value-reads)

           :else
           (assoc obs :valid? true)))))))

(defn- r [_ _] {:type :invoke, :f :read,  :value nil})
(defn- w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn- cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn workload
  "Options:
    :durability   kahuna.client/persistent (default) or /ephemeral
    :key-count    how many independent registers to exercise concurrently
    :ops-per-key  history length per key before rotating to a fresh one"
  [opts]
  (let [durability (:durability opts kc/persistent)]
    {:client    (RegisterClient. nil durability)
     :key-space key-space
     :checker   (checker/compose
                  {:linear
                   (independent/checker
                     (checker/compose
                       ;; :wgl (Wing-Gong with Lowe's optimizations) uses far
                       ;; less memory than :linear on histories with many
                       ;; indeterminate ops — and combined partition+kill runs
                       ;; are ~27% :info, which OOM'd :linear even at -Xmx11g.
                       {:linear   (checker/linearizable
                                    {:model     (model/cas-register)
                                     :algorithm (:linearizable-algorithm opts :wgl)})
                        :timeline (timeline/html)}))
                   ;; Runs over the WHOLE history, not per key, and that is the
                   ;; point. One register legitimately holding nil for a run is
                   ;; ordinary; the run as a whole never observing a value is
                   ;; the read path being broken. A per-key floor would fire on
                   ;; the first and miss the second.
                   :observations (observation-checker)})
     ;; Per-key concurrency is the dominant term in Knossos's search cost — it
     ;; is roughly exponential in the number of processes concurrently touching
     ;; one key. 3 keeps histories checkable; 5+ regularly ran out of memory.
     :generator (independent/concurrent-generator
                  (:concurrency-per-key opts 3)
                  (range)
                  (fn [k]
                    (->> (gen/mix [r w cas])
                         (gen/limit (:ops-per-key opts 200)))))}))
