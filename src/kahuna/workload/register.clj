(ns kahuna.workload.register
  "Linearizable-register workload over Kahuna's key/value store.

  Each Jepsen key is one Kahuna key holding a small integer. Operations are
  read / write / compare-and-set, where CAS is expressed with Kahuna's
  SetIfEqualToValue flag (compare on value, not revision — the CAS the register
  model actually needs).

  Analysis is Knossos linearizability over jepsen's cas-register model, run
  per-key via jepsen.independent so the search space stays tractable."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [kahuna.client :as kc]
            [slingshot.slingshot :refer [try+]]))

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
          key   (str "jepsen/register/" k)
          opts  {:durability durability :timeout 5000}]
      (with-errors op
        (case (:f op)
          :read
          (let [r (kc/kv-get node key opts)]
            (case (:type r)
              :get            (assoc op :type :ok
                                        :value (independent/tuple k (parse-value (:value r))))
              :does-not-exist (assoc op :type :ok
                                        :value (independent/tuple k nil))
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
     :checker   (independent/checker
                  (checker/compose
                    {:linear   (checker/linearizable
                                 {:model     (model/cas-register)
                                  :algorithm :linear})
                     :timeline (timeline/html)}))
     ;; Per-key concurrency is the dominant term in Knossos's search cost — it
     ;; is roughly exponential in the number of processes concurrently touching
     ;; one key. 3 keeps histories checkable; 5+ regularly ran out of memory.
     :generator (independent/concurrent-generator
                  (:concurrency-per-key opts 3)
                  (range)
                  (fn [k]
                    (->> (gen/mix [r w cas])
                         (gen/limit (:ops-per-key opts 200)))))}))
