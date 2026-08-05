(ns kahuna.workload.lock
  "Mutual-exclusion workload over Kahuna's distributed locks.

  Kahuna locks are *leased*: `LockActor` grants a lock until
  `now + expiresMs`, and once that deadline passes another owner may take it
  even though the original holder never released and may still believe it holds
  the lock. That is deliberate — it is what stops a crashed holder from wedging
  the resource forever — and it means a checker that simply flags two
  overlapping holders would report Kahuna's designed behaviour as a bug.

  So this workload checks the two properties that survive lease expiry:

    exclusion — two owners' *definite* hold windows must not overlap. A hold
                window is trimmed to the earliest instant the lease could have
                expired, so an overlap explainable by expiry is never reported.

    fencing   — the guarantee that makes leases safe downstream. Tokens must
                never go backwards, and must strictly increase when the lock
                changes hands. Re-acquisition by the current holder returning
                the same token is expected (LockActor returns
                `entry.FencingToken` unchanged in that case) and is not a
                violation."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [kahuna.client :as kc]
            [slingshot.slingshot :refer [try+]]))

;; ---------------------------------------------------------------------------
;; Client
;; ---------------------------------------------------------------------------

(def resource
  "A single contended resource: mutual exclusion is a per-resource property, and
  one hot resource finds violations faster than many cold ones."
  "jepsen/lock")

(defmacro with-errors
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

(defrecord LockClient [node owner durability expires-ms]
  client/Client
  (open! [this test n]
    ;; One stable owner id per client. A process that crashes gets a new Jepsen
    ;; process number but keeps this client — and therefore keeps its identity
    ;; as far as Kahuna is concerned, which is what makes lease reasoning work.
    (assoc this :node n :owner (str "jepsen-" (java.util.UUID/randomUUID))))

  (setup! [this test])

  (invoke! [this test op]
    (let [opts {:durability durability :timeout 5000}]
      (with-errors op
        (case (:f op)
          :acquire
          (let [r (kc/try-lock! node resource owner expires-ms opts)]
            (case (:type r)
              ;; :node is diagnostic, not part of any checked property — when a
              ;; fencing token regresses, the first question is which node
              ;; served the stale value.
              :locked (assoc op :type :ok
                                :value {:owner owner
                                        :token (:fencing-token r)
                                        :node  node})
              :busy   (assoc op :type :fail :error :busy)
              (assoc op :type (kc/response-class (:type r)) :error (:type r))))

          :release
          (let [r (kc/try-unlock! node resource owner opts)]
            (case (:type r)
              :unlocked (assoc op :type :ok :value {:owner owner})
              ;; We were not the holder — the lease expired and someone else
              ;; took it, or we never held it. Definitely no state change.
              :invalid-owner       (assoc op :type :fail :error :invalid-owner)
              :lock-does-not-exist (assoc op :type :fail :error :does-not-exist)
              (assoc op :type (kc/response-class (:type r)) :error (:type r))))))))

  (teardown! [this test])

  (close! [this test]))

;; ---------------------------------------------------------------------------
;; History reconstruction
;; ---------------------------------------------------------------------------

(defn- thread-of
  "Jepsen renumbers a process after it crashes (new process = old +
  concurrency), but the underlying thread — and therefore the lock owner —
  stays the same. Recover the thread so holds are attributed to the owner
  rather than to a transient process number."
  [test op]
  (let [c (:concurrency test)]
    (if (and c (pos? c) (number? (:process op)))
      (mod (:process op) c)
      (:process op))))

(defn holds
  "Reconstructs the intervals during which each owner *definitely* held the
  lock. Deliberately conservative at both ends, so an overlap it reports cannot
  be explained away by lease expiry or by uncertainty about when the server
  acted:

    start — the acquire's *completion* time. The server granted at or before
            this instant, so from here on the owner certainly held it.
    end   — the earliest of:
              * invoke-time + expires-ms  (the soonest the lease could lapse;
                the server started the clock at or after our invoke)
              * the instant a release was invoked (before that we certainly
                still held it; after, we may not)
            minus a safety margin for clock-rate differences between the
            control node and the server.

  Ops that end in :info are dropped: an acquire whose outcome is unknown cannot
  support a claim about who held what."
  [test history expires-ms margin-ms]
  (let [expires-ns (* 1000000 (long expires-ms))
        margin-ns  (* 1000000 (long margin-ms))]
    (loop [ops      (seq history)
           invokes  {}    ; thread -> invoke time of in-flight op
           pending  {}    ; thread -> partially built hold
           done     []]
      (if-not ops
        ;; Anything still held at the end of the history runs to lease expiry.
        (concat done (keep (fn [[_ h]]
                             (let [end (- (:lease-end h) margin-ns)]
                               (when (< (:start h) end)
                                 (assoc h :end end))))
                           pending))
        (let [op (first ops)
              t  (thread-of test op)]
          (case [(:type op) (:f op)]
            [:invoke :acquire]
            (recur (next ops) (assoc invokes t (:time op)) pending done)

            [:ok :acquire]
            (let [inv (get invokes t (:time op))]
              (recur (next ops) (dissoc invokes t)
                     (assoc pending t {:owner      (get-in op [:value :owner])
                                       :token      (get-in op [:value :token])
                                       :thread     t
                                       :start      (:time op)
                                       :lease-end  (+ inv expires-ns)})
                     done))

            [:invoke :release]
            ;; Mark when the release began: up to that instant the owner
            ;; certainly still held the lock.
            (recur (next ops) (assoc invokes t (:time op))
                   (if-let [h (get pending t)]
                     (assoc pending t (assoc h :release-start (:time op)))
                     pending)
                   done)

            (let [h (get pending t)]
              (if (and h (#{:ok :fail} (:type op)) (= :release (:f op)))
                (let [end (- (min (:lease-end h)
                                  (or (:release-start h) (:time op)))
                             margin-ns)]
                  (recur (next ops) (dissoc invokes t) (dissoc pending t)
                         (if (< (:start h) end)
                           (conj done (assoc h :end end))
                           done)))
                (recur (next ops) invokes pending done)))))))))

;; ---------------------------------------------------------------------------
;; Checkers
;; ---------------------------------------------------------------------------

(defn exclusion-checker
  "Fails if two different owners definitely held the lock at the same time."
  [expires-ms margin-ms]
  (reify checker/Checker
    (check [this test history opts]
      (let [hs         (sort-by :start (holds test history expires-ms margin-ms))
            violations (->> (partition 2 1 hs)
                            (keep (fn [[a b]]
                                    (when (and (not= (:owner a) (:owner b))
                                               (< (:start b) (:end a)))
                                      {:a a :b b
                                       :overlap-ns (- (:end a) (:start b))})))
                            (into []))]
        {:valid?      (empty? violations)
         :hold-count  (count hs)
         :violations  (take 10 violations)}))))

(defn fencing-checker
  "Fails if fencing tokens go backwards, or fail to increase when the lock
  changes hands.

  Only compares acquisitions that are genuinely ordered — A completed strictly
  before B was invoked. Concurrent acquisitions have no required order, and
  comparing them would invent violations that the protocol never promised."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [;; One pass, carrying each thread's in-flight invoke time, so every
            ;; completed acquire knows when it started. Pairing by thread (not
            ;; process) survives the renumbering that follows a crash.
            acquires (->> (reduce
                            (fn [[invokes acc] op]
                              (let [t (thread-of test op)]
                                (cond
                                  (and (= :invoke (:type op)) (= :acquire (:f op)))
                                  [(assoc invokes t (:time op)) acc]

                                  (and (= :ok (:type op)) (= :acquire (:f op)))
                                  [(dissoc invokes t)
                                   (conj acc {:owner  (get-in op [:value :owner])
                                              :token  (get-in op [:value :token])
                                              :node   (get-in op [:value :node])
                                              :time   (:time op)
                                              :invoke (get invokes t (:time op))})]

                                  :else [invokes acc])))
                            [{} []]
                            history)
                          second
                          (filter :token)
                          (sort-by :time)
                          vec)
            ;; Running max token over completions, so a later acquire can be
            ;; compared against everything that finished before it started.
            prefix   (reduce (fn [acc a]
                               (let [best (peek acc)]
                                 (conj acc (if (or (nil? best)
                                                   (> (:token a) (:token best)))
                                             a best))))
                             [] acquires)
            violations
            (->> acquires
                 (keep-indexed
                   (fn [i b]
                     ;; Everything completed strictly before b started.
                     (let [b-start (:invoke b)
                           j (loop [lo 0, hi (long (dec i)), best -1]
                               (if (> lo hi)
                                 best
                                 (let [mid (quot (+ lo hi) 2)]
                                   (if (< (:time (nth acquires mid)) b-start)
                                     (recur (inc mid) hi mid)
                                     (recur lo (dec mid) best)))))]
                       (when (<= 0 j)
                         (let [a (nth prefix j)]
                           (cond
                             (< (:token b) (:token a))
                             {:type :token-went-backwards :earlier a :later b}

                             (and (= (:token b) (:token a))
                                  (not= (:owner b) (:owner a)))
                             {:type :token-reused-by-other-owner
                              :earlier a :later b}))))))
                 (into []))]
        {:valid?        (empty? violations)
         :acquire-count (count acquires)
         :max-token     (:token (peek prefix))
         :violations    (take 10 violations)}))))

;; ---------------------------------------------------------------------------
;; Workload
;; ---------------------------------------------------------------------------

(defn workload
  "Options:
    :durability      kahuna.client/persistent (default) or /ephemeral
    :lock-expires-ms lease length handed to try-lock
    :lease-margin-ms slack subtracted from hold windows to absorb clock-rate
                     differences between the control node and the servers"
  [opts]
  (let [expires-ms (:lock-expires-ms opts 10000)
        margin-ms  (:lease-margin-ms opts 500)]
    {:client    (LockClient. nil nil (:durability opts kc/persistent) expires-ms)
     :checker   (checker/compose
                  {:exclusion (exclusion-checker expires-ms margin-ms)
                   :fencing   (fencing-checker)
                   :timeline  (timeline/html)})
     ;; Each thread cycles acquire → release, so the history is full of genuine
     ;; hand-offs rather than one lucky thread hoarding the lock.
     :generator (gen/each-thread
                  (cycle [{:type :invoke, :f :acquire}
                          {:type :invoke, :f :release}]))}))
