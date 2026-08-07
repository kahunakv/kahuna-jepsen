(ns kahuna.workload.snapshot-test
  "Negative controls for the snapshot checkers.

  Each checker gets a history containing a known violation (it must fail) AND
  a history that looks like a violation but is legal (it must pass). The second
  half carries the weight here: this workload's entire design rests on
  reclamation being allowed once a hold is released or its lease lapses, so a
  checker that asserted over unprotected reads would turn correct server
  behaviour into a red run on every test that ran long enough."
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [kahuna.client :as kc]
            [kahuna.workload.snapshot :as snap]))

(def test-map {:concurrency 2})

(def lease-ms 10000)
(def margin-ms 0)

(defn- ms [n] (long (* 1e6 n)))

(defn hlc [l] {:n 1 :l l :c 0})

(defn invoke
  ([process f at] (invoke process f at nil))
  ([process f at value]
   {:type :invoke :f f :process process :time (ms at) :value value}))

(defn pin-ok
  "A completed pin: hold `h` on key `k` at timestamp `t`, anchored to `v`."
  [process at h k t v]
  {:type :ok :f :pin :process process :time (ms at)
   :value {:hold-id h :key k :t t :value v :node "n1"}})

(defn read-ok
  ([process at h k t v] (read-ok process at h k t v t))
  ([process at h k t v last-modified]
   {:type :ok :f :read :process process :time (ms at)
    :value {:hold-id h :key k :t t :value v
            :last-modified last-modified :revision 3 :node "n1"}}))

(defn write-invoke [process at k v]
  (invoke process :write at {:key k :value v}))

(defn check* [c history]
  (checker/check c test-map history {}))

(defn stability [] (snap/stability-checker lease-ms margin-ms))

;; A pin at t=100 anchored to "v1", preceded by its invocation. Reused by most
;; of the histories below.
(defn pinned
  ([] (pinned 0))
  ([at]
   [(invoke 0 :pin at {:key 0})
    (pin-ok 0 (inc at) "h1" 0 (hlc 100) "v1")]))

;; ---------------------------------------------------------------------------
;; Stability — the core safety property
;; ---------------------------------------------------------------------------

(deftest a-pinned-snapshot-that-changes-its-answer-is-caught
  (let [history (concat (pinned)
                        [(invoke 1 :read 10)
                         (read-ok 1 11 "h1" 0 (hlc 100) "v9")])
        r (check* (stability) history)]
    (is (false? (:valid? r)))
    (is (= 1 (count (:violations r))))
    (is (= {:pinned "v1" :read "v9"}
           (select-keys (first (:violations r)) [:pinned :read])))))

(deftest history-vanishing-under-a-live-hold-is-caught
  ;; The symptom of reclamation running past the floor: the key reads as
  ;; absent at a timestamp where it provably had a value. This must NOT be
  ;; silently dropped as an uninteresting failed read.
  (let [history (concat (pinned)
                        [(invoke 1 :read 10)
                         (read-ok 1 11 "h1" 0 (hlc 100) nil)])
        r (check* (stability) history)]
    (is (false? (:valid? r)))
    (is (= 1 (:history-vanished r)))))

(deftest reads-agreeing-with-each-other-but-not-with-the-pin-are-caught
  ;; Comparing reads only to each other would pass this. The pinned value is
  ;; the anchor precisely so a snapshot that moved once, before any read, is
  ;; still a violation.
  (let [history (concat (pinned)
                        [(invoke 1 :read 10)
                         (read-ok 1 11 "h1" 0 (hlc 100) "v2")
                         (invoke 1 :read 12)
                         (read-ok 1 13 "h1" 0 (hlc 100) "v2")])
        r (check* (stability) history)]
    (is (false? (:valid? r)))
    (is (= 2 (count (:violations r))))))

(deftest a-stable-snapshot-passes
  (let [history (concat (pinned)
                        (mapcat (fn [i]
                                  [(invoke 1 :read (+ 10 i))
                                   (read-ok 1 (+ 10 i) "h1" 0 (hlc 100) "v1")])
                                (range 30)))
        r (check* (stability) history)]
    (is (true? (:valid? r)))
    (is (= 30 (:protected-reads r)))))

;; ---------------------------------------------------------------------------
;; Protection windows — where false positives would come from
;; ---------------------------------------------------------------------------

(deftest a-changed-answer-after-release-is-legal
  ;; Once the hold is gone the revision may be reclaimed, and a read as of that
  ;; timestamp may answer with anything older or nothing at all. Asserting here
  ;; would fail correct servers.
  (let [history (concat (pinned)
                        [(invoke 0 :release 10 nil)
                         {:type :ok :f :release :process 0 :time (ms 11)
                          :value {:hold-id "h1" :key 0 :t (hlc 100)}}
                         (invoke 1 :read 20)
                         (read-ok 1 21 "h1" 0 (hlc 100) nil)])
        r (check* (stability) history)]
    (is (not (false? (:valid? r))))
    (is (= 0 (:protected-reads r)))
    (is (= 1 (:unprotected-reads r)))))

(deftest protection-ends-when-the-release-is-sent-not-when-it-completes
  ;; A release that takes 5 s to answer under partition has already removed the
  ;; guarantee at the moment it was sent. Measuring to the completion would
  ;; credit protection to that whole window and manufacture a violation from a
  ;; read the server was entitled to answer differently.
  (let [history (concat (pinned)
                        [(invoke 0 :release 10 nil)
                         (invoke 1 :read 12)
                         (read-ok 1 13 "h1" 0 (hlc 100) nil)
                         {:type :ok :f :release :process 0 :time (ms 20)
                          :value {:hold-id "h1" :key 0 :t (hlc 100)}}])
        r (check* (stability) history)]
    (is (not (false? (:valid? r))))
    (is (= 0 (:protected-reads r)))))

(deftest an-indeterminate-release-still-ends-protection
  ;; A release that timed out may well have taken effect.
  (let [history (concat (pinned)
                        [(invoke 0 :release 10 nil)
                         {:type :info :f :release :process 0 :time (ms 11)
                          :value {:hold-id "h1"} :error :timeout}
                         (invoke 1 :read 20)
                         (read-ok 1 21 "h1" 0 (hlc 100) nil)])
        r (check* (stability) history)]
    (is (not (false? (:valid? r))))
    (is (= 0 (:protected-reads r)))))

(deftest a-changed-answer-after-the-lease-lapses-is-legal
  ;; No release, but the read lands past pin-invocation + lease. The hold is
  ;; not live, so the answer is free to change. Without this bound a long run
  ;; would accumulate expired holds and report correct reclamation as a bug.
  (let [history (concat (pinned)
                        [(invoke 1 :read (+ lease-ms 100))
                         (read-ok 1 (+ lease-ms 101) "h1" 0 (hlc 100) nil)])
        r (check* (stability) history)]
    (is (not (false? (:valid? r))))
    (is (= 0 (:protected-reads r)))))

(deftest a-renewal-extends-the-protection-window
  ;; The same read that was unprotected above becomes protected once a renewal
  ;; has pushed the lease out — so a violation there is real.
  (let [history (concat (pinned)
                        [(invoke 0 :renew (- lease-ms 100))
                         {:type :ok :f :renew :process 0
                          :time (ms (- lease-ms 90))
                          :value {:hold-id "h1"}}
                         (invoke 1 :read (+ lease-ms 100))
                         (read-ok 1 (+ lease-ms 101) "h1" 0 (hlc 100) nil)])
        r (check* (stability) history)]
    (is (false? (:valid? r)))
    (is (= 1 (:protected-reads r)))))

(deftest a-read-before-the-pin-completes-is-not-protected
  ;; Protection is only established by the pin's completion.
  (let [history [(invoke 0 :pin 0 {:key 0})
                 (invoke 1 :read 1)
                 (read-ok 1 2 "h1" 0 (hlc 100) nil)
                 (pin-ok 0 5 "h1" 0 (hlc 100) "v1")]
        r (check* (stability) history)]
    (is (not (false? (:valid? r))))
    (is (= 0 (:protected-reads r)))))

(deftest a-run-with-too-few-protected-reads-is-unknown-not-clean
  ;; The washout guard. An empty history satisfies "no snapshot ever moved"
  ;; perfectly, and must not read as a pass.
  (let [r (check* (stability) (pinned))]
    (is (= :unknown (:valid? r)))
    (is (some? (:insufficient-data r)))))

;; ---------------------------------------------------------------------------
;; Depth — did the run exercise retained history at all
;; ---------------------------------------------------------------------------

(defn write-ok [process at k v revision]
  {:type :ok :f :write :process process :time (ms at) :index (+ 1000 at)
   :value {:key k :value v :revision revision}})

(defn- indexed
  "Jepsen assigns :index; the depth pass needs it to tell reads apart."
  [ops]
  (map-indexed (fn [i op] (assoc op :index i)) ops))

(defn- depth-history
  "A pin plus 30 protected reads, all served at `served-rev`, with the key's
  newest acknowledged revision at `head-rev`."
  [head-rev served-rev]
  (indexed
    (concat (pinned)
            [(write-ok 0 2 0 "v1" head-rev)]
            (mapcat (fn [i]
                      [(invoke 1 :read (+ 10 i))
                       (assoc-in (read-ok 1 (+ 10 i) "h1" 0 (hlc 100) "v1")
                                 [:value :revision] served-rev)])
                    (range 30)))))

(deftest reads-served-entirely-at-head-are-unknown-not-clean
  ;; Every read was satisfied by the key's current version, so none of them
  ;; touched the retained history a hold exists to protect. Reporting this as a
  ;; pass would be the quietest possible no-op: 30 protected reads, no
  ;; violations, and nothing actually tested.
  (let [r (check* (stability) (depth-history 5 5))]
    (is (= :unknown (:valid? r)))
    (is (= 0 (:reads-below-head r)))
    (is (some? (:insufficient-data r)))))

(deftest reads-served-from-retained-history-are-a-real-pass
  (let [r (check* (stability) (depth-history 40 5))]
    (is (true? (:valid? r)))
    (is (= 30 (:reads-below-head r)))
    (is (= 35 (:max-depth r)))))

(deftest depth-uses-the-newest-acknowledged-revision-at-the-time-of-the-read
  ;; A write landing after a read must not deepen that read retroactively.
  (let [history (indexed
                  (concat (pinned)
                          [(write-ok 0 2 0 "a" 10)
                           (invoke 1 :read 5)
                           (assoc-in (read-ok 1 6 "h1" 0 (hlc 100) "v1")
                                     [:value :revision] 8)
                           (write-ok 0 7 0 "b" 90)]))
        depths (snap/read-depths history)]
    (is (= [2] (map :depth depths)))))

;; ---------------------------------------------------------------------------
;; Leakage — the server disagreeing with itself
;; ---------------------------------------------------------------------------

(deftest a-revision-committed-after-the-snapshot-is-caught
  (let [history [(invoke 1 :read 10)
                 (read-ok 1 11 "h1" 0 (hlc 100) "v2" (hlc 150))]
        r (check* (snap/leakage-checker) history)]
    (is (false? (:valid? r)))
    (is (= (hlc 150) (:last-modified (first (:violations r)))))))

(deftest a-revision-at-the-snapshot-itself-is-legal
  (let [history [(invoke 1 :read 10)
                 (read-ok 1 11 "h1" 0 (hlc 100) "v1" (hlc 100))]
        r (check* (snap/leakage-checker) history)]
    (is (true? (:valid? r)))))

(deftest leakage-is-caught-even-without-a-hold
  ;; This check needs no protection window: an expired lease cannot explain a
  ;; response that contradicts itself, so it applies to every read.
  (let [history [(invoke 1 :read 10)
                 (read-ok 1 11 "no-such-hold" 0 (hlc 100) "v2" (hlc 150))]
        r (check* (snap/leakage-checker) history)]
    (is (false? (:valid? r)))))

(deftest leakage-ordering-uses-the-counter-not-just-physical-time
  ;; Same millisecond, higher counter: still the future. A comparator that
  ;; stopped at :l would agree with the real one on nearly every other pair,
  ;; and disagree exactly here.
  (let [history [(invoke 1 :read 10)
                 (read-ok 1 11 "h1" 0 {:n 1 :l 100 :c 4} "v2" {:n 1 :l 100 :c 9})]
        r (check* (snap/leakage-checker) history)]
    (is (false? (:valid? r)))))

;; ---------------------------------------------------------------------------
;; Provenance
;; ---------------------------------------------------------------------------

(deftest a-value-nobody-wrote-is-caught
  (let [history [(write-invoke 0 1 0 "v1")
                 (invoke 1 :read 10)
                 (read-ok 1 11 "h1" 0 (hlc 100) "garbage")]
        r (check* (snap/provenance-checker) history)]
    (is (false? (:valid? r)))))

(deftest a-value-from-a-write-that-never-acknowledged-is-legal
  ;; The write timed out, so only its invocation is in the history — but it may
  ;; well have landed, and reading it back is correct behaviour.
  (let [history [(write-invoke 0 1 0 "v1")
                 {:type :info :f :write :process 0 :time (ms 2) :error :timeout}
                 (invoke 1 :read 10)
                 (read-ok 1 11 "h1" 0 (hlc 100) "v1")]
        r (check* (snap/provenance-checker) history)]
    (is (true? (:valid? r)))))

;; ---------------------------------------------------------------------------
;; Floor
;; ---------------------------------------------------------------------------

(deftest holds-vanishing-from-the-registry-are-caught
  (let [history (concat (pinned)
                        [(invoke 1 :floor 10)
                         {:type :ok :f :floor :process 1 :time (ms 11)
                          :value {:floor kc/hlc-zero :live-holds 0 :node "n1"}}])
        r (check* (snap/floor-checker lease-ms margin-ms) history)]
    (is (false? (:valid? r)))
    (is (= 1 (:expected-live (first (:violations r)))))))

(deftest an-empty-registry-with-no-live-holds-is-legal
  (let [history [(invoke 1 :floor 10)
                 {:type :ok :f :floor :process 1 :time (ms 11)
                  :value {:floor kc/hlc-zero :live-holds 0 :node "n1"}}]
        r (check* (snap/floor-checker lease-ms margin-ms) history)]
    (is (true? (:valid? r)))))

(deftest an-empty-registry-after-release-is-legal
  (let [history (concat (pinned)
                        [(invoke 0 :release 10 nil)
                         {:type :ok :f :release :process 0 :time (ms 11)
                          :value {:hold-id "h1"}}
                         (invoke 1 :floor 20)
                         {:type :ok :f :floor :process 1 :time (ms 21)
                          :value {:floor kc/hlc-zero :live-holds 0 :node "n1"}}])
        r (check* (snap/floor-checker lease-ms margin-ms) history)]
    (is (true? (:valid? r)))))

;; ---------------------------------------------------------------------------
;; HLC ordering
;; ---------------------------------------------------------------------------

(deftest hlc-ordering-matches-kommanders
  (is (neg? (kc/hlc-compare {:n 0 :l 1 :c 0} {:n 0 :l 2 :c 0})))
  (is (neg? (kc/hlc-compare {:n 0 :l 1 :c 0} {:n 0 :l 1 :c 1})))
  (is (neg? (kc/hlc-compare {:n 0 :l 1 :c 1} {:n 5 :l 1 :c 1})))
  (is (zero? (kc/hlc-compare {:n 1 :l 1 :c 1} {:n 1 :l 1 :c 1})))
  (is (pos? (kc/hlc-compare {:n 0 :l 2 :c 0} {:n 9 :l 1 :c 9}))))

(deftest zero-is-recognised-as-the-read-latest-sentinel
  (is (kc/hlc-zero? kc/hlc-zero))
  (is (kc/hlc-zero? nil))
  (is (not (kc/hlc-zero? (hlc 100)))))
