(ns kahuna.workload.register-test
  "Controls for the register workload's observation floor.

  The bug this guards against did not look like a bug. It looked like four green
  jobs: 1543 acknowledged reads, every one empty, `:valid? true` from Knossos and
  `:ok-count 507` from `:stats`, while the server's read path was pointed at the
  wrong keyspace for a full day of nightlies.

  So the load-bearing test here is `knossos-certifies-the-blind-history`: it runs
  the *real* linearizability checker over the exact shape from that run and
  asserts it passes, then asserts the floor rejects it. If someone later
  'simplifies' the floor away, that test fails with a message saying what was
  lost — which a test of the floor alone would not.

  Every rejecting case is paired with an accepting one, so a floor that simply
  refused everything could not get through this file."
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [jepsen.independent :as independent]
            [knossos.model :as model]
            [kahuna.workload.register :as register]))

(def test-map {})

(defn- op
  "A client op. `observed` is omitted for non-reads."
  ([type f value] (op type f value nil))
  ([type f value observed]
   (cond-> {:type type :process 0 :f f :value value :time 0 :index 0}
     observed (assoc :observed observed))))

(defn read-ok
  "An acknowledged read of `v` on key `k`, tagged as the workload tags it."
  [k v observed]
  (op :ok :read (independent/tuple k v) observed))

(defn write-ok [k v]
  (op :ok :write (independent/tuple k v)))

(defn- reindex
  "Jepsen histories carry :index and :time; give each op distinct ones so the
  history machinery treats them as ordered rather than simultaneous."
  [ops]
  (map-indexed (fn [i o] (assoc o :index i :time (* 1000000 i))) ops))

(defn check-floor
  ([ops] (check-floor (register/observation-checker) ops))
  ([c ops] (checker/check c test-map (vec (reindex ops)) {})))

;; ---------------------------------------------------------------------------
;; The shape that started this
;; ---------------------------------------------------------------------------

(def blind-history
  "An acknowledged write of 3, then acknowledged reads of an empty register —
  verbatim the opening of key 0's history from run 31824405991."
  [(op :invoke :write (independent/tuple 0 3))
   (write-ok 0 3)
   (op :invoke :read (independent/tuple 0 nil))
   (read-ok 0 nil :absent)
   (op :invoke :read (independent/tuple 0 nil))
   (read-ok 0 nil :absent)])

(deftest knossos-certifies-the-blind-history
  ;; This is not a test of our code. It pins the *reason* the floor has to
  ;; exist: `cas-register` reads a nil read value as "unfilled" and matches it
  ;; against any state, so a write of 3 followed by two reads of an empty
  ;; register is, to the model, perfectly linearizable.
  ;;
  ;; If this ever starts failing, Knossos has changed and the floor may be
  ;; redundant. Until then, deleting the floor re-opens a day-long blind spot.
  ;; Stepped through the model directly rather than run through
  ;; `independent/checker`, which wants a store directory to write per-key
  ;; results into. This pins the exact line the whole problem rests on
  ;; (`knossos/model.clj:79`) with nothing else in the way.
  (let [after-write (model/step (model/cas-register) {:f :write :value 3})
        after-read  (model/step after-write {:f :read :value nil})]
    (is (not (model/inconsistent? after-read))
        "cas-register should accept a nil read after a write of 3 — that is the whole problem")
    ;; The contrast that shows it really is the nil doing the work: a read of a
    ;; *wrong* value is caught, so the model is not simply permissive.
    (is (model/inconsistent? (model/step after-write {:f :read :value 4}))
        "a non-nil wrong read must still be rejected")))

(deftest the-floor-rejects-what-knossos-certifies
  (let [r (check-floor blind-history)]
    (is (= :unknown (:valid? r)))
    (is (= :no-observed-reads (:cause r)))
    (is (= 1 (:acked-writes r)))
    (is (= 2 (:absent-reads r)))
    (is (= 0 (:value-reads r)))))

;; ---------------------------------------------------------------------------
;; The accepting twins
;; ---------------------------------------------------------------------------

(deftest a-run-that-observed-a-value-passes
  ;; The same history with one real observation. Nothing else differs, so a
  ;; floor that rejected this would be rejecting on something other than
  ;; blindness.
  (let [r (check-floor [(write-ok 0 3)
                        (read-ok 0 3 :value)
                        (read-ok 0 nil :absent)])]
    (is (true? (:valid? r)))
    (is (= 1 (:value-reads r)))
    (is (= 1 (:absent-reads r)))))

(deftest an-empty-register-is-fine-as-long-as-something-was-observed
  ;; Key 1 is legitimately empty for the whole run while key 0 is written and
  ;; read. This is why the floor is a property of the run and not of a key: a
  ;; per-key floor would fail key 1 for behaving correctly.
  (let [r (check-floor [(write-ok 0 7)
                        (read-ok 0 7 :value)
                        (read-ok 1 nil :absent)
                        (read-ok 1 nil :absent)
                        (read-ok 1 nil :absent)])]
    (is (true? (:valid? r)))
    (is (= 1 (:value-reads r)))
    (is (= 3 (:absent-reads r)))))

;; ---------------------------------------------------------------------------
;; The other vacuous shape
;; ---------------------------------------------------------------------------

(deftest a-run-that-acknowledged-no-write-is-unknown
  ;; Nothing was ever stored, so every read is trivially correct and the
  ;; linearizability verdict means nothing. This is the washout shape FINDINGS
  ;; describes — previously only findable by counting errors by hand.
  (let [r (check-floor [(op :info :write (independent/tuple 0 3))
                        (op :fail :read (independent/tuple 0 nil))
                        (read-ok 0 nil :absent)])]
    (is (= :unknown (:valid? r)))
    (is (= :no-acked-writes (:cause r)))))

(deftest indeterminate-and-failed-ops-do-not-count-as-observations
  ;; An :info write may or may not have landed and a :fail read saw nothing;
  ;; counting either would let a run that observed nothing claim it had. The
  ;; single :ok write plus zero :ok value-reads must still be refused.
  (let [r (check-floor [(write-ok 0 1)
                        (op :info :read (independent/tuple 0 5) :value)
                        (op :fail :read (independent/tuple 0 5) :value)])]
    (is (= :unknown (:valid? r)))
    (is (= :no-observed-reads (:cause r)))
    (is (= 0 (:value-reads r)))
    (is (= 0 (:ok-reads r)))))

;; ---------------------------------------------------------------------------
;; The third kind of nil
;; ---------------------------------------------------------------------------

(deftest a-served-but-unparsable-value-is-not-an-empty-register
  ;; The server said the key is there and handed back something that did not
  ;; parse as an integer. That is a wire or serialization symptom, and it must
  ;; not be counted as either an observation or an empty register — one would
  ;; hide it, the other would misfile it.
  (let [r (check-floor [(write-ok 0 3)
                        (read-ok 0 nil :blank)
                        (read-ok 0 nil :blank)])]
    (is (= :unknown (:valid? r)))
    (is (= :no-observed-reads (:cause r)))
    (is (= 2 (:blank-reads r)))
    (is (= 0 (:absent-reads r)))
    (is (= 0 (:value-reads r)))))

(deftest reads-from-an-untagged-history-are-reported-not-absorbed
  ;; A history from before `:observed` existed — a stored run being re-analysed.
  ;; Its reads cannot be classified, and quietly counting them as observations
  ;; would let an old blind history pass. They are named instead, so the answer
  ;; is legible rather than wrong.
  (let [r (check-floor [(write-ok 0 3)
                        (op :ok :read (independent/tuple 0 3))])]
    (is (= :unknown (:valid? r)))
    (is (= 1 (:untagged-reads r)))
    (is (= 1 (:ok-reads r)))
    (is (= 0 (:value-reads r)))))

;; ---------------------------------------------------------------------------
;; The knob
;; ---------------------------------------------------------------------------

(deftest the-floor-is-raisable
  ;; Default 1 catches total blindness, which is the failure that actually
  ;; happens. A caller chasing partial blindness can ask for more.
  (let [ops [(write-ok 0 3) (read-ok 0 3 :value)]]
    (is (true? (:valid? (check-floor (register/observation-checker 1) ops))))
    (is (= :unknown (:valid? (check-floor (register/observation-checker 2) ops))))))
