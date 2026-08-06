(ns kahuna.checker.recovery-test
  "Controls for the recovery-latency checker.

  This checker reports a number rather than a verdict, which makes it easier to
  get quietly wrong than a pass/fail checker: nothing goes red when the
  arithmetic is off, the run just carries a plausible-looking measurement. So
  each test fixes a history whose answer is known by construction and asserts
  the exact millisecond.

  The pairing test is the one that matters most. Nemesis invocations and their
  completions BOTH have :type :info, so they cannot be told apart by type; the
  checker relies on them strictly alternating. If that assumption breaks, every
  latency is measured from the wrong op and every number in every run is wrong
  while still looking reasonable."
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [kahuna.checker.recovery :as recovery]))

(def test-map {})

(defn check* [history]
  (checker/check (recovery/checker) test-map history {}))

(defn nem
  "A nemesis op at `ms` milliseconds. Jepsen stores :time in nanoseconds."
  [ms f value]
  {:type :info :process :nemesis :f f :time (* 1000000 ms) :value value})

(defn ok
  "A successful client op at `ms` milliseconds."
  [ms]
  {:type :ok :process 0 :f :txn :time (* 1000000 ms) :value [[:append 1 1]]})

(defn fail
  [ms]
  {:type :fail :process 0 :f :txn :time (* 1000000 ms) :error :connection-refused})

;; ---------------------------------------------------------------------------
;; Pairing: the assumption everything else rests on
;; ---------------------------------------------------------------------------

(deftest measures-from-the-completion-not-the-invocation
  ;; :kill invokes at 0 and completes at 100. :start invokes at 1000 and
  ;; completes at 3000 — the restart itself took 2 s. The first success is at
  ;; 3500.
  ;;
  ;; Recovery is 500 ms (3500 - 3000), NOT 2500 ms (3500 - 1000): the two
  ;; seconds jepsen spent restarting the process is not the cluster failing to
  ;; recover. Getting this backwards inflates every measurement by the duration
  ;; of the nemesis op.
  (let [r (check* [(nem 0 :kill :majority)
                   (nem 100 :kill {"n1" :killed})
                   (nem 1000 :start :all)
                   (nem 3000 :start {"n1" :started})
                   (ok 3500)])]
    (is (= 1 (:windows r)))
    (is (= 1 (:recovered r)))
    (is (= 500 (:recovered-after-ms (first (:detail r)))))))

;; ---------------------------------------------------------------------------
;; The measurement the open question needs
;; ---------------------------------------------------------------------------

(deftest a-window-cut-short-by-the-next-fault-is-not-a-recovery
  ;; The cluster comes back at 2000 and is killed again at 12000 having served
  ;; nothing. This is the shape of the runs that commit nothing: never
  ;; recovered, and the window is the 10 s the nemesis allowed.
  (let [r (check* [(nem 0 :kill :majority)
                   (nem 100 :kill {"n1" :killed})
                   (nem 1000 :start :all)
                   (nem 2000 :start {"n1" :started})
                   (fail 5000)
                   (fail 9000)
                   (nem 12000 :kill :majority)
                   (nem 12100 :kill {"n1" :killed})])]
    (is (= 1 (:windows r)))
    (is (zero? (:recovered r)))
    (is (= 1 (:never-recovered r)))
    (is (= [10000] (:starved-window-ms r)))
    ;; No successes at all, so there is no latency summary to report. An
    ;; absent :recovery-ms must not read as "recovered in 0 ms".
    (is (nil? (:recovery-ms r)))))

(deftest failed-operations-do-not-count-as-recovery
  ;; A :fail is the cluster answering "no" — often a clean rejection from a
  ;; node that knows it has no leader. Only an :ok proves it is serving again.
  (let [r (check* [(nem 0 :start-partition :majorities-ring)
                   (nem 100 :start-partition {"n1" :isolated})
                   (nem 1000 :stop-partition nil)
                   (nem 1100 :stop-partition :network-healed)
                   (fail 1200)
                   (fail 1300)
                   (ok 4100)])]
    (is (= 3000 (:recovered-after-ms (first (:detail r)))))))

(deftest window-open-at-the-end-of-the-history-is-censored-not-starved
  ;; The test simply stopped watching. Counting this as "never recovered" would
  ;; inflate the failure statistic once per run, since the final generator heals
  ;; the cluster and then the history ends.
  (let [r (check* [(nem 0 :kill :one)
                   (nem 100 :kill {"n1" :killed})
                   (nem 1000 :start :all)
                   (nem 2000 :start {"n1" :started})])]
    (is (= 1 (:windows r)))
    (is (zero? (:recovered r)))
    (is (zero? (:never-recovered r)))
    (is (= :history (:end (first (:detail r)))))))

;; ---------------------------------------------------------------------------
;; Fault coverage
;; ---------------------------------------------------------------------------

(deftest every-fault-ending-op-opens-a-window
  ;; :resume (pause), :stop-partition (partition), :start (kill) and :join
  ;; (membership) all end faults. A missing :f here means that fault's recovery
  ;; is silently unmeasured.
  (let [r (check* [(nem 0 :pause :one)         (nem 10 :pause {"n1" :paused})
                   (nem 100 :resume :all)      (nem 110 :resume {"n1" :resumed})
                   (ok 200)
                   (nem 300 :start-partition :one) (nem 310 :start-partition {})
                   (nem 400 :stop-partition nil)   (nem 410 :stop-partition :healed)
                   (ok 500)
                   (nem 600 :leave nil)        (nem 610 :leave {:removed true})
                   (nem 700 :join nil)         (nem 710 :join {:join :joined})
                   (ok 800)])]
    (is (= 3 (:windows r)))
    (is (= 3 (:recovered r)))
    (is (= [90 90 90] (map :recovered-after-ms (:detail r))))))

(deftest reports-a-latency-summary-across-windows
  (let [r (check* [(nem 0 :start :all)    (nem 10 :start {})   (ok 110)
                   (nem 200 :start :all)  (nem 210 :start {})  (ok 510)
                   (nem 600 :start :all)  (nem 610 :start {})  (ok 1310)])]
    (is (= {:min 100 :median 300 :p95 700 :max 700} (:recovery-ms r)))))

;; ---------------------------------------------------------------------------
;; Degenerate input
;; ---------------------------------------------------------------------------

(deftest a-history-with-no-faults-reports-nothing-rather-than-crashing
  (let [r (check* [(ok 100) (ok 200)])]
    (is (true? (:valid? r)))
    (is (zero? (:windows r)))))

(deftest empty-history-is-safe
  (let [r (check* [])]
    (is (true? (:valid? r)))
    (is (zero? (:windows r)))))

(deftest never-fails-a-run
  ;; The checker is an instrument. Even a history where nothing ever recovered
  ;; must report :valid? true, or every slow-but-correct run goes red.
  (let [r (check* [(nem 0 :start :all)
                   (nem 10 :start {})
                   (nem 20 :kill :all)
                   (nem 30 :kill {})])]
    (is (true? (:valid? r)))
    (is (= 1 (:never-recovered r)))))
