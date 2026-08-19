(ns kahuna.nemesis.range-test
  "Controls for the range nemesis's choice of split key.

  This is the function the whole fault rests on. The server refuses a split
  whose key falls outside the covering range (`InvalidSplitKey`) and one that
  would leave either half empty (`BelowMinRangeSize`), so a nemesis that guessed
  at keys would spend a run collecting refusals and then report a vacuous pass
  with a straight face — the exact shape this suite keeps rediscovering.

  The guarantee is therefore not 'usually picks something reasonable' but: given
  two or more distinct live keys, the result is **strictly greater than the
  smallest and no greater than the largest**. That is precisely the server's
  precondition, so a split built on it cannot be refused for either reason.
  These tests pin that inequality rather than specific return values, because it
  is the inequality the server checks."
  (:require [clojure.test :refer :all]
            [kahuna.nemesis.range :as range-nemesis]))

(defn- valid-split-point?
  "Does `k` satisfy exactly what the server demands of a split key for a page of
  scanned keys — a non-empty left half and a non-empty right half?"
  [ks k]
  (let [sorted (sort (distinct (remove nil? ks)))]
    (and (some? k)
         ;; Something below it, so [start, k) holds a key.
         (neg? (compare (first sorted) k))
         ;; k itself is live, so [k, end) holds a key.
         (some #{k} sorted))))

;; ---------------------------------------------------------------------------
;; When there is nowhere to cut, say so
;; ---------------------------------------------------------------------------

(deftest a-range-with-too-few-keys-yields-no-split-point
  ;; nil, not a bound. Returning the single key would be a split whose left half
  ;; is empty; returning the range's own start would be InvalidSplitKey. Both
  ;; are refusals the nemesis would then have to explain away.
  (is (nil? (range-nemesis/median-key [])))
  (is (nil? (range-nemesis/median-key ["jepsen/register/1"])))
  (is (nil? (range-nemesis/median-key [nil nil]))))

(deftest repeated-keys-do-not-look-like-two-keys
  ;; A page can legitimately repeat — the same key at several revisions. Counting
  ;; rows rather than distinct keys would return the one key present and produce
  ;; a split with an empty left half.
  (is (nil? (range-nemesis/median-key ["jepsen/register/4"
                                       "jepsen/register/4"
                                       "jepsen/register/4"]))))

;; ---------------------------------------------------------------------------
;; When there is, the choice satisfies the server's precondition
;; ---------------------------------------------------------------------------

(deftest two-keys-cut-between-them
  (let [ks ["jepsen/register/1" "jepsen/register/2"]
        k  (range-nemesis/median-key ks)]
    (is (= "jepsen/register/2" k))
    (is (valid-split-point? ks k))))

(deftest the-split-point-always-leaves-both-halves-non-empty
  ;; The load-bearing property, over the shapes a real scan produces: an even
  ;; page, an odd page, an unsorted one, and one with duplicates mixed in.
  (doseq [ks [["jepsen/register/0" "jepsen/register/1"]
              ["jepsen/register/0" "jepsen/register/1" "jepsen/register/2"]
              ["jepsen/register/2" "jepsen/register/0" "jepsen/register/1"]
              ["jepsen/register/0" "jepsen/register/0" "jepsen/register/9"]
              (mapv #(str "jepsen/register/" %) (range 64))]]
    (let [k (range-nemesis/median-key ks)]
      (is (valid-split-point? ks k)
          (str "no valid split point for " (pr-str ks))))))

(deftest keys-are-ordered-ordinally
  ;; The server compares range bounds ordinally, so "…/10" sorts *between*
  ;; "…/1" and "…/2" — which makes it the median of these three, and "…/2" the
  ;; answer a numeric ordering would give. Picking "…/2" would not merely
  ;; bisect unevenly: on a descriptor already cut at "…/10" it falls in the
  ;; upper half while the nemesis believes it is splitting the lower one, and
  ;; the server answers InvalidSplitKey.
  (let [ks ["jepsen/register/1" "jepsen/register/10" "jepsen/register/2"]]
    (is (= "jepsen/register/10" (range-nemesis/median-key ks)))
    (is (valid-split-point? ks (range-nemesis/median-key ks)))))

(deftest a-page-of-nils-and-one-key-is-not-splittable
  ;; The scan's items carry an optional key; a row without one tells us nothing
  ;; and must not be counted as a distinct key.
  (is (nil? (range-nemesis/median-key [nil "jepsen/register/1" nil]))))

;; ---------------------------------------------------------------------------
;; Retrying a failed split
;; ---------------------------------------------------------------------------
;;
;; `TransferFailed` became a *loud* failure in Kahuna's `cfbb55d2` — the copy
;; phase refusing a contended scan page rather than silently truncating it — and
;; under a write-heavy workload it is routine. Abandoning the attempt on it cost
;; run 32206164668 every one of its 11 splits. But reissuing a split that may
;; still be in flight is how a harness manufactures a map corruption and then
;; reports it as a finding, so the retry is gated on proof that the first
;; attempt landed nothing. These pin that gate, in both directions.

(def ^:private retry-safe? #'range-nemesis/retry-safe?)

(def ^:private map-a [[nil "k" 1 1] ["k" nil 2 1]])
(def ^:private map-b [[nil "k" 1 1] ["k" "m" 2 2] ["m" nil 3 1]])

(deftest retries-a-transfer-failure-that-left-the-map-alone
  ;; The common case: the copy refused a page, nothing was committed, and the
  ;; map proves it. Retrying here is what keeps a run from being starved.
  (is (retry-safe? {:outcome "TransferFailed" :determinate false} map-a map-a)))

(deftest retries-a-transfer-failure-the-server-calls-determinate
  ;; The server's own word is proof enough and needs no map comparison —
  ;; `determinate true` is the contract's "this is the final answer".
  (is (retry-safe? {:outcome "TransferFailed" :determinate true} map-a map-b)))

(deftest refuses-to-retry-when-the-map-moved-under-an-indeterminate-answer
  ;; The failure that matters. The split may have landed after the call
  ;; returned; a second one would race the first, and the resulting map would be
  ;; this nemesis's doing rather than Kahuna's.
  (is (not (retry-safe? {:outcome "TransferFailed" :determinate false}
                        map-a map-b))))

(deftest refuses-to-retry-outcomes-outside-the-retryable-set
  ;; A clean map is necessary but never sufficient: these are different failures
  ;; and retrying them is not obviously safe, so they stop.
  (doseq [o ["QuiesceFailed" "CutoverFailed" "ConcurrentSplit" "InvalidSplitKey"
             "BelowMinRangeSize" "Indeterminate"]]
    (is (not (retry-safe? {:outcome o :determinate false} map-a map-a))
        (str o " must not be retried"))
    (is (not (retry-safe? {:outcome o :determinate true} map-a map-a))
        (str o " must not be retried even when determinate"))))

(deftest refuses-to-retry-an-unreachable-node
  ;; `:unreachable` is a lost answer, not a clean abort — and it is already
  ;; handled by the leader hunt, which moves to the next node.
  (is (not (retry-safe? {:outcome :unreachable :determinate false} map-a map-a))))

(deftest a-nil-outcome-is-not-retryable
  ;; An unparsable body reads as `:outcome nil`. Unknown is not proof.
  (is (not (retry-safe? {:outcome nil :determinate false} map-a map-a))))

;; `with-retry` end to end. These pay ~1 s + 2 s of real backoff for the paths
;; that exhaust the budget, which is the cost of the backoff being real rather
;; than injected — worth it here, because the thing worth pinning is that the
;; loop terminates and reports honestly, not that it sleeps.

(def ^:private with-retry #'range-nemesis/with-retry)

(def ^:private a-test {:nodes ["n1" "n2" "n3"]})

(defn- stub
  "A `call!` that answers from `answers` in order, recording each call."
  [calls answers]
  (fn [_node]
    (let [i (count @calls)]
      (swap! calls conj i)
      (nth answers (min i (dec (count answers)))))))

(def ^:private ok {:success true :determinate true :outcome "Succeeded"})
(def ^:private transfer-failed
  {:success false :determinate false :outcome "TransferFailed"})

(deftest a-first-attempt-that-succeeds-is-not-retried
  (let [calls (atom [])
        r     (with-retry a-test (stub calls [ok]) (constantly map-a))]
    (is (:success r))
    (is (= 1 (count @calls)))
    (is (nil? (:retries r)) "a clean run should carry no retry record")))

(deftest a-transfer-failure-then-a-success-is-recorded-as-a-retry
  (let [calls (atom [])
        r     (with-retry a-test (stub calls [transfer-failed ok])
                          (constantly map-a))]
    (is (:success r))
    (is (= 2 (count @calls)))
    (is (= [["n1" "TransferFailed"]] (mapv #(vector "n1" (second %)) (:retries r)))
        "the abandoned attempt must survive into the history")))

(deftest repeated-transfer-failures-stop-at-the-attempt-budget
  ;; The loop must terminate. Three attempts total, two of them recorded as
  ;; retries, and the caller still gets the last real answer rather than a
  ;; synthesised one.
  (let [calls (atom [])
        r     (with-retry a-test (stub calls [transfer-failed])
                          (constantly map-a))]
    (is (not (:success r)))
    (is (= "TransferFailed" (:outcome r)))
    (is (= 3 (count @calls)))
    (is (= 2 (count (:retries r))))))

(deftest a-moving-map-stops-the-retry-immediately
  ;; The safety case. The digest differs across the attempt, so the split may
  ;; have landed after the call returned and must not be reissued.
  (let [calls   (atom [])
        digests (atom [map-a map-b])
        r       (with-retry a-test (stub calls [transfer-failed])
                            (fn [] (let [d (first @digests)]
                                     (swap! digests rest)
                                     (or d map-b))))]
    (is (= 1 (count @calls)) "must not reissue against a map that moved")
    (is (nil? (:retries r)))))

(deftest nobody-attempting-is-not-a-retry
  ;; Every node refusing for lack of leadership is `:attempted false`, which is
  ;; its own finding — retrying would blur it into "attempted and failed".
  (let [calls (atom [])
        r     (with-retry a-test
                (stub calls [{:success false :determinate false :outcome "NotLeader"}])
                (constantly map-a))]
    (is (false? (:attempted r)))
    (is (= 3 (count @calls)) "one call per node, then stop")
    (is (nil? (:retries r)))))
