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
