(ns kahuna.workload.append-test
  "The wait-die rule for a refused Shared point lock.

  The rule is small, and a mistake in it would be quiet: waiting in both
  directions still produces a valid history, it just deadlocks two clients
  until the lock-wait deadline on every contended key. These pin the
  direction — an older requester waits, a younger one gives up — and the two
  edges where no order exists."
  (:require [clojure.test :refer :all]
            [kahuna.client :as kc]
            [kahuna.workload.append :as append]))

(def older   {:l 1000 :c 0 :n 1})
(def younger {:l 2000 :c 0 :n 1})

(deftest older-requester-waits
  (is (true? (append/wait-for-holder? older younger))))

(deftest younger-requester-gives-up
  (is (false? (append/wait-for-holder? younger older))))

(deftest same-millisecond-orders-on-counter-then-node
  (testing "counter breaks the tie before node id"
    (is (true?  (append/wait-for-holder? {:l 5 :c 0 :n 9} {:l 5 :c 1 :n 1})))
    (is (false? (append/wait-for-holder? {:l 5 :c 1 :n 1} {:l 5 :c 0 :n 9}))))
  (testing "node id breaks the last tie"
    (is (true?  (append/wait-for-holder? {:l 5 :c 1 :n 1} {:l 5 :c 1 :n 2})))))

(deftest no-holder-means-give-up
  (testing "a zero holder carries no transaction to order against"
    (is (false? (append/wait-for-holder? older kc/hlc-zero))))
  (testing "an absent holder field likewise"
    (is (false? (append/wait-for-holder? older nil)))))

(deftest equal-ids-do-not-wait
  (is (false? (append/wait-for-holder? older older))))
