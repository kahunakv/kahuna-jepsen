(ns kahuna.hash-test
  "Drift guards for the key-space → partition rule.

  Every vector here was produced by Kommander's `HashUtils.ConsistentHash`
  itself (`dotnet run` against the library), not derived by hand, and the first
  block is the golden corpus from `Kommander.Tests/HashUtilsTests.cs`. The one
  that matters operationally is `jepsen/register` at a pool of 8: the nightly
  logs show every register write compacting partition 3, and this must agree,
  or the nemesis aims at a partition nobody writes."
  (:require [clojure.test :refer [deftest is testing]]
            [kahuna.hash :as hash]))

(def ^:private kommander-buckets [1 2 3 7 16 1024])

(deftest matches-kommander-golden-corpus
  (doseq [[s expected] {"a"                    [0 1 1 1 14 726]
                        "hello"                [0 1 1 4 7 374]
                        "partition-key-12345"  [0 0 2 6 10 659]
                        "ünïcødé-Ω≈ç√"         [0 0 0 0 0 934]
                        (apply str (repeat 500 "x")) [0 1 1 4 11 178]
                        ""                     [0 1 2 2 2 225]}]
    (testing s
      (is (= expected (mapv #(hash/consistent-hash s %) kommander-buckets))))))

(deftest matches-the-suite-key-spaces
  ;; Pools 1 2 3 7 8 16 1024, captured from the library. 8 is the nightly
  ;; `--partitions`, so bucket 2 is partition 3 for the register workload.
  (doseq [[s expected] {"jepsen/register"   [0 0 2 2 2 13 372]
                        "jepsen/append"     [0 0 2 5 5 5 92]
                        "jepsen/snapshot"   [0 0 2 2 2 9 531]
                        "jepsen/register/5" [0 0 0 6 6 6 6]
                        "ünïcödé/κόσμε"     [0 1 2 2 2 14 145]
                        "0123456789abcdef0123456789abcdef!" [0 0 0 0 0 9 755]}]
    (testing s
      (is (= expected (mapv #(hash/consistent-hash s %) [1 2 3 7 8 16 1024]))))))

(deftest rejects-an-empty-pool
  (is (thrown? IllegalArgumentException (hash/consistent-hash "a" 0))))

(deftest hash-partition-offsets-into-the-pool
  (let [routing {:hash-algorithm        hash/algorithm
                 :hash-pool-size        8
                 :hash-partition-offset 1}]
    (testing "the register key space is partition 3 at the nightly pool"
      (is (= 3 (hash/hash-partition routing "jepsen/register"))))
    (testing "a group separator cuts the key space before hashing"
      (is (= (hash/hash-partition routing "jepsen/register")
             (hash/hash-partition routing "jepsen/register|tenant-7"))))
    (testing "an unknown rule yields no answer rather than a wrong one"
      (is (nil? (hash/hash-partition (assoc routing :hash-algorithm "something-else-v2")
                                     "jepsen/register"))))
    (testing "no key space, no answer"
      (is (nil? (hash/hash-partition routing nil))))
    (testing "an empty pool yields no answer"
      (is (nil? (hash/hash-partition (assoc routing :hash-pool-size 0) "jepsen/register"))))))
