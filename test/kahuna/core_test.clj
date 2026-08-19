(ns kahuna.core-test
  "Controls for the CLI's fault parsing.

  `--faults` decides what the nemesis does, and the test's *name* is built from
  the parsed set — store/ is keyed by that string. So a parse that quietly
  produces the wrong set does not just run the wrong test, it files the result
  under the wrong name, which is how this suite has been misled before."
  (:require [clojure.test :refer [deftest is testing]]
            [kahuna.core :as core]))

(deftest parse-faults-handles-the-two-keywords
  (testing "'all' is every fault, not a fault literally named :all"
    (is (= core/all-faults (core/parse-faults "all"))))

  (testing "'none' is the empty set, which is what a control run needs"
    (is (= #{} (core/parse-faults "none"))))

  (testing "'none' is not parsed as a fault named :none"
    ;; The failure this guards is silent: #{:none} passes every `some #{:x}`
    ;; check as false, so the run would behave correctly and be named wrongly.
    (is (not (contains? (core/parse-faults "none") :none)))))

(deftest parse-faults-reads-an-explicit-list
  (is (= #{:partition :kill} (core/parse-faults "partition,kill")))
  (is (= #{:range} (core/parse-faults "range"))))

(deftest every-parsed-fault-is-a-real-fault
  (testing "the words the matrix uses all exist in all-faults"
    ;; Catches a fault renamed in one place and not the other: the nemesis
    ;; package would no-op and the run would still be named for it.
    (doseq [s ["partition" "kill" "pause" "membership" "placement" "range"]]
      (is (contains? core/all-faults (first (core/parse-faults s)))
          (str s " is not in all-faults")))))
