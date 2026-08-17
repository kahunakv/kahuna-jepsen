(ns kahuna.client-test
  "Controls for response-body parsing.

  These exist because of a specific failure. clj-http decodes `:as :json` only
  for *unexceptional* statuses — `:coerce` defaults to `:unexceptional` — so a
  409 arrives as a raw String. Kahuna reports every refusal as 409 **with a
  body**, which means its entire outcome vocabulary sits precisely where
  clj-http stops parsing.

  The first CI run of the key-range profile showed what that costs: six nodes
  refused a registration, and the harness recorded
  `{:outcome nil, :reason nil, :http-status 409}` six times — the status was the
  only thing that survived, and it cannot distinguish 'not the leader, ask
  someone else' from 'the seed may still be in flight'. Worse, the range
  nemesis decides whether a failed split is safe to retry by reading
  `determinate` out of that same body; unparsed, every refusal would have read
  as indeterminate and the nemesis would have stopped at the first node it
  asked, which is the one guaranteed to answer `NotLeader`.

  So the tests below are about a parser, but the property is: **a refusal must
  arrive with its reason attached.**"
  (:require [clojure.test :refer :all]
            [cheshire.core :as json]
            [kahuna.client]))

(def parse
  "The private body parser, reached through its var. Private because nothing
  outside the HTTP layer should need it; tested because everything depends on
  it."
  #'kahuna.client/json-body)

(deftest an-already-decoded-body-passes-through
  ;; The 2xx path, which clj-http decodes for us. Re-parsing it would be a
  ;; no-op at best and a ClassCastException at worst.
  (is (= {:success true :status "Seeded"}
         (parse {:success true :status "Seeded"}))))

(deftest a-refusal-body-arrives-with-its-reason
  ;; The exact shape a 409 hands back: a String, because clj-http would not
  ;; touch it. Everything the caller needs is in there.
  (let [body (json/generate-string {:success false
                                    :status "Indeterminate"
                                    :seeded false
                                    :routingMode "KeyRange"
                                    :descriptorCount 0
                                    :reason "The seed was directed at the meta-partition leader…"})
        r    (parse body)]
    (is (= "Indeterminate" (:status r)))
    (is (false? (:success r)))
    (is (= "KeyRange" (:routingMode r)))
    (is (= 0 (:descriptorCount r)))))

(deftest a-split-refusal-keeps-the-flag-the-retry-rule-reads
  ;; `determinate` is what says whether trying another node is safe. A
  ;; NotLeader refusal is determinate — nothing was attempted — while a
  ;; CutoverFailed is not, and retrying it elsewhere would put two cutovers on
  ;; one range. Losing this field does not merely blind the harness, it makes
  ;; it act wrongly.
  (let [not-leader (parse (json/generate-string
                            {:success false :status "NotLeader" :determinate true
                             :leaderHint "n3:8082"}))
        cutover    (parse (json/generate-string
                            {:success false :status "CutoverFailed" :determinate false}))]
    (is (= "NotLeader" (:status not-leader)))
    (is (true? (:determinate not-leader)))
    (is (= "n3:8082" (:leaderHint not-leader)))
    (is (= "CutoverFailed" (:status cutover)))
    (is (false? (:determinate cutover)))))

(deftest a-body-that-is-not-json-yields-nil-rather-than-throwing
  ;; Why this parses by hand instead of asking clj-http for `:coerce :always`:
  ;; that decodes unconditionally and throws on a non-JSON body, and an
  ;; unhandled ASP.NET 500 answers text/plain. This suite has already recorded
  ;; a run's worth of those.
  (is (nil? (parse "System.InvalidOperationException: no leader for partition 0")))
  (is (nil? (parse "")))
  (is (nil? (parse nil))))

(deftest a-json-scalar-is-not-a-body
  ;; `null` and `42` are valid JSON and parse fine — to something no caller can
  ;; use. Returning them would put a Long where a map is expected and move the
  ;; failure somewhere far away from here.
  (is (nil? (parse "null")))
  (is (nil? (parse "42")))
  (is (nil? (parse "[1,2,3]"))))
