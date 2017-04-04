(ns event-data-query.server-test
  (:require [clojure.test :refer :all]
            [event-data-query.server :as server]
            [event-data-query.common :as common]
            [clj-time.core :as clj-time]))

(deftest try-parse-ymd-date
  (testing "try-parse-ymd-date should parse dates, or return :error on error"
    (is (= (server/try-parse-ymd-date "2017-04-04") (clj-time/date-time 2017 4 4)) "Should parse valid")
    (is (= (server/try-parse-ymd-date "INVALID") :error) "Should return :error on invalid")))


(deftest and-queries
  (testing "and-queries should combine all non-null inputs in an $and clause"
    (is (= {"$and" [{"query" "one"} {"query" "two"} {"query" "three"}]}
           (server/and-queries nil {"query" "one"} nil {"query" "two"} nil nil {"query" "three"} nil nil))
        "All non-nil inputs should be combined in an $and query.")

    (is (= {} (server/and-queries))
        "No input results in nil output")

    (is (= {} (server/and-queries nil))
        "Nil input results in nil output")))

(deftest q-from-occurred-date
  (testing "q-from-occurred-date creates query including :from-occurred-date when present"
    (is (= (server/q-from-occurred-date {:from-occurred-date "2017-01-01"})
           {:_occurred-date {"$gte" (clj-time/date-time 2017 1 1)}})
        "'occurred' greater than or equal date using special indexed field")))

(deftest q-until-occurred-date
  (testing "q-until-occurred-date creates query including :until-occurred-date when present"
    (is (= (server/q-until-occurred-date {:until-occurred-date "2017-01-01"})
           {:_occurred-date {"$lt" (clj-time/date-time 2017 1 2)}})
      "'occurred' at less than next day using special indexed field")))

(deftest q-from-collected-date
  (testing "q-from-collected-date creates query including :from-collected-date when present"
    (is (= (server/q-from-collected-date {:from-collected-date "2017-01-01"})
           {:_timestamp-date {"$gte" (clj-time/date-time 2017 1 1)}})
      "'timestamp' greater than or equal date using special indexed field")))

(deftest q-until-collected-date
  (testing "q-until-collected-date creates query including :until-collected-date when present"
    (is (= (server/q-until-collected-date {:until-collected-date "2017-01-01"})
           {:_timestamp-date {"$lt" (clj-time/date-time 2017 1 2)}})
      "'timestamp' at less than next day using special indexed field")))

(deftest q-work
  (testing "q-work creates query including :work when present, and normalizes"
    (is (= (server/q-work {:work "10.5555/12345678"})
           {"$or" [{:_subj_doi "https://doi.org/10.5555/12345678"}
                   {:_obj_doi "https://doi.org/10.5555/12345678"}]})
          "looking in special subj or obj DOI field.")))
    
(deftest q-prefix
  (testing "q-prefix creates query including :prefix when present"
    (is (= (server/q-prefix {:prefix "10.5555"})
           {"$or" [{:_subj_prefix "10.5555"}
                   {:_obj_prefix "10.5555"}]})
          "looking in special subj or obj prefix field.")))

(deftest q-source-whitelist-not-overriden
  (reset! server/sourcelist #{"source-one" "source-two"})
  (reset! server/whitelist-override false)

  (testing "q-source creates query including :source when present and recognised in whitelist, when whitelist isn't overridden"
    (is (= (server/q-source {:source "source-one"})
           {:source_id "source-one"})
      "single source should be specified when it meets the whitelist"))

  (testing "q-source creates query with all sources when source param not present, when whitelist isn't overridden"
    (is (= (server/q-source {})
        {:source_id {"$in" ["source-one" "source-two"]}})
      "all whitelist sources shoudl be specified when none supplied"))

  (testing "q-source creates query including nil source when present but not recognised in whitelist, when whitelist isn't overridden"
    (is (= (server/q-source {:source "UNRECOGNISED-SOURCE"})
           {:source_id nil})
      "source should be nil")))

(deftest q-source-whitelist-overriden
  (reset! server/sourcelist #{"source-one" "source-two"})
  (reset! server/whitelist-override true)

  (testing "q-source creates query with no specified sources (i.e. no restriction) when source param not present, when whitelist is overridden"
    (is (= (server/q-source {})
           nil)
          "no source query should be given"))

  (testing "q-source creates query source when present, regardless of whitelist, when whitelist is overridden"
    (is (= (server/q-source {:source "UNRECOGNISED-SOURCE"})
        {:source_id "UNRECOGNISED-SOURCE"})
      "source should be per parameter")))

(deftest build-filter-query
  (reset! server/sourcelist #{"source-one" "source-two"})
  (reset! server/whitelist-override false)

  (testing "build-filter-query combines output of all clauses"
    (let [input {:from-occurred-date "2011-01-01"
                 :until-occurred-date "2012-01-01"
                 :from-collected-date "2013-01-01"
                 :until-collected-date "2014-01-01"
                 :work "10.5555/12345678"
                 :prefix "10.5555"
                 :source "source-one"}
          result (server/build-filter-query input)]
      (is (= result
        {"$and" [{:_occurred-date {"$gte" (clj-time/date-time 2011 1 1)}}
                 {:_occurred-date {"$lt" (clj-time/date-time 2012 1 2)}}
                 {:_timestamp-date {"$gte" (clj-time/date-time 2013 1 1)}}
                 {:_timestamp-date {"$lt" (clj-time/date-time 2014 1 2)}}
                 {"$or" [{:_subj_doi "https://doi.org/10.5555/12345678"}
                         {:_obj_doi "https://doi.org/10.5555/12345678"}]}
                 {"$or" [{:_subj_prefix "10.5555"}
                         {:_obj_prefix "10.5555"}]}
                 {:source_id "source-one"}]}))))

  (testing "build-filter-query handles empty query"
    (is (= (server/build-filter-query {})
            ; default values from empty source
            {"$and" [{:source_id {"$in" ["source-one" "source-two"]}}]}))))

(deftest mq-cursor
  (testing "cursor included when supplied"
    (is (= (server/mq-cursor {"cursor" "CURSOR_VALUE"})
           {:_id {"$gt" "CURSOR_VALUE"}})))

  (testing "cursor not included when not supplied"
    (is (= (server/mq-cursor {})
           nil))))

(deftest mq-experimental
  (testing "when experimental not true, exclude experimental"
    (is (= (server/mq-experimental {})
           {:experimental nil})))

  (testing "when experimental true, don't exclude experimental"
    (is (= (server/mq-experimental {"experimental" "true"})
           {}))))

(deftest mq-updated-since-date
  (testing "when update-date supplied, filter includes all and only events since that date"
    (is (= (server/mq-updated-since-date {"from-updated-date" "2017-01-01"})
           {"$and" [{:_updated-date {"$gte" (clj-time/date-time 2017 1 1)}}
                    {:updated {"$exists" true}}]})))

  (testing "when update-date not supplied, filter excludes events that have been deleted"
    (is (= (server/mq-updated-since-date {})
           {:updated {"$ne" "deleted"}}))))


(deftest split-filter
  (testing "split-filter can split valid strings into keyword-keyed maps"
    (is (= (server/split-filter "one:two,three:four")
           {:one "two" :three "four"}))))

(deftest try-parse-int
  (testing "try-parse-int can parse valid integer"
    (= 12345 (server/try-parse-int "1234"))
    (= :error (server/try-parse-int "ONE TWO THREE FOUR"))))


(deftest export-event
  (testing "export-event adds extra fields"
    (is (= (server/export-event {"input" "event"})
           {"input" "event"
            ; terms taken from docker-compose.yml config
            "terms" "https://doi.org/10.13003/CED-terms-of-use"}))))
