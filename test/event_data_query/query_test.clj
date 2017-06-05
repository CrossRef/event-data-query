(ns event-data-query.query-test
  (:require [clojure.test :refer :all]
            [event-data-query.query :as query]
            [clj-time.core :as clj-time]))

(deftest and-queries
  (testing "and-queries should combine all non-null inputs in an $and clause"
    (is (= {"$and" [{"query" "one"} {"query" "two"} {"query" "three"}]}
           (query/and-queries nil {"query" "one"} nil {"query" "two"} nil nil {"query" "three"} nil nil))
        "All non-nil inputs should be combined in an $and query.")

    (is (= {} (query/and-queries))
        "No input results in nil output")

    (is (= {} (query/and-queries nil))
        "Nil input results in nil output")))

(deftest q-from-occurred-date
  (testing "q-from-occurred-date creates query including :from-occurred-date when present"
    (is (= (query/q-from-occurred-date {:from-occurred-date "2017-01-01"})
           {:_occurred-date {"$gte" (clj-time/date-time 2017 1 1)}})
        "'occurred' greater than or equal date using special indexed field")))

(deftest q-until-occurred-date
  (testing "q-until-occurred-date creates query including :until-occurred-date when present"
    (is (= (query/q-until-occurred-date {:until-occurred-date "2017-01-01"})
           {:_occurred-date {"$lt" (clj-time/date-time 2017 1 2)}})
      "'occurred' at less than next day using special indexed field")))

(deftest q-from-collected-date
  (testing "q-from-collected-date creates query including :from-collected-date when present"
    (is (= (query/q-from-collected-date {:from-collected-date "2017-01-01"})
           {:_timestamp-date {"$gte" (clj-time/date-time 2017 1 1)}})
      "'timestamp' greater than or equal date using special indexed field")))

(deftest q-until-collected-date
  (testing "q-until-collected-date creates query including :until-collected-date when present"
    (is (= (query/q-until-collected-date {:until-collected-date "2017-01-01"})
           {:_timestamp-date {"$lt" (clj-time/date-time 2017 1 2)}})
      "'timestamp' at less than next day using special indexed field")))

(deftest q-work
  (testing "when 'work' is present and it's a DOI, q-work creates query that matches subject or object DOI."
    (is (= (query/q-work {:work "10.5555/12345678"})
           {"$or" [{:_subj_doi "https://doi.org/10.5555/12345678"}
                   {:_obj_doi "https://doi.org/10.5555/12345678"}]})
          "Looking in special subj or obj DOI field for normalized DOI.")

    (is (= (query/q-work {:work "http://dx.doi.org/10.5555/12345678"})
           {"$or" [{:_subj_doi "https://doi.org/10.5555/12345678"}
                   {:_obj_doi "https://doi.org/10.5555/12345678"}]})
          "Looking in special subj or obj DOI field for normalized DOI."))

  (testing "when 'work' is present and it's a DOI, q-work creates query that matches subject or object DOI."
    (is (= (query/q-work {:work "10.5555/12345678"})
           {"$or" [{:_subj_doi "https://doi.org/10.5555/12345678"}
                   {:_obj_doi "https://doi.org/10.5555/12345678"}]})
          "Looking in special subj or obj DOI field for normalized DOI.")

    (is (= (query/q-work {:work "https://hypothes.is/a/NQw-fhJwEeeFer_r68hwoQ"})
           {"$or" [{:_subj_id "https://hypothes.is/a/NQw-fhJwEeeFer_r68hwoQ"}
                   {:_obj_id "https://hypothes.is/a/NQw-fhJwEeeFer_r68hwoQ"}]})
          "Looking in subj or obj id field when not a DOI.")))

(deftest q-relation
  (testing "q-relation adds relation filter when present"
    (is (= (query/q-relation {:relation "discusses"})
           {:relation_type_id "discusses"}))))

(deftest q-prefix
  (testing "q-prefix creates query including :prefix when present"
    (is (= (query/q-prefix {:prefix "10.5555"})
           {"$or" [{:_subj_prefix "10.5555"}
                   {:_obj_prefix "10.5555"}]})
          "looking in special subj or obj prefix field.")))

(deftest q-source-whitelist
  (testing "q-source creates query with no specified sources (i.e. no restriction) when source param not present"
    (is (= (query/q-source {})
           nil)
          "no source query should be given"))

  (testing "q-source creates query source when present, regardless of whitelist"
    (is (= (query/q-source {:source "UNRECOGNISED-SOURCE"})
        {:source_id "UNRECOGNISED-SOURCE"})
      "source should be per parameter")))

(deftest q-alternative-id
  (testing "q-alternative-id creates query including alternative-id field on subj or obj, when present"
    (is (= (query/q-alternative-id {:alternative-id "123456"})
           {"$or" [{:subj.alternative-id "123456"}
                   {:obj.alternative-id "123456"}]})
          "looking in subj or obj alternative-id field.")))

(deftest q-subj.url
  (testing "q-subj.url creates query for subj.url field, when present"
    (is (= (query/q-subj-url {:subj.url "http://example.com/123"})
           {:subj.url "http://example.com/123"})
          "looking in subj.url field.")))

(deftest q-obj.url
  (testing "q-sub.url creates query for subj.url field, when present"
    (is (= (query/q-obj-url {:obj.url "http://example.com/123"})
           {:obj.url "http://example.com/123"})
          "looking in subj.url field.")))

(deftest build-filter-query
  (testing "build-filter-query combines output of all clauses"
    (let [input {:from-occurred-date "2011-01-01"
                 :until-occurred-date "2012-01-01"
                 :from-collected-date "2013-01-01"
                 :until-collected-date "2014-01-01"
                 :work "10.5555/12345678"
                 :prefix "10.5555"
                 :relation "discusses"
                 :source "source-one"
                 :alternative-id "123456"}
          result (query/build-filter-query input)]
      (is (= result
        {"$and" [{:_occurred-date {"$gte" (clj-time/date-time 2011 1 1)}}
                 {:_occurred-date {"$lt" (clj-time/date-time 2012 1 2)}}
                 {:_timestamp-date {"$gte" (clj-time/date-time 2013 1 1)}}
                 {:_timestamp-date {"$lt" (clj-time/date-time 2014 1 2)}}
                 {"$or" [{:_subj_doi "https://doi.org/10.5555/12345678"}
                         {:_obj_doi "https://doi.org/10.5555/12345678"}]}
                 {"$or" [{:_subj_prefix "10.5555"}
                         {:_obj_prefix "10.5555"}]}
                 {:source_id "source-one"}
                 {"$or" [{:subj.alternative-id "123456"}
                         {:obj.alternative-id "123456"}]}
                 {:relation_type_id "discusses"}]}))))

  (testing "build-filter-query handles empty query"
    (is (= (query/build-filter-query {})
            ; default values from empty source
            {}))))

(deftest mq-cursor
  (testing "cursor included when supplied"
    (is (= (query/mq-cursor {"cursor" "CURSOR_VALUE"})
           {:_id {"$gt" "CURSOR_VALUE"}})))

  (testing "cursor not included when not supplied"
    (is (= (query/mq-cursor {})
           nil))))

(deftest mq-experimental
  (testing "when experimental not true, exclude experimental"
    (is (= (query/mq-experimental {})
           {:experimental nil})))

  (testing "when experimental true, don't exclude experimental"
    (is (= (query/mq-experimental {"experimental" "true"})
           {}))))

(deftest mq-updated-since-date
  (testing "when update-date supplied, filter includes all and only events since that date"
    (is (= (query/mq-updated-since-date {"from-updated-date" "2017-01-01"})
           {"$and" [{:_updated_date {"$gte" (clj-time/date-time 2017 1 1)}}
                    {:updated {"$exists" true}}]})))

  (testing "when update-date not supplied, filter excludes events that have been deleted"
    (is (= (query/mq-updated-since-date {})
           {:updated {"$ne" "deleted"}}))))


