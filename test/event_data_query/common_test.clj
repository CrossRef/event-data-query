(ns event-data-query.common-test
  (:require [clojure.test :refer :all]
            [event-data-query.common :as common]
            [clj-time.core :as clj-time]))

(deftest transform-for-index
  (testing "DOI fields should be associated with the Event when present."
    (let [input {"obj_id" "http://dx.doi.org/10.5555/12345678" ; old-style DOI supplied
                 "source_token" "45a1ef76-4f43-4cdc-9ba8-5a6ad01cc231"
                 "occurred_at" "2017-03-04T05:56:53Z"
                 "subj_id" "https://doi.org/10.6666/87654321",
                 "id" "00037012-c6b8-4862-93ef-5a5043c657bb",
                 "action" "add",
                 "subj" {"some" "subj_data"},
                 "source_id" "my_source_id",
                 "obj" {"some" "obj_data"},
                 "timestamp" "2017-04-04T05:57:19Z",
                 "evidence-record" "https://evidence.eventdata.crossref.org/evidence/20170404-twitter-66e922df-9754-45f7-886f-f7ddaa6ea8ba",
                 "relation_type_id" "discusses"
                 "updated_date" "2018-03-04T05:56:53Z"}
          processed (common/transform-for-index input)]

      (is (every? processed (keys input)) "All input fields should be present in output.")
      (is (= (processed "_subj_prefix") "10.6666") "Subj DOI prefix should be derived as special field")
      (is (= (processed "_obj_prefix") "10.5555") "Obj DOI prefix should be derived as special field")
      (is (= (processed "_subj_doi") "https://doi.org/10.6666/87654321") "Subj DOI prefix should be derived and normalized as special field")
      (is (= (processed "_obj_doi") "https://doi.org/10.5555/12345678") "Obj DOI prefix should be derived and normalized as special field")
      (is (= (processed "_occurred-date") (clj-time/date-time 2017 3 4 5 56 53)) "Occurred date should be parsed to date object as special field")
      (is (= (processed "_timestamp-date") (clj-time/date-time 2017 4 4 5 57 19)) "Timestamp date should be parsed to date object as special field")
      (is (= (processed "_updated_date") (clj-time/date-time 2018 3 4 5 56 53)) "Optional updated date should be parsed to date object as special field")))

  (testing "When subj and obj are not DOIs, should be passed through."
    (let [input {"obj_id" "http://example.com/obj_id"
                 "source_token" "45a1ef76-4f43-4cdc-9ba8-5a6ad01cc231"
                 "occurred_at" "2017-04-04T05:56:53Z"
                 "subj_id" "http://example.com/subj_id",
                 "id" "00037012-c6b8-4862-93ef-5a5043c657bb",
                 "action" "add",
                 "subj" {"some" "subj_data"},
                 "source_id" "my_source_id",
                 "obj" {"some" "obj_data"},
                 "timestamp" "2017-03-04T05:57:19Z",
                 "evidence-record" "https://evidence.eventdata.crossref.org/evidence/20170404-twitter-66e922df-9754-45f7-886f-f7ddaa6ea8ba",
                 "relation_type_id" "discusses"}
          processed (common/transform-for-index input)]

      (is (every? processed (keys input)) "All input fields should be present in output.")
      (is (nil? (processed "_subj_prefix")) "No obj DOI prefix when no obj DOI")
      (is (nil? (processed "_obj_prefix")) "No obj DOI prefix when no obj DOI")
      (is (nil? (processed "_subj_doi")) "No subj DOI when no subj DOI")
      (is (nil? (processed "_obj_doi")) "No obj DOI when no obj DOI")
      (is (= (processed "_occurred-date") (clj-time/date-time 2017 4 4 5 56 53)) "Occurred date should be parsed to date object as special field")
      (is (= (processed "_timestamp-date") (clj-time/date-time 2017 3 4 5 57 19)) "Timestamp date should be parsed to date object as special field")
      (is (nil? (processed "_updated_date")) "No parsed updated date when no input suppplied."))))


(deftest start-of
  (testing "start-of should parse and create first instant of day"
    (= (common/start-of "2017-02-05") (clj-time/date-time 2017 2 5))))

(deftest end-of
  (testing "end-of should parse and create first instant of next day"
    (= (common/start-of "2017-02-05") (clj-time/date-time 2017 2 6))))
