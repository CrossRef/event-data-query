(ns event-data-query.elastic-test
  (:require [clojure.test :refer :all]
            [event-data-query.elastic :as elastic]
            [clj-time.core :as clj-time]))



(deftest transform-for-index
  (testing "DOI fields should be associated with the Event when present."
    (let [input {"obj_id" "http://dx.doi.org/10.5555/12345678" ; old-style DOI supplied
                 "source_token" "45a1ef76-4f43-4cdc-9ba8-5a6ad01cc231"
                 "occurred_at" "2017-03-04T05:56:53Z"
                 "subj_id" "https://doi.org/10.6666/87654321"
                 "id" "00037012-c6b8-4862-93ef-5a5043c657bb"
                 "action" "add"
                 "subj" {"some" "subj_data"
                         "url" "http://www.example.com/1234567"
                         "alternative-id" "1234567"}
                 "source_id" "my_source_id"
                 "obj" {"some" "obj_data"}
                 "timestamp" "2017-04-04T05:57:19Z"
                 "evidence-record" "https://evidence.eventdata.crossref.org/evidence/20170404-twitter-66e922df-9754-45f7-886f-f7ddaa6ea8ba"
                 "relation_type_id" "discusses"
                 "updated_date" "2018-03-04T05:56:53Z"}
          processed (elastic/transform-for-index input)]

  
  (is (= (:obj-alternative-id processed) nil) "")
  (is (= (:updated-date processed) 1520143013000) "")
  (is (= (:subj-url-domain processed) "www.example.com") "")
  (is (= (:occurred processed) 1488607013000) "")
  (is (= (:subj-prefix processed) "10.6666") "")
  (is (= (:updated processed) nil) "")
  (is (= (:subj-alternative-id processed) "1234567") "")
  (is (= (:source processed) "my_source_id") "")
  (is (= (:subj-id-domain processed) "doi.org") "")
  (is (= (:obj-url-domain processed) nil) "")
  (is (= (:obj-id-domain processed) "doi.org") "")
  (is (= (:obj-id processed) "https://doi.org/10.5555/12345678") "")
  (is (= (:subj-id processed) "https://doi.org/10.6666/87654321") "")
  (is (= (:event processed) input) "")
  (is (= (:obj-prefix processed) "10.5555") "")
  (is (= (:id processed) "00037012-c6b8-4862-93ef-5a5043c657bb") "")
  (is (= (:subj-doi processed) "https://doi.org/10.6666/87654321") "")
  (is (= (:obj-doi processed) "https://doi.org/10.5555/12345678") "")
  (is (= (:obj-url processed) "") "")
  (is (= (:timestamp processed) 1491285439000) "")
  (is (= (:relation processed) "discusses") "")
  (is (= (:subj-url processed) "http://www.example.com/1234567") "")))

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
          processed (elastic/transform-for-index input)]

      (is (= (:subj-id-domain processed) "example.com") "")
      (is (= (:obj-id-domain processed) "example.com") "")
      (is (= (:obj-id processed) "http://example.com/obj_id") "")
      (is (= (:subj-id processed) "http://example.com/subj_id") "")
      (is (= (:obj-prefix processed) nil) "")
      (is (= (:subj-prefix processed) nil) "")
      (is (= (:subj-doi processed) nil) "")
      (is (= (:obj-doi processed) nil) ""))))