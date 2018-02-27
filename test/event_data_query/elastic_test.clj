(ns event-data-query.elastic-test
  (:require [clojure.test :refer :all]
            [event-data-query.elastic :as elastic]
            [clj-time.core :as clj-time]
            [config.core :refer [env]]))

(deftest search-url
  (testing "Memoized function returns correct search URL according to config.")
    (is (= (-> env :query-deployment) "test_")
      "Precondition: Expected the QUERY_DEPLOYMENT config to be '_test'.")
    (is (= (elastic/search-url :standard) "test_standard/_search") "Correct search URL returned based on config")
    (is (= (elastic/search-url :standard) "test_standard/_search") "Twice.")
    (is (= (elastic/search-url :distinct) "test_distinct/_search") "Correct URL for Distinct index.")
    (is (= (elastic/search-url :edited) "test_edited/_search") "Correct URL for Edited index.")
    (is (= (elastic/search-url :deleted) "test_deleted/_search") "Correct URL for Deleted index.")
    (is (= (elastic/search-url :experimental) "test_experimental/_search") "Correct URL for Experimental index."))

(deftest event->document
  (testing "DOI fields should be associated with the Event when present."
    (let [event {"obj_id" "http://dx.doi.org/10.5555/12345678" ; old-style DOI supplied
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
          document (elastic/event->document event)]
      (is (= (:id document) nil) "ID should not be set here, as its behaviour depends on the index.")
      (is (= (:obj-alternative-id document) nil) "")
      (is (= (:updated-date document) 1520143013000) "")
      (is (= (:subj-url-domain document) "www.example.com") "")
      (is (= (:occurred document) 1488607013000) "")
      (is (= (:subj-prefix document) "10.6666") "")
      (is (= (:updated document) nil) "")
      (is (= (:subj-alternative-id document) "1234567") "")
      (is (= (:source document) "my_source_id") "")
      (is (= (:subj-id-domain document) "doi.org") "")
      (is (= (:obj-url-domain document) nil) "")
      (is (= (:obj-id-domain document) "dx.doi.org") "")
      (is (= (:obj-id document) "https://doi.org/10.5555/12345678") "")
      (is (= (:subj-id document) "https://doi.org/10.6666/87654321") "")
      (is (= (:event document) event) "")
      (is (= (:obj-prefix document) "10.5555") "")
      (is (= (:subj-doi document) "https://doi.org/10.6666/87654321") "")
      (is (= (:obj-doi document) "https://doi.org/10.5555/12345678") "")
      (is (= (:obj-url document) "") "")
      (is (= (:timestamp document) 1491285439000) "")
      (is (= (:relation-type document) "discusses") "")
      (is (= (:subj-url document) "http://www.example.com/1234567") "")))

  (testing "When subj and obj are not DOIs, should be passed through."
    (let [event {"obj_id" "http://example.com/obj_id"
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
          document (elastic/event->document event)]

      (is (= (:subj-id-domain document) "example.com") "")
      (is (= (:obj-id-domain document) "example.com") "")
      (is (= (:obj-id document) "http://example.com/obj_id") "")
      (is (= (:subj-id document) "http://example.com/subj_id") "")
      (is (= (:obj-prefix document) nil) "")
      (is (= (:subj-prefix document) nil) "")
      (is (= (:subj-doi document) nil) "")
      (is (= (:obj-doi document) nil) ""))))

(def aggregation-response
  {:source
   {:doc_count_error_upper_bound 0,
    :sum_other_doc_count 199230,
    :buckets [{:key "wikipedia", :doc_count 266027}]}
   :relation-type
   {:doc_count_error_upper_bound 0,
    :sum_other_doc_count 329463,
    :buckets [{:key "references", :doc_count 135794}
              {:key "disparages", :doc_count 66}]}})

(def expected-response
  {:source {:value-count 1
            :values {"wikipedia" 266027}}
   :relation-type {:value-count 2
                   :values {"references" 135794
                            "disparages" 66}}})

(deftest aggregation-results
  (testing "Aggregation result correctly parsed"
    (is (= (elastic/parse-aggregation-response aggregation-response)
           expected-response))))

(deftest index-routing
  (is (= (-> env :query-deployment) "test_")
      "Precondition: Expected the QUERY_DEPLOYMENT config to be 'test_'.")

  (testing "A normal Event should appear in the 'standard' and 'distinct' indexes.
            Not in the 'updated' or 'experimental'.
            Should be removed from 'deleted'."

      ; Don't need an entire Document.
    (is (= (elastic/document->batch-actions {:event {"id" "1234"}})
           [{:index {:_index "test_standard", :_type "event", :_id "1234"}}
            {:event {"id" "1234"}, :id "1234"}
            {:index {:_index "test_distinct", :_type "event", :_id "4c761f170e016836ff84498202b99827"}}
            {:event {"id" "1234"}, :id "4c761f170e016836ff84498202b99827"}
            {:delete {:_index "test_deleted", :_type "event", :_id "1234"}}])))

  (testing "An updated Event should appear in the 'standard', 'distinct' and 'updated' indexes.
            Should be removed from the 'deleted' index.
            Should not be in 'experimental'."

    (is (= (elastic/document->batch-actions {:event {"id" "1234"} :updated "edited"})
           [{:index {:_index "test_standard" :_type "event" :_id "1234"}}
            {:event {"id" "1234"} :updated "edited" :id "1234"}
            {:index {:_index "test_distinct" :_type "event" :_id "4c761f170e016836ff84498202b99827"}}
            {:event {"id" "1234"} :updated "edited" :id "4c761f170e016836ff84498202b99827"}
            {:index {:_index "test_edited" :_type "event" :_id "1234"}}
            {:event {"id" "1234"} :updated "edited" :id "1234"}
            {:delete {:_index "test_deleted" :_type "event" :_id "1234"}}])))
  (testing "An experimental Event should appear in the 'experimental' index only."
    (is (= (elastic/document->batch-actions {:event {"id" "1234" "experimental" true}})
           [{:delete {:_index "test_deleted", :_type "event", :_id "1234"}}
            {:index {:_index "test_experimental" :_type "event" :_id "1234"}}
            {:event {"id" "1234" "experimental" true} :id "1234"}])))

  (testing "A deleted Event should be removed from the 'standard', 'distinct', 'updated' and 'experimental' indexes.
            It should be inserted into the 'deleted' index."

    (is (= (elastic/document->batch-actions {:event {"id" "1234"} :updated "deleted"})
           [{:delete {:_index "test_standard" :_type "event" :_id "1234"}}
            {:delete {:_index "test_distinct" :_type "event" :_id "4c761f170e016836ff84498202b99827"}}
            {:delete {:_index "test_edited" :_type "event" :_id "1234"}}
            {:index {:_index "test_deleted" :_type "event" :_id "1234"}}
            {:event {"id" "1234"} :updated "deleted" :id "1234"}]))))

