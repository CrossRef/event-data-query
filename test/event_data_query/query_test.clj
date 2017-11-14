(ns event-data-query.query-test
  (:require [clojure.test :refer :all]
            [event-data-query.query :as query]
            [clj-time.core :as clj-time]))

(deftest all-query-functions-registered
  (testing "All query parsing functions should be included in `filters`. Every function starting with 'q' should be registered. Just count functions."
    (let [query-fns (set (filter #(.startsWith % "q-") (map str (keys (ns-publics 'event-data-query.query)))))]
      (is (= (count query-fns) (count query/filters))))))

(deftest start-of
  (testing "start-of should parse and create first instant of day"
    (is (= (query/start-of "2017-02-05") (clj-time/date-time 2017 2 5))))

  (testing "start-of should throw IllegalArgumentException on error."
    (is (thrown? IllegalArgumentException (query/start-of "2017-XXX")))))

(deftest end-of
  (testing "end-of should parse and create first instant of next day"
    (is (= (query/end-of "2017-02-05") (clj-time/date-time 2017 2 6))))

  (testing "end-of should throw IllegalArgumentException on error."
    (is (thrown? IllegalArgumentException (query/end-of "2017-XXX")))))

(deftest q-from-occurred-date
  (testing "q-from-occurred-date creates query including :occurred when present"
    (is (= (query/q-from-occurred-date {:from-occurred-date "2017-01-01"})  
           {:range {:occurred {:gte 1483228800000}}})
        "'occurred' greater than or equal date using indexed field"))

  (testing "from-occurred-date throws exception on invalid date"
    (is (thrown-with-msg? Exception #"from-occurred-date incorrect"
                          (query/q-from-occurred-date {:from-occurred-date "Threeth Octember 201"})))))

(deftest q-until-occurred-date
  (testing "q-until-occurred-date creates query including :occurred when present"
    (is (= (query/q-until-occurred-date {:until-occurred-date "2017-01-01"})
           {:range {:occurred {:lt 1483315200000}}})
      "'occurred' at less than next day using indexed field"))

  (testing "until-occurred-date throws exception on invalid date"
    (is (thrown-with-msg? Exception #"until-occurred-date incorrect"
                          (query/q-until-occurred-date {:until-occurred-date "Threeth Octember 201"})))))

(deftest q-from-collected-date
  (testing "q-from-collected-date creates query including :timestamp when present"
    (is (= (query/q-from-collected-date {:from-collected-date "2017-01-01"})
           {:range {:timestamp {:gte 1483228800000}}})
      "'timestamp' greater than or equal date using special indexed field"))
  
  (testing "from-collected-date throws exception on invalid date"
    (is (thrown-with-msg? Exception #"from-collected-date incorrect"
                          (query/q-from-collected-date {:from-collected-date "Threeth Octember 201"})))))

(deftest q-until-collected-date
  (testing "q-until-collected-date creates query including :until-collected-date when present"
    (is (= (query/q-until-collected-date {:until-collected-date "2017-01-01"})
           {:range {:timestamp {:lt 1483315200000}}})
      "'timestamp' at less than next day using special indexed field"))
  
  (testing "until-collected-date throws exception on invalid date"
    (is (thrown-with-msg? Exception #"until-collected-date incorrect"
                          (query/q-until-collected-date {:until-collected-date "Threeth Octember 201"})))))

; Note that this is a bit special, see docs for query/q-from-updated-date
(deftest q-from-updated-date
  (testing "q-from-updated-date filters :update field from start of given day when date present"
    (is (= (query/q-from-updated-date {:updated-date nil})
           {:bool {:must_not {:term {:updated "deleted"}}}})
      "'timestamp' greater than or equal date using special indexed field"))

  (testing "q-from-updated-date excludes deleted items if not present"
    (is (= (query/q-from-updated-date {})
           {:bool {:must_not {:term {:updated "deleted"}}}})
      "'timestamp' greater than or equal date using special indexed field"))
  
  (testing "from-updated-date throws exception on invalid date"
    (is (thrown-with-msg? Exception #"from-updated-date incorrect"
                          (query/q-from-updated-date {:from-updated-date "Threeth Octember 201"})))))

(deftest q-subj-id
  (testing "q-subj-id should issue query for normalized DOI, if input looks like a DOI, or verbatim URL otherwise"
    (is (= (query/q-subj-id {:subj-id "10.5555/12345678"}) {:term {:subj-doi "https://doi.org/10.5555/12345678"}}))
    (is (= (query/q-subj-id {:subj-id "http://dx.doi.org/10.5555/12345678"}) {:term {:subj-doi "https://doi.org/10.5555/12345678"}}))
    (is (= (query/q-subj-id {:subj-id "https://doi.org/10.5555/12345678"}) {:term {:subj-doi "https://doi.org/10.5555/12345678"}}))

    (is (= (query/q-subj-id {:subj-id "http://example.com"}) {:term {:subj-id "http://example.com"}}))))

(deftest q-obj-id
  (testing "q-obj-id should issue query for normalized DOI, if input looks like a DOI, or verbatim URL otherwise"
    (is (= (query/q-obj-id {:obj-id "10.5555/12345678"}) {:term {:obj-doi "https://doi.org/10.5555/12345678"}}))
    (is (= (query/q-obj-id {:obj-id "http://dx.doi.org/10.5555/12345678"}) {:term {:obj-doi "https://doi.org/10.5555/12345678"}}))
    (is (= (query/q-obj-id {:obj-id "https://doi.org/10.5555/12345678"}) {:term {:obj-doi "https://doi.org/10.5555/12345678"}}))

    (is (= (query/q-obj-id {:obj-id "http://example.com"}) {:term {:obj-id "http://example.com"}}))))

(deftest q-subj-id-prefix
  (testing "q-subj-id-prefix should query for subj-prefix field"
    (is (= (query/q-subj-id-prefix {:subj-id.prefix "10.5555"})
            {:term {:subj-prefix "10.5555"}}))))

(deftest q-obj-id-prefix
  (testing "q-obj-id-prefix should query for obj-prefix field"
    (is (= (query/q-obj-id-prefix {:obj-id.prefix "10.5555"})
            {:term {:obj-prefix "10.5555"}}))))

(deftest q-subj-id-domain
  (testing "q-subj-id-domain should query for subj-id-domain field, i.e. domain of subj_id"
    (is (= (query/q-subj-id-domain {:subj-id.domain "example.com"})
            {:term {:subj-id-domain "example.com"}}))))

(deftest q-obj-id-domain
  (testing "q-obj-id-domain should query for obj-id-domain field, i.e. domain of obj_id"
    (is (= (query/q-obj-id-domain {:obj-id.domain "example.com"})
            {:term {:obj-id-domain "example.com"}}))))

(deftest q-subj-url
  (testing "q-subj-url should query for subj.url field"
    (is (= (query/q-subj-url {:subj.url "http://example.com/123"})
            {:term {:subj-url "http://example.com/123"}}))))

(deftest q-obj-url
  (testing "q-obj-url should query for obj.url field"
    (is (= (query/q-obj-url {:obj.url "http://example.com/123"})
            {:term {:obj-url "http://example.com/123"}}))))

(deftest q-subj-url-domain
  (testing "q-subj-url-domain should query for subj-url-domain field, i.e. domain of subj.url"
    (is (= (query/q-subj-url-domain {:subj.url.domain "example.com"})
            {:term {:subj-url-domain "example.com"}}))))


(deftest q-obj-url-domain
  (testing "q-obj-url-domain should query for obj-url-domain field, i.e. domain of obj.url"
    (is (= (query/q-obj-url-domain {:obj.url.domain "example.com"})
            {:term {:obj-url-domain "example.com"}}))))

(deftest q-subj-alternative-id
  (testing "q-subj-alternative-id should query for subj-url-domain field, i.e. domain of subj.url"
    (is (= (query/q-subj-alternative-id {:subj.alternative-id "12345"})
            {:term {:subj-alternative-id "12345"}}))))

(deftest q-obj-alternative-id
  (testing "q-obj-alternative-id should query for obj-url-domain field, i.e. domain of obj.url"
    (is (= (query/q-obj-alternative-id {:obj.alternative-id "12345"})
            {:term {:obj-alternative-id "12345"}}))))

(deftest q-source
  (testing "q-source should query for source field"
    (is (= (query/q-source {:source "doi-chat"})
            {:term {:source "doi-chat"}}))))

(deftest q-experimental
  (testing "q-experimental should query for true if present, else false"
    (is (= (query/q-experimental {:experimental "true"})
            {:term {:experimental true}}))

    (is (= (query/q-experimental {:experimental "false"})
            {:term {:experimental false}}))

    (is (= (query/q-experimental {})
            {:term {:experimental false}}))))

