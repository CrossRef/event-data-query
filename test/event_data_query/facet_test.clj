(ns event-data-query.facet-test
  (:require [clojure.test :refer :all]
            [event-data-query.facet :as facet]
            [event-data-query.parameters :as parameters]
            [slingshot.test]
            [slingshot.test :as stest]))

; A regression test to make sure that parameters/parse suits the facet syntax.
(deftest parse-input
  (testing "parameters/parse can parse facets input"
    (is (= (parameters/parse "source:*,relation:66,obj.url.domain:99" identity)
           {"source" "*"
            "relation" "66"
            "obj.url.domain" "99"}))))

(deftest error-handling
  (testing "Maximum size can't be exceeded"
    (is (thrown+? [:type :validation-failure, :subtype :facet-unavailable]
          (facet/validate {"denomenation" "1"})))

    (is (thrown+? [:type :validation-failure :subtype :facet-size]
          (facet/validate {"source" "999999"})))

    (is (thrown+? [:type :validation-failure :subtype :facet-size-malformed]
          (facet/validate {"source" "a million"})))))

(deftest generate-aggregation-query
  (testing "An aggregation query can be created from a query string input."
    (is (= (facet/build-facet-query (parameters/parse "source:5,relation-type:*" identity))
           {"source" {:terms {:field "source" :size 5}}
            "relation-type" {:terms {:field "relation-type" :size 100}}})))

  (testing "No facet gives empty query"
    (is (= nil (facet/build-facet-query (parameters/parse "" identity))))))


