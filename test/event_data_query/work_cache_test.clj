(ns event-data-query.work-cache-test
  (:require [clojure.test :refer :all]
            [event-data-query.work-cache :as work-cache]
            [config.core :refer [env]]))

(deftest doi->id
  (testing "The same DOI expressed different ways results in the same cache ID"
    (is (=
           (work-cache/doi->id "10.5555/12345678abc")
           (work-cache/doi->id "10.5555/12345678ABC")
           (work-cache/doi->id "doi:10.5555/12345678abc")
           (work-cache/doi->id "doi.org/10.5555/12345678abc")
           (work-cache/doi->id "http://doi.org/10.5555/12345678abc")))))
