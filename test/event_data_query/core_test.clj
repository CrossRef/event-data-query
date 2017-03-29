(ns event-data-query.core-test
  (:require [clojure.test :refer :all]
            [event-data-query.core :refer :all]))

(deftest run-ingest


  (testing "Run-ingest should not repeat queries for days that have already been indexed if force flag false.")

  (testing "Run-ingest should repeat queries for days that have already been indexed if force flag true.")

  (testing "Run-ingest should not repeat queries for days that have already been indexed if force flag false.")

)

