(ns event-data-query.core-test
  (:require [clojure.test :refer :all]
            [event-data-query.core :refer :all]))

(deftest run-ingest
  (testing "Run-ingest should query for all days betweeb date and epoch if not already queried for.")

  (testing "Run-ingest should not repeat queries for days that have already been indexed if force flag false.")

  (testing "Run-ingest should repeat queries for days that have already been indexed if force flag true.")

  (testing "Run-ingest should not repeat queries for days that have already been indexed if force flag false.")

)

(deftest query-date-epoch
  (testing "query should not ")

  )




