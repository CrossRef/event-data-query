(ns event-data-query.server-test
  (:require [clojure.test :refer :all]
            [event-data-query.server :as server]
            [event-data-query.common :as common]
            [clj-time.core :as clj-time]))

(deftest try-parse-ymd-date
  (testing "try-parse-ymd-date should parse dates, or return :error on error"
    (is (= (server/try-parse-ymd-date "2017-04-04") (clj-time/date-time 2017 4 4)) "Should parse valid")
    (is (= (server/try-parse-ymd-date "INVALID") :error) "Should return :error on invalid")))

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
