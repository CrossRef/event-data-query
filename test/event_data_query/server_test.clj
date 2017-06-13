(ns event-data-query.server-test
  (:require [clojure.test :refer :all]
            [event-data-query.server :as server]
            [clj-time.core :as clj-time]))

(deftest try-parse-int
  (testing "try-parse-int can parse valid integer"
    (is (= 12345 (server/try-parse-int "12345")))
    (is (thrown? NumberFormatException (server/try-parse-int "ONE TWO THREE FOUR")))))

(deftest export-event
  (testing "export-event adds extra fields"
    (is (= (server/export-event {"input" "event"})
           {"input" "event"
            ; terms taken from docker-compose.yml config
            "terms" "https://doi.org/10.13003/CED-terms-of-use"}))))
