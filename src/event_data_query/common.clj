(ns event-data-query.common
 (:require  
  [cheshire.core :as cheshire]
  [clj-http.client :as client]
  [clj-time.core :as clj-time]
  [clj-time.format :as clj-time-format]
  [clj-time.periodic :as clj-time-periodic]
  [clojure.data.json :as json]
  [clojure.java.io :as io]
  [clojure.tools.logging :as log]
  [config.core :refer [env]]
  [crossref.util.doi :as cr-doi]
  [org.httpkit.server :as server]))

(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))
(def full-format-no-ms (:date-time-no-ms clj-time-format/formatters))
(def full-format (:date-time clj-time-format/formatters))
        
(defn parse-date   
  "Parse two kinds of dates."
  [date-str]
  (try
    (clj-time-format/parse full-format-no-ms date-str)
    (catch IllegalArgumentException e   
       (clj-time-format/parse full-format date-str))))

(defn yesterday
  []
  (clj-time/minus (clj-time/now) (clj-time/days 1)))

(def default-page-size 1000)

(defn start-of [date-str]
  (let [parsed (clj-time-format/parse ymd-format date-str)]
    (clj-time/date-time (clj-time/year parsed) (clj-time/month parsed) (clj-time/day parsed))))

(defn end-of [date-str]
  (let [parsed (clj-time/plus (clj-time-format/parse ymd-format date-str) (clj-time/days 1))]
    (clj-time/date-time (clj-time/year parsed) (clj-time/month parsed) (clj-time/day parsed))))

