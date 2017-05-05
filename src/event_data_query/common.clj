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
  [monger.collection :as mc]
  [monger.core :as mg]
  [monger.operators :as o]
  [monger.query :as q]
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

(def event-mongo-collection-name "events")
(def default-page-size 1000)

; Keep `transform-for-index and special-fields together.
(defn transform-for-index
  [event]
  (assoc event
    "_subj_prefix" (when-let [pid (get event "subj_id")] (when (cr-doi/well-formed pid) (cr-doi/get-prefix pid)))
    "_obj_prefix" (when-let [pid (get event "obj_id")] (when (cr-doi/well-formed pid) (cr-doi/get-prefix pid)))

    "_subj_doi" (when-let [pid (get event "subj_id")] (when (cr-doi/well-formed pid) (cr-doi/normalise-doi pid)))
    "_obj_doi" (when-let [pid (get event "obj_id")] (when (cr-doi/well-formed pid) (cr-doi/normalise-doi pid)))

    "_occurred-date" (parse-date (get event "occurred_at"))
    "_timestamp-date" (parse-date (get event "timestamp"))
    "_updated_date" (when-let [date (get event "updated_date")] (parse-date date))))

(def special-fields
  "Fields that we add for indexing, should not be exposed."
  [:_subj_prefix
   :_obj_prefix
   :_subj_doi
   :_obj_doi
   :_occurred-date
   :_timestamp-date
   :_updated_date
   ; mongo adds this
   :_id])

(def extra-index-fields
  "Fields that we want to index in addition to special-fields as [field unique]"
  [; For uniqueness constraint.
   ["id" true]
   ["_updated_date" false]
   ["source_id" false]
   ["experimental" false]
   ["updated" false]
   ["subj.internal-id" false]
   ["obj.internal-id" false]
   ["relation_type_id" false]
   ["subj_id" false]
   ["obj_id" false]

   ; Index occurred_at for sorting.
   ["occurred_at" false]])

(defn start-of [date-str]
  (let [parsed (clj-time-format/parse ymd-format date-str)]
    (clj-time/date-time (clj-time/year parsed) (clj-time/month parsed) (clj-time/day parsed))))

(defn end-of [date-str]
  (let [parsed (clj-time/plus (clj-time-format/parse ymd-format date-str) (clj-time/days 1))]
    (clj-time/date-time (clj-time/year parsed) (clj-time/month parsed) (clj-time/day parsed))))

