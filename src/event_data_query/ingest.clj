(ns event-data-query.ingest
 (:require [event-data-query.common :as common]
           [cheshire.core :as cheshire]
           [clj-http.client :as client]
           [clj-time.core :as clj-time]
           [clj-time.format :as clj-time-format]
           [clj-time.periodic :as clj-time-periodic]
           [clojure.data.json :as json]
           [clojure.java.io :as io]
           [clojure.tools.logging :as log]
           [clojurewerkz.quartzite.jobs :as qj]
           [clojurewerkz.quartzite.jobs :refer [defjob]]
           [clojurewerkz.quartzite.schedule.calendar-interval :as cal]
           [clojurewerkz.quartzite.schedule.cron :as qc]
           [clojurewerkz.quartzite.schedule.daily-interval :as daily]
           [clojurewerkz.quartzite.scheduler :as qs]
           [clojurewerkz.quartzite.triggers :as qt]
           [compojure.core :refer [defroutes GET]]
           [config.core :refer [env]]
           [crossref.util.doi :as cr-doi]
           [event-data-common.artifact :as artifact]
           [liberator.core :refer [defresource]]
           [liberator.representation :as representation]
           [monger.collection :as mc]
           [monger.core :as mg]
           ; Not directly used, but converts clj-time dates in the background.
           [monger.joda-time]
           [monger.operators :as o]
           [monger.query :as q])
  (:import [org.bson.types ObjectId]
           [com.mongodb DB WriteConcern])
  (:gen-class))

(defn ingest-one
  "Ingest one event with string keys, pre-transformed."
  [db transformed-event]
  (mc/update-by-id db common/event-mongo-collection-name (get transformed-event "id") transformed-event {:upsert true}))

(defn add-indexes 
  "Add indexes to database if not exist"
  []
  (log/info "Adding indexes...")
  (let [{:keys [conn db]} (mg/connect-via-uri (:mongodb-uri env))
        counter (atom 0)
        total-fields (+ (count common/special-fields) (count common/extra-index-fields))]
    (doseq [field common/special-fields]
      (swap! counter inc)
      (log/info "Adding" field " " @counter "/" total-fields)
      (mc/ensure-index db common/event-mongo-collection-name {field 1}))

    (doseq [[field unique] common/extra-index-fields]
      (swap! counter inc)
      (log/info "Adding" field " " @counter "/" total-fields)
      (mc/ensure-index db common/event-mongo-collection-name {field 1} {:unique unique}))))

(defn run-ingest
  "Ingest data from the start date to the end date inclusive."
  ([start-date end-date force?]
    (log/info "Ingest from" start-date "to" end-date ", force:" force?)
    (mg/set-default-write-concern! WriteConcern/ACKNOWLEDGED)
    (let [{:keys [conn db]} (mg/connect-via-uri (:mongodb-uri env))
          date-range (take-while #(clj-time/before? % (clj-time/plus end-date (clj-time/days 1))) (clj-time-periodic/periodic-seq start-date (clj-time/days 1)))
          total-count (atom 0)]
      (doseq [date date-range]
        (let [date-str (clj-time-format/unparse common/ymd-format date)
              previously-indexed (common/indexed-day? db date-str)
              should-index (or (nil? previously-indexed) force?)]

          (when-not should-index
            (log/info "Not indexing" date-str "already did on" previously-indexed))

          (when should-index
            (let [query-url (format (:source-query env) date-str)
                  result (client/get query-url {:as :stream})]
              (log/info "Downloading date" date "from" query-url "...")
              (with-open [body (io/reader (:body result))]
                (let [stream (cheshire/parse-stream body)
                      events (get stream "events")]
                  (log/info "Inserting...")
                  (doseq [event events]
                    (ingest-one db (common/transform-for-index event))
                    (swap! total-count inc)
                    (when (zero? (mod @total-count 10000))
                      (log/info "Ingested" @total-count "this session, currently Downloading" date))))))
            (common/set-indexed-day! db date-str)))

        (log/info "Pushed" @total-count "this session..."))
      (log/info "Finished ingesting dates. Set indexes...")
      
      (add-indexes)

      (log/info "Done!"))))

