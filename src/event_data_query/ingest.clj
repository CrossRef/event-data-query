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


(defn run-ingest
  "Ingest data from the epoch to the end date inclusive."
  ([force?] (run-ingest [(common/yesterday) force?]))
  ([end-date force?]
    (log/info "Ingest from" @common/epoch "to" end-date "force?" force?)
    (mg/set-default-write-concern! WriteConcern/ACKNOWLEDGED)
    (let [{:keys [conn db]} (mg/connect-via-uri (:mongodb-uri env))
          date-range (take-while #(clj-time/before? % end-date) (clj-time-periodic/periodic-seq @common/epoch (clj-time/days 1)))
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
                    (mc/update-by-id db common/event-mongo-collection-name (get event "id") (common/transform-for-index event) {:upsert true})
                    (swap! total-count inc)
                    (when (zero? (mod @total-count 10000))
                      (log/info "Ingested" @total-count "this session, currently Downloading" date))
                    ))))
            (common/set-indexed-day! db date-str)))

        (log/info "Pushed" @total-count "this session..."))
      (log/info "Finished ingesting dates. Set indexes...")
      ; These will probably already be in place from last time.
      (mc/ensure-index db common/event-mongo-collection-name {"_subj_prefix" 1})
      (mc/ensure-index db common/event-mongo-collection-name {"_obj_prefix" 1})
      (mc/ensure-index db common/event-mongo-collection-name {"id" 1})
      (mc/ensure-index db common/event-mongo-collection-name {"_occurred-date" 1})
      (mc/ensure-index db common/event-mongo-collection-name {"_timestamp-date" 1})
      (mc/ensure-index db common/event-mongo-collection-name {"_updated-date" 1})

      ; Index occurred_at for sorting.
      (mc/ensure-index db common/event-mongo-collection-name {"occurred_at" 1})

      (log/info "Done!"))))

