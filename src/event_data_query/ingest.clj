(ns event-data-query.ingest
 (:require [event-data-query.elastic :as elastic]
           [event-data-common.queue :as queue]
           [event-data-common.artifact :as artifact]
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
           [clojure.math.combinatorics :as combinatorics]
           [robert.bruce :refer [try-try-again]]
           [clojure.walk :as walk])
  (:gen-class))

(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))

(defn yesterday
  []
  (clj-time/minus (clj-time/now) (clj-time/days 1)))

(defn retrieve-source-whitelist
  "Retrieve set of source IDs according to config, or nil if not configured."
  []
  (when-let [artifact-name (:whitelist-artifact-name env)]
    (let [source-names (-> artifact-name artifact/fetch-latest-artifact-string (clojure.string/split #"\n") set)]
      (log/info "Retrieved source names:" source-names)
      source-names)))

(def source-whitelist
  (delay (retrieve-source-whitelist)))

(defn ingest-one
  "Ingest one event with string keys, pre-transformed. Reject if there is a source whitelist and it's not allowed."
  [event]
  (when (or (nil? @source-whitelist)
            (@source-whitelist (get event "source_id")))
    (elastic/insert-event event)))

(defn ingest-many
  "Ingest many event with string keys, pre-transformed. Reject if there is a source whitelist and it's not allowed."
  [events]
  (if (nil? @source-whitelist)
    (elastic/insert-events events)
    (elastic/insert-events 
      (filter #(@source-whitelist (get % "source_id")) events))))

(def replica-collected-url-default
  "https://query.eventdata.crossref.org/events?filter=from-collected-date:%1$s&cursor=%2$s&rows=10000")

(def replica-updated-url-default
  "https://query.eventdata.crossref.org/events?from-updated-date=%1$s&cursor=%2$s&rows=10000")

(defn fetch-query-api
  "Fetch a lazy seq of Events updated since the given YYYY-MM-DD date string.
   Format string should have date at index 1 and cursor at index 2."
  ([format-str date-str] (fetch-query-api format-str date-str ""))
  ([format-str date-str cursor]
    (log/info "Fetch Query API" date-str "cursor" cursor)
    (let [url (format format-str date-str cursor)
          response (try-try-again {:sleep 30000 :tries 10} #(client/get url {:as :stream :timeout 900000}))
          body (json/read (io/reader (:body response)))
          events (get-in body ["message" "events"])
          next-cursor (get-in body ["message" "next-cursor"])]
      (if next-cursor
        (lazy-cat events (fetch-query-api format-str date-str next-cursor))
        events))))

(defn replicate-backfill-days
  "Replicate the last n days from the upstream Query API.
   Retrieves both Events for that date and Events updated since that date."
  [num-days]
  (let [date (clj-time/minus (clj-time/now) (clj-time/days num-days))
        date-str (clj-time-format/unparse ymd-format date)
        counter (atom 0)]
    
    (log/info "Replicating occurred Events from" date-str)
    (doseq [event (fetch-query-api (:replica-collected-url env replica-collected-url-default) date-str)]
      (ingest-one event)
      (swap! counter inc)
      (when (zero? (mod @counter 1000))
                    (log/info "Ingested" @counter "this session, currently Downloading" date)))

    (log/info "Replicating updated Events from" date-str)
    (doseq [event (fetch-query-api (:replica-updated-url env replica-collected-url-default) date-str)]
      (ingest-one event)
      (swap! counter inc)
      (when (zero? (mod @counter 1000))
                    (log/info "Ingested" @counter "this session, currently Downloading" date)))
    (log/info "Done replicating.")))

(defjob replicate-yesterday-job
  [ctx]
  (log/info "Start replicating yesterday's data on schedule.")
  (replicate-backfill-days 1)
  (log/info "Finished replicating yesterday's data on schedule."))

(defn replicate-continuous
  "Start schedule to replicate yesterday's data. Block."
  []
  (log/info "Start scheduler")
  (let [s (-> (qs/initialize) qs/start)
        job (qj/build
              (qj/of-type replicate-yesterday-job)
              (qj/with-identity (qj/key "jobs.noop.1")))
        trigger (qt/build
                  (qt/with-identity (qt/key "triggers.1"))
                  (qt/start-now)
                  (qt/with-schedule (qc/cron-schedule "5 0 0 * * ?")))]
  (qs/schedule s job trigger)))

(def event-bus-archive-prefix-length 2)
(def hexadecimal [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \a \b \c \d \e \f])

(defn event-bus-prefixes-length
  [length]
  (map #(apply str %) (combinatorics/selections hexadecimal length)))

(def insert-chunk-size 1000)

(defn bus-backfill-days
  [num-days]
  (let [prefixes (event-bus-prefixes-length event-bus-archive-prefix-length)
        end-date (clj-time/now)
        start-date (clj-time/minus end-date (clj-time/days num-days))
        date-range (take-while #(clj-time/before? % end-date) (clj-time-periodic/periodic-seq start-date (clj-time/days 1)))
        total-count (atom 0)]
    (doseq [date date-range]
      (let [date-str (clj-time-format/unparse ymd-format date)
            prefix-urls (map #(str (:event-bus-base env) "/events/archive/" date-str "/" %) prefixes)
            api-results (pmap (fn [url]
                                 (log/info "Fetch archive prefix URL " url)
                                 (client/get url {:as :stream
                                                :timeout 900000
                                                :headers {"Authorization" (str "Bearer " (:jwt-token env))}})) prefix-urls)
            events (mapcat #(with-open [body (io/reader (:body %))]
                              (let [stream (cheshire/parse-stream body)]
                                (get stream "events"))) api-results)
            event-chunks (partition-all insert-chunk-size events)]
        (doseq [chunk event-chunks]
          (swap! total-count #(+ % (count chunk)))
          (log/info "Ingested" @total-count "this session, currently Downloading" date)
          (ingest-many chunk))
        (log/info "Done all chunks for day.")))
    (log/info "Done all days.")))



(defn queue-continuous
  "Ingest Events from an ActiveMQ Queue. Block."
  []
  (let [config {:username (:activemq-username env) :password (:activemq-password env) :url (:activemq-url env) :queue-name (:activemq-queue env)}]
    (log/info "Starting to listening to queue with config" config)
    ; The queue library kindly deserializes JSON back into objects, but keywor
    (queue/process-queue config #(ingest-one (walk/stringify-keys %)))
    (log/error "Finished listening to queue.")))

