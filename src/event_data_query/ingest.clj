(ns event-data-query.ingest
 (:require [event-data-query.elastic :as elastic]
           [event-data-common.event-bus :as event-bus]
           [clj-http.client :as client]
           [clj-time.core :as clj-time]
           [clj-time.format :as clj-time-format]
           [clj-time.periodic :as clj-time-periodic]
           [clojure.data.json :as json]
           [clojure.java.io :as io]
           [clojure.tools.logging :as log]
           [clojurewerkz.quartzite.jobs :as qj]
           [clojurewerkz.quartzite.jobs :refer [defjob]]
           [clojurewerkz.quartzite.schedule.cron :as qc]
           [clojurewerkz.quartzite.scheduler :as qs]
           [clojurewerkz.quartzite.triggers :as qt]
           [compojure.core :refer [defroutes GET]]
           [config.core :refer [env]]
           [liberator.core :refer [defresource]]
           [liberator.representation :as representation]
           [robert.bruce :refer [try-try-again]]
           [clojure.walk :as walk]
           [clojure.tools.logging :as log]
           [com.climate.claypoole :as cp]
           [clojure.core.async :refer [chan >! >!! <!! go close! onto-chan]]
           [event-data-common.whitelist :as whitelist])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerRecords])
  (:gen-class))


(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))

(def insert-chunk-size 1000)

(defn yesterday
  []
  (clj-time/minus (clj-time/now) (clj-time/days 1)))

(defn ingest-many
  "Ingest many event with string keys, pre-transformed.
   Reject if there is a source whitelist and it's not allowed."
  [events]
  (elastic/insert-events (whitelist/filter-events events)))

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
          body (json/read (io/reader (:body response)) :key-fn keyword)
          events (-> body :message :events)
          next-cursor (-> body :message :next-cursor)]
      (if next-cursor
        (lazy-cat events (fetch-query-api format-str date-str next-cursor))
        events))))

(defn replicate-backfill-days
  "Replicate the last n days from the upstream Query API.
   Retrieves both Events for that date and Events updated since that date."
  [num-days]
  (let [date (clj-time/minus (clj-time/now) (clj-time/days num-days))
        date-str (clj-time-format/unparse ymd-format date)

        events-collected-chunks (partition-all insert-chunk-size (fetch-query-api (:query-replica-collected-url env replica-collected-url-default) date-str))
        events-updated-chunks (partition-all insert-chunk-size (fetch-query-api (:query-replica-updated-url env replica-collected-url-default) date-str))

        collected-count (atom 0)
        updated-count (atom 0)]

    (doseq [chunk events-collected-chunks]
          (swap! collected-count #(+ % (count chunk)))
          (log/info "Ingested" @collected-count "this session, currently Downloading" date)
          (ingest-many chunk))

    (doseq [chunk events-updated-chunks]
          (swap! updated-count #(+ % (count chunk)))
          (log/info "Ingested" @updated-count "this session, currently Downloading" date)
          (ingest-many chunk))

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

(def bus-fetch-parallelism
  "How many simultaneous requests to the Event Bus archive?"
  10)

(defn bus-backfill-day
  [date]
  (let [date-str (clj-time-format/unparse ymd-format date)
        ; Chunks are partitioned before entering the chan, so we only need a couple of chunks' worth.
        event-channel (chan 2 (partition-all insert-chunk-size))
        total-count (atom 0)]
    (log/info "Ingest for date" date-str "...")

    ; This goroutine will be throttled by the chan's bounded buffer.
    (go
      (log/info "Start retreive...")
      (event-bus/retrieve-events-for-date date
        (fn [events]
          ; Iterate over lazy collection. core.async/onto-chan won't do the job, as it doesn't modify the chan it in place.
          (doseq [event events]
            (when event (>!! event-channel event)))))
      (close! event-channel)
      (log/info "Finish retreive!"))
    
    (log/info "Ingesting chunks for date" date-str)
    (loop [chunk (<!! event-channel)]
      (log/info "Ingesting chunk starting" (-> chunk first :id) "for" date-str)
      (ingest-many chunk)
      (swap! total-count #(+ % (count chunk)))
      (log/info "Finished ingesting chunk starting" (-> chunk first :id) "for" date-str "! Done" @total-count "so far...")

      (when-let [chunk (<!! event-channel)]
        (recur chunk)))

    (log/info "Finished ingestion for date" date-str "!")))


(defn bus-backfill-days
  [from-date num-days]
  (elastic/set-refresh-interval! "-1")
  (let [start-date (clj-time/minus from-date (clj-time/days num-days))
        date-range (take-while #(clj-time/before? % from-date) (clj-time-periodic/periodic-seq start-date (clj-time/days 1)))]
    
    (log/info "Backfill" num-days "from" start-date "...")
    ; Backfill a given day in one 
    (doseq [date date-range]
      ; Do each day as a distinct job, so we don't get overlapping partial days in parallel.
      (bus-backfill-day date)
      (log/info "Finished " num-days "days from" start-date "!"))))
    

(defn run-ingest-kafka
  []
  (let [properties  {"bootstrap.servers" (:global-kafka-bootstrap-servers env)
                     
                     ; In case there are concurrent ingestions mid query-update, make sure
                     ; each ingestion group doesn't interfere with the others.
                     "group.id" (-> env :query-deployment (str "query-input"))
                     
                      "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                      "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                      ; This is only used in the absence of an existing marker for the group.
                      "auto.offset.reset" "earliest"}

        ; Chunks are partitioned before entering the chan, so we only need a couple of chunks' worth.
        event-channel (chan 2 (partition-all insert-chunk-size))
        consumer (KafkaConsumer. properties)
        topic-name (:global-bus-output-topic env)
        total-count (atom 0)]

    ; In 'normal' ingestion mode, this gives Elastic enough slack to cope with high volumes.
    (elastic/set-refresh-interval! "60s")

    ; This goroutine will be throttled by the chan's bounded buffer.
    (go
      (log/info "Subscribing to" topic-name)
      (.subscribe consumer (list topic-name))
      (log/info "Subscribed to" topic-name "got" (count (or (.assignment consumer) [])) "assigned partitions")

      (loop []
        (log/info "Polling...")
        (let [^ConsumerRecords records (.poll consumer (int 1000))
              events (map #(json/read-str (.value %) :key-fn keyword) records)]
          
          (doseq [event events]
            (when event (>!! event-channel event)))
          
          ; This should never terminate.
          (recur))))
    
    (log/info "Ingest chunks...")
    (loop [chunk (<!! event-channel)]
      (log/info "Ingesting chunk starting" (-> chunk first :id) "with timestamp" (-> chunk first :timestamp))
      (ingest-many chunk)
      (swap! total-count #(+ % (count chunk)))
      (log/info "Finished ingesting chunk starting" (-> chunk first :id) "with timestamp" (-> chunk first :timestamp) "! Done" @total-count "so far...")

      (when-let [chunk (<!! event-channel)]
        (recur chunk)))))

