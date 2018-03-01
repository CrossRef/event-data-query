(ns event-data-query.ingest
 (:require [event-data-query.elastic :as elastic]
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
           [clojurewerkz.quartzite.schedule.cron :as qc]
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
           [clojure.walk :as walk]
           [clojure.tools.logging :as log]
           [com.climate.claypoole :as cp])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerRecords])
  (:gen-class))


(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))

(def insert-chunk-size 1000)

(defn yesterday
  []
  (clj-time/minus (clj-time/now) (clj-time/days 1)))

(defn retrieve-source-whitelist
  "Retrieve set of source IDs according to config, or nil if not configured."
  []
  (when-let [artifact-name (:query-whitelist-artifact-name env)]
    (let [source-names (-> artifact-name artifact/fetch-latest-artifact-string (clojure.string/split #"\n") set)]
      (log/info "Retrieved source names:" source-names)
      source-names)))

(def source-whitelist
  (delay (retrieve-source-whitelist)))

(defn retrieve-prefix-whitelist
  "Retrieve set of DOI prefixes as a set according to config, or nil if not configured."
  []
  (when-let [artifact-name (:query-prefix-whitelist-artifact-name env)]
    (let [prefixes (-> artifact-name artifact/fetch-latest-artifact-string (clojure.string/split #"\n") set)]
      (log/info "Retrieved " (count prefixes) "prefixes" (type prefixes))
      prefixes)))

(def prefix-whitelist
  (delay (retrieve-prefix-whitelist)))

(defn filter-prefix-whitelist
  [events]
  (if-let [prefixes (deref prefix-whitelist)]
    (filter #(let [subj-id (get % "subj_id")
                   obj-id (get % "obj_id")]
             (or 
                 ; There's no DOI in either subj or obj position.
                 ; This can happen in theory.
                 (not (or (cr-doi/well-formed subj-id)
                          (cr-doi/well-formed obj-id)))
                 ; Or there's a whitelisted DOI prefix in the subject or object.
                 (prefixes (cr-doi/get-prefix subj-id))
                 (prefixes (cr-doi/get-prefix obj-id)))) events)
    events))

(defn filter-source-whitelist
  [events]
  (if-let [sources (deref source-whitelist)]
    (filter #(sources (get % "source_id")) events)
    events))

(defn filter-whitelists
  [events]
  (let [filtered (-> events filter-source-whitelist filter-prefix-whitelist)]
    (log/info "Whitelist filter kept:" (count filtered) "/" (count events) "removed:" (- (count events) (count filtered)))
    filtered))

(defn ingest-many
  "Ingest many event with string keys, pre-transformed. Reject if there is a source whitelist and it's not allowed."
  ([events] (ingest-many events false))
  ([events force?]
    (elastic/insert-events (filter-whitelists events) force?)))

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

(def event-bus-archive-prefix-length 2)
(def hexadecimal [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \a \b \c \d \e \f])

(defn event-bus-prefixes-length
  [length]
  (map #(apply str %) (combinatorics/selections hexadecimal length)))

(def bus-fetch-parallelism
  "How many simultaneous requests to the Event Bus archive?"
  10)

(defn bus-backfill-days
  [num-days force?]
  (elastic/set-refresh-interval! "-1")
  (let [prefixes (event-bus-prefixes-length event-bus-archive-prefix-length)
        end-date (clj-time/now)
        start-date (clj-time/minus end-date (clj-time/days num-days))
        date-range (take-while #(clj-time/before? % end-date) (clj-time-periodic/periodic-seq start-date (clj-time/days 1)))
        total-count (atom 0)]
    (doseq [date date-range]
      (let [date-str (clj-time-format/unparse ymd-format date)]
        (log/info "Ingest for date" date-str "...")

        (cp/pdoseq bus-fetch-parallelism [prefix prefixes]
          (let [url (str (:query-event-bus-base env) "/events/archive/" date-str "/" prefix)]
            (try
              (try-try-again
                {:sleep 30000 :tries 10}
                (fn []
                  (log/info "Try" date-str prefix)
                  (let [response (client/get url
                                   {:as :stream
                                    :timeout 900000
                                    :headers
                                    {"Authorization" (str "Bearer " (:query-jwt env))}})]
                      (log/info "Got" date-str prefix)
                      (with-open [body (io/reader (:body response))]
                        (let [stream (cheshire/parse-stream body)
                              events (get stream "events")
                              event-chunks (partition-all insert-chunk-size events)
                              chunks-count (atom 0)]
                          (log/info "Inserting chunks for" date-str prefix)
                          (doseq [chunk event-chunks]
                            (log/info "Ingesting chunk" @chunks-count "for" date-str prefix)
                            (ingest-many chunk force?)
                            (swap! total-count #(+ % (count chunk)))
                            (swap! chunks-count inc))
                          (log/info "Finished day prefix" date-str prefix)
                          (log/info "Inserted" @total-count "events this session"))))))
              (catch Exception ex (do
                (log/fatal "Unhandled failure ingesting archive" ex))))))
      (log/info "Finished day" date-str)))

      (elastic/set-refresh-interval! "60s")
      (log/info "Done all days.")
      (log/info "Inserted" @total-count "events this session, which is now over.")))


(defn run-ingest-kafka
  []
  (let [properties (java.util.Properties.)]
     (.put properties "bootstrap.servers" (:global-kafka-bootstrap-servers env))
     (.put properties "group.id"  "query-input")
     (.put properties "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     (.put properties "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     
     ; This is only used in the absence of an existing marker for the group.
     (.put properties "auto.offset.reset" "earliest")

     (elastic/set-refresh-interval! "60s")

     (let [consumer (KafkaConsumer. properties)
           topic-name (:global-bus-output-topic env)]
       (log/info "Subscribing to" topic-name)
       (.subscribe consumer (list topic-name))
       (log/info "Subscribed to" topic-name "got" (count (or (.assignment consumer) [])) "assigned partitions")
       (loop []
         (log/info "Polling...")
         (let [^ConsumerRecords records (.poll consumer (int 1000))
               events (map #(json/read-str (.value %)) records)]
            (log/info "Ingested" (count events) "events")
            (ingest-many events)
            (recur))))))


