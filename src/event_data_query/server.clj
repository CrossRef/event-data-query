(ns event-data-query.server
 (:require  [event-data-query.common :as common]
            [event-data-common.status :as status]
            [event-data-query.ingest :as ingest]
            [event-data-query.query :as query]
            [event-data-common.jwt :as jwt]
            [config.core :refer [env]]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.periodic :as clj-time-periodic]
            [crossref.util.doi :as cr-doi]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clj-http.client :as client]
            [cheshire.core :as cheshire]
            [clojurewerkz.quartzite.triggers :as qt]
            [clojurewerkz.quartzite.jobs :as qj]
            [clojurewerkz.quartzite.schedule.daily-interval :as daily]
            [clojurewerkz.quartzite.schedule.calendar-interval :as cal]
            [clojurewerkz.quartzite.jobs :refer [defjob]]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.schedule.cron :as qc]

            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.query :as q]
            ; Not directly used, but converts clj-time dates in the background.
            [monger.joda-time]
            [clojure.data.json :as json]
            [clojure.java.io :as io]

            [org.httpkit.server :as server]
            [event-data-common.artifact :as artifact]
            [clojure.data.json :as json]
            [config.core :refer [env]]
            [compojure.core :refer [defroutes GET POST]]
            [ring.middleware.params :as middleware-params]
            [ring.middleware.resource :as middleware-resource]
            [ring.middleware.content-type :as middleware-content-type]
            [liberator.core :refer [defresource]]
            [liberator.representation :as representation]
            [ring.util.response :as ring-response]
            [overtone.at-at :as at-at])

  (:import [org.bson.types ObjectId]
           [com.mongodb DB WriteConcern])
  (:gen-class))

(def db (delay (:db (mg/connect-via-uri (:mongodb-uri env)))))

(def event-data-homepage "https://www.crossref.org/services/event-data")

(def terms-url
  "URL of terms and conditions, or nil."
  (:terms env))

(def sourcelist-name
  "Artifact name for our source list."
  "crossref-sourcelist")

(defn get-sourcelist
  "Fetch a set of source_ids that we're allowed to serve."
  []
  (let [source-names (-> sourcelist-name artifact/fetch-latest-artifact-string (clojure.string/split #"\n") set)]
    (log/info "Retrieved source names:" source-names)
    source-names))

(defn execute-query
  "Execute a query against the database."
  [db query rows]
    (let [cnt (mc/count db common/event-mongo-collection-name query)

          ; Mongo ignores limit parmeter when it's zero, so don't run query in that case.
          results (if (zero? rows)
                    []
                    (q/with-collection db common/event-mongo-collection-name
                      (q/find query)
                      (q/sort (array-map :id 1))
                      (q/limit rows)))
          events (map #(apply dissoc % common/special-fields) results)

          next-cursor (-> results last :_id)]          
      [events next-cursor cnt]))

(defn get-event
  [db id]
  (when-let [event (mc/find-one-as-map db common/event-mongo-collection-name {:id id})]
    (apply dissoc event common/special-fields)))

(defn split-filter
  "Split a comma and colon separated filter into a map, or :error."
  [filter-str]
  (when filter-str
    (try
      (into {} (map (fn [pair] (let [[k v] (clojure.string/split pair #":")] [(keyword k) v])) (clojure.string/split filter-str #",")))
      (catch IllegalArgumentException e :error))))

(defn try-parse-int
  [value]
  (when value
    (try (Integer/parseInt value)
      (catch IllegalArgumentException ex :error))))

(defn try-parse-ymd-date
  "Parse date when present, or :error on failure."
  [date-str]
  (when date-str
    (try
      (clj-time-format/parse common/ymd-format date-str)
      (catch Exception _ :error))))

(defn export-event
  "Transform an event to send out."
  [event]
  (if terms-url (assoc event "terms" terms-url) event))

(defresource events
  []
  :available-media-types ["application/json"]
  
  :malformed? (fn [ctx]
                (let [rows (or
                             (try-parse-int (get-in ctx [:request :params "rows"]))
                             common/default-page-size)

                      filters (split-filter (get-in ctx [:request :params "filter"]))
                      
                      filter-query (try (query/build-filter-query filters) (catch IllegalArgumentException ex :error))
                      meta-query (try (query/build-meta-query (get-in ctx [:request :params])) (catch IllegalArgumentException ex :error))
                      
                      malformed (:error (set [rows filter-query meta-query]))
                      the-query (when-not malformed (query/and-queries filter-query meta-query))]

                  (log/info "Execute query" the-query)

                  [malformed
                   {::rows rows
                    ::query the-query}]))

  :handle-ok (fn [ctx]
               (let [[events next-cursor total-results] (execute-query @db (::query ctx) (::rows ctx))
                      exported-events (map export-event events)]
                (status/send! "query" "serve" "event" (count events))
                (status/send! "query" "serve" "request" 1)
                {:status "ok"
                 :message-type "event-list"
                 :message {
                   :next-cursor next-cursor
                   :total-results total-results
                   :items-per-page (::rows ctx)
                   :events exported-events}})))

(defresource event
  [id]
  :available-media-types ["application/json"]
  
  :exists? (fn [ctx]
            (let [the-event (get-event @db id)]
              [the-event {::event the-event}]))

  :handle-ok (fn [ctx]
              (status/send! "query" "serve" "event" 1)
              (status/send! "query" "serve" "request" 1)
              {:status "ok"
               :message-type "event"
               :message {
                 :event (export-event (::event ctx))}})

  :handle-not-found (fn [ctx] {:status "not-found"}))

(defresource post-events
  []
  :allowed-methods [:post]
  :available-media-types ["application/json"]
  :authorized? (fn [ctx]
                ; must validate ok, nothing more
                (-> ctx :request :jwt-claims))

  :malformed? (fn [ctx]
                (let [event-body (try (-> ctx :request :body slurp json/read-str) (catch Exception ex nil))
                      transformed-body (try  (common/transform-for-index event-body) (catch Exception ex nil))]
                  [(not (and event-body transformed-body)) {::transformed-body transformed-body}]))

  :post! (fn [ctx]
           (status/send! "query" "ingest" "event" 1)
           (ingest/ingest-one @db (::transformed-body ctx))))


(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
                (representation/ring-response
                  (ring-response/redirect event-data-homepage))))

(defroutes app-routes
  (GET "/" [] (home))
  (GET "/events" [] (events))
  (GET "/events/:id" [id] (event id))
  (POST "/events" [] (post-events)))

(defn wrap-cors [handler]
  (fn [request]
    (let [response (handler request)]
      (assoc-in response [:headers "Access-Control-Allow-Origin"] "*"))))

(def app
  (-> app-routes
     middleware-params/wrap-params
     (jwt/wrap-jwt (:jwt-secrets env))
     (middleware-resource/wrap-resource "public")
     (middleware-content-type/wrap-content-type)
     (wrap-cors)))


(def schedule-pool (at-at/mk-pool))


(defn run []
  (reset! query/sourcelist (get-sourcelist))
  (reset! query/whitelist-override (:whitelist-override env))

  (let [port (Integer/parseInt (:port env))]
    (at-at/every 10000 #(status/send! "query" "heartbeat" "tick" 1) schedule-pool)

    (log/info "Start server on " port)
    (server/run-server app {:port port})))
