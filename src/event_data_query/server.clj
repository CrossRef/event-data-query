(ns event-data-query.server
 (:require  [event-data-query.common :as common]
            [event-data-common.status :as status]
            [event-data-query.ingest :as ingest]
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
            [monger.operators :as o]
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

; Load at startup. The list changes so infrequently that the server can be restarted when a new one is added.
(def sourcelist
  "Set of whitelisted source ids"
  (atom nil))

(def whitelist-override
  "Ignore the whitelist?"
  (atom nil))


(defn and-queries
  "Return a Mongo query object that's the result of anding all params. Nils allowed."
  [& terms]
  (let [terms (remove nil? terms)]
    (if (empty? terms)
      {}
      {o/$and terms})))

(defn q-from-occurred-date
  [params]
  (when-let [date (:from-occurred-date params)]
    {:_occurred-date {o/$gte (common/start-of date)}}))

(defn q-until-occurred-date
  [params]
  (when-let [date (:until-occurred-date params)]
    {:_occurred-date {o/$lt (common/end-of date)}}))

(defn q-from-collected-date
  [params]
  (when-let [date (:from-collected-date params)]
    {:_timestamp-date {o/$gte (common/start-of date)}}))

(defn q-until-collected-date
  [params]
  (when-let [date (:until-collected-date params)] 
    {:_timestamp-date {o/$lt (common/end-of date)}}))

(defn q-work
  [params]
  (when-let [doi (:work params)]
    {o/$or [{:_subj_doi (cr-doi/normalise-doi doi)}
            {:_obj_doi (cr-doi/normalise-doi doi)}]}))

(defn q-prefix
  [params]
  (when-let [prefix (:prefix params)]
    {o/$or [{:_subj_prefix prefix} {:_obj_prefix prefix}]}))

(defn q-source
  [params]
  (if-let [source (:source params)]
     ; If source provided, use that, subject to whitelist.
    (if @whitelist-override
      {:source_id source}
      {:source_id (@sourcelist source)}) 
    ; Otherwise if no source provided, return all that match the whitelist unless overriden.
    (if @whitelist-override
       nil
       {:source_id {o/$in (vec @sourcelist)}})))

(def query-processors
  (juxt q-from-occurred-date
        q-until-occurred-date
        q-from-collected-date
        q-until-collected-date
        q-work
        q-prefix
        q-source))

(defn build-filter-query
  "Transform filter params dictionary into mongo query."
  [params]
  (apply and-queries (query-processors params)))

; Build meta-query fragments from query parameter dictionary. 
; Throw exceptions.
(defn mq-cursor
  [params]
  (when-let [value (params "cursor")]
    {:_id {o/$gt value}}))

(defn mq-experimental
  [params]
  (if (= (params "experimental") "true")
    {}
    {:experimental nil}))

(defn mq-updated-since-date
  [params]
  (if-let [date-str (params "from-updated-date")]
    {o/$and [{:_updated-date {o/$gte (common/start-of date-str)}}
             {:updated {o/$exists true}}]}
    {:updated {o/$ne "deleted"}}))

(def meta-query-processors
  (juxt mq-cursor
        mq-experimental
        mq-updated-since-date))

(defn build-meta-query
  "Transform query params dictionary into mongo query."
  [params]
  (apply and-queries (meta-query-processors params)))


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
                      
                      filter-query (try (build-filter-query filters) (catch IllegalArgumentException ex :error))
                      meta-query (try (build-meta-query (get-in ctx [:request :params])) (catch IllegalArgumentException ex :error))
                      
                      malformed (:error (set [rows filter-query meta-query]))
                      query (when-not malformed (and-queries filter-query meta-query))]

                  (log/info "Execute query" query)

                  [malformed
                   {::rows rows
                    ::query query}]))

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
  (reset! sourcelist (get-sourcelist))
  (reset! whitelist-override (:whitelist-override env))

  (let [port (Integer/parseInt (:port env))]
    (at-at/every 10000 #(status/send! "query" "heartbeat" "tick" 1) schedule-pool)

    (log/info "Start server on " port)
    (server/run-server app {:port port})))
