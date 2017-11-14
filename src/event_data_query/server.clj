(ns event-data-query.server
 (:require  [event-data-query.ingest :as ingest]
            [event-data-query.elastic :as elastic]
            [event-data-query.parameters :as parameters]
            [event-data-query.query :as query]
            [event-data-query.facet :as facet]
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

            [clojure.data.json :as json]
            [clojure.java.io :as io]

            [org.httpkit.server :as server]
            
            [clojure.data.json :as json]
            [config.core :refer [env]]
            [compojure.core :refer [defroutes GET POST]]
            [ring.middleware.params :as middleware-params]
            [ring.middleware.resource :as middleware-resource]
            [ring.middleware.content-type :as middleware-content-type]
            [liberator.core :refer [defresource]]
            [liberator.representation :as representation]
            [ring.util.response :as ring-response]
            [overtone.at-at :as at-at]
            [slingshot.slingshot :refer [try+ throw+]])
  (:gen-class))

(def event-data-homepage "https://www.crossref.org/services/event-data")

(def terms-url
  "URL of terms and conditions, or nil."
  (:query-terms-url env))

(defn try-parse-int
  "Parse integer, if present, or throw."
  [value]
  (when value
    (Integer/parseInt value)))

(defn export-event
  "Transform an event to send out."
  [event]
  (if terms-url (assoc event "terms" terms-url) event))

(def default-page-size 1000)

(defn get-rows
  [ctx]
  (or (try-parse-int (get-in ctx [:request :params "rows"]))
      default-page-size))

(defn get-filters
      [ctx]
      (when-let [params (get-in ctx [:request :params "filter"])]
        (let [filters (parameters/parse params keyword)]
          
          ; Throws validation exception.
          (query/validate-filter-keys filters)

          ; The from-updated-date parameter is special so it gets its own query parameter (outside filter).
          ; But we merge it in with the filter params at this point.
          (if-let [updated-date (get-in ctx [:request :params "from-updated-date"])]
            (assoc filters :from-updated-date updated-date)
            filters))))

(defn get-facets
  [ctx]
  (when-let [params (get-in ctx [:request :params "facet"])]
    (let [facets (parameters/parse params identity)]
    
     ; Throws validation exception.
     (facet/validate facets)
     facets)))

(defn get-cursor-value
  [ctx]
  (get-in ctx [:request :params "cursor"]))

(defn get-cursor-event
  [ctx]
  (when-let [event-id (not-empty (get-cursor-value ctx))]
    (let [event (elastic/get-by-id-full event-id elastic/event-type-name)]
      (when-not event
        (throw (new IllegalArgumentException "Invalid cursor supplied.")))
      event)))

(defn all-events-query
  [ctx]
  (elastic/search-query
    (::query ctx)
    (::facet-query ctx)
    elastic/event-type-name
    (::rows ctx)
    [{:timestamp "asc"} {:_uid "desc"}]
    [(or (-> ctx ::cursor-event :timestamp) 0) (or (-> ctx ::cursor-event :id) "")]))

(defn distinct-events-query
  [ctx]
  (elastic/search-query
  (::query ctx)
  (::facet-query ctx)
  elastic/distinct-type-name
  (::rows ctx)
  [{:_uid "asc"}]
  ; Need to re-created the value of _uid, which is combination of 
  ; type and _id.
  [(str elastic/distinct-type-name "#" (or (::cursor-value ctx) ""))]))

(def events-defaults
  "Base for 'events' and 'distinct events' resources."
  {:available-media-types ["application/json"]
   
   :handle-exception
   (fn [ctx]
    {:status "failed"
     :message "An internal error occurred. If you see this message repeatedly, please contact us."})

   ; Content negotiation doesn't work for this handler.
   ; https://github.com/clojure-liberator/liberator/issues/94
   :handle-malformed (fn [ctx]
                       (json/write-str {:status "failed"
                                        :message-type (::error-type ctx)
                                        :message [{:type (::error-subtype ctx)
                                                  :message (::error-message ctx)}]}))})


; 'events' has two different views
; - 'all events' shows every Event that exists.
; - 'distinct events' only shows one Events per distinct subj_id, obj_id pair.
; Each is served from a different index.
; In the 'events' index, the _id field is the object ID.
; In the 'distinct' index, the _id field is the hash of (subj_id, obj_id)
; For 'events' queries, Events are sorted by their timestamp, then by id.
; For 'distinct' queries, Events are sorted by their hash id (_id)
; Because of this, we pass in both 'sort' and 'search after' criteria to elastic/search-query
; The _uid field is a combination of type and _id, e.g. "latest#12345". This is indexed where _id isn't.
; Therefore, we use _uid to sort etc.

(defresource all-events
  []
  events-defaults
  :malformed?
  (fn [ctx]
    (try+
      (let [rows (get-rows ctx)
            filters (get-filters ctx)
            facets (get-facets ctx)
            query (query/build-filter-query filters)
            facet-query (facet/build-facet-query facets)

            ; Get the Event that corresponds to the cursor, if supplied.
            cursor-event (get-cursor-event ctx)]

        (log/info "Got filters:" filters "facet:" facets)
        (log/info "Execute query:" query "facet:" facet-query)

        [false
         {::rows rows
          ::query query
          ::facet-query facet-query
          ::cursor-event cursor-event}])

      (catch [:type :validation-failure] {:keys [message type subtype]}
        [true {::error-type type
               ::error-subtype subtype
               ::error-message message}])
      
      (catch IllegalArgumentException ex
        [true {::error-message (.getMessage ex)}])))

  :handle-ok
  (fn [ctx]
    (let [[events
           hits
           facet-results] (all-events-query ctx)
           total-results (elastic/count-query (::query ctx) elastic/event-type-name)
          
          next-cursor-id (-> events last :id)
 
          message {:next-cursor next-cursor-id
                   :total-results total-results
                   :items-per-page (::rows ctx)
                   :events (map export-event events)}
 
          ; facet-query can be null if not supplied.
          ; we don't want to show a nil result for facets if there was no facet query supplied
          message (if (::facet-query ctx)
                     (assoc message :facets facet-results)
                     message)]
      
     {:status "ok"
      :message-type "event-list"
      :message message})))

(defresource distinct-events
  []
  events-defaults

  :malformed?
  (fn [ctx]
    (try+
      (let [rows (get-rows ctx)
            filters (get-filters ctx)
            facets (get-facets ctx)
            query (query/build-filter-query filters)
            facet-query (facet/build-facet-query facets)

            cursor-value (get-cursor-value ctx)]

        (log/info "Got filters:" filters "facet:" facets)
        (log/info "Execute query:" query "facet:" facet-query)

        [false
         {::rows rows
          ::query query
          ::facet-query facet-query
          ::cursor-value cursor-value}])

      (catch [:type :validation-failure] {:keys [message type subtype]}
        [true {::error-type type
               ::error-subtype subtype
               ::error-message message}])
      
      (catch IllegalArgumentException ex
        [true {::error-message (.getMessage ex)}])))

  :handle-ok
  (fn [ctx]
    (let [[events hits facet-results] (distinct-events-query ctx)
          total-results (elastic/count-query (::query ctx) elastic/distinct-type-name)
         
          next-cursor-id (-> hits last :_id)

          message {:next-cursor next-cursor-id
                   :total-results total-results
                   :items-per-page (::rows ctx)
                   :events (map export-event events)}

          ; facet-query can be null if not supplied.
          ; we don't want to show a nil result for facets if there was no facet query supplied
          message (if (::facet-query ctx)
                     (assoc message :facets facet-results)
                     message)]

    {:status "ok"
     :message-type "event-list"
     :message message})))

(defn get-interval
  [ctx]
  (condp = (get-in ctx [:request :params "interval"])
    "day" :day
    "week" :week
    "month" :month
    "year" :year
    (throw+ {:type :validation-failure
             :subtype :interval-unrecognised
             :message "Value of time interval unrecognised. Supply 'interval' value of 'day', 'week', 'month' or 'year'"})))

(defn get-field
  [ctx]
  (condp = (get-in ctx [:request :params "field"])
    "collected" :timestamp
    "occurred" :occurred
    "updated-date" :updated-date
    (throw+ {:type :validation-failure
             :subtype :field-unrecognised
             :message "Value of 'field' unrecognised. Supply 'field' value of 'collected', 'occurred' or 'updated-date'"})))


(defresource events-time
  [elastic-type]
  :available-media-types ["text/csv"]
  :malformed?
  (fn [ctx]
    (try+
      (let [filters (get-filters ctx)
            query (query/build-filter-query filters)
            interval (get-interval ctx)
            field (get-field ctx)]

        (log/info "All Events by time filters:" filters " with interval:" interval " on field:" field)
        (log/info "All Events by time query:" query)

        [false
         {::interval interval
          ::query query
          ::field field}])

      (catch [:type :validation-failure] {:keys [message type subtype]}
        [true {::error-type type
               ::error-subtype subtype
               ::error-message message}])))

  ; Handle this in 'exists' in case there's no data all for the range.
  ; Unlikely, but we can't produce an empty CSV file.
  :exists?
  (fn [ctx]
    (let [results (elastic/time-facet-query
                    (::query ctx)
                    elastic-type
                    (::field ctx)
                    (::interval ctx))
          ; Need to massage to with with Liberator's representations requirements.
          response (map (fn [[d v]] {:date d :value v}) results)]
      
      (if (empty? response)
        false
        [true {::response response}])))

  :handle-ok
  (fn [ctx]
    (::response ctx))

  :handle-not-found
  (fn [ctx]
    (json/write-str {:status "not-found"}))

   ; Content negotiation doesn't work for this handler.
   ; https://github.com/clojure-liberator/liberator/issues/94
   :handle-malformed (fn [ctx]
                       (json/write-str {:status "failed"
                                        :message-type (::error-type ctx)
                                        :message [{:type (::error-subtype ctx)
                                                  :message (::error-message ctx)}]})))

(defresource event
  [id]
  :available-media-types ["application/json"]
  
  :exists? (fn [ctx]
            (let [the-event (elastic/get-by-id id elastic/event-type-name)
                  deleted (= (get the-event :updated) "deleted")
                  ; User can request to show anyway
                  include-deleted (= (get-in ctx [:request :params "include-deleted"]) "true")]
              [(and the-event (or include-deleted
                                  (not deleted))) {::event the-event}]))

  :handle-ok (fn [ctx]
              {:status "ok"
               :message-type "event"
               :message {
                 :event (export-event (::event ctx))}})

  :handle-not-found (fn [ctx] {:status "not-found"}))


(defresource alternative-ids-check
  []
  :allowed-methods [:get]
  :available-media-types ["application/json"]
  :handle-ok (fn [ctx]
               (let [ids (vec (.split
                                (get-in ctx [:request :params "ids"] "")
                                ","))
               
                    matches (elastic/alternative-ids-exist ids elastic/event-type-name)]
                {:alternative-ids matches})))

(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
                (representation/ring-response
                  (ring-response/redirect event-data-homepage))))

(defroutes app-routes
  (GET "/" [] (home))
  (GET "/events" [] (all-events))
  (GET "/events/distinct" [] (distinct-events))

  (GET "/events/time.csv" [] (events-time elastic/event-type-name))
  (GET "/events/distinct/time.csv" [] (events-time elastic/distinct-type-name))

  (GET "/events/:id" [id] (event id))
  (GET "/special/alternative-ids-check" [] (alternative-ids-check)))

(defn wrap-cors [handler]
  (fn [request]
    (let [response (handler request)]
      (assoc-in response [:headers "Access-Control-Allow-Origin"] "*"))))

(def app
  (-> app-routes
     middleware-params/wrap-params
     (middleware-resource/wrap-resource "public")
     (middleware-content-type/wrap-content-type)
     (wrap-cors)))

(def schedule-pool (at-at/mk-pool))

(defn run []
  (let [port (Integer/parseInt (:query-port env))]
    (log/info "Start server on " port)
    (server/run-server app {:port port})))
