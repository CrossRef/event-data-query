(ns event-data-query.server
 (:require  [event-data-query.ingest :as ingest]
            [event-data-query.scholix :as scholix]
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

            [clojure.data.json :as json]
            [clojure.java.io :as io]

            [org.httpkit.server :as server]
            
            [clojure.data.json :as json]
            [config.core :refer [env]]
            [compojure.core :refer [defroutes GET POST context]]
            [ring.middleware.params :as middleware-params]
            [ring.middleware.resource :as middleware-resource]
            [ring.middleware.content-type :as middleware-content-type]
            [liberator.core :refer [defresource]]
            [liberator.representation :as representation]
            [ring.util.response :as ring-response]
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

(defn document->event
  "Transform a Document into an Event to send out."
  [document]
  
  (let [event (:event document)]
    (if terms-url
      (assoc event "terms" terms-url)
      event)))

(def default-page-size 1000)

(defn get-rows
  [ctx]
  (or (try-parse-int (get-in ctx [:request :params "rows"]))
      default-page-size))

(defn get-filters-deprecated
  "DEPRECATED. Get filter dictionary from the 'filter' param."
      [ctx]
      (when-let [params (get-in ctx [:request :params "filter"])]

        (let [; If multiple 'filter' params supplied, join them.
              params (if (sequential? params)
                          (clojure.string/join "," params)
                          params)

              filters (parameters/parse params keyword)]
          
          ; Throws validation exception.
          (query/validate-filter-keys filters)

          ; The from-updated-date parameter is special so it gets its own query parameter (outside filter).
          ; But we merge it in with the filter params at this point.
          (if-let [updated-date (get-in ctx [:request :params "from-updated-date"])]
            (assoc filters :from-updated-date updated-date)
            filters))))


(defn get-filters
  "Get filter dictionary from query params."
  [ctx]
  (let [params (get-in ctx [:request :params])
        as-keywords (map (fn [[k v]] [(keyword k) v]) params)
        relevant (filter (fn [[k v]] (query/filters k)) as-keywords)]
    (into {} relevant)))

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
  [index-id ctx]
  (when-let [event-id (not-empty (get-cursor-value ctx))]
    (let [event (:event (elastic/get-by-id index-id event-id))]
      (when-not event
        (throw (new IllegalArgumentException "Invalid cursor supplied.")))
      event)))

(def events-defaults
  "Base for 'events' and 'distinct events' resources."
  {:available-media-types ["application/json"]
   
   :handle-exception
   (fn [ctx]
    (log/error (:exception ctx))
    (clojure.pprint/pprint ctx)
    {:status "failed"
     :message "An internal error occurred. If you see this message repeatedly, please contact us."})

   ; Content negotiation doesn't work for this handler.
   ; https://github.com/clojure-liberator/liberator/issues/94
   :handle-malformed (fn [ctx]
                       (json/write-str {:status "failed"
                                        :message-type (::error-type ctx)
                                        :message [{:type (::error-subtype ctx)
                                                  :message (::error-message ctx)}]}))})

(def events-recognised-parameters
  "Set of query parameters we should expect on the /events routes."
  (clojure.set/union
    (->> query/filters keys (map keyword) set)
    #{:cursor :rows :facet :filter}))


(defn unrecognised-query-params
  [ctx]
  (let [param-keys (->> ctx :request :params keys (map keyword) set)]
    (clojure.set/difference param-keys events-recognised-parameters)))


; 'events' has two different views
; - 'all events' shows every Event that exists.
; - 'distinct events' only shows one Events per distinct subj_id, obj_id pair.
; Each is served from a different index.
; In the 'events' index, the _id field is the object ID.
; In the 'distinct' index, the _id field is the hash of (subj_id, obj_id)
; For 'events' queries, Events are sorted by their timestamp, then by id.
; For 'distinct' queries, Events are sorted by their hash id (_id)
; Because of this, we pass in both 'sort' and 'search after' criteria to elastic/search-query
; The _id field is a combination of type and id, e.g. "latest#12345".
(defresource query-events
  [index-id event-transform-f]
  events-defaults
  :malformed?
  (fn [ctx]
    (try+
      (let [rows (get-rows ctx)

            ; Support deprecated filters and new-style filters for now.
            ; Only one type is allowed though, see :filter-conflict error.
            deprecated-filters (get-filters-deprecated ctx)
            new-filters (get-filters ctx)
            filters (merge deprecated-filters new-filters)

            facets (get-facets ctx)
            query (query/build-filter-query filters)
            facet-query (facet/build-facet-query facets)

            ; Get the Event that corresponds to the cursor, if supplied.
            cursor-event (get-cursor-event index-id ctx)

            unrecognised (unrecognised-query-params ctx)]

        (log/info "Got new-filters:" new-filters "deprecated-filters:" deprecated-filters "overall-filters:" filters "facet:" facets)
        (log/info "Execute query:" query "facet:" facet-query)

        ; Log the mailto.
        (log/info (json/write-str {:mailto (get-in ctx [:request :params "mailto"])}))

        (when (not-empty unrecognised)
          (throw+ {:type :validation-failure
             :subtype :query-parameter-unrecognised
             :message (str "You supplied unrecognised query parameters: " (clojure.string/join ", " (map name unrecognised)) ". The following query parameters are available:" (clojure.string/join ", " (map name events-recognised-parameters)))}))

        (when (and (not-empty deprecated-filters) (not-empty new-filters))
          (throw+ {:type :validation-failure
             :subtype :filter-conflict
             :message "Deprecated filter style supplied at the same time as new filter style. Please use new style, see the documentation."}))

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
    (let [[events facet-results]
          (elastic/search-query
            index-id
            (::query ctx)
            (::facet-query ctx)
            (::rows ctx)
            [{:timestamp "asc"} {:_id "desc"}]
            [(or (-> ctx ::cursor-event :timestamp) 0)
             (or (-> ctx ::cursor-event :id) "")])


           total-results (elastic/count-query index-id (::query ctx))
          
          next-cursor-id (-> events last :id)
 
          message {:next-cursor next-cursor-id
                   :total-results total-results
                   :items-per-page (::rows ctx)
                   ; If, under exceptional circumstances, event-transform-f returns nil, exclude that.
                   :events (keep event-transform-f events)}
 
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
    (throw+ {:type :validation-failure
             :subtype :field-unrecognised
             :message "Value of 'field' unrecognised. Supply 'field' value of 'collected', 'occurred'"})))


(defresource events-time
  [index-id]
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
                    index-id
                    (::query ctx)
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
  [index-id id event-transform-f]
  :available-media-types ["application/json"]
  
  :exists? (fn [ctx]
             (if-let [document (elastic/get-by-id index-id id)]
               [true {::document document}]
               [false {}]))

  :handle-ok (fn [ctx]
              {:status "ok"
               :message-type "event"
               :message {
                 :event (event-transform-f (::document ctx))}})

  :handle-not-found (fn [ctx] {:status "not-found"}))

(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
                (representation/ring-response
                  (ring-response/redirect event-data-homepage))))

(defroutes app-routes
  (GET "/" [] (home))

  (context "/v1" []

    ; All resources and sub-resources for each collection.
    (GET "/events/edited" [] (query-events :edited document->event))
    (GET "/events/edited/time.csv" [] (events-time :edited))
    (GET "/events/edited/:id" [id] (event :edited id document->event))

    (GET "/events/deleted" [] (query-events :deleted document->event))
    (GET "/events/deleted/time.csv" [] (events-time :deleted))
    (GET "/events/deleted/:id" [id] (event :deleted id document->event))

    (GET "/events/experimental" [] (query-events :experimental document->event))
    (GET "/events/experimental/time.csv" [] (events-time :experimental))
    (GET "/events/experimental/:id" [id] (event :experimental id document->event))

    (GET "/events/distinct" [] (query-events :distinct document->event))
    (GET "/events/distinct/time.csv" [] (events-time :distinct))
    (GET "/events/distinct/:id" [id] (event :distinct id document->event))

    (GET "/events/scholix" [] (query-events :scholix scholix/document->event))
    (GET "/events/scholix/time.csv" [] (events-time :scholix))
    (GET "/events/scholix/:id" [id] (event :scholix id scholix/document->event))

    (GET "/events" [] (query-events :standard document->event))
    (GET "/events/time.csv" [] (events-time :standard))
    (GET "/events/:id" [id] (event :standard id document->event))))

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

(defn run
  []
  (let [port (Integer/parseInt (:query-port env))]
    (log/info "Start server on " port)
    (server/run-server app {:port port})))
