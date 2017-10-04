(ns event-data-query.server
 (:require  [event-data-common.status :as status]
            [event-data-query.ingest :as ingest]
            [event-data-query.elastic :as elastic]
            [event-data-query.parameters :as parameters]
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
            [slingshot.slingshot :refer [try+]])
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

(defresource events
  [type-name]
  :available-media-types ["application/json"]
  
  :malformed? (fn [ctx]
                (try+
                  (let [rows (or
                               (try-parse-int (get-in ctx [:request :params "rows"]))
                               default-page-size)

                        filters (when-let [params (get-in ctx [:request :params "filter"])] (parameters/parse params keyword))
                        

                        ; The from-updated-date parameter is special so it gets its own query parameter (outside filter).
                        ; But we merge it in with the filter params at this point.
                        filters (if-let [updated-date (get-in ctx [:request :params "from-updated-date"])]
                                (assoc filters :from-updated-date updated-date)
                                filters)

                        query (query/build-filter-query filters)

                        ; Get the whole event that is represented by the cursor ID. If supplied.
                        cursor-event (when-let [event-id (let [cursor-val (get-in ctx [:request :params "cursor"])]
                                                           (when-not (clojure.string/blank? cursor-val)
                                                             cursor-val))]
                                       (let [event (elastic/get-by-id-full event-id)]
                                        (when-not event
                                          (throw (new IllegalArgumentException "Invalid cursor supplied.")))
                                        event))]

                    ; This may throw.
                    (query/validate-filter-keys filters)

                    (log/info "Got filters" filters)
                    (log/info "Execute query" query)

                    [false
                     {::rows rows
                      ::query query
                      ::cursor-event cursor-event}])

                  (catch [:type :validation-failure] {:keys [message type subtype]}
                    [true {::error-type type
                           ::error-subtype subtype
                           ::error-message message}])
                  
                  (catch IllegalArgumentException ex
                    [true {::error-message (.getMessage ex)}])))

  :handle-ok (fn [ctx]
               (let [events (elastic/search-query (::query ctx)
                                                  type-name
                                                  (::rows ctx)
                                                  (-> ctx ::cursor-event :timestamp)
                                                  (-> ctx ::cursor-event :id))
                     total-results (elastic/count-query (::query ctx) type-name)
                     next-cursor-id (-> events last :id)]
                
                (when (:status-service env)
                  (status/send! "query" "serve" "event" (count events))
                  (status/send! "query" "serve" "request" 1))

                {:status "ok"
                 :message-type "event-list"
                 :message {
                   :next-cursor next-cursor-id
                   :total-results total-results
                   :items-per-page (::rows ctx)
                   :events (map export-event events)}}))

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
            (let [the-event (elastic/get-by-id id)
                  deleted (= (get the-event :updated) "deleted")
                  ; User can request to show anyway
                  include-deleted (= (get-in ctx [:request :params "include-deleted"]) "true")]
              [(and the-event (or include-deleted
                                  (not deleted))) {::event the-event}]))

  :handle-ok (fn [ctx]
              (when (:status-service env)
                (status/send! "query" "serve" "event" 1)
                (status/send! "query" "serve" "request" 1))
              {:status "ok"
               :message-type "event"
               :message {
                 :event (export-event (::event ctx))}})

  :handle-not-found (fn [ctx] {:status "not-found"}))

(def x (atom nil))

(defresource alternative-ids-check
  []
  :allowed-methods [:get]
  :available-media-types ["application/json"]
  :handle-ok (fn [ctx]
    (reset! x ctx)
               (let [ids (vec (.split
                                (get-in ctx [:request :params "ids"] "")
                                ","))
               
                    matches (elastic/alternative-ids-exist ids)]
                {:alternative-ids matches})))

(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
                (representation/ring-response
                  (ring-response/redirect event-data-homepage))))

(defroutes app-routes
  (GET "/" [] (home))
  (GET "/events" [] (events elastic/event-type-name))
  (GET "/events/distinct" [] (events elastic/latest-type-name))
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
    (when (:status-service env)
      (at-at/every 10000 #(status/send! "query" "heartbeat" "tick" 1) schedule-pool))

    (log/info "Start server on " port)
    (server/run-server app {:port port})))
