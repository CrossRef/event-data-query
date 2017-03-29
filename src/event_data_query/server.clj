(ns event-data-query.server
 (:require  [event-data-query.common :as common]
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
            [clojure.tools.logging :as log]
            [config.core :refer [env]]
            [compojure.core :refer [defroutes GET]]
            [ring.middleware.params :as middleware-params]
            [ring.middleware.resource :as middleware-resource]
            [ring.middleware.content-type :as middleware-content-type]
            [liberator.core :refer [defresource]]
            [liberator.representation :as representation]
            [ring.util.response :as ring-response])


  (:import [org.bson.types ObjectId]
           [com.mongodb DB WriteConcern])
  (:gen-class))

(def db (delay (:db (mg/connect-via-uri (:mongodb-uri env)))))

(def event-data-homepage "https://www.crossref.org/services/event-data")

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
(def sourcelist (delay (get-sourcelist)))

; TODO merge this with some parts of execute-query
(defn build-query
  "Transform query parameters into a Mongo query. Throw IllegalArgumentException on error."
  [params]
  (let [from-occurred (when-let [date (:from-occurred-date params)] (common/start-of date))
        until-occurred (when-let [date (:until-occurred-date params)] (common/end-of date))

        from-collected (when-let [date (:from-collected-date params)] (common/start-of date))
        until-collected (when-let [date (:until-collected-date params)] (common/end-of date))

        work (when-let [doi (:work params)] (cr-doi/normalise-doi doi))

        prefix (:prefix params)
        source (:source params)
    
        ; Build a set of queries from the filter params, then merge them.
        from-occurred-q (when from-occurred {:_occurred-date {o/$gte from-occurred}})
        until-occurred-q (when until-occurred {:_occurred-date {o/$lt until-occurred}})

        from-collected-q (when from-collected {:_timestamp-date {o/$gte from-collected}})
        until-collected-q (when until-collected {:_timestamp-date {o/$lt until-collected}})

        work-q (when work {o/$or [{:_subj_doi work} {:_obj_doi work}]})

        prefix-q (when prefix {o/$or [{:_subj_prefix prefix} {:_obj_prefix prefix}]})
        source-q (when source {:source_id source})]

      (merge-with merge from-occurred-q until-occurred-q from-collected-q until-collected-q work-q prefix-q source-q)))

(defn execute-query
  "Execute a query against the database."
  [db query rows cursor ignore-whitelist? include-experimental? updated-since-date]
  (prn "query" query "rows" rows "cursor" cursor "ignore-whitelist?" ignore-whitelist? "include-experimental?" include-experimental? "updated-since-date" updated-since-date)

  (let [with-cursor {:_id {o/$gt (or cursor "")}}
        with-whitelist (when-not ignore-whitelist? {:source_id {o/$in @sourcelist}})
        ; experimental is not present or true
        with-experimental (when include-experimental? {:experimental nil})
        ; don't serve deleted events by default. If updated-since date is included, do include them in addition to the filter.
        with-updated-since-date (if updated-since-date {:_updated-date {o/$gte updated-since-date} :updated o/$exists}
                                                       {:updated {o/$ne "deleted"}})

        query-with-extras (merge-with merge query with-cursor with-whitelist with-experimental with-updated-since-date)

        cnt (mc/count db common/event-mongo-collection-name query-with-extras)

        results (q/with-collection db common/event-mongo-collection-name
                 (q/find query-with-extras)
                 (q/sort (array-map :id 1))
                 (q/limit rows))
        events (map #(apply dissoc % common/special-fields) results)


        next-cursor (-> results last :_id)]
        (prn query-with-extras)
    [events next-cursor cnt]))


(defn split-filter
  "Split a comma and colon separated filter into a map, or :error."
  [filter-str]
  (when filter-str
    (try
      (into {} (map (fn [pair] (let [[k v] (clojure.string/split pair #":")] [(keyword k) v])) (clojure.string/split filter-str #",")))
      (catch IllegalArgumentException e :error))))

(defn ymd-date-from-ctx
  "Try and parse a YYYY-MM-DD date from a named parameter. Return date, or nil, or :error"
  [value]
  ; Return nil if not present.
  (when value
    (or (common/try-parse-ymd-date value)
        :error)))

(defn try-parse-int
  [value]
  (when value
    (try (Integer/parseInt value)
      (catch IllegalArgumentException ex :error))))

(defresource events
  []
  :available-media-types ["application/json"]
  
  :malformed? (fn [ctx]
                (let [override-whitelist (= (get-in ctx [:request :params "whitelist"]) "false")
                      experimental (= (get-in ctx [:request :params "experimental"]) "true")
                      cursor (get-in ctx [:request :params "cursor"])
                      rows (or (try-parse-int (get-in ctx [:request :params "rows"])) common/default-page-size)

                      filters (split-filter (get-in ctx [:request :params "filter"]))
                      
                      ; update-since is a query parameter
                      updated-since-date (ymd-date-from-ctx (get-in ctx [:request :params "updated-since"]))

                      query (try (build-query filters) (catch IllegalArgumentException ex :error))]

                  [(:error (set [updated-since-date rows query]))
                   {::updated-since-date updated-since-date
                    ::filters filters
                    ::experimental experimental
                    ::override-whitelist override-whitelist
                    ::cursor cursor
                    ::rows rows
                    ::query query}]))

  :handle-ok (fn [ctx]
               (let [[events next-cursor total-results] (execute-query
                                                 @db
                                                 (::query ctx)
                                                 (::rows ctx)
                                                 (::cursor ctx)
                                                 (::override-whitelist ctx)
                                                 (::experimental ctx)
                                                 (::updated-since-date ctx))]
                {:status "ok"
                 :message-type "event-list"
                 :message {
                   :next-cursor next-cursor
                   :total-results total-results
                   :items-per-page (::rows ctx)
                   :events events}})))

(defresource home
  []
  :available-media-types ["text/html"]
  :handle-ok (fn [ctx]
                (representation/ring-response
                  (ring-response/redirect event-data-homepage))))

(defroutes app-routes
  (GET "/" [] (home))
  (GET "/events" [] (events)))

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

(defn run []
  (let [port (Integer/parseInt (:port env))]


    (log/info "Start server on " port)
    (server/run-server app {:port port})))
