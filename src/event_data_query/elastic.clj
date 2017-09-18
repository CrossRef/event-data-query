(ns event-data-query.elastic
   (:require [crossref.util.doi :as cr-doi]
             [qbits.spandex.utils :as s-utils]
             [qbits.spandex :as s]
             [clj-time.coerce :as coerce]
             [clj-time.format :as clj-time-format]
             [clojure.tools.logging :as log]
             [config.core :refer [env]])
   (:import [java.net URL MalformedURLException]
            [org.elasticsearch.client ResponseException]))

(def index-name "event-data-query")
(def type-name "event")

(def full-format-no-ms (:date-time-no-ms clj-time-format/formatters))
(def full-format (:date-time clj-time-format/formatters))
        
(defn parse-date   
  "Parse two kinds of dates."
  [date-str]
  (try
    (clj-time-format/parse full-format-no-ms date-str)
    (catch IllegalArgumentException e   
       (clj-time-format/parse full-format date-str))))


(def mappings
  {type-name {
    ; We explicitly project and create all required indexes.
    :dynamic false
    :properties {
      ; we also have an unindexed 'event' field which is used contain the original Event verbatim.
      :id {:type "keyword"}
      :obj-doi {:type "keyword"}
      :obj-id {:type "keyword"}
      :obj-alternative-id {:type "keyword"}
      :subj-alternative-id {:type "keyword"}
      :obj-prefix {:type "keyword"}
      :obj-url {:type "keyword"}
      :obj-id-domain  {:type "keyword"}
      :obj-url-domain {:type "keyword"}
      :occurred {:type "date" :format "epoch_millis"} 
      :subj-doi {:type "keyword"}
      :subj-id {:type "keyword"}
      :subj-id-domain {:type "keyword"}
      :subj-prefix {:type "keyword"}
      :subj-url {:type "keyword"}
      :subj-url-domain {:type "keyword"}
      :timestamp {:type "date" :format "epoch_millis"}
      :source {:type "keyword"}
      :experimental {:type "boolean"}
      :relation-type {:type "keyword"}
      :updated-date {:type "date" :format "epoch_millis"}
      :updated {:type "keyword"}}}})

(def connection (delay
  (s/client {:hosts [(:query-elastic-uri env)]})))

(defn delete-index
  "Delete the index."
  []
  (s/request @connection {:url index-name :method :delete}))

(defn ensure-index
  "Set up Indexes. This should be run first."
  []
  (try
    (log/info "Ensuring index" index-name)
    (s/request @connection {:url index-name :method :head})
    (catch Exception ex
      (log/info "Need to create index" index-name)
      (s/request @connection {:url index-name
                              :method :put
                              :body {:settings {"index.mapping.depth.limit" 1
                                                "index.mapper.dynamic" false
                                                "number_of_shards" 8
                                                "number_of_replicas" 2}
                                                :mappings mappings}}))))

(defn close! []
  (s/close! @connection))

(defn insert-prepared-event
  "Insert Event with string keys in index-ready format."
  [event]
  (when-not event (throw (new IllegalArgumentException "No Event supplied")))
  (let [id (:id event)]
    (when-not id (throw (new IllegalArgumentException "No ID supplied")))
    (s/request @connection {:url (str index-name "/" type-name "/" id)
                            :method :post
                            :body event})))

(defn transform-for-index
  "Transform an Event with string keys into an Elastic document."
  [event] 
  (when-not (event "subj_id") (throw (new IllegalArgumentException "Missing subj_id")))
  (when-not (event "obj_id") (throw (new IllegalArgumentException "Missing obj_id")))
  ; subj_id and obj_id may or may not be DOIs.
  (let [; view them as DOIs, with prefixes
        subj-doi (when-let [pid (get event "subj_id")]
                   (when (cr-doi/well-formed pid) pid))
        
        obj-doi (when-let [pid (get event "obj_id")]
                  (when (cr-doi/well-formed pid) pid))

        subj-doi-prefix (when subj-doi (cr-doi/get-prefix subj-doi))
        obj-doi-prefix (when obj-doi (cr-doi/get-prefix obj-doi))
        
        ; view them as URLs (possibly malformed)
        subj-id-url (try (new URL (get event "subj_id"))
                     (catch MalformedURLException _ nil))

        obj-id-url (try (new URL (get event "subj_id"))
                     (catch MalformedURLException _ nil))

        ; subj.url and obj.url are optional and may be malformed
        subj-url (when-let [url-str (get-in event ["subj" "url"])]
                   (try (new URL url-str)
                     (catch MalformedURLException _ nil)))

        obj-url (when-let [url-str (get-in event ["obj" "url"])]
                   (try (new URL url-str)
                     (catch MalformedURLException _ nil)))]

    {; an event wrapped up in an event. Allow us to retrieve it later.
     :event event
     :id (event "id")
     :subj-alternative-id (get-in event ["subj" "alternative-id"])
     :relation-type (event "relation_type_id")
     :obj-alternative-id (get-in event ["obj" "alternative-id"])
     :obj-doi (when obj-doi
                (cr-doi/normalise-doi obj-doi))
     ; if it's a DOI then normalize, otherwise pass through
     :obj-id (if obj-doi
               (cr-doi/normalise-doi obj-doi)
               (event "obj_id"))
     :obj-id-domain (when obj-id-url (.getHost obj-id-url))
     :obj-prefix (when obj-doi (cr-doi/get-prefix obj-doi))
     :obj-url (str obj-url)
     :obj-url-domain (when obj-url (.getHost obj-url))
     :occurred (coerce/to-long (parse-date (get event "occurred_at")))
     :subj-doi (when subj-doi
                 (cr-doi/normalise-doi subj-doi))
     :subj-id (if subj-doi
               (cr-doi/normalise-doi subj-doi)
               (event "subj_id"))
     :subj-id-domain (when subj-id-url (.getHost subj-id-url))
     :subj-prefix (when subj-doi (cr-doi/get-prefix subj-doi))
     :subj-url (str subj-url)
     :subj-url-domain (when subj-url (.getHost subj-url))
     :source (event "source_id")
     ; Any value in this field means true, default to false.
     :experimental (if (event "experimental") true false)
     :timestamp (coerce/to-long (parse-date (get event "timestamp")))
     :updated-date (when-let [date (get event "updated_date")] (coerce/to-long (parse-date date)))
     :updated (event "updated")}))

(defn insert-event
  "Insert Event with string keys."
  [event]
  (when-not event (throw (new IllegalArgumentException "No Event supplied")))
  (insert-prepared-event (transform-for-index event)))

(defn insert-events
  [events]
  "Insert a batch of Events with string keys."
  (when-not (empty? events)
    (let [transformed (map transform-for-index events)
          chunks (s/chunks->body (mapcat (fn [event]
                           [{:index {:_index index-name
                                    :_type type-name
                                    :_id (:id event)}}
                            event]) transformed))]
      (s/request @connection {:url (str index-name "/" type-name "/_bulk")
                              :method :post
                              :body  chunks}))))

(defn search-query
  [query page-size search-after-timestamp search-after-id]
  (let [search-after-timestamp (or search-after-timestamp 0)
        search-after-id (or search-after-id "")
        body {:size page-size
               :sort [{:timestamp "asc"} {:_uid "desc"}]
               :query query
               :search_after [search-after-timestamp search-after-id]}
        result (s/request @connection
                          {:url (str index-name "/" type-name "/_search")
                           :method :post
                           :body body})
        events (->> result :body :hits :hits (map (comp :event :_source)))]
    events))

(defn count-query
  "Get list of original Events by the query."
  [query]
  (let [body {:query query}
        result (s/request @connection {:url (str index-name "/" type-name "/_count")
                                       :body body})]
    (-> result :body :count)))

(defn get-by-id
  "Get original Event by ID."
  [id]
  (try
    (let [result (s/request @connection
                            {:url (str index-name "/" type-name "/" id)
                             :method :get})]
      (->> result :body :_source :event))
    (catch Exception ex
      nil)))

(defn get-by-id-full
  "Get full Elastic entry by ID."
  [id]
  (try
    (let [result (s/request @connection
                            {:url (str index-name "/" type-name "/" id)
                             :method :get})]
      (->> result :body :_source))
    (catch Exception ex
      nil)))

(defn alternative-ids-exist
  "From a list of alternative IDs, find the ones that intersect with Subject alternative IDs."
  [alternative-ids]
  (let [query {:query {:bool {:filter {:terms {:subj-alternative-id alternative-ids}}}}}
        result (s/request @connection
                          {:url (str index-name "/" type-name "/_search")
                           :body query})]
    (map #(-> % :_source :subj-alternative-id) (-> result :body :hits :hits))))
