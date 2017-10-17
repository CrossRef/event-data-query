(ns event-data-query.elastic
   (:require [crossref.util.doi :as cr-doi]
             [crossref.util.string :as cr-str]
             [qbits.spandex.utils :as s-utils]
             [qbits.spandex :as s]
             [clj-time.coerce :as coerce]
             [clj-time.format :as clj-time-format]
             [clojure.tools.logging :as log]
             [robert.bruce :refer [try-try-again]]
             [config.core :refer [env]])
   (:import [java.net URL MalformedURLException]
            [org.elasticsearch.client ResponseException]))

(def index-name "event-data-query")

; Index of all Events
(def event-type-name "event")

; Index of distinct latest version of Event for each subj_id, obj_id
(def distinct-type-name "latest")

(def full-format-no-ms (:date-time-no-ms clj-time-format/formatters))
(def full-format (:date-time clj-time-format/formatters))
        
(defn parse-date   
  "Parse two kinds of dates."
  [date-str]
  (try
    (clj-time-format/parse full-format-no-ms date-str)
    (catch IllegalArgumentException e   
       (clj-time-format/parse full-format date-str))))


(def mapping-properties
  {; we also have an unindexed 'event' field which is used contain the original Event verbatim.
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
   :updated {:type "keyword"}})
 
(def mappings
  {event-type-name
   {:dynamic false
    :properties mapping-properties}
  distinct-type-name
   {:dynamic false
    :properties mapping-properties}})

(def event-mappings
  {event-type-name
   {:dynamic false
    :properties mapping-properties}})

(def distinct-mappings
  {distinct-type-name
   {:dynamic false
    :properties mapping-properties}})

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
(defn update-mappings
  []
  "Update mappings in-place."
  (s/request @connection {:url (str index-name "/" event-type-name "/_mapping")
                          :method :post
                          :body event-mappings})
  (s/request @connection {:url (str index-name "/" distinct-type-name "/_mapping")
                          :method :post
                          :body distinct-mappings}))
(defn close! []
  (s/close! @connection))

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

        obj-id-url (try (new URL (get event "obj_id"))
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

     ; both :timestamp and :updated-date are used for the 'distinct' type.
     :timestamp (coerce/to-long (parse-date (get event "timestamp")))
     :updated-date (when-let [date (get event "updated_date")] (coerce/to-long (parse-date date)))
     :updated (event "updated")}))

(defn id-for-event-distinct
  [transformed-event]
  ; Currently need to bodge wikipedia.
  (cr-str/md5
    (str
      (condp = (:source transformed-event)
        "wikipedia" (:subj-url transformed-event)
        (:subj-id transformed-event))
      "~"
      (:obj-id transformed-event))))

(defn insert-events
  [events]
  "Insert a batch of Events with string keys."
  (when-not (empty? events)
    (let [transformed (map transform-for-index events)
          event-chunks (s/chunks->body (mapcat (fn [event]
                          [{:index {:_index index-name
                                     :_type event-type-name
                                     :_id (:id event)}}
                           event]) transformed))
          
          distinct-chunks (s/chunks->body (mapcat (fn [event]
                          [{:index {:_index index-name
                                    :_type distinct-type-name
                                    ; Use the most recent date for which there was any activity.
                                    :_version (or (:updated-date event) (:timestamp event))
                                    :_version_type "external"
                                    :_id (id-for-event-distinct event)}}
                            event]) transformed))]
      
      (s/request @connection {:url (str index-name "/" event-type-name "/_bulk")
                              :method :post
                              :body event-chunks})

      ; The result for conflicts will be 40x, but we can safely ignore this.
      ; If we inserted an older version of an Event, we deliebrately want it to be igonred.
      (try-try-again
        {:sleep 30000 :tries 5}
        #(let [result (s/request
                         @connection {:url (str index-name "/" distinct-type-name "/_bulk")
                         :method :post
                         :body distinct-chunks})
              items (-> result :body :items)
              problem-items (remove (fn [item]
                                (-> item :index :status #{409 201 200}))
                              items)]
          
          ; If there is an HTTP exception, this will be handled by try-try-again and then an exception will be thrown.
          ; If there is an error within the request (i.e. an individual Event document), re-trying won't help
          ; so just report and keep going.
          (when (not-empty problem-items)
            (log/error "Unexpected response items" problem-items)))))))

(defn value-sorted-map
  [input]
  (into (sorted-map-by (fn [a b]
                     (compare [(get input b) b]
                              [(get input a) a]))) input))

(defn parse-aggregation-response
  "Parse the :aggregations part of an ElasticSearch response."
  [result]
  (into {}
    (map
      (fn [[nom info]]
        [nom {:value-count (-> info :buckets count)
              ; Sort values as they go into the hash-map.
              :values (value-sorted-map (into {} (map (juxt :key :doc_count) (:buckets info))))}])
      result)))

(defn search-query
  "Issue search. Return ElasticSearch results, both as indexed documents and scoped into Events."
  [query facet-query type-name page-size sort-criteria search-after-criteria]
  (let [body {:size page-size
              :sort sort-criteria
              :query query
              :aggregations (or facet-query {})
              :search_after search-after-criteria}]
    (try
      (let [result (s/request
                     @connection
                     {:url (str index-name "/" type-name "/_search")
                      :method :post
                      :body body})
            
            ; Return both the documents as indexed and the source Events, for ease of use.
            hits (->> result :body :hits :hits)
            events (map (comp :event :_source) hits)

            facet-results (when facet-query (-> result :body :aggregations parse-aggregation-response))]

        [events hits facet-results])
    
    (catch Exception e
      (log/error "Exception from ElasticSearch")
      (log/error "Sent:" body)
      (log/error "Exception:" e)
      ; Rethrow so Liberator returns a 500.
      (throw (new Exception "ElasticSearch error"))))))

(defn count-query
  "Get list of original Events by the query."
  [query type-name]
  (let [body {:query query}
        result (s/request @connection {:url (str index-name "/" type-name "/_count")
                                       :body body})]
    (-> result :body :count)))


(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))

(defn parse-time-aggregation-response
  [response]

  (let [rows (-> response :time-hist :buckets)
        pairs (map #(vector
                      (clj-time-format/unparse
                        ymd-format
                        (coerce/from-long (Long/parseLong (:key_as_string %))))
                      
                      (:doc_count %)) rows)]
  pairs))

(defn time-facet-query
  "Time-facet the query by all three date fields."
  [query type-name field interval]
  {:pre [(#{:timestamp :occurred :updated-date} field)
         (#{:day :week :month :year} interval)]}

  (let [body {:size 0
              :query query
              :aggregations {"time-hist" {"date_histogram" {:field field :interval interval}}}}]

    (try
      (let [result (s/request
                     @connection
                     {:url (str index-name "/" type-name "/_search")
                      :method :post
                      :body body})
            
            facet-results (-> result :body :aggregations parse-time-aggregation-response)]

        facet-results)
    
    (catch Exception e
      (log/error "Exception from ElasticSearch")
      (log/error "Sent:" body)
      (log/error "Exception:" e)
      ; Rethrow so Liberator returns a 500.
      (throw (new Exception "ElasticSearch error"))))))


(defn get-by-id
  "Get original Event by ID."
  [id type-name]
  (try
    (let [result (s/request @connection
                            {:url (str index-name "/" type-name "/" id)
                             :method :get})]
      (->> result :body :_source :event))
    (catch Exception ex
      nil)))

(defn get-by-id-full
  "Get full Elastic entry by ID."
  [id type-name]
  (try
    (let [result (s/request @connection
                            {:url (str index-name "/" type-name "/" id)
                             :method :get})]
      (->> result :body :_source))
    (catch Exception ex
      nil)))

(defn alternative-ids-exist
  "From a list of alternative IDs, find the ones that intersect with Subject alternative IDs."
  [alternative-ids type-name]
  (let [query {:query {:bool {:filter {:terms {:subj-alternative-id alternative-ids}}}}}
        result (s/request @connection
                          {:url (str index-name "/" type-name "/_search")
                           :body query})]
    (map #(-> % :_source :subj-alternative-id) (-> result :body :hits :hits))))
