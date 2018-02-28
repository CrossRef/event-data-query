(ns event-data-query.elastic
  (:require [event-data-query.work-cache :as work-cache]
            [crossref.util.doi :as cr-doi]
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

; Index of all Events
(def event-type-name "event")

(def sources-lookup-ra-metadata
  "Look up the RA metadata only for these sources.
   These are only done because of Scholix, and are quite expensive, so are narrowly scoped."
  #{"crossref" "datacite"})

(def full-format-no-ms (:date-time-no-ms clj-time-format/formatters))
(def full-format (:date-time clj-time-format/formatters))

(defn parse-date
  "Parse two kinds of dates."
  [date-str]
  (try
    (clj-time-format/parse full-format-no-ms date-str)
    (catch IllegalArgumentException e
      (clj-time-format/parse full-format date-str))))

(def base-mappings
  "The common mapping for Documents. Each index is a slightly different varation."
  {; we also have an unindexed 'event' field which is used contain the original Event verbatim.
   :event {:type "object" :enabled false}
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
   :relation-type {:type "keyword"}
   :updated-date {:type "date" :format "epoch_millis"}
   :updated {:type "keyword"}
   :subj-ra {:type "keyword"}
   :obj-ra {:type "keyword"}
   :subj-content-type {:type "keyword"}
   :obj-content-type {:type "keyword"}})

(def index-configs
  "Configuration for each index.
   Each index covers a particular use case, and the `document->action` function defines whether a given Event input should result in a creation, update or deletion for that Event in the index.
   For example, a deleted Event should be deleted from the 'standard' index, but inserted into the 'deleted' index.
   document->action should return one of :index, to include, :delete, to delete, or nil to ignore."
   {
    
   ;; 'Standard' index.
   ;; Index of all Events that haven't been deleted. Doesn't support update-related queries.
   :standard
   {:name (-> env :query-deployment (str "standard"))
    
    ; Don't index on 'update' fields. A different index is used for that.
    :mappings (dissoc base-mappings :updated-date :updated)

    ; Insert everything (except experimental) that hasn't been deleted.
    ; Delete everything that's updated with the 'delete' type.
    :document->action #(cond
                         (= (:updated %) "deleted") :delete
                         (get-in % [:event "experimental"]) nil
                         :default :index)

    ; Document ID should be Event ID.
    :document->id #(get-in % [:event "id"])}
   
   ;; 'Distinct' index.
   ;; Index of the latest Event per distinct subject-relation-object triple.
   ;; Doesn't support updated-related queries.
   :distinct
   {:name (-> env :query-deployment (str "distinct"))
    
    ; Don't index on 'update' fields. This index is a quick-and-dirty view where Event IDs appear and disappear.
    ; Allowing filtering for updated Events would give false assurance.
    :mappings (dissoc base-mappings :updated-date :updated)
    
    ; Insert everything (except experimental) that hasn't been deleted.
    ; Delete everything that's updated with the 'delete' type.
    :document->action #(cond
                         (= (:updated %) "deleted") :delete
                         (get-in % [:event "experimental"]) nil
                         :default :index)

    ; Document ID should be defined by the subj-obj-pair, so we only store one document per pair.
    :document->id (fn [document] (cr-str/md5
                      (str (:subj-id document) "~" (:obj-id document))))}

   ;; 'Edited' index
   ;; Only those Events that have been Edited.
   :edited
   {:name (-> env :query-deployment (str "edited"))
    
    ; Index on update-date, that's the purpose of the index.
    :mappings base-mappings

    ; Insert only events that have been edited. 
    ; Remove if they've subsequently been deleted.
    ; Otherwise ignore.
    :document->action #(cond
                         (get-in % [:event "experimental"]) nil
                         (= (:updated %) "edited") :index
                         (= (:updated %) "deleted") :delete
                         :default nil)

    ; Document ID should be Event ID.
    :document->id #(get-in % [:event "id"])}

   ;; 'Deleted' index
   ;; Only those Events that have been deleted.
   :deleted
   {:name (-> env :query-deployment (str "deleted"))
    
    ; Index on updated date, that's the purpose of the index.
    :mappings base-mappings

    ; Insert only Events that have been deleted.
    ; If by some chance an Event is edited to be un-deleted, the correct semantics must be
    ; to remove from this index.
    :document->action #(condp = (:updated %)
                      "deleted" :index
                      :delete)

    ; Document ID should be Event ID.
    :document->id #(get-in % [:event "id"])}

    ;; 'Experimental' index.
    ;; All experimental events, non-production events.
   :experimental
   {:name (-> env :query-deployment (str "experimental"))
    :mappings (dissoc base-mappings :updated-date :updated)

    :document->action #(if (get-in % [:event "experimental"])
                      :index
                      nil)
    
    ; Document ID should be Event ID.
    :document->id #(get-in % [:event "id"])}

    ;; 'Scholix' index.
    ;; All Events from the 'crossref' and 'datacite' sources.
   :scholix
   {:name (-> env :query-deployment (str "scholix"))
    :mappings (dissoc base-mappings :updated-date :updated)

    :document->action #(cond 
                         ; Ignore if not a recognised source.
                         (not (#{"crossref" "datacite"} (:source %))) nil
                         
                         ; Of the relevant sources, delete if it's been deleted.
                         (= (:updated %) "deleted") :delete
                        
                         ; Index everything else.
                         :default :index)

    ; Document ID should be Event ID.
    :document->id #(get-in % [:event "id"])}})


(def search-url
  "Look up the _search URL for a given index-id."
  (memoize #(-> % index-configs :name (str "/_search"))))

(def count-url
  "Look up the _count URL for a given index-id."
  (memoize #(-> % index-configs :name (str "/_count"))))

(def connection (delay
                 (s/client {:hosts [(:query-elastic-uri env)]
                            :max-retry-timeout 60000
                            :request {:connect-timeout 60000
                                      :socket-timeout 60000}})))

(defn set-refresh-interval!
  "Set the refresh_interval for all indexes to the given value.
   Used before and after a big index insertion.
   Back-of-the-envelope, on a single local instance, this saves a couple of minutes per 200,000 Events."
  [value]
  (doseq [index-config (vals index-configs)]
    (s/request @connection {:url (str (:name index-config) "/_settings")
                            :method :put
                            :body {:index {"refresh_interval" value}}})))

(defn ensure-index
  "Set up a given Index. This should be run first."
  [index-config]
  (try
    (s/request @connection {:url (:name index-config) :method :head})
    (catch Exception ex
      (log/info "Need to create index" (:name index-config))
      (try
        (s/request @connection {:url (:name index-config)
                                :method :put
                                :body {:settings {;"index.mapping.depth.limit" 1
                                                  "number_of_shards" 8
                                                  "number_of_replicas" 1}
                                       :mappings {event-type-name {:properties (:mappings index-config)}}}})
        (catch Exception ex2
          (log/error "Failed to create index!" ex2))))))

(defn ensure-indexes
  []
  (log/info "Ensuring indexes...")
  (doseq [index-config (vals index-configs)]
    (log/info "Ensuring index" (:name index-config))
    (ensure-index index-config))
  (log/info "Finished ensuring indexes."))

(defn update-mapping
  "Update mappings in-place."
  [index-config]
  (s/request @connection {:url (str (:name index-config) "/" event-type-name "/_mapping")
                          :method :post
                          :body {event-type-name {:properties (:mappings index-config)}}}))

(defn update-mappings
  []
  (log/info "Start updating mappings...")
  (doseq [index-config (vals index-configs)]
    (log/info "Ensure mapping for" (:name index-config))
    (try
      (update-mapping index-config)
      (catch Exception ex (log/error "Failed to update mappings for" (:name index-config) ":" ex))))
  (log/info "Finished updating mappings."))

(defn close! []
  (s/close! @connection))

(defn event->document
  "Transform an Event with string keys into an Elastic document.
   Don't include the :id field, as it depends on the index."
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
                       (catch MalformedURLException _ nil)))

        source-id (event "source_id")

        should-lookup-ra (sources-lookup-ra-metadata source-id)

        ; Hashmap of {doi {:ra _ :content-type _}} when applicable.
        ra-info (when should-lookup-ra
                      (work-cache/get-for-dois [subj-doi obj-doi]))]

    {; an event wrapped up in an event. Allow us to retrieve it later.
     :event event
     ; The :id field depends on the index configuration. See :document->id functions.
     :subj-alternative-id (get-in event ["subj" "alternative-id"])
     :relation-type (event "relation_type_id")
     :obj-alternative-id (get-in event ["obj" "alternative-id"])
     :obj-doi (when obj-doi
                (cr-doi/normalise-doi obj-doi))
     ; if it's a DOI then normalize, otherwise pass through
     :obj-id (if obj-doi
               (cr-doi/normalise-doi obj-doi)
               (event "obj_id"))

     ; RA info. These may or may not be present, so will be nil when not applicable.
     :subj-ra (get-in ra-info [subj-doi :ra])
     :obj-ra (get-in ra-info [obj-doi :ra])
     :subj-content-type (get-in ra-info [subj-doi :content-type])
     :obj-content-type (get-in ra-info [obj-doi :content-type])

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
     :source source-id

     ; both :timestamp and :updated-date are used for the 'distinct' type.
     :timestamp (coerce/to-long (parse-date (get event "timestamp")))
     :updated-date (when-let [date (get event "updated_date")] (coerce/to-long (parse-date date)))
     :updated (event "updated")}))

(defn document->batch-actions
  "From a document create a sequence of Batch API insert actions.
   These will include actions for all indexes."
  [document]
  (apply
    concat
    (for [index-config (vals index-configs)]
      (let [; Becuase each index can formulate its :id differently,
            ; calculate it and insert into the document now.
            id ((:document->id index-config) document)
            document (assoc document :id id)
            action ((:document->action index-config) document)
            
            index-name (:name index-config)]

        ; We're returning a sequence of what will be newline-delimited JSON (NDJSON) for consumption
        ; by the ElasticSearch bulk API. The number of lines for each document-index combination depends
        ; on the action.
        (condp = action
          
          :index
          ; Index instruction should be followed by the document. 
          [{:index {:_index index-name
                    :_type event-type-name
                    :_id id}}
            document]

          :delete
          ; Delete instruction is not followed by a document.
          [{:delete {:_index index-name
                 :_type event-type-name
                 :_id id}}]

          ; Anything else, pass.
          [])))))

(def acceptable-index-status-codes
  "We expect a variety of status codes back from an ElasticSearch 'index' action.
   Any others should be treated as errors."
  #{; Conflict. Ok, as we have an update strategy.
    409 
    ; Created.
    201 
    ; Ok.
    200 })

(def acceptable-delete-status-codes
  "We expect a variety of status codes back from ElasticSearch 'delete' action.
  Any others should be treated as errors."
  #{; Conflict. Ok, as we have an update strategy.
    409 
    ; Ok.
    200 
    ; Tried to delete non-existent. That's OK.
    404 })

(defn insert-events
  "Insert a batch of Events with string keys."
  ([events] (insert-events events false))
  ([events force?]
   (when-not (empty? events)
     (let [documents (map event->document events)
           
           ; For this set of documents create the appropriate actions 
           batch-actions (mapcat document->batch-actions documents)]

      ; The result for conflicts will be 40x, but we can safely ignore this.
      ; If we inserted an older version of an Event, we deliebrately want it to be igonred.
       (try-try-again
        {:sleep 30000 :tries 5}
        (fn []
          (let [result (s/request
                        @connection {:url "_bulk"
                                     :method :post
                                     :body (s/chunks->body batch-actions)})
                items (-> result :body :items)

                problem-items (remove (fn [item]
                                        (or
                                          (-> item :index :status acceptable-index-status-codes)
                                          (-> item :delete :status acceptable-delete-status-codes)))
                                      items)]

            ; If there is an HTTP exception, this will be handled by try-try-again and then an exception will be thrown.
            ; If there is an error within the request (i.e. an individual Event document), re-trying won't help
            ; so just report and keep going.
            (when (not-empty problem-items)
              (log/error "Unexpected response items" problem-items)))))))))

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
  [index-id query facet-query page-size sort-criteria search-after-criteria]
  (let [body {:size page-size
              :sort sort-criteria
              :query query
              :aggregations (or facet-query {})
              :search_after search-after-criteria}]
    (try
      (let [result (s/request
                    @connection
                    {:url (search-url index-id)
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
  [index-id query]
  (let [body {:query query}
        result (s/request @connection {:url (count-url index-id)
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
  [index-id query field interval]
  {:pre [(#{:timestamp :occurred} field)
         (#{:day :week :month :year} interval)]}

  (let [body {:size 0
              :query query
              :aggregations {"time-hist" {"date_histogram" {:field field :interval interval}}}}]

    (try
      (let [result (s/request
                    @connection
                    {:url (search-url index-id)
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
  "Get original Event by ID from the given index."
  [index-id id]
  (try
    (let [result (s/request @connection
                            {:url (-> index-id index-configs :name (str "/" event-type-name "/" id))
                             :method :get})]
      (->> result :body :_source :event))
    (catch Exception ex
      nil)))
