(ns event-data-query.work-cache
  "Work metadata cache, retrieved using Content Negotiation and stored in ElasticSearch."
  (:require [crossref.util.doi :as cr-doi]
            [crossref.util.string :as cr-str]
            [qbits.spandex :as s]
            [clj-http.client :as client]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [robert.bruce :refer [try-try-again]]
            [config.core :refer [env]])

  (:import [java.net URLEncoder]
           [org.elasticsearch.client ResponseException]))

(def work-type-name :work)

(def mapping
  {:doi {:type "keyword"}
   :ra {:type "keyword"}
   :content-type {:type "keyword"}})

(def index-id (delay (-> env :query-deployment (str "work_cache"))))

(def connection (delay
                 (s/client {:hosts [(:query-elastic-uri env)]
                            :max-retry-timeout 60000
                            :request {:connect-timeout 60000
                                      :socket-timeout 60000}})))

(defn doi->id
  [doi]
  (-> doi cr-doi/normalise-doi cr-str/md5))

(defn get-work
  "Get the given URL's metadata from the cache."
  [doi]
  (let [id (doi->id doi)]
    (try
      (->
        (s/request @connection
          {:url [@index-id work-type-name id]
           :method :get})
        :body
        :_source)
      ; Expect to get 404 sometimes.
      (catch Exception ex nil))))

(defn ensure-index
  "Set up Index."
  []
  (try
    (s/request @connection {:url [@index-id] :method :head})
    (catch Exception ex
      (log/info "Need to create Work index")
      (try
        (s/request @connection {:url [@index-id]
                                :method :put
                                :body {:settings {"number_of_shards" 8
                                                  "number_of_replicas" 2}
                                       :mappings {work-type-name {:properties mapping}}}})
        (catch Exception ex2
          (log/error "Failed to create Work index!" ex2))))))

(defn insert-work
  "Insert a work's metadata, replacing already exists."
  [doi data]
  (let [id (doi->id doi)]
    (try-try-again
     {:sleep 30000 :tries 5}
       #(s/request @connection 
                   {:url [@index-id work-type-name id]
                    :method :put
                    :body data}))))

(defn get-ra-api
  "Get the Registration Agency from the DOI RA API. 
   Return :crossref :datacite or nil."
  [non-url-normalized-doi]
  (when non-url-normalized-doi
    (try
      (-> non-url-normalized-doi
          (URLEncoder/encode "UTF-8")
          (#(str "https://doi.org/doiRA/" %))
          client/get
          :body
          (json/read-str :key-fn keyword)
          first
          :RA
          (or "")
          clojure.string/lower-case
          {"datacite" :datacite
           "crossref" :crossref})

      ; Not found, or invalid data, return nil.
      (catch Exception ex (do (log/error ex) nil)))))

(defn get-work-api
  "Get the work metadata from the Crossref or DataCite API."
  [non-url-normalized-doi]
  ; We might get nils.
  (when non-url-normalized-doi
    (try
      (let [ra (get-ra-api non-url-normalized-doi)
            safe-doi (URLEncoder/encode non-url-normalized-doi "UTF-8")
            url (condp = ra
                  :crossref (str "https://api.crossref.org/v1/works/" safe-doi "?mailto=eventdata@crossref.org")
                  :datacite (str "https://api.datacite.org/works/" safe-doi "?include=resource-type")
                  nil)

            response (try-try-again
                        {:sleep 10000 :tries 2}
                        ; Only retry on genuine exceptions. 404 etc won't be fixed by retrying.
                        #(when url (client/get url {:throw-exceptions false})))
            body (when (= 200 (:status response)) (-> response :body (json/read-str :key-fn keyword)))
            work-type (condp = ra
                        :crossref (-> body :message :type)
                        :datacite (-> body :data :attributes :resource_type_id)
                        nil)]
      ; If we couldn't discover the RA, then this isn't a real DOI. 
      ; Return nil so this doens't get cached (could produce a false-negative in future).

      (when (and ra work-type)
        {:content-type work-type :ra ra :doi non-url-normalized-doi}))
      (catch Exception ex
        (do
          (log/error "Failed to retrieve metadata for DOI" non-url-normalized-doi "error:" (str ex))
          nil)))))


(defn get-for-dois
  "For a sequence of DOIs, perform cached lookups and return in a hash-map.
   When an input isn't a valid DOI, the response is nil."
  [dois]
  ; Map of inputs to normalized DOI (or nil if it isn't valid).
  (let [inputs-normalized (map (fn [input]
                                   [input (when (cr-doi/well-formed input)
                                            (cr-doi/non-url-doi input))]) dois)
 
        ; Look up each entry into triple.
        from-cache (map (fn [[input-doi normalized-doi]]
                            [input-doi normalized-doi (when normalized-doi (get-work normalized-doi))])
                        inputs-normalized)
        missing-entries (filter (fn [[input-doi normalized-doi result]] (nil? result))
                                from-cache)

        ; Look up missing ones into triples.
        from-api (map (fn [[input-doi normalized-doi _]]
                          [input-doi normalized-doi (when normalized-doi (get-work-api normalized-doi))])
                      missing-entries)]
        (doseq [[_ normalized-doi result] from-api]
          ; Don't save in cache if it wasn't a DOI (or was nil).
          ; Don't save if we couldn't retrieve any data from the API (DOI-like doesn't exist).
          (when (and normalized-doi result)
            (insert-work normalized-doi result)))

        (merge (into {} (map (fn [[input-doi _ result]]
                                 [input-doi result]) from-cache))

               (into {} (map (fn [[input-doi _ result]]
                                 [input-doi result]) from-api)))))


