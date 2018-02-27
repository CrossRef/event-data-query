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
  [doi]
  (try
    (-> doi
        cr-doi/non-url-doi
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
    (catch Exception ex (do (log/error ex) nil))))

(defn get-work-api
  "Get the work metadata from the Crossref or DataCite API."
  [doi]
  (let [ra (get-ra-api doi)
        safe-doi (URLEncoder/encode (cr-doi/non-url-doi doi) "UTF-8")
        url (condp = ra
              :crossref (str "https://api.crossref.org/v1/works/" safe-doi)
              :datacite (str "https://api.datacite.org/works/" safe-doi "?include=resource-type")
              nil)
        response (when url (client/get url))
        body (when response (-> response :body (json/read-str :key-fn keyword)))

        work-type (condp = ra
                    :crossref (-> body :message :type)
                    :datacite (->> body
                                  :included
                                  (filter #(= (:type %) "resource-types"))
                                  first
                                  :id)
                    nil)]

  {:content-type work-type :ra ra :doi doi}))


(defn get-for-dois
  "For a sequence of DOIs, perform cached lookups and return in a hash-map."
  [dois]
  (let [distinct-dois (set dois)
        ; doi->result
        from-cache (into {} (map (fn [doi]
                                      [doi (get-work doi)])
                                  distinct-dois))

        missing-dois (->> from-cache
                       (filter (fn [[k v]] (nil? v)))
                       (map first))

        from-api (into {} (map (fn [doi]
                                  [doi (get-work-api doi)]) missing-dois))]

        (doseq [[doi data] from-api]
          (insert-work doi data))

        (merge from-cache from-api)))


