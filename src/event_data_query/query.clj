(ns event-data-query.query
  (:require [clj-time.core :as clj-time]
            [clj-time.coerce :as coerce]
            [clj-time.format :as clj-time-format]
            [crossref.util.doi :as cr-doi]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+]]))

(def ymd-format (clj-time-format/formatter "yyyy-MM-dd"))

(defn start-of [date-str]
  (let [parsed (clj-time-format/parse ymd-format date-str)]
    (clj-time/date-time (clj-time/year parsed) (clj-time/month parsed) (clj-time/day parsed))))

(defn end-of [date-str]
  (let [parsed (clj-time/plus (clj-time-format/parse ymd-format date-str) (clj-time/days 1))]
    (clj-time/date-time (clj-time/year parsed) (clj-time/month parsed) (clj-time/day parsed))))


(defn q-from-occurred-date
  [params]
  (when-let [date-str (:from-occurred-date params)]
    (try
      {:range {:occurred {:gte (coerce/to-long (start-of date-str))}}}
    (catch IllegalArgumentException _
      (throw+ {:type :validation-failure
               :subtype :invalid-date
               :message (str "Date format suplied to from-occurred-date incorrect. Expected YYYY-MM-DD, got: " date-str)})))))

(defn q-until-occurred-date
  [params]
  (when-let [date-str (:until-occurred-date params)]
    (try
      {:range {:occurred {:lt (coerce/to-long (end-of date-str))}}}
    (catch IllegalArgumentException _
      (throw+ {:type :validation-failure
               :subtype :invalid-date
               :message (str "Date format suplied to until-occurred-date incorrect. Expected YYYY-MM-DD, got: " date-str)})))))

(defn q-from-collected-date
  [params]
  (when-let [date-str (:from-collected-date params)]
    (try
      {:range {:timestamp {:gte (coerce/to-long (start-of date-str))}}}
    (catch IllegalArgumentException _
      (throw+ {:type :validation-failure
               :subtype :invalid-date
               :message (str "Date format suplied to from-collected-date incorrect. Expected YYYY-MM-DD, got: " date-str)})))))

(defn q-until-collected-date
  [params]
  (when-let [date-str (:until-collected-date params)] 
    (try
      {:range {:timestamp {:lt (coerce/to-long (end-of date-str))}}}
    (catch IllegalArgumentException _
      (throw+ {:type :validation-failure
               :subtype :invalid-date
               :message (str "Date format suplied to until-collected-date incorrect. Expected YYYY-MM-DD, got: " date-str)})))))

; The from-updated-date comes from a separate query parameter but is merged in the handler in server.
(defn q-from-updated-date
  [params]
  ; Don't serve up deleted content unless we're showing updates.
  (if-let [date-str (:from-updated-date params)]
    (try
      {:range {:updated-date {:gte (coerce/to-long (start-of date-str))}}}
      (catch IllegalArgumentException _
      (throw+ {:type :validation-failure
               :subtype :invalid-date
               :message (str "Date format suplied to from-updated-date incorrect. Expected YYYY-MM-DD, got: " date-str)})))
      {:bool {:must_not {:term {:updated "deleted"}}}}))
  


; subj_id and obj_id

(defn q-subj-id
  "Query for the subj-id. If it's a DOI, normalize."
  [params]
  (when-let [id (:subj-id params)]
    (if (cr-doi/well-formed id)
      (let [doi (cr-doi/normalise-doi id)]
        {:term {:subj-doi doi}})
        {:term {:subj-id id}})))

(defn q-obj-id
  "Query for the obj-id. If it's a DOI, normalize."
  [params]
  (when-let [id (:obj-id params)]
    (if (cr-doi/well-formed id)
      (let [doi (cr-doi/normalise-doi id)]
        {:term {:obj-doi doi}})
        {:term {:obj-id id}})))

(defn q-subj-id-prefix
  [params]
  (when-let [prefix (:subj-id.prefix params)]
    {:term {:subj-prefix prefix}}))

(defn q-obj-id-prefix
  [params]
  (when-let [prefix (:obj-id.prefix params)]
    {:term {:obj-prefix prefix}}))

(defn q-subj-id-domain
  [params]
  (when-let [domain (:subj-id.domain params)]
    {:term {:subj-id-domain domain}}))

(defn q-obj-id-domain
  [params]
  (when-let [domain (:obj-id.domain params)]
    {:term {:obj-id-domain domain}}))


; subj and obj metadata

(defn q-subj-url
  [params]
  (when-let [prefix (:subj.url params)]
    {:term {:subj-url prefix}}))

(defn q-obj-url
  [params]
  (when-let [prefix (:obj.url params)]
    {:term {:obj-url prefix}}))

(defn q-subj-url-domain
  [params]
  (when-let [prefix (:subj.url.domain params)]
    {:term {:subj-url-domain prefix}}))

(defn q-obj-url-domain
  [params]
  (when-let [prefix (:obj.url.domain params)]
    {:term {:obj-url-domain prefix}}))

(defn q-subj-alternative-id
  [params]
  (when-let [id (:subj.alternative-id params)]
    {:term {:subj-alternative-id id}}))

(defn q-obj-alternative-id
  [params]
  (when-let [id (:obj.alternative-id params)]
    {:term {:obj-alternative-id id}}))

; others.

(defn q-relation
  [params]
  (when-let [relation (:relation params)]
    {:term {:relation-type relation}}))

(defn q-source
  [params]
  (if-let [source (:source params)]
    {:term {:source source}}))

; Experimental must be false by default.
(defn q-experimental
  [params]
  (let [experimental (:experimental params)]
    (if (= "true" experimental)
      {:term {:experimental true}}
      {:term {:experimental false}})))

(def filters
  "Map of input parameter to function that parses it out."
  {:from-occurred-date q-from-occurred-date
   :until-occurred-date q-until-occurred-date
   :from-collected-date q-from-collected-date
   :until-collected-date q-until-collected-date
   :from-updated-date q-from-updated-date
   :subj-id q-subj-id
   :obj-id q-obj-id
   :subj-id.prefix q-subj-id-prefix
   :obj-id.prefix q-obj-id-prefix
   :subj-id.domain q-subj-id-domain
   :obj-id.domain q-obj-id-domain
   :subj.url q-subj-url
   :obj.url q-obj-url
   :subj.url.domain q-subj-url-domain
   :obj.url.domain q-obj-url-domain
   :subj.alternative-id q-subj-alternative-id
   :obj.alternative-id q-obj-alternative-id
   :relation-type q-relation
   :experimental q-experimental
   :source q-source}) 

(def query-process-fns
  (apply juxt (vals filters)))

(defn validate-filter-keys
  "Validate that all filter keys are recognised. Return nil or throw exception for first error."
  [params]
  (when-let [unrecognised (first (clojure.set/difference (set (keys params)) (set (keys filters))))]
    (throw+ {:type :validation-failure
             :subtype :filter-not-available
             :message (str "Filter " (name unrecognised) " specified but there is no such filter for this route. Valid filters for this route are: " (clojure.string/join ", " (map name (keys filters))))})))

(defn build-filter-query
  "Transform filter params dictionary into ElasticSearch query."
  [params]
  (let [result {:bool {:filter (remove nil? (query-process-fns params))}}]
    (log/info "Build filter query" params "->" result)
    result))
  

