(ns event-data-query.query
  (:require [monger.operators :as o]
            [event-data-query.common :as common]
            [crossref.util.doi :as cr-doi]))

(def whitelist-override
  "Ignore the whitelist?"
  (atom nil))

; Loaded at startup. The list changes so infrequently that the server can be restarted when a new one is added.
(def sourcelist
  "Set of whitelisted source ids"
  (atom nil))

(defn and-queries
  "Return a Mongo query object that's the result of anding all params. Nils allowed."
  [& terms]
  (let [terms (remove nil? terms)]
    (if (empty? terms)
      {}
      {o/$and terms})))

(defn q-from-occurred-date
  [params]
  (when-let [date (:from-occurred-date params)]
    {:_occurred-date {o/$gte (common/start-of date)}}))

(defn q-until-occurred-date
  [params]
  (when-let [date (:until-occurred-date params)]
    {:_occurred-date {o/$lt (common/end-of date)}}))

(defn q-from-collected-date
  [params]
  (when-let [date (:from-collected-date params)]
    {:_timestamp-date {o/$gte (common/start-of date)}}))

(defn q-until-collected-date
  [params]
  (when-let [date (:until-collected-date params)] 
    {:_timestamp-date {o/$lt (common/end-of date)}}))

(defn q-work
  [params]
  (when-let [doi (:work params)]
    {o/$or [{:_subj_doi (cr-doi/normalise-doi doi)}
            {:_obj_doi (cr-doi/normalise-doi doi)}]}))

(defn q-prefix
  [params]
  (when-let [prefix (:prefix params)]
    {o/$or [{:_subj_prefix prefix} {:_obj_prefix prefix}]}))

(defn q-source
  [params]
  (if-let [source (:source params)]
     ; If source provided, use that, subject to whitelist.
    (if @whitelist-override
      {:source_id source}
      {:source_id (@sourcelist source)}) 
    ; Otherwise if no source provided, return all that match the whitelist unless overriden.
    (if @whitelist-override
       nil
       {:source_id {o/$in (vec @sourcelist)}})))

(def query-processors
  (juxt q-from-occurred-date
        q-until-occurred-date
        q-from-collected-date
        q-until-collected-date
        q-work
        q-prefix
        q-source))

(defn build-filter-query
  "Transform filter params dictionary into mongo query."
  [params]
  (apply and-queries (query-processors params)))

; Build meta-query fragments from query parameter dictionary. 
; Throw exceptions.
(defn mq-cursor
  [params]
  (when-let [value (params "cursor")]
    {:_id {o/$gt value}}))

(defn mq-experimental
  [params]
  (if (= (params "experimental") "true")
    {}
    {:experimental nil}))

(defn mq-updated-since-date
  [params]
  (if-let [date-str (params "from-updated-date")]
    {o/$and [{:_updated-date {o/$gte (common/start-of date-str)}}
             {:updated {o/$exists true}}]}
    {:updated {o/$ne "deleted"}}))

(def meta-query-processors
  (juxt mq-cursor
        mq-experimental
        mq-updated-since-date))

(defn build-meta-query
  "Transform query params dictionary into mongo query."
  [params]
  (apply and-queries (meta-query-processors params)))
