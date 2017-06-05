(ns event-data-query.query
  (:require [monger.operators :as o]
            [event-data-query.common :as common]
            [crossref.util.doi :as cr-doi]))

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
  "Query for the work. If it's a DOI, normalize."
  [params]
  (when-let [work (:work params)]
    (if (cr-doi/well-formed work)
      (let [doi (cr-doi/normalise-doi work)]
        {o/$or [{:_subj_doi doi}
                {:_obj_doi doi}]})

      {o/$or [{:_subj_id work}
              {:_obj_id work}]})))

(defn q-relation
  [params]
  (when-let [relation (:relation params)]
    {:relation_type_id relation}))

(defn q-prefix
  [params]
  (when-let [prefix (:prefix params)]
    {o/$or [{:_subj_prefix prefix} {:_obj_prefix prefix}]}))

(defn q-source
  [params]
  (if-let [source (:source params)]
    {:source_id source}))

(defn q-alternative-id
  [params]
  (when-let [id (:alternative-id params)]
    {o/$or [{:subj.alternative-id id}
            {:obj.alternative-id id}]}))

(defn q-subj-url
  [params]
  (when-let [id (:subj.url params)]
    {:subj.url id}))

(defn q-obj-url
  [params]
  (when-let [id (:obj.url params)]
    {:obj.url id}))

(def query-processors
  (juxt q-from-occurred-date
        q-until-occurred-date
        q-from-collected-date
        q-until-collected-date
        q-work
        q-prefix
        q-source
        q-alternative-id
        q-relation
        q-subj-url
        q-obj-url))

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
    {o/$and [{:_updated_date {o/$gte (common/start-of date-str)}}
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
