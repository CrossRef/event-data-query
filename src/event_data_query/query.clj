(ns event-data-query.query
  (:require [event-data-query.common :as common]
            [clj-time.coerce :as coerce]
            [crossref.util.doi :as cr-doi]))

(defn q-from-occurred-date
  [params]
  (when-let [date (:from-occurred-date params)]
    {:range {:occurred {:gte (coerce/to-long (common/start-of date))}}}))

(defn q-until-occurred-date
  [params]
  (when-let [date (:until-occurred-date params)]
    {:range {:occurred {:lt (coerce/to-long (common/end-of date))}}}))

(defn q-from-collected-date
  [params]
  (when-let [date (:from-collected-date params)]
    {:range {:timestamp {:lt (coerce/to-long (common/start-of date))}}}))

(defn q-until-collected-date
  [params]
  (when-let [date (:until-collected-date params)] 
    {:range {:timestamp {:lt (coerce/to-long (common/end-of date))}}}))

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
    {:term {:relation relation}}))

(defn q-source
  [params]
  (if-let [source (:source params)]
    {:term {:source source}}))

(def query-processors
  (juxt 
    q-from-occurred-date
    q-until-occurred-date
    q-from-collected-date
    q-until-collected-date
    q-subj-id
    q-obj-id
    q-subj-id-prefix
    q-obj-id-prefix
    q-subj-id-domain
    q-obj-id-domain
    q-subj-url
    q-obj-url
    q-subj-url-domain
    q-obj-url-domain
    q-subj-alternative-id
    q-obj-alternative-id
    q-relation
    q-source))

(defn build-filter-query
  "Transform filter params dictionary into mongo query."
  [params]
  {:bool {:filter (remove nil? (query-processors params))}})
