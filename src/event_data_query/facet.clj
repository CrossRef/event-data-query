(ns event-data-query.facet
  (:require [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+]]))


(def max-size 100)

(def facetable-fields
  "Mapping of acceptable query field to ElasticSearch indexed field."
  {"source" "source"
   "relation-type" "relation-type"
   "obj-id.prefix" "obj-prefix"
   "subj-id.prefix" "subj.prefix"
   "subj-id.domain" "subj-id-domain"
   "obj-id.domain" "obj-id-domain"
   "subj.url.domain" "subj-url-domain"
   "obj.url.domain" "obj-url-domain"})

(defn validate
  "Validate that all facet keys are recognised. Return nil or throw exception for first error."
  [params]
  (when-let [unrecognised (first (clojure.set/difference (set (keys params)) (set (keys facetable-fields))))]
    (throw+ {:type :validation-failure
             :subtype :facet-unavailable
             :message (str "Facet " (name unrecognised) " specified but there is no such facet for this route. Valid facets for this route are: " (clojure.string/join ", " (map name (keys facetable-fields))))}))

    (doseq [[k v] params]
      (when-not (= v "*")
        (try 
          (let [n (Integer/parseInt v)]
            (when (> n max-size)
              (throw+ {:type :validation-failure
                       :subtype :facet-size
                       :message (str "Facet size value of '" v "' greater than the maximum value of " max-size)})))
          (catch NumberFormatException ex
            (throw+ {:type :validation-failure
              :subtype :facet-size-malformed
              :message (str "Facet size value of '" v "' unrecognised for '" k "'. Please supply an integer or '*'.")}))))))


(defn build-facet-query
  "Transform facet params dictionary into ElasticSearch query.
   Produce a top-level Elastic request body, which can be merged at the top level with a query."
  [params]
  (when (not-empty params)
    (let [; as seq of elastic field, size
          with-sizes (map
                       (fn [[k v ]]
                        [(facetable-fields k)
                         (if (= "*" v)
                           max-size
                           (Integer/parseInt v))]) params)
          result (into
                   {}
                   (map (fn [[k size]]
                     [k {:terms {:field k :size size}}]) with-sizes))]

    (log/info "Build facet query" params "->" result)
    result)))
  

