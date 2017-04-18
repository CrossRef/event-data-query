(ns event-data-query.core
  (:require [event-data-query.ingest :as ingest]
            [event-data-query.server :as server]
            [event-data-query.common :as common]
            [clj-time.format :as clj-time-format])
  (:gen-class))


(defn -main
  [& args]
  (condp = (first args)
    "server" (server/run)
    "add-indexes" (ingest/add-indexes)
    "ingest-yesterday" (ingest/run-ingest (common/yesterday) (common/yesterday) false)
    "ingest-range" (ingest/run-ingest (clj-time-format/parse common/ymd-format (second args))
                                      (clj-time-format/parse common/ymd-format (nth args 2)) false)
    "reingest-range" (ingest/run-ingest (clj-time-format/parse common/ymd-format (second args))
                                        (clj-time-format/parse common/ymd-format (nth args 2)) true)))
