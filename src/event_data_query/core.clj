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
    "ingest-yesterday" (ingest/run-ingest (common/yesterday) (common/yesterday) false)
    "ingest-range" (ingest/run-ingest (common/try-parse-ymd-date (second args)) (common/try-parse-ymd-date (nth args 2)) false)
    "reingest-range" (ingest/run-ingest (common/try-parse-ymd-date (second args)) (common/try-parse-ymd-date (nth args 2)) true)))
