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
    "ingest-yesterday" (ingest/run-ingest false)
    "ingest-until" (ingest/run-ingest (common/try-parse-ymd-date (second args)) false)
    "reingest-until" (ingest/run-ingest (common/try-parse-ymd-date (second args)) true)))
