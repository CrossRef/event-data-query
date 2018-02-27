(ns event-data-query.core
  (:require [event-data-query.ingest :as ingest]
            [event-data-query.elastic :as elastic]
            [event-data-query.work-cache :as work-cache]
            [event-data-query.server :as server]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce]
            [clojure.tools.logging :as log]
            [event-data-common.core :as common])
  (:gen-class))

(defn close []
  (shutdown-agents)
  (elastic/close!))

(defn -main
  [& args]
  (let [command (first args)
        rest-args (drop 2 args)]

    (common/init)

    (elastic/ensure-indexes)
    (work-cache/ensure-index)

    (condp = command
      "update-mappings" (elastic/update-mappings)
      "server" (server/run)
      "replicate-continuous" (ingest/replicate-continuous)
      "replicate-backfill-days" (do (ingest/replicate-backfill-days (Integer/parseInt (second args)))
                                    (close))
      
      "ingest-kafka" (try
                       (ingest/run-ingest-kafka)
                       (catch Exception ex
                          (do (log/error "Error caught, exiting" ex)
                              (System/exit 1))))

      "bus-backfill-days" (do (ingest/bus-backfill-days (Integer/parseInt (second args)) false)
                              (close))

      ; Useful for re-indexing data to cover a period when there was a bug,
      ; so data needs to be re-indexed even if the version number is the same.
      "bus-backfill-days-force" (do (ingest/bus-backfill-days (Integer/parseInt (second args)) true)
                                    (close))

    (log/error "Didn't recognise command" (first args) ". Have another go."))))
