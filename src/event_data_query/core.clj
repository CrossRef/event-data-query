(ns event-data-query.core
  (:require [event-data-query.ingest :as ingest]
            [event-data-query.elastic :as elastic]
            [event-data-query.server :as server]
            [event-data-query.common :as common]
            [clj-time.format :as clj-time-format]
            [clojure.tools.logging :as log])
  (:gen-class))

(defn -main
  [& args]
  (elastic/ensure-index)
  (condp = (first args)
    "server" (server/run)
    "replicate-continuous" (ingest/replicate-continuous)
    "replicate-backfill-days" (ingest/replicate-backfill-days (Integer/parseInt (second args)))
    "queue-continuous" (ingest/queue-continuous)
    "bus-backfill-days" (ingest/bus-backfill-days (Integer/parseInt (second args)))
    (log/error "Didn't recognise command" (first args) ". Have another go.")))
