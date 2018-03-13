(ns event-data-query.core
  (:require [event-data-query.ingest :as ingest]
            [event-data-query.elastic :as elastic]
            [event-data-query.work-cache :as work-cache]
            [event-data-query.server :as server]
            [clj-time.format :as clj-time-format]
            [clj-time.core :as clj-time]
            [clj-time.coerce :as clj-time-coerce]
            [clojure.tools.logging :as log]
            [event-data-common.core :as common])
  (:gen-class))

(defn close []
  (log/info "Close!")
  (shutdown-agents)
  (elastic/close!))

(defn -main
  [& args]
  (let [command (first args)
        rest-args (drop 2 args)]

    (elastic/ensure-indexes)
    (work-cache/ensure-index)

    (condp = command
      "update-mappings" (elastic/update-mappings)
      "server" (do (common/init)
                   (server/run))
      "replicate-continuous" (ingest/replicate-continuous)
      "replicate-backfill-days" (do (ingest/replicate-backfill-days (Integer/parseInt (second args)))
                                    (close))
      
      "ingest-kafka" (try
                       (common/init)
                       (ingest/run-ingest-kafka)
                       (catch Exception ex
                          (do (log/error "Error caught, exiting" ex)
                              (System/exit 1))))

      "bus-backfill-days" (do (ingest/bus-backfill-days (clj-time/now) (Integer/parseInt (second args)))
                              (close)
                              (log/info "Bye!"))

      ; Add a day to the supplied date, as the supplied date is never visited (waits until midnight so the archive is complete).
      "bus-backfill-days-from" (do (ingest/bus-backfill-days (clj-time/plus
                                                               (clj-time-coerce/from-string (second args))
                                                               (clj-time/days 1))
                                                             (Integer/parseInt (nth args 2)))
                              (close))

    (log/error "Didn't recognise command" (first args) ". Have another go."))))
