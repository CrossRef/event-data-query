(ns event-data-query.end-to-end-test
  "Load Events from an archive, index, query."
  (:require [clojure.test :refer :all]
            [event-data-query.elastic :as elastic]
            [event-data-query.ingest :as ingest]
            [event-data-query.server :as server]
            [qbits.spandex :as s]
            [clj-time.core :as clj-time]
            [clj-http.fake :as fake]
            [ring.mock.request :as mock]
            [config.core :refer [env]]
            [clojure.data.json :as json]))

(defn get-response
  [url-path]
  (-> (mock/request :get url-path)
      server/app
      :body
      (json/read-str :key-fn keyword)
      :message
      :events))

(deftest end-to-end
  (testing "Events ingested and can be ingested from the archive and retrieved from each Events collection."
    
    (is (= (-> env :query-deployment) "test_")
        "Precondition: Expected the QUERY_DEPLOYMENT config to be '_test'.")

    ;; Do this on the 2nd of January, which will result in querying the archive for the 1st.
    (clj-time/do-at (clj-time/date-time 2018 01 02 12 00)
      (with-redefs [
        ; Fake out source-whitelist fetching from the Artifact registry.
        ingest/source-whitelist
        (delay #{"crossref" "datacite" "wikipedia"})

        ; Fake out source-whitelist fetching from the Artifact registry.
        ingest/prefix-whitelist
        (delay #{"10.5555" "10.6666" "10.1016" "10.5167"})

        ; Fake out fetching metadata from RAs for Scholix examples.
        ; These match deposits in the resources/test/end-to-end.json
        event-data-query.work-cache/get-for-dois
        (fn [_]
          {"https://doi.org/10.1016/s0305-9006(99)00007-0"
           {:content-type "journal-article" :ra "crossref" :doi "10.1016/s0305-9006(99)00007-0"}

           "https://doi.org/10.5167/UZH-30455"
           {:content-type "text" :ra "datacite" :doi "10.5167/UZH-30455"}})]

        ; Clear the index first if it exists.
        (try
          (:status (s/request @elastic/connection {:url "/test_*" :method :delete}))
          (catch Exception _ nil))

        ; Create indexes.
        (elastic/ensure-indexes)

        (fake/with-fake-routes-in-isolation
          {
            ; Serve up all test data in the first partition.
            "https://bus.eventdata.crossref.org/events/archive/2018-01-01/00"
            (fn [request] {:status 200 :body (slurp "resources/test/end-to-end.json")})

            ; The rest are blank.
            #"https://bus.eventdata.crossref.org/events/archive/2018-01-01/.*"
            (fn [request] {:status 200 :body "[]"})}

          (ingest/bus-backfill-days 1 false))

        ; Force Elastic to wait for indexes.
        (is (= 200 (:status (s/request @elastic/connection {:url "_refresh" :method :post}))))

        (let [standard-events (get-response "/v1/events")
              distinct-events (get-response "/v1/events/distinct")
              edited-events (get-response "/v1/events/edited")
              deleted-events (get-response "/v1/events/deleted")
              experimental-events (get-response "/v1/events/experimental")
              scholix-events (get-response "/v1/events/scholix")]

          (is (= (->> standard-events (map :id) set)
                 #{ ; Standard 1
                    "00000000-0000-0000-0000-000000000001" 

                    ; Standard 2
                    "00000000-0000-0000-0000-000000000002"

                    ; Edited 1
                    "00000000-0000-0000-0000-000000000005" 

                    ; Edited 2
                    "00000000-0000-0000-0000-000000000006"

                    ; Scholix 1
                    "00000000-0000-0000-0000-000000000009"
                    ; Scholix 2
                    "00000000-0000-0000-0000-00000000000a"}))
          
          (is (= (->> distinct-events (map :id) set)
                 #{ ; Standard 2
                    "00000000-0000-0000-0000-000000000002"

                    ; Edited 1
                    "00000000-0000-0000-0000-000000000005" 

                    ; Edited 2
                    "00000000-0000-0000-0000-000000000006"

                    ; Scholix 1
                    "00000000-0000-0000-0000-000000000009"

                    ; Scholix 2
                    "00000000-0000-0000-0000-00000000000a"}))

          (is (= (->> edited-events (map :id) set) 
                 #{ ; Edited 1
                    "00000000-0000-0000-0000-000000000005" 

                    ; Edited 2
                    "00000000-0000-0000-0000-000000000006"}))

          (is (= (->> deleted-events (map :id) set)
                 #{; Deleted 1
                   "00000000-0000-0000-0000-000000000007"

                   ; Deleted 2
                   "00000000-0000-0000-0000-000000000008"}))

          (is (= (->> experimental-events (map :id) set)
                 #{; Experimental 1
                   "00000000-0000-0000-0000-000000000003"

                   ; Experimental 2
                   "00000000-0000-0000-0000-000000000004"}))

          (is (= (->> scholix-events (map :id) set) 
                 #{; Scholix 1
                   "00000000-0000-0000-0000-000000000009"

                   ; Scholix 2
                   "00000000-0000-0000-0000-00000000000a"})))))))
