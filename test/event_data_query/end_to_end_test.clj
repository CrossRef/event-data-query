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
  [url-path message-key]
  (-> (mock/request :get url-path)
      server/app
      :body
      (json/read-str :key-fn keyword)
      :message
      message-key))

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

          (ingest/bus-backfill-days (clj-time/now) 1))

        ; Force Elastic to wait for indexes.
        (is (= 200 (:status (s/request @elastic/connection {:url "_refresh" :method :post}))))

        ; Use both the "events" endpoints and the "events ids" endpoint to check they both return the right selection of Events.
        (let [standard-events (get-response "/v1/events" :events)
              distinct-events (get-response "/v1/events/distinct" :events)
              edited-events (get-response "/v1/events/edited" :events)
              deleted-events (get-response "/v1/events/deleted" :events)
              experimental-events (get-response "/v1/events/experimental" :events)
              scholix-events (get-response "/v1/events/scholix" :link-packages)

              standard-event-ids (get-response "/v1/events/ids" :event-ids)
              distinct-event-ids (get-response "/v1/events/distinct/ids" :event-ids)
              edited-event-ids (get-response "/v1/events/edited/ids" :event-ids)
              deleted-event-ids (get-response "/v1/events/deleted/ids" :event-ids)
              experimental-event-ids (get-response "/v1/events/experimental/ids" :event-ids)
              scholix-event-ids (get-response "/v1/events/scholix/ids" :link-package-ids)]
          
          (is (= (->> standard-events (map :id) set)
                 (set standard-event-ids)
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
                    "00000000-0000-0000-0000-00000000000a"})
            "/standard and /standard/ids return the right set of Events")
          
          (is (= (->> distinct-events (map :id) set)
                 (set distinct-event-ids)
                 #{ ; Standard 2
                    "00000000-0000-0000-0000-000000000002"

                    ; Edited 1
                    "00000000-0000-0000-0000-000000000005" 

                    ; Edited 2
                    "00000000-0000-0000-0000-000000000006"

                    ; Scholix 1
                    "00000000-0000-0000-0000-000000000009"

                    ; Scholix 2
                    "00000000-0000-0000-0000-00000000000a"})
            "/distinct and /distinct/ids return the right set of Events")

          (is (= (->> edited-events (map :id) set)
                 (set edited-event-ids)
                 #{ ; Edited 1
                    "00000000-0000-0000-0000-000000000005" 

                    ; Edited 2
                    "00000000-0000-0000-0000-000000000006"})
            "/edited and /edited/ids return the right set of Events")

          (is (= (->> deleted-events (map :id) set)
                 (set deleted-event-ids)
                 #{; Deleted 1
                   "00000000-0000-0000-0000-000000000007"

                   ; Deleted 2
                   "00000000-0000-0000-0000-000000000008"})
            "/deleted and /deleted/ids return the right set of Events")

          (is (= (->> experimental-events (map :id) set)
                 (set experimental-event-ids)
                 #{; Experimental 1
                   "00000000-0000-0000-0000-000000000003"

                   ; Experimental 2
                   "00000000-0000-0000-0000-000000000004"})
            "/experimental and /experimental/ids return the right set of Events")

          (is (= (set scholix-events)
                  #{{:LinkPublicationDate "2018-01-01T00:01:01Z",
                     :LinkProvider [{:Name "crossref"}],
                     :RelationshipType {:Name "References"},
                     :LicenseURL "https://creativecommons.org/publicdomain/zero/1.0/",
                     :Url
                     "https://api.eventdata.crossref.org/v1/events/scholix/00000000-0000-0000-0000-00000000000a",
                     :Source
                     {:Identifier
                      {:ID "10.1016/S0305-9006(99)00007-0",
                       :IDScheme "DOI",
                       :IDUrl "https://doi.org/10.1016/S0305-9006(99)00007-0"},
                      :Type
                      {:Name "literature",
                       :SubType "journal-article",
                       :SubTypeSchema "crossref"}},
                     :Target
                     {:Identifier
                      {:ID "10.5167/UZH-30455",
                       :IDScheme "DOI",
                       :IDUrl "https://doi.org/10.5167/UZH-30455"},
                      :Type
                      {:Name "literature", :SubType "text", :SubTypeSchema "datacite"}}}
                    {:LinkPublicationDate "2018-01-01T00:01:01Z",
                     :LinkProvider [{:Name "datacite"}],
                     :RelationshipType {:Name "IsReferencedBy"},
                     :LicenseURL "https://creativecommons.org/publicdomain/zero/1.0/",
                     :Url
                     "https://api.eventdata.crossref.org/v1/events/scholix/00000000-0000-0000-0000-000000000009",
                     :Source
                     {:Identifier
                      {:ID "10.5167/UZH-30455",
                       :IDScheme "DOI",
                       :IDUrl "https://doi.org/10.5167/UZH-30455"},
                      :Type {:Name "literature", :SubType "text", :SubTypeSchema "datacite"}},
                     :Target
                     {:Identifier
                      {:ID "10.1016/S0305-9006(99)00007-0",
                       :IDScheme "DOI",
                       :IDUrl "https://doi.org/10.1016/S0305-9006(99)00007-0"},
                      :Type
                      {:Name "literature",
                       :SubType "journal-article",
                       :SubTypeSchema "crossref"}}}})))))))


