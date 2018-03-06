(ns event-data-query.work-cache-test
  (:require [clojure.test :refer :all]
            [event-data-query.work-cache :as work-cache]
            [clj-http.fake :as fake]
            [qbits.spandex :as s]
            [event-data-query.elastic :as elastic]
            [slingshot.slingshot :refer [try+]]
            [config.core :refer [env]]))

(deftest doi->id
  (testing "The same DOI expressed different ways results in the same cache ID"
    (is (=
           (work-cache/doi->id "10.5555/12345678abc")
           (work-cache/doi->id "10.5555/12345678ABC")
           (work-cache/doi->id "doi:10.5555/12345678abc")
           (work-cache/doi->id "doi.org/10.5555/12345678abc")
           (work-cache/doi->id "http://doi.org/10.5555/12345678abc")))))

(deftest get-for-dois-mixed-input
  (testing "get-for-dois can cope with nils, dois and non-dois"
    (try
      (:status (s/request @elastic/connection {:url @work-cache/index-id :method :delete}))
      (catch Exception _))
    
    (work-cache/ensure-index)

    (fake/with-fake-routes-in-isolation
          {
            ; Fake lookups for RA
            "https://doi.org/doiRA/10.1016%2FS0305-9006%2899%2900007-0"
            (fn [request] {:status 200 :body "[{\"DOI\": \"10.1016/S0305-9006(99)00007-0\", \"RA\": \"Crossref\"}]"})

            "https://doi.org/doiRA/10.5555%2FIN-DOI-BUT-NOT-CROSSREF-API"
            (fn [request] {:status 200 :body "[{\"DOI\": \"10.5555/IN-DOI-BUT-NOT-CROSSREF-API\", \"RA\": \"Crossref\"}]"})

            "https://doi.org/doiRA/10.5167%2FUZH-30455"
            (fn [request] {:status 200 :body "[{\"DOI\": \"10.5167/UZH-30455\",\"RA\": \"DataCite\"}]"})

            ; doiRA uses 200 to report a missing DOI. Not 400 or 404.
            "https://doi.org/doiRA/10.999999%2F999999999999999999999999999999999999"
            (fn [request] {:status 200 :body "[{\"DOI\": \"10.999999/999999999999999999999999999999999999\",\"status\": \"DOI does not exist\"}]"})

            "https://api.crossref.org/v1/works/10.1016%2FS0305-9006%2899%2900007-0?mailto=eventdata@crossref.org"
            (fn [request] {:status 200 :body "{\"message\": {\"type\": \"journal-article\"}}"})

            ; This one's in the DOI system, but is missing from the REST API for some reason.
            "https://api.crossref.org/v1/works/10.5555%2FIN-DOI-BUT-NOT-CROSSREF-API?mailto=eventdata@crossref.org"
            (fn [request] {:status 404})

            "https://api.datacite.org/works/10.5167%2FUZH-30455?include=resource-type"
            (fn [request] {:status 200 :body "{\"data\": {\"attributes\": {\"resource_type_id\": \"text\"}}}"})}

        (let [inputs
               ; Nil may find its way here.
              [nil
              ; We could find any nonsense in a subj-id or obj-id field.
              "blah"
              
              ; We could find non-DOI urls.
              "http://www.example.com"

              ; Non-URL form Crossref
              "10.1016/s0305-9006(99)00007-0"

              ; URL-form DataCite
              "https://doi.org/10.5167/uzh-30455"

              ; Non-existent
              "10.999999/999999999999999999999999999999999999"

              ; In the DOI API but the Crossref API doesn't have it.
              "10.5555/in-doi-but-not-crossref-api"]

              result (work-cache/get-for-dois inputs)]

          (is (= result
            {; Non-DOI has empty response.
             nil
             nil

             "http://www.example.com"
             nil

             ; Non-DOI has empty response.
             "blah"
             nil

             ; DOI-looking, but non-existent, has empty response.
             ; Explicitly returning nil in this case ensures we don't get a false-negative in future e.g. if the DOI is subsequently registered.
             "10.999999/999999999999999999999999999999999999"
             nil

             ; Non-URL DOI returned OK.
             "10.1016/s0305-9006(99)00007-0"
             {:content-type "journal-article"
              :ra :crossref
              :doi "10.1016/S0305-9006(99)00007-0"}

             ; URL DOI returned OK with correct info.
             ; DOI is normalized to non-url, lower-case form.
             "https://doi.org/10.5167/uzh-30455"
             {:content-type "text"
              :ra :datacite
              :doi "10.5167/UZH-30455"}

              "10.5555/in-doi-but-not-crossref-api"
              nil}))))))

