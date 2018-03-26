(defproject event-data-query "0.2.6"
  :description "Event Data Query"
  :url "http://eventdata.crossref.org/"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.cache "0.6.5"]
                 [event-data-common "0.1.46"]
                 [org.clojure/data.json "0.2.6"]
                 [crossref-util "0.1.15"]
                 [clj-http "3.4.1"]
                 [clj-http-fake "1.0.3"]
                 [ring/ring-mock "0.3.0"]
                 [overtone/at-at "1.2.0"]
                 [robert/bruce "0.8.0"]
                 [yogthos/config "0.8"]
                 [clj-time "0.12.2"]
                 [org.apache.httpcomponents/httpclient "4.5.2"]
                 [org.apache.commons/commons-io "1.3.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clojurewerkz/quartzite "2.0.0"]
                 [slingshot "0.12.2"]
                 [cc.qbits/spandex "0.6.0"]
                 [compojure "1.5.1"]
                 [liberator "0.14.1"]
                 [ring "1.5.0"]
                 [ring/ring-servlet "1.5.0"]
                 [com.climate/claypoole "1.1.4"]
                 [camel-snake-kebab "0.4.0"]
                 [org.clojure/core.async "0.4.474"]]
  :main ^:skip-aot event-data-query.core
  :plugins [[lein-cljfmt "0.5.7"]]
  :target-path "target/%s"
  :jvm-opts ["-Duser.timezone=UTC" "-Xmx1G" "-XX:-OmitStackTraceInFastThrow"]
  :profiles {:uberjar {:aot :all}})
