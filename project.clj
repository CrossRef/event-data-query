(defproject event-data-query "0.1.7"
  :description "Event Data Query"
  :url "http://eventdata.crossref.org/"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.cache "0.6.5"]
                 [event-data-common "0.1.25"]
                 [org.clojure/data.json "0.2.6"]
                 [crossref-util "0.1.10"]
                 [clj-http "3.4.1"]
                 [overtone/at-at "1.2.0"]
                 [robert/bruce "0.8.0"]
                 [yogthos/config "0.8"]
                 [clj-time "0.12.2"]
                 [org.apache.httpcomponents/httpclient "4.5.2"]
                 [org.apache.commons/commons-io "1.3.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.apache.logging.log4j/log4j-core "2.6.2"]
                 [org.slf4j/slf4j-simple "1.7.21"]
                 [clojurewerkz/quartzite "2.0.0"]
                 [cheshire "5.7.0"]
                 [com.novemberain/monger "3.1.0"]
                 [compojure "1.5.1"]
                 [org.eclipse.jetty/jetty-server "9.4.0.M0"]
                 [liberator "0.14.1"]
                 [ring "1.5.0"]
                 [ring/ring-jetty-adapter "1.5.0"]
                 [ring/ring-servlet "1.5.0"]]
  :main ^:skip-aot event-data-query.core
  :target-path "target/%s"
  :jvm-opts ["-Duser.timezone=UTC"]
  :profiles {:uberjar {:aot :all}})
