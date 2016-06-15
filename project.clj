(defproject bin-packing "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [gorillalabs/sparkling "1.2.5"]
                 [clj-http "2.1.0"]
                 [enlive "1.1.6"]
                 [org.apache.spark/spark-core_2.11 "1.6.1"]]

  :main bin-packing.spark-example

  :aot [#".*" sparkling.serialization sparkling.destructuring
        bin-packing.spark-example]

  :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.11 "1.6.1"]]}
             :dev {:plugins [[lein-dotenv "RELEASE"]]}})
