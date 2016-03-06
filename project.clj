(defproject bin-packing "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [gorillalabs/sparkling "1.2.3"]
                 [clj-http "2.1.0"]
                 [enlive "1.1.6"]]

  :aot [#".*" sparkling.serialization sparkling.destructuring]

  :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.10 "1.5.1"]]}
             :dev {:plugins [[lein-dotenv "RELEASE"]]}})
