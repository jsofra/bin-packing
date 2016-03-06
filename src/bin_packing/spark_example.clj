(ns bin-packing.spark-example
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as de]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [bin-packing.gutenberg :as gutenberg]
            [bin-packing.tf-idf :as tf-idf]))

(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "local")
              (conf/app-name "bin packing example"))]
    (spark/spark-context c)))
