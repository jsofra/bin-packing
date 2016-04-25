(ns bin-packing.spark-example
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as de]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [bin-packing.gutenberg :as gutenberg]
            [bin-packing.tf-idf :as tf-idf])
  (:import [org.apache.spark Partitioner]))

(defn get-bookshelf-ebooks-urls! []
  (-> (gutenberg/get-bookself-ids!)
      gutenberg/bookshelf-ebooks))

(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "local")
              (conf/app-name "bin packing example"))]
    (spark/spark-context c)))

(defn partitioner-fn [n-partitions partition-fn]
  (proxy [Partitioner] []
    (getPartition [key] (partition-fn key))
    (numPartitions [] n-partitions)))

(defn make-bookshelves-rdd [sc bookshelf-urls]
  (spark/parallelize sc (map (fn [url bookshelf]
                               (spark/tuple url (:ebooks bookshelf)))
                             bookshelf-urls)))

(defn bookshelf-ebooks [bookshelfs-ids]
  (spark/map
   (fn [url ids]
     (let [urls (gutenburg/generate-ebook-urls ids)]
       (spark/tuple [url {:ebooks (map vector ids (map first urls))
                          :size (apply + (map second urls))}])))
   bookshelfs-ids))

(defn run-analysis []
  (let [sc (make-spark-context)
        bookshelfs-ids (gutenberg/get-bookself-ids!)
        ebook-ids-rdd (spark/parallelize sc ebook-ids)
        ebook-urls (generate-ebook-urls ebook-ids-rdd)
        bookshelves-rdd (make-bookshelves-rdd bs-urls)]
    bookshelves-rdd))

(comment
  (def ebook-ids (gutenberg/get-bookself-ids!))
  (def f-ebook-ids (first ebook-ids))
  (def f-texts (bookshelf-ebooks [f-ebook-ids]))
  (def bs (-> f-texts first second))
  (def bs-texts (gutenberg/get-ebook-texts bs))
  (def ss (tf-idf/tf-idf-pairs (:ebooks bs-texts)))
  )
