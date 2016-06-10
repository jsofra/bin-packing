(ns bin-packing.spark-example
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as de]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [bin-packing.gutenberg :as gutenberg]
            [bin-packing.tf-idf :as tf-idf]
            [bin-packing.spark-utils :as su])
  (:import [org.apache.spark Partitioner]))

(defn get-bookshelf-ebooks-urls! []
  (-> (gutenberg/get-bookself-ids!)
      gutenberg/bookshelf-ebooks))

(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/app-name "bin packing example"))]
    (spark/spark-context c)))

(defn make-local-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "local")
              (conf/app-name "bin packing example"))]
    (spark/spark-context c)))

(defn partitioner-fn [n-partitions partition-fn]
  (proxy [Partitioner] []
    (getPartition [key] (partition-fn key))
    (numPartitions [] n-partitions)))

(defn bookshelf-ebooks [bookshelfs-ids]
  (spark/map-to-pair
   (fn [[url ids]]
     (let [urls (gutenberg/generate-ebook-urls ids)]
       (spark/tuple url {:ebooks (map vector ids (map first urls))
                         :size (apply + (map second urls))})))
   bookshelfs-ids))

(defn ebooks-tf-idf [bookshelf-texts]
  (su/map-vals #(-> % :ebooks tf-idf/tf-idf) bookshelf-texts))

(defn bookshelf-combined-texts [bookshelf-texts]
  (spark/map-values #(->> %
                          :ebooks
                          (map second)
                          (interpose " ")
                          (apply str))
                    bookshelf-texts))

(defn bookshelfs-tf-idf [bookshelf-texts]
  (tf-idf/tf-idf (bookshelf-combined-texts bookshelf-texts)))

(defn run-analysis [sc]
  (let [bookshelfs-ids (gutenberg/get-bookself-ids!)
        ;; [[bookshelf-url [ebook-id]]]
        ebook-ids-rdd  (spark/parallelize sc bookshelfs-ids)
        ebook-urls     (bookshelf-ebooks ebook-ids-rdd)
        ;; [#tuple[bookshelf-url {:ebooks [[ebook-id ebook-url]] :size total-ebook-size}]]
        bs-texts       (spark/cache (spark/map-values gutenberg/get-ebook-texts
                                                      ebook-urls))
        ;; [#tuple[bookshelf-url {:ebooks [[ebook-id text]] :size total-ebook-size}]]
        books-tf-idf   (ebooks-tf-idf bs-texts)
        ;; [#tuple[bookshelf-url [[ebook-id tf-idf]]]]
        bs-tf-idf      (bookshelfs-tf-idf bs-texts)
        ;; [#tuple[bookshelf-url tf-idf]]
        ]
    {:books-tf-idf books-tf-idf
     :bookshelf-tf-idf bs-tf-idf}))

;; (set! *print-length* 3)
;; (set! *print-level* 5)

(comment
  (def sc (make-local-spark-context))
  (def ebook-ids (gutenberg/get-bookself-ids!))
  ;; [[bookshelf-url [ebook-id]]]
  (def f-ebook-ids (update-in (first ebook-ids) [1] (partial take 2)))
  (def ebook-urls (bookshelf-ebooks (spark/parallelize sc [f-ebook-ids])))
  ;; [#tuple[bookshelf-url {:ebooks [[ebook-id ebook-url]] :size total-ebook-size}]]
  (def bs-texts (spark/cache (spark/map-values gutenberg/get-ebook-texts ebook-urls)))
  ;; [#tuple[bookshelf-url {:ebooks [[ebook-id text]] :size total-ebook-size}]]
  (def ss (spark/map-values #(-> % :ebooks tf-idf/tf-idf) bs-texts))
  ;; [#tuple[bookshelf-url [[ebook-id tf-idf]]]]
  (def bs-combined-text (spark/map-values #(->> %
                                                :ebooks
                                                (map second)
                                                (interpose " ")
                                                (apply str))
                                          bs-texts))
  ;; [#tuple[bookshelf-url text]]
  (def full-tf-idf (tf-idf/tf-idf bs-combined-text))
  )
