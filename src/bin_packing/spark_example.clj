(ns bin-packing.spark-example
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as de]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [bin-packing.gutenberg :as gutenberg]
            [bin-packing.tf-idf :as tf-idf]
            [bin-packing.spark-utils :as su]
            [bin-packing.core :as bin-packing])
  (:import [org.apache.spark Partitioner])
  (:gen-class))

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
    ;; get partition index
    (getPartition [key] (partition-fn key))
    (numPartitions [] n-partitions)))

(defn bookshelf-ebooks [bookshelfs-ids]
  (spark/map-to-pair
   (fn [[url ids-and-titles]]
     (let [ids  (map first ids-and-titles)
           urls (gutenberg/generate-ebook-urls ids)]
       (spark/tuple url {:ebooks (map vector ids-and-titles (map first urls))
                         :size (apply + (map second urls))})))
   bookshelfs-ids))

(defn ebooks-tf-idf [bookshelf-texts]
  (println "DEBUG: Calculating ebooks tf-idf ...")
  (su/map-vals #(-> % :ebooks tf-idf/tf-idf) bookshelf-texts))

(defn bookshelf-combined-texts [bookshelf-texts]
  (spark/map-values #(->> %
                          :ebooks
                          (map second)
                          (interpose " ")
                          (apply str))
                    bookshelf-texts))

(defn bookshelfs-tf-idf [bookshelf-texts]
  (println "DEBUG: Calculating bookshelfs tf-idf ...")
  (tf-idf/tf-idf (bookshelf-combined-texts bookshelf-texts)))

(defn get-ebook-urls [sc & {:keys [ebook-urls-path]}]
  (if ebook-urls-path
    (->> ebook-urls-path
         (spark/text-file sc)
         (spark/map-to-pair #(apply spark/tuple (clojure.edn/read-string %)))
         (spark/repartition 100))
    (let [_ (println "*** GETTING BOOK IDS")
          bookshelfs-ids (gutenberg/get-bookself-ids-and-titles!)
          ;; [[bookshelf-url [ebook-id]]]
          _ (println "*** CREATING IDS RDD")
          ebook-ids-rdd  (spark/parallelize sc bookshelfs-ids)]
      (println "*** GETTING URLS")
      ;; [#tuple[bookshelf-url {:ebooks [[ebook-id ebook-url]] :size total-ebook-size}]]
      (bookshelf-ebooks ebook-ids-rdd))))

(defn partition-into-bins [ebook-urls]
  (let [ebook-urls     (spark/cache ebook-urls)
        packing-items  (spark/collect (su/map (fn [[k v]] [k (:size v)]) ebook-urls))
        item-indices   (-> packing-items bin-packing/pack bin-packing/item-indices)
        ebook-urls     (spark/partition-by
                        (partitioner-fn (:bin-count item-indices)
                                        (:item-indices item-indices))
                        ebook-urls)]
    ebook-urls))

(defn run-analysis [sc & {:keys [pack]}]
  (let [ebook-urls   (get-ebook-urls
                      sc
                      :ebook-urls-path "s3://silverpond/bin-packing-example/ebook_urls.txt")
        ebook-urls   (if pack (partition-into-bins ebook-urls) ebook-urls)

        _ (println "*** GETTING TEXTS")
        bs-texts     (spark/map-values gutenberg/get-ebook-texts ebook-urls)
        ;; [#tuple[bookshelf-url {:ebooks [[ebook-id text]] :size total-ebook-size}]]
        _ (println "*** RUNNING TF-IDF ON TEXTS")
        books-tf-idf (spark/map-values (partial into [])
                                       (ebooks-tf-idf bs-texts))
        ;; [#tuple[bookshelf-url [[ebook-id tf-idf]]]]
        ]
    {:books-tf-idf books-tf-idf}))


(defn -main [& args]
  (println "*** CREATING SPARK CONTEXT")
  (let [sc (make-spark-context)
        {:keys [books-tf-idf
                bookshelf-tf-idf]} (run-analysis sc :pack true)]
    (spark/save-as-text-file (str "s3://silverpond/bin-packing-example/output/"
                                  (first args)) ;books_tf_idf_packed.txt
                             books-tf-idf)
    ;(spark/save-as-text-file "s3://silverpond/bin-packing-example/output/bookshelf_tf_idf.txt"
    ;                         bookshelf-tf-idf)
    ))

;; (set! *print-length* 3)
;; (set! *print-level* 6)

(comment
  (def sc (make-local-spark-context))
  (def ebook-ids (gutenberg/get-bookself-ids-and-titles!))
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
