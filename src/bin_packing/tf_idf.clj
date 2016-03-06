(ns bin-packing.tf-idf
  (:require [clojure.string :as string]))

(defn map-vals [f m]
  (into {} (for [[k v] m] [k (f v)])))

(def stop-words #{"a" "all" "and" "any" "are" "is" "in" "of" "on"
                  "or" "our" "so" "this" "the" "that" "to" "we"})

(defn split-words [s]
  (->> (string/split s #"\W+")
       (map string/lower-case)))

(defn get-terms [s]
  (remove stop-words (split-words s)))

(defn calc-tf [n-terms term-freq]
  (/ term-freq n-terms))

(defn calc-idf [n-docs term-n-docs]
  (Math/log (/ n-docs (+ 1.0 term-n-docs))))

(defn tf [doc]
  (let [terms   (get-terms doc)
        n-terms (count terms)]
    (map-vals (partial calc-tf n-terms) (frequencies terms))))

(defn term-doc-counts [tfs]
  (->> (mapcat keys tfs)
       (group-by identity)
       (map-vals count)))

(defn idf [n-docs term-doc-counts]
  (map-vals (partial calc-idf n-docs) term-doc-counts))

(defn tf-idf [docs]
  (let [tfs (map tf docs)
        idf (idf (count docs) (term-doc-counts tfs))]
    (map #(merge-with * % (select-keys idf (keys %)))
         tfs)))

(defn calc-scores [id-doc-pairs]
  )

(defn score-query [query tf-idf]
  (reduce + (for [t (get-terms query)
                  :let [score (get tf-idf t)]
                  :when score]
              score)))
