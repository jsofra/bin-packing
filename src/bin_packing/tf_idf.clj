(ns bin-packing.tf-idf
  (:require [clojure.string :as string]
            [bin-packing.spark-utils :as su]))

(def stop-words #{"" "a" "all" "and" "any" "are" "is" "in" "of" "on"
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

(defn tf [terms]
  (let [n-terms (count terms)]
    (println (str "DEBUG: Calculating tf for " n-terms " terms ..."))
    (su/map-vals (partial calc-tf n-terms) (frequencies terms))))

(defn term-doc-counts [id-and-terms]
  (->> (su/mapcat (fn [[k v]] v) id-and-terms)
       (su/group-by identity)
       (su/map-vals count)))

(defn idf [n-docs term-doc-counts]
  (println (str "DEBUG: Calculating idf for " n-docs " docs ..."))
  (su/map-vals (partial calc-idf n-docs) term-doc-counts))

(defn calc-tf-and-idf [id-doc-pairs]
  (let [id-and-terms (su/cache (su/map-vals get-terms id-doc-pairs))]
    {:tfs (su/map-vals tf id-and-terms)
     :idf (su/to-map (idf (su/count id-doc-pairs)
                          (term-doc-counts id-and-terms)))}))

(defn calc-tf-idf [{:keys [tfs idf]}]
  (su/map-vals #(merge-with * % (select-keys idf (keys %)))
               tfs))

(defn tf-idf [id-doc-pairs]
  (-> id-doc-pairs
      calc-tf-and-idf
      calc-tf-idf))

(defn calc-scores [id-doc-pairs]
  )

(defn score-query [query tf-idf]
  (reduce + (for [t (get-terms query)
                  :let [score (get tf-idf t)]
                  :when score]
              score)))
