(ns bin-packing.core)

(defn add-to-bin [bin [_ size :as item]]
  {:size (+ (:size bin) size)
   :items (conj (:items bin) item)})

(defn select-bin [bins [_ size] max-size]
  (letfn [(fits? [bin]
            (let [new-size (+ (:size bin) size)]
              (<= new-size max-size)))]
    (-> (keep-indexed (fn [i b] (when (fits? b) i)) bins)
        first)))

(defn pack
  ([items] (pack items (second (first items))))
  ([items max-size]
   (let [empty-bin {:size 0 :items []}
         init-bins [empty-bin]]
     (reduce (fn [bins item]
               (if-let [fit (select-bin bins item max-size)]
                 (update-in bins [fit] #(add-to-bin % item))
                 (conj bins (add-to-bin empty-bin item))))
             init-bins items))))

(defn pack-n-bins-ish [items n]
  (let [max-size (/ (reduce + (map second items)) (dec n))]
    (pack items max-size)))

(defn pack-n-bins
  ([items n] (pack-n-bins items n (second (first items))))
  ([items n max-size]
   (let [p-items (partition n n (repeat nil) items)
         bin-slices (map-indexed (fn [idx i] (if (even? idx) i (reverse i)))
                                 p-items)]
     (apply map (fn [& items]
                  (let [items (filter identity items)]
                    {:size (reduce + (map second items))
                     :items items}))
            bin-slices))))

(defn item-indices [bins]
  {:bin-count (count bins)
   :item-indices (into {}
                       (for [[idx bin] (map-indexed vector bins)
                             [k _] (:items bin)]
                         [k idx]))})

(let [sizes [9   8  7  6  5  4  3  2  1  0]
      keys  [:a :b :c :d :e :f :g :h :i :j]
      items (map vector keys sizes)]
  (assert
   (= (pack items)
      [{:size 9 :items [[:a 9] [:j 0]]}
       {:size 9 :items [[:b 8] [:i 1]]}
       {:size 9 :items [[:c 7] [:h 2]]}
       {:size 9 :items [[:d 6] [:g 3]]}
       {:size 9 :items [[:e 5] [:f 4]]}]))
  (assert
   (= (item-indices (pack items))
      {:bin-count 5
       :item-indices {:a 0 :b 1 :c 2 :d 3 :e 4
                      :f 4 :g 3 :h 2 :i 1 :j 0}})))
