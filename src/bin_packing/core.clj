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

(defn select-smallest-bin [bins]
  (first (apply min-key
                (fn [[idx bin]] (:size bin))
                (map-indexed vector bins))))

(defn pack
  "Simple first fit packing algorithm."
  ([items no-fit-fn] (pack items no-fit-fn (second (first items))))
  ([items no-fit-fn max-size]
   (let [init-bins [{:size 0 :items []}]]
     (reduce (fn [bins item]
               (if-let [fit (select-bin bins item max-size)]
                 (update-in bins [fit] #(add-to-bin % item))
                 (no-fit-fn bins item)))
             init-bins items))))

(defn grow-bins [bins item]
  (conj bins (add-to-bin {:size 0 :items []} item)))

(defn first-fit
  "Simple first fit algorithm that grows bins once reaching max-size.
   Should give min required bins."
  ([items] (pack items grow-bins))
  ([items max-size] (pack items grow-bins max-size)))

(defn add-to-smallest-bin [n bins item]
  (if (< (count bins) n)
    (grow-bins bins item)
    (update-in bins [(select-smallest-bin bins)]
              #(add-to-bin % item))))

(defn pack-n-bins
  "Simple first fit algorithm that continues to add to the smallest bin
   once n bins have been filled to max-size."
  ([items n] (pack items (partial add-to-smallest-bin n)))
  ([items n max-size] (pack items (partial add-to-smallest-bin n) max-size)))

(defn pack-n-bins-ish [items n]
  (let [max-size (/ (reduce + (map second items)) (dec n))]
    (pack items max-size)))

(defn stich-bin-slices [bin-slices]
  (apply map (fn [& items]
               (let [items (filter identity items)]
                 {:size (reduce + (map second items))
                  :items items}))
         bin-slices))

(defn pack-n-bins-2
  ([items n] (pack-n-bins-2 items n (second (first items))))
  ([items n max-size]
   (let [p-items (partition n n (repeat nil) items)
         bin-slices (map-indexed (fn [idx i] (if (even? idx) i (reverse i)))
                                 p-items)]
     (stich-bin-slices bin-slices))))

(defn pack-n-bins-3
  ([items n] (pack-n-bins-3 items n (second (first items))))
  ([items n max-size]
   (let [bin-slices (->> (iterate (fn [[h t]]
                                    [(conj h (take n t)) (reverse (drop n t))])
                                  [[] items])
                         (drop-while (fn [[h t]] (not (nil? (first t)))))
                         ffirst)
         bin-slices (conj (drop-last bin-slices)
                          (concat (last bin-slices)
                                  (repeat (rem n (count items)) nil)))]
     (stich-bin-slices bin-slices))))

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
   (= (first-fit items)
      [{:size 9 :items [[:a 9] [:j 0]]}
       {:size 9 :items [[:b 8] [:i 1]]}
       {:size 9 :items [[:c 7] [:h 2]]}
       {:size 9 :items [[:d 6] [:g 3]]}
       {:size 9 :items [[:e 5] [:f 4]]}]))
  (assert
   (= (item-indices (first-fit items))
      {:bin-count 5
       :item-indices {:a 0 :b 1 :c 2 :d 3 :e 4
                      :f 4 :g 3 :h 2 :i 1 :j 0}})))
