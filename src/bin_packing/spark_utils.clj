(ns bin-packing.spark-utils
  (:require [sparkling.core :as spark]
            [sparkling.destructuring :as de])
  (:import org.apache.spark.api.java.JavaRDD
           org.apache.spark.api.java.JavaPairRDD)
  (:refer-clojure :exclude [map count group-by mapcat
                            reduce filter select-keys get]))

(defprotocol Transformers
  (xmap [coll f])
  (xmap-vals [coll f])
  (xgroup-by [coll f])
  (xmapcat [coll f])
  (xreduce [coll f])
  (xfilter [coll f])
  (xcount [coll])
  (xselect-keys [coll ks])
  (xto-map [coll])
  (xget [coll k d])
  (xcache [coll]))

(defn -map-vals [f coll]
  (clojure.core/map (fn [[k v]] [k (f v)]) coll))

(defn -smap-vals [f coll]
  (spark/map (fn [[k v]] [k (f v)]) coll))

(extend-protocol Transformers
  org.apache.spark.api.java.JavaRDD
  (xmap [coll f] (spark/map f coll))
  (xmap-vals [coll f] (-smap-vals f coll))
  (xgroup-by [coll f] (spark/group-by f coll))
  (xmapcat [coll f] (spark/flat-map f coll))
  (xreduce [coll f] (spark/reduce f coll))
  (xfilter [coll f] (spark/filter f coll))
  (xselect-keys [coll ks] (xfilter coll (fn [[k v]] ((into #{} ks) k))))
  (xto-map [coll] (into {} (spark/collect coll)))
  (xcount [coll] (spark/count coll))
  (xcache [coll] (spark/cache coll))
  org.apache.spark.api.java.JavaPairRDD
  (xmap [coll f] (spark/map (de/key-value-fn (fn [k v] (f [k v]))) coll))
  (xmap-vals [coll f] (spark/map-values f coll))
  (xgroup-by [coll f] (spark/group-by f coll))
  (xmapcat [coll f] (spark/flat-map (de/key-value-fn (fn [k v] (f [k v]))) coll))
  (xreduce [coll f] (spark/reduce f coll))
  (xfilter [coll f] (spark/filter (de/key-value-fn (fn [k v] (f [k v]))) coll))
  (xselect-keys [coll ks] (spark/filter (de/key-fn (into #{} ks)) coll))
  (xto-map [coll] (spark/collect-map coll))
  (xcount [coll] (spark/count coll))
  (xget [coll k d] (or (first (spark/lookup coll k)) d))
  (xcache [coll] (spark/cache coll))
  clojure.lang.IPersistentMap
  (xmap [coll f] (clojure.core/map f coll))
  (xmap-vals [coll f] (into {} (-map-vals f coll)))
  (xselect-keys [coll ks] (clojure.core/select-keys coll ks))
  (xto-map [coll] (into {} coll))
  (xcount [coll] (clojure.core/count coll))
  (xget [coll k d] (clojure.core/get coll k))
  (xcache [coll] coll)
  clojure.lang.IPersistentCollection
  (xmap [coll f] (clojure.core/map f coll))
  (xmap-vals [coll f] (-map-vals f coll))
  (xgroup-by [coll f] (clojure.core/group-by f coll))
  (xmapcat [coll f] (clojure.core/mapcat f coll))
  (xreduce [coll f] (clojure.core/reduce f coll))
  (xfilter [coll f] (clojure.core/filter f coll))
  (xselect-keys [coll ks] (clojure.core/select-keys coll ks))
  (xto-map [coll] (into {} coll))
  (xcount [coll] (clojure.core/count coll))
  (xget [coll k d] (or (first (clojure.core/filter #(= (first %) k) coll)) d))
  (xcache [coll] coll))

(defn map [f coll] (xmap coll f))
(defn map-vals [f coll] (xmap-vals coll f))
(defn group-by [f coll] (xgroup-by coll f))
(defn mapcat [f coll] (xmapcat coll f))
(defn reduce [f coll] (xreduce coll f))
(defn filter [f coll] (xfilter coll f))
(defn select-keys [coll ks] (xselect-keys coll ks))
(defn to-map [coll] (xto-map coll))
(defn count [coll] (xcount coll))
(defn get [coll k d] (xget coll k d))
(defn cache [coll] (xcache coll))
