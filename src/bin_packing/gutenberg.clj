(ns bin-packing.gutenberg
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [clj-http.client :as client]
            [net.cgrand.enlive-html :as html]))

(defn generate-ebook-urls [ids]
  (let [root "http://www.gutenberg.lib.md.us"
        exts [".txt" "-8.txt" "-0.txt"]]
    (for [id ids, ext exts
          :let [path     (string/join "/" (butlast (str id)))
                url-path (format "%s/%s/%d/%d%s" root path id id ext)
                url      (io/as-url url-path)
                headers  (.getHeaderFields (.openConnection url))
                status   (first (get headers nil))
                length   (Integer/parseInt (first (get headers "Content-Length")))]
          :when (= "HTTP/1.1 200 OK" (first (get headers nil)))]
      [url-path length])))
