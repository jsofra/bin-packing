(ns bin-packing.gutenberg
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [clj-http.client :as client]
            [net.cgrand.enlive-html :as html]))

(defn generate-ebook-url [id]
  (let [root "http://www.gutenberg.lib.md.us"
        exts [".txt" "-8.txt" "-0.txt"]]
    (first
     (for [ext exts
           :let [path     (if (< id 10)
                            "0"
                            (string/join "/" (butlast (str id))))
                 url-path (format "%s/%s/%d/%d%s" root path id id ext)
                 resp     (client/head url-path {:throw-exceptions false
                                                 :ignore-unknown-host? true})]
           :when (= (:status resp) 200)]
       (let [length (-> resp
                        (get-in [:headers "Content-Length"])
                        Integer/parseInt)]
         [url-path length])))))

(def text-start-markers #{"*END*THE SMALL PRINT",
                          "*** START OF THE PROJECT GUTENBERG",
                          "*** START OF THIS PROJECT GUTENBERG",
                          "This etext was prepared by",
                          "E-text prepared by",
                          "Produced by",
                          "Distributed Proofreading Team",
                          "Proofreading Team at http://www.pgdp.net",
                          "http://gallica.bnf.fr)",
                          "      http://archive.org/details/",
                          "http://www.pgdp.net",
                          "by The Internet Archive)",
                          "by The Internet Archive/Canadian Libraries",
                          "by The Internet Archive/American Libraries",
                          "public domain material from the Internet Archive",
                          "Internet Archive)",
                          "Internet Archive/Canadian Libraries",
                          "Internet Archive/American Libraries",
                          "material from the Google Print project",
                          "*END THE SMALL PRINT",
                          "***START OF THE PROJECT GUTENBERG",
                          "This etext was produced by",
                          "*** START OF THE COPYRIGHTED",
                          ;"The Project Gutenberg",
                          "http://gutenberg.spiegel.de/ erreichbar.",
                          "Project Runeberg publishes",
                          "Beginning of this Project Gutenberg",
                          "Project Gutenberg Online Distributed",
                          "Gutenberg Online Distributed",
                          "the Project Gutenberg Online Distributed",
                          "Project Gutenberg TEI",
                          "This eBook was prepared by",
                          "http://gutenberg2000.de erreichbar.",
                          "This Etext was prepared by",
                          "This Project Gutenberg Etext was prepared by",
                          "Gutenberg Distributed Proofreaders",
                          "Project Gutenberg Distributed Proofreaders",
                          "the Project Gutenberg Online Distributed Proofreading Team",
                          "**The Project Gutenberg",
                          "*SMALL PRINT!",
                          "More information about this book is at the top of this file.",
                          "tells you about restrictions in how the file may be used.",
                          "l'authorization à les utilizer pour preparer ce texte.",
                          "of the etext through OCR.",
                          "*****These eBooks Were Prepared By Thousands of Volunteers!*****",
                          "We need your donations more than ever!",
                          " *** START OF THIS PROJECT GUTENBERG",
                          "****     SMALL PRINT!",
                          "[\"Small Print\" V.",
                          "      (http://www.ibiblio.org/gutenberg/",
                          "and the Project Gutenberg Online Distributed Proofreading Team",
                          "Mary Meehan, and the Project Gutenberg Online Distributed Proofreading",
                          "                this Project Gutenberg edition."})

(def text-end-markers #{"*** END OF THE PROJECT GUTENBERG",
                        "*** END OF THIS PROJECT GUTENBERG",
                        "***END OF THE PROJECT GUTENBERG",
                        "End of the Project Gutenberg",
                        "End of The Project Gutenberg",
                        "Ende dieses Project Gutenberg",
                        "by Project Gutenberg",
                        "End of Project Gutenberg",
                        "End of this Project Gutenberg",
                        "Ende dieses Projekt Gutenberg",
                        "        ***END OF THE PROJECT GUTENBERG",
                        "*** END OF THE COPYRIGHTED",
                        "End of this is COPYRIGHTED",
                        "Ende dieses Etextes ",
                        "Ende dieses Project Gutenber",
                        "Ende diese Project Gutenberg",
                        "**This is a COPYRIGHTED Project Gutenberg Etext, Details Above**",
                        "Fin de Project Gutenberg",
                        "The Project Gutenberg Etext of ",
                        "Ce document fut presente en lecture",
                        "Ce document fut présenté en lecture",
                        "More information about this book is at the top of this file.",
                        "We need your donations more than ever!",
                        "END OF PROJECT GUTENBERG",
                        " End of the Project Gutenberg",
                        " *** END OF THIS PROJECT GUTENBERG"})


(defn generate-ebook-urls [ids]
  (->> (map generate-ebook-url ids)
       (filter identity)))

(defn bookshelf-ebooks [bookshelfs-ids]
  (for [[url ids] bookshelfs-ids
        :let [ids (filter identity ids)
              urls (generate-ebook-urls ids)]]
    [url {:ebooks (map vector ids (map first urls))
          :size (apply + (map second urls))}]))

(defn get-ebook-text [url]
  (with-open [rdr (io/reader url)]
    (apply str
           (interpose " "
                      (doall (->> (line-seq rdr)
                                  (drop-while #(not-any? (partial string/starts-with? %)
                                                         text-start-markers))
                                  rest
                                  (take-while #(not-any? (partial string/starts-with? %)
                                                         text-end-markers))))))))

(defn get-ebook-texts [ebooks-urls]
  (update ebooks-urls :ebooks
          #(map (fn [[id url]]
                  (println (str "DEBUG: Getting text of " id ", " url " ..."))
                  [id (get-ebook-text url)]) %)))


(def book-shelf-headers {"User-Agent"
                         "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36"})

(def gutenberg-base-url "https://www.gutenberg.org")
(def bookshelf-url-1 (io/as-url (str gutenberg-base-url "/wiki/Category:Bookshelf")))
(def bookshelf-url-2 (io/as-url (str gutenberg-base-url "/w/index.php?title=Category:Bookshelf&pagefrom=The+Girls+Own+Paper+%28Bookshelf%29#mw-pages")))

(defn get-a-elements [url-path]
  (-> url-path
      io/as-url
      (html/html-resource {:insecure? true})
      (html/select [:a])))

(defn get-attr [url-path attr]
  (map #(get-in % [:attrs attr]) (get-a-elements url-path)))

(defn get-hrefs [url-path] (get-attr url-path :href))
(defn get-titles [url-path] (get-attr url-path :title))


(defn get-titles-and-content [url-path]
  (map (fn [e] [(-> e :attrs :title) (-> e :content first)])
       (get-a-elements url-path)))

(def non-bookself-hrefs
  #{"/wiki/Category:DE_B%C3%BCcherregal"
    "/wiki/Category:FR_Genre"
    "/wiki/Category:IT_Biblioteca"
    "/wiki/Category:PT_Prateleira"
    "/wiki/Category:Categories"
    "/wiki/Category:Bookshelf"
    "/wiki/Special:Categories"
    "/wiki/Special:Search"
    "/wiki/Main_Page"})

(defn get-categories [hrefs]
  (->> hrefs
       (filter identity)
       (remove non-bookself-hrefs)
       (filter #(re-find #"^/wiki/Category" %))))

(defn get-bookshelves [hrefs]
  (->> hrefs
       (filter identity)
       (remove non-bookself-hrefs)
       (filter #(re-find #"^/wiki/.+\(Bookshelf\)" %))))

(defn get-ebook-ids-and-titles [bookshelf-url]
  (->> bookshelf-url
       get-titles-and-content
       (filter (fn [[t c]] (and t c)))
       (map (fn [[t c]] [(re-find #"ebook:(\d+)" t) c]))
       (filter (comp first identity))
       (map (fn [[t c]] [(last t) c]))
       (map (fn [[id c]] [(Integer/parseInt id) c]))))

(defn get-ebook-ids [bookshelf-url]
  (->> bookshelf-url
       get-titles
       (filter identity)
       (map #(re-find #"ebook:(\d+)" %))
       (filter identity)
       (map last)
       (map #(Integer/parseInt %))))


(defn get-urls [url-filter base-url bookshelf-url]
  (let [hrefs (get-hrefs bookshelf-url)]
    (map (partial str base-url)
         (url-filter hrefs))))

(def get-category-urls (partial get-urls get-categories))
(def get-bookshelf-urls (partial get-urls get-bookshelves))

(defn get-bookshelf-ebook-ids-and-titles [urls]
  (->> (for [url urls]
         [url (get-ebook-ids-and-titles url)])
       (filter #(seq (second %)))))

(defn get-bookshelf-ebook-ids [urls]
  (->> (for [url urls]
         [url (get-ebook-ids url)])
       (filter #(seq (second %)))))

(defn get-all-bookshelf-urls! []
  (concat (get-bookshelf-urls gutenberg-base-url bookshelf-url-1)
          (get-bookshelf-urls gutenberg-base-url bookshelf-url-2)))

(defn get-bookself-ids-and-titles! []
  (-> (get-all-bookshelf-urls!)
      get-bookshelf-ebook-ids-and-titles))

(defn get-bookself-ids! []
  (-> (get-all-bookshelf-urls!)
      get-bookshelf-ebook-ids))
