(ns clojure-blog-search.core
  (:require [alida.crawl :as crawl]
            [alida.scrape :as scrape]
            [alida.lucene :as lucene]
            [alida.util :as util]
            [alida.db :as db]
            [net.cgrand.enlive-html :as enlive]))

(defn crawl-planet-sites [database]
  (db/add-batched-documents
   database
   @(crawl/directed-crawl
     "planet-crawl"
     (util/make-timestamp)
     0 ;; no need for a delay, crawling different hosts
     "http://planet.clojure.in/"
     [{:selector [[:section.sidebar-list (enlive/nth-of-type 4)] [:a]]}])))

(defn scrape-planet-clojure-links [database]
  (let [planet-page (db/get-page database
                                 "planet-crawl"
                                 "http://planet.clojure.in/")
        all-hrefs (map #(:href (:attrs %))
                       (enlive/select
                        (enlive/html-resource
                         (java.io.StringReader. (:body planet-page)))
                        [[:section.sidebar-list (enlive/nth-of-type 3)]
                         [:a]]))]
    (into #{}
          (filter #(and (string? %)
                        (not (re-matches #"http://pipes.yahoo.*" %)))
                  all-hrefs))))

(defn scrape-planet-lisp-links [database]
  (let [planet-page (db/get-page database
                                 "planet-crawl"
                                 "http://planet.lisp.org/")
        all-hrefs (map #(:href (:attrs %))
                       (enlive/select
                        (enlive/html-resource
                         (java.io.StringReader. (:body planet-page)))
                        [[:#sidebar]
                         [:ul  (enlive/nth-of-type 3)]
                         [:a]]))]
    (into #{}
          (filter #(not (re-matches #"(.*rss.*)|(.*atom.*)|(.*xml.*)" %))
                  all-hrefs))))

(defn scrape-planet-haskell-links [database]
  (let [planet-page (db/get-page database
                                 "planet-crawl"
                                 "http://planet.haskell.org/")
        all-hrefs (map #(:href (:attrs %))
                       (enlive/select
                        (enlive/html-resource
                         (java.io.StringReader. (:body planet-page)))
                        [[:.sidebar]
                         [:ul  (enlive/nth-of-type 1)]
                         [:li]
                         [:a (enlive/nth-of-type 1)]]))]
    (into #{} (filter not-empty all-hrefs))))

(defn page-scoring-fn [uri depth request]
  (if (scrape/content-type-is-html? request)
    (count (re-seq #"Clojure|clojure" (:body request)))
    -1))

(defn link-checker-fn [seed-uri current-uri]
  (= (:host (util/get-uri-segments seed-uri))
     (:host (util/get-uri-segments current-uri))))

(defn crawl-planet-clojure-blogs [crawl-timestamp database]
  (doseq [uri (scrape-planet-clojure-links database)]
    (crawl/weighted-crawl database
                          "planet-clojure-links-crawl"
                          crawl-timestamp
                          5000
                          uri
                          2
                          page-scoring-fn
                          (partial link-checker-fn uri))))

(defn scrape [crawl-tag crawl-timestamp database]
  (scrape/extract-and-store-data
   database
   crawl-tag
   crawl-timestamp
   (fn [raw-page]
     (let [title (scrape/get-trimmed-content (:body raw-page) [:title])]
       ;; some RSS pages still set a text/html content type,
       ;; so they slipped through the Content-Type check
       ;; during the scrape
       (when (string? title)
         {:uri (:uri raw-page)
          :title title
          :fulltext (scrape/html-to-plaintext (:body raw-page))})))))

(defn create-lucene-index [crawl-tag crawl-timestamp database index-filename]
  (let [analyzer (lucene/create-analyzer)
        dir (lucene/create-directory index-filename)
        fields-map {:title (lucene/create-field "title"
                                                ""
                                                :stored
                                                :indexed
                                                :tokenized)
                    :uri (lucene/create-field "uri"
                                              ""
                                              :stored
                                              :indexed)
                    :fulltext (lucene/create-field "fulltext"
                                                   ""
                                                   :indexed
                                                   :tokenized)}]
    (with-open [writer (lucene/create-index-writer analyzer dir :create)]
      (lucene/add-documents-to-index!
       writer
       fields-map
       ;; quick hack to get rid of bad (e.g. RSS) docs
       (filter #(string? (:title %))
               (:documents
                (db/get-scrape-results database
                                       crawl-tag
                                       crawl-timestamp
                                       10000)))))))