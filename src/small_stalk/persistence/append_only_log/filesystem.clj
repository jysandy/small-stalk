(ns small-stalk.persistence.append-only-log.filesystem
  (:require [small-stalk.persistence.append-only-log :as aol]
            [clojure.string :as string]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import (java.io File FilenameFilter Closeable IOException)))

(defn- join-path [prefix postfix]
  (str prefix "/" postfix))

(defn- aof-file-number [aof-file-name]
  (-> aof-file-name
      (string/split #"/")
      last
      (string/split #"\.")
      first
      (Integer/parseInt)))

(defn- element-after [x coll]
  (first (rest (drop-while #(not= x %) coll))))

(defn- sorted-aof-files [^String directory-path]
  (->> (.list (File. directory-path)
              (reify FilenameFilter
                (accept [_ _ filename]
                  (boolean (re-matches #".*\.aof\.edn$" filename)))))
       (vec)
       (sort-by aof-file-number)))

(defn- generate-next-file-name [aof-file-name]
  (str (inc (aof-file-number aof-file-name)) ".aof.edn"))

(defn- find-first-file-name [^String directory-path]
  (->> (sorted-aof-files directory-path)
       first))

(defn- find-last-file-name [^String directory-path]
  (->> (sorted-aof-files directory-path)
       last))

(defn- next-file-name [{:keys [directory-path current-file-name file-list] :as _aof-reader}]
  (if file-list
    (element-after @current-file-name file-list)
    (->> (sorted-aof-files directory-path)
         (element-after @current-file-name))))

(defn- open-next-file! [{:keys [directory-path current-reader current-file-name] :as aof-reader}]
  (let [following-file-name (next-file-name aof-reader)]
    (.close @current-reader)
    (reset! current-file-name following-file-name)
    (reset! current-reader (-> (join-path directory-path following-file-name)
                               (io/file)
                               (io/reader)))))

(defn- read-line-from-aof [{:keys [current-reader] :as aof-reader}]
  (let [next-line (try
                    (.readLine @current-reader)
                    (catch IOException _
                      ;; The reader is closed
                      nil))]
    (cond
      (some? next-line) (edn/read-string next-line)
      (nil? next-line) (when (next-file-name aof-reader)
                         (open-next-file! aof-reader)
                         (aol/read-entry aof-reader)))))

(defn- close-aof-reader [aof-reader]
  (.close @(:current-reader aof-reader)))

(defrecord AOFReader [directory-path current-file-name current-reader]
  aol/AppendOnlyLogReader
  (read-entry [reader] (read-line-from-aof reader))

  Closeable
  (close [reader] (close-aof-reader reader)))

(defn- aof-reader [directory-path]
  (when-let [initial-file-name (find-first-file-name directory-path)]
    (map->AOFReader
      {:directory-path    directory-path
       :current-file-name (atom initial-file-name)
       :current-reader    (atom (-> (join-path directory-path
                                               initial-file-name)
                                    (io/file)
                                    (io/reader)))})))

(def empty-reader (reify
                    aol/AppendOnlyLogReader
                    (read-entry [_] nil)
                    Closeable
                    (close [_] nil)))

(defn- aof-reader-of-inactive-files
  "Returns a reader which only reads from files that are not currently
  being written to. Effectively, these are all but the last file in the
  directory."
  [directory-path]
  (let [file-names        (sorted-aof-files directory-path)
        initial-file-name (first file-names)]
    (if (>= (count file-names) 2)
      (map->AOFReader
        {:directory-path    directory-path
         :file-list         (butlast file-names)
         :current-file-name (atom initial-file-name)
         :current-reader    (atom (-> (join-path directory-path
                                                 initial-file-name)
                                      (io/file)
                                      (io/reader)))})
      empty-reader)))

(defn- aof-writer
  "Creates an AOF writer. This and its associated functions are not thread-safe. Use from one thread only."
  [data-directory entry-limit-per-file]
  (let [last-file-name (or (find-last-file-name data-directory)
                           "1.aof.edn")
        last-file-path (io/file (join-path data-directory last-file-name))]
    {:data-directory       data-directory
     :entry-limit-per-file entry-limit-per-file
     :current-file-name    (atom last-file-name)
     :current-writer       (atom (io/writer last-file-path :append true))
     :entries-written      (atom (or (count (line-seq (io/reader last-file-path)))
                                     0))}))

(defn- close-aof-writer [aof-writer]
  (.close @(:current-writer aof-writer)))

(defn- current-file-full? [aof-writer]
  (>= @(:entries-written aof-writer) (:entry-limit-per-file aof-writer)))

(defn- rotate-writer!
  [{:keys [current-writer
           entries-written
           current-file-name
           data-directory]}]
  (.close @current-writer)
  (let [next-file-name (join-path data-directory (generate-next-file-name @current-file-name))]
    (reset! current-file-name next-file-name)
    (reset! current-writer (io/writer (io/file next-file-name)))
    (reset! entries-written 0)))

(defn- write-to-aof [aof-writer aof-entry]
  (if (current-file-full? aof-writer)
    (do (rotate-writer! aof-writer)
        (recur aof-writer aof-entry))
    (do (.write @(:current-writer aof-writer) (str aof-entry "\n"))
        (.flush @(:current-writer aof-writer))
        (swap! (:entries-written aof-writer) inc))))

(defrecord FilesystemAppendOnlyLog [directory-path directory-writer]
  aol/AppendOnlyLog
  (new-reader [_] (aof-reader directory-path))
  (write-entry [_ aof-entry] (write-to-aof directory-writer aof-entry))

  Closeable
  (close [_]
    (close-aof-writer directory-writer)))

(defn filesystem-append-only-log [directory-path entry-limit-per-file]
  (map->FilesystemAppendOnlyLog
    {:directory-path   directory-path
     :directory-writer (aof-writer directory-path entry-limit-per-file)}))

(defn new-inactive-file-reader [{:keys [directory-path] :as _fs-aol}]
  (aof-reader-of-inactive-files directory-path))

(defn delete-inactive-files [fs-aol]
  (let [inactive-file-names (butlast (sorted-aof-files (:directory-path fs-aol)))]
    (doseq [path (map (partial str (:directory-path fs-aol) "/") inactive-file-names)]
      (io/delete-file path))))
