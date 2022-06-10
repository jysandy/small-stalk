(ns small-stalk.persistence.append-only-log.string
  (:require [small-stalk.persistence.append-only-log :as aol]
            [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import (java.io StringWriter StringReader BufferedReader Closeable)))

(defrecord StringAppendOnlyLog [^StringWriter string-writer]
  aol/AppendOnlyLog
  (new-reader [_] (let [reader (io/reader (StringReader.
                                            (.toString string-writer)))]
                    (reify
                      aol/AppendOnlyLogReader
                      (read-entry [_]
                        (edn/read-string (.readLine ^BufferedReader reader)))

                      Closeable
                      (close [_] (.close reader)))))
  (write-entry [_ aof-entry] (.write string-writer (str aof-entry "\n")))

  Closeable
  (close [_] (.close string-writer)))

(defn string-append-only-log [initial-contents]
  (let [writer (StringWriter.)
        log (->StringAppendOnlyLog writer)]
    (doseq [entry initial-contents]
      (aol/write-entry log entry))
    log))
