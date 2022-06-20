(ns small-stalk.queue-service.mutation-log
  (:require [small-stalk.persistence.append-only-log.filesystem :as aol-filesystem]
            [integrant.core :as ig]
            [small-stalk.persistence.append-only-log :as aol]))

(defmethod ig/init-key ::append-only-log
  [_ {:keys [file-path entries-per-file]}]
  (aol-filesystem/filesystem-append-only-log file-path entries-per-file))

(defmethod ig/halt-key! ::append-only-log
  [_ log]
  (aol/close log))

(defn write-to-log [append-only-log mutation]
  (aol/write-entry append-only-log (dissoc mutation :return-promise)))

(defn read-mutations-from-log [aof-file]
  (with-open [reader (aol/new-reader aof-file)]
    (doall (aol/entry-seq reader))))
