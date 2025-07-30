(ns small-stalk.persistence.service
  (:require [small-stalk.persistence.append-only-log.filesystem :as aol-filesystem]
            [small-stalk.persistence.append-only-log.string :as aol-string]
            [small-stalk.persistence.append-only-log :as aol]
            [small-stalk.queue-service.state :as state]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [small-stalk.threads :as vthreads])
  (:import (java.io IOException)))

(def dump-file-name "base.dump.edn")

(defn- file-exists?
  [file-path]
  (.exists (io/file file-path)))

(defprotocol PersistenceService
  (write-mutation [service mutation])
  (load-state-atom [service])
  (written-contents [service])
  (stop [service]))

(defn- write-to-log [log mutation]
  (aol/write-entry log (dissoc mutation :return-promise)))

(defn- log-contents [log]
  (with-open [reader (aol/new-reader log)]
    (doall (aol/entry-seq reader))))

(defn- load-state-atom-from-dump [directory-path]
  (if (file-exists? (str directory-path "/" dump-file-name))
    (state/load-dump (str directory-path "/" dump-file-name))
    (state/new-state)))

(defn compact [{:keys [append-only-log directory-path]}]
  (let [queue-state-atom (load-state-atom-from-dump directory-path)]
    (with-open [reader (aol-filesystem/new-inactive-file-reader append-only-log)]
      (state/replay-from-aof! queue-state-atom reader)
      (state/dump-state @queue-state-atom (str directory-path "/" dump-file-name)))
    ;; On Windows, the reader needs to be closed before
    ;; the inactive files can be deleted
    (aol-filesystem/delete-inactive-files append-only-log)))

(defrecord FSPersistenceService [directory-path entries-per-file append-only-log]
  PersistenceService
  (write-mutation [_ mutation]
    (write-to-log append-only-log mutation))
  (load-state-atom [_]
    (let [dump-file-path (str directory-path "/" dump-file-name)
          loaded-atom    (if (file-exists? dump-file-path)
                           (state/load-dump dump-file-path)
                           (state/new-state))]
      (with-open [reader (aol/new-reader append-only-log)]
        (state/replay-from-aof! loaded-atom reader))
      loaded-atom))
  (written-contents [_]
    (log-contents append-only-log))
  (stop [_]
    (.close append-only-log)))

(defn- start-compactor-thread [persistence-service]
  (vthreads/start-worker-thread
    "Compactor"
    (fn []
      (Thread/sleep 10000)
      (try
        (compact persistence-service)
        (catch IOException e
          (println e))))))

(defn fs-persistence-service [directory-path entries-per-file]
  (let [append-only-log (aol-filesystem/filesystem-append-only-log directory-path
                                                                   entries-per-file)]
    (map->FSPersistenceService {:directory-path   directory-path
                                :entries-per-file entries-per-file
                                :append-only-log  append-only-log})))

(defrecord StringPersistenceService [initial-contents append-only-log]
  PersistenceService
  (write-mutation [_ mutation]
    (write-to-log append-only-log mutation))
  (load-state-atom [_]
    (let [loaded-atom (state/new-state)]
      (with-open [reader (aol/new-reader append-only-log)]
        (state/replay-from-aof! loaded-atom reader))
      loaded-atom))
  (written-contents [_]
    (log-contents append-only-log))
  (stop [_]
    (.close append-only-log)))

(defn string-persistence-service [initial-contents]
  (let [append-only-log (aol-string/string-append-only-log initial-contents)]
    (map->StringPersistenceService {:initial-contents initial-contents
                                    :append-only-log  append-only-log})))

(defmethod ig/init-key ::persistence-service
  [_ {:keys [directory-path entries-per-file]}]
  (fs-persistence-service directory-path entries-per-file))

(defmethod ig/halt-key! ::persistence-service
  [_ service]
  (stop service))

(defmethod ig/init-key ::compactor-thread
  [_ {:keys [persistence-service]}]
  (start-compactor-thread persistence-service))

(defmethod ig/halt-key! ::compactor-thread
  [_ compactor-thread]
  (.interrupt compactor-thread))
