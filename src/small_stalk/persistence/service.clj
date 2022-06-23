(ns small-stalk.persistence.service
  (:require [small-stalk.persistence.append-only-log.filesystem :as aol-filesystem]
            [small-stalk.persistence.append-only-log.string :as aol-string]
            [small-stalk.persistence.append-only-log :as aol]
            [small-stalk.queue-service.state :as state]
            [clojure.java.io :as io]
            [integrant.core :as ig]))

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

(defn fs-persistence-service [directory-path entries-per-file]
  (let [append-only-log (aol-filesystem/filesystem-append-only-log directory-path
                                                                   entries-per-file)]
    (reify PersistenceService
      (write-mutation [_ mutation]
        (write-to-log append-only-log mutation))

      (load-state-atom [_]
        (let [loaded-atom (if (file-exists? (str directory-path "/" dump-file-name))
                            (state/load-dump directory-path)
                            (state/new-state))]
          (with-open [reader (aol/new-reader append-only-log)]
            (state/replay-from-aof! loaded-atom reader))
          loaded-atom))

      (written-contents [_]
        (log-contents append-only-log))

      (stop [_]
        (.close append-only-log)))))

(defn string-persistence-service [initial-contents]
  (let [append-only-log (aol-string/string-append-only-log initial-contents)]
    (reify PersistenceService
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
        (.close append-only-log)))))

(defmethod ig/init-key ::persistence-service
  [_ {:keys [directory-path entries-per-file]}]
  (fs-persistence-service directory-path entries-per-file))

(defmethod ig/halt-key! ::persistence-service
  [_ service]
  (stop service))
