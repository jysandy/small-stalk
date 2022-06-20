(ns small-stalk.test-helpers
  (:require [clojure.test :refer :all]
            [small-stalk.system :as system]
            [small-stalk.persistence.append-only-log.string :as aol-string]
            [integrant.core :as ig]
            [small-stalk.persistence.append-only-log :as aol]))

(defn written-contents [append-only-log]
  (aol/entry-seq (aol/new-reader append-only-log)))

(derive ::string-append-only-log :small-stalk.queue-service.mutation-log/append-only-log)

(def test-config (-> system/config
                     (dissoc :small-stalk.queue-service.mutation-log/append-only-log)
                     (assoc ::string-append-only-log "")))

(defmethod ig/init-key ::string-append-only-log
  [_ fake-string]
  (aol-string/string-append-only-log fake-string))

(defn open-system!
  ([]
   (system/open-system! test-config))
  ([keys]
   (system/open-system! test-config keys)))

(defn open-system-with-aof-contents!
  [keys aof-contents]
  (system/open-system! (assoc test-config ::string-append-only-log aof-contents)
                       keys))