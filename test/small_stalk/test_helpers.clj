(ns small-stalk.test-helpers
  (:require [clojure.test :refer :all]
            [small-stalk.system :as system]
            [integrant.core :as ig]
            [clojure.java.io :as io]
            [small-stalk.persistence.service :as p-service]))

(derive ::string-persistence-service :small-stalk.persistence.service/persistence-service)

(def test-config (-> system/config
                     (dissoc :small-stalk.persistence.service/persistence-service)
                     (assoc ::string-persistence-service "")))

(defmethod ig/init-key ::string-persistence-service
  [_ fake-string]
  (p-service/string-persistence-service fake-string))

(defn open-system!
  ([]
   (system/open-system! test-config))
  ([keys]
   (system/open-system! test-config keys)))

(defn open-system-with-aof-contents!
  [keys aof-contents]
  (system/open-system! (assoc test-config ::string-persistence-service aof-contents)
                       keys))

(defn clear-directory
  [path]
  (doall (map io/delete-file (.listFiles (io/file path)))))

(defn directory-contents
  [path]
  (set (map #(.getName %) (.listFiles (io/file path)))))

(defn make-directory
  [directory-path-without-trailing-slash]
  (io/make-parents (str directory-path-without-trailing-slash "/foo")))