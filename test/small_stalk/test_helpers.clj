(ns small-stalk.test-helpers
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [small-stalk.system :as system]
            [integrant.core :as ig])
  (:import (java.io StringReader StringWriter BufferedReader)))

;; Masquerades as a java.io.File, but reading to / writing from it using strings.
(defrecord FakeFile [string-reader string-writer]
  io/IOFactory
  (make-writer [this _] (:string-writer this))
  (make-reader [this _] (:string-reader this)))

(defn make-fake-file [fake-content-string]
  (->FakeFile (BufferedReader. (StringReader. fake-content-string)) (StringWriter.)))

(defn written-contents [fake-aof-file]
  (.toString (:string-writer fake-aof-file)))

(derive ::fake-aof-file :small-stalk.queue-service.service/aof-file)

(def test-config (-> system/config
                     (dissoc :small-stalk.queue-service.service/aof-file)
                     (assoc ::fake-aof-file "")))

(defmethod ig/init-key ::fake-aof-file
  [_ fake-string]
  (make-fake-file fake-string))

(defn open-system!
  ([]
   (system/open-system! test-config))
  ([keys]
   (system/open-system! test-config keys)))

(defn open-system-with-aof-contents!
  [keys aof-contents]
  (system/open-system! (assoc test-config ::fake-aof-file aof-contents)
                       keys))