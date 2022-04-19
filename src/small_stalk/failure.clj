(ns small-stalk.failure
  (:require [failjure.core :as failjure]))

(defrecord SimpleFailure []
  failjure/HasFailed
  (failed? [self] true)
  (message [self] nil))

(def fail "Constructs a failure out of any map."
  map->SimpleFailure)

(defn first-failure [coll]
  (reduce (fn [out v]
            (if (failjure/failed? v)
              (reduced v)
              (conj out v)))
          []
          coll))
