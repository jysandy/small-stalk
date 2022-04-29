(ns small-stalk.queues
  (:refer-clojure :exclude [take peek])
  (:require [integrant.core :as ig])
  (:import (java.util.concurrent PriorityBlockingQueue)))

(defmethod ig/init-key ::job-id-counter
  [_ init-value]
  (atom init-value))

(defn generate-job-id [job-id-counter]
  (swap! job-id-counter inc))

(defn job-comparator [job1 job2]
  (compare (:priority job1) (:priority job2)))

(defmethod ig/init-key ::default-queue
  [_ _]
  (PriorityBlockingQueue. 1 job-comparator))

(defn put [^PriorityBlockingQueue queue item]
  (.put queue item))

(defn take [^PriorityBlockingQueue queue]
  (.take queue))

(defn peek [^PriorityBlockingQueue queue]
  (.peek queue))

(defn make-job [job-id-counter priority data]
  {:id       (generate-job-id job-id-counter)
   :priority priority
   :data     data})

(defn insert [^PriorityBlockingQueue queue job-id-counter priority data]
  (let [job (make-job job-id-counter priority data)]
    (put queue job)
    job))

(defn as-vector [^PriorityBlockingQueue queue]
  (vec (.toArray queue)))

(comment
  ;; Job:
  {:id       123
   :priority 2
   :data     "foobar"})

