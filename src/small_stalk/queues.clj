(ns small-stalk.queues
  (:refer-clojure :exclude [take peek])
  (:import (java.util.concurrent PriorityBlockingQueue)))

(def previous-job-id (atom -1))

(defn generate-job-id []
  (swap! previous-job-id inc))

(defn reset-job-id! [] (reset! previous-job-id -1))

(defn job-comparator [job1 job2]
  (compare (:priority job1) (:priority job2)))

(def default-queue (PriorityBlockingQueue. 1 job-comparator))

(defn put [^PriorityBlockingQueue queue item]
  (.put queue item))

(defn take [^PriorityBlockingQueue queue]
  (.take queue))

(defn peek [^PriorityBlockingQueue queue]
  (.peek queue))

(defn make-job [priority data]
  {:id       (generate-job-id)
   :priority priority
   :data     data})

(defn insert-into-default [priority data]
  (let [job (make-job priority data)]
    (put default-queue job)
    job))

(defn as-vector [^PriorityBlockingQueue queue]
  (vec (.toArray queue)))

(comment
  ;; Job:
  {:id       123
   :priority 2
   :data     "foobar"})

