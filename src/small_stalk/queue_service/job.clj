(ns small-stalk.queue-service.job
  (:require [integrant.core :as ig]))

(defmethod ig/init-key ::job-id-counter
  [_ init-value]
  (atom init-value))

(defn generate-job-id [job-id-counter]
  (swap! job-id-counter inc))

(defn make-job [job-id-counter priority data]
  {:id       (generate-job-id job-id-counter)
   :priority priority
   :data     data})