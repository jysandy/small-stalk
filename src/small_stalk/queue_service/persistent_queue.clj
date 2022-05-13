(ns small-stalk.queue-service.persistent-queue
  (:import (clojure.lang PersistentQueue)))

(defn remove-by
  "Returns a new persistent queue with all items matching pred removed."
  [pred persistent-queue]
  (->> (seq persistent-queue)
       (remove pred)
       (into (PersistentQueue/EMPTY))))

(defn find-by
  [pred persistent-queue]
  (->> (seq persistent-queue)
       (filter pred)))
