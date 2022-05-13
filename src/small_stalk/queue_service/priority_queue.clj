(ns small-stalk.queue-service.priority-queue
  "This is a persistent priority queue.
  Not the most efficient, but it gets the job done as long as you don't need a huge number of priorities.
  Who needs 100 priorities anyway?"
  (:refer-clojure :exclude [pop peek])
  (:require [medley.core :as medley]
            [small-stalk.queue-service.persistent-queue :as persistent-queue])
  (:import (clojure.lang PersistentQueue)))

(comment
  "The keys are priorities. The values are clojure.lang.PersistentQueues.
  Empty queues are not maintained as values. As soon as a queue becomes empty, it is removed."
  {2  q1
   5  q2
   6  q3
   10 q4})

(defn create
  "Creates a new persistent priority queue.
  Items will be ordered by priority first, and FIFO second.
  The first item is the item with the least priority. If there are multiple
  items with the same priority, they will be popped in FIFO order."
  [] {})

(defn push
  "Puts an item into a persistent priority queue."
  [queue priority item]
  (update queue
          priority
          (fnil conj (PersistentQueue/EMPTY))
          item))

(defn- first-priority [queue]
  (some->> queue
           (keys)
           (apply min)))

(defn pop
  "Returns a new persistent priority queue with the first item removed."
  [queue]
  (let [priority-key  (first-priority queue)
        updated-queue (update queue priority-key (fn [persistent-queue]
                                                   (if-not (clojure.core/peek persistent-queue)
                                                     persistent-queue
                                                     (clojure.core/pop persistent-queue))))]
    (if (empty? (get updated-queue priority-key))
      (dissoc updated-queue priority-key)
      updated-queue)))

(defn peek
  "Returns the first item of the persistent priority queue."
  [queue]
  (clojure.core/peek (get queue (first-priority queue))))

(defn- clear-empty-queues
  "Removes all keys mapped to empty persistent queues."
  [queue]
  (medley/remove-vals empty? queue))

(defn delete-by
  "Returns a new queue with items matching pred removed."
  [pred queue]
  (->> queue
       (medley/map-vals #(persistent-queue/remove-by pred %))
       clear-empty-queues))

(defn to-seq
  "Converts a persistent priority queue to a sequence of priority-value tuples.
  The tuples are ordered according to the priority queue's ordering rules."
  [queue]
  (->> queue
       (seq)
       (sort-by first)
       (mapcat (fn [[priority persistent-queue]]
                 (map (fn [item]
                        [priority item])
                      persistent-queue)))))

(defn find-by
  "Returns elements in the queue matching pred. Does not include their priorities,
  but elements will be in priority order."
  [pred queue]
  (->> queue
       to-seq
       (map second)
       (filter pred)))
