(ns small-stalk.queue-service.service
  "This is small-stalk's queueing system.
  Reads are served trivially. Mutations are enqueued and are processed serially by a mutation thread.
  Serializing mutations allows us to:
  1. ensure that clients waiting for reserves are given jobs fairly,
  2. write the contents of the mutation queue out to a redo log
  and so on."
  (:refer-clojure :exclude [peek])
  (:require [small-stalk.queue-service.priority-queue :as pqueue]
            [small-stalk.threads :as vthreads]
            [integrant.core :as ig])
  (:import (java.util.concurrent LinkedBlockingQueue BlockingQueue)
           (clojure.lang PersistentQueue)))

(comment
  {2  q1
   5  q2
   6  q3
   10 q4}

  ;; reserved jobs
  #{}

  ;; queued reserves
  [r1 r2]

  ;; BlockingQueue
  [{:command        :reserve
    :worker         2
    :return-promise (promise)}
   {:command        :put
    :priority       2
    :item           "foo"
    :return-promise (promise)}]
  ;; acceptor thread which processes these commands

  ;; need timers / timer threads to process timeouts and TTRs



  ;; service state
  {:state-atom     (atom {:pqueue           (pqueue/create)
                          :reserved-jobs    #{}
                          :waiting-reserves (PersistentQueue/EMPTY)})
   :mutation-queue (LinkedBlockingQueue.)}

  ;; mutations
  {:type           ::put
   :job            {:id       1
                    :priority 2
                    :data     "foo"}
   :return-promise (promise)}

  ;; WIP:
  {:type           ::reserve
   :job-id         nil
   :return-promise (promise)}

  )

;; Mutators
(defmulti process-mutation (fn [_state-atom mutation] (:type mutation)))

(defmethod process-mutation ::put
  [state-atom {:keys [job return-promise] :as _mutation}]
  (swap! state-atom update :pqueue pqueue/push (:priority job) job)
  (deliver return-promise job))

;; Initialization and mutation thread
(defn start-queue-service [{:keys [state-atom mutation-queue] :as _service}]
  (vthreads/start-thread
    (fn []
      (try
        (loop []
          (let [mutation (.take ^BlockingQueue mutation-queue)]
            (process-mutation state-atom mutation)
            (recur)))
        (catch InterruptedException _
          (println "Queue service mutation thread interrupted! Shutting it down!"))))))

(defmethod ig/init-key ::mutation-thread
  [_ {:keys [queue-service]}]
  (start-queue-service queue-service))

(defmethod ig/halt-key! ::mutation-thread
  [_ mutation-thread]
  (.interrupt mutation-thread))

(defmethod ig/init-key ::queue-service
  [_ _]
  {:state-atom     (atom {:pqueue (pqueue/create)})
   :mutation-queue (LinkedBlockingQueue.)})

;; API
(defn put [{:keys [mutation-queue] :as _service} job]
  (let [return-promise (promise)]
    (.put ^BlockingQueue mutation-queue {:type           ::put
                                         :job            job
                                         :return-promise return-promise})
    @return-promise))

(defn peek-ready [{:keys [state-atom] :as _service}]
  (pqueue/peek (:pqueue @state-atom)))