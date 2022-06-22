(ns small-stalk.queue-service.service
  "This is small-stalk's queueing system.
  Reads are served trivially. Mutations are enqueued and are processed serially by a mutation thread.
  Serializing mutations allows us to:
  1. ensure that clients waiting for reserves are given jobs fairly,
  2. write the contents of the mutation queue out to a redo log
  and so on."
  (:require [small-stalk.queue-service.priority-queue :as pqueue]
            [small-stalk.threads :as vthreads]
            [integrant.core :as ig]
            [small-stalk.queue-service.state :as state]
            [small-stalk.queue-service.mutation-log :as mutation-log])
  (:import (java.util.concurrent LinkedBlockingQueue BlockingQueue)))

;; Mutation thread
(defn start-mutation-thread [state-atom mutation-queue append-only-log]
  (vthreads/start-thread
    (fn []
      (try
        (loop []
          (if (.isInterrupted (Thread/currentThread))
            (println "Queue service mutation thread interrupted! Shutting it down!")
            (let [mutation (.take ^BlockingQueue mutation-queue)]
              (mutation-log/write-to-log append-only-log mutation)
              (state/process-mutation {:state-atom     state-atom
                                       :mutation-queue mutation-queue}
                                      mutation
                                      false)
              (recur))))
        (catch InterruptedException _
          (println "Queue service mutation thread interrupted! Shutting it down!"))
        (finally
          (state/cancel-all-timers @state-atom))))))

;; Initial state
(defn start-queue-service [append-only-log]
  (let [state-atom     (state/new-state)
        mutation-queue (LinkedBlockingQueue.)]
    (state/replay-from-aof! state-atom append-only-log)
    {:state-atom
     state-atom
     :mutation-queue
     mutation-queue
     :mutation-thread
     (start-mutation-thread state-atom mutation-queue append-only-log)}))

(defmethod ig/init-key ::queue-service
  [_ {:keys [append-only-log]}]
  (start-queue-service append-only-log))

(defmethod ig/halt-key! ::queue-service
  [_ {:keys [mutation-thread]}]
  (.interrupt mutation-thread))

;; API
(defn put [{:keys [mutation-queue] :as _service} job-description]
  (let [return-promise (promise)]
    (state/enqueue-mutation mutation-queue {:type            ::state/put
                                            :job-description job-description
                                            :return-promise  return-promise})
    @return-promise))

(defn peek-ready [{:keys [state-atom] :as _service}]
  (pqueue/peek (:pqueue @state-atom)))

(defn reserve
  ([service connection-id]
   (reserve service connection-id nil))
  ([{:keys [mutation-queue] :as _service} connection-id timeout-secs]
   (let [return-promise (promise)
         mutation       {:type           ::state/reserve
                         :connection-id  connection-id
                         :return-promise return-promise}]
     (state/enqueue-mutation mutation-queue (if (and (some? timeout-secs)
                                                     (<= 0 timeout-secs))
                                              (assoc mutation :timeout-secs timeout-secs)
                                              mutation))
     @return-promise)))

(defn delete [{:keys [mutation-queue] :as _service} connection-id job-id]
  (let [return-promise (promise)]
    (state/enqueue-mutation mutation-queue {:type           ::state/delete
                                            :connection-id  connection-id
                                            :job-id         job-id
                                            :return-promise return-promise})
    @return-promise))

(defn release [{:keys [mutation-queue] :as _service} connection-id job-id new-priority]
  (let [return-promise (promise)]
    (state/enqueue-mutation mutation-queue {:type           ::state/release
                                            :connection-id  connection-id
                                            :job-id         job-id
                                            :new-priority   new-priority
                                            :return-promise return-promise})
    @return-promise))