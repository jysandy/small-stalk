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
            [small-stalk.failure :as ssf])
  (:import (java.util.concurrent LinkedBlockingQueue BlockingQueue)
           (clojure.lang PersistentQueue)))

(defn- add-ready-job [state job]
  (update state :pqueue pqueue/push (:priority job) job))

(defn- next-waiting-reserve [{:keys [waiting-reserves] :as _state}]
  (clojure.core/peek waiting-reserves))

(defn- reserve-job [state job connection-id]
  (update state :reserved-jobs conj (assoc job :reserved-by connection-id)))

(defn- put-job [state job]
  (if-let [waiting-reserve (next-waiting-reserve state)]
    (-> state
        (update :waiting-reserves pop)
        (reserve-job job (:connection-id waiting-reserve)))
    (add-ready-job state job)))

(defn- find-reserved-job [{:keys [reserved-jobs] :as _current-state} job-id connection-id]
  (->> reserved-jobs
       (filter (fn [{:keys [id reserved-by]}]
                 (and (= job-id id)
                      (= connection-id reserved-by))))
       first))

(defn- release-job [current-state connection-id job-id new-priority]
  (if-let [reserved-job (find-reserved-job current-state
                                           job-id
                                           connection-id)]
    (-> current-state
        (update :reserved-jobs disj reserved-job)
        (put-job (-> reserved-job
                     (dissoc :reserved-by)
                     (assoc :priority new-priority))))
    current-state))

(defn- enqueue-reserve [state reserve-mutation]
  (update state :waiting-reserves conj reserve-mutation))

(defn- find-ready-job [{:keys [pqueue] :as _current-state} job-id]
  (first (pqueue/find-by #(= job-id (:id %)) pqueue)))

;; Mutators
(defmulti process-mutation (fn [_state-atom mutation] (:type mutation)))

(defmethod process-mutation ::put
  [state-atom {:keys [job return-promise] :as _mutation}]
  (let [[old-state] (swap-vals! state-atom put-job job)]
    (when-let [waiting-reserve (next-waiting-reserve old-state)]
      (deliver (:return-promise waiting-reserve) job))
    (deliver return-promise job)))

(defmethod process-mutation ::reserve
  [state-atom {:keys [return-promise connection-id] :as mutation}]
  ;; swap-vals! is used to make sure that we read the reserved job correctly
  ;; after the swap, without race conditions and without needing a lock.
  ;; Yes, the mutation processor is single-threaded (for now). But it doesn't
  ;; hurt to be defensive.
  ;; We need to read it separately to deliver it because we can't put side effects
  ;; into a swap.
  (let [[old-state] (swap-vals! state-atom
                                (fn [state]
                                  (if-let [ready-job (pqueue/peek (:pqueue state))]
                                    (-> state
                                        (reserve-job ready-job connection-id)
                                        (update :pqueue pqueue/pop))
                                    (enqueue-reserve state mutation))))
        ready-job (pqueue/peek (:pqueue old-state))]
    (when ready-job
      (deliver return-promise ready-job))))

(defmethod process-mutation ::delete
  [state-atom {:keys [return-promise job-id connection-id]}]
  (let [[old-state] (swap-vals! state-atom
                                (fn [current-state]
                                  (let [reserved-job-to-delete (find-reserved-job current-state
                                                                                  job-id
                                                                                  connection-id)
                                        ready-job-to-delete    (find-ready-job current-state job-id)]
                                    (cond
                                      (some? reserved-job-to-delete) (update current-state
                                                                             :reserved-jobs
                                                                             disj
                                                                             reserved-job-to-delete)
                                      (some? ready-job-to-delete) (update current-state
                                                                          :pqueue
                                                                          (partial pqueue/delete-by
                                                                                   #(= job-id (:id %))))
                                      :else current-state))))]
    (if-let [job-to-delete (or (find-reserved-job old-state
                                                  job-id
                                                  connection-id)
                               (find-ready-job old-state job-id))]
      (deliver return-promise (dissoc job-to-delete :reserved-by))
      (deliver return-promise (ssf/fail {:type ::job-not-found})))))

(defmethod process-mutation ::release
  [state-atom {:keys [connection-id job-id new-priority return-promise] :as _mutation}]
  (let [[old-state] (swap-vals! state-atom release-job connection-id job-id new-priority)]
    (if-let [reserved-job (find-reserved-job old-state
                                             job-id
                                             connection-id)]
      (let [released-job (-> reserved-job
                             (dissoc :reserved-by)
                             (assoc :priority new-priority))]
        (do (when-let [waiting-reserve (next-waiting-reserve old-state)]
              (deliver (:return-promise waiting-reserve) released-job))
            (deliver return-promise released-job)))
      (deliver return-promise (ssf/fail {:type ::job-not-found})))))

;; Initialization and mutation thread
(defn start-queue-service [{:keys [state-atom mutation-queue] :as _service}]
  (vthreads/start-thread
    (fn []
      (try
        (loop []
          (if (.isInterrupted (Thread/currentThread))
            (println "Queue service mutation thread interrupted! Shutting it down!")
            (let [mutation (.take ^BlockingQueue mutation-queue)]
              (process-mutation state-atom mutation)
              (recur))))
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
  {:state-atom     (atom {:pqueue           (pqueue/create)
                          :reserved-jobs    #{}
                          :waiting-reserves (PersistentQueue/EMPTY)})
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

(defn reserve [{:keys [mutation-queue] :as _service} connection-id]
  (let [return-promise (promise)]
    (.put ^BlockingQueue mutation-queue {:type           ::reserve
                                         :connection-id  connection-id
                                         :return-promise return-promise})
    @return-promise))

(defn delete [{:keys [mutation-queue] :as _service} connection-id job-id]
  (let [return-promise (promise)]
    (.put ^BlockingQueue mutation-queue {:type           ::delete
                                         :connection-id  connection-id
                                         :job-id         job-id
                                         :return-promise return-promise})
    @return-promise))

(defn release [{:keys [mutation-queue] :as _service} connection-id job-id new-priority]
  (let [return-promise (promise)]
    (.put ^BlockingQueue mutation-queue {:type           ::release
                                         :connection-id  connection-id
                                         :job-id         job-id
                                         :new-priority   new-priority
                                         :return-promise return-promise})
    @return-promise))