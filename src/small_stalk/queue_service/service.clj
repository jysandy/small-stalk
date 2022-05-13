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
            [small-stalk.failure :as ssf]
            [medley.core :as medley]
            [small-stalk.queue-service.persistent-queue :as persistent-queue])
  (:import (java.util.concurrent LinkedBlockingQueue BlockingQueue)
           (clojure.lang PersistentQueue)))

;; Initial state
(defmethod ig/init-key ::queue-service
  [_ _]
  {:state-atom     (atom {
                          ;; The priority queue.
                          :pqueue                 (pqueue/create)
                          ;; The set of jobs that were reserved, including a reserved-by key in each.
                          :reserved-jobs          #{}
                          ;; A queue of waiting reserve mutations.
                          :waiting-reserves       (PersistentQueue/EMPTY)
                          ;; A map of connection IDs to reserve timeout futures.
                          :reserve-timeout-timers {}})
   :mutation-queue (LinkedBlockingQueue.)})

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

(defn- enqueue-mutation [^BlockingQueue mutation-queue mutation]
  (.put mutation-queue mutation))

(defn- cleanup-reserve-timers
  "Throw away the finished timers that we don't need anymore."
  [state]
  (update state :reserve-timeout-timers (partial medley/remove-vals future-done?)))

(defn- cancel-all-reserve-timers
  [state]
  (map future-cancel (vals (:reserve-timeout-timers state))))

(defn- register-reserve-timeout [state connection-id reserve-timeout-future]
  (-> state
      cleanup-reserve-timers
      (update :reserve-timeout-timers assoc connection-id reserve-timeout-future)))

(defn- cancel-reserve-timer
  [state connection-id]
  (when-let [reserve-timer (get (:reserve-timeout-timers state)
                                connection-id)]
    (future-cancel reserve-timer)))

(defn- launch-reserve-timeout-timer [mutation-queue timeout-secs connection-id]
  (vthreads/schedule
    (* 1000 timeout-secs)
    (fn []
      (enqueue-mutation mutation-queue
                        {:type          ::reserve-waiting-timed-out
                         :connection-id connection-id}))))

(defn- remove-waiting-reserve [state connection-id]
  (-> state
      (update
        :waiting-reserves
        (partial persistent-queue/remove-by #(= connection-id
                                                (:connection-id %))))
      cleanup-reserve-timers))

;; Mutators
(defn- find-waiting-reserve [state connection-id]
  (->> (:waiting-reserves state)
       (persistent-queue/find-by #(= connection-id
                                     (:connection-id %)))
       first))

(defmulti process-mutation (fn [_q-service mutation] (:type mutation)))

(defmethod process-mutation ::put
  [{:keys [state-atom]} {:keys [job return-promise] :as _mutation}]
  (let [[old-state] (swap-vals! state-atom put-job job)]
    ;; put needs to immediately deliver the job to a waiting reserve, if present.
    (when-let [waiting-reserve (next-waiting-reserve old-state)]
      (cancel-reserve-timer old-state (:connection-id waiting-reserve))
      (swap! state-atom cleanup-reserve-timers)
      (deliver (:return-promise waiting-reserve) job))
    (deliver return-promise job)))

(defmethod process-mutation ::reserve
  [{:keys [state-atom mutation-queue]} {:keys [return-promise connection-id timeout-secs] :as mutation}]
  (let [[old-state] (swap-vals! state-atom
                                (fn [state]
                                  (if-let [ready-job (pqueue/peek (:pqueue state))]
                                    (-> state
                                        (reserve-job ready-job connection-id)
                                        (update :pqueue pqueue/pop))
                                    (if (= 0 timeout-secs)
                                      state
                                      ;; if no job is ready, enqueue the reserve mutation
                                      ;; until a job is put into the queue.
                                      (enqueue-reserve state mutation)))))
        ready-job (pqueue/peek (:pqueue old-state))]
    (cond
      ;; If a job is ready, return it.
      (some? ready-job) (deliver return-promise ready-job)
      (= 0 timeout-secs) (deliver return-promise (ssf/fail {:type ::reserve-waiting-timed-out}))
      ;; When a timeout is present, we:
      ;; 1. launch and register a timer thread which publishes a timeout mutation after the timeout
      ;; 2. handle the timeout mutation by removing the waiting reserve and sending a timeout error
      ;;    to the client waiting on the reserve.
      ;; The connection ID is sufficient to identify the timed out reserve, because one client
      ;; cannot be blocked on two reserves at once.
      (and (some? timeout-secs)
           (< 0 timeout-secs)) (let [timeout-future (launch-reserve-timeout-timer mutation-queue
                                                                                  timeout-secs
                                                                                  connection-id)]
                                 (swap! state-atom register-reserve-timeout connection-id timeout-future))
      :else nil)))

(defmethod process-mutation ::reserve-waiting-timed-out
  [{:keys [state-atom]} {:keys [connection-id]}]
  ;; remove the waiting reserve and send a timeout error on the return promise
  (let [[old-state] (swap-vals! state-atom remove-waiting-reserve connection-id)]
    (when-let [timed-out-reserve (find-waiting-reserve old-state connection-id)]
      (deliver (:return-promise timed-out-reserve)
               (ssf/fail {:type ::reserve-waiting-timed-out})))))

(defmethod process-mutation ::delete
  [{:keys [state-atom]} {:keys [return-promise job-id connection-id]}]
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
  [{:keys [state-atom]} {:keys [connection-id job-id new-priority return-promise] :as _mutation}]
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

;; Mutation thread
(defn start-queue-service [{:keys [mutation-queue state-atom] :as service}]
  (vthreads/start-thread
    (fn []
      (try
        (loop []
          (if (.isInterrupted (Thread/currentThread))
            (println "Queue service mutation thread interrupted! Shutting it down!")
            (let [mutation (.take ^BlockingQueue mutation-queue)]
              (process-mutation service mutation)
              (recur))))
        (catch InterruptedException _
          (println "Queue service mutation thread interrupted! Shutting it down!"))
        (finally
          (cancel-all-reserve-timers @state-atom))))))

(defmethod ig/init-key ::mutation-thread
  [_ {:keys [queue-service]}]
  (start-queue-service queue-service))

(defmethod ig/halt-key! ::mutation-thread
  [_ mutation-thread]
  (.interrupt mutation-thread))

;; API
(defn put [{:keys [mutation-queue] :as _service} job]
  (let [return-promise (promise)]
    (enqueue-mutation mutation-queue {:type           ::put
                                      :job            job
                                      :return-promise return-promise})
    @return-promise))

(defn peek-ready [{:keys [state-atom] :as _service}]
  (pqueue/peek (:pqueue @state-atom)))

(defn reserve
  ([service connection-id]
   (reserve service connection-id nil))
  ([{:keys [mutation-queue] :as _service} connection-id timeout-secs]
   (let [return-promise (promise)
         mutation       {:type           ::reserve
                         :connection-id  connection-id
                         :return-promise return-promise}]
     (enqueue-mutation mutation-queue (if (and (some? timeout-secs)
                                               (<= 0 timeout-secs))
                                        (assoc mutation :timeout-secs timeout-secs)
                                        mutation))
     @return-promise)))

(defn delete [{:keys [mutation-queue] :as _service} connection-id job-id]
  (let [return-promise (promise)]
    (enqueue-mutation mutation-queue {:type           ::delete
                                      :connection-id  connection-id
                                      :job-id         job-id
                                      :return-promise return-promise})
    @return-promise))

(defn release [{:keys [mutation-queue] :as _service} connection-id job-id new-priority]
  (let [return-promise (promise)]
    (enqueue-mutation mutation-queue {:type           ::release
                                      :connection-id  connection-id
                                      :job-id         job-id
                                      :new-priority   new-priority
                                      :return-promise return-promise})
    @return-promise))