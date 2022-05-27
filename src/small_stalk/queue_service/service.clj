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

(defn- enqueue-mutation [^BlockingQueue mutation-queue mutation]
  (.put mutation-queue mutation))

(defn- add-ready-job [state job]
  (update state :pqueue pqueue/push (:priority job) job))

(defn- next-waiting-reserve [{:keys [waiting-reserves] :as _state}]
  (clojure.core/peek waiting-reserves))

(defn- launch-time-to-run-timer [mutation-queue time-to-run-secs job-id]
  (vthreads/schedule
    (* 1000 time-to-run-secs)
    (fn []
      (enqueue-mutation mutation-queue
                        {:type   ::time-to-run-expired
                         :job-id job-id}))))

(defn- cleanup-timers
  "Throw away the finished timers that we don't need anymore."
  [state]
  (-> state
      (update :reserve-timeout-timers (partial medley/remove-vals future-done?))
      (update :time-to-run-timers (partial medley/remove-vals future-done?))))

(defn- register-time-to-run-timer [state job-id time-to-run-future]
  (-> state
      cleanup-timers
      (update :time-to-run-timers assoc job-id time-to-run-future)))

(defn- reserve-job! [state-atom mutation-queue {:keys [time-to-run-secs id] :as job} connection-id]
  (when (some? time-to-run-secs)
    (let [timer-future (launch-time-to-run-timer mutation-queue time-to-run-secs id)]
      (swap! state-atom register-time-to-run-timer id timer-future)))
  (swap! state-atom update :reserved-jobs conj (assoc job :reserved-by connection-id)))

(defn- find-reserved-job
  ([current-state job-id]
   (find-reserved-job current-state job-id nil))
  ([{:keys [reserved-jobs] :as _current-state} job-id connection-id]
   (->> reserved-jobs
        (filter (fn [{:keys [id reserved-by]}]
                  (if connection-id
                    (and (= job-id id)
                         (= connection-id reserved-by))
                    (= job-id id))))
        first)))

(defn- enqueue-reserve [state reserve-mutation]
  (update state :waiting-reserves conj reserve-mutation))

(defn- find-ready-job [{:keys [pqueue] :as _current-state} job-id]
  (first (pqueue/find-by #(= job-id (:id %)) pqueue)))

(defn- cancel-all-timers
  [state]
  (doseq [timer (vals (:reserve-timeout-timers state))]
    (future-cancel timer))
  (doseq [timer (vals (:time-to-run-timers state))]
    (future-cancel timer)))

(defn- register-reserve-timeout [state connection-id reserve-timeout-future]
  (-> state
      cleanup-timers
      (update :reserve-timeout-timers assoc connection-id reserve-timeout-future)))

(defn- cancel-reserve-timer!
  [state-atom connection-id]
  (when-let [reserve-timer (get (:reserve-timeout-timers @state-atom)
                                connection-id)]
    (future-cancel reserve-timer)
    (swap! state-atom cleanup-timers)))

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
      cleanup-timers))

(defn- find-waiting-reserve [state connection-id]
  (->> (:waiting-reserves state)
       (persistent-queue/find-by #(= connection-id
                                     (:connection-id %)))
       first))

(defn- take-waiting-reserve!
  "Removes and returns a waiting reserve."
  [state-atom]
  (when-let [waiting-reserve (next-waiting-reserve @state-atom)]
    (swap! state-atom update :waiting-reserves pop)
    (cancel-reserve-timer! state-atom (:connection-id waiting-reserve))
    waiting-reserve))

(defn- take-ready-job!
  "Removes and returns a ready job."
  [state-atom]
  (when-let [ready-job (pqueue/peek (:pqueue @state-atom))]
    (swap! state-atom update :pqueue pqueue/pop)
    ready-job))

(defn- put-ready-job! [state-atom mutation-queue job]
  (if-let [waiting-reserve (take-waiting-reserve! state-atom)]
    ;; put needs to immediately deliver the job to a waiting reserve, if present.
    (do (reserve-job! state-atom mutation-queue job (:connection-id waiting-reserve))
        (deliver (:return-promise waiting-reserve) job))
    (swap! state-atom add-ready-job job)))

;; Mutators

(defmulti process-mutation (fn [_q-service mutation] (:type mutation)))

(defmethod process-mutation ::put
  [{:keys [state-atom mutation-queue]} {:keys [job return-promise] :as _mutation}]
  (put-ready-job! state-atom mutation-queue job)
  (deliver return-promise job))

(defmethod process-mutation ::reserve
  [{:keys [state-atom mutation-queue]} {:keys [return-promise connection-id timeout-secs] :as mutation}]
  (let [ready-job (take-ready-job! state-atom)]
    (cond
      ;; If a job is ready, return it.
      (some? ready-job) (do (reserve-job! state-atom mutation-queue ready-job connection-id)
                            (deliver return-promise ready-job))
      (= 0 timeout-secs) (deliver return-promise (ssf/fail {:type ::reserve-waiting-timed-out}))
      ;; When a timeout is present, we:
      ;; 1. enqueue the reserve
      ;; 2. launch and register a timer thread which publishes a timeout mutation after the timeout
      ;; 3. handle the timeout mutation by removing the waiting reserve and sending a timeout error
      ;;    to the client waiting on the reserve.
      ;; The connection ID is sufficient to identify the timed out reserve, because one client
      ;; cannot be blocked on two reserves at once.
      (and (some? timeout-secs)
           (< 0 timeout-secs)) (let [timeout-future (launch-reserve-timeout-timer mutation-queue
                                                                                  timeout-secs
                                                                                  connection-id)]
                                 (swap! state-atom #(-> %
                                                        (enqueue-reserve mutation)
                                                        (register-reserve-timeout connection-id timeout-future))))
      ;; Just enqueue the reserve if no timeout is present.
      :else (swap! state-atom enqueue-reserve mutation))))

(defmethod process-mutation ::reserve-waiting-timed-out
  [{:keys [state-atom]} {:keys [connection-id]}]
  ;; remove the waiting reserve and send a timeout error on the return promise
  (when-let [timed-out-reserve (find-waiting-reserve @state-atom connection-id)]
    (swap! state-atom remove-waiting-reserve connection-id)
    (deliver (:return-promise timed-out-reserve)
             (ssf/fail {:type ::reserve-waiting-timed-out}))))

(defmethod process-mutation ::time-to-run-expired
  [{:keys [state-atom mutation-queue]} {:keys [job-id]}]
  ;; remove the job from the reserved set and put it back into the ready queue
  (when-let [reserved-job (find-reserved-job @state-atom job-id)]
    (swap! state-atom update :reserved-jobs disj reserved-job)
    (put-ready-job! state-atom mutation-queue (dissoc reserved-job :reserved-by))))

(defmethod process-mutation ::delete
  [{:keys [state-atom]} {:keys [return-promise job-id connection-id]}]
  (let [reserved-job-to-delete (find-reserved-job @state-atom
                                                  job-id
                                                  connection-id)
        ready-job-to-delete    (find-ready-job @state-atom job-id)]
    (cond
      (some? reserved-job-to-delete) (do (swap! state-atom
                                                update
                                                :reserved-jobs
                                                disj
                                                reserved-job-to-delete)
                                         (deliver return-promise
                                                  (dissoc reserved-job-to-delete
                                                          :reserved-by)))
      (some? ready-job-to-delete) (do (swap! state-atom
                                             update
                                             :pqueue
                                             (partial pqueue/delete-by
                                                      #(= job-id (:id %))))
                                      (deliver return-promise
                                               (dissoc ready-job-to-delete
                                                       :reserved-by)))
      :else (deliver return-promise (ssf/fail {:type ::job-not-found})))))

(defmethod process-mutation ::release
  [{:keys [state-atom mutation-queue]} {:keys [connection-id job-id new-priority return-promise] :as _mutation}]
  (if-let [reserved-job (find-reserved-job @state-atom
                                           job-id
                                           connection-id)]
    (let [released-job (-> reserved-job
                           (dissoc :reserved-by)
                           (assoc :priority new-priority))]
      (put-ready-job! state-atom mutation-queue released-job)
      (swap! state-atom update :reserved-jobs disj reserved-job)
      (deliver return-promise released-job))
    (deliver return-promise (ssf/fail {:type ::job-not-found}))))

;; Mutation thread
(defn start-queue-service []
  (let [state-atom     (atom {
                              ;; The priority queue.
                              :pqueue                 (pqueue/create)
                              ;; The set of jobs that were reserved, including a reserved-by key in each.
                              :reserved-jobs          #{}
                              ;; A queue of waiting reserve mutations.
                              :waiting-reserves       (PersistentQueue/EMPTY)
                              ;; A map of connection IDs to reserve timeout futures.
                              :reserve-timeout-timers {}
                              ;; A map of job IDs to TTR timeout futures.
                              :time-to-run-timers     {}})
        mutation-queue (LinkedBlockingQueue.)]
    {:state-atom
     state-atom
     :mutation-queue
     mutation-queue
     :mutation-thread
     (vthreads/start-thread
       (fn []
         (try
           (loop []
             (if (.isInterrupted (Thread/currentThread))
               (println "Queue service mutation thread interrupted! Shutting it down!")
               (let [mutation (.take ^BlockingQueue mutation-queue)]
                 (process-mutation {:state-atom     state-atom
                                    :mutation-queue mutation-queue}
                                   mutation)
                 (recur))))
           (catch InterruptedException _
             (println "Queue service mutation thread interrupted! Shutting it down!"))
           (finally
             (cancel-all-timers @state-atom)))))}))

;; Initial state
(defmethod ig/init-key ::queue-service
  [_ _]
  (start-queue-service))

(defmethod ig/halt-key! ::queue-service
  [_ {:keys [mutation-thread]}]
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