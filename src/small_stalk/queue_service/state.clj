(ns small-stalk.queue-service.state
  (:require [small-stalk.queue-service.priority-queue :as pqueue]
            [small-stalk.threads :as vthreads]
            [small-stalk.failure :as ssf]
            [medley.core :as medley]
            [small-stalk.queue-service.persistent-queue :as persistent-queue]
            [small-stalk.queue-service.mutation-log :as mutation-log]
            [clojure.edn :as edn]
            [small-stalk.persistence.append-only-log :as aol])
  (:import (java.util.concurrent BlockingQueue)
           (clojure.lang PersistentQueue)))

(defn new-state
  "Returns an atom seeded with the initial state."
  []
  (atom {
         :job-id-counter         -1
         ;; The priority queue.
         :pqueue                 (pqueue/create)
         ;; The set of jobs that were reserved, including a reserved-by key in each.
         :reserved-jobs          #{}
         ;; A queue of waiting reserve mutations.
         :waiting-reserves       (PersistentQueue/EMPTY)
         ;; A map of connection IDs to reserve timeout futures.
         :reserve-timeout-timers {}
         ;; A map of job IDs to TTR timeout futures.
         :time-to-run-timers     {}}))

(defmacro live-mode-only
  "Marks code that should not be run during replay mode."
  [replay-mode? & body]
  `(if (not ~replay-mode?)
     (do ~@body)
     nil))

(defn deliver-if-live [promise val replay-mode?]
  (live-mode-only replay-mode? (deliver promise val)))

(defn enqueue-mutation
  ([^BlockingQueue mutation-queue mutation]
   (enqueue-mutation mutation-queue mutation false))
  ([^BlockingQueue mutation-queue mutation replay-mode?]
   (live-mode-only replay-mode?
     (.put mutation-queue mutation))))

(defn- add-ready-job [state job]
  (update state :pqueue pqueue/push (:priority job) job))

(defn- next-waiting-reserve [{:keys [waiting-reserves] :as _state}]
  (clojure.core/peek waiting-reserves))

(defn- launch-time-to-run-timer [mutation-queue time-to-run-secs job-id replay-mode?]
  (live-mode-only replay-mode?
    (vthreads/schedule
      (* 1000 time-to-run-secs)
      (fn []
        (enqueue-mutation mutation-queue
                          {:type   ::time-to-run-expired
                           :job-id job-id})))))

(defn- cleanup-timers
  "Throw away the finished timers that we don't need anymore."
  [state]
  (-> state
      (update :reserve-timeout-timers (partial medley/remove-vals future-done?))
      (update :time-to-run-timers (partial medley/remove-vals future-done?))))

(defn- register-time-to-run-timer! [state-atom job-id time-to-run-future replay-mode?]
  (live-mode-only replay-mode?
    (swap! state-atom #(-> %
                           cleanup-timers
                           (update :time-to-run-timers assoc job-id time-to-run-future)))))

(defn- reserve-job! [state-atom mutation-queue {:keys [time-to-run-secs id] :as job} connection-id replay-mode?]
  (when (some? time-to-run-secs)
    (let [timer-future (launch-time-to-run-timer mutation-queue time-to-run-secs id replay-mode?)]
      (register-time-to-run-timer! state-atom id timer-future replay-mode?)))
  (swap! state-atom update :reserved-jobs conj (assoc job :reserved-by connection-id)))

(defn- cleanup-after-replay! [state-atom]
  (swap! state-atom (fn [{:keys [pqueue reserved-jobs] :as state}]
                      (-> state
                          ;; Dump all the reserved jobs back into the ready queue.
                          (assoc :pqueue (reduce (fn [pqueue job]
                                                   (pqueue/push pqueue
                                                                (:priority job)
                                                                (dissoc job :reserved-by)))
                                                 pqueue
                                                 reserved-jobs))
                          (assoc :reserved-jobs #{})
                          ;; Get rid of any waiting reserves.
                          (assoc :waiting-reserves (PersistentQueue/EMPTY))))))

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

(defn cancel-all-timers
  [state]
  (doseq [timer (vals (:reserve-timeout-timers state))]
    (future-cancel timer))
  (doseq [timer (vals (:time-to-run-timers state))]
    (future-cancel timer)))

(defn- register-reserve-timeout! [state-atom connection-id reserve-timeout-future replay-mode?]
  (live-mode-only replay-mode?
    (swap! state-atom #(-> %
                           cleanup-timers
                           (update :reserve-timeout-timers assoc connection-id reserve-timeout-future)))))

(defn- cancel-reserve-timer!
  [state-atom connection-id replay-mode?]
  (live-mode-only replay-mode?
    (when-let [reserve-timer (get (:reserve-timeout-timers @state-atom)
                                  connection-id)]
      (future-cancel reserve-timer)
      (swap! state-atom cleanup-timers))))

(defn- launch-reserve-timeout-timer [mutation-queue timeout-secs connection-id replay-mode?]
  (live-mode-only replay-mode?
    (vthreads/schedule
      (* 1000 timeout-secs)
      (fn []
        (enqueue-mutation mutation-queue
                          {:type          ::reserve-waiting-timed-out
                           :connection-id connection-id})))))

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
  [state-atom replay-mode?]
  (when-let [waiting-reserve (next-waiting-reserve @state-atom)]
    (swap! state-atom update :waiting-reserves pop)
    (cancel-reserve-timer! state-atom (:connection-id waiting-reserve) replay-mode?)
    waiting-reserve))

(defn- take-ready-job!
  "Removes and returns a ready job."
  [state-atom]
  (when-let [ready-job (pqueue/peek (:pqueue @state-atom))]
    (swap! state-atom update :pqueue pqueue/pop)
    ready-job))

(defn- put-ready-job! [state-atom mutation-queue job replay-mode?]
  (if-let [waiting-reserve (take-waiting-reserve! state-atom replay-mode?)]
    ;; put needs to immediately deliver the job to a waiting reserve, if present.
    (do (reserve-job! state-atom mutation-queue job (:connection-id waiting-reserve) replay-mode?)
        (deliver-if-live (:return-promise waiting-reserve) job replay-mode?))
    (swap! state-atom add-ready-job job)))

(defn- generate-job-id! [state-atom]
  (let [new-state (swap! state-atom #(update % :job-id-counter inc))]
    (:job-id-counter new-state)))

(defn- add-job-id! [job state-atom]
  (assoc job :id (generate-job-id! state-atom)))

;; Mutators

(defmulti process-mutation (fn [_q-service mutation _replay-mode?] (:type mutation)))

(defmethod process-mutation ::put
  [{:keys [state-atom mutation-queue]} {:keys [job-description return-promise] :as _mutation} replay-mode?]
  (let [job-with-id (add-job-id! job-description state-atom)]
    (put-ready-job! state-atom mutation-queue job-with-id replay-mode?)
    (deliver-if-live return-promise job-with-id replay-mode?)))

(defmethod process-mutation ::reserve
  [{:keys [state-atom mutation-queue]} {:keys [return-promise connection-id timeout-secs] :as mutation} replay-mode?]
  (let [ready-job (take-ready-job! state-atom)]
    (cond
      ;; If a job is ready, return it.
      (some? ready-job) (do (reserve-job! state-atom mutation-queue ready-job connection-id replay-mode?)
                            (deliver-if-live return-promise ready-job replay-mode?))
      (= 0 timeout-secs) (deliver-if-live return-promise (ssf/fail {:type ::reserve-waiting-timed-out}) replay-mode?)
      ;; When a timeout is present, we:
      ;; 1. enqueue the reserve
      ;; 2. launch and register a timer thread which publishes a timeout mutation after the timeout
      ;; 3. handle the timeout mutation by removing the waiting reserve and sending a timeout error
      ;;    to the client waiting on the reserve.
      ;; The connection ID is sufficient to identify the timed out reserve, because one client
      ;; cannot be blocked on two reserves at once.
      (and (some? timeout-secs)
           (< 0 timeout-secs)) (let [timeout-future (live-mode-only replay-mode?
                                                      (launch-reserve-timeout-timer mutation-queue
                                                                                    timeout-secs
                                                                                    connection-id
                                                                                    replay-mode?))]
                                 (swap! state-atom enqueue-reserve mutation)
                                 (register-reserve-timeout! state-atom connection-id timeout-future replay-mode?))
      ;; Just enqueue the reserve if no timeout is present.
      :else (swap! state-atom enqueue-reserve mutation))))

(defmethod process-mutation ::reserve-waiting-timed-out
  [{:keys [state-atom]} {:keys [connection-id]} replay-mode?]
  ;; remove the waiting reserve and send a timeout error on the return promise
  (when-let [timed-out-reserve (find-waiting-reserve @state-atom connection-id)]
    (swap! state-atom remove-waiting-reserve connection-id)
    (deliver-if-live (:return-promise timed-out-reserve)
                     (ssf/fail {:type ::reserve-waiting-timed-out})
                     replay-mode?)))

(defmethod process-mutation ::time-to-run-expired
  [{:keys [state-atom mutation-queue]} {:keys [job-id]} replay-mode?]
  ;; remove the job from the reserved set and put it back into the ready queue
  (when-let [reserved-job (find-reserved-job @state-atom job-id)]
    (swap! state-atom update :reserved-jobs disj reserved-job)
    (put-ready-job! state-atom mutation-queue (dissoc reserved-job :reserved-by) replay-mode?)))

(defmethod process-mutation ::delete
  [{:keys [state-atom]} {:keys [return-promise job-id connection-id]} replay-mode?]
  (let [reserved-job-to-delete (find-reserved-job @state-atom
                                                  job-id
                                                  connection-id)
        ready-job-to-delete    (find-ready-job @state-atom job-id)]
    (cond
      ;; TODO: Delete the TTR timer if present.
      (some? reserved-job-to-delete) (do (swap! state-atom
                                                update
                                                :reserved-jobs
                                                disj
                                                reserved-job-to-delete)
                                         (deliver-if-live return-promise
                                                          (dissoc reserved-job-to-delete
                                                                  :reserved-by)
                                                          replay-mode?))
      (some? ready-job-to-delete) (do (swap! state-atom
                                             update
                                             :pqueue
                                             (partial pqueue/delete-by
                                                      #(= job-id (:id %))))
                                      (deliver-if-live return-promise
                                                       (dissoc ready-job-to-delete
                                                               :reserved-by)
                                                       replay-mode?))
      :else (deliver-if-live return-promise (ssf/fail {:type ::job-not-found}) replay-mode?))))

(defmethod process-mutation ::release
  [{:keys [state-atom mutation-queue]} {:keys [connection-id job-id new-priority return-promise] :as _mutation} replay-mode?]
  (if-let [reserved-job (find-reserved-job @state-atom
                                           job-id
                                           connection-id)]
    (let [released-job (-> reserved-job
                           (dissoc :reserved-by)
                           (assoc :priority new-priority))]
      (put-ready-job! state-atom mutation-queue released-job replay-mode?)
      (swap! state-atom update :reserved-jobs disj reserved-job)
      (deliver-if-live return-promise released-job replay-mode?))
    (deliver-if-live return-promise (ssf/fail {:type ::job-not-found}) replay-mode?)))

(defn replay-mutations! [state-atom mutations]
  (doseq [mutation mutations]
    (process-mutation {:state-atom     state-atom
                       :mutation-queue nil}
                      mutation
                      true))
  (cleanup-after-replay! state-atom))

;; AOF persistence
(defn replay-from-aof! [state-atom append-only-reader]
  (replay-mutations! state-atom (aol/entry-seq append-only-reader)))

;; Dump persistence

(defn dump-state [state file-path]
  (spit file-path
        (-> state
            (select-keys [:job-id-counter
                          :pqueue
                          :reserved-jobs])
            (update :pqueue pqueue/to-seq)
            str)))

(defn load-dump [file-path]
  (let [state-atom (new-state)]
    (swap! state-atom merge (-> file-path
                                (slurp)
                                (edn/read-string)
                                (update :pqueue pqueue/from-seq)))
    (cleanup-after-replay! state-atom)
    state-atom))