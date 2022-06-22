(ns small-stalk.commands.handlers
  (:require [small-stalk.io :as ssio]
            [clojure.string :as string]
            [small-stalk.queue-service.service :as queue-service]
            [integrant.core :as ig]
            [failjure.core :as f]))

(defmulti handle-command (fn [_queue-service command _connection_id _in-stream _out-stream]
                           (:command-name command)))

(defn- make-job [priority data time-to-run-secs]
  {:priority         priority
   :data             data
   :time-to-run-secs time-to-run-secs})

(defmethod handle-command "put"
  [queue-service {:keys [priority time-to-run-secs]} _connection-id input-stream output-stream]
  (f/attempt-all [data (f/ok-> (ssio/read-string-until-crlf input-stream)
                               string/trim)
                  job  (queue-service/put queue-service
                                          (make-job priority data time-to-run-secs))]
    (ssio/write-crlf-string output-stream (str "INSERTED " (:id job)))))

(defmethod handle-command "reserve"
  [queue-service _command connection-id _input-stream output-stream]
  (f/attempt-all [{:keys [id data] :as _reserved-job} (queue-service/reserve queue-service
                                                                             connection-id)
                  _ (ssio/write-crlf-string output-stream (str "RESERVED " id))]
    (ssio/write-crlf-string output-stream (str data))))

(defmethod handle-command "reserve-with-timeout"
  [queue-service {:keys [timeout-secs]} connection-id _input-stream output-stream]
  (let [reserved-job-or-error (queue-service/reserve queue-service
                                                     connection-id
                                                     timeout-secs)]
    (cond
      (f/ok? reserved-job-or-error) (do (ssio/write-crlf-string output-stream
                                                                (str "RESERVED " (:id reserved-job-or-error)))
                                        (ssio/write-crlf-string output-stream
                                                                (str (:data reserved-job-or-error))))
      (= :small-stalk.queue-service.state/reserve-waiting-timed-out
         (:type reserved-job-or-error)) (ssio/write-crlf-string output-stream "TIMED_OUT")
      :else reserved-job-or-error)))

(defmethod handle-command "peek-ready"
  [queue-service _command _connection-id _input-stream output-stream]
  (if-let [job (queue-service/peek-ready queue-service)]
    (do (ssio/write-crlf-string output-stream (str "FOUND " (:id job)))
        (ssio/write-crlf-string output-stream (:data job)))
    (ssio/write-crlf-string output-stream "NOT_FOUND")))

(defmethod handle-command "delete"
  [queue-service {:keys [job-id] :as _command} connection-id _input-stream output-stream]
  (let [deleted-job-or-error (queue-service/delete queue-service connection-id job-id)]
    (cond
      (f/ok? deleted-job-or-error) (ssio/write-crlf-string output-stream "DELETED")
      (= :small-stalk.queue-service.state/job-not-found
         (:type deleted-job-or-error)) (ssio/write-crlf-string output-stream "NOT_FOUND")
      :else deleted-job-or-error)))

(defmethod handle-command "release"
  [queue-service {:keys [job-id new-priority] :as _command} connection-id _input-stream output-stream]
  (let [released-job-or-error (queue-service/release queue-service connection-id job-id new-priority)]
    (cond
      (f/ok? released-job-or-error) (ssio/write-crlf-string output-stream "RELEASED")
      (= :small-stalk.queue-service.state/job-not-found
         (:type released-job-or-error)) (ssio/write-crlf-string output-stream "NOT_FOUND")
      :else released-job-or-error)))

(defmethod ig/init-key ::command-handler
  [_ {:keys [queue-service]}]
  (partial handle-command queue-service))