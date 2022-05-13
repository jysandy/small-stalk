(ns small-stalk.commands.handlers
  (:require [small-stalk.io :as ssio]
            [clojure.string :as string]
            [small-stalk.queue-service.job :as job]
            [small-stalk.queue-service.service :as queue-service]
            [integrant.core :as ig]
            [failjure.core :as f]))

(defmulti handle-command (fn [_queue-service _job-id-counter command _connection_id _in-stream _out-stream]
                           (:command-name command)))

(defmethod handle-command "put"
  [queue-service job-id-counter {:keys [priority]} _connection-id input-stream output-stream]
  (f/attempt-all [data (f/ok-> (ssio/read-string-until-crlf input-stream)
                               string/trim)
                  job  (queue-service/put queue-service
                                          (job/make-job job-id-counter priority data))]
    (ssio/write-crlf-string output-stream (str "INSERTED " (:id job)))))

(defmethod handle-command "reserve"
  [queue-service _job-id-counter _command connection-id _input-stream output-stream]
  (f/attempt-all [{:keys [id data] :as _reserved-job} (queue-service/reserve queue-service
                                                                             connection-id)
                  _ (ssio/write-crlf-string output-stream (str "RESERVED " id))]
    (ssio/write-crlf-string output-stream (str data))))

(defmethod handle-command "reserve-with-timeout"
  [queue-service _job-id-counter {:keys [timeout-secs]} connection-id _input-stream output-stream]
  (let [reserved-job-or-error (queue-service/reserve queue-service
                                                     connection-id
                                                     timeout-secs)]
    (cond
      (f/ok? reserved-job-or-error) (do (ssio/write-crlf-string output-stream
                                                                (str "RESERVED " (:id reserved-job-or-error)))
                                        (ssio/write-crlf-string output-stream
                                                                (str (:data reserved-job-or-error))))
      (= ::queue-service/reserve-waiting-timed-out
         (:type reserved-job-or-error)) (ssio/write-crlf-string output-stream "TIMED_OUT")
      :else reserved-job-or-error)))

(defmethod handle-command "peek-ready"
  [queue-service _job-id-counter _command _connection-id _input-stream output-stream]
  (if-let [job (queue-service/peek-ready queue-service)]
    (do (ssio/write-crlf-string output-stream (str "FOUND " (:id job)))
        (ssio/write-crlf-string output-stream (:data job)))
    (ssio/write-crlf-string output-stream "NOT_FOUND")))

(defmethod handle-command "delete"
  [queue-service _job-id-counter {:keys [job-id] :as _command} connection-id _input-stream output-stream]
  (let [deleted-job-or-error (queue-service/delete queue-service connection-id job-id)]
    (cond
      (f/ok? deleted-job-or-error) (ssio/write-crlf-string output-stream "DELETED")
      (= ::queue-service/job-not-found
         (:type deleted-job-or-error)) (ssio/write-crlf-string output-stream "NOT_FOUND")
      :else deleted-job-or-error)))

(defmethod handle-command "release"
  [queue-service _job-id-counter {:keys [job-id new-priority] :as _command} connection-id _input-stream output-stream]
  (let [released-job-or-error (queue-service/release queue-service connection-id job-id new-priority)]
    (cond
      (f/ok? released-job-or-error) (ssio/write-crlf-string output-stream "RELEASED")
      (= ::queue-service/job-not-found
         (:type released-job-or-error)) (ssio/write-crlf-string output-stream "NOT_FOUND")
      :else released-job-or-error)))

(defmethod ig/init-key ::command-handler
  [_ {:keys [queue-service job-id-counter]}]
  (partial handle-command queue-service job-id-counter))