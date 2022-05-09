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

(defmethod handle-command "peek-ready"
  [queue-service _job-id-counter _command _connection-id _input-stream output-stream]
  (if-let [job (queue-service/peek-ready queue-service)]
    (do (ssio/write-crlf-string output-stream (str "FOUND " (:id job)))
        (ssio/write-crlf-string output-stream (:data job)))
    (ssio/write-crlf-string output-stream "NOT_FOUND")))

(defmethod ig/init-key ::command-handler
  [_ {:keys [queue-service job-id-counter]}]
  (partial handle-command queue-service job-id-counter))