(ns small-stalk.commands.handlers
  (:require [small-stalk.io :as ssio]
            [clojure.string :as string]
            [small-stalk.queues :as queues]
            [integrant.core :as ig]
            [failjure.core :as f]))

(defmulti handle-command (fn [_queue _job-id-counter command _in-stream _out-stream] (:command-name command)))

(defmethod handle-command "put"
  [queue job-id-counter {:keys [priority]} input-stream output-stream]
  (f/attempt-all [data (f/ok-> (ssio/read-string-until-crlf input-stream)
                               string/trim)
                  job  (queues/insert queue job-id-counter priority data)]
    (ssio/write-crlf-string output-stream (str "INSERTED " (:id job)))
    (f/when-failed [e]
      (if (= ::ssio/eof-reached (:type e))
        nil
        e))))

(defmethod ig/init-key ::command-handler
  [_ {:keys [default-queue job-id-counter]}]
  (partial handle-command default-queue job-id-counter))