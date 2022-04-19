(ns small-stalk.commands.handlers
  (:require [small-stalk.io :as ssio]
            [clojure.string :as string]
            [small-stalk.queues :as queues]))

(defmulti handle-command (fn [command _in-stream _out-stream] (:command-name command)))

(defmethod handle-command "put"
  [{:keys [priority]} input-stream output-stream]
  (let [data     (string/trim (ssio/read-string-until-crlf input-stream))
        job      (queues/insert-into-default priority data)]
    (ssio/write-crlf-string output-stream (str "INSERTED " (:id job)))))
