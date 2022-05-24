(ns small-stalk.server
  (:require [small-stalk.threads :as vthreads]
            [clojure.string :as string]
            [small-stalk.io :as ssio]
            [small-stalk.commands.parsing :as parsing]
            [failjure.core :as f]
            [integrant.core :as ig]
            [small-stalk.server.connection :as connection])
  (:import (java.net ServerSocket SocketException)
           (java.io IOException)))

(defn- handle-message [command-handler connection-id input-stream output-stream message]
  (println "Received message: " message)
  (f/attempt-all [tokens  (string/split message #" ")
                  command (parsing/parse-command tokens)]
    (f/try* (command-handler command connection-id input-stream output-stream))
    (f/when-failed [e]
      (cond
        (instance? Exception e)
        (do
          (println "Uncaught exception! " (ex-message e))
          (.printStackTrace e)
          (ssio/write-crlf-string output-stream "INTERNAL_ERROR"))

        (#{::ssio/output-stream-closed ::ssio/eof-reached} (:type e))
        nil

        (= {:type ::parsing/parser-failure
            :name ::parsing/unknown-command} (select-keys e [:type :name]))
        (ssio/write-crlf-string output-stream "UNKNOWN_COMMAND")

        (= ::parsing/parser-failure (:type e))
        (ssio/write-crlf-string output-stream "BAD_FORMAT")

        (= ::ssio/invalid-character (:type e))
        (ssio/write-crlf-string output-stream "BAD_FORMAT")

        :else
        (ssio/write-crlf-string output-stream "INTERNAL_ERROR")))))

(defn command-processing-loop [command-handler connection-id input-stream output-stream]
  (loop []
    (f/if-let-ok? [message (ssio/read-string-until-crlf input-stream)]
      (do (handle-message command-handler
                          connection-id
                          input-stream
                          output-stream
                          (string/trim message))
          (recur))
      (case (:type message)
        ::ssio/eof-reached (do (println "Connection closed from foreign host!")
                               nil)
        ::ssio/invalid-character (do (ssio/write-crlf-string output-stream "BAD_FORMAT")
                                     (recur))

        (do (ssio/write-crlf-string output-stream "INTERNAL_ERROR")
            (recur))))))

(defn- handle-connection [connection-registry command-handler socket]
  (vthreads/start-thread
    (fn []
      (let [connection-id (connection/register-connection! connection-registry socket)]
        (try
          (with-open [input-stream  (.getInputStream socket)
                      output-stream (.getOutputStream socket)]
            (command-processing-loop command-handler connection-id input-stream output-stream))
          (catch SocketException _
            (println "Connection thread interrupted!"))
          (catch IOException _
            (println "Connection closed!"))
          (finally
            (connection/remove-connection! connection-registry connection-id)))))))

(defn start-accepting-connections [connection-registry command-handler server-instance]
  (vthreads/start-thread
    (fn []
      (try
        (loop []
          (if (Thread/interrupted)
            (do (println "Stopping acceptor thread!")
                (connection/remove-all-connections! connection-registry))
            (let [new-socket (.accept server-instance)]
              (handle-connection connection-registry command-handler new-socket)
              (recur))))
        (catch SocketException _
          ;; This means the server instance was stopped. https://download.java.net/java/early_access/loom/docs/api/java.base/java/net/ServerSocket.html#accept()
          (println "Stopping acceptor thread!")
          (connection/remove-all-connections! connection-registry))))))

(defmethod ig/init-key ::acceptor-thread
  [_ {:keys [connection-registry tcp-server command-handler]}]
  (start-accepting-connections connection-registry command-handler tcp-server))

(defmethod ig/halt-key! ::acceptor-thread
  [_ acceptor-thread]
  (.interrupt acceptor-thread))

(defmethod ig/init-key ::tcp-server
  [_ {:keys [port]}]
  (ServerSocket. port))

(defmethod ig/halt-key! ::tcp-server
  [_ server]
  (println "Stopping server!")
  (.close server))