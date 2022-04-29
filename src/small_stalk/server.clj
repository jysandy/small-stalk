(ns small-stalk.server
  (:require [small-stalk.threads :as vthreads]
            [clojure.string :as string]
            [small-stalk.io :as ssio]
            [small-stalk.commands.parsing :as parsing]
            [failjure.core :as f]
            [integrant.core :as ig])
  (:import (java.net ServerSocket SocketException)
           (java.io IOException)))

(defn- handle-message [command-handler input-stream output-stream message]
  (println "Received message: " message)
  (f/attempt-all [tokens  (string/split message #" ")
                  command (parsing/parse-command tokens)]
    (f/try* (command-handler command input-stream output-stream))
    (f/when-failed [e]
      (cond
        (instance? Exception e)
        (ssio/write-crlf-string output-stream "INTERNAL_ERROR")

        (= ::ssio/output-stream-closed (:type e))
        nil

        (= {:type ::parsing/parser-failure
            :name ::parsing/unknown-command} (select-keys e [:type :name]))
        (ssio/write-crlf-string output-stream "UNKNOWN_COMMAND")

        (= ::parsing/parser-failure (:type e))
        (ssio/write-crlf-string output-stream "BAD_FORMAT")

        :else
        (ssio/write-crlf-string output-stream "INTERNAL_ERROR")))))

(defn command-processing-loop [command-handler input-stream output-stream]
  (loop []
    (let [message (ssio/read-string-until-crlf input-stream)]
      (if (f/failed? message)
        (do (println "Connection closed from foreign host!")
            nil)
        (do (handle-message command-handler input-stream output-stream (string/trim message))
            (recur))))))

(defmethod ig/init-key ::connection-registry
  [_ _]
  (atom #{}))

(defn- register-socket! [connection-registry socket]
  (swap! connection-registry conj socket))

(defn- remove-socket! [connection-registry socket]
  (when-not (.isClosed socket)
    (.close socket))
  (swap! connection-registry disj socket))

(defn- remove-all-sockets! [connection-registry]
  (doseq [socket @connection-registry]
    (remove-socket! connection-registry socket)))

(defn- handle-connection [connection-registry command-handler socket]
  (vthreads/start-thread
    (fn []
      (try
        (register-socket! connection-registry socket)
        (with-open [input-stream  (.getInputStream socket)
                    output-stream (.getOutputStream socket)]
          (command-processing-loop command-handler input-stream output-stream))
        (catch SocketException _
          (println "Connection thread interrupted!"))
        (catch IOException _
          (println "Connection closed!"))
        (finally
          (remove-socket! connection-registry socket))))))

(defn start-accepting-connections [connection-registry command-handler server-instance]
  (vthreads/start-thread
    (fn []
      (try
        (loop []
          (if (Thread/interrupted)
            (do (println "Stopping acceptor thread!")
                (remove-all-sockets! connection-registry))
            (let [new-socket (.accept server-instance)]
              (handle-connection connection-registry command-handler new-socket)
              (recur))))
        (catch SocketException _
          ;; This means the server instance was stopped. https://download.java.net/java/early_access/loom/docs/api/java.base/java/net/ServerSocket.html#accept()
          (println "Stopping acceptor thread!")
          (remove-all-sockets! connection-registry))))))

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