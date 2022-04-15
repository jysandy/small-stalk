(ns small-stalk.server
  (:require [small-stalk.threads :as vthreads]
            [clojure.string :as string]
            [small-stalk.io :as ssio])
  (:import (java.net ServerSocket SocketException)
           (java.io IOException)))

(defonce server (atom nil))

(defn start-server! []
  (reset! server (ServerSocket. 6969)))

(defn stop-server! []
  (when @server
    (.close @server)
    (reset! server nil)))

(defn- handle-message [input-stream output-stream message]
  (println "Received message: " message)
  (ssio/write-crlf-string output-stream message))

(defn command-processing-loop [input-stream output-stream]
  (loop []
    (let [message (ssio/read-string-until-crlf input-stream)]
      (if (string/blank? message)
        (do (println "Connection closed from foreign host!")
            nil)
        (do (handle-message input-stream output-stream (string/trim message))
            (recur))))))

(defn- handle-connection [socket]
  (vthreads/start-thread
    (fn []
      (try
        (with-open [input-stream  (.getInputStream socket)
                    output-stream (.getOutputStream socket)]
          (command-processing-loop input-stream output-stream))
        (catch SocketException _
          (println "Connection thread interrupted!"))
        (catch IOException _
          (println "Connection closed!"))))))

(defn start-accepting-connections []
  (vthreads/start-thread
    (fn []
      (try
        (loop []
          (let [new-socket (.accept @server)]
            (handle-connection new-socket)
            (recur)))
        (catch SocketException _
          ;; This means the thread was interrupted. https://download.java.net/java/early_access/loom/docs/api/java.base/java/net/ServerSocket.html#accept()
          (println "Stopping server")
          (stop-server!))))))

(defn restart-server-and-accept-connections! []
  (stop-server!)
  (start-server!)
  (start-accepting-connections))