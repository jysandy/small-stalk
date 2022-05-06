(ns small-stalk.server.connection
  (:require [integrant.core :as ig]))

(defmethod ig/init-key ::connection-registry
  [_ _]
  (atom {
         ;; Incrementing number to generate IDs for new connections.
         :connection-id-counter -1


         ;; connections is a map of connection IDs to sockets
         ;; {1 connection-socket}
         :connections           {}}))

(defn register-connection!
  "Registers a connection and returns its ID."
  [connection-registry socket]
  (:connection-id-counter
    (swap! connection-registry
           (fn [{:keys [connection-id-counter] :as registry}]
             (let [new-connection-id (inc connection-id-counter)]
               (-> registry
                   (update :connection-id-counter inc)
                   (update :connections assoc new-connection-id socket)))))))

(defn remove-connection! [connection-registry connection-id]
  (let [socket (get-in @connection-registry [:connections connection-id])]
    (when (and socket
               (not (.isClosed socket)))
      (.close socket))
    (swap! connection-registry update :connections dissoc connection-id)))

(defn remove-all-connections! [connection-registry]
  (let [[old-registry _] (swap-vals! connection-registry assoc :connections {})]
    (doseq [socket (vals (:connections old-registry))]
      (when (and socket
                 (not (.isClosed socket)))
        (.close socket)))))