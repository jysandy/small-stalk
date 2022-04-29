(ns small-stalk.system
  (:refer-clojure :exclude [get])
  (:require [integrant.core :as ig])
  (:import (java.io Closeable)))

(def config
  {:small-stalk.queues/job-id-counter             -1
   :small-stalk.queues/default-queue              nil
   :small-stalk.commands.handlers/command-handler {:default-queue  (ig/ref :small-stalk.queues/default-queue)
                                                   :job-id-counter (ig/ref :small-stalk.queues/job-id-counter)}
   :small-stalk.server/connection-registry        nil
   :small-stalk.server/tcp-server                 {:port 6969}
   :small-stalk.server/acceptor-thread            {:connection-registry (ig/ref :small-stalk.server/connection-registry)
                                                   :command-handler     (ig/ref :small-stalk.commands.handlers/command-handler)
                                                   :tcp-server          (ig/ref :small-stalk.server/tcp-server)}})

(defonce system (atom nil))

(defn start-system! []
  (ig/load-namespaces config)
  (reset! system (ig/init config)))

(defn stop-system! []
  (ig/halt! @system)
  (reset! system nil))

(defrecord IntegrantSystem [system]
  Closeable
  (close [this] (ig/halt! (:system this))))

(defn open-system!
  "Opens and returns the system as a Closeable. It can be opened in with-open. Useful for tests."
  ([]
   (ig/load-namespaces config)
   (let [system (ig/init config)]
     (IntegrantSystem. system)))
  ([keys]
   (ig/load-namespaces config)
   (let [system (ig/init config keys)]
     (IntegrantSystem. system))))

(defn get [system key]
  (if (instance? IntegrantSystem system)
    (get-in system [:system key])
    (clojure.core/get system key)))