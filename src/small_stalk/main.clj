(ns small-stalk.main
  (:require [small-stalk.system :as system]))

(defn- wait-forever [] @(promise))

(defn -main [& _args]
  (system/start-system!)
  (.addShutdownHook (Runtime/getRuntime)
                    (Thread. ^Runnable #(system/stop-system!)))
  (wait-forever))
