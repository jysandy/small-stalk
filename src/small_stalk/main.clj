(ns small-stalk.main
  (:require [small-stalk.server :as server]))

(defn -main [& _args]
  (server/start-server!)
  (.join (server/start-accepting-connections)))
