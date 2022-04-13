(ns small-stalk.main
  (:require [small-stalk.threads :as vthreads]))

(defn test1 []
  (println "Starting 10000 virtual threads sleeping for 1 second each")
  (dotimes [_ 10000] (vthreads/future (Thread/sleep 10000)))
  (println "Waiting for 2 seconds")
  (Thread/sleep 2000))

(defn -main [& _args]
  (test1))
