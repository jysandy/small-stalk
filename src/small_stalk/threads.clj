(ns small-stalk.threads
  (:refer-clojure :exclude [future future-call])
  (:import (java.util.concurrent Executors)))

(def virtual-thread-executor (Executors/newVirtualThreadPerTaskExecutor))

(defn future-call [f]
  (.submit virtual-thread-executor f))

(defmacro future
  "Like clojure.core/future, but uses virtual threads."
  [& body] `(future-call (^{:once true} fn* [] ~@body)))

(defn start-thread [f]
  (Thread/startVirtualThread f))

(defn schedule
  "Runs f after delay-ms. Can be cancelled with future-cancel."
  [^Long delay-ms f]
  (future (Thread/sleep delay-ms)
          (f)))

(defn start-worker-thread
  "Starts a thread which runs f in an infinite loop until interrupted
  or until f returns ::interrupt."
  ([thread-name f]
   (start-worker-thread thread-name f (fn [])))
  ([thread-name f finally-f]
   (start-thread
     (fn []
       (try
         (loop []
           (when-not (.isInterrupted (Thread/currentThread))
             (let [ret (f)]
               (if (= ret ::interrupt)
                 nil
                 (recur)))))
         (catch InterruptedException _
           (println (format "%s thread interrupted! Shutting it down!" thread-name)))
         (finally
           (finally-f)))))))