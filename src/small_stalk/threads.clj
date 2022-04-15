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