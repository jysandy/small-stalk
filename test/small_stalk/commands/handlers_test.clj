(ns small-stalk.commands.handlers-test
  (:require [clojure.test :refer :all]
            [small-stalk.commands.handlers :as handlers]
            [clojure.java.io :as io]
            [small-stalk.queues :as queues]
            [small-stalk.system :as system])
  (:import (java.io ByteArrayOutputStream)))

(deftest handle-command-test
  (testing "put"
    (with-open [system     (system/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes "foobar\r\n"))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)
            default-queue  (system/get system ::queues/default-queue)]
        (handle-command {:command-name "put"
                         :priority     2}
                        in-stream
                        out-stream)
        (is (= "INSERTED 0\r\n" (.toString out-stream "US-ASCII")))
        (is (= {:id       0
                :priority 2
                :data     "foobar"}
               (queues/peek default-queue)))))))
