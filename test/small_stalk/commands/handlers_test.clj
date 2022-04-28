(ns small-stalk.commands.handlers-test
  (:require [clojure.test :refer :all]
            [small-stalk.commands.handlers :as handlers]
            [clojure.java.io :as io]
            [small-stalk.queues :as queues])
  (:import (java.io ByteArrayOutputStream)))

(deftest handle-command-test
  (testing "put"
    (with-open [in-stream  (io/input-stream (.getBytes "foobar\r\n"))
                out-stream (ByteArrayOutputStream.)]
      (queues/reset-job-id!)
      (handlers/handle-command {:command-name "put"
                                :priority     2}
                               in-stream
                               out-stream)
      (let [output-string (.toString out-stream "US-ASCII")]
        (is (= "INSERTED 0\r\n" output-string))
        (is (= {:id       0
                :priority 2
                :data     "foobar"}
               (queues/peek queues/default-queue)))))))
