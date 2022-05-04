(ns small-stalk.commands.handlers-test
  (:require [clojure.test :refer :all]
            [small-stalk.commands.handlers :as handlers]
            [small-stalk.queue-service.service :as queue-service]
            [clojure.java.io :as io]
            [small-stalk.system :as system]
            [small-stalk.queue-service.priority-queue :as pqueue])
  (:import (java.io ByteArrayOutputStream)))

(deftest handle-command-test
  (testing "put"
    (testing "when a valid command is sent"
      (with-open [system     (system/open-system! [::handlers/command-handler
                                                   ::queue-service/mutation-thread])
                  in-stream  (io/input-stream (.getBytes "foobar\r\n"))
                  out-stream (ByteArrayOutputStream.)]
        (let [handle-command (system/get system ::handlers/command-handler)
              {:keys [state-atom]} (system/get system ::queue-service/queue-service)]
          (handle-command {:command-name "put"
                           :priority     2}
                          in-stream
                          out-stream)
          (is (= "INSERTED 0\r\n" (.toString out-stream "US-ASCII")))
          (is (= {:id       0
                  :priority 2
                  :data     "foobar"}
                 (pqueue/peek (:pqueue @state-atom)))))))

    (testing "when the data ends without a CRLF"
      (with-open [system     (system/open-system! [::handlers/command-handler
                                                   ::queue-service/mutation-thread])
                  in-stream  (io/input-stream (.getBytes "foobar"))
                  out-stream (ByteArrayOutputStream.)]
        (let [handle-command (system/get system ::handlers/command-handler)
              {:keys [state-atom]} (system/get system ::queue-service/queue-service)]
          (handle-command {:command-name "put"
                           :priority     2}
                          in-stream
                          out-stream)
          (is (= "" (.toString out-stream "US-ASCII")))
          (is (nil? (pqueue/peek (:pqueue @state-atom)))))))))
