(ns small-stalk.commands.handlers-test
  (:require [clojure.test :refer :all]
            [small-stalk.commands.handlers :as handlers]
            [small-stalk.queue-service.service :as queue-service]
            [clojure.java.io :as io]
            [small-stalk.system :as system]
            [small-stalk.io :as ssio]
            [small-stalk.failure :as ssf]
            [small-stalk.test-helpers :as test-helpers])
  (:import (java.io ByteArrayOutputStream)))

(deftest handle-put-command-test
  (testing "when a valid command is sent"
    (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes "foobar\r\n"))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)]
        (handle-command {:command-name     "put"
                         :priority         2
                         :time-to-run-secs 5}
                        69
                        in-stream
                        out-stream)
        (is (= "INSERTED 0\r\n" (.toString out-stream "US-ASCII"))))))

  (testing "when the data ends without a CRLF"
    (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes "foobar"))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)
            error          (handle-command {:command-name     "put"
                                            :priority         2
                                            :time-to-run-secs 5}
                                           69
                                           in-stream
                                           out-stream)]
        (is (= (ssf/fail {:type           ::ssio/eof-reached
                          :remaining-data "foobar"})
               error))
        (is (= "" (.toString out-stream "US-ASCII")))))))

(deftest handle-peek-ready-command-test
  (testing "when a job is available"
    (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes ""))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)
            q-service      (system/get system ::queue-service/queue-service)
            job            (queue-service/put q-service {:priority 2
                                                         :data     "foobar"})]
        (handle-command {:command-name "peek-ready"}
                        69
                        in-stream
                        out-stream)
        (is (= (format "FOUND %d\r\nfoobar\r\n" (:id job))
               (.toString out-stream "US-ASCII"))))))

  (testing "when no job is ready"
    (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes ""))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)]
        (handle-command {:command-name "peek-ready"}
                        69
                        in-stream
                        out-stream)
        (is (= "NOT_FOUND\r\n" (.toString out-stream "US-ASCII")))))))

(deftest handle-reserve-command-test
  (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
              in-stream  (io/input-stream (.getBytes ""))
              out-stream (ByteArrayOutputStream.)]
    (let [handle-command (system/get system ::handlers/command-handler)
          q-service      (system/get system ::queue-service/queue-service)
          job            (queue-service/put q-service {:id       5
                                                       :priority 2
                                                       :data     "foobar"})]
      (handle-command {:command-name "reserve"}
                      69
                      in-stream
                      out-stream)
      (is (= (format "RESERVED %d\r\nfoobar\r\n" (:id job)) (.toString out-stream "US-ASCII"))))))

(deftest handle-reserve-with-timeout-command-test
  (testing "when a job was found"
    (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes ""))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)
            q-service      (system/get system ::queue-service/queue-service)
            job            (queue-service/put q-service {:priority 2
                                                         :data     "foobar"})]
        (handle-command {:command-name "reserve-with-timeout"
                         :timeout-secs 0}
                        69
                        in-stream
                        out-stream)
        (is (= (format "RESERVED %d\r\nfoobar\r\n" (:id job)) (.toString out-stream "US-ASCII"))))))

  (testing "when the reserve times out"
    (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes ""))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)]
        (handle-command {:command-name "reserve-with-timeout"
                         :timeout-secs 0}
                        69
                        in-stream
                        out-stream)
        (is (= "TIMED_OUT\r\n" (.toString out-stream "US-ASCII")))))))

(deftest handle-delete-command-test
  (testing "when the job was found"
    (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes ""))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)
            q-service      (system/get system ::queue-service/queue-service)
            _              (queue-service/put q-service {
                                                         :priority 2
                                                         :data     "foobar"})
            reserved-job   (queue-service/reserve q-service 69)]
        (handle-command {:command-name "delete"
                         :job-id       (:id reserved-job)}
                        69
                        in-stream
                        out-stream)
        (is (= "DELETED\r\n" (.toString out-stream "US-ASCII"))))))

  (testing "when the job was not found"
    (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes ""))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)]
        (handle-command {:command-name "delete"
                         :job-id       5}
                        69
                        in-stream
                        out-stream)
        (is (= "NOT_FOUND\r\n" (.toString out-stream "US-ASCII")))))))

(deftest handle-release-command-test
  (testing "when the job was found"
    (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes ""))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)
            q-service      (system/get system ::queue-service/queue-service)
            _              (queue-service/put q-service {:id       5
                                                         :priority 2
                                                         :data     "foobar"})
            reserved-job   (queue-service/reserve q-service 69)]
        (handle-command {:command-name "release"
                         :job-id       (:id reserved-job)
                         :new-priority 1}
                        69
                        in-stream
                        out-stream)
        (is (= "RELEASED\r\n" (.toString out-stream "US-ASCII"))))))

  (testing "when the job was not found"
    (with-open [system     (test-helpers/open-system! [::handlers/command-handler])
                in-stream  (io/input-stream (.getBytes ""))
                out-stream (ByteArrayOutputStream.)]
      (let [handle-command (system/get system ::handlers/command-handler)]
        (handle-command {:command-name "release"
                         :job-id       5
                         :new-priority 1}
                        69
                        in-stream
                        out-stream)
        (is (= "NOT_FOUND\r\n" (.toString out-stream "US-ASCII")))))))
