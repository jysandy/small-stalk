(ns small-stalk.server-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [small-stalk.server :as server])
  (:import (java.io ByteArrayOutputStream)))

(deftest command-processing-loop-test
  (testing "it echoes what is written to the input stream"
    (with-open [input-stream  (io/input-stream (.getBytes "foobar\r\nbaz\r\nquux\r\n"))
                output-stream (ByteArrayOutputStream.)]
      (server/command-processing-loop input-stream output-stream)
      (is (= "foobar\r\nbaz\r\nquux\r\n"
             (.toString output-stream "US-ASCII"))))))