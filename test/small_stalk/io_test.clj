(ns small-stalk.io-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [small-stalk.io :as ssio]))

(deftest read-string-until-crlf-test
  (testing "reads and returns a string until CRLF"
    (is (= "foo\r\n"
           (ssio/read-string-until-crlf (io/input-stream
                                          (.getBytes "foo\r\nbar\r\n")))))
    (is (= "foo\r\n"
           (ssio/read-string-until-crlf (io/input-stream
                                          (.getBytes "foo\r\n"))))))
  (testing "returns the entire stream if CRLF is not encountered"
    (is (= "foobar"
           (ssio/read-string-until-crlf (io/input-stream
                                          (.getBytes "foobar"))))))
  (testing "returns an empty string if the stream has reached EOF"
    (let [stream (io/input-stream (.getBytes "foobar"))]
      (ssio/read-string-until-crlf stream)
      (is (= ""
             (ssio/read-string-until-crlf stream))))))


