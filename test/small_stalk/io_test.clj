(ns small-stalk.io-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [small-stalk.io :as ssio])
  (:import (java.io ByteArrayOutputStream)))

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

(deftest write-crlf-string-test
  (testing "writes the string to an output stream and add CRLF at the end"
    (with-open [out (ByteArrayOutputStream.)]
      (ssio/write-crlf-string out "foobar")
      (is (= "foobar\r\n"
             (.toString out "US-ASCII"))))))

(deftest string-reading-and-writing-test
  (testing "can write a string using write-crlf-string and read it back"
    (with-open [out (ByteArrayOutputStream.)]
      (ssio/write-crlf-string out "foobar")
      (is (= "foobar\r\n"
             (ssio/read-string-until-crlf (io/input-stream (.toByteArray out))))))))
