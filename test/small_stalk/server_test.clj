(ns small-stalk.server-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [small-stalk.server :as server]
            [mock-clj.core :as mc]
            [small-stalk.commands.parsing :as parsing]
            [small-stalk.failure :as ssf]
            [small-stalk.io :as ssio])
  (:import (java.io ByteArrayOutputStream)))

(deftest command-processing-loop-test
  (testing "when the command parsing succeeds"
    (testing "it calls handle-command with the command"
      (with-open [input-stream  (io/input-stream (.getBytes "foo bar\r\n"))
                  output-stream (ByteArrayOutputStream.)]
        (let [command-handler (mc/make-mock)]
          (mc/with-mock [parsing/parse-command {:foo "bar"}]
            (server/command-processing-loop command-handler input-stream output-stream)
            (let [[command] (mc/last-call command-handler)]
              (is (= [["foo" "bar"]] (mc/last-call parsing/parse-command)))
              (is (= {:foo "bar"} command))))))))

  (testing "when the command is unrecognized"
    (testing "it writes UNKNOWN_COMMAND to the output stream"
      (with-open [input-stream  (io/input-stream (.getBytes "foo bar\r\n"))
                  output-stream (ByteArrayOutputStream.)]
        (mc/with-mock [parsing/parse-command (parsing/parser-failure {:name ::parsing/unknown-command})]
          (server/command-processing-loop (constantly nil) input-stream output-stream)
          (is (= "UNKNOWN_COMMAND\r\n" (.toString output-stream "US-ASCII")))))))

  (testing "when the command is recognized but the format is bad"
    (testing "it writes BAD_FORMAT to the output stream"
      (with-open [input-stream  (io/input-stream (.getBytes "foo bar\r\n"))
                  output-stream (ByteArrayOutputStream.)]
        (mc/with-mock [parsing/parse-command (parsing/parser-failure {:name ::parsing/parsing-number-failed})]
          (server/command-processing-loop (constantly nil) input-stream output-stream)
          (is (= "BAD_FORMAT\r\n" (.toString output-stream "US-ASCII")))))))

  (testing "when the command parsing succeeds but the command handling returns an error"
    (testing "it writes INTERNAL_ERROR to the output stream"
      (with-open [input-stream  (io/input-stream (.getBytes "foo bar\r\n"))
                  output-stream (ByteArrayOutputStream.)]
        (let [command-handler (constantly (ssf/fail {:type ::unknown-failure}))]
          (mc/with-mock [parsing/parse-command {:foo "bar"}]
            (server/command-processing-loop command-handler input-stream output-stream)
            (is (= "INTERNAL_ERROR\r\n" (.toString output-stream "US-ASCII"))))))))

  (testing "when the input stream ends before CRLF"
    (testing "it does nothing and returns"
      (with-open [input-stream  (io/input-stream (.getBytes "foo bar"))
                  output-stream (ByteArrayOutputStream.)]
        (server/command-processing-loop (constantly nil) input-stream output-stream)
        (is (= "" (.toString output-stream "US-ASCII"))))))

  (testing "when the command handler returns an output stream closed error"
    (testing "it does nothing and returns"
      (with-open [input-stream  (io/input-stream (.getBytes "foo bar"))
                  output-stream (ByteArrayOutputStream.)]
        (server/command-processing-loop (constantly (ssf/fail {:type ::ssio/output-stream-closed}))
                                        input-stream
                                        output-stream)
        (is (= "" (.toString output-stream "US-ASCII")))))))