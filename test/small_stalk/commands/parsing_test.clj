(ns small-stalk.commands.parsing-test
  (:require [clojure.test :refer :all]
            [small-stalk.commands.parsing :as parsing]
            [small-stalk.failure :as ssf]))

(deftest parse-command-test
  (testing "Parsing an invalid command"
    (is (= (ssf/fail {:type ::parsing/parser-failure
                      :name ::parsing/unknown-command})
           (parsing/parse-command ["foo" "2"]))))
  (testing "Parsing put"
    (is (= {:command-name     "put"
            :priority         2
            :time-to-run-secs 5}
           (parsing/parse-command ["put" "2" "5"])))
    (is (= (= {:command-name     "put"
               :priority         5
               :time-to-run-secs 6}
              (parsing/parse-command ["put" "5" "6"]))))))

(deftest parse-args-test
  (testing "Parsing valid input against a grammar"
    (is (= {:foo 2
            :bar 3}
           (parsing/parse-args [[:foo ::parsing/non-negative-number]
                                [:bar ::parsing/non-negative-number]]
                               ["2" "3"]))))
  (testing "Parsing invalid input"
    (is (= (ssf/fail {:type ::parsing/parser-failure
                      :name ::parsing/unexpected-negative-number
                      :key  :bar})
           (parsing/parse-args [[:foo ::parsing/non-negative-number]
                                [:bar ::parsing/non-negative-number]]
                               ["2" "-3"])))
    (is (= (ssf/fail {:type ::parsing/parser-failure
                      :name ::parsing/too-few-args})
           (parsing/parse-args [[:foo ::parsing/non-negative-number]
                                [:bar ::parsing/non-negative-number]]
                               ["2"])))
    (is (= (ssf/fail {:type ::parsing/parser-failure
                      :name ::parsing/too-many-args})
           (parsing/parse-args [[:foo ::parsing/non-negative-number]
                                [:bar ::parsing/non-negative-number]]
                               ["2" "3" "4"])))))