(ns small-stalk.persistence.append-only-log.filesystem-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [small-stalk.persistence.append-only-log.filesystem :as aol-filesystem]
            [small-stalk.persistence.append-only-log :as aol]
            [clojure.edn :as edn]
            [small-stalk.test-helpers :as test-helpers]))

(deftest aof-writer-test
  (test-helpers/make-directory "/tmp/aof-files-test")
  (test-helpers/clear-directory "/tmp/aof-files-test")
  (with-open [log (aol-filesystem/filesystem-append-only-log "/tmp/aof-files-test"
                                                             3)]
    (doseq [n (range 8)]
      (aol/write-entry log {:test n})))
  (is (= #{"1.aof.edn" "2.aof.edn" "3.aof.edn"}
         (test-helpers/directory-contents "/tmp/aof-files-test")))
  (let [first-file-contents  (->> (io/reader "/tmp/aof-files-test/1.aof.edn")
                                  (line-seq)
                                  (map edn/read-string))
        second-file-contents (->> (io/reader "/tmp/aof-files-test/2.aof.edn")
                                  (line-seq)
                                  (map edn/read-string))
        third-file-contents  (->> (io/reader "/tmp/aof-files-test/3.aof.edn")
                                  (line-seq)
                                  (map edn/read-string))]
    (is (= [{:test 0}
            {:test 1}
            {:test 2}]
           first-file-contents))
    (is (= [{:test 3}
            {:test 4}
            {:test 5}]
           second-file-contents))
    (is (= [{:test 6}
            {:test 7}]
           third-file-contents))))

(deftest aof-writing-and-reading-test
  (testing "the regular reader"
    (test-helpers/make-directory "/tmp/aof-files-test")
    (test-helpers/clear-directory "/tmp/aof-files-test")
    (with-open [log (aol-filesystem/filesystem-append-only-log "/tmp/aof-files-test"
                                                               3)]
      (let [aof-records (map (fn [n] {:test n}) (range 8))]
        (doall (map (partial aol/write-entry log)
                    aof-records))
        (is (= #{"1.aof.edn" "2.aof.edn" "3.aof.edn"}
               (test-helpers/directory-contents "/tmp/aof-files-test")))
        (with-open [reader (aol/new-reader log)]
          (is (= aof-records (aol/entry-seq reader)))))))

  (testing "the inactive file reader"
    (test-helpers/make-directory "/tmp/aof-files-test")
    (test-helpers/clear-directory "/tmp/aof-files-test")
    (with-open [log (aol-filesystem/filesystem-append-only-log "/tmp/aof-files-test"
                                                               3)]
      (let [aof-records (map (fn [n] {:test n}) (range 8))]
        (doall (map (partial aol/write-entry log)
                    aof-records))
        (is (= #{"1.aof.edn" "2.aof.edn" "3.aof.edn"}
               (test-helpers/directory-contents "/tmp/aof-files-test")))
        (with-open [reader (aol-filesystem/new-inactive-file-reader log)]
          (is (= (take 6 aof-records) (aol/entry-seq reader))))))))
