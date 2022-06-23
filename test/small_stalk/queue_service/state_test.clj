(ns small-stalk.queue-service.state-test
  (:require [clojure.test :refer :all]
            [small-stalk.test-helpers :as test-helpers]
            [small-stalk.queue-service.state :as state]
            [small-stalk.queue-service.priority-queue :as pqueue])
  (:import (clojure.lang PersistentQueue)))

(def test-dir "/tmp/small-stalk/dump-test")

(deftest dump-and-load-test
  (testing "when there are no reserved jobs"
    (test-helpers/make-directory test-dir)
    (test-helpers/clear-directory test-dir)
    (let [state-atom (state/new-state)
          mutations  [{:type     ::state/put
                       :priority 2
                       :data     "foo"}
                      {:type     ::state/put
                       :priority 1
                       :data     "bar"}
                      {:type     ::state/put
                       :priority 3
                       :data     "baz"}]]
      (doseq [mutation mutations]
        (state/process-mutation {:state-atom     state-atom
                                 :mutation-queue nil}
                                mutation
                                true))
      (state/dump-state @state-atom (str test-dir "/test-dump.edn"))
      (is (= @state-atom
             @(state/load-dump (str test-dir "/test-dump.edn"))))))

  (testing "when there are reserved jobs"
    (test-helpers/make-directory test-dir)
    (test-helpers/clear-directory test-dir)
    (let [state-atom (state/new-state)
          mutations  [{:type            ::state/put
                       :job-description {
                                         :priority 2
                                         :data     "foo"}}
                      {:type            ::state/put
                       :job-description {:priority 1
                                         :data     "bar"}}
                      {:type            ::state/put
                       :job-description {:priority 3
                                         :data     "baz"}}
                      {:type          ::state/reserve
                       :connection-id 69}]]
      (doseq [mutation mutations]
        (state/process-mutation {:state-atom     state-atom
                                 :mutation-queue nil}
                                mutation
                                true))
      (state/dump-state @state-atom (str test-dir "/test-dump.edn"))
      (let [loaded-state-atom (state/load-dump (str test-dir "/test-dump.edn"))]
        (is (= [[1 {:id       1
                    :priority 1
                    :data     "bar"}]
                [2 {:id       0
                    :priority 2
                    :data     "foo"}]
                [3 {:id       2
                    :priority 3
                    :data     "baz"}]]
               (pqueue/to-seq (:pqueue @loaded-state-atom))))

        (is (= {:job-id-counter         2
                :reserved-jobs          #{}
                :waiting-reserves       (PersistentQueue/EMPTY)
                :reserve-timeout-timers {}
                :time-to-run-timers     {}}
               (dissoc @loaded-state-atom :pqueue)))))))
