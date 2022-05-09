(ns small-stalk.queue-service.service-test
  (:require [clojure.test :refer :all]
            [small-stalk.system :as system]
            [small-stalk.queue-service.service :as queue-service]
            [small-stalk.threads :as vthreads]))

(deftest put-test
  (testing "when there are no pending reserves"
    (with-open [system (system/open-system! [::queue-service/mutation-thread])]
      (let [q-service    (system/get system ::queue-service/queue-service)
            job          {:id       0
                          :priority 2
                          :data     "foobar"}
            inserted-job (queue-service/put q-service job)]
        (is (= job inserted-job))
        (is (= {:id       0
                :priority 2
                :data     "foobar"}
               (queue-service/peek-ready q-service))))))

  (testing "when there is a pending reserve"
    (with-open [system (system/open-system! [::queue-service/mutation-thread])]
      (let [q-service           (system/get system ::queue-service/queue-service)
            reserved-job-future (vthreads/future (queue-service/reserve q-service 69))
            job                 {:id       5
                                 :priority 2
                                 :data     "foobar"}]
        (Thread/sleep 50)
        (queue-service/put q-service job)
        (is (= job @reserved-job-future))
        (is (nil? (@#'queue-service/next-waiting-reserve @(:state-atom q-service))))))))

(deftest reserve-test
  (testing "when a job is ready"
    (with-open [system (system/open-system! [::queue-service/mutation-thread])]
      (let [q-service    (system/get system ::queue-service/queue-service)
            job          {:id       5
                          :priority 2
                          :data     "foobar"}
            _            (queue-service/put q-service job)
            reserved-job (queue-service/reserve q-service 69)]
        (is (= job reserved-job))
        (is (nil? (queue-service/peek-ready q-service)))
        (is (nil? (@#'queue-service/next-waiting-reserve @(:state-atom q-service)))))))

  (testing "when no job is ready"
    (with-open [system (system/open-system! [::queue-service/mutation-thread])]
      (let [q-service (system/get system ::queue-service/queue-service)
            _         (vthreads/future (queue-service/reserve q-service 69))]
        (Thread/sleep 50)
        (is (some? (@#'queue-service/next-waiting-reserve @(:state-atom q-service))))))))
