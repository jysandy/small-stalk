(ns small-stalk.queue-service.service-test
  (:require [clojure.test :refer :all]
            [small-stalk.system :as system]
            [small-stalk.queue-service.service :as queue-service]
            [small-stalk.threads :as vthreads]
            [small-stalk.failure :as ssf]))

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

(deftest delete-test
  (testing "when the current connection has reserved the job"
    (with-open [system (system/open-system! [::queue-service/mutation-thread])]
      (let [q-service (system/get system ::queue-service/queue-service)
            job       {:id       5
                       :priority 2
                       :data     "foobar"}
            _         (queue-service/put q-service job)
            _         (queue-service/reserve q-service 69)]
        (is (= job (queue-service/delete q-service 69 5)))
        (is (empty? (:reserved-jobs @(:state-atom q-service)))))))

  (testing "when the job was reserved by a different connection"
    (with-open [system (system/open-system! [::queue-service/mutation-thread])]
      (let [q-service (system/get system ::queue-service/queue-service)
            job       {:id       5
                       :priority 2
                       :data     "foobar"}
            _         (queue-service/put q-service job)
            _         (queue-service/reserve q-service 69)]
        (is (= (ssf/fail {:type ::queue-service/job-not-found}) (queue-service/delete q-service 42 5)))
        (is (= #{(assoc job :reserved-by 69)} (:reserved-jobs @(:state-atom q-service)))))))

  (testing "when the job is in the ready queue"
    (with-open [system (system/open-system! [::queue-service/mutation-thread])]
      (let [q-service (system/get system ::queue-service/queue-service)
            job       {:id       5
                       :priority 2
                       :data     "foobar"}
            _         (queue-service/put q-service job)]
        (is (= job (queue-service/delete q-service 69 5)))
        (is (nil? (queue-service/peek-ready q-service))))))

  (testing "when a job with the given ID is not present"
    (with-open [system (system/open-system! [::queue-service/mutation-thread])]
      (let [q-service (system/get system ::queue-service/queue-service)]
        (is (= (ssf/fail {:type ::queue-service/job-not-found}) (queue-service/delete q-service 42 5)))))))
