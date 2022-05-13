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
  (testing "without a timeout"
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

  (testing "with a timeout"
    (testing "when no job is received within the timeout period"
      (with-open [system (system/open-system! [::queue-service/mutation-thread])]
        (let [q-service      (system/get system ::queue-service/queue-service)
              reserve-future (vthreads/future (queue-service/reserve q-service 69 1))]
          (is (= (ssf/fail {:type ::queue-service/reserve-waiting-timed-out})
                 (deref reserve-future 3000 :deref-timed-out)))
          (is (empty? (:reserve-timeout-timers @(:state-atom q-service)))))))

    (testing "when a job is received before the reserve times out"
      (with-open [system (system/open-system! [::queue-service/mutation-thread])]
        (let [q-service      (system/get system ::queue-service/queue-service)
              reserve-future (vthreads/future
                               (queue-service/reserve q-service 69 1))
              job            {:id       5
                              :priority 2
                              :data     "foobar"}]
          (queue-service/put q-service job)
          (is (= job
                 (deref reserve-future 2000 :deref-timed-out)))
          (is (empty? (:reserve-timeout-timers @(:state-atom q-service)))))))

    (testing "when the timeout is zero"
      (testing "when a job is ready"
        (with-open [system (system/open-system! [::queue-service/mutation-thread])]
          (let [q-service    (system/get system ::queue-service/queue-service)
                job          {:id       5
                              :priority 2
                              :data     "foobar"}
                _            (queue-service/put q-service job)
                reserved-job (queue-service/reserve q-service 69 0)]
            (is (= job reserved-job))
            (is (nil? (queue-service/peek-ready q-service)))
            (is (nil? (@#'queue-service/next-waiting-reserve @(:state-atom q-service)))))))

      (testing "when no job is ready"
        (with-open [system (system/open-system! [::queue-service/mutation-thread])]
          (let [q-service      (system/get system ::queue-service/queue-service)
                reserve-future (vthreads/future (queue-service/reserve q-service 69 0))]
            (is (= (ssf/fail {:type ::queue-service/reserve-waiting-timed-out})
                   (deref reserve-future 3000 :deref-timed-out)))
            (is (nil? (@#'queue-service/next-waiting-reserve @(:state-atom q-service))))))))))

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

(deftest release-test
  (testing "when the current connection has reserved the job"
    (testing "when there are no pending reserves"
      (with-open [system (system/open-system! [::queue-service/mutation-thread])]
        (let [q-service (system/get system ::queue-service/queue-service)
              job       {:id       5
                         :priority 2
                         :data     "foobar"}
              _         (queue-service/put q-service job)
              _         (queue-service/reserve q-service 69)]
          (is (= {:id       5
                  :priority 3
                  :data     "foobar"}
                 (queue-service/release q-service 69 5 3)))
          (is (empty? (:reserved-jobs @(:state-atom q-service))))
          (is (= {:id       5
                  :priority 3
                  :data     "foobar"}
                 (queue-service/peek-ready q-service))))))

    (testing "when there is a pending reserve"
      (with-open [system (system/open-system! [::queue-service/mutation-thread])]
        (let [q-service (system/get system ::queue-service/queue-service)
              job       {:id       5
                         :priority 2
                         :data     "foobar"}
              _         (queue-service/put q-service job)
              _         (queue-service/reserve q-service 69)
              _         (vthreads/future (queue-service/reserve q-service 42))]
          (Thread/sleep 20)
          (is (= {:id       5
                  :priority 3
                  :data     "foobar"}
                 (queue-service/release q-service 69 5 3)))
          (is (= #{{:id          5
                    :priority    3
                    :data        "foobar"
                    :reserved-by 42}}
                 (:reserved-jobs @(:state-atom q-service))))
          (is (nil? (queue-service/peek-ready q-service)))))))

  (testing "when the job was reserved by a different connection"
    (with-open [system (system/open-system! [::queue-service/mutation-thread])]
      (let [q-service (system/get system ::queue-service/queue-service)
            job       {:id       5
                       :priority 2
                       :data     "foobar"}
            _         (queue-service/put q-service job)
            _         (queue-service/reserve q-service 69)]
        (is (= (ssf/fail {:type ::queue-service/job-not-found})
               (queue-service/release q-service 42 5 3)))
        (is (= #{{:id          5
                  :priority    2
                  :data        "foobar"
                  :reserved-by 69}}
               (:reserved-jobs @(:state-atom q-service))))
        (is (nil? (queue-service/peek-ready q-service))))))

  (testing "when the job with the given ID doesn't exist in the reserve set"
    (with-open [system (system/open-system! [::queue-service/mutation-thread])]
      (let [q-service (system/get system ::queue-service/queue-service)
            job       {:id       5
                       :priority 2
                       :data     "foobar"}
            _         (queue-service/put q-service job)
            _         (queue-service/reserve q-service 69)]
        (is (= (ssf/fail {:type ::queue-service/job-not-found})
               (queue-service/release q-service 69 6 3)))
        (is (= #{{:id          5
                  :priority    2
                  :data        "foobar"
                  :reserved-by 69}}
               (:reserved-jobs @(:state-atom q-service))))
        (is (nil? (queue-service/peek-ready q-service)))))))
