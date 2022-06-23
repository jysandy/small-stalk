(ns small-stalk.queue-service.service-test
  (:require [clojure.test :refer :all]
            [small-stalk.system :as system]
            [small-stalk.queue-service.service :as queue-service]
            [small-stalk.threads :as vthreads]
            [small-stalk.failure :as ssf]
            [small-stalk.test-helpers :as test-helpers]
            [small-stalk.queue-service.priority-queue :as pqueue]
            [small-stalk.queue-service.state :as state]
            [small-stalk.persistence.service :as p-service]))

(deftest put-test
  (testing "when there are no pending reserves"
    (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service    (system/get system ::queue-service/queue-service)
            jd           {:priority 2
                          :data     "foobar"}
            inserted-job (queue-service/put q-service jd)]
        (is (= {:id       0
                :priority 2
                :data     "foobar"} inserted-job))
        (is (= {:id       0
                :priority 2
                :data     "foobar"}
               (queue-service/peek-ready q-service))))))

  (testing "when there is a pending reserve"
    (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service           (system/get system ::queue-service/queue-service)
            reserved-job-future (vthreads/future (queue-service/reserve q-service 69))
            job                 {:priority 2
                                 :data     "foobar"}]
        (Thread/sleep 50)
        (queue-service/put q-service job)
        (is (= (assoc job :id 0) (deref reserved-job-future 2000 :deref-timed-out)))
        (is (nil? (@#'state/next-waiting-reserve @(:state-atom q-service))))))))

(deftest reserve-test
  (testing "without a timeout"
    (testing "when a job is ready"
      (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
        (let [q-service    (system/get system ::queue-service/queue-service)
              jd           {:priority 2
                            :data     "foobar"}
              job          (queue-service/put q-service jd)
              reserved-job (queue-service/reserve q-service 69)]
          (is (= job reserved-job))
          (is (nil? (queue-service/peek-ready q-service)))
          (is (nil? (@#'state/next-waiting-reserve @(:state-atom q-service)))))))

    (testing "when no job is ready"
      (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
        (let [q-service (system/get system ::queue-service/queue-service)
              _         (vthreads/future (queue-service/reserve q-service 69))]
          (Thread/sleep 50)
          (is (some? (@#'state/next-waiting-reserve @(:state-atom q-service))))))))

  (testing "with a timeout"
    (testing "when no job is received within the timeout period"
      (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
        (let [q-service      (system/get system ::queue-service/queue-service)
              reserve-future (vthreads/future (queue-service/reserve q-service 69 1))]
          (is (= (ssf/fail {:type ::state/reserve-waiting-timed-out})
                 (deref reserve-future 3000 :deref-timed-out)))
          (is (empty? (:reserve-timeout-timers @(:state-atom q-service)))))))

    (testing "when a job is received before the reserve times out"
      (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
        (let [q-service      (system/get system ::queue-service/queue-service)
              reserve-future (vthreads/future
                               (queue-service/reserve q-service 69 1))
              jd             {:id       5
                              :priority 2
                              :data     "foobar"}
              job            (queue-service/put q-service jd)]
          (is (= job
                 (deref reserve-future 2000 :deref-timed-out)))
          (is (empty? (:reserve-timeout-timers @(:state-atom q-service)))))))

    (testing "when the timeout is zero"
      (testing "when a job is ready"
        (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
          (let [q-service    (system/get system ::queue-service/queue-service)
                jd           {:priority 2
                              :data     "foobar"}
                job          (queue-service/put q-service jd)
                reserved-job (queue-service/reserve q-service 69 0)]
            (is (= job reserved-job))
            (is (nil? (queue-service/peek-ready q-service)))
            (is (nil? (@#'state/next-waiting-reserve @(:state-atom q-service)))))))

      (testing "when no job is ready"
        (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
          (let [q-service      (system/get system ::queue-service/queue-service)
                reserve-future (vthreads/future (queue-service/reserve q-service 69 0))]
            (is (= (ssf/fail {:type ::state/reserve-waiting-timed-out})
                   (deref reserve-future 3000 :deref-timed-out)))
            (is (nil? (@#'state/next-waiting-reserve @(:state-atom q-service)))))))))

  (testing "when the job has a time to run"
    (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service      (system/get system ::queue-service/queue-service)
            reserve-future (vthreads/future
                             (queue-service/reserve q-service 69 1))
            jd             {:priority         2
                            :data             "foobar"
                            :time-to-run-secs 1}
            job            (queue-service/put q-service jd)]
        (is (= job
               (deref reserve-future 2000 :deref-timed-out)))
        (is (empty? (:reserve-timeout-timers @(:state-atom q-service))))
        (is (seq (:time-to-run-timers @(:state-atom q-service))))
        (is (nil? (queue-service/peek-ready q-service)))
        (Thread/sleep 1010)
        (is (= job
               (queue-service/peek-ready q-service)))))))

(deftest delete-test
  (testing "when the current connection has reserved the job"
    (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service (system/get system ::queue-service/queue-service)
            jd        {:priority 2
                       :data     "foobar"}
            job       (queue-service/put q-service jd)
            _         (queue-service/reserve q-service 69)]
        (is (= job (queue-service/delete q-service 69 (:id job))))
        (is (empty? (:reserved-jobs @(:state-atom q-service)))))))

  (testing "when the job was reserved by a different connection"
    (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service (system/get system ::queue-service/queue-service)
            jd        {:priority 2
                       :data     "foobar"}
            job       (queue-service/put q-service jd)
            _         (queue-service/reserve q-service 69)]
        (is (= (ssf/fail {:type ::state/job-not-found}) (queue-service/delete q-service 42 5)))
        (is (= #{(assoc job :reserved-by 69)} (:reserved-jobs @(:state-atom q-service)))))))

  (testing "when the job is in the ready queue"
    (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service (system/get system ::queue-service/queue-service)
            jd        {:priority 2
                       :data     "foobar"}
            job       (queue-service/put q-service jd)]
        (is (= job (queue-service/delete q-service 69 (:id job))))
        (is (nil? (queue-service/peek-ready q-service))))))

  (testing "when a job with the given ID is not present"
    (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service (system/get system ::queue-service/queue-service)]
        (is (= (ssf/fail {:type ::state/job-not-found}) (queue-service/delete q-service 42 5)))))))

(deftest release-test
  (testing "when the current connection has reserved the job"
    (testing "when there are no pending reserves"
      (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
        (let [q-service    (system/get system ::queue-service/queue-service)
              jd           {:priority 2
                            :data     "foobar"}
              job          (queue-service/put q-service jd)
              _            (queue-service/reserve q-service 69)
              released-job (queue-service/release q-service 69 (:id job) 3)]
          (is (= (assoc job :priority 3)
                 released-job))
          (is (empty? (:reserved-jobs @(:state-atom q-service))))
          (is (= released-job
                 (queue-service/peek-ready q-service))))))

    (testing "when there is a pending reserve"
      (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
        (let [q-service    (system/get system ::queue-service/queue-service)
              jd           {:priority 2
                            :data     "foobar"}
              job          (queue-service/put q-service jd)
              _            (queue-service/reserve q-service 69)
              _            (vthreads/future (queue-service/reserve q-service 42))
              released-job (queue-service/release q-service 69 (:id job) 3)]
          (Thread/sleep 20)
          (is (= (assoc job :priority 3)
                 released-job))
          (is (= #{(assoc released-job :reserved-by 42)}
                 (:reserved-jobs @(:state-atom q-service))))
          (is (nil? (queue-service/peek-ready q-service)))))))

  (testing "when the job was reserved by a different connection"
    (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service (system/get system ::queue-service/queue-service)
            jd        {:priority 2
                       :data     "foobar"}
            job       (queue-service/put q-service jd)
            _         (queue-service/reserve q-service 69)]
        (is (= (ssf/fail {:type ::state/job-not-found})
               (queue-service/release q-service 42 (:id job) 3)))
        (is (= #{(assoc job :reserved-by 69)}
               (:reserved-jobs @(:state-atom q-service))))
        (is (nil? (queue-service/peek-ready q-service))))))

  (testing "when the job with the given ID doesn't exist in the reserve set"
    (with-open [system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service (system/get system ::queue-service/queue-service)
            jd        {:priority 2
                       :data     "foobar"}
            job       (queue-service/put q-service jd)
            _         (queue-service/reserve q-service 69)]
        (is (= (ssf/fail {:type ::state/job-not-found})
               (queue-service/release q-service 69 6 3)))
        (is (= #{(assoc job :reserved-by 69)}
               (:reserved-jobs @(:state-atom q-service))))
        (is (nil? (queue-service/peek-ready q-service)))))))

(deftest aof-reading-test
  (testing "it can persist contents to an AOF file and read them back"
    (with-open [old-system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service (system/get old-system ::queue-service/queue-service)]
        (queue-service/put q-service {:priority 2
                                      :data     "foobar"})
        (queue-service/reserve q-service 69)
        (queue-service/release q-service 69 0 3)
        (queue-service/put q-service {:priority 2
                                      :data     "baz"})
        (queue-service/put q-service {:priority 2
                                      :data     "quuxy"})
        (queue-service/delete q-service 69 1)

        (let [old-queue-state @(:state-atom q-service)]
          (with-open [new-system (test-helpers/open-system-with-aof-contents!
                                   [::queue-service/queue-service]
                                   (-> old-system
                                       (system/get ::test-helpers/string-persistence-service)
                                       p-service/written-contents))]
            (is (= old-queue-state
                   (-> new-system
                       (system/get ::queue-service/queue-service)
                       :state-atom
                       deref)))
            (is (= 2
                   (-> new-system
                       (system/get ::queue-service/queue-service)
                       :state-atom
                       deref
                       :job-id-counter))))))))

  (testing "when there are reserved jobs remaining"
    (with-open [old-system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service (system/get old-system ::queue-service/queue-service)]
        (queue-service/put q-service {:priority 2
                                      :data     "foobar"})
        (queue-service/put q-service {:priority 2
                                      :data     "baz"})
        (queue-service/put q-service {:priority 2
                                      :data     "quuxy"})
        (vthreads/future (queue-service/reserve q-service 69))
        (Thread/sleep 50)

        (with-open [new-system (test-helpers/open-system-with-aof-contents!
                                 [::queue-service/queue-service]
                                 (-> old-system
                                     (system/get ::test-helpers/string-persistence-service)
                                     p-service/written-contents))]
          (is (= [[2 {:id       1
                      :priority 2
                      :data     "baz"}]
                  [2 {:id       2
                      :priority 2
                      :data     "quuxy"}]
                  [2 {:id       0
                      :priority 2
                      :data     "foobar"}]]
                 (-> new-system
                     (system/get ::queue-service/queue-service)
                     :state-atom
                     deref
                     :pqueue
                     pqueue/to-seq))
              "it puts the reserved jobs back into the queue")
          (is (= 2
                 (-> new-system
                     (system/get ::queue-service/queue-service)
                     :state-atom
                     deref
                     :job-id-counter)))))))

  (testing "when there are waiting reserves"
    (with-open [old-system (test-helpers/open-system! [::queue-service/queue-service])]
      (let [q-service (system/get old-system ::queue-service/queue-service)
            job       (queue-service/put q-service {:priority 2
                                                    :data     "foobar"})]
        (queue-service/delete q-service 69 (:id job))
        (vthreads/future (queue-service/reserve q-service 69))
        (Thread/sleep 50)

        (with-open [new-system (test-helpers/open-system-with-aof-contents!
                                 [::queue-service/queue-service]
                                 (-> old-system
                                     (system/get ::test-helpers/string-persistence-service)
                                     p-service/written-contents))]
          (is (= (pqueue/create)
                 (-> new-system
                     (system/get ::queue-service/queue-service)
                     :state-atom
                     deref
                     :pqueue)))
          (is (empty? (-> new-system
                          (system/get ::queue-service/queue-service)
                          :state-atom
                          deref
                          :waiting-reserves)))
          (is (= 0
                 (-> new-system
                     (system/get ::queue-service/queue-service)
                     :state-atom
                     deref
                     :job-id-counter))))))))
