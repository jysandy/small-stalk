(ns small-stalk.queue-service.priority-queue-test
  (:require [clojure.test :refer :all]
            [small-stalk.queue-service.priority-queue :as pqueue]))

(defn- call-repeatedly [x f n]
  (if (<= n 0)
    x
    (recur (f x) f (dec n))))

(deftest push-pop-and-peek-test
  (let [queue (-> (pqueue/create)
                  (pqueue/push 10 "foo")
                  (pqueue/push 5 "bar")
                  (pqueue/push 1 "baz"))]
    (is (= "baz" (pqueue/peek queue)))
    (is (= "bar"
           (-> queue
               (pqueue/pop)
               (pqueue/peek)))))

  (let [queue (-> (pqueue/create)
                  (pqueue/push 10 "foo")
                  (pqueue/push 5 "quuxy")
                  (pqueue/push 5 "bar")
                  (pqueue/push 1 "baz"))]
    (is (= "quuxy"
           (-> queue
               (pqueue/pop)
               (pqueue/peek))))
    (is (= "bar"
           (-> queue
               (call-repeatedly pqueue/pop 2)
               (pqueue/peek))))
    (is (= "foo"
           (-> queue
               (call-repeatedly pqueue/pop 3)
               (pqueue/peek))))
    (is (nil? (pqueue/peek (call-repeatedly queue pqueue/pop 4))))
    (is (= (pqueue/create)
           (call-repeatedly queue pqueue/pop 4)))
    (is (= [[1 "baz"] [5 "quuxy"] [5 "bar"] [10 "foo"]]
           (pqueue/to-seq queue))))

  (let [queue (-> (pqueue/create)
                  (pqueue/push 10 {:foo "bar"})
                  (pqueue/push 5 #{'baz})
                  (pqueue/push 5 ["quux" :quuxy])
                  (pqueue/push 1 :fart))]

    (is (= [[1 :fart] [5 #{'baz}] [5 ["quux" :quuxy]] [10 {:foo "bar"}]]
           (pqueue/to-seq queue)))
    (is (= :fart
           (-> queue
               (pqueue/peek))))
    (is (= #{'baz}
           (-> queue
               (pqueue/pop)
               (pqueue/peek))))
    (is (= ["quux" :quuxy]
           (-> queue
               (call-repeatedly pqueue/pop 2)
               (pqueue/peek))))
    (is (= {:foo "bar"}
           (-> queue
               (call-repeatedly pqueue/pop 3)
               (pqueue/peek))))))

(deftest delete-by-test
  (let [queue          (-> (pqueue/create)
                           (pqueue/push 10 1)
                           (pqueue/push 5 2)
                           (pqueue/push 1 3)
                           (pqueue/push 1 4))
        filtered-queue (pqueue/delete-by even? queue)]
    (is (= 3 (pqueue/peek filtered-queue)))
    (is (= [[1 3] [10 1]] (pqueue/to-seq filtered-queue)))
    (is (= (-> (pqueue/create)
               (pqueue/push 10 1)
               (pqueue/push 1 3))
           filtered-queue))))

(deftest find-by-test
  (let [queue          (-> (pqueue/create)
                           (pqueue/push 10 1)
                           (pqueue/push 5 2)
                           (pqueue/push 1 3)
                           (pqueue/push 1 4))]
    (is (= [3 1] (pqueue/find-by odd? queue)))
    (is (= [4 2] (pqueue/find-by even? queue)))))
