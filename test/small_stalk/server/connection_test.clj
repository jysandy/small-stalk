(ns small-stalk.server.connection-test
  (:require [clojure.test :refer :all]
            [small-stalk.server.connection :as connection]
            [small-stalk.test-helpers :as test-helpers])
  (:import (java.net Socket)))

(defn- fake-socket []
  (let [closed (atom false)]
    (proxy [Socket] []
      (isClosed [] @closed)
      (close [] (reset! closed true)))))

(deftest register-connection-test
  (with-open [system-closeable (test-helpers/open-system! [::connection/connection-registry])]
    (let [connection-registry (::connection/connection-registry (:system system-closeable))
          connection-id-1     (connection/register-connection! connection-registry "foobar")
          connection-id-2     (connection/register-connection! connection-registry "baz")]
      (is (= {connection-id-1 "foobar"
              connection-id-2 "baz"}
             (:connections @connection-registry))))))

(deftest remove-connection-test
  (with-open [system-closeable (test-helpers/open-system! [::connection/connection-registry])]
    (let [connection-registry (::connection/connection-registry (:system system-closeable))
          connection-id-1     (connection/register-connection! connection-registry "foobar")
          a-socket            (fake-socket)
          connection-id-2     (connection/register-connection! connection-registry a-socket)]
      (connection/remove-connection! connection-registry connection-id-2)
      (is (= {connection-id-1 "foobar"}
             (:connections @connection-registry)))
      (is (.isClosed a-socket)))))

(deftest remove-all-connections-test
  (with-open [system-closeable (test-helpers/open-system! [::connection/connection-registry])]
    (let [connection-registry (::connection/connection-registry (:system system-closeable))
          socket-1            (fake-socket)
          socket-2            (fake-socket)]
      (connection/register-connection! connection-registry socket-1)
      (connection/register-connection! connection-registry socket-2)
      (connection/remove-all-connections! connection-registry)
      (is (= {}
             (:connections @connection-registry)))
      (is (.isClosed socket-1))
      (is (.isClosed socket-2)))))
