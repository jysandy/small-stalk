(ns small-stalk.persistence.service-test
  (:require [clojure.test :refer :all]
            [small-stalk.test-helpers :as test-helpers]
            [small-stalk.system :as system]
            [small-stalk.persistence.service :as p-service]
            [small-stalk.queue-service.state :as state]
            [clojure.string :as string])
  (:import (java.io File FilenameFilter)))

(def ^String test-directory "/tmp/small-stalk/compaction-test")

(defn prepare-test-directory []
  (test-helpers/make-directory test-directory)
  (test-helpers/clear-directory test-directory))

(defn spit-mutations-into-aof [mutations entries-per-file]
  (->> mutations
       (partition entries-per-file entries-per-file [])
       (map-indexed (fn [index mutations]
                      (spit (str test-directory "/" (inc index) ".aof.edn")
                            (string/join "\n" mutations))))
       doall))

(deftest compact-test
  (testing "compaction of AOF files"
    (testing "when there are two AOF files"
      (prepare-test-directory)
      (with-open [system (system/open-system!
                           (-> system/config
                               (assoc-in
                                 [::p-service/persistence-service
                                  :directory-path]
                                 test-directory)
                               (assoc-in
                                 [::p-service/persistence-service
                                  :entries-per-file]
                                 3))
                           [::p-service/persistence-service])]
        (let [persistence-service
                         (system/get system
                                     ::p-service/persistence-service)

              mutations  [{:type             ::state/put
                           :priority         2
                           :data             "foo"
                           :time-to-run-secs 3}
                          {:type             ::state/put
                           :priority         3
                           :data             "bar"
                           :time-to-run-secs 3}
                          {:type             ::state/put
                           :priority         3
                           :data             "baz"
                           :time-to-run-secs 3}
                          {:type          ::state/delete
                           :job-id        2
                           :connection-id 69}]
              state-atom (state/new-state)]
          (spit-mutations-into-aof mutations 3)
          (state/replay-mutations! state-atom mutations)
          (p-service/compact persistence-service)
          (is (= @state-atom @(p-service/load-state-atom persistence-service))
              "The persisted state should not change")

          (reset! state-atom @(state/new-state))
          (state/replay-mutations! state-atom (drop-last mutations))
          (is (= @state-atom
                 @(state/load-dump (str test-directory "/" p-service/dump-file-name)))
              "The dump file should contain data from all but the last AOF file"))))

    (testing "when there is just one AOF file"
      (prepare-test-directory)
      (with-open [system (system/open-system!
                           (-> system/config
                               (assoc-in
                                 [::p-service/persistence-service
                                  :directory-path]
                                 test-directory)
                               (assoc-in
                                 [::p-service/persistence-service
                                  :entries-per-file]
                                 3))
                           [::p-service/persistence-service])]
        (let [persistence-service
                         (system/get system
                                     ::p-service/persistence-service)

              mutations  [{:type             ::state/put
                           :priority         2
                           :data             "foo"
                           :time-to-run-secs 3}
                          {:type             ::state/put
                           :priority         3
                           :data             "bar"
                           :time-to-run-secs 3}
                          {:type             ::state/put
                           :priority         3
                           :data             "baz"
                           :time-to-run-secs 3}]
              state-atom (state/new-state)]
          (spit-mutations-into-aof mutations 3)
          (state/replay-mutations! state-atom mutations)

          (p-service/compact persistence-service)

          (is (= @state-atom @(p-service/load-state-atom persistence-service))
              "The persisted state should not change")
          (is (= @(state/new-state)
                 @(state/load-dump (str test-directory "/" p-service/dump-file-name)))
              "The dump file should contain contain the initial state"))))

    (testing "when there are more than two AOF files"
      (prepare-test-directory)
      (with-open [system (system/open-system!
                           (-> system/config
                               (assoc-in
                                 [::p-service/persistence-service
                                  :directory-path]
                                 test-directory)
                               (assoc-in
                                 [::p-service/persistence-service
                                  :entries-per-file]
                                 2))
                           [::p-service/persistence-service])]
        (let [persistence-service
                         (system/get system
                                     ::p-service/persistence-service)

              mutations  [{:type             ::state/put
                           :priority         2
                           :data             "foo"
                           :time-to-run-secs 3}
                          {:type             ::state/put
                           :priority         3
                           :data             "bar"
                           :time-to-run-secs 3}
                          {:type             ::state/put
                           :priority         3
                           :data             "baz"
                           :time-to-run-secs 3}
                          {:type          ::state/delete
                           :job-id        2
                           :connection-id 69}
                          {:type             ::state/put
                           :priority         3
                           :data             "quux"
                           :time-to-run-secs 3}]
              state-atom (state/new-state)]
          (spit-mutations-into-aof mutations 2)
          (state/replay-mutations! state-atom mutations)

          (p-service/compact persistence-service)

          (is (= @state-atom @(p-service/load-state-atom persistence-service))
              "The persisted state should not change")

          (reset! state-atom @(state/new-state))
          (state/replay-mutations! state-atom (drop-last mutations))
          (is (= @state-atom
                 @(state/load-dump (str test-directory "/" p-service/dump-file-name)))
              "The dump file should contain data from all but the last AOF file")
          (is (= ["3.aof.edn"]
                 (vec (.list (File. test-directory)
                             (reify FilenameFilter
                               (accept [_ _ filename]
                                 (boolean (re-matches #".*\.aof\.edn$" filename)))))))
              "There should be only the last AOF file left"))))

    ;; TODO: When AOF files and a dump already exist
    ))