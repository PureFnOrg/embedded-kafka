(ns org.purefn.embedded-kafka-test
  (:require [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [org.purefn.embedded-kafka :as embedded-kafka]
            [org.purefn.embedded-zookeeper :as embedded-zookeeper]
            [org.purefn.util :as k])
  (:import (java.util UUID)))

(defn- system []
  (component/system-map
   :zk (embedded-zookeeper/embedded-zk)
   :kafka (component/using
           (embedded-kafka/embedded-kafka)
           [:zk])))

(defmacro with-start
  [component & body]
  `(let [component# (component/start ~component)]
     (try
       ~@body
       (finally
         (component/stop component#)))))

(deftest simple-produce-consume-test
  (with-start (system)
    (let [group-id (str (UUID/randomUUID))
          test-props (assoc k/default-props
                            "bootstrap.servers" "localhost:9092"
                            "group.id" group-id)
          c (k/consumer test-props)
          p (k/producer test-props)
          topic "greetings"]

      (k/create-topics (k/admin-client) (k/new-topic topic 1))

      ;; Fails if we subscribe instead of assign
      (k/assign c {topic [0]})

      @(k/send-message p {:topic topic
                          :key "hello"
                          :value "kafka"})

      ;; Fails without this
      (.seekToBeginning c [(k/topic-partition topic 0)])

      (is (= [{:key "hello"
               :value "kafka"}]
             (map #(select-keys % [:key :value])
                  (k/poll c 100)))))))
