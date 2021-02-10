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
    (let [test-props (assoc k/default-props
                            "bootstrap.servers" "localhost:9092"
                            "group.id" (str (UUID/randomUUID)))
          c (k/consumer test-props)
          p (k/producer test-props)
          topic "greetings"]

      (k/subscribe c topic)

      (Thread/sleep 1000)

      @(k/send-message p {:topic topic
                          :key "hello"
                          :value "kafka"})

      (is (seq (k/poll c 1000))))))
