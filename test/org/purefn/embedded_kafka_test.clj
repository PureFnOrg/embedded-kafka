(ns org.purefn.embedded-kafka-test
  (:require [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [org.purefn.embedded-kafka :as embedded-kafka]
            [org.purefn.embedded-zookeeper :as embedded-zookeeper]
            [org.purefn.util :as k])
  (:import (java.util UUID)))

(defn- system
  [kafka-port]
  (component/system-map
   :zk (embedded-zookeeper/embedded-zk)
   :kafka (component/using
           (embedded-kafka/embedded-kafka {:port kafka-port})
           [:zk])))

(defmacro with-start
  [component & body]
  `(let [component# (component/start ~component)]
     (try
       ~@body
       (finally
         (component/stop component#)))))

(deftest simple-produce-consume-test
  ;; Pick a random kafka broker port between 10000 and 20000 so the
  ;; tests don't collide with a proper running kafka broker or one
  ;; another
  (let [kafka-port (+ (rand-int 10000) 10000)]
    (with-start (system kafka-port)
      (let [group-id (str (UUID/randomUUID))
            test-props (assoc k/default-props
                              "bootstrap.servers" (str "localhost:" kafka-port)
                              "group.id" group-id
                              ;; this configures the starting offset
                              ;; for newly created consumer
                              ;; groups. The default is "latest" which
                              ;; puts the group at the end of the
                              ;; topic, "earliest" sets it at the
                              ;; begining
                              "auto.offset.reset" "earliest")
            topic-name "greetings"]
        (with-open [;; producers and in particular consumers need to
                    ;; be closed to prevent resource leaks
                    c (k/consumer test-props)
                    p (k/producer test-props)]

          (k/create-topics (k/admin-client) (k/new-topic topic-name 1))

          @(k/send-message p {:topic topic-name
                              :key "hello"
                              :value "kafka"})

          (k/subscribe c topic-name)

          (is (= [{:key "hello"
                   :value "kafka"}]
                 (map #(select-keys % [:key :value])
                      ;; setting timeout high as sometimes takes some
                      ;; time for the first poll of a consumer group
                      ;; to work
                      (k/poll c 10e3)))))))))
