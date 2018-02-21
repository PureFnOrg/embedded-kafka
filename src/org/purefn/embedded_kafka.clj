(ns org.purefn.embedded-kafka
    "Embedded Kafka component for dev/testing"
  (:require [com.stuartsierra.component :as com]
            [me.raynes.fs :as fs]
            [org.purefn.embedded-zookeeper :as zk])
  (:import (kafka.server KafkaServerStartable KafkaConfig)
           org.apache.kafka.common.utils.Time
           scala.collection.JavaConversions))

(def default-port 9092)

(defn props
  "Creates something usable as props (map of string=>string) from a
   config map."
  [config]
  (assert (not (some coll? (vals config))))
  (zipmap (map str (keys config))
          (map str (vals config))))

(defrecord EmbeddedKafka
    [config zk server]

  com/Lifecycle
  (start [this]
    (cond server
          (do (println "EmbeddedKafka already started")
              this)
          zk
          (let [zk-connect (zk/connect-string zk)
                port (or (:port config) default-port)
                broker-id (:broker-id config)
                log-dir (or (:log-dir config)
                            (str (fs/temp-dir "kafka" "log" 100)))
                config (KafkaConfig.
                        (cond->
                            (merge
                             (props (dissoc config :port :broker-id :log-dir))
                             {"listeners" (str "PLAINTEXT://localhost:" port)
                              "log.dir" log-dir
                              "zookeeper.connect" zk-connect
                              "offsets.topic.replication.factor" "1"})
                          broker-id
                          (assoc "broker.id" (Integer. broker-id))))
                srv (KafkaServerStartable. config)]
            (println "Starting EmbeddedKafka")
            (.startup srv)
            (println "EmbeddedKafka started on port" port)
            (assoc this :server srv))
          :else
          (do
            (println "EmbeddedKafka missing required zookeeper component; can't start")
            this)))

  (stop [this]
    (if server
      (do (println "Stopping EmbeddedKafka")
          (.shutdown server)
          (.awaitShutdown server)
          (assoc this :server nil))
      (do (println "EmbeddedKafka not started")
          this))))


(defn embedded-kafka
  "Takes an optional config map with following keys:

    * :port       Port to bind to on localhost [9092]
    * :log-dir    Directory to store log data [random tmp dir]
    * :broker-id  Kafka broker id [auto]

   Along with whatever other keys you want, which will be included."
  ([]
   (embedded-kafka {}))
  ([config]
   (->EmbeddedKafka config nil nil)))
