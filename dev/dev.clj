(ns dev
  "Tools for interactive development with the REPL. This file should
  not be included in a production build of the application.

  Call `(reset)` to reload modified code and (re)start the system.

  The system under development is `system`, referred from
  `com.stuartsierra.component.repl/system`.

  See also https://github.com/stuartsierra/component.repl"
  (:require
   [clojure.java.io :as io]
   [clojure.java.javadoc :refer [javadoc]]
   [clojure.pprint :refer [pprint]]
   [clojure.reflect :refer [reflect]]
   [clojure.repl :refer [apropos dir doc find-doc pst source]]
   [clojure.set :as set]
   [clojure.string :as string]
   [clojure.test :as test]
   [clojure.tools.namespace.repl :refer [refresh refresh-all clear]]
   [com.stuartsierra.component :as component]
   [com.stuartsierra.component.repl :refer [reset set-init start stop system]]
   [org.purefn.embedded-kafka :as embedded-kafka]
   [org.purefn.embedded-zookeeper :as embedded-zookeeper])
  (:import java.util.Properties
           scala.collection.JavaConversions
           org.apache.kafka.common.serialization.Serdes
           kafka.admin.AdminClient
           (org.apache.kafka.clients.producer
            KafkaProducer ProducerRecord)
           (org.apache.kafka.clients.consumer
            KafkaConsumer ConsumerRecord)
           org.apache.kafka.common.TopicPartition
           (org.apache.kafka.streams.processor
            Processor ProcessorContext ProcessorSupplier
            TopologyBuilder)
           (org.apache.kafka.streams
            KafkaStreams StreamsConfig)))

;; Do not try to load source code from 'resources' directory
(clojure.tools.namespace.repl/set-refresh-dirs "dev" "src" "test")

(defn dev-system
  "Constructs a system map suitable for interactive development."
  []
  (component/system-map
   :zk (embedded-zookeeper/embedded-zk)
   :kafka (component/using
           (embedded-kafka/embedded-kafka)
           [:zk])))

(set-init (fn [_] (dev-system)))


(def default-props
  {"bootstrap.servers" "localhost:9092"
   "acks" "all"
   "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "enable.auto.commit" true})

;;;;
;; helpers
;;;;

(defn message
  [^ConsumerRecord cr]
  {:key (.key cr)
   :value (.value cr)
   :partition (.partition cr)
   :offset (.offset cr)
   :timestamp (.timestamp cr)})

(defn topic-partition
  [^String topic partition]
  (TopicPartition. topic (int partition)))

;;;;
;; consumers
;;;;


(defn consumer
  [props]
  (KafkaConsumer. props))

(defn assign
  "Takes a consumer and a map of topic-name => [partition] and assigns
   each of the topics/partitions to the consumer."
  [^KafkaConsumer consumer topics]
  (let [topic-parts (mapcat (fn [[topic parts]]
                              (map (partial topic-partition topic) parts))
                            topics)]
    (.assign consumer topic-parts)))

(defn topics
  "Returns map of {topic-name #{partition-num}} for all extant topics."
  [^KafkaConsumer consumer]
  (let [kafka-topics (.listTopics consumer)]
    (into {}
          (map (fn [[topic part-infos]]
                 [topic
                  (set (map #(.partition %) part-infos))]))
          kafka-topics)))

(defn poll
  ([^KafkaConsumer consumer]
   (poll consumer 0))
  ([^KafkaConsumer consumer timeout]
   (when-let [msgs (seq (.poll consumer (long timeout)))]
     (map message msgs))))



;;;;
;; producers
;;;;

(defn producer
  [props]
  (KafkaProducer. props))

(defn producer-record
  ([msg]
   (ProducerRecord. (:topic msg) (:key msg) (:value msg)))
  ([topic key val]
   (ProducerRecord. topic key val)))

(defn send-message
  ([^KafkaProducer producer msg]
   (.send producer (producer-record msg)))
  ([^KafkaProducer producer topic key val]
   (.send producer (producer-record topic key val))))


;;;;
;; admin
;;;;

(defn admin-client
  ([]
   (admin-client "localhost:9092"))
  ([broker-url]
   (AdminClient/createSimplePlaintext broker-url)))

(defn groups
  "List of groups, which is I guess what the admin client calls consumer
   groups that use Kafka topic-backed offset storage, rather than
   zookeeper."
  [ac]
  (->>
   (.listAllGroupsFlattened ac)
   (JavaConversions/seqAsJavaList)
   (map #(.groupId %))))

(defn group-offsets
   "Returns map of topic offsets for a group.

   {topic-name {partition offset}}"
  [ac group-id]
  (->> (.listGroupOffsets ac group-id)
       (JavaConversions/mapAsJavaMap)
       (reduce (fn [topic-offsets [topic-partition offset]]
                 (assoc-in topic-offsets
                           [(.topic topic-partition)
                            (.partition topic-partition)]
                           offset))
               {})))


;;;;
;; dev stuff
;;;;

(defn setup-state
  []
  (def p (producer default-props))
  (def c (consumer (assoc default-props "group.id" "dev-consumer")))
  (send-message p "test-topic" "hello" "world")
  (assign c {"test-topic" [0]})
  (poll c 100)
  (send-message p "test-topic" "goodbye" "world"))
