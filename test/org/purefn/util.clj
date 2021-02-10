(ns org.purefn.util
  (:import (org.apache.kafka.clients.admin AdminClient NewTopic)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord)
           (org.apache.kafka.common TopicPartition)))

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

(defn new-topic
  ([^String topic partition-count]
   (new-topic topic partition-count 1))
  ([^String topic partition-count replication-factor]
   (NewTopic. topic (int partition-count) (short replication-factor))))

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

(defn subscribe
  [^KafkaConsumer consumer topic & topics]
  (.subscribe consumer (into [topic] topics)))

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
   (AdminClient/create {"bootstrap.servers" broker-url})))

(defn groups
  "List of groups, which is I guess what the admin client calls consumer
   groups that use Kafka topic-backed offset storage, rather than
   zookeeper."
  [ac]
  (->>
   (.listAllGroupsFlattened ac)
   (map #(.groupId %))))

(defn group-offsets
   "Returns map of topic offsets for a group.

   {topic-name {partition offset}}"
  [ac group-id]
  (reduce (fn [topic-offsets [topic-partition offset-metadata]]
            (assoc-in topic-offsets
                      [(.topic topic-partition)
                       (.partition topic-partition)]
                      (.offset offset-metadata)))
          {}
          @(.partitionsToOffsetAndMetadata
            (.listConsumerGroupOffsets ac group-id))))

(defn create-topics
  [ac topic & topics]
  (.createTopics ac (into [topic] topics)))

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

#_(defn stupid-processor
    []
    (reify Processor
      (init [_ _]
        (println "Println I'm inited!!!!")
        nil)
      (process [_ _ _]
        (println "Println It called me!!!")
        nil)
      (punctuate [_ _] nil)
      (close [_]
        (println "Println It closed me!!!")
        nil)))

#_(defn stupid-supplier
    []
    (reify
      ProcessorSupplier
      (get [_]
        (stupid-processor))))

#_(defn stupid-topology
    []
    (let [tb (TopologyBuilder.)]
      (.. tb
          (addSource "stupid-source"
                     (into-array String ["stupid-topic"]))
          (addProcessor "stupid-processor" (stupid-supplier)
                        (into-array String ["stupid-source"])))))


#_(defn stupid-streams-config
    []
    (StreamsConfig. {"application.id" "stupid-app"
                     "bootstrap.servers" "localhost:9092"
                     "key.serde" (.getClass (Serdes/String))
                     "value.serde" (.getClass (Serdes/String))}))

#_(defn start-stupid-streams
    []
    (let [p (producer default-props)
          _ (send-message p "stupid-topic" "hello" "world") ;; make sure the topic exists
          ks (KafkaStreams. (stupid-topology) (stupid-streams-config))]
      (.close p)
      (.start ks)
      (def ks ks)
      ks))
