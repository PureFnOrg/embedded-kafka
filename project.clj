(defproject org.purefn/embedded-kafka "0.1.0"
  :description "An embedded kafka broker and zookeeper server"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.stuartsierra/component "0.3.2"]
                 [me.raynes/fs "1.4.6"]

                 [org.apache.zookeeper/zookeeper "3.4.10"]
                 [org.apache.kafka/kafka_2.12 "1.0.0"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [com.stuartsierra/component.repl "0.2.0"]
                                  [org.apache.kafka/kafka-streams "1.0.0"]
                                  [org.apache.kafka/kafka-clients "1.0.0"]]
                   :source-paths ["dev"]}})
