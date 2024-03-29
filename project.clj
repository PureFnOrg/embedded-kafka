(defproject org.purefn/embedded-kafka "0.3.0"
  :description "An embedded kafka broker and zookeeper server"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.stuartsierra/component "1.0.0"]
                 [me.raynes/fs "1.4.6"]
                 [org.apache.kafka/kafka_2.13 "3.3.1"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [com.stuartsierra/component.repl "0.2.0"]
                                  [org.apache.kafka/kafka-streams "3.3.1"]
                                  [org.apache.kafka/kafka-clients "3.3.1"]]
                   :source-paths ["dev" "test"]}})
