(ns org.purefn.embedded-zookeeper
  "Embedded zookeeper component for dev/testing"
  (:require [com.stuartsierra.component :as com]
            [me.raynes.fs :as fs])
  (:import java.net.InetSocketAddress
           (org.apache.zookeeper.server ZooKeeperServer NIOServerCnxnFactory)))

(def default-port 12182)

(defrecord EmbeddedZk
    [config zk server]

  com/Lifecycle
  (start [this]
    (if server
      (do
        (println "EmbeddedZk already started")
        this)
      (let [tick-time 500
            port (or (:port config)
                     default-port)
            zk (ZooKeeperServer. (fs/temp-dir "zookeeper-snapshot")
                                 (fs/temp-dir "zookeeper-log")
                                 tick-time)
            server (NIOServerCnxnFactory.)]
        (println "Starting EmbeddedZk on port" port)
        (doto server
          ;; not sure what magic number 60 means
          (.configure (InetSocketAddress. (Integer. port)) 60)
          (.startup zk))
        (assoc this :zk zk :server server))))

  (stop [this]
    (if server
      (do
        (println "Stopping EmbeddedZk")
        (.shutdown server)
        (assoc this :zk nil :server nil))
      (do
        (println "EmbeddedZk server not running")
        this))))

(defn embedded-zk
   "Returns new EmbeddedZk record, with optional config.
     * :port  port to run server on"
  ([] (embedded-zk {}))
  ([config]
   (->EmbeddedZk config nil nil)))

(defn connect-string
  "Returns the connect string for EmbeddedZk."
  [zk]
  (str "127.0.0.1:" (.getClientPort (:zk zk))))
