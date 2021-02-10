(ns dev
  "Tools for interactive development with the REPL. This file should
  not be included in a production build of the application.

  Call `(reset)` to reload modified code and (re)start the system.

  The system under development is `system`, referred from
  `com.stuartsierra.component.repl/system`.

  See also https://github.com/stuartsierra/component.repl"
  (:require
   clojure.tools.namespace.repl
   [com.stuartsierra.component :as component]
   [com.stuartsierra.component.repl :refer
    [reset set-init start stop system]]
   [org.purefn.embedded-kafka :as embedded-kafka]
   [org.purefn.embedded-zookeeper :as embedded-zookeeper]
   [org.purefn.util :as k]))

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

(comment

  (reset)
  (stop)
  (start)

  (def topic "farewells")

  (k/create-topics (k/admin-client) (k/new-topic topic 1))

  @(.listings (.listTopics (k/admin-client))))
