(ns hxgm30.event.dev
  (:require
    [clojure.core.async :as async]
    [clojure.tools.namespace.repl :refer [refresh]]
    [kinsky.async :as kafka]
    [kinsky.client :as kafka-client]))

(def kafka-server {:bootstrap.servers "localhost:9092"})

(defn send-msgs
  []
  (let [ch (kafka/producer kafka-server :keyword :edn)]
    (async/go
      (async/>! ch {:topic "account" :key :a :value {:data 1}})
      (async/>! ch {:topic "account" :key :b :value {:data 2}}))))

(defn rcv-msgs
  []
  (let [[rec-ch ctl-ch] (kafka/consumer
                         (assoc kafka-server
                                :group.id (str (java.util.UUID/randomUUID)))
                         :keyword
                         :edn)
        topic "account"]
    (async/put! ctl-ch {:op :subscribe :topic topic})
    (async/go-loop []
      (when-let [record (async/<! rec-ch)]
        (println (pr-str record))
        (recur)))
    (send-msgs)))


    ; (async/go-loop []
    ;   (when-let [record (async/<! rec-ch)]
    ;     (println (pr-str record))
    ;     (recur)))
