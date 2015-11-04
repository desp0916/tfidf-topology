(ns storm.cookbook.tfif.TermTopology
  (:use [clojure.test])
  (:require [backtype.storm [testing :as t]])
  (:use clojure.java.shell)
  (:import [storm.cookbook.tfidf TermTopology])
  (:import [backtype.storm.generated KillOptions])
  (:import [backtype.storm.utils Time])
  (:use [storm.trident testing])
  (:use [backtype.storm util config])
  )

(bootstrap-imports)

(defn bootstrap-db []
  (sh "/Users/admin/Downloads/Development/dsc-cassandra-1.1.6/bin/cassandra-cli" "-f" "dropAndCreateSchema.txt")
  )

(defn with-topology-debug* [cluster topo body-fn]
  (t/submit-local-topology (:nimbus cluster) "tester" {TOPOLOGY-DEBUG true} (.build topo))
  (body-fn)
  (.killTopologyWithOpts (:nimbus cluster) "tester" (doto (KillOptions.) (.set_wait_secs 0)))
  )

(defmacro with-topology-debug [[cluster topo] & body]
  `(with-topology-debug* ~cluster ~topo (fn [] ~@body)))

(deftest test-tfidf
  ;(bootstrap-db)
    (t/with-local-cluster [cluster]
      (with-drpc [drpc]
        (letlocals
          (bind feeder (feeder-spout ["url"]))
          (bind topo (TermTopology/buildTopology feeder drpc)) 
          (with-topology-debug [cluster topo]
              (feed feeder [["doc01"] ["doc02"] ["doc03"] ["doc04"] ["doc05"]])
              (is (= [["twitter" 5]] (exec-drpc drpc "dQuery" "twitter")))
              (is (= [["area" 3]] (exec-drpc drpc "dfQuery" "area")))
              (is (= [["doc01" "area" 0.44628710262841953]] (exec-drpc drpc "tfidfQuery" "doc01 area")))  
            )
          )
        )
      )
  )

;(defn -main
;  (run-tests ))


