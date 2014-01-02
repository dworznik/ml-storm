(ns storm.ml.twitter-counter
  (:import [backtype.storm StormSubmitter LocalCluster]
           [storm.trident TridentTopology])
  (:use [backtype.storm clojure config]
        [storm.ml.twitter]
        [clojure.java.io :only [reader writer]]
        [clojure.string :as str :only [split join]])
  (:require [oauth.client :as oauth]
            [clj-http.client :as http]
            [clj-yaml.core :as yaml]
            [clojure.data.json :as json]
            [marceline.storm.trident :as t])
  (:gen-class))


(defbolt split-text ["word"] [tuple collector]
  (let [text (.getString tuple 0)
        words (map str/lower-case (str/split text #"\W+"))]
    (doseq [w words]
      (emit-bolt! collector [w] :anchor tuple))
    (ack! collector tuple)
    ))

(defbolt tweet-text ["text"]
  [tuple collector]
  (let [tweet (json/read-str (.getString tuple 0))
        text (tweet "text")]
    (if (not (empty? text)) (emit-bolt! collector [text] :anchor tuple))
    (ack! collector tuple)))

(defbolt printer []
  [tuple collector]
  (let [text (.getString tuple 0)]
    (println (str "text: " text))
    (ack! collector tuple)))

(defbolt count-printer []
  [tuple collector]
  (let [word (.getString tuple 0)
        cnt (.getLong tuple 1)]
    (println (str word ": " cnt))
    (ack! collector tuple)))

(defbolt word-count ["word" "count"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolt
     (execute [tuple]
       (let [word (.getString tuple 0)]
         (swap! counts (partial merge-with +) {word 1})
         (emit-bolt! collector [word (@counts word)] :anchor tuple)
         (ack! collector tuple)
         ))
     (cleanup []
              ;; dump word counters on exit
              (doseq [c @counts]
                (println c))))))

(defn mk-counter-topology []
  (topology
   {"1" (spout-spec tweet-spout)}
   {"2" (bolt-spec {"1" :shuffle}
                   tweet-text
                   :p 1)
    "3" (bolt-spec {"2" :shuffle}
                   split-text
                   :p 1)
    "4" (bolt-spec {"3" :shuffle}
                   printer
                   :p 2)
    "5" (bolt-spec {"3" ["word"]} word-count :p 3)
    ; "6" (bolt-spec {"5" :shuffle} count-printer :p 3)
    }))

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "tweet-word-counter" {TOPOLOGY-DEBUG false} (mk-counter-topology))
    (Thread/sleep 10000)
    (.shutdown cluster)
    ))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology
   name
   {TOPOLOGY-DEBUG false
    TOPOLOGY-WORKERS 3}
   (mk-counter-topology)))

(defn -main
  ([]
   (run-local!))
  ([name]
   (submit-topology! name)))


