(ns storm.ml.twitter-sentiment
  (:import [backtype.storm StormSubmitter LocalCluster LocalDRPC]
           [storm.trident TridentTopology]
           [com.github.pmerienne.trident.ml.nlp TwitterSentimentClassifier])
  (:use storm.ml.twitter
        [backtype.storm clojure config]
        [clojure.java.io :only [reader writer]]
        [clojure.string :as str :only [split join]])
  (:require [clojure.data.json :as json]
            [marceline.storm.trident :as t])
  (:gen-class))


(t/deftridentfn printer
  [tuple collector]
  (println tuple))

(t/deftridentfn tweet-reader
                [tuple collector]
                (when-let [data (t/first tuple)]
                  (if-let [text ((json/read-str data) "text")]
                    (t/emit-fn collector text))))

(t/deftridentfn classify-tweet
                [tuple collector]
                (let [classifier (TwitterSentimentClassifier.)]
                  (.execute classifier tuple collector)))

(defn mk-sentiment-topology []
  (let [trident-topology (TridentTopology.)
        tweet-sentiment (-> (t/new-stream trident-topology "tweet-texts" tweet-spout)
                        (t/each ["tweet"]
                                tweet-reader
                                ["text"])
                        (t/each ["text"]
                                classify-tweet
                                ["sentiment"])
                        (t/each ["sentiment"]
                                printer
                                []))]   
      (.build trident-topology)))


(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "tweet-sentiment" {TOPOLOGY-DEBUG false} (mk-sentiment-topology))
    (Thread/sleep 10000)
    (.shutdown cluster)
    ))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology
   name
   {TOPOLOGY-DEBUG false
    TOPOLOGY-WORKERS 3}
   (mk-sentiment-topology)))

(defn -main
  ([]
   (run-local!))
  ([name]
   (submit-topology! name)))


