(ns storm.ml.test
  (:use [clojure.java.io :only [reader writer]])
  (:require [oauth.client :as oauth]
            [clj-yaml.core :as yaml]
            [clj-http.client :as http]
            [clojure.data.json :as json]))


(def stream-url "https://stream.twitter.com/1/statuses/sample.json")

(def oauth-conf (yaml/parse-string (slurp ".oauth.yml")))

(def consumer (oauth/make-consumer (:consumer-key oauth-conf)
                (:consumer-secret oauth-conf)
                "http://api.twitter.com/oauth/request_token"
                "http://api.twitter.com/oauth/access_token"
                "http://api.twitter.com/oauth/authorize"
                :hmac-sha1 ))

(def credentials (oauth/credentials consumer
                   (:access-token oauth-conf)
                   (:access-token-secret oauth-conf)
                   :GET stream-url))


(defn -main []
  (let [stream (http/get stream-url {:as :stream :query-params credentials})]
    (with-open [reader (reader (stream :body))]
      (let [tweets (line-seq reader)]
        (doseq [tweet tweets]
          (println (:text tweet)))))))