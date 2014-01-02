(defproject storm-ml "0.0.1-SNAPSHOT"
  :source-paths ["src/clj"]
  :dependencies [
                 [commons-collections/commons-collections "3.2.1"]
                 ]
  
  :profiles {:dev
              {:dependencies [
                              [com.github.pmerienne/trident-ml "0.0.3"]
                              [storm "0.9.0.1"]
                              [org.clojure/clojure "1.4.0"]
                              [clj-http "0.7.8"]
                              [clj-yaml "0.4.0"]
                              [org.clojure/data.json "0.2.2"]
                              [clj-oauth "1.4.0"]
                              [yieldbot/marceline "0.1.0-SNAPSHOT"]
                              [org.testng/testng "6.8.5"]
                              [org.easytesting/fest-assert-core "2.0M8"]
                              [org.mockito/mockito-all "1.9.0"]
                              [org.jmock/jmock "2.6.0"]]}}
  :min-lein-version "2.0.0"
  )
