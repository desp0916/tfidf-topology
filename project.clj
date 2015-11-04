(defproject tfidf-topology "0.0.1-SNAPSHOT"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm" "test/jvm"]
  :test-paths ["test/clj"]
  :javac-options     ["-target" "1.6" "-source" "1.6"]
  :resource-paths ["src/resources"]
  :aot :all
  :repositories {
                   "twitter4j" "http://twitter4j.org/maven2"
                 }

  :dependencies [
                   [org.twitter4j/twitter4j-core "3.0.3"]
                   [org.twitter4j/twitter4j-stream "3.0.3"]
                   [trident-cassandra "0.0.1-wip2"]
                   [org.slf4j/slf4j-log4j12 "1.6.1"]
                   [com.googlecode.json-simple/json-simple "1.1"]
                   [redis.clients/jedis "2.1.0"]
                   [org.apache.tika/tika-parsers "1.2"]
                   [org.apache.lucene/lucene-analyzers "3.6.2"]
                   [org.apache.lucene/lucene-spellchecker "3.6.2"]
                   [edu.washington.cs.knowitall/morpha-stemmer "1.0.4"]
                   [trident-cassandra/trident-cassandra "0.0.1-bucketwip1"]
                   [commons-collections/commons-collections "3.2.1"]]

  :profiles {:dev {:dependencies [[storm "0.8.2"]
                     [org.clojure/clojure "1.4.0"]
                     [junit/junit "4.11"]
                     [org.jmock/jmock-legacy "2.5.1"]
                     [org.mockito/mockito-all "1.8.4"]
                     [org.easytesting/fest-assert-core "2.0M8"]
                     [net.sf.opencsv/opencsv "2.3"]
                     [org.testng/testng "6.1.1"]]}}
  
  )

