(defproject datomic-queue "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.datomic/dev-local "1.0.243"]]
  :repl-options {:init-ns datomic-queue.core}
  :profiles {:dev {:dependencies [[djblue/portal "0.37.1"]]
                   :repl-options {:init-ns datomic-queue.core}}})
