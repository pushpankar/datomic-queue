(ns datomic-queue.core-test
  (:require [clojure.test :refer :all]
            [datomic-queue.core :as q]
            [datomic.client.api :as d]))

;; Test utils
(defmacro is-not [form]
  `(is (not ~form)))

(defmacro is-eq [form1 form2]
  `(is (= ~form1 ~form2)))

(defmacro count= [coll value]
  `(= (count ~coll) ~value))


(def ^:dynamic *conn* nil)

(defn setup [f]
  (let [client (d/client {:server-type :dev-local
                          :storage-dir :mem
                          :system "ci"})
        _ (d/create-database client {:db-name "dm-test-queue"})
        conn (d/connect client {:db-name "dm-test-queue"})
        _ (q/init-db conn)]
    (binding [*conn* conn]
      (f))))


(deftest push-peek-test
  (testing "Peek on empty queue"
    (let [queue (q/create-dbqueue *conn*)]
      (is (nil? (q/dpeek queue)))))
  (testing "Peek on non-empty queue"
    (let [queue (q/create-dbqueue *conn*)
          _ (q/dpush queue "1")]
      (is-not (nil? (q/dpeek queue))))))

(deftest push-peek-last-test
  (testing "Peek last on empty queue"
    (let [queue (q/create-dbqueue *conn*)]
      (is (nil? (q/dpeek-last queue)))))
  (testing "Peek last on queue with one node"
    (let [queue (q/create-dbqueue *conn*)
          _ (q/dpush queue "1")]
      (is-not (nil? (q/dpeek-last queue))))))

(deftest push-pop-test
  (testing "Pop empty queue"
    (let [queue (q/create-dbqueue *conn*)]
      (is (nil? (q/dpop queue)))))
  (testing "Pop non-empty queue"
    (let [queue (q/create-dbqueue *conn*)
          _ (q/dpush queue "1")]
      (is-not (nil? (q/dpeek queue)))
      (is-not (nil? (q/dpop queue)))
      (is (nil? (q/dpeek queue)))))
  (testing "Drain and fill the queue"
    (let [queue (q/create-dbqueue *conn*)
          _ (q/dpush queue "1")
          _ (q/dpush queue "2")
          _ (q/dpush queue "3")]
      (while (not (nil? (q/dpeek queue)))
        (q/dpop queue))
      (is (nil? (q/dpeek queue)))
      (q/dpush queue "1")
      (is-not (nil? (q/dpeek queue))))))

(use-fixtures :once setup)
(run-all-tests)
