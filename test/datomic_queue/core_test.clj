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
      (is (nil? (q/peek queue)))))
  (testing "Peek on non-empty queue"
    (let [queue (q/create-dbqueue *conn*)
          _ (q/push queue "1")]
      (is-not (nil? (q/peek queue))))))

(deftest push-peek-last-test
  (testing "Peek last on empty queue"
    (let [queue (q/create-dbqueue *conn*)]
      (is (nil? (q/peek-last queue)))))
  (testing "Peek last on queue with one node"
    (let [queue (q/create-dbqueue *conn*)
          _ (q/push queue "1")]
      (is-not (nil? (q/peek-last queue))))))

(deftest push-pop-test
  (testing "Pop empty queue"
    (let [queue (q/create-dbqueue *conn*)]
      (is (nil? (q/pop queue)))))
  (testing "Pop non-empty queue"
    (let [queue (q/create-dbqueue *conn*)
          _ (q/push queue "1")]
      (is-not (nil? (q/peek queue)))
      (is-not (nil? (q/pop queue)))
      (is (nil? (q/peek queue)))))
  (testing "Drain and fill the queue"
    (let [queue (q/create-dbqueue *conn*)
          _ (q/push queue "1")
          _ (q/push queue "2")
          _ (q/push queue "3")]
      (while (not (nil? (q/peek queue)))
        (q/pop queue))
      (is (nil? (q/peek queue)))
      (q/push queue "1")
      (is-not (nil? (q/peek queue))))))

(use-fixtures :once setup)
(run-all-tests)
