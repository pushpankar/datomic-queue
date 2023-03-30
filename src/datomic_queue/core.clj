(ns datomic-queue.core
  (:require [datomic.client.api :as d]
            [portal.api :as p]))

(def client (d/client {:server-type :dev-local
                       :storage-dir :mem
                       :system "ci"}))

(d/create-database client {:db-name "dm-queue"})
(def conn (d/connect client {:db-name "dm-queue"}))

(def queue-schema [{:db/ident       ::queue-id
                    :db/valueType   :db.type/uuid
                    :db/unique      :db.unique/identity
                    :db/cardinality :db.cardinality/one}

                   {:db/ident       ::queue.head
                    :db/valueType   :db.type/ref
                    :db/cardinality :db.cardinality/one}

                   {:db/ident       ::queue.tail
                    :db/valueType   :db.type/ref
                    :db/cardinality :db.cardinality/one}

                   {:db/ident       ::queue.item
                    :db/valueType   :db.type/tuple
                    :db/tupleTypes  [:db.type/ref :db.type/ref]
                    :db/cardinality :db.cardinality/one}

                   {:db/ident       ::queue.dummy-item}

                   {:db/ident       ::data
                    :db/valueType   :db.type/ref
                    :db/unique      :db.unique/value
                    :db/cardinality :db.cardinality/many}


                   {:db/ident       ::data.data
                    :db/valueType   :db.type/string
                    :db/cardinality :db.cardinality/one}])


(d/transact conn {:tx-data queue-schema})

(defprotocol IDatomicQueue
  (dpush [this data])
  (dpop [this])
  (dpeek [this])
  (dpeek-last [this]))


(defn- peek-last-helper [db queue-id]
  (let [query '[:find (pull ?item-id [*])
                :in $ ?queue-id
                :where
                [?queue ::queue-id ?queue-id]
                [?queue ::queue.tail ?item-id]]]
    (ffirst (d/q query db queue-id))))


(def queue-empty? nil?)


(defn- one-node? [head]
  (= (-> head
       :next-data
       :db/ident)
    ::queue.dummy-item))


(defrecord DbQueue [conn id]
  IDatomicQueue
  (dpeek-last [this]
    (peek-last-helper (d/db conn) id))
  (dpush [this data]
    (tap> {:tag 'push :data (empty? (dpeek-last this))})
    (if (empty? (dpeek-last this))
      (d/transact conn {:tx-data [[:db/add "data-id" ::data.data data]
                                  [:db/add "new-item" ::queue.item ["data-id" [:db/ident ::queue.dummy-item]]]
                                  [:db/add [::queue-id id] ::queue.head "new-item"]
                                  [:db/add [::queue-id id] ::queue.tail "new-item"]]})
      (let [last-node (dpeek-last this)
            last-item-id (:db/id last-node)
            last-data-id (first (::queue.item last-node))]
        (d/transact conn {:tx-data [[:db/add "data-id" ::data.data data]
                                    [:db/add "new-item" ::queue.item ["data-id" [:db/ident ::queue.dummy-item]]]
                                    [:db/add last-item-id ::queue.item [last-data-id "new-item"]]
                                    [:db/add [::queue-id id] ::queue.tail "new-item"]]}))))
  (dpeek [_] (let [db (d/db conn)
                   [head [data next-data]] (first  (d/q '[:find ?queue-head ?item
                                             :in $ ?queue-id
                                             :where
                                             [?queue ::queue-id ?queue-id]
                                             [?queue ::queue.head ?queue-head]
                                             [?queue-head ::queue.item ?item ]
                                             [(untuple ?item) (?data-ref ?next-ref)]
                                             [?data-ref ::data.data ?data]]
                                        db
                                        id))]
               (when head
                 {:db/id head
                  :data (d/pull db '[*] data)
                  :next-data (d/pull db '[*] next-data)})))

  (dpop [this]
    (let [head (dpeek this)]
      (cond
        (queue-empty? head) nil
        (one-node? head) (d/transact conn {:tx-data [[:db/retractEntity (:db/id head)]]})
        :else (d/transact conn {:tx-data [[:db/retractEntity (:db/id head)]
                                          [:db/add [::queue-id id] ::queue.head (:db/id (:next-data head))]]})))))




(defn create-dbqueue [conn]
  (let [id (random-uuid)
        q (->DbQueue conn id)]
    (d/transact conn {:tx-data [{::queue-id id}]})
    q))


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(def test-q (create-dbqueue conn))
(dpeek test-q)
(dpush test-q "4")
(dpop test-q)
;; => 87960930222214
(d/pull (d/db (:conn test-q)) '[*] 101155069755465)


;; => {:db/id 87960930222211, :data {:db/id 87960930222210, :datomic-queue.core/data.data "1"}, :next-data #:db{:id 101155069755465, :ident :datomic-queue.core/queue.dummy-item}}
(comment


  (def p (p/open))
  (add-tap #'p/submit)
  (tap> 2)



  
  (tap> test-q)




  (dpop test-q)
  (peek-last-helper (d/db (:conn test-q)) (:id test-q))
  ;; Problem probably is properly cleaning the queue

  (tap> (dpush test-q "2"))
  (tap> (d/db conn))
  (dpop test-q)
  ;; pop is wrong. I am able to delete even after there should be nothing

  (d/transact (:conn test-q) {:tx-data [[:db/retractEntity (first (dpeek-node test-q))]]})

  (dpeek test-q)
  (dpush test-q "1")


  (ffirst [])
  (empty? nil)


  (d/transact (:conn test-q-2) {:tx-data [[:db/retractEntity 87960930222176]]})

  (ffirst (d/q '[:find ?empty-node-id
                :where
                [?empty-node-id :db/ident ::queue.dummy-item]]
            (d/db conn)))

  )
