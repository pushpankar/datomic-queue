(ns datomic-queue.core
  (:require [datomic.client.api :as d]
            [portal.api :as p]))

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


(defn init-db [conn]
  (d/transact conn {:tx-data queue-schema}))

(defprotocol IDatomicQueue
  (dpush [this data])
  (dpop [this])
  (dpeek [this])
  (dpeek-last [this]))


(def queue-empty? nil?)

(defn get-node
  ([db queue node-id]
   (let [[data next-data] (ffirst (d/q '[:find ?item
                                         :in $ ?queue-id ?node-id
                                         :where
                                         [?node-id ::queue.item ?item ]
                                         [(untuple ?item) (?data-ref ?next-ref)]
                                         [?data-ref ::data.data ?data]]
                                    db
                                    (:id queue)
                                    node-id))]
     {:db/id node-id
      :data (d/pull db '[*] data)
      :next (d/pull db '[*] next-data)}))
  ([queue node-id]
   (get-node (d/db (:conn queue)) queue node-id)))


(defn- one-node? [head]
  (= (-> head
       :next
       :db/ident)
    ::queue.dummy-item))


(defrecord DbQueue [conn id]
  IDatomicQueue
  (dpeek-last [this]
    (let [db (d/db conn)
          tail (ffirst  (d/q '[:find ?queue-tail
                               :in $ ?queue-id
                               :where
                               [?queue ::queue-id ?queue-id]
                               [?queue ::queue.tail ?queue-tail]]
                          db
                          id))]
      (when tail
        (get-node db this tail))))
  (dpush [this data]
    (if (empty? (dpeek-last this))
      (d/transact conn {:tx-data [[:db/add "data-id" ::data.data data]
                                  [:db/add "new-item" ::queue.item ["data-id" [:db/ident ::queue.dummy-item]]]
                                  [:db/add [::queue-id id] ::queue.head "new-item"]
                                  [:db/add [::queue-id id] ::queue.tail "new-item"]]})
      (let [last-node (dpeek-last this)
            last-item-id (:db/id last-node)
            last-data-id (-> last-node :data :db/id)]
        (d/transact conn {:tx-data [[:db/add "data-id" ::data.data data]
                                    [:db/add "new-item" ::queue.item ["data-id" [:db/ident ::queue.dummy-item]]]
                                    [:db/add last-item-id ::queue.item [last-data-id "new-item"]]
                                    [:db/add [::queue-id id] ::queue.tail "new-item"]]}))))
  (dpeek [this] (let [db (d/db conn)
                   head (ffirst
                          (d/q '[:find ?queue-head
                                 :in $ ?queue-id
                                 :where
                                 [?queue ::queue-id ?queue-id]
                                 [?queue ::queue.head ?queue-head]]
                            db
                            id))]
               (when head
                 (get-node db this head))))

  (dpop [this]
    (let [head (dpeek this)]
      (cond
        (queue-empty? head) nil
        (one-node? head) (d/transact conn {:tx-data [[:db/retractEntity (:db/id head)]]})
        :else (d/transact conn {:tx-data [[:db/retractEntity (:db/id head)]
                                          [:db/add [::queue-id id] ::queue.head (-> head :next :db/id)]]})))))




(defn create-dbqueue [conn]
  (let [id (random-uuid)
        q (->DbQueue conn id)]
    (d/transact conn {:tx-data [{::queue-id id}]})
    q))

#_(defn peek
  ([queue])
  ([db queue]))

#_(defn peek-last
  ([queue])
  ([db queue]))

#_(defn pop [queue])
#_(defn push [queue])

;; API

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(comment
  (def p (p/open))
  (add-tap #'p/submit)
  (tap> 2)
  (def test-q (create-dbqueue conn))

  (def client (d/client {:server-type :dev-local
                        :storage-dir :mem
                        :system "ci"}))

  (d/create-database client {:db-name "dm-queue"})
  (def conn (d/connect client {:db-name "dm-queue"}))
  (init-db conn)



(dpeek-last test-q)
(dpush test-q "5")
(one-node? (dpeek test-q))
(dpeek-last test-q)
(dpop test-q)
(dpeek test-q)

(let [queue (create-dbqueue conn)
          _ (dpush queue "1")]
      (dpeek queue))



  (peek-last-helper (d/db (:conn test-q)) (:id test-q))
  ;; Problem probably is properly cleaning the queue

  (tap> (dpush test-q "2"))
  (tap> (d/db conn))
  (dpop test-q)

  (dpeek test-q)
  (dpush test-q "1")
  (dpeek-last test-q)


  (ffirst [])
  (empty? nil)


  (d/transact (:conn test-q-2) {:tx-data [[:db/retractEntity 87960930222176]]})

  (ffirst (d/q '[:find ?empty-node-id
                :where
                [?empty-node-id :db/ident ::queue.dummy-item]]
            (d/db conn)))

  )
