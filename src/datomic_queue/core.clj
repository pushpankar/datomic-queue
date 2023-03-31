(ns datomic-queue.core
  (:require
   [datomic.client.api :as d]
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

(def queue-empty? nil?)

(defn- get-node [db queue-id node-id]
   (let [[data next-data] (ffirst (d/q '[:find ?item
                                         :in $ ?queue-id ?node-id
                                         :where
                                         [?node-id ::queue.item ?item ]
                                         [(untuple ?item) (?data-ref ?next-ref)]
                                         [?data-ref ::data.data ?data]]
                                    db
                                    queue-id
                                    node-id))]
     {:db/id node-id
      :data (d/pull db '[*] data)
      :next (d/pull db '[*] next-data)}))


(defn- one-node? [head]
  (= (-> head
       :next
       :db/ident)
    ::queue.dummy-item))

(defn- dpeek [db queue-id]
  (let [head (ffirst
               (d/q '[:find ?queue-head
                      :in $ ?queue-id
                      :where
                      [?queue ::queue-id ?queue-id]
                      [?queue ::queue.head ?queue-head]]
                 db
                 queue-id))]
    (when head
      (get-node db queue-id head))))

(defn- dpeek-last [db queue-id]
  (let [tail (ffirst  (d/q '[:find ?queue-tail
                             :in $ ?queue-id
                             :where
                             [?queue ::queue-id ?queue-id]
                             [?queue ::queue.tail ?queue-tail]]
                        db
                        queue-id))]
      (when tail
        (get-node db queue-id tail))))



(defn- dpop [conn queue-id]
  (let [db (d/db conn)
        head (dpeek db queue-id)]
    (cond
      (queue-empty? head) nil
      (one-node? head) (d/transact conn {:tx-data [[:db/retractEntity (:db/id head)]]})
      :else (d/transact conn {:tx-data [[:db/retractEntity (:db/id head)]
                                        [:db/add [::queue-id queue-id] ::queue.head (-> head :next :db/id)]]}))))

(defn- dpush [conn queue-id data]
  (let [db (d/db conn)]
    (if (empty? (dpeek-last db queue-id))
      (d/transact conn
        {:tx-data [[:db/add "data-id" ::data.data data]
                   [:db/add "new-item" ::queue.item ["data-id" [:db/ident ::queue.dummy-item]]]
                   [:db/add [::queue-id queue-id] ::queue.head "new-item"]
                   [:db/add [::queue-id queue-id] ::queue.tail "new-item"]]})
      (let [last-node (dpeek-last db queue-id)
            last-item-id (:db/id last-node)
            last-data-id (-> last-node :data :db/id)]
        (d/transact conn
          {:tx-data [[:db/add "data-id" ::data.data data]
                     [:db/add "new-item" ::queue.item ["data-id" [:db/ident ::queue.dummy-item]]]
                     [:db/add last-item-id ::queue.item [last-data-id "new-item"]]
                     [:db/add [::queue-id queue-id] ::queue.tail "new-item"]]})))))


;; API

(defn create-dbqueue [conn]
  (let [id (random-uuid)
        q {:conn  conn
           :id    id}]
    (d/transact conn {:tx-data [{::queue-id id}]})
    q))

(defn peek
  ([queue]
   (peek (d/db (:conn queue)) queue))
  ([db queue]
   (dpeek db (:id queue))))

(defn peek-last
  ([queue]
   (peek-last (d/db (:conn queue)) queue))
  ([db queue]
   (dpeek-last db (:id queue))))

(defn pop [queue]
  (dpop (:conn queue) (:id queue)))

(defn push [queue data]
  (dpush (:conn queue) (:id queue) data))


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(comment
  (def p (p/open))
  (add-tap #'p/submit)
  (tap> 2)


  (def client (d/client {:server-type :dev-local
                        :storage-dir :mem
                        :system "ci"}))

  (d/create-database client {:db-name "dm-queue"})
  (def conn (d/connect client {:db-name "dm-queue"}))
  (init-db conn)
(def test-q (create-dbqueue conn))



(peek test-q)
(push test-q "5")
(one-node? (dpeek test-q))
(dpeek-last test-q)
(dpop test-q)
(peek test-q)
(peek-last (d/db conn) test-q)

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
