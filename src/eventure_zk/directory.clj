(ns eventure-zk.directory
  "Directory service implementation using ZooKeeper."
  (:require [eventure.core :as api :refer (DirectoryReader DirectoryWriter ->SourceUpdate)]
            [clojure.core.async :as async]
            [zookeeper :as zk]
            [zookeeper.internal :as zi])
  (:import [java.net URI]
           [java.security MessageDigest]
           [org.apache.commons.codec.binary Base64]
           [java.util.concurrent CountDownLatch TimeUnit]
           [org.apache.zookeeper ZooKeeper KeeperException]))


;;; Helper methods.

(defn- conj-set
  "Ensures a conj results in a set."
  [coll val]
  (set (conj coll val)))


(defn- uri->bytes
  [^URI uri]
  (when uri
    (.. uri toString (getBytes "UTF-8"))))


(defn- bytes->uri
  [bytes]
  (when bytes
    (URI. (String. bytes "UTF-8"))))


(defn- uri->base64
  [^URI uri]
  (let [sha256 (MessageDigest/getInstance "SHA-256")]
    (Base64/encodeBase64URLSafeString (.digest sha256 (uri->bytes uri)))))


;;; Directory implementation.

(defn- sources
  [{:keys [client config] :as zkdir} topic & {:keys [sort-ctime? watcher]}]
  (let [base-path (str (:zk-root config) "/" topic)]
    (when-let [children (zk/children @client base-path :watcher watcher)]
      (let [nodes (map (comp (partial zk/data @client) (partial str base-path "/")) children)
            sorted (if sort-ctime? (sort-by (comp - :ctime :stat) nodes) nodes)]
        (keep (comp bytes->uri :data) sorted)))))


(defn- handle-watch
  [old {:keys [watches] :as zkdir} topic {:keys [event-type path] :as event}]
  (println "Handling watch" event)
  (if-not (= event-type :NodeChildrenChanged)
    old
    (let [ag (get-in @watches [topic :agent])
          channels (get-in @watches [topic :channels])
          new (try
                (sources zkdir topic :sort-ctime? true
                         :watcher #(send-off ag handle-watch zkdir topic %))
                (catch KeeperException ke
                  (println "Could not get the sources when handling the watch on topic" topic
                           "\nError was:" ke)))
          added (remove (set old) new)
          removed (remove (set new) old)]
      (if-not new
        old
        (do (doseq [uri (reverse added)
                    chan channels]
              (when-not (async/put! chan (->SourceUpdate topic uri :joined))
                (dosync (alter watches update-in [topic :channels] disj chan))))
            (doseq [uri (reverse removed)
                    chan channels]
              (when-not (async/put! chan (->SourceUpdate topic uri :left))
                (dosync (alter watches update-in [topic :channels] disj chan))))
            new)))))


(defn- refresh
  [{:keys [watches cache] :as zkdir}]
  (println "Refreshing data.")
  (doseq [[topic uris] @cache
          uri (reverse uris)]
    (api/add-source zkdir topic uri))
  (doseq [[topic {:keys [agent]}] @watches]
    (send-off agent handle-watch zkdir topic {:event-type :NodeChildrenChanged})))


(defn- handle-global
  [connect-str timeout-msec {:keys [client] :as zkdir} {:keys [event-type keeper-state] :as event}]
  (if (= :None event-type)
    (case keeper-state
      :SyncConnected (do (println "Client connected.")
                         (refresh zkdir))
      :Disconnected (println "Client disconnected.")
      :Expired (let [watcher (zi/make-watcher (partial handle-global connect-str
                                                       timeout-msec zkdir))]
                 (println "Client expired, creating new one.")
                 (zk/close @client)
                 (reset! client (ZooKeeper. connect-str timeout-msec watcher))))))


;; cache = (ref {"topic" (uri-2 uri-1)})
;; watches = (ref {"topic" {:channels #{chan chan chan} :agent (agent (uri-2 uri-1))}})
(defrecord ZooKeeperDirectory [client config cache watches]
  DirectoryWriter
  (add-source [this topic uri]
    (let [base64 (uri->base64 uri)]
      (dosync (when-not (get (set (get @cache topic)) uri)
                (alter cache update-in [topic] conj uri)))
      (try
        (zk/create-all @client (str (:zk-root config) "/" topic "/" base64)
                       :data (uri->bytes uri))
        (catch KeeperException ke
          (println "Could not register" uri "for topic" topic " (cache did succeed)."
                   "Will be retried on reconnect. \nError was:" ke)))))

  (remove-source [this topic uri]
    (dosync (alter cache update-in [topic] (fn [uris] (remove (partial = uri) uris))))
    (let [base64 (uri->base64 uri)]
      (try
        (zk/delete @client (str (:zk-root config) "/" topic "/" base64))
        (catch KeeperException ke
          ;;---TODO Also keep a cache of what should be removed on reconnect/refresh?
          (println "Could not unregister" uri "for topic" topic " (cache did succeed)."
                   "\nError was:" ke)))))

  DirectoryReader
  (sources [this topic]
    (sources this topic :sort-ctime? false))

  (watch-sources [this topic init]
    (api/watch-sources this topic init
                       (async/chan (async/sliding-buffer (get config :subscribe-buffer 10)))))

  (watch-sources [this topic init chan]
    ;; Create agent when not yet existing for this topic.
    (when-let [ag (dosync
                    (when-not (get-in @watches [topic :agent])
                      (let [ag (agent nil :error-handler
                                      (fn [ag ex]
                                        (println "Agent error for ag:" ag)
                                        (.printStackTrace ex)))]
                        (alter watches assoc-in [topic :agent] ag)
                        ag)))]
      (try
        (let [current (sources this topic :sort-ctime? true
                               :watcher #(send-off ag handle-watch this topic %))]
          (send-off ag (constantly current)))
        (catch KeeperException ke
          (println "Could not get initial sources for topic" topic "."
                   "Will be retried on reconnect.\nError was:" ke))))

    ;; Add channel to topic watchers, if not already in there.
    (when (dosync
            (when-not (contains? (set (get-in @watches [topic :channels])) chan)
              (alter watches update-in [topic :channels] conj-set chan)))
      (when-let [sources (seq @(get-in @watches [topic :agent]))]
        (case init
          :last (async/put! chan (->SourceUpdate topic (first sources) :joined))
          :all (doseq [uri sources] (async/put! chan (->SourceUpdate topic uri :joined)))
          :random (async/put! chan (->SourceUpdate topic (rand-nth sources) :joined)))))

    chan)

  (unwatch-sources [this topic chan]
    (dosync
      (alter watches update-in [topic] disj chan))
    chan))


(defn start-directory
  [connect-str & {:keys [subscribe-buffer zk-root timeout-msec]
                  :or {zk-root "/eventure"
                       timeout-msec 30000}
                  :as config}]
  (let [client (atom nil)
        zkdir (ZooKeeperDirectory. client (assoc config :zk-root zk-root) (ref {}) (ref {}))
        watcher (zi/make-watcher (partial handle-global connect-str timeout-msec zkdir))]
    (reset! client (ZooKeeper. connect-str timeout-msec watcher))
    zkdir))


(defn stop-directory
  [{:keys [client] :as zkdir}]
  (zk/close @client))



;; Zookeeper connection loop.

;; (defn- connect-loop
;;   "Makes sure a non-expired client is in the given `client-atom`.
;;   Returns a function which can be called to close the client and stop
;;   the loop. The following options are supported:

;;   :timeout-msec - The number of milliseconds until a connection
;;   expires in the cluster.

;;   :heartbeat-msec - The number of milliseconds between expiry checks.
;;   Default is 100.

;;   :new-client-callback - An optional function which is called whenever
;;   a new client is created. The function receives the new (possibly
;;   still connecting) client as its sole argument. This function can for
;;   example be used for putting back ephemeral data.

;;   :watcher - An optional default event watcher. See zookeeper-clj for
;;   more info on this."
;;   [client-atom connect-str & {:keys [timeout-msec heartbeat-msec new-client-callback watcher]
;;                               :or {heartbeat-msec 100}}]
;;   (let [stop-latch (CountDownLatch. 1)
;;         stopped-latch (CountDownLatch. 1)
;;         new-zookeeper (fn []
;;                         (let [new-client (ZooKeeper. connect-str timeout-msec
;;                                                      (when watcher (zi/make-watcher watcher)))]
;;                           (reset! client-atom new-client)
;;                           (when new-client-callback (future (new-client-callback new-client)))))
;;         thread-fn (fn []
;;                     (if (.await stop-latch heartbeat-msec TimeUnit/MILLISECONDS)
;;                       (do (println "Stopping Zookeeper connection loop.")
;;                           (when-let [client @client-atom]
;;                             (reset! client-atom nil)
;;                             (zk/close client))
;;                           (.countDown stopped-latch))
;;                       (do (when-let [client @client-atom]
;;                             (when (= (zk/state client) :CLOSED)
;;                               (println "Zookeeper connection expired, creating new.")
;;                               (zk/close client)
;;                               (new-zookeeper)))
;;                           (recur))))
;;         stop-fn (fn []
;;                   (.countDown stop-latch)
;;                   (.await stopped-latch)
;;                   (println "Client closed."))]
;;     (when-not @client-atom
;;       (println "Creating initial Zookeeper connection.")
;;       (new-zookeeper))
;;     (.start (Thread. thread-fn "Directory service connect loop"))
;;     (println "Started Zookeeper connection loop.")
;;     stop-fn))


;; (let [client (atom nil)
;;       stop-fn (atom nil)
;;       zkdir (ZooKeeperDirectory. client (assoc config :zk-root zk-root) stop-fn nil (ref {}))]
;;   (reset! stop-fn (connect-loop client connect-str
;;                                 :timeout-msec timeout-msec
;;                                 :new-client-callback (partial new-client zkdir)
;;                                 :watcher (fn [event] (prn 'GLOBAL event))))
;;   zkdir)
