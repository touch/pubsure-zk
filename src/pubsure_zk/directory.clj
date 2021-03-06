(ns pubsure-zk.directory
  "Directory service implementation using ZooKeeper."
  (:require [pubsure.core :as api :refer (DirectoryReader DirectoryWriter ->SourceUpdate)]
            [pubsure.utils :refer (conj-set)]
            [clojure.core.async :as async]
            [clojure.string :refer (trim)]
            [zookeeper :as zk]
            [zookeeper.internal :as zi]
            [taoensso.timbre :as timbre])
  (:import [java.net URI]
           [java.security MessageDigest]
           [org.apache.commons.codec.binary Base64]
           [java.util.concurrent CountDownLatch TimeUnit]
           [org.apache.zookeeper ZooKeeper KeeperException]))
(timbre/refer-timbre)

;;; Helper methods.

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
    (trace "Getting sources for topic" topic "at path" base-path)
    (when-let [children (zk/children @client base-path :watcher watcher)]
      (let [nodes (map (comp (partial zk/data @client) (partial str base-path "/")) children)
            sorted (if sort-ctime? (sort-by (comp - :ctime :stat) nodes) nodes)]
        (keep (comp bytes->uri :data) sorted)))))


(defn- handle-watch
  [old {:keys [watches] :as zkdir} topic {:keys [event-type path] :as event}]
  (debug "Handling data watch on topic" topic "-" event)
  (if-not (= event-type :NodeChildrenChanged)
    old
    (let [ag (get-in @watches [topic :agent])
          new (try
                (sources zkdir topic :sort-ctime? true
                         :watcher #(send-off ag handle-watch zkdir topic %))
                (catch KeeperException ke
                  (error "Could not get the sources when handling the watch on topic "
                         (str topic ".") "Keeping the old cache.\nError was:" ke)
                  :error))]
      (if (= :error new)
        old
        (let [added (remove (set old) new)
              removed (remove (set new) old)]
          (debug "Added sources for topic" topic "are" added)
          (doseq [uri (reverse added)
                  chan (get-in @watches [topic :channels])]
            (trace "Sending joined source update for" uri "regarding topic" topic "to" chan)
            (when (false? (async/put! chan (->SourceUpdate topic uri :joined)))
              (dosync (alter watches update-in [topic :channels] disj chan))
              (debug "Watch channel" chan "was closed, thus removed.")))
          (debug "Removed sources for topic" topic "are" removed)
          (doseq [uri (reverse removed)
                  chan (get-in @watches [topic :channels])]
            (when (false? (async/put! chan (->SourceUpdate topic uri :left)))
              (dosync (alter watches update-in [topic :channels] disj chan))
              (debug "Watch channel" chan "was closed, thus removed.")))
          new)))))


;;---TODO Have a periodic refresh?
(defn- refresh
  [{:keys [watches cache] :as zkdir}]
  (debug "Refreshing data using cached data.")
  (doseq [[topic uris] @cache
          uri (reverse uris)]
    (api/add-source zkdir topic uri))
  (doseq [[topic {:keys [agent]}] @watches]
    (send-off agent handle-watch zkdir topic {:event-type :NodeChildrenChanged})))


(defn- handle-global
  [connect-str timeout-msec {:keys [client] :as zkdir} {:keys [event-type keeper-state] :as event}]
  (when (= :None event-type)
    (debug "Handling global event" event)
    (case keeper-state
      :SyncConnected (do (info "Client connected, initiating refresh of cached data.")
                         (refresh zkdir))
      :Disconnected (info "Client disconnected, waiting for it to reconnect.")
      :Expired (let [watcher (zi/make-watcher (partial handle-global connect-str
                                                       timeout-msec zkdir))]
                 (info "Client expired, creating new one.")
                 (zk/close @client)
                 (reset! client (ZooKeeper. connect-str timeout-msec watcher))))))


;; cache = (ref {"topic" (uri-2 uri-1)})
;; watches = (ref {"topic" {:channels #{chan chan chan} :agent (agent (uri-2 uri-1))}})
(defrecord ZooKeeperDirectory [client config cache watches]
  DirectoryWriter
  (add-source [this topic uri]
    (debug "Adding source" uri "for topic" topic)
    (let [base64 (uri->base64 uri)]
      (dosync (when-not (get (set (get @cache topic)) uri)
                (alter cache update-in [topic] conj uri)))
      (try
        (zk/create-all @client (str (:zk-root config) "/" topic "/" base64)
                       :data (uri->bytes uri))
        (catch KeeperException ke
          (error "Could not register" uri "for topic" topic " (cache did succeed)."
                 "Will be retried on reconnect. \nError was:" ke)))))

  (remove-source [this topic uri]
    (debug "Removing source" uri "for topic" topic)
    (dosync (alter cache update-in [topic] (fn [uris] (remove (partial = uri) uris))))
    (let [base64 (uri->base64 uri)]
      (try
        (zk/delete @client (str (:zk-root config) "/" topic "/" base64))
        (catch KeeperException ke
          ;;---TODO Also keep a cache of what should be removed on reconnect/refresh?
          (error "Could not unregister" uri "for topic" topic " (cache did succeed)."
                 "\nError was:" ke)))))

  DirectoryReader
  (sources [this topic]
    (debug "Sources requested for topic" topic)
    (sources this topic :sort-ctime? false))

  (watch-sources [this topic init]
    (api/watch-sources this topic init
                       (async/chan (async/sliding-buffer (get config :subscribe-buffer 100)))))

  (watch-sources [this topic init chan]
    ;; Create agent when not yet existing for this topic.
    (debug "Watch requested for" topic "on" chan "using modifier" init)
    (when-let [ag (dosync
                    (when-not (get-in @watches [topic :agent])
                      (let [ag (agent nil :error-handler #(error "Agent error on" %1 "-" %2))]
                        (alter watches assoc-in [topic :agent] ag)
                        ag)))]
      (debug "Created agent for topic" (str topic ",")
             "getting current sources from Zookeeper and setting watch.")
      (try
        (let [current (sources this topic :sort-ctime? true
                               :watcher #(send-off ag handle-watch this topic %))]
          (send-off ag (constantly current)))
        (catch KeeperException ke
          (error "Could not get initial sources for topic" topic "."
                 "Will be retried on reconnect.\nError was:" ke))))

    ;; Add channel to topic watchers, if not already in there.
    (when (dosync
            (when-not (contains? (set (get-in @watches [topic :channels])) chan)
              (alter watches update-in [topic :channels] conj-set chan)))
      (debug "Added channel" chan "to watches of topic" topic)
      (when-let [sources (seq @(get-in @watches [topic :agent]))]
        (debug "Handling initial modifier" init "for channel" chan "on topic" topic)
        (case init
          :last (async/put! chan (->SourceUpdate topic (first sources) :joined))
          :all (doseq [uri sources] (async/put! chan (->SourceUpdate topic uri :joined)))
          :random (async/put! chan (->SourceUpdate topic (rand-nth sources) :joined))
          :none 'noop)))

    chan)

  (unwatch-sources [this topic chan]
    (debug "Unwatch requested for topic" topic "on channel" chan)
    (dosync
      (alter watches update-in [topic :channels] disj chan))
    chan))


(defn start-directory
  "Starts a Zookeeper backed DirectoryReader and DirectoryWriter. The
  following options can be specified:

   :timeout-msec - The number of milliseconds until the internal
   Zookeeper client is expired and the registered URIs through this
   `DirectoryWriter` are removed automatically. Default is 10000 (10
   seconds).

   :zk-root - The root path of where to store the topics and the
   source URIs. Default is `/pubsure`.

   :subscribe-buffer - The size of the sliding core.async buffer used
   in the `DirectoryReader/watch-sources` function, in case no
   core.async channel is supplied. Default is 100.

  The return value must be used for the `stop-directory` function."
  [connect-str & {:keys [subscribe-buffer zk-root timeout-msec]
                  :or {zk-root "/pubsure"
                       timeout-msec 10000}
                  :as config}]
  (info "Starting Zookeeper directory service client, using connection string" connect-str
        "and config" config "...")
  (assert (and connect-str (seq (trim connect-str)))
          "Zookeeper connection string cannot be nil or empty.")
  (assert (= \/ (first zk-root))
          "Zookeeper root must start with a '/'.")
  (let [client (atom nil)
        zkdir (ZooKeeperDirectory. client (assoc config :zk-root zk-root) (ref {}) (ref {}))
        watcher (zi/make-watcher (partial handle-global connect-str timeout-msec zkdir))]
    (reset! client (ZooKeeper. connect-str timeout-msec watcher))
    (info "Started Zookeeper directory service client.")
    zkdir))


(defn stop-directory
  "Given the return value of the `start-directory` function, this
  stops the Zookeeper client"
  [{:keys [client] :as zkdir}]
  (info "Stopping Zookeeper directory service client...")
  (zk/close @client)
  (info "Stopped Zookeeper directory service client."))



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
