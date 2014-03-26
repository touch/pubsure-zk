(ns eventure-zk.directory
  "Directory service implementation using ZooKeeper."
  (:require [eventure.core :as api :refer (DirectoryReader DirectoryWriter)]
            [clojure.core.async :as async]
            [zookeeper :as zk]
            [zookeeper.internal :as zi])
  (:import [java.net URI]
           [java.security MessageDigest]
           [org.apache.commons.codec.binary Base64]
           [java.util.concurrent CountDownLatch TimeUnit]
           [org.apache.zookeeper ZooKeeper]))


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


;;; Zookeeper connection loop.

(defn- connect-loop
  "Makes sure a non-expired client is in the given `client-atom`.
  Returns a function which can be called to close the client and stop
  the loop. The following options are supported:

  :timeout-msec - The number of milliseconds until a connection
  expires in the cluster.

  :heartbeat-msec - The number of milliseconds between expiry checks.
  Default is 100.

  :new-client-callback - An optional function which is called whenever
  a new client is created. The function receives the new (possibly
  still connecting) client as its sole argument. This function can for
  example be used for putting back ephemeral data.

  :watcher - An optional default event watcher. See zookeeper-clj for
  more info on this."
  [client-atom connect-str & {:keys [timeout-msec heartbeat-msec new-client-callback watcher]
                              :or {heartbeat-msec 100}}]
  (let [stop-latch (CountDownLatch. 1)
        stopped-latch (CountDownLatch. 1)
        new-zookeeper (fn []
                        (let [new-client (ZooKeeper. connect-str timeout-msec
                                                     (when watcher (zi/make-watcher watcher)))]
                          (reset! client-atom new-client)
                          (when new-client-callback (new-client-callback new-client))))
        thread-fn (fn []
                    (if (.await stop-latch heartbeat-msec TimeUnit/MILLISECONDS)
                      (do (println "Stopping Zookeeper connection loop.")
                          (when-let [client @client-atom]
                            (reset! client-atom nil)
                            (zk/close client))
                          (.countDown stopped-latch))
                      (do (when-let [client @client-atom]
                            (when (= (zk/state client) :CLOSED)
                              (println "Zookeeper connection expired, creating new.")
                              (zk/close client)
                              (new-zookeeper)))
                          (recur))))
        stop-fn (fn []
                  (.countDown stop-latch)
                  (.await stopped-latch)
                  (println "Client closed."))]
    (when-not @client-atom
      (println "Creating initial Zookeeper connection.")
      (new-zookeeper))
    (.start (Thread. thread-fn "Directory service connect loop"))
    (println "Started Zookeeper connection loop.")
    stop-fn))


;;; Directory implementation.

(defrecord ZooKeeperDirectory [client config stop-fn cache watches]
  DirectoryWriter
  (add-source [this topic uri]
    (let [base64 (uri->base64 uri)]
      (zk/create-all @client (str (:zk-root config) "/" topic "/" base64)
                     :data (uri->bytes uri))))

  (remove-source [this topic uri]
    (let [base64 (uri->base64 uri)]
      (zk/delete @client (str (:zk-root config) "/" topic "/" base64))))

  DirectoryReader
  (sources [this topic]
    (let [base-path (str (:zk-root config) "/" topic)]
      (when-let [children (zk/children @client base-path)]
        (doall (keep (comp bytes->uri :data (partial zk/data @client) (partial str base-path "/"))
                     children)))))

  (watch-sources [this topic init]
    (api/watch-sources topic init
                       (async/chan (async/sliding-buffer (get config :subscribe-buffer 10)))))

  (watch-sources [this topic init chan]
    ;; LEFT HERE
    )

  (unwatch-sources [this topic chan]))


(defn start-directory
  [connect-str & {:keys [subscribe-buffer zk-root timeout-msec]
                  :or {zk-root "/eventure"
                       timeout-msec 30000}
                  :as config}]
  (let [client (atom nil)
        stop-fn (connect-loop client connect-str
                              :timeout-msec timeout-msec
                              :new-client-callback (fn [c] (println "GOT NEW CLIENT!!!!1" c)))]
    (ZooKeeperDirectory. client (assoc config :zk-root zk-root) stop-fn nil nil)))


(defn stop-directory
  [{:keys [stop-fn] :as zkdir}]
  (stop-fn))
