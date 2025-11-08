(ns tailrecursion.chasma
  "Transactional actor runtime for Clojure"
  (:import (java.util UUID)
           (java.util.concurrent Executors LinkedBlockingQueue ConcurrentHashMap TimeUnit CompletableFuture ExecutorService)
           (java.util.function Function)
           (java.util.concurrent.locks ReentrantLock)))

(defrecord Actor [^UUID id ^clojure.lang.Atom beh])

(defn spawn
  "Create an actor from a behavior function (variadic fn). With no args, spawns a no-op actor."
  (^Actor []
   (spawn (constantly nil)))
  (^Actor [beh]
   (->Actor (UUID/randomUUID) (atom beh))))

(def ^:private ^LinkedBlockingQueue queue (LinkedBlockingQueue.))
(def ^:private running? (atom false))
(def ^:private ^ExecutorService pool
  (delay (Executors/newFixedThreadPool
           (max 2 (.availableProcessors (Runtime/getRuntime))))))
(def ^:private pump (atom nil))

(def ^:private serializers (ConcurrentHashMap.))
(defn- ser-lock ^ReentrantLock [token]
  (.computeIfAbsent serializers token
    (reify Function (apply [_ _] (ReentrantLock. true)))))


(def ^:dynamic *self*   nil)
(def ^:dynamic *sender* nil)
(def ^:dynamic *reply*  (fn [_] (throw (ex-info "No sender to reply to" {}))))
(def ^:dynamic *tx*     nil)


(defrecord Delivery [^Actor to ^Actor from payload ser-token retries])
(defn- enqueue! [^Delivery d] (.put queue d))


(defrecord Txn [sends becomes effects])

(defn- new-tx [] (->Txn (atom []) (atom []) (atom [])))

(declare commit!)

(defn send!
  "Send a message to target. Forms:
   (send! target arg1 arg2 ...)
   (send! target {:ser token} arg1 arg2 ...)

   Outside a behavior, enqueues immediately.
   Inside a behavior (turn), buffers in *tx* and flushes on commit."
  [^Actor target & xs]
  (let [[opts payload] (if (and (seq xs) (map? (first xs)))
                         [(first xs) (next xs)]
                         [nil xs])
        d (->Delivery target *self* (vec payload) (some-> opts :ser) 0)]
    (if *tx* (swap! (:sends *tx*) conj d) (enqueue! d))
    nil))

(defn- validate-pairs [more]
  (when (odd? (count more))
    (throw (IllegalArgumentException. "become! requires actor/behavior pairs")))
  nil)

(defn- enqueue-becomes! [tx pairs]
  (doseq [[^Actor actor beh] pairs]
    (let [observed @(.-beh actor)]
      (swap! (:becomes tx) conj {:actor actor :old observed :new beh}))))

(defn- apply-become-pairs! [pairs]
  (if *tx*
    (do
      (enqueue-becomes! *tx* pairs)
      nil)
    (loop []
      (let [tx (new-tx)]
        (enqueue-becomes! tx pairs)
        (if (try
              (commit! tx)
              true
              (catch Throwable _
                false))
          nil
          (recur))))))

(defn become!
  "Schedule one or more behavior changes. Inside a turn, the changes defer to commit.
   Outside a turn, all provided actor/behavior pairs apply atomically."
  ([^Actor act new-beh]
   (apply-become-pairs! [[act new-beh]]))
  ([^Actor act new-beh & more]
   (validate-pairs more)
   (let [pairs (cons [act new-beh] (partition 2 more))]
     (apply-become-pairs! pairs))))

(defn ask
  "Request/response helper. Returns a CompletableFuture that completes with the reply.
   Inside the callee, call (*reply* value) to answer."
  [^Actor target & msg]
  (let [fut (CompletableFuture.)
        me  (spawn)]
    (reset! (.-beh me)
            (fn [v]
              (.complete fut v)
              (become! me (fn [& _] nil))))
    (enqueue! (->Delivery target me (vec msg) nil 0))
    fut))

(defmacro on-commit!
  "Run body only if the surrounding turn commits. Requires an active turn."
  [& body]
  `(if *tx*
     (do
       (swap! (:effects *tx*) conj (fn [] ~@body))
       nil)
     (throw (IllegalStateException. "on-commit requires an active turn"))))

(defn- commit! [^Txn tx]
  (doseq [{:keys [^Actor actor old new]} @(:becomes tx)]
    (when-not (compare-and-set! (.-beh actor) old new)
      (throw (ex-info "Commit conflict" {:actor actor}))))
  (doseq [f @(:effects tx)]
    (binding [*tx* nil]
      (f)))
  (doseq [^Delivery d @(:sends tx)]
    (enqueue! d)))

(defn- backoff-ms [n]
  (min 500 (long (* 5 (Math/pow 2.0 (max 0 (dec (double n))))))))

(defn- run-delivery! [^Delivery d]
  (let [^Actor act (:to d)
        beh @(.-beh act)
        tx  (new-tx)
        reply-fn (fn [v] (when *sender* (enqueue! (->Delivery *sender* *self* [v] nil 0))))
        runnable (fn []
                   (binding [*self*  act
                             *sender* (:from d)
                             *reply*  reply-fn
                             *tx*     tx]
                     (apply beh (:payload d))))]
    (try
      (if-let [t (:ser-token d)]
        (let [^ReentrantLock l (ser-lock t)]
          (.lock l)
          (try (runnable) (finally (.unlock l))))
        (runnable))
      (commit! tx)
      (catch Throwable _
        (let [n  (inc (long (:retries d)))
              ms (backoff-ms n)]
          (when (pos? ms)
            (try (.sleep TimeUnit/MILLISECONDS ms) (catch InterruptedException _)))
          (enqueue! (assoc d :retries n)))))))

(defn- pump-loop []
  (future
    (while @running?
      (when-let [^Delivery d (.poll queue 100 TimeUnit/MILLISECONDS)]
        (.submit ^ExecutorService @pool ^Runnable #(run-delivery! d))))))

(defn start!
  "Start the dispatcher (idempotent)."
  []
  (when (compare-and-set! running? false true)
    (reset! pump (pump-loop))
    :started))

(defn shutdown!
  "Stop the dispatcher and shut down the worker pool."
  []
  (when (compare-and-set! running? true false)
    (when-let [p @pump] (future-cancel p))
    (.shutdownNow ^ExecutorService @pool)
    :stopped))
