(ns tailrecursion.chasma
  "Transactional actor runtime for Clojure"
  (:import (java.util ArrayDeque UUID)
           (java.util.concurrent Executors LinkedBlockingQueue TimeUnit CompletableFuture ExecutorService)
           (clojure.lang Atom)))

(defrecord Actor [^UUID id ^Atom beh])
(defrecord Serializer [^ArrayDeque queue ^Atom running?])
(defrecord Lane [^Actor target ^Serializer serializer])

(defn spawn
  "Create an actor from a behavior function (variadic fn). With no args, spawns a no-op actor."
  (^Actor []
   (spawn (constantly nil)))
  (^Actor [beh]
   (->Actor (UUID/randomUUID) (atom beh))))

(defn lane
  "Create a private serialized target for actor."
  [^Actor actor]
  (->Lane actor (->Serializer (ArrayDeque.) (atom false))))

(def ^:private ^LinkedBlockingQueue queue (LinkedBlockingQueue.))
(def ^:private running? (atom false))
(def ^:private pool (atom nil))
(def ^:private pump (atom nil))

(def ^:dynamic *self*   nil)
(def ^:private ^:dynamic *self-target* nil)
(def ^:dynamic *sender* nil)
(def ^:dynamic *reply*  (fn [_] (throw (ex-info "No sender to reply to" {}))))
(def ^:dynamic *tx*     nil)


(defrecord Delivery [target ^Actor to from payload ^Serializer serializer retries])
(defn- enqueue! [^Delivery d] (.put queue d))


(defrecord Txn [sends becomes effects])

(defn- new-tx [] (->Txn (atom []) (atom []) (atom [])))

(declare commit!)

(defn- target-delivery [target]
  (if (instance? Lane target)
    [target (:target target) (:serializer target)]
    [target target nil]))

(defn- stage-or-enqueue! [^Delivery d]
  (if *tx*
    (swap! (:sends *tx*) conj d)
    (enqueue! d))
  nil)

(defn send!
  "Send a message to target.

   Outside a behavior, enqueues immediately.
   Inside a behavior (turn), buffers in *tx* and flushes on commit."
  [target & xs]
  (let [[target actor serializer] (target-delivery target)]
    (stage-or-enqueue!
      (->Delivery target actor *self-target* (vec xs) serializer 0))))

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
  [target & msg]
  (let [[target actor serializer] (target-delivery target)
        fut (CompletableFuture.)
        me  (spawn)]
    (reset! (.-beh me)
            (fn [v]
              (.complete fut v)
              (become! me (fn [& _] nil))))
    (stage-or-enqueue! (->Delivery target actor me (vec msg) serializer 0))
    fut))

(defmacro on-commit!
  "Run body only if the surrounding turn commits. Requires an active turn."
  [& body]
  `(if *tx*
     (do
       (swap! (:effects *tx*) conj (binding [*tx* nil]
                                      (bound-fn [] ~@body)))
       nil)
     (throw (IllegalStateException. "on-commit requires an active turn"))))

(defn- compact-becomes [becomes]
  (vals
    (reduce (fn [m {:keys [actor old new]}]
              (if-let [entry (get m actor)]
                (assoc m actor (assoc entry :new new))
                (assoc m actor {:actor actor :old old :new new})))
            {}
            becomes)))

(defn- lock-actors! [actors f]
  (letfn [(step [actors]
            (if-let [actor (first actors)]
              (locking actor
                (step (next actors)))
              (f)))]
    (step (sort-by :id actors))))

(defn- report-effect-failure! [^Throwable t]
  (binding [*out* *err*]
    (println "on-commit! effect failed:" (.getMessage t))))

(defn- run-commit-effects! [effects]
  (doseq [f effects]
    (try
      (binding [*tx* nil]
        (f))
      (catch Throwable t
        (report-effect-failure! t)))))

(defn- commit! [^Txn tx]
  (let [becomes (vec (compact-becomes @(:becomes tx)))
        actors  (mapv :actor becomes)]
    (lock-actors! actors
      (fn []
        (doseq [{:keys [^Actor actor old]} becomes]
          (when-not (identical? old @(.-beh actor))
            (throw (ex-info "Commit conflict" {:actor actor}))))
        (doseq [{:keys [^Actor actor new]} becomes]
          (reset! (.-beh actor) new)))))
  (doseq [^Delivery d @(:sends tx)]
    (enqueue! d))
  (run-commit-effects! @(:effects tx)))

(defn- backoff-ms [n]
  (min 500 (long (* 5 (Math/pow 2.0 (max 0 (dec (double n))))))))

(defn- retry-delivery [^Delivery d]
  (let [n  (inc (long (:retries d)))
        ms (backoff-ms n)]
    (when (pos? ms)
      (try (.sleep TimeUnit/MILLISECONDS ms) (catch InterruptedException _)))
    (assoc d :retries n)))

(defn- run-turn! [^Delivery d]
  (let [^Actor act (:to d)
        beh @(.-beh act)
        tx  (new-tx)
        reply-fn (fn [v] (when *sender* (send! *sender* v)))
        runnable (fn []
                   (binding [*self*  act
                             *self-target* (:target d)
                             *sender* (:from d)
                             *reply*  reply-fn
                             *tx*     tx]
                     (apply beh (:payload d))))]
    (runnable)
    (commit! tx)))

(defn- run-with-retries! [^Delivery d]
  (loop [d d]
    (when-let [retry (try
                       (run-turn! d)
                       nil
                       (catch Throwable _
                         (retry-delivery d)))]
      (recur retry))))

(declare drain-serializer!)

(defn- schedule-serializer! [^Serializer s]
  (when-let [^ExecutorService executor @pool]
    (.submit executor ^Runnable #(drain-serializer! s))))

(defn- enqueue-serialized! [^Serializer s ^Delivery d]
  (let [start? (locking s
                 (.addLast ^ArrayDeque (:queue s) d)
                 (when-not @(:running? s)
                   (reset! (:running? s) true)
                   true))]
    (when start?
      (schedule-serializer! s))))

(defn- next-serialized-delivery [^Serializer s]
  (locking s
    (if-let [d (.pollFirst ^ArrayDeque (:queue s))]
      d
      (do
        (reset! (:running? s) false)
        nil))))

(defn- drain-serializer! [^Serializer s]
  (loop []
    (when-let [d (next-serialized-delivery s)]
      (run-with-retries! d)
      (recur))))

(defn- run-delivery! [^Delivery d]
  (if-let [s (:serializer d)]
    (enqueue-serialized! s d)
    (when-let [retry (try
                       (run-turn! d)
                       nil
                       (catch Throwable _
                         (retry-delivery d)))]
      (enqueue! retry))))

(defn- pump-loop []
  (future
    (while @running?
      (when-let [^Delivery d (.poll queue 100 TimeUnit/MILLISECONDS)]
        (if-let [s (:serializer d)]
          (enqueue-serialized! s d)
          (when-let [^ExecutorService executor @pool]
            (.submit executor ^Runnable #(run-delivery! d))))))))

(defn start!
  "Start the dispatcher (idempotent)."
  []
  (when (compare-and-set! running? false true)
    (reset! pool (Executors/newFixedThreadPool
                   (max 2 (.availableProcessors (Runtime/getRuntime)))))
    (reset! pump (pump-loop))
    :started))

(defn shutdown!
  "Stop the dispatcher and shut down the worker pool."
  []
  (when (compare-and-set! running? true false)
    (when-let [p @pump] (future-cancel p))
    (when-let [^ExecutorService executor @pool]
      (.shutdownNow executor))
    (.clear queue)
    (reset! pump nil)
    (reset! pool nil)
    :stopped))
