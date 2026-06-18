(ns tailrecursion.chasma
  "Transactional actor runtime for Clojure"
  (:import (java.util ArrayDeque UUID)
           (java.util.concurrent CompletableFuture ExecutorService Executors
                                 LinkedBlockingQueue TimeUnit)
           (clojure.lang Atom)))

(def ^:private default-options
  {:threads (max 2 (.availableProcessors (Runtime/getRuntime)))
   :pump-poll-ms 100
   :retry-base-ms 5
   :retry-max-ms 500
   :effect-error-handler (fn [^Throwable t]
                           (binding [*out* *err*]
                             (println "on-commit! effect failed:"
                                      (.getMessage t))))})

(defrecord Universe [^LinkedBlockingQueue queue ^Atom running? ^Atom pool
                     ^Atom pump opts])
(defrecord Actor [^Universe universe ^UUID id ^Atom beh])
(defrecord Serializer [^ArrayDeque queue ^Atom running?])
(defrecord Lane [^Universe universe ^Actor target ^Serializer serializer])

(defn- non-negative-int? [x] (and (integer? x) (not (neg? x))))
(defn- positive-int? [x] (and (integer? x) (pos? x)))

(defn- validate-options [opts]
  (when-not (positive-int? (:threads opts))
    (throw (IllegalArgumentException. ":threads must be a positive integer")))
  (doseq [k [:pump-poll-ms :retry-base-ms :retry-max-ms]]
    (when-not (non-negative-int? (k opts))
      (throw (IllegalArgumentException.
               (str (name k) " must be a non-negative integer")))))
  (when (< (:retry-max-ms opts) (:retry-base-ms opts))
    (throw (IllegalArgumentException.
             ":retry-max-ms must be greater than or equal to :retry-base-ms")))
  (when-not (fn? (:effect-error-handler opts))
    (throw (IllegalArgumentException.
             ":effect-error-handler must be a function")))
  opts)

(defn universe
  "Create a stopped universe. Options:
   :threads       worker thread count
   :pump-poll-ms  dispatcher poll interval
   :retry-base-ms       retry backoff base
   :retry-max-ms        retry backoff cap
   :effect-error-handler fn called with on-commit! effect failures"
  (^Universe []
   (universe {}))
  (^Universe [opts]
   (let [opts (validate-options (merge default-options opts))]
     (->Universe (LinkedBlockingQueue.) (atom false) (atom nil) (atom nil)
                 opts))))

(def ^:dynamic *universe* nil)
(def ^:dynamic *self*     nil)
(def ^:private ^:dynamic *self-target* nil)
(def ^:dynamic *sender*   nil)
(def ^:dynamic *reply*    (fn [_] (throw (ex-info "No sender to reply to" {}))))
(def ^:dynamic *tx*       nil)

(defn- ensure-universe [u]
  (when-not (instance? Universe u)
    (throw (IllegalArgumentException. "Expected a Chasma universe")))
  u)

(defn- ensure-running! [^Universe u]
  (when-not @(:running? u)
    (throw (IllegalStateException. "Universe is stopped")))
  u)

(defn- current-universe! []
  (or *universe*
      (throw (IllegalStateException.
               "No current universe; pass a universe outside a turn"))))

(defn spawn
  "Create an actor. Outside a turn use (spawn universe) or (spawn universe beh).
   Inside a turn, (spawn) and (spawn beh) use *universe*."
  (^Actor []
   (spawn (current-universe!) (constantly nil)))
  (^Actor [u-or-beh]
   (if (instance? Universe u-or-beh)
     (spawn u-or-beh (constantly nil))
     (spawn (current-universe!) u-or-beh)))
  (^Actor [u beh]
   (let [u (ensure-universe u)]
     (->Actor u (UUID/randomUUID) (atom beh)))))

(defn lane
  "Create a private serialized target for actor."
  [^Actor actor]
  (->Lane (:universe actor) actor (->Serializer (ArrayDeque.) (atom false))))

(defrecord Delivery [target ^Actor to from payload ^Serializer serializer retries])
(defrecord Txn [^Universe universe sends becomes effects])

(defn- new-tx [^Universe u] (->Txn u (atom []) (atom []) (atom [])))

(declare commit!)

(defn- delivery-universe [^Delivery d] (:universe (:to d)))

(defn- target-delivery [target]
  (cond
    (instance? Lane target)
      [(:universe target) target (:target target) (:serializer target)]
    (instance? Actor target)
      [(:universe target) target target nil]
    :else
      (throw (IllegalArgumentException. "Expected an actor or lane target"))))

(defn- put-delivery! [^Delivery d]
  (.put (:queue (delivery-universe d)) d))

(defn- enqueue! [^Delivery d]
  (ensure-running! (delivery-universe d))
  (put-delivery! d))

(defn- stage-or-enqueue! [^Delivery d]
  (if *tx*
    (swap! (:sends *tx*) conj d)
    (enqueue! d))
  nil)

(defn send!
  "Send a message to an actor or lane target."
  [target & xs]
  (let [[u target actor serializer] (target-delivery target)]
    (ensure-running! u)
    (stage-or-enqueue!
      (->Delivery target actor *self-target* (vec xs) serializer 0))))

(defn- become-pairs [act new more]
  (when (odd? (count more))
    (throw (IllegalArgumentException. "become! requires actor/behavior pairs")))
  (let [pairs (vec (cons [act new] (partition 2 more)))
        u (:universe (ffirst pairs))]
    (doseq [[actor _] pairs]
      (when-not (instance? Actor actor)
        (throw (IllegalArgumentException. "become! requires actor/behavior pairs")))
      (when-not (identical? u (:universe actor))
        (throw (IllegalArgumentException.
                 "become! cannot atomically update actors from multiple universes"))))
    [u pairs]))

(defn- enqueue-becomes! [tx pairs]
  (doseq [[^Actor actor beh] pairs]
    (let [observed @(.-beh actor)]
      (swap! (:becomes tx) conj {:actor actor :old observed :new beh}))))

(defn- apply-become-pairs! [u pairs]
  (when (and *tx* (not (identical? u (:universe *tx*))))
    (throw (IllegalArgumentException.
             "become! cannot atomically update actors from multiple universes")))
  (if *tx*
    (enqueue-becomes! *tx* pairs)
    (loop []
      (let [tx (new-tx u)]
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
   (let [[u pairs] (become-pairs act new-beh ())]
     (apply-become-pairs! u pairs)))
  ([^Actor act new-beh & more]
   (let [[u pairs] (become-pairs act new-beh more)]
     (apply-become-pairs! u pairs))))

(defn ask
  "Request/response helper. Returns a CompletableFuture that completes with the reply.
   Inside the callee, call (*reply* value) to answer."
  [target & msg]
  (let [[u target actor serializer] (target-delivery target)
        _   (ensure-running! u)
        fut (CompletableFuture.)
        me  (spawn u)]
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

(defn- run-commit-effects! [^Universe u effects]
  (doseq [f effects]
    (try
      (binding [*tx* nil]
        (f))
      (catch Throwable t
        ((:effect-error-handler (:opts u)) t)))))

(defn- validate-staged-sends! [sends]
  (doseq [^Delivery d sends]
    (ensure-running! (delivery-universe d))))

(defn- commit! [^Txn tx]
  (let [sends   @(:sends tx)
        becomes (vec (compact-becomes @(:becomes tx)))
        actors  (mapv :actor becomes)]
    (validate-staged-sends! sends)
    (lock-actors! actors
      (fn []
        (doseq [{:keys [^Actor actor old]} becomes]
          (when-not (identical? old @(.-beh actor))
            (throw (ex-info "Commit conflict" {:actor actor}))))
        (doseq [{:keys [^Actor actor new]} becomes]
          (reset! (.-beh actor) new))))
    (doseq [^Delivery d sends]
      (put-delivery! d))
    (run-commit-effects! (:universe tx) @(:effects tx))))

(defn- backoff-ms [^Universe u n]
  (let [{:keys [retry-base-ms retry-max-ms]} (:opts u)]
    (min retry-max-ms
         (long (* retry-base-ms
                  (Math/pow 2.0 (max 0 (dec (double n)))))))))

(defn- retry-delivery [^Delivery d]
  (let [n  (inc (long (:retries d)))
        ms (backoff-ms (delivery-universe d) n)]
    (when (pos? ms)
      (try (.sleep TimeUnit/MILLISECONDS ms) (catch InterruptedException _)))
    (assoc d :retries n)))

(defn- run-turn! [^Delivery d]
  (let [^Actor act (:to d)
        u   (:universe act)
        beh @(.-beh act)
        tx  (new-tx u)
        reply-fn (fn [v] (when *sender* (send! *sender* v)))
        runnable (fn []
                   (binding [*universe* u
                             *self*     act
                             *self-target* (:target d)
                             *sender*   (:from d)
                             *reply*    reply-fn
                             *tx*       tx]
                     (apply beh (:payload d))))]
    (runnable)
    (commit! tx)))

(defn- run-with-retries! [^Delivery d]
  (loop [d d]
    (when @(:running? (delivery-universe d))
      (when-let [retry (try
                         (run-turn! d)
                         (catch Throwable _
                           (retry-delivery d)))]
        (recur retry)))))

(declare drain-serializer!)

(defn- schedule-serializer! [^Universe u ^Serializer s]
  (when-let [^ExecutorService executor @(:pool u)]
    (.submit executor ^Runnable #(drain-serializer! u s))))

(defn- enqueue-serialized! [^Universe u ^Serializer s ^Delivery d]
  (ensure-running! u)
  (let [start? (locking s
                 (.addLast ^ArrayDeque (:queue s) d)
                 (compare-and-set! (:running? s) false true))]
    (when start?
      (schedule-serializer! u s))))

(defn- next-serialized-delivery [^Serializer s]
  (locking s
    (if-let [d (.pollFirst ^ArrayDeque (:queue s))]
      d
      (do
        (reset! (:running? s) false)
        nil))))

(defn- drain-serializer! [^Universe u ^Serializer s]
  (loop []
    (when @(:running? u)
      (when-let [d (next-serialized-delivery s)]
        (run-with-retries! d)
        (recur)))))

(defn- run-delivery! [^Delivery d]
  (when-let [retry (try
                     (run-turn! d)
                     (catch Throwable _
                       (retry-delivery d)))]
    (enqueue! retry)))

(defn- pump-loop [^Universe u]
  (future
    (let [queue (:queue u)
          poll-ms (:pump-poll-ms (:opts u))]
      (while @(:running? u)
        (when-let [^Delivery d (.poll queue poll-ms TimeUnit/MILLISECONDS)]
          (if-let [s (:serializer d)]
            (enqueue-serialized! (delivery-universe d) s d)
            (when-let [^ExecutorService executor @(:pool u)]
              (.submit executor ^Runnable #(run-delivery! d)))))))))

(defn start!
  "Start a universe. Idempotent; returns the universe."
  [^Universe u]
  (let [u (ensure-universe u)]
    (when (compare-and-set! (:running? u) false true)
      (reset! (:pool u) (Executors/newFixedThreadPool (:threads (:opts u))))
      (reset! (:pump u) (pump-loop u)))
    u))

(defn stop!
  "Stop a universe and shut down its worker pool. Idempotent; returns the universe."
  [^Universe u]
  (let [u (ensure-universe u)]
    (when (compare-and-set! (:running? u) true false)
      (when-let [p @(:pump u)] (future-cancel p))
      (when-let [^ExecutorService executor @(:pool u)]
        (.shutdownNow executor))
      (.clear (:queue u))
      (reset! (:pump u) nil)
      (reset! (:pool u) nil))
    u))
