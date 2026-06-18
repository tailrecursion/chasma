(ns tailrecursion.chasma-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [tailrecursion.chasma :as ch])
  (:import (java.util.concurrent CountDownLatch TimeUnit)))

(defn counter
  "Stateful counter behavior using become! to hold the current value."
  [n]
  (fn [cmd]
    (case cmd
      :inc (ch/become! ch/*self* (counter (inc n)))
      :get (ch/*reply* n)
      nil)))

(defn flaky-behavior
  "Behavior that throws on the first attempt to demonstrate transactional retry."
  [value attempts fail-once?]
  (fn [cmd]
    (when (= cmd :value)
      (let [attempt (swap! attempts inc)]
        (when (compare-and-set! fail-once? true false)
          (throw (ex-info "boom" {:attempt attempt})))
        (ch/become! ch/*self* (constantly nil))
        (ch/*reply* value)))))

(defn deferred-sender
  "Sends to `target` every time but throws on the first attempt so the staged send is dropped."
  [target attempts fail-once?]
  (fn [& _]
    (let [n (swap! attempts inc)]
      (ch/send! target :attempt n)
      (when (compare-and-set! fail-once? true false)
        (throw (ex-info "boom" {:attempt n}))))))

(defn on-commit-probe
  [counter attempts latch fail-once?]
  (fn [& _]
    (let [n (swap! attempts inc)]
      (ch/on-commit!
        (swap! counter inc)
        (.countDown ^CountDownLatch latch))
      (when (compare-and-set! fail-once? true false)
        (throw (ex-info "boom" {:attempt n}))))))

(defn restartable-counter
  "State machine that increments via become!; first attempt throws to force a retry."
  [n fail-once? attempts]
  (fn [cmd]
    (when (= cmd :tick)
      (swap! attempts inc)
      (ch/become! ch/*self* (restartable-counter (inc n) fail-once? attempts))
      (when (compare-and-set! fail-once? true false)
        (throw (ex-info "boom" {:state n})))
      (ch/*reply* n))))

(defn lane-probe-behavior
  "Records how many instances of this behavior run concurrently."
  [^CountDownLatch latch in-flight max-in-flight]
  (fn [& _]
    (let [current (swap! in-flight inc)]
      (swap! max-in-flight #(max % current))
      (Thread/sleep 10)
      (swap! in-flight dec)
      (.countDown latch))))

(defn run-lane-probe
  "Sends multiple messages to the given target."
  [serialized?]
  (let [n        16
        latch    (CountDownLatch. n)
        in-flight (atom 0)
        max-inf  (atom 0)
        actor    (ch/spawn (lane-probe-behavior latch in-flight max-inf))
        target   (if serialized? (ch/lane actor) actor)]
    (dotimes [_ n]
      (ch/send! target :tick))
    (when-not (.await latch 2000 TimeUnit/MILLISECONDS)
      (throw (ex-info "Timed out waiting for lane probe" {:serialized? serialized?})))
    @max-inf))

(defn lane-blocker
  [start release done]
  (fn [& _]
    (.countDown ^CountDownLatch start)
    (.await ^CountDownLatch release 1000 TimeUnit/MILLISECONDS)
    (.countDown ^CountDownLatch done)))

(deftest on-commit-requires-turn
  (is (try
        (ch/on-commit! :nope)
        false
        (catch IllegalStateException _ true))))

(deftest on-commit-returns-nil
  (let [observed (atom :unset)
        actor    (ch/spawn
                   (fn [& _]
                     (let [ret (ch/on-commit! (reset! observed :committed))]
                       (ch/*reply* ret))))]
    (is (= nil (deref (ch/ask actor) 1000 ::timeout)))
    (is (= :committed @observed))))

(defn responder
  [label]
  (fn [& _]
    (ch/*reply* label)))

(deftest spawn-without-args-produces-noop
  (let [actor (ch/spawn)]
    (ch/become! actor (fn [cmd]
                        (when (= cmd :hello)
                          (ch/*reply* :hi))))
    (is (= :hi (deref (ch/ask actor :hello) 1000 ::timeout)))))

(deftest become!-requires-complete-pairs
  (let [actor (ch/spawn)]
    (is (try
          (ch/become! actor (responder :ok) actor)
          false
          (catch IllegalArgumentException _ true)))))

(deftest multi-become-outside-turn
  (let [a (ch/spawn (responder :old-a))
        b (ch/spawn (responder :old-b))]
    (ch/become! a (responder :new-a) b (responder :new-b))
    (is (= :new-a (deref (ch/ask a) 1000 ::timeout)))
    (is (= :new-b (deref (ch/ask b) 1000 ::timeout)))))

(deftest multi-become-inside-turn
  (let [a   (ch/spawn (responder :old-a))
        b   (ch/spawn (responder :old-b))
        ctl (ch/spawn
              (fn [& _]
                (ch/become! a (responder :new-a) b (responder :new-b))
                (ch/*reply* :done)))]
    (is (= :done (deref (ch/ask ctl :swap) 1000 ::timeout)))
    (is (= :new-a (deref (ch/ask a) 1000 ::timeout)))
    (is (= :new-b (deref (ch/ask b) 1000 ::timeout)))))

(deftest multi-become-compacts-repeated-actors
  (let [a   (ch/spawn (responder :old))
        ctl (ch/spawn
              (fn [& _]
                (ch/become! a (responder :middle))
                (ch/become! a (responder :final))
                (ch/*reply* :done)))]
    (is (= :done (deref (ch/ask ctl) 1000 ::timeout)))
    (is (= :final (deref (ch/ask a) 1000 ::timeout)))))

(deftest multi-become-commit-is-all-or-nothing
  (let [a      (ch/spawn (responder :old-a))
        b      (ch/spawn (responder :old-b))
        tx     (#'ch/new-tx)
        old-a  @(.-beh a)
        old-b  @(.-beh b)]
    (swap! (:becomes tx) conj {:actor a :old old-a :new (responder :new-a)})
    (swap! (:becomes tx) conj {:actor b :old old-b :new (responder :new-b)})
    (reset! (.-beh b) (responder :conflict-b))
    (is (thrown? clojure.lang.ExceptionInfo (#'ch/commit! tx)))
    (is (identical? old-a @(.-beh a)))
    (is (= :conflict-b (deref (ch/ask b) 1000 ::timeout)))))

(use-fixtures :once
  (fn [f]
    (ch/start!)
    (try
      (f)
      (finally
        (ch/shutdown!)))))

(deftest counter-behavior-test
  (let [counter (ch/lane (ch/spawn (counter 0)))]
    (ch/send! counter :inc)
    (ch/send! counter :inc)
    (is (= 2 (deref (ch/ask counter :get) 1000 ::timeout)))))

(deftest ask-retries-after-failure
  (let [attempts (atom 0)
        actor    (ch/spawn (flaky-behavior 42 attempts (atom true)))
        fut      (ch/ask actor :value)
        result   (deref fut 1000 ::timeout)]
    (is (not= ::timeout result))
    (is (= 42 result))
    (is (= 2 @attempts))))

(deftest serializer-lane-enforces-exclusion
  (let [no-lane-max (run-lane-probe false)
        lane-max    (run-lane-probe true)]
    (is (> no-lane-max 1) "Without a lane, multiple deliveries should overlap")
    (is (= 1 lane-max) "Serializer lane forces sequential turns")))

(deftest map-first-arg-is-payload
  (let [observed (atom nil)
        latch    (CountDownLatch. 1)
        actor    (ch/spawn (fn [& payload]
                              (reset! observed (vec payload))
                              (.countDown latch)))]
    (ch/send! actor {:ser :not-options} :value)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (= [{:ser :not-options} :value] @observed))))

(deftest send-effects-commit-only-after-success
  (let [hits     (atom [])
        latch    (CountDownLatch. 1)
        target   (ch/spawn (fn [& payload]
                              (swap! hits conj (vec payload))
                              (.countDown latch)))
        attempts (atom 0)
        actor    (ch/spawn (deferred-sender target attempts (atom true)))]
    (ch/send! actor :go)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (= [[:attempt 2]] @hits))
    (is (= 2 @attempts))))

(deftest on-commit-defers-effects-until-success
  (let [counter  (atom 0)
        attempts (atom 0)
        latch    (CountDownLatch. 1)
        actor    (ch/spawn (on-commit-probe counter attempts latch (atom true)))]
    (ch/send! actor :run)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (= 1 @counter))
    (is (= 2 @attempts))))

(deftest become-rolls-back-on-failure
  (let [fail-once? (atom true)
        attempts   (atom 0)
        actor      (ch/spawn (restartable-counter 0 fail-once? attempts))]
    (is (= 0 (deref (ch/ask actor :tick) 1000 ::timeout)))
    (is (= 1 (deref (ch/ask actor :tick) 1000 ::timeout)))
    (is (= 3 @attempts))))

(deftest serializer-lanes-do-not-block-each-other
  (let [start   (CountDownLatch. 2)
        release (CountDownLatch. 1)
        done    (CountDownLatch. 2)
        actor   (ch/spawn (lane-blocker start release done))
        lane-a  (ch/lane actor)
        lane-b  (ch/lane actor)]
    (ch/send! lane-a :ping)
    (ch/send! lane-b :pong)
    (try
      (is (.await start 500 TimeUnit/MILLISECONDS)
          "Distinct lanes should run concurrently")
      (finally
        (.countDown release)))
    (is (.await done 1000 TimeUnit/MILLISECONDS))))

(deftest sender-preserves-lane-capability
  (let [observed (atom nil)
        latch    (CountDownLatch. 1)
        a        (ch/spawn (fn [_ target]
                             (ch/send! target :from-a)
                             (ch/become! ch/*self*
                               (fn [v]
                                 (reset! observed v)
                                 (.countDown latch)))))
        a-lane   (ch/lane a)
        b        (ch/spawn (fn [& _]
                             (ch/send! ch/*sender*
                                       (instance? tailrecursion.chasma.Lane ch/*sender*))))]
    (ch/send! a-lane :go b)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (= true @observed))))

(deftest ask-effects-commit-only-after-success
  (let [hits      (atom [])
        latch     (CountDownLatch. 1)
        target    (ch/spawn (fn [& payload]
                              (swap! hits conj (vec payload))
                              (.countDown latch)
                              (ch/*reply* :ok)))
        attempts  (atom 0)
        fail-once (atom true)
        actor     (ch/spawn (fn [& _]
                              (let [n (swap! attempts inc)]
                                (ch/ask target :attempt n)
                                (when (compare-and-set! fail-once true false)
                                  (throw (ex-info "boom" {:attempt n}))))))]
    (ch/send! actor :run)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (= [[:attempt 2]] @hits))
    (is (= 2 @attempts))))

(deftest on-commit-preserves-dynamic-context
  (let [observed (atom nil)
        latch    (CountDownLatch. 1)
        actor-ref (atom nil)
        actor    (ch/spawn
                   (fn [& _]
                     (ch/on-commit!
                       (reset! observed {:self?  (identical? ch/*self* @actor-ref)
                                         :sender ch/*sender*
                                         :tx     ch/*tx*})
                       (.countDown latch))
                     (ch/*reply* :done)))]
    (reset! actor-ref actor)
    (is (= :done (deref (ch/ask actor) 1000 ::timeout)))
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (:self? @observed))
    (is (some? (:sender @observed)))
    (is (nil? (:tx @observed)))))

(deftest on-commit-failure-does-not-retry-turn
  (let [attempts (atom 0)
        hits     (atom [])
        latch    (CountDownLatch. 1)
        target   (ch/spawn (fn [& payload]
                             (swap! hits conj (vec payload))
                             (.countDown latch)))
        actor    (ch/spawn (fn [& _]
                             (swap! attempts inc)
                             (ch/send! target :committed)
                             (ch/on-commit!
                               (throw (ex-info "effect failed" {})))))]
    (ch/send! actor :run)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (Thread/sleep 100)
    (is (= 1 @attempts))
    (is (= [[:committed]] @hits))))

(deftest runtime-can-restart
  (ch/shutdown!)
  (try
    (is (= :started (ch/start!)))
    (let [counter (ch/lane (ch/spawn (counter 0)))]
      (ch/send! counter :inc)
      (is (= 1 (deref (ch/ask counter :get) 1000 ::timeout))))
    (is (= :stopped (ch/shutdown!)))
    (is (= :started (ch/start!)))
    (let [counter (ch/lane (ch/spawn (counter 0)))]
      (ch/send! counter :inc)
      (ch/send! counter :inc)
      (is (= 2 (deref (ch/ask counter :get) 1000 ::timeout))))
    (finally
      (ch/start!))))

(deftest odd-even-mutual-recursion
  (let [even (ch/spawn)
        odd  (ch/spawn)
        await (fn [actor n]
                (deref (ch/ask actor n) 1000 ::timeout))
        even-beh (fn even-beh
                   ([n] (even-beh n ch/*sender*))
                   ([n cust]
                    (if (zero? n)
                      (ch/send! cust true)
                      (ch/send! odd (dec n) cust))))
        odd-beh  (fn odd-beh
                   ([n] (odd-beh n ch/*sender*))
                   ([n cust]
                    (if (zero? n)
                      (ch/send! cust false)
                      (ch/send! even (dec n) cust))))]
    (ch/become! even even-beh odd odd-beh)
    (is (= true  (await even 42)))
    (is (= false (await even 17)))
    (is (= true  (await odd 17)))
    (is (= false (await odd 18)))))
