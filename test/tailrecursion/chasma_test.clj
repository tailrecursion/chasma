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
  "Sends multiple messages to the given actor, optionally through a serializer lane."
  [token]
  (let [n        16
        latch    (CountDownLatch. n)
        in-flight (atom 0)
        max-inf  (atom 0)
        actor    (ch/spawn (lane-probe-behavior latch in-flight max-inf))]
    (dotimes [_ n]
      (if token
        (ch/send! actor {:ser token} :tick)
        (ch/send! actor :tick)))
    (when-not (.await latch 2000 TimeUnit/MILLISECONDS)
      (throw (ex-info "Timed out waiting for lane probe" {:token token})))
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

(use-fixtures :once
  (fn [f]
    (ch/start!)
    (try
      (f)
      (finally
        (ch/shutdown!)))))

(deftest counter-behavior-test
  (let [counter (ch/spawn (counter 0))]
    (ch/send! counter :inc)
    (ch/send! counter :inc)
    (let [value (loop [attempts 0]
                  (let [v (deref (ch/ask counter :get) 1000 ::timeout)]
                    (if (or (= v 2) (>= attempts 20))
                      v
                      (do
                        (Thread/sleep 10)
                        (recur (inc attempts))))))]
      (is (= 2 value)))))

(deftest ask-retries-after-failure
  (let [attempts (atom 0)
        actor    (ch/spawn (flaky-behavior 42 attempts (atom true)))
        fut      (ch/ask actor :value)
        result   (deref fut 1000 ::timeout)]
    (is (not= ::timeout result))
    (is (= 42 result))
    (is (= 2 @attempts))))

(deftest serializer-lane-enforces-exclusion
  (let [no-lane-max (run-lane-probe nil)
        lane-max    (run-lane-probe :lane/test)]
    (is (> no-lane-max 1) "Without a lane, multiple deliveries should overlap")
    (is (= 1 lane-max) "Serializer lane forces sequential turns")))

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
        actor   (ch/spawn (lane-blocker start release done))]
    (ch/send! actor {:ser :lane/a} :ping)
    (ch/send! actor {:ser :lane/b} :pong)
    (try
      (is (.await start 500 TimeUnit/MILLISECONDS)
          "Distinct serializer tokens should run concurrently")
      (finally
        (.countDown release)))
    (is (.await done 1000 TimeUnit/MILLISECONDS))))
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
