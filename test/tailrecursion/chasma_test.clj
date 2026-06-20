(ns tailrecursion.chasma-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [tailrecursion.chasma :as ch])
  (:import (java.util.concurrent CountDownLatch TimeUnit)))

(def test-universe (atom nil))

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
      (ch/on-commit! (swap! counter inc) (.countDown ^CountDownLatch latch))
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
  (let [n 16
        latch (CountDownLatch. n)
        in-flight (atom 0)
        max-inf (atom 0)
        actor (ch/spawn @test-universe
                        (lane-probe-behavior latch in-flight max-inf))
        target (if serialized? (ch/lane actor) actor)]
    (dotimes [_ n] (ch/send! target :tick))
    (when-not (.await latch 2000 TimeUnit/MILLISECONDS)
      (throw (ex-info "Timed out waiting for lane probe"
                      {:serialized? serialized?})))
    @max-inf))

(defn lane-blocker
  [start release done]
  (fn [& _]
    (.countDown ^CountDownLatch start)
    (.await ^CountDownLatch release 1000 TimeUnit/MILLISECONDS)
    (.countDown ^CountDownLatch done)))

(deftest on-commit-requires-turn
  (is (try (ch/on-commit! :nope) false (catch IllegalStateException _ true))))

(deftest on-commit-returns-nil
  (let [observed (atom :unset)
        actor (ch/spawn @test-universe
                        (fn [& _]
                          (let [ret (ch/on-commit! (reset! observed
                                                     :committed))]
                            (ch/*reply* ret))))]
    (is (nil? (deref (ch/ask actor) 1000 ::timeout)))
    (is (= :committed @observed))))

(defn responder [label] (fn [& _] (ch/*reply* label)))

(deftest spawn-without-args-produces-noop
  (let [actor (ch/spawn @test-universe)]
    (ch/become! actor (fn [cmd] (when (= cmd :hello) (ch/*reply* :hi))))
    (is (= :hi (deref (ch/ask actor :hello) 1000 ::timeout)))))

(deftest spawn-without-universe-requires-turn
  (is (thrown? IllegalStateException (ch/spawn (constantly nil)))))

(deftest become!-requires-complete-pairs
  (let [actor (ch/spawn @test-universe)]
    (is (try (ch/become! actor (responder :ok) actor)
             false
             (catch IllegalArgumentException _ true)))))

(deftest multi-become-outside-turn
  (let [a (ch/spawn @test-universe (responder :old-a))
        b (ch/spawn @test-universe (responder :old-b))]
    (ch/become! a (responder :new-a) b (responder :new-b))
    (is (= :new-a (deref (ch/ask a) 1000 ::timeout)))
    (is (= :new-b (deref (ch/ask b) 1000 ::timeout)))))

(deftest multi-become-inside-turn
  (let [a (ch/spawn @test-universe (responder :old-a))
        b (ch/spawn @test-universe (responder :old-b))
        ctl (ch/spawn @test-universe
                      (fn [& _]
                        (ch/become! a (responder :new-a) b (responder :new-b))
                        (ch/*reply* :done)))]
    (is (= :done (deref (ch/ask ctl :swap) 1000 ::timeout)))
    (is (= :new-a (deref (ch/ask a) 1000 ::timeout)))
    (is (= :new-b (deref (ch/ask b) 1000 ::timeout)))))

(deftest multi-become-compacts-repeated-actors
  (let [a (ch/spawn @test-universe (responder :old))
        ctl (ch/spawn @test-universe
                      (fn [& _]
                        (ch/become! a (responder :middle))
                        (ch/become! a (responder :final))
                        (ch/*reply* :done)))]
    (is (= :done (deref (ch/ask ctl) 1000 ::timeout)))
    (is (= :final (deref (ch/ask a) 1000 ::timeout)))))

(deftest multi-become-commit-is-all-or-nothing
  (let [a (ch/spawn @test-universe (responder :old-a))
        b (ch/spawn @test-universe (responder :old-b))
        tx (#'ch/new-tx @test-universe)
        old-a @(.-beh a)
        old-b @(.-beh b)]
    (swap! (:becomes tx) conj {:actor a, :old old-a, :new (responder :new-a)})
    (swap! (:becomes tx) conj {:actor b, :old old-b, :new (responder :new-b)})
    (reset! (.-beh b) (responder :conflict-b))
    (is (thrown? clojure.lang.ExceptionInfo (#'ch/commit! tx)))
    (is (identical? old-a @(.-beh a)))
    (is (= :conflict-b (deref (ch/ask b) 1000 ::timeout)))))

(use-fixtures :once
              (fn [f]
                (reset! test-universe (ch/start! (ch/universe
                                                   {:effect-error-handler
                                                      (fn [_])})))
                (try (f) (finally (ch/stop! @test-universe)))))

(deftest counter-behavior-test
  (let [counter (ch/lane (ch/spawn @test-universe (counter 0)))]
    (ch/send! counter :inc)
    (ch/send! counter :inc)
    (is (= 2 (deref (ch/ask counter :get) 1000 ::timeout)))))

(deftest ask-retries-after-failure
  (let [attempts (atom 0)
        actor (ch/spawn @test-universe (flaky-behavior 42 attempts (atom true)))
        fut (ch/ask actor :value)
        result (deref fut 1000 ::timeout)]
    (is (not= ::timeout result))
    (is (= 42 result))
    (is (= 2 @attempts))))

(deftest serializer-lane-enforces-exclusion
  (let [no-lane-max (run-lane-probe false)
        lane-max (run-lane-probe true)]
    (is (> no-lane-max 1) "Without a lane, multiple deliveries should overlap")
    (is (= 1 lane-max) "Serializer lane forces sequential turns")))

(deftest map-first-arg-is-payload
  (let [observed (atom nil)
        latch (CountDownLatch. 1)
        actor (ch/spawn @test-universe
                        (fn [& payload]
                          (reset! observed (vec payload))
                          (.countDown latch)))]
    (ch/send! actor {:ser :not-options} :value)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (= [{:ser :not-options} :value] @observed))))

(deftest send-effects-commit-only-after-success
  (let [hits (atom [])
        latch (CountDownLatch. 1)
        target (ch/spawn @test-universe
                         (fn [& payload]
                           (swap! hits conj (vec payload))
                           (.countDown latch)))
        attempts (atom 0)
        actor (ch/spawn @test-universe
                        (deferred-sender target attempts (atom true)))]
    (ch/send! actor :go)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (= [[:attempt 2]] @hits))
    (is (= 2 @attempts))))

(deftest on-commit-defers-effects-until-success
  (let [counter (atom 0)
        attempts (atom 0)
        latch (CountDownLatch. 1)
        actor (ch/spawn @test-universe
                        (on-commit-probe counter attempts latch (atom true)))]
    (ch/send! actor :run)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (= 1 @counter))
    (is (= 2 @attempts))))

(deftest become-rolls-back-on-failure
  (let [fail-once? (atom true)
        attempts (atom 0)
        actor (ch/spawn @test-universe
                        (restartable-counter 0 fail-once? attempts))]
    (is (= 0 (deref (ch/ask actor :tick) 1000 ::timeout)))
    (is (= 1 (deref (ch/ask actor :tick) 1000 ::timeout)))
    (is (= 3 @attempts))))

(deftest serializer-lanes-do-not-block-each-other
  (let [start (CountDownLatch. 2)
        release (CountDownLatch. 1)
        done (CountDownLatch. 2)
        actor (ch/spawn @test-universe (lane-blocker start release done))
        lane-a (ch/lane actor)
        lane-b (ch/lane actor)]
    (ch/send! lane-a :ping)
    (ch/send! lane-b :pong)
    (try (is (.await start 500 TimeUnit/MILLISECONDS)
             "Distinct lanes should run concurrently")
         (finally (.countDown release)))
    (is (.await done 1000 TimeUnit/MILLISECONDS))))

(deftest sender-preserves-lane-capability
  (let [observed (atom nil)
        latch (CountDownLatch. 1)
        a (ch/spawn @test-universe
                    (fn [_ target]
                      (ch/send! target :from-a)
                      (ch/become!
                        ch/*self*
                        (fn [v] (reset! observed v) (.countDown latch)))))
        a-lane (ch/lane a)
        b (ch/spawn @test-universe
                    (fn [& _]
                      (ch/send! ch/*sender*
                                (instance? tailrecursion.chasma.Lane
                                           ch/*sender*))))]
    (ch/send! a-lane :go b)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (true? @observed))))

(deftest ask-effects-commit-only-after-success
  (let [hits (atom [])
        latch (CountDownLatch. 1)
        target (ch/spawn @test-universe
                         (fn [& payload]
                           (swap! hits conj (vec payload))
                           (.countDown latch)
                           (ch/*reply* :ok)))
        attempts (atom 0)
        fail-once (atom true)
        actor (ch/spawn @test-universe
                        (fn [& _]
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
        latch (CountDownLatch. 1)
        actor-ref (atom nil)
        actor (ch/spawn
                @test-universe
                (fn [& _]
                  (ch/on-commit! (reset! observed
                                   {:self? (identical? ch/*self* @actor-ref),
                                    :universe? (identical? ch/*universe*
                                                           @test-universe),
                                    :sender ch/*sender*,
                                    :tx ch/*tx*})
                                 (.countDown latch))
                  (ch/*reply* :done)))]
    (reset! actor-ref actor)
    (is (= :done (deref (ch/ask actor) 1000 ::timeout)))
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (is (:self? @observed))
    (is (:universe? @observed))
    (is (some? (:sender @observed)))
    (is (nil? (:tx @observed)))))

(deftest on-commit-failure-does-not-retry-turn
  (let [attempts (atom 0)
        hits (atom [])
        latch (CountDownLatch. 1)
        target (ch/spawn @test-universe
                         (fn [& payload]
                           (swap! hits conj (vec payload))
                           (.countDown latch)))
        actor (ch/spawn @test-universe
                        (fn [& _]
                          (swap! attempts inc)
                          (ch/send! target :committed)
                          (ch/on-commit! (throw (ex-info "effect failed"
                                                         {})))))]
    (ch/send! actor :run)
    (is (.await latch 1000 TimeUnit/MILLISECONDS))
    (Thread/sleep 100)
    (is (= 1 @attempts))
    (is (= [[:committed]] @hits))))

(deftest runtime-can-restart
  (let [u (ch/universe {:retry-base-ms 0, :retry-max-ms 0})]
    (try (is (identical? u (ch/start! u)))
         (let [counter (ch/lane (ch/spawn u (counter 0)))]
           (ch/send! counter :inc)
           (is (= 1 (deref (ch/ask counter :get) 1000 ::timeout))))
         (is (identical? u (ch/stop! u)))
         (is (identical? u (ch/start! u)))
         (let [counter (ch/lane (ch/spawn u (counter 0)))]
           (ch/send! counter :inc)
           (ch/send! counter :inc)
           (is (= 2 (deref (ch/ask counter :get) 1000 ::timeout))))
         (finally (ch/stop! u)))))

(deftest universe-validates-options
  (is (thrown? IllegalArgumentException (ch/universe {:threads 0})))
  (is (thrown? IllegalArgumentException (ch/universe {:pump-poll-ms -1})))
  (is (thrown? IllegalArgumentException (ch/universe {:retry-base-ms -1})))
  (is (thrown? IllegalArgumentException
               (ch/universe {:retry-base-ms 10, :retry-max-ms 5})))
  (is (thrown? IllegalArgumentException
               (ch/universe {:effect-error-handler :not-a-fn}))))

(deftest single-thread-universe-serializes-direct-deliveries
  (let [u (ch/start! (ch/universe {:threads 1}))
        n 8
        latch (CountDownLatch. n)
        in-flight (atom 0)
        max-inf (atom 0)
        actor (ch/spawn u (lane-probe-behavior latch in-flight max-inf))]
    (try (dotimes [_ n] (ch/send! actor :tick))
         (is (.await latch 2000 TimeUnit/MILLISECONDS))
         (is (= 1 @max-inf))
         (finally (ch/stop! u)))))

(deftest stopped-universe-rejects-delivery
  (let [u (ch/universe)
        actor (ch/spawn u (responder :ok))]
    (is (thrown? IllegalStateException (ch/send! actor :go)))
    (is (thrown? IllegalStateException (ch/ask actor :go)))
    (ch/start! u)
    (ch/stop! u)
    (is (thrown? IllegalStateException (ch/send! actor :go)))
    (is (thrown? IllegalStateException (ch/ask actor :go)))))

(deftest universes-run-independently
  (let [u1 (ch/start! (ch/universe))
        u2 (ch/start! (ch/universe))
        latch1 (CountDownLatch. 1)
        latch2 (CountDownLatch. 1)
        a1 (ch/spawn u1 (fn [& _] (.countDown latch1)))
        a2 (ch/spawn u2 (fn [& _] (.countDown latch2)))]
    (try (ch/send! a1 :one)
         (ch/send! a2 :two)
         (is (.await latch1 1000 TimeUnit/MILLISECONDS))
         (is (.await latch2 1000 TimeUnit/MILLISECONDS))
         (ch/stop! u1)
         (let [latch3 (CountDownLatch. 1)
               a3 (ch/spawn u2 (fn [& _] (.countDown latch3)))]
           (is (thrown? IllegalStateException (ch/send! a1 :stopped)))
           (ch/send! a3 :still-running)
           (is (.await latch3 1000 TimeUnit/MILLISECONDS)))
         (finally (ch/stop! u1) (ch/stop! u2)))))

(deftest targets-and-ask-carry-universe
  (let [observed (atom nil)
        actor (ch/spawn @test-universe
                        (fn [& _]
                          (reset! observed {:self-universe ch/*universe*,
                                            :sender-universe (:universe
                                                               ch/*sender*)})
                          (ch/*reply* :ok)))
        lane (ch/lane actor)]
    (is (identical? @test-universe (:universe actor)))
    (is (identical? @test-universe (:universe lane)))
    (is (= :ok (deref (ch/ask lane :go) 1000 ::timeout)))
    (is (identical? @test-universe (:self-universe @observed)))
    (is (identical? @test-universe (:sender-universe @observed)))))

(deftest spawn-without-universe-works-inside-turn
  (let [observed (atom nil)
        actor (ch/spawn @test-universe
                        (fn [& _]
                          (let [child (ch/spawn (fn [& _] nil))]
                            (reset! observed (identical? ch/*universe*
                                                         (:universe child)))
                            (ch/*reply* :done))))]
    (is (= :done (deref (ch/ask actor :go) 1000 ::timeout)))
    (is (true? @observed))))

(deftest cross-universe-become-is-rejected
  (let [u1 (ch/start! (ch/universe))
        u2 (ch/start! (ch/universe))
        a (ch/spawn u1 (responder :old-a))
        b (ch/spawn u2 (responder :old-b))]
    (try (is (thrown? IllegalArgumentException
                      (ch/become! a (responder :new-a) b (responder :new-b))))
         (is (= :old-a (deref (ch/ask a) 1000 ::timeout)))
         (is (= :old-b (deref (ch/ask b) 1000 ::timeout)))
         (finally (ch/stop! u1) (ch/stop! u2)))))

(deftest odd-even-mutual-recursion
  (let [even (ch/spawn @test-universe)
        odd (ch/spawn @test-universe)
        await-result (fn [actor n] (deref (ch/ask actor n) 1000 ::timeout))
        even-beh
          (fn even-beh
            ([n] (even-beh n ch/*sender*))
            ([n cust]
             (if (zero? n) (ch/send! cust true) (ch/send! odd (dec n) cust))))
        odd-beh (fn odd-beh
                  ([n] (odd-beh n ch/*sender*))
                  ([n cust]
                   (if (zero? n)
                     (ch/send! cust false)
                     (ch/send! even (dec n) cust))))]
    (ch/become! even even-beh odd odd-beh)
    (is (true? (await-result even 42)))
    (is (false? (await-result even 17)))
    (is (true? (await-result odd 17)))
    (is (false? (await-result odd 18)))))
