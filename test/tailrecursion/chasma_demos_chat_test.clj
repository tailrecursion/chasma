(ns tailrecursion.chasma-demos-chat-test
  (:require [clojure.test :refer [deftest is]]
            [tailrecursion.chasma :as ch]
            [tailrecursion.chasma.demos.chat :as chat])
  (:import (java.util.concurrent CountDownLatch TimeUnit)))

(def client-id ::client)

(deftest decode-wire-messages
  (is (= (struct chat/join client-id "Ada")
         (chat/decode client-id {"type" "join", "name" "Ada"})))
  (is (= (struct chat/say client-id "hello")
         (chat/decode client-id {"type" "say", "text" "hello"})))
  (is (= (struct chat/rename client-id "Grace")
         (chat/decode client-id {"type" "rename", "name" "Grace"})))
  (is (= (struct chat/invalid client-id "unknown-message")
         (chat/decode client-id {"type" "bogus"}))))

(deftest encode-domain-messages
  (is (= {"type" "ready"} (chat/encode (chat/ready-message))))
  (is (= {"type" "joined", "name" "Ada"}
         (chat/encode (struct chat/joined "Ada"))))
  (is (= {"type" "left", "name" "Ada"} (chat/encode (struct chat/left "Ada"))))
  (is (= {"type" "message", "from" "Ada", "text" "hi"}
         (chat/encode (struct chat/message "Ada" "hi"))))
  (is (= {"type" "renamed", "from" "Ada", "to" "Grace"}
         (chat/encode (struct chat/renamed "Ada" "Grace"))))
  (is (= {"type" "error", "reason" "invalid-json"}
         (chat/encode (struct chat/error "invalid-json")))))

(defn- collector
  [messages ^CountDownLatch latch]
  (fn [message] (swap! messages conj message) (.countDown latch)))

(deftest room-broadcasts-through-client-targets
  (let [u (ch/start! (ch/universe {:retry-base-ms 0, :retry-max-ms 0}))
        latch (CountDownLatch. 9)
        a-messages (atom [])
        b-messages (atom [])
        a (ch/lane (ch/spawn u (collector a-messages latch)))
        b (ch/lane (ch/spawn u (collector b-messages latch)))
        room (ch/lane (ch/spawn u (chat/room (struct chat/room-state {}))))]
    (try (ch/send! room (struct chat/client-opened a))
         (ch/send! room (struct chat/client-opened b))
         (ch/send! room (struct chat/join a "Ada"))
         (ch/send! room (struct chat/say a "hi"))
         (ch/send! room (struct chat/rename a "Grace"))
         (ch/send! room (struct chat/client-closed a))
         (is (.await latch 1000 TimeUnit/MILLISECONDS))
         (is (= [{"type" "ready"} {"type" "joined", "name" "Ada"}
                 {"type" "message", "from" "Ada", "text" "hi"}
                 {"type" "renamed", "from" "Ada", "to" "Grace"}]
                (mapv #(chat/encode %) @a-messages)))
         (is (= [{"type" "ready"} {"type" "joined", "name" "Ada"}
                 {"type" "message", "from" "Ada", "text" "hi"}
                 {"type" "renamed", "from" "Ada", "to" "Grace"}
                 {"type" "left", "name" "Grace"}]
                (mapv #(chat/encode %) @b-messages)))
         (finally (ch/stop! u)))))
