(ns tailrecursion.chasma.demos.chat
  "Undertow WebSocket chat demo for Chasma and CLOS."
  (:require [clojure.data.json :as json]
            [tailrecursion.chasma :as ch]
            [tailrecursion.chasma.clos :as clos])
  (:import (io.undertow Handlers Undertow)
           (io.undertow.server HttpHandler)
           (io.undertow.server.handlers.resource ClassPathResourceManager
                                                 ResourceHandler)
           (io.undertow.websockets WebSocketConnectionCallback)
           (io.undertow.websockets.core AbstractReceiveListener
                                        BufferedTextMessage
                                        WebSocketChannel
                                        WebSockets)
           (io.undertow.websockets.spi WebSocketHttpExchange)
           (org.xnio ChannelListener)))

(defstruct socket-text :payload)
(defstruct socket-closed :_)
(defstruct client-opened :client)
(defstruct client-closed :client)
(defstruct join :client :name)
(defstruct say :client :text)
(defstruct rename :client :name)
(defstruct invalid :client :reason)
(defstruct ready :_)
(defstruct joined :name)
(defstruct left :name)
(defstruct message :from :text)
(defstruct renamed :from :to)
(defstruct error :reason)
(defstruct room-state :sessions)
(defstruct client-state :conn :room :client)

(clos/defgeneric decode)
(clos/defgeneric encode)
(clos/defgeneric handle-room)
(clos/defgeneric handle-client)

(defn ready-message "Returns the ready outbound message." [] (struct ready nil))

(defn- clean [x fallback] (or (not-empty (str x)) fallback))

(clos/defmethod decode [client (m {"type" "join"})]
  (struct join client (clean (get m "name") "anon")))

(clos/defmethod decode [client (m {"type" "say"})]
  (struct say client (str (get m "text"))))

(clos/defmethod decode [client (m {"type" "rename"})]
  (struct rename client (clean (get m "name") nil)))

(clos/defmethod decode [client m] (struct invalid client "unknown-message"))

(clos/defmethod encode [(m ready)] {"type" "ready"})
(clos/defmethod encode [(m joined)] {"type" "joined", "name" (:name m)})
(clos/defmethod encode [(m left)] {"type" "left", "name" (:name m)})

(clos/defmethod encode [(m message)]
  {"type" "message", "from" (:from m), "text" (:text m)})

(clos/defmethod encode [(m renamed)]
  {"type" "renamed", "from" (:from m), "to" (:to m)})

(clos/defmethod encode [(m error)] {"type" "error", "reason" (:reason m)})

(defn- send-json!
  [^WebSocketChannel conn m]
  (try (WebSockets/sendText (json/write-str (encode m)) conn nil)
       (catch Throwable _ nil)))

(defn- broadcast!
  [state m]
  (doseq [client (keys (:sessions state))] (ch/send! client m)))

(defn- session-name
  [state client]
  (get-in state [:sessions client :name] "anon"))

(clos/defmethod handle-room [(state room-state) (event client-opened)]
  (let [client (:client event)
        state (update state :sessions assoc client {:name "anon"})]
    (ch/send! client (ready-message))
    state))

(clos/defmethod handle-room [(state room-state) (event client-closed)]
  (let [client (:client event)
        name (session-name state client)
        state (update state :sessions dissoc client)]
    (broadcast! state (struct left name))
    state))

(clos/defmethod handle-room [(state room-state) (event join)]
  (let [client (:client event)
        name (:name event)
        state (assoc-in state [:sessions client :name] name)]
    (broadcast! state (struct joined name))
    state))

(clos/defmethod handle-room [(state room-state) (event say)]
  (broadcast!
    state
    (struct message (session-name state (:client event)) (:text event)))
  state)

(clos/defmethod handle-room [(state room-state) (event rename)]
  (let [client (:client event)
        old-name (session-name state client)
        new-name (or (:name event) old-name)
        state (assoc-in state [:sessions client :name] new-name)]
    (broadcast! state (struct renamed old-name new-name))
    state))

(clos/defmethod handle-room [(state room-state) (event invalid)]
  (ch/send! (:client event) (struct error (:reason event)))
  state)

(clos/defmethod handle-client [(state client-state) (event socket-text)]
  (try (ch/send! (:room state)
                 (decode (:client state) (json/read-str (:payload event))))
       (catch Exception _ (handle-client state (struct error "invalid-json"))))
  state)

(clos/defmethod handle-client [(state client-state) (event socket-closed)]
  (ch/send! (:room state) (struct client-closed (:client state)))
  state)

(clos/defmethod handle-client [(state client-state) event]
  (send-json! (:conn state) event)
  state)

(defn room
  "Returns room actor behavior with state."
  [state]
  (fn [event] (ch/become! ch/*self* (room (handle-room state event)))))

(defn client
  "Returns client actor behavior with state."
  [state]
  (fn [event] (ch/become! ch/*self* (client (handle-client state event)))))

(defn- receive-listener
  [client-target]
  (proxy [AbstractReceiveListener] []
    (^void onFullTextMessage
      [^WebSocketChannel _ ^BufferedTextMessage message]
      (ch/send! client-target (struct socket-text (.getData message))))))

(defn- ws-handler
  [room-target]
  (reify
    WebSocketConnectionCallback
      (^void onConnect
        [_ ^WebSocketHttpExchange _ ^WebSocketChannel channel]
        (let [client-actor (ch/spawn (:universe room-target))
              client-target (ch/lane client-actor)]
          (ch/become!
            client-actor
            (client (struct client-state channel room-target client-target)))
          (ch/send! room-target (struct client-opened client-target))
          (.set (.getCloseSetter channel)
                (reify
                  ChannelListener
                    (handleEvent [_ _]
                      (ch/send! client-target (struct socket-closed nil)))))
          (.set (.getReceiveSetter channel) (receive-listener client-target))
          (.resumeReceives channel)))))

(defn- resources
  []
  (let [root "tailrecursion/chasma/demos/chat"
        loader (.getContextClassLoader (Thread/currentThread))]
    (doto (ResourceHandler. (ClassPathResourceManager. loader root))
      (.addWelcomeFiles (into-array String ["index.html"]))
      (.setDirectoryListingEnabled false))))

(defn- handler
  [room-target]
  (let [routes (Handlers/path)]
    (.addPrefixPath routes "/ws" (Handlers/websocket (ws-handler room-target)))
    (.addPrefixPath routes "/" ^HttpHandler (resources))
    routes))

(defn- server
  [port room-target]
  (-> (Undertow/builder)
      (.addHttpListener port "0.0.0.0")
      (.setHandler (handler room-target))
      (.build)))

(defn -main
  [& [port]]
  (let [port (parse-long (or port "8080"))
        universe (ch/start! (ch/universe))
        room-target (ch/lane (ch/spawn universe (room (struct room-state {}))))
        ^Undertow undertow (server port room-target)]
    (.start undertow)
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn [] (.stop undertow) (ch/stop! universe))))
    (println (str "Chat demo listening on http://localhost:" port "/"))))
