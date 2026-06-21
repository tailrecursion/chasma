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

(defstruct opened :conn)
(defstruct closed :conn)
(defstruct text :conn :payload)
(defstruct parsed :conn :message)
(defstruct room-state :sessions)

(clos/defgeneric handle)
(clos/defgeneric handle-client)
(clos/defgeneric decode)

(defn- send-json!
  [^WebSocketChannel conn m]
  (WebSockets/sendText (json/write-str m) conn nil))

(defn- broadcast!
  [state m]
  (doseq [conn (keys (:sessions state))] (send-json! conn m)))

(defn- session-name [state conn] (get-in state [:sessions conn :name] "anon"))

(clos/defmethod decode [(m {"type" "join"})] m)
(clos/defmethod decode [(m {"type" "say"})] m)
(clos/defmethod decode [(m {"type" "rename"})] m)
(clos/defmethod decode [m] {"type" "error", "reason" "unknown-message"})

(clos/defmethod handle [(state room-state) (event opened)]
  (let [conn (:conn event)
        state (update state :sessions assoc conn {:name "anon"})]
    (send-json! conn {"type" "ready"})
    state))

(clos/defmethod handle [(state room-state) (event closed)]
  (let [conn (:conn event)
        name (session-name state conn)
        state (update state :sessions dissoc conn)]
    (broadcast! state {"type" "left", "name" name})
    state))

(clos/defmethod handle [(state room-state) (event text)]
  (try (let [message (decode (json/read-str (:payload event)))]
         (handle state (struct parsed (:conn event) message)))
       (catch Exception _
         (send-json! (:conn event) {"type" "error", "reason" "invalid-json"})
         state)))

(clos/defmethod handle [(state room-state) (event parsed)]
  (handle-client state (:conn event) (:message event)))

(clos/defmethod handle-client [(state room-state) conn (m {"type" "join"})]
  (let [name (or (not-empty (str (get m "name"))) "anon")
        state (assoc-in state [:sessions conn :name] name)]
    (broadcast! state {"type" "joined", "name" name})
    state))

(clos/defmethod handle-client [(state room-state) conn (m {"type" "say"})]
  (broadcast! state
              {"type" "message",
               "from" (session-name state conn),
               "text" (str (get m "text"))})
  state)

(clos/defmethod handle-client [(state room-state) conn (m {"type" "rename"})]
  (let [old-name (session-name state conn)
        new-name (or (not-empty (str (get m "name"))) old-name)
        state (assoc-in state [:sessions conn :name] new-name)]
    (broadcast! state {"type" "renamed", "from" old-name, "to" new-name})
    state))

(clos/defmethod handle-client [(state room-state) conn (m {"type" "error"})]
  (send-json! conn m)
  state)

(defn- room
  [state]
  (fn [event] (ch/become! ch/*self* (room (handle state event)))))

(defn- receive-listener
  [room-target]
  (proxy [AbstractReceiveListener] []
    (^void onFullTextMessage
      [^WebSocketChannel channel ^BufferedTextMessage message]
      (ch/send! room-target (struct text channel (.getData message))))))

(defn- ws-handler
  [room-target]
  (reify
    WebSocketConnectionCallback
      (^void onConnect
        [_ ^WebSocketHttpExchange _ ^WebSocketChannel channel]
        (ch/send! room-target (struct opened channel))
        (.set (.getCloseSetter channel)
              (reify
                ChannelListener
                  (handleEvent [_ ch]
                    (ch/send! room-target (struct closed ch)))))
        (.set (.getReceiveSetter channel) (receive-listener room-target))
        (.resumeReceives channel))))

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
