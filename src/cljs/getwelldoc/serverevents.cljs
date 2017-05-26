(ns getwelldoc.serverevents
  (:require
    [reagent.core :as r]
    [taoensso.sente :as sente :refer (cb-success?)]))

; sente js setup
(def ws-connection (sente/make-channel-socket! "/channel" {:type :auto}))
(let [{:keys [ch-recv send-fn]}
      ws-connection]
  (def receive-channel (:ch-recv ws-connection))
  (def send-channel! (:send-fn ws-connection)))


; handle application-specific events
(defn- app-message-received [[msgType data]]
  (.log js/console "Received message: \n")
  (.log js/console "msgType: " msgType "\n")
  (.log js/console "data: " data "\n"))

; handle websocket-connection-specific events
(defn- channel-state-message-received [state]
  (if (:first-open? state)
    (.log js/console "First open!!!\n")))

; main router for websocket events
(defn- event-handler [[id data] _]
  (case id
    :chsk/state (channel-state-message-received data)
    :chsk/recv (app-message-received data)
    (.log js/console "Unmatched connection event")))

(sente/start-chsk-router-loop! event-handler receive-channel)



