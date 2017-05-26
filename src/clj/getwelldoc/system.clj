(ns getwelldoc.system
  (:require [taoensso.sente :as sente]
            [clojure.tools.logging :as log]))


;sente setup, This function will be called whenever a new channel is open
(defn- get-user-id [request]
  (str (java.util.UUID/randomUUID))) ;; Random user

(def ws-connection (sente/make-channel-socket! {:user-id-fn get-user-id}))
(let [{:keys [ch-recv send-fn ajax-post-fn ajax-get-or-ws-handshake-fn
              connected-uids]}
      ws-connection]
  (def ring-ws-post ajax-post-fn)
  (def ring-ws-handoff ajax-get-or-ws-handshake-fn)
  (def receive-channel ch-recv)
  (def channel-send! send-fn)
  (def connected-uids connected-uids))

(defn- ws-msg-handler []
  (fn [{:keys [event] :as msg} _]
    (let [[id data :as ev] event]
      (log/info "Event: " id))))

(defn ws-message-router []
  (sente/start-chsk-router-loop! (ws-msg-handler) receive-channel))