(ns getwelldoc.system
  (:require [taoensso.sente :as sente]
            [clojure.tools.logging :as log]
            [org.httpkit.server :as http-kit]
            [taoensso.sente.server-adapters.http-kit :refer (get-sch-adapter)]))

(let [;; Serializtion format, must use same val for client + server:
      packer :edn ; Default packer, a good choice in most cases
      ;; (sente-transit/get-transit-packer) ; Needs Transit dep

      chsk-server
      (sente/make-channel-socket-server!
        (get-sch-adapter) {:packer packer})

      {:keys [ch-recv send-fn connected-uids
              ajax-post-fn ajax-get-or-ws-handshake-fn]}
      chsk-server]
  (def ring-ws-post ajax-post-fn)
  (def ring-ws-handoff ajax-get-or-ws-handshake-fn)
  (def receive-channel ch-recv)
  (def channel-send! send-fn)
  (def connected-uids connected-uids))

;; We can watch this atom for changes if we like
(add-watch connected-uids :connected-uids
           (fn [_ _ old new]
             (when (not= old new)
               (log/info "Connected uids change: %s" new))))

;; Messages handler
(defn login-handler
  "Here's where you'll add your server-side login/auth procedure (Friend, etc.).
  In our simplified example we'll just always successfully authenticate the user
  with whatever user-id they provided in the auth request."
  [ring-req]
  (let [{:keys [session params]} ring-req
        {:keys [user-id]} params]
    (log/info "Login request: %s" params)
    (log/info "Session: %s" (str session))
    {:status 200 :session (assoc session :uid user-id)}))

(defn- ws-msg-handler []
  (fn [{:keys [event] :as msg} _]
    (let [[id data :as ev] event]
      (log/info "Event: " id))))

(defn ws-message-router []
  (sente/start-chsk-router-loop! (ws-msg-handler) receive-channel))