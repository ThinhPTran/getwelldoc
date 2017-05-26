(ns getwelldoc.serverevents
  (:require
    [goog.dom :as gdom]
    [reagent.core :as r]
    [getwelldoc.db :as mydb]
    [clojure.string :as str]
    [taoensso.encore :as encore :refer-macros (have have?)]
    [taoensso.timbre :as timbre :refer-macros (tracef debugf infof warnf errorf)]
    [taoensso.sente :as sente :refer (cb-success?)]))



; sente js setup
(let [chsk-type :auto
      ;; Serializtion format, must use same val for client + server:
      packer :edn ; Default packer, a good choice in most cases
      ;; (sente-transit/get-transit-packer) ; Needs Transit dep

      {:keys [chsk ch-recv send-fn state]}
      (sente/make-channel-socket-client!
        "/chsk" ; Must match server Ring routing URL
        {:type   chsk-type
         :packer packer})]
  (def receive-channel ch-recv)
  (def send-channel! send-fn)
  (def chsk chsk)
  (def chsk-state state))

;; Event handlers

(defn usernameChange [_]
  (let [v (.-value (gdom/getElement "input-login"))]
    ;(.log js/console "change something!!!: " v)
    (swap! mydb/app-state assoc :input-text v)))

; Login handler
(defn loginHandler [ev]
  (let [user-id (:input-text @mydb/app-state)]
    (if (str/blank? user-id)
      (js/alert "Please enter a user-id first")
      (do
        (.log js/console "Logging in with user-id %s" user-id)

        ;;; Use any login procedure you'd like. Here we'll trigger an Ajax
        ;;; POST request that resets our server-side session. Then we ask
        ;;; our channel socket to reconnect, thereby picking up the new
        ;;; session.

        (sente/ajax-lite "/login"
                         {:method :post
                          :headers {:X-CSRF-Token (:csrf-token @chsk-state)}
                          :params  {:user-id (str user-id)}}

                         (fn [ajax-resp]
                           (.log js/console "Ajax login response: %s" ajax-resp)
                           (let [login-successful? true] ; Your logic here

                             (if-not login-successful?
                               (.log js/console "Login failed")
                               (do
                                 (.log js/console "Login successful")
                                 (sente/chsk-reconnect! chsk))))))))))


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
    (.log js/console "Unmatched connection event with " id " and data " data)))

(sente/start-chsk-router-loop! event-handler receive-channel)



