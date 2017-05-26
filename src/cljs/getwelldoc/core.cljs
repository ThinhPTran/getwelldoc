(ns getwelldoc.core
  (:require [goog.dom :as gdom]
            [getwelldoc.db :as mydb]
            [getwelldoc.serverevents :as se]
            [reagent.core :as reagent]))





;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Page

(defn page []
  (let [input-text (:input-text @mydb/app-state)]
    [:div ""
     [:h1 "Sendte reference example"]
     [:p "A WebSocket has been configured for this example"]
     [:hr]
     [:p [:strong "Step 1: "] " try hitting the buttons:"]
     [:p
      [:button#btn1 {:type "button"} "chsk-send! (w/o reply)"]
      [:button#btn2 {:type "button"} "chsk-send! (with reply)"]]
     [:p
      [:button#btn3 {:type "button"} "Test rapid server>user async pushes"]
      [:button#btn4 {:type "button"} "Toggle server>user async broadcast push loop"]]
     [:p
      [:button#btn5 {:type "button"} "Disconnect!"]
      [:button#btn6 {:type "button"} "Reconnect"]]
     [:hr]
     [:h2 "Step 2: try login with a user-id"]
     [:p "The server can use this id to send events to *you* specifically."]
     [:p
      [:input#input-login {:type :text
                           :placeholder "User-id"
                           :value input-text
                           :onChange se/usernameChange}]
      [:button#btn-login {:type "button"
                          :onClick se/loginHandler} "Secure login!"]]
     [:div (str "input-text: " input-text)]]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Initialize App

(defn dev-setup []
  (when ^boolean js/goog.DEBUG
    (enable-console-print!)
    (println "dev mode")))


(defn reload []
  (reagent/render [page]
                  (.getElementById js/document "app")))

(defn ^:export main []
  (dev-setup)
  (reload))
