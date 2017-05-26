(ns getwelldoc.core
  (:gen-class)
  (:require [getwelldoc.config :as config]
            [getwelldoc.utils :as utils]
            [getwelldoc.database.core :as dbcore]
            [getwelldoc.common :as com]
            [getwelldoc.mimerefs :as mref :refer [str->ref realize-refstr]]
            [getwelldoc.system :as sys]
            [org.httpkit.server :as server]
            [ring.util.response :as response]
            [compojure.core :as compcore :refer [defroutes GET POST]]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [clojure.pprint :as pp]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]))

(defroutes app-routes
           (GET "/" [] (response/resource-response "public/index.html"))
           (GET  "/chsk" req (sys/ring-ws-handoff req))
           (POST "/chsk" req (sys/ring-ws-post req))
           (POST "/login" req (sys/login-handler req))
           (route/resources "/")
           (route/not-found "<h1>Page not found</h1>"))

(defn- wrap-request-logging [handler]
  (fn [{:keys [request-method uri] :as req}]
    (let [resp (handler req)]
      (log/info (name request-method) (:status resp)
                (if-let [qs (:query-string req)]
                  (str uri "?" qs) uri))
      resp)))

(def app
  (-> app-routes
      (handler/site)
      (wrap-request-logging)))

(defn -main [& args]
  (sys/ws-message-router)
  (time (server/run-server app {:port 3000}))
  (time (com/initinfo))
  (time (com/get-well-mstr-map))
  (time (com/get-modl-ctrl-map)))



