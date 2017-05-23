(ns getwelldoc.dbutils
  (:require [getwelldoc.config :as config]
            [honeysql.core :as sql]
            [jdbc.core :as jdbc]))

(defn dbspec-of [dsnkey]
  "gets the db connection specification from the config for the given dsn"
  [dsnkey]
  (let [dbs (dsnkey (:data-sources @config/tao2-cfg))]
    (if (nil? dbs)
      (throw (Exception.
               (format "no such datasource: %s" (name dsnkey)))))
    dbs))

(defn query-rows
  "helper to get rows of a query"
  [dbc qry]
  (let [sqltext (sql/format qry)
        _ (comment (prn sqltext))]
    (jdbc/fetch dbc sqltext)))