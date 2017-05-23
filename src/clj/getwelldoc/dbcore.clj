(ns getwelldoc.dbcore
  (:require [getwelldoc.config :as config]
            [getwelldoc.dbutils :as du]
            [honeysql.core :as sql]
            [honeysql.helpers :as sqlh]))


(defn get-db-rev
  "returns the db revision of the winglue database, dbc
  the revision is the same as WinGLUE, 3.18 reported as 318"
  [dbc]
  ;            [:modifiers ["TOP 1"]]
  (let [qry
        (-> (sqlh/select :major_rev_nbr :minor_rev_nbr)
            (sqlh/from :asi_schema_rev_tb)
            (sqlh/order-by [:major_rev_nbr :desc] [:minor_rev_nbr :desc]))
        row (first (du/query-rows dbc qry))]
    (+ (* 100 (:major_rev_nbr row)) (:minor_rev_nbr row))))

(defn get-data-source-revs
  "returns the revision of all configured data sources"
  []
  (into {}
        (for [dbkey (keys (:data-sources @config/tao2-cfg))]
          (let [dbrev (get-db-rev (du/dbspec-of dbkey))
                dsn (assoc (dbkey (:data-sources @config/tao2-cfg)) :db-rev dbrev)]
            [dbkey dsn]))))
