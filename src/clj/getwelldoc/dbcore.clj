(ns getwelldoc.dbcore
  (:require [getwelldoc.config :as config]
            [getwelldoc.dbutils :as du]
            [jdbc.core :as jdbc]
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


;;------------------------------------------------------------------------------
;; querying field/lease/well/dual matches
;;------------------------------------------------------------------------------
(defn- matching-wells-select-list
  "returns the select clause for get-matching-wells
  Asserts if nothing selected"
  [select-set]
  (let [svec
        [(if (contains? select-set :field)
           [:%rtrim.f.opru_fld_nme "opru_fld_nme"])
         (if (contains? select-set :lease)
           [:%rtrim.w.prod_govt_lse_nbr "prod_govt_lse_nbr"])
         (if (contains? select-set :well)
           [:%rtrim.w.well_nbr "well_nbr"])
         (if (contains? select-set :cmpl)
           [:%rtrim.w.well_cmpl_seq_nbr "well_cmpl_seq_nbr"])]
        nnsvec (filter (complement nil?) svec)]
    (if-not (seq nnsvec)
      (throw (Exception. (format "Nothing usable in select-set: %s" select-set))))
    (apply sqlh/select nnsvec)))

(defn- matching-wells-where-clause
  "builds the where clause for get-matching-wells.  nil if unconstrained"
  [where-map]
  (let [wvec
        [[:= :f.opru_fld_nbr :w.opru_fld_nbr]
         (if (contains? where-map :field)
           [:like :f.opru_fld_nme (format "%s%%" (:field where-map))])
         (if (contains? where-map :lease)
           [:like :w.prod_govt_lse_nbr (format "%s%%" (:lease where-map))])
         (if (contains? where-map :well)
           [:like :w.well_nbr (format "%s%%" (:well where-map))])
         (if (contains? where-map :cmpl)
           [:like :w.well_cmpl_seq_nbr (format "%s%%" (:cmpl where-map))])]
        nnwvec (filter (complement nil?) wvec)]
    (if (seq nnwvec) (apply sqlh/where nnwvec))))

(defn- matching-wells-order-by-clause
  "builds the order-by clause for get-matching-wells"
  [select-set]
  (let [ovec
        [(if (contains? select-set :field) :opru_fld_nme)
         (if (contains? select-set :lease) :prod_govt_lse_nbr)
         (if (contains? select-set :well)  :well_nbr)
         (if (contains? select-set :cmpl)  :well_cmpl_seq_nbr)]
        nnovec (filter (complement nil?) ovec)]
    (assert (seq nnovec) "Nothing in select list")
    (apply sqlh/order-by nnovec)))

(defn- matching-wells-results
  "makes a rectangle out of the matching wells result set"
  [select-set rows]
  (let [keys
        (filter (complement nil?)
                [(if (contains? select-set :field) :opru_fld_nme)
                 (if (contains? select-set :lease) :prod_govt_lse_nbr)
                 (if (contains? select-set :well)  :well_nbr)
                 (if (contains? select-set :cmpl)  :well_cmpl_seq_nbr)])
        select-values (comp vals select-keys)]
    (map (fn[r] (select-values r keys)) rows)))

(defn get-matching-wells
  "returns an array of array of the matching tuples which are (f,l,w,d)
  containing only the columns indicated by 'select-set' set, and constrained
  by the match criteria of 'where-map'.  Use this right, you can figure out
  fields, leases in field, wells in a (field, lease) etc.

  select-set must contain at least one item

  where-map may be empty (indicating no constraints)

  keywords for both select-set and where-map are
  {:field :lease :well :cmpl}

  result is vector of vector with the inner vector being in [flwc] order
  including only the requested fields"

  [dsn {:keys [select-set where-map]
        :or   {select-set #{:field :lease :well :cmpl}
               where-map  {}}}]
  (let [qry
        (merge (matching-wells-select-list select-set)
               (matching-wells-where-clause where-map)
               (sqlh/modifiers :distinct)
               (sqlh/from [:well_mstr_tb :w] [:fld_nme_lkup_tb :f])
               (matching-wells-order-by-clause select-set))]
    (with-open [dbc (jdbc/connection (du/dbspec-of dsn))]
      ;(du/query-rows dbc qry)
      (matching-wells-results select-set (du/query-rows dbc qry)))))

;;------------------------------------------------------------------------------