(ns getwelldoc.database.core
  (:require [clj-time.coerce :as time-coerce]
            [jdbc.core :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :as sqlh]
            [clojure.set :as cset]
            [getwelldoc.config :as config]
            [getwelldoc.database.utils :as du]
            [getwelldoc.mimerefs :as mref]
            [getwelldoc.utils :as util]))


;;;------------------------------------------------------------------------------
;;; mime-fetchers for wgdb/well
;;;------------------------------------------------------------------------------
(defn- mykw
  "creates a keyword in my namespace"
  [nm]
  (keyword (format "getwelldoc.%s" nm)))

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
  "returns the revision of of all configured data sources"
  []
  (into {}
        (for [dbkey  (keys (:data-sources @config/tao2-cfg))]
          (let [dbrev (get-db-rev (du/dbspec-of dbkey))
                dsn (assoc (dbkey (:data-sources @config/tao2-cfg)) :db-rev dbrev)]
            [dbkey dsn]))))

(defn execute-well-update
  [dsn statement pwi-nbr]
  (with-open [dbc (jdbc/connection (du/dbspec-of dsn))]
    (do
      (jdbc/execute dbc (sql/format statement))
      (if (< 317 (:db-rev (dsn (:data-sources @config/tao2-cfg))))
        (let [last-save-stmt (-> (sqlh/update :well_mstr_tb)
                                 (sqlh/where [:= :pwi_nbr pwi-nbr])
                                 (sqlh/sset {:last_saved_dte
                                             (du/jodatime->sqltime (new java.util.Date))}))]
          (jdbc/execute dbc (sql/format last-save-stmt)))))))


;;------------------------------------------------------------------------------
;; wells
;;------------------------------------------------------------------------------
(def wells-fixums
  {:well_nbr               (du/strcol "name")
   :prod_govt_lse_nbr      (du/strcol "lease")
   :well_dual_cde          (du/strcol "dual")
   :api_well_nbr           (du/strcol "api-number")
   :well_type_cde          (du/strcol "type")
   :well_kb_elev_qty       (du/dblcol "kb-elev")
   :plfm_jkt_nme           (du/strcol "jacket-name")
   :pwi_nbr                (du/intcol "pwi")
   :well_cmpl_seq_nbr      (du/strcol "cmpl-seq")
   :cmpl_ptr               (du/intcol "cmpl")
   :well_head_md_qty       (du/dblcol "wellhead-md")
   :last_saved_dte         (du/dtecol "last-saved-date")})

(def well-master-key-redos
  {:name         :well_nbr
   :lease        :prod_govt_lse_nbr
   :dual         :well_dual_cde
   :api-number   :api_well_nbr
   :type         :well_type_cde
   :kb-elev      :well_kb_elev_qty
   :jacket-name  :plfm_mkt_nme
   :pwi          :pwi_nbr
   :cmpl-seq     :well_cmpl_seq_nbr
   :cmpl         :cmpl_ptr
   :wellhead-md  :well_head_md_qty})

(defn get-well-db-keys
  "merge the relevent database keys (:pwi :seq) into the dbkeys
  for a given well.  If the well does not exist, yield nil
  specification can be with :field, :lease, :well, :cmpl
  or it can be by :api-number the options are mutually exclusive"
  [dbc dbkeys]
  (let [kcount (fn [coll keys]
                 (reduce (fn [c k] (if (k coll) (inc c) c)) 0 keys))
        nflw (kcount dbkeys [:field :lease :well :cmpl])
        apino (:api-number dbkeys)
        qry (cond
              ;; got 4 flwc keys, no api
              (and (nil? apino) (= 4 nflw))
              (->(sqlh/select :pwi_nbr :cmpl_ptr)
                 (sqlh/from :well_mstr_tb :fld_nme_lkup_tb)
                 (sqlh/where [:and
                              [:= :%rtrim.well_nbr (:well dbkeys)]
                              [:= :well_mstr_tb.opru_fld_nbr
                               :fld_nme_lkup_tb.opru_fld_nbr]
                              [:= :%rtrim.fld_nme_lkup_tb.opru_fld_nme
                               (:field dbkeys)]
                              [:= :%rtrim.well_mstr_tb.prod_govt_lse_nbr
                               (:lease dbkeys)]
                              [:= :%rtrim.well_mstr_tb.well_cmpl_seq_nbr
                               (:cmpl dbkeys)]]))

              ;; got api and no flwc keys
              (and apino (zero? nflw))
              (->(sqlh/select :pwi_nbr :cmpl_ptr)
                 (sqlh/from :well_mstr_tb)
                 (sqlh/where [:= :%rtrim.well_mstr_tb.api_well_nbr apino]))

              ;; neener
              :else nil)]
    (if qry
      (if-let [row (first (du/query-rows dbc qry))]
        (merge dbkeys  {:pwi (int (:pwi_nbr row))
                        :cmpl (int (:cmpl_ptr row))})))))

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

(defn resolve-well-by-api
  "resolves a well api number into a single well, or indicates an error
   if works,     { :field f :lease l :well w :cmpl c }
   if not found  { :error :not-found }
   if multiples  { :error :ambiguous
                   :matching [ {:field :lease :well :cmpl} ... ]}"
  [dsn api-number]
  (let [qry
        (-> (sqlh/select
              [:%rtrim.f.opru_fld_nme "field"]
              [:%rtrim.w.prod_govt_lse_nbr "lease"]
              [:%rtrim.w.well_nbr "well"]
              [:%rtrim.w.well_cmpl_seq_nbr "cmpl"])
            (sqlh/from [:well_mstr_tb :w] [:fld_nme_lkup_tb :f])
            (sqlh/where [:and
                         [:= :w.opru_fld_nbr :f.opru_fld_nbr]
                         [:= :%rtrim.w.api_well_nbr api-number]]))
        dbc (jdbc/connection (du/dbspec-of dsn))
        rows (du/query-rows dbc qry)]
    (condp = (count rows)
      0 {:error :not-found}
      1 (first rows)
      {:error :ambiguous
       :matching rows})))

(defn get-well-master
  "get master record for a well"
  [dbc pwi dsn]
  (let [db-rev (:db-rev (dsn (:data-sources @config/tao2-cfg)))
        sel-list (into [] (for [x (remove nil? [:well_nbr
                                                :prod_govt_lse_nbr
                                                :well_dual_cde
                                                :api_well_nbr
                                                :well_type_cde
                                                :well_kb_elev_qty
                                                :plfm_jkt_nme
                                                :pwi_nbr
                                                :well_cmpl_seq_nbr
                                                :cmpl_ptr
                                                :well_head_md_qty
                                                (when (< 317 db-rev) :last_saved_dte)])]
                            [x x]))
        qry (-> (apply sqlh/select sel-list)
                (sqlh/from :well_mstr_tb)
                (sqlh/where [:= :pwi_nbr pwi]))
        row (first (du/query-rows dbc qry))]
    (if row (du/apply-fixums row wells-fixums))))

(defn update-well-master
  [dsn pwi updates]
  (let [ok-keys #{(keys well-master-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-well-master: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :well_mstr_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/sset (util/rename-map-keys updates well-master-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------

;;------------------------------------------------------------------------------
;; welltest
;;------------------------------------------------------------------------------
(def welltest-fixums
  {:well_ptst_aloc_cde       (du/wgccol "ignore-as-bad")
   :ptst_desc_txt            (du/strcol "description")
   :sepr_prsr_qty            (du/dblcol "separator-press")
   :cmpl_pdtv_ix_qty         (du/dblcol "productivity-index")
   :well_ptst_dte            (du/dtecol "welltest-date")
   :ptst_lg_mismatch_tol_qty (du/dblcol "lift-gas-mismatch-tol")
   :dstm_fln_prsr_qty        (du/dblcol "downstream-flowline-press") ; manifold-press
   :cmpl_max_nflo_qty        (du/dblcol "qmax-liquid")
   :ptst_cmnt_txt            (du/strcol "welltest-comments")
   :ptst_sep_name            (du/strcol "separator-name")
   :well_chok_size_qty       (du/dblcol "meas-wellhead-choke-id")
   :well_ptst_oil_qty        (du/dblcol "meas-oil-rate")
   :well_ptst_wtr_qty        (du/dblcol "meas-water-rate")
   :well_ptst_fgas_qty       (du/dblcol "meas-form-gas-rate")
   :well_ptst_lgas_qty       (du/dblcol "meas-lift-gas-rate")
   :well_csg_prsr_qty        (du/dblcol "meas-casing-head-press")
   :well_ftp_qty             (du/dblcol "meas-flowing-tubing-press")
   :well_ptst_inj_lgas_qty   (du/dblcol "lift-gas-inj-rate") ; used, but not entered?
   :cmpl_est_fbhp_qty        (du/dblcol "est-fbhp")
   :chok_size_caln_qty       (du/dblcol "calib-wellhead-choke-id")
   :ptst_oil_caln_qty        (du/dblcol "calib-oil-rate")
   :ptst_wtr_caln_qty        (du/dblcol "calib-water-rate")
   :ptst_fgas_caln_qty       (du/dblcol "calib-formation-gas-rate")
   :ptst_lgas_caln_qty       (du/dblcol "calib-lift-gas-rate")
   :csg_prsr_caln_qty        (du/dblcol "calib-casing-head-press")
   :ftp_caln_qty             (du/dblcol "calib-flowing-tubing-press")
   :lift_dpth_est_qty        (du/dblcol "est-lift-meas-depth")
   :well_ptst_hrs_qty        (du/dblcol "test-duration-hours")})

(def welltest-history-fixums
  {
   :well_ptst_aloc_cde       (du/wgccol "ignore-as-bad")
   :chok_size_caln_qty       (du/dblcol "calib-wellhead-choke-id")
   :ptst_desc_txt            (du/strcol "description")
   :sepr_prsr_qty            (du/dblcol "separator-press")
   :cmpl_pdtv_ix_qty         (du/dblcol "productivity-index")
   :well_ptst_dte            (du/dtecol "welltest-date")
   :ptst_lg_mismatch_tol_qty (du/dblcol "lift-gas-mismatch-tol")
   :dstm_fln_prsr_qty        (du/dblcol "downstream-flowline-press")
   :cmpl_max_nflo_qty        (du/dblcol "qmax-liquid")
   :ptst_cmnt_txt            (du/strcol "welltest-comments")
   :ptst_sep_name            (du/strcol "separator-name")
   :well_chok_size_qty       (du/dblcol "meas-wellhead-choke-id")
   :well_ptst_oil_qty        (du/dblcol "meas-oil-rate")
   :well_ptst_wtr_qty        (du/dblcol "meas-water-rate")
   :well_ptst_fgas_qty       (du/dblcol "meas-form-gas-rate")
   :well_ptst_lgas_qty       (du/dblcol "meas-lift-gas-rate")
   :well_csg_prsr_qty        (du/dblcol "meas-casing-head-press")
   :well_ftp_qty             (du/dblcol "meas-flowing-tubing-press")
   :well_ptst_inj_lgas_qty   (du/dblcol "lift-gas-inj-rate")
   :cmpl_est_fbhp_qty        (du/dblcol "est-fbhp")
   :ptst_oil_caln_qty        (du/dblcol "calib-oil-rate")
   :ptst_wtr_caln_qty        (du/dblcol "calib-water-rate")
   :ptst_fgas_caln_qty       (du/dblcol "calib-formation-gas-rate")
   :ptst_lgas_caln_qty       (du/dblcol "calib-lift-gas-rate")
   :csg_prsr_caln_qty        (du/dblcol "calib-casing-head-press")
   :ftp_caln_qty             (du/dblcol "calib-flowing-tubing-press")
   :lift_dpth_est_qty        (du/dblcol "est-lift-meas-depth")
   :well_ptst_hrs_qty        (du/dblcol "test-duration-hours")})

(def welltest-key-redos
  {:ignore-as-bad              :well_ptst_aloc_cde
   :description                :ptst_desc_txt
   :separator-press            :sepr_prsr_qty
   :productivity-index         :cmpl_pdtv_ix_qty
   :welltest-date              :well_ptst_dte
   :lift-gas-mismatch-tol      :ptst_lg_mismatch_tol_qty
   :downstream-flowline-press  :dstm_fln_prsr_qty
   :qmax-liquid                :cmpl_max_nflo_qty
   :welltest-comments          :ptst_cmnt_txt
   :separator-name             :ptst_sep_name
   :meas-wellhead-choke-id     :well_chok_size_qty
   :meas-oil-rate              :well_ptst_oil_qty
   :meas-water-rate            :well_ptst_wtr_qty
   :meas-form-gas-rate         :well_ptst_fgas_qty
   :meas-lift-gas-rate         :well_ptst_lgas_qty
   :meas-casing-head-press     :well_csg_prsr_qty
   :meas-flowing-tubing-press  :well_ftp_qty
   :lift-gas-inj-rate          :well_ptst_inj_lgas_qty
   :est-fbhp                   :cmpl_est_fbhp_qty
   :calib-wellhead-choke-id    :chok_size_caln_qty
   :calib-oil-rate             :ptst_oil_caln_qty
   :calib-water-rate           :ptst_wtr_caln_qty
   :calib-formation-gas-rate   :ptst_fgas_caln_qty
   :calib-lift-gas-rate        :ptst_lgas_caln_qty
   :calib-casing-head-press    :csg_prsr_caln_qty
   :calib-flowing-tubing-press :ftp_caln_qty
   :est-lift-meas-depth        :lift_dpth_est_qty
   :test-duration-hours        :well_ptst_hrs_qty})

(defn get-welltest-history
  "get well test history for the specified well"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select [:well_ptst_dte "dte"])
            (sqlh/from :well_ptst_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/order-by [:dte :desc]))
        rows (du/query-rows dbc qry)]
    (map :dte rows)))

(defn new-welltest
  "add a new welltest"
  [{:keys [well date]}]
  (with-open [dbc (jdbc/connection (du/dbspec-of (:dsn well)))]
    (if-let [well (get-well-db-keys dbc well)]
      (let [date (time-coerce/from-date date)
            stmt(->(sqlh/insert-into :well_ptst_tb)
                   (sqlh/columns :pwi_nbr :well_ptst_dte)
                   (sqlh/values [[(:pwi well) (du/jodatime->sqltime date)]]))]
        (jdbc/execute dbc (sql/format stmt))
        (mref/->refstr (mykw "welltest") {:dsn (:dsn well) :pwi (:pwi well) :time (du/jodatime->str date)})))))

(defn- get-calib
  "get the calibrated value - use measured if calibrated is nil"
  [wt [meas calib]]
  (assoc wt calib (if (nil? (calib wt)) (if (nil? (meas wt)) 0.0 (meas wt)) (calib wt))))

(defn- set-calib
  "set calibrated value = measured value if calibrated is nil"
  [wt]
  (reduce get-calib wt
          [[:meas-oil-rate             :calib-oil-rate]
           [:meas-water-rate           :calib-water-rate]
           [:meas-wellhead-choke-id    :calib-wellhead-choke-id]
           [:meas-form-gas-rate        :calib-formation-gas-rate]
           [:meas-lift-gas-rate        :calib-lift-gas-rate]
           [:meas-casing-head-press    :calib-casing-head-press]
           [:meas-flowing-tubing-press :calib-flowing-tubing-press]]))

(defn get-welltest
  "get welltest"
  [dbc pwi dte]
  (let [qry
        (->(sqlh/select :*)
           (sqlh/from :well_ptst_tb)
           (sqlh/where [:and
                        [:= :pwi_nbr pwi]
                        [:= :well_ptst_dte dte]]))
        row (first (du/query-rows dbc qry))]
    (if row
      (let [rslt (util/augment-welltest (set-calib (du/apply-fixums row welltest-fixums)))]
        rslt))))

(defn get-welltests
  "get welltests for a well"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :well_ptst_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/order-by [:well_ptst_dte :desc]))
        rslt  (map (fn [r] (util/augment-welltest (set-calib (du/apply-fixums r welltest-fixums))))
                   (du/query-rows dbc qry))]
    rslt))

(defn update-welltest
  [dsn pwi dte updates]
  (let [ ok-keys (set (keys welltest-key-redos))
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-welltest: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :well_ptst_tb)
            (sqlh/where [:and
                         [:= :pwi_nbr pwi]
                         [:= :well_ptst_dte (du/str->sqltime dte)]])
            (sqlh/sset (util/rename-map-keys updates welltest-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------
;; directional survey
;;------------------------------------------------------------------------------
(def dsvy-fixums
  {:well_dsvy_md_qty (du/dblcol "meas-depth-list")
   :well_tvd_qty     (du/dblcol "vert-depth-list")})

(def dsvy-key-redos
  {:meas-depth-list  :well_dsvy_md_qty
   :tvd :well_tvd_qty})

(defn get-dsvy
  "get a directional survey using cmpl number from well"
  [dbc cmpl]
  (let [qry
        (->(sqlh/select :*)
           (sqlh/from :dsvy_tb)
           (sqlh/where [:= :cmpl_ptr cmpl])
           (sqlh/order-by :well_dsvy_md_qty))
        rows (map #(du/apply-fixums % dsvy-fixums) (du/query-rows dbc qry))]
    (du/vector-slices rows)))

(defn update-dsvy
  [dsn pwi cmpl updates]
  (let [ok-keys #{(keys dsvy-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-dsvy: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :dsvy_tb)
            (sqlh/where [:and
                         [:= :pwi_nbr pwi]
                         [:= :cmpl_ptr cmpl]])
            (sqlh/sset (util/rename-map-keys updates dsvy-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------
;; reservoir
;;------------------------------------------------------------------------------
(def reservoir-history-fixums
  {:rsvr_surv_dte (du/dtecol "date")
   :cmpl_sbhp_qty (du/dblcol "sbhp")})

(defn get-reservoir-survey-history
  "get reservoir survey history (note this brings sbhp with it, since only one)"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :rsvr_surv_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/order-by [:rsvr_surv_dte :desc]))
        rows (map #(du/apply-fixums % reservoir-history-fixums)
                  (du/query-rows dbc qry))]
    rows))

(defn get-reservoir-survey
  "get reservoir survey history (note this brings sbhp with it, since only one)"
  [dbc pwi dte]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :rsvr_surv_tb)
            (sqlh/where [:and
                         [:= :pwi_nbr pwi]
                         [:= :rsvr_surv_dte (du/jodatime->sqltime dte)]]))
        row (first (du/query-rows dbc qry))]
    (if row (du/apply-fixums row reservoir-history-fixums))))

(def reservoir-fixums
  {:well_api_grav_qty   (du/dblcol "well-api-gravity")
   :rsvr_bp_prsr_qty    (du/dblcol "bubble-point-press")
   :cmpl_max_nflo_qty   (du/dblcol "max-inflow")
   :rsvr_oil_fvf_qty    (du/dblcol "oil-fvf")
   :rsvr_flud_visc_qty  (du/dblcol "rsvr-fluid-visc")
   :rsvr_gas_spg_qty    (du/dblcol "rsvr-gas-spg")
   :rsvr_wtr_spg_qty    (du/dblcol "rsvr-water-spg")
   :perf_bht_qty        (du/dblcol "perf-bht")
   :wlhd_sttc_temp_qty  (du/dblcol "wlhd-static-temp")
   :rsvr_flud_perm_qty  (du/dblcol "rsvr-fluid-perm")
   :cmpl_nflo_skin_qty  (du/dblcol "inflow-skin")
   :cmpl_net_tvt_qty    (du/dblcol "net-tvt")
   :cmpl_drng_rad_qty   (du/dblcol "drainage-radius")
   :cmpl_wlbr_rad_qty   (du/dblcol "wellbore-radius")
   :perf_top_md_qty     (du/dblcol "perf-top-md")
   :darcys_law_flg      (du/wgccol "darcys-law")
   :rsvr_oil_spg_qty    (du/dblcol "rsvr-oil-spg")
   :zone_nme            (du/strcol "zone")
   :hc_rsvr_cde         (du/dblcol "hc-rsvr")
   :rsvr_pvt_modl_cde   (du/intcol "rsvr-pvt-modl")})


(def reservoir-key-redos
  {:well-api-gravity    :well_api_grav_qty
   :bubble-point-press  :rsvr_bp_prsr_qty
   :max-inflow          :cmpl_max_nflo_qty
   :oil-fvf             :rsvr_oil_fvf_qty
   :rsvr-fluid-visc     :rsvr_flud_visc_qty
   :rsvr-gas-spg        :rsvr_gas_spg_qty
   :rsvr-water-spg      :rsvr_wtr_spg_qty
   :perf-bht            :perf_bht_qty
   :wlhd-static-temp    :wlhd_sttc_temp_qty
   :rsvr-fluid-perm     :rsvr_flud_perm_qty
   :inflow-skin         :cmpl_nflo_skin_qty
   :net-tvt             :cmpl_net_tvt_qty
   :drainage-radius     :cmpl_drng_rad_qty
   :wellbore-radius     :cmpl_wlbr_rad_qty
   :perf-top-md         :perf_top_md_qty
   :darcys-law          :darcys_law_flg
   :rsvr-oil-spg        :rsvr_oil_spg_qty
   :zone                :zone_nme
   :hc-rsvr             :hc_rsvr_cde
   :rsvr-pvt-modl       :rsvr_pvt_modl_cde})

(defn get-reservoir
  "gets the current reservoir data (latest date)"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :rsvr_tb)
            (sqlh/where [:= :pwi_nbr pwi]))
        row (first (du/query-rows dbc qry))]
    (if row (du/apply-fixums row reservoir-fixums))))

(defn update-reservoir
  [dsn pwi updates]
  (let [ok-keys #{(keys reservoir-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-reservoir: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :rsvr_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/sset (util/rename-map-keys updates reservoir-key-redos)))]
      (execute-well-update dsn stmt pwi))))


;;------------------------------------------------------------------------------
;; pvt-properties
;;------------------------------------------------------------------------------
(def pvt-properties-fixums
  {:pvt_api_grav_qty          (du/dblcol "api-gravity")
   :pvt_sep_gas_grav_qty      (du/dblcol "sep-gas-gravity")
   :pvt_tank_gas_grav_qty     (du/dblcol "tank-gas-gravity")
   :pvt_rsvr_temp_qty         (du/dblcol "reservoir-temp")
   :pvt_bub_pt_prsr_qty       (du/dblcol "bp-press")
   :pvt_sep_prsr_qty          (du/dblcol "separator-presss")
   :pvt_ovf_fact_qty          (du/dblcol "ovf-factor")
   :pvt_gor_qty               (du/dblcol "gor")})


(def pvt-sample-key-redos
  {:api-gravity      :pvt_api_grav_qty
   :sep-gas-gravity  :pvt_sep_gas_grav_qty
   :tank-gas-gravity :pvt_tank_gas_grav_qty
   :reservoir-temp   :pvt_rsvr_temp_qty
   :bp-pres          :pvt_bub_pt_prsr_qty
   :separator-press         :pvt_sep_prsr_qty
   :ovf-factor       :pvt_ovf_fact_qty
   :gor              :pvt_gor_qty})

(defn get-pvt-properties-history
  "get pvt sample history for the specified well"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :pvt_sample_dte)
            (sqlh/from :pvt_properties_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/order-by [:pvt_sample_dte :desc]))
        rows (map #(du/apply-fixums % pvt-properties-fixums)
                  (du/query-rows dbc qry))]

    rows))

(defn get-pvt-sample
  "get pvt sample for given well and date"
  [dbc pwi]
  ; add dte back in!!!
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :pvt_properties_tb)
            (sqlh/where [:and
                         [:= :pwi_nbr pwi]
                         [:= :pvt_sample_dte (max :pvt_sample_dte)]]))
        ;[:= :pvt_sample_dte dte]

        row (first (du/query-rows dbc qry))]
    (if row (du/apply-fixums row pvt-properties-fixums))))


(defn update-pvt-sample
  [dsn pwi dte updates]
  (let [ok-keys #{(keys pvt-sample-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-pvt-sample: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :pvt_properties_tb)
            (sqlh/where [:and
                         [:= :pwi_nbr pwi]
                         [:= :pvt_sample_dte (du/str->sqltime dte)]])
            (sqlh/sset (util/rename-map-keys updates pvt-sample-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------
;; flowing-gradient-survey
;;------------------------------------------------------------------------------
(defn- get-tvd
  "return the tvd given md list, tvd list and an md"
  [md-list tvd-list md]
  (first (for [x (range (- (count md-list) 1))
               :let [tvd (util/calc-line
                           (nth md-list x)
                           (nth tvd-list x)
                           (nth md-list (+ x 1))
                           (nth tvd-list (+ x 1))
                           md)]
               :when (and (<= (nth md-list x) md)
                          (> (nth md-list (+ x 1)) md))]
           tvd)))

(defn- add-tvd
  "Add the tvd to a map given a completion, the key for the
  measured depth and a key for the vert depth.  The measured depth
  list must be sorted in ascending order."
  [dbc cmpl obj src dest]
  (let [dsvy (get-dsvy dbc cmpl)
        mdlist (into [] (concat [0.0] (:meas-depth-list dsvy)))
        tvdlist (into [] (concat [0.0] (:vert-depth-list dsvy)))]

    (merge obj
           {dest (into []
                       (for [ii (range (count (src obj)))]
                         (get-tvd mdlist tvdlist (nth (src obj) ii))))})))

(defn- get-tail
  "Calculate the end values for an FGS data list
  Returns a vector with [tvd, val]"
  [val-lst md-list perf-tvd grad]
  (if (< 0 (count val-lst))
    (let [bot-tvd (get-tvd (last md-list))]
      (if (and (< bot-tvd perf-tvd) (< 0.0 grad))
        (let [bot-val (+ (* grad (- perf-tvd bot-tvd)) (last val-lst))]
          [bot-tvd bot-val])))))

(defn- add-tails
  "return a new details struct with one extra point added to the
   end by extrapolating to the perforations (if needed) and using
   the pressure or temperature gradients from the flowing survey"
  [details pgrad tgrad]
  (if (every? #(= (count (:meas-depth-list details)) (count (% details))) (keys details))
    details
    (merge details
           (into {}
                 (for [pair [[:avg-pres-list pgrad]
                             [:low-pres-list pgrad]
                             [:high-pres-list pgrad]
                             [:avg-temp-list tgrad]
                             [:low-temp-list tgrad]
                             [:high-temp-list tgrad]]]
                   (let [k (first pair)
                         v (second pair)
                         tvd-list (:vert-depth-list details)
                         vcnt (count tvd-list)]

                     (if (or (nil? v) (> 0.001 v))
                       (if (= (count (k details)) vcnt)
                         [k (k details)]
                         [k (conj (k details) (last (k details)))])
                       [k (conj (k details)
                                (+ (last (k details))
                                   (* v (- (last tvd-list)
                                           (nth tvd-list (- vcnt 2))))))])))))))

(def flowing-gradient-survey-hist-map-fixums
  {:fbhp_bhp_at_perf_qty    (du/dblcol "fbhp-at-perf")
   :fbhp_surv_dte           (du/dtecol "date")
   :fbhp_surv_prsr_grad_qty (du/dblcol "pressure-grad")
   :fbhp_surv_estd_md_qty   (du/dblcol "est-meas-depth")
   :fbhp_surv_temp_grad_qty (du/dblcol "est-temp-grad")
   :fbhp_temp_at_perf_qty   (du/dblcol "temp-at-perf")})

(def flowing-gradient-survey-fixums
  {:fbhp_surv_md_qty        (du/dblcol "meas-depth-list")
   :fbhp_surv_avg_prsr_qty  (du/dblcol "avg-pres-list")
   :fbhp_surv_avg_temp_qty  (du/dblcol "avg-temp-list")
   :fbhp_surv_low_prsr_qty  (du/dblcol "low-pres-list")
   :fbhp_surv_low_temp_qty  (du/dblcol "low-temp-list")
   :fbhp_surv_hi_prsr_qty   (du/dblcol "high-pres-list")
   :fbhp_surv_hi_temp_qty   (du/dblcol "high-temp-list")})

(defn- add-perf-md
  [details perf-md]
  (let [p-md (if (nil? perf-md) 0 perf-md)
        l-md (if (nil? (last (:meas-depth-list details))) 0 (last (:meas-depth-list details)))]
    (if (> p-md l-md)
      (assoc details :meas-depth-list (conj (:meas-depth-list details) perf-md))
      details)))

(defn get-flowing-gradient-survey
  "get a flowing survey"
  [dbc pwi dte cmpl]
  (let [hist-qry
        (-> (sqlh/select :*)
            (sqlh/from :fbhp_surv_tb)
            (sqlh/where [:and
                         [:= :pwi_nbr pwi]
                         [:= :fbhp_surv_dte dte]]))
        rsvr (get-reservoir dbc pwi)
        perf-md (:perf-top-md rsvr)]
    (if-let [hrow (first (du/query-rows dbc hist-qry))]
      (let [result (du/apply-fixums hrow flowing-gradient-survey-hist-map-fixums)
            dtl-qry
            (-> (sqlh/select :*)
                (sqlh/from :fbhp_surv_dtl_tb)
                (sqlh/where [:and
                             [:= :pwi_nbr pwi]
                             [:= :fbhp_surv_dte dte]])
                (sqlh/order-by [:fbhp_surv_md_qty :asc]))
            detail-map (map #(du/apply-fixums % flowing-gradient-survey-fixums)
                            (du/query-rows dbc dtl-qry))]
        (let [vslices1 (du/vector-slices detail-map)
              vslices (add-perf-md vslices1 perf-md)
              vtvd (add-tvd dbc cmpl vslices :meas-depth-list :vert-depth-list)
              rslt (assoc result :detail-map (add-tails vtvd (:pressure-grad result)
                                                        (:est-temp-grad result)))]
          rslt)))))

;;------------------------------------------------------------------------------
;; flowline mechanical
;;------------------------------------------------------------------------------
(def flowline-fixums
  {:fln_sgmt_edst_qty   (du/dblcol "edst-list")
   :fln_sgmt_eht_qty    (du/dblcol "eht-list")
   :fln_sgmt_id_qty     (du/dblcol "id-list")
   :fln_sgmt_ruff_qty   (du/dblcol "ruff-list")
   :fln_sgmt_temp_qty   (du/dblcol "temp-list")})

(def flowline-key-redos
  {:edst-list  :fln_sgmt_edst_qty
   :eht-list   :fln_sgmt_eht_qty
   :id-list    :fln_sgmt_id_qty
   :ruff-list  :fln_sgmt_temp_qty
   :temp-list  :fln_sgmt_temp_qty})

(defn get-flowline-mech
  "get the flowline mechanical data"
  [dbc pwi]
  (let [qry
        (->(sqlh/select :*)
           (sqlh/from :flow_line_mech_tb)
           (sqlh/where [:= :pwi_nbr pwi])
           (sqlh/order-by :fln_sgmt_edst_qty))
        rows (map #(du/apply-fixums % flowline-fixums) (du/query-rows dbc qry))]
    (du/vector-slices rows)))

(defn update-flowline-mech
  [dsn pwi updates]
  (let [ok-keys #{(keys flowline-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-flowline-mech: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :flow_line_mech_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/sset (util/rename-map-keys updates flowline-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------
;; injection mechanical
;;------------------------------------------------------------------------------
(def injmech-fixums
  {:inj_sgmt_bmd_qty     (du/dblcol "meas-depth-list")
   :inj_sgmt_id_qty      (du/dblcol "id-list")
   :inj_cstg_od_qty      (du/dblcol "csng-od-list")
   :inj_sgmt_ruff_qty    (du/dblcol "ruff-list")
   :inj_sgmt_tube_od_qty (du/dblcol "tbg-od-list")})

(def injmech-key-redos
  {:meas-depth-list :inj_sgmt_bmd_qty
   :id-list       :inj_sgmt_id_qty
   :csng-od-list  :inj_cstg_od_qty
   :ruff-list     :inj_sgmt_ruff_qty
   :tbg-od-list   :inj_sgmt_tube_od_qty})

(defn get-injection-mech
  "get the injection mechanical data"
  [dbc pwi]
  (let [qry
        (->(sqlh/select :*)
           (sqlh/from :inj_strg_mech_tb)
           (sqlh/where [:= :pwi_nbr pwi])
           (sqlh/order-by :inj_sgmt_bmd_qty))
        rows (map #(du/apply-fixums % injmech-fixums) (du/query-rows dbc qry))]
    (du/vector-slices rows)))

(defn update-injection-mech
  [dsn pwi updates]
  (let [ok-keys #{(keys injmech-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-injection-mech: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :inj_strg_mech_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/sset (util/rename-map-keys updates injmech-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------
;; production mechanical
;;------------------------------------------------------------------------------
(def prodmech-fixums
  {:prod_sgmt_bmd_qty      (du/dblcol "meas-depth-list")
   :prod_sgmt_id_qty       (du/dblcol "id-list")
   :prod_cstg_od_qty       (du/dblcol "csng-od-list")
   :prod_sgmt_ruff_qty     (du/dblcol "ruff-list")
   :prod_sgmt_tube_od_qty  (du/dblcol "tbg-od-list")})


(def prodmech-key-redos
  {:meas-depth-list :prod_sgmt_bmd_qty
   :id-list       :prod_sgmt_id_qty
   :csng-od-list  :prod_cstg_od_qty
   :ruff-list     :prod_sgmt_ruff_qty
   :tbg-od-list   :prod_sgmt_tube_od_qty})

(defn get-prod-mechanical
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :well_mech_dtl_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/order-by :prod_sgmt_bmd_qty))
        rows (map #(du/apply-fixums % prodmech-fixums) (du/query-rows dbc qry))]
    (du/vector-slices rows)))

(defn update-prod-mech
  [dsn pwi updates]
  (let [ok-keys #{(keys prodmech-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-prod-mech: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :well_mech_dtl_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/sset (util/rename-map-keys updates prodmech-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------
;; installed mandrels
;;------------------------------------------------------------------------------
(def inst-mnrl-fixums
  {:glv_avt_bchk_tst_status  (du/strcol "avt-backcheck-status-list")
   :glv_avt_trvl_tst_status  (du/strcol "avt-travel-status-list")
   :glv_avt_ldrt_tst_status  (du/strcol "avt-ldrt-status-list")
   :glv_avt_close_tst_status (du/strcol "avt-close-status-list")
   :glv_avt_open_tst_status  (du/strcol "avt-open-status-list")
   :glv_avt_leak_tst_status  (du/strcol "avt-leak-status-list")
   :glv_tst_pvc_qty          (du/dblcol "tst-pvc-list")
   :mnrl_series_nme          (du/strcol "mandrel-series-list")
   :mnrl_mfgr_nme            (du/strcol "mandrel-manf-list")
   :mnrl_desc_txt            (du/strcol "mandrel-desc-list")
   :mnrl_pocket_id_qty       (du/dblcol "mandrel-pocket-id-list")
   :mnrl_od_qty              (du/dblcol "mandrel-od-list")
   :mnrl_id_qty              (du/dblcol "mandrel-id-list")
   :glv_type_cde             (du/strcol "glv-type-list")
   :glv_corr_nme             (du/strcol "glv-correlation-list")
   :glv_sres_nme             (du/strcol "glv-series-list")
   :glv_mfgr_nme             (du/strcol "glv-manf-list")
   :glv_desc_txt             (du/strcol "glv-desc-list")
   :glv_ctgy_cde             (du/strcol "glv-category-list")
   :glv_xtr1_qty             (du/dblcol "xtr1-list")
   :glv_xtr2_qty             (du/dblcol "xtr2-list")
   :glv_xtr3_qty             (du/dblcol "xtr3-list")
   :glv_xtr4_qty             (du/dblcol "xtr4-list")
   :glv_maxdx_qty            (du/dblcol "maxdx-list")
   :glv_maxxt_qty            (du/dblcol "maxxt-list")
   :glv_maxcv_qty            (du/dblcol "maxcv-list")
   :glv_dxa_qty              (du/dblcol "dxa-list")
   :glv_dxb_qty              (du/dblcol "dxb-list")
   :glv_dxc_qty              (du/dblcol "dxc-list")
   :glv_dxd_qty              (du/dblcol "dxd-list")
   :glv_xta_qty              (du/dblcol "xta-list")
   :glv_xtb_qty              (du/dblcol "xtb-list")
   :glv_xtc_qty              (du/dblcol "xtc-list")
   :glv_xtd_qty              (du/dblcol "xtd-list")
   :glv_cva_qty              (du/dblcol "cva-list")
   :glv_cvb_qty              (du/dblcol "cvb-list")
   :glv_cvc_qty              (du/dblcol "cvc-list")
   :glv_cvd_qty              (du/dblcol "cvd-list")
   :glv_slop_qty             (du/dblcol "slope-list")
   :glv_ldvt_qty             (du/dblcol "ldvt-list")
   :glv_fill_qty             (du/dblcol "fill-list")
   :glv_dome_qty             (du/dblcol "dome-list")
   :glv_temp_qty             (du/dblcol "temp-list")
   :glv_tef_qty              (du/dblcol "tef-list")
   :glv_sprg_prsr_qty        (du/dblcol "spring-pres-list")
   :glv_prta_qty             (du/dblcol "prta-list")
   :glv_port_id_qty          (du/dblcol "port-id-list")
   :glv_nmnl_od_qty          (du/dblcol "mandrel-nominal-od-list")
   :glv_usr_inj_rate_qty     (du/dblcol "user-inj-rate-list")
   :mnrl_surv_dte            (du/dtecol "mandrel-survey-date-list")
   :glv_install_dte          (du/dtecol "installed-list")
   :glv_pvcd_qty             (du/dblcol "pvcd-list")
   :glv_pdov_qty             (du/dblcol "pdov-list")
   :glv_bchk_leak_qty        (du/dblcol "bchk-leak-list")
   :glv_port_leak_qty        (du/dblcol "port-leak-list")
   :mnrl_tech_nme            (du/strcol "mandrel-name-list")
   :glv_rfact_qty            (du/dblcol "rfact-list")
   :glv_bela_qty             (du/dblcol "bellowsa-list")
   :glv_trvl_qty             (du/dblcol "travel-list")
   :mnrl_sres_nbr            (du/strcol "mandrel-series-num-list")
   :glv_test_time            (du/dtecol "test-time-list")
   :glv_opcl_dsp_cde         (du/strcol "opcl-dsp-cde-list")
   :glv_prfm_dsp_cde         (du/strcol "prfm-dsp-cde-list")
   :mnrl_md_qty              (du/dblcol "meas-depth-list")
   :glv_chok_size_qty        (du/dblcol "choke-list")
   :glv_tro_qty              (du/dblcol "tro-list")})



(def installed-mandrels-key-redos
  {:avt-backcheck-status-list :glv_avt_bchk_tst_status
   :avt-travel-status-list    :glv_avt_trvl_tst_status
   :avt-ldrt-status-list      :glv_avt_ldrt_tst_status
   :avt-close-status-list     :glv_avt_close_tst_status
   :avt-open-status-list      :glv_avt_open_tst_status
   :avt-leak-status-list      :glv_avt_leak_tst_status
   :tst-pvc-list              :glv_tst_pvc_qty
   :mandrel-series-list       :mnrl_series_nme
   :mandrel-manf-list         :mnrl_mfgr_nme
   :mandrel-pocket-id-list    :mnrl_pocket_id_qty
   :mandrel-od-list           :mnrl_od_qty
   :mnrl_id_qty-list          :mandrel-id
   :glv_type_cde-list         :glv-type
   :glv-correlation-list      :glv_corr_nme
   :glv-series-list           :glv_sres_nme
   :glv-manf-list             :glv_mfgr_nme
   :glv-desc-list             :glv_desc_txt
   :glv-category-list         :glv_ctgy_cde
   :xtr1-list                 :glv_xtr1_qty
   :xtr2-list                 :glv_xtr2_qty
   :xtr3-list                 :glv_xtr3_qty
   :xtr4-list                 :glv_xtr4_qty
   :maxdx-list                :glv_maxdx_qty
   :maxxt-list                :glv_maxxt_qty
   :maxcv-list                :glv_maxcv_qty
   :dxa-list                  :glv_dxa_qty
   :dxb-list                  :glv_dxb_qty
   :dxc-list                  :glv_dxc_qty
   :dxd-list                  :glv_dxd_qty
   :xta-list                  :glv_xta_qty
   :xtb-list                  :glv_xtb_qty
   :xtc-list                  :glv_xtc_qty
   :xtd-list                  :glv_xtd_qty
   :cva-list                  :glv_cva_qty
   :cvb-list                  :glv_cvb_qty
   :cvc-list                  :glv_cvc_qty
   :cvd-list                  :glv_cvd_qty
   :slope-list                :glv_slop_qty
   :ldvt-list                 :glv_ldvt_qty
   :fill-list                 :glv_fill_qty
   :dome-list                 :glv_dome_qty
   :temp-list                 :glv_temp_qty
   :tef-list                  :glv_tef_qty
   :spring_pres-list          :glv_sprg_prsr_qty
   :prta-list                 :glv_prta_qty
   :port-id-list              :glv_port_id_qty
   :mandrel-nominal-od-list   :glv_nmnl_od_qty
   :user-inj-rate-list        :glv_usr_inj_rate_qty
   :mandrel-survey-date-list  :mnrl_surv_dte
   :installed-list            :glv_install_dte
   :pvcd-list                 :glv_pvcd_qty
   :pdov-list                 :glv_pdov_qty
   :bchk-leak-list            :glv_bchk_leak_qty
   :port-leak-list            :glv_port_leak_qty
   :mandrel-name-list         :mnrl_tech_nme
   :rfact-list                :glv_rfact_qty
   :bellowsa-list             :glv_bela_qty
   :travel-list               :glv_trvl_qty
   :mandrel-series-num-list   :mnrl_sres_nbr
   :test-time-list            :glv_test_time
   :opcl-dsp-cde-list         :glv_opcl_dsp_cde
   :prfm-dsp-cde-list         :glv_prfm_dsp_cde
   :meas-depth-list           :mnrl_md_qty
   :choke-list                :glv_chok_size_qty
   :tro-list                  :glv_tro_qty})

(defn get-current-installed-mandrels
  "get installed mandrels history for the specified well"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :mnrl_surv_dte)
            (sqlh/from :mnrl_surv_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/order-by [:mnrl_surv_dte :desc] [:mnrl_md_qty]))
        rows (map #(du/apply-fixums % inst-mnrl-fixums) (du/query-rows dbc qry))]

    (if rows (:mandrel-survey-date (first rows)))))

(defn get-all-installed-mandrels-history
  "get installed mandrels history for the specified well"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :mnrl_surv_dte)
            (sqlh/from :mnrl_surv_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/order-by [:mnrl_surv_dte :desc]))
        rows (map #(du/apply-fixums % inst-mnrl-fixums) (du/query-rows dbc qry))]

    rows))

(defn get-installed-mandrels
  "get the installed mandrels"
  [dbc pwi dte]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :mnrl_nsln_tb)
            (sqlh/where [:and
                         [:= :pwi_nbr pwi]
                         [:= :mnrl_surv_dte (du/jodatime->sqltime dte)]])
            (sqlh/order-by [:mnrl_surv_dte :desc] [:mnrl_md_qty]))
        rows (map #(du/apply-fixums % inst-mnrl-fixums) (du/query-rows dbc qry))]
    (du/vector-slices rows)))


(defn update-installed-mandrels
  [dsn pwi dte updates]
  (let [ok-keys #{(keys installed-mandrels-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-installed-mandrels: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :mnrl_nsln_tb)
            (sqlh/where [:and
                         [:= :pwi_nbr pwi]
                         [:= :mnrl_surv_dte (du/str->sqltime dte)]])
            (sqlh/sset (util/rename-map-keys updates installed-mandrels-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------
;; lift gas properties
;;------------------------------------------------------------------------------
(def lgas-prop-fixums
  {:wlhd_inj_temp_qty     (du/dblcol "wlhd-inj-temp")
   :perf_inj_temp_qty     (du/dblcol "perf-inj-temp")
   :lgas_spg_qty          (du/dblcol "spg")
   :lgas_methane_pct      (du/dblcol "pct-methane")
   :lgas_co2_pct          (du/dblcol "pct-co2")
   :lgas_h2s_pct          (du/dblcol "pct-h2s")})


(def lgas-props-key-redos
  {:wlhd-inj-temp :wlhd_inj_temp_qty
   :perf-inj-temp :perf_inj_temp_qty
   :spg           :lgas_spg_qty
   :pct-methane   :lgas_methane_pct
   :pct-co2       :lgas_co2_pct
   :pct-h2s       :lgas_h2s_pct})

(defn get-lgas-props
  "get lift gas properties"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :lgas_prop_tb)
            (sqlh/where [:= :pwi_nbr pwi]))
        row (first (du/query-rows dbc qry))]
    (if row (du/apply-fixums row lgas-prop-fixums))))


(defn update-lgas-props
  [dsn pwi updates]
  (let [ok-keys #{(keys lgas-props-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-lgas-props: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :lgas_prop_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/sset (util/rename-map-keys updates lgas-props-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------
;; model control
;;------------------------------------------------------------------------------
(def model-control-fixums
  {:fln_modl_cde                (du/wgccol "flowline-model")
   :chok_modl_cde               (du/wgccol "choke-model")
   :wlhd_chok_coef_qty          (du/dblcol "wlhd-choke-coeff")
   :glft_max_md_qty             (du/dblcol "gaslift-max-meas-depth")
   :ruff_mult_qty               (du/dblcol "ruff-multiplier")
   :id_shrk_qty                 (du/dblcol "id-shrink")
   :wlhd_chok_body_size_qty     (du/dblcol "wlhd-choke-body-size")
   :cal_surf_temp_qty           (du/dblcol "calib-surf-temp")
   :fgs_temp_type_cde           (du/strcol "fgs-temp-type")
   :phys_comp_cde               (du/intcol "physical-comp")
   :phys_visc_cde               (du/intcol "physical-visc")
   :phys_crit_cde               (du/intcol "physical-crit")
   :inj_prsr_modl_cde           (du/intcol "injection-press-model")
   :nflo_modl_cde               (du/intcol "inflow-model")
   :well_prsr_modl_cde          (du/intcol "prod-press-model")
   :well_temp_modl_cde          (du/intcol "well-temp-model")
   :fln_prsr_modl_cde           (du/intcol "flowline-press-model")
   :lift_dpth_est_cde           (du/intcol "lift-depth-pt")})

(def model-control-key-redos
  {:flowline-model          :fln_modl_cde
   :choke-model             :chok_modl_cde
   :wlhd-choke-coeff        :wlhd_chok_coef_qty
   :gaslift-max-meas-depth  :glft_max_md_qty
   :ruff-multiplier         :ruff_mult_qty
   :id-shrink               :id_shrk_qty
   :wlhd-choke-body-size    :wlhd_chok_body_size_qty
   :calib-surf-temp         :cal_surf_temp_qty
   :fgs-temp-type           :fgs_temp_type_cde
   :physical-comp           :phys_comp_cde
   :physical-visc           :phys_visc_cde
   :physical-crit           :phys_crit_cde
   :injection-press-model   :inj_prsr_modl_cde
   :well-temp-model         :well_temp_modl_cde
   :flowline-press-model    :fln_prsr_modl_cde
   :lift-depth-pt           :lift_dpth_est_cde})

(defn get-model-control
  "get the model control values"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :modl_ctrl_tb)
            (sqlh/where [:= :pwi_nbr pwi]))
        row (first (du/query-rows dbc qry))]
    (if row
      (merge (du/apply-fixums row model-control-fixums)
             {:find-liquid-rate       false
              :top-to-bot             true
              :flw-use-downstream-dir true}))))

(defn update-model-control
  [dsn pwi updates]
  (let [ok-keys #{(keys model-control-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-model-control: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :modl_ctrl_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/sset (util/rename-map-keys updates model-control-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------
;; stored lift gas response curve
;;------------------------------------------------------------------------------
(def stored-lgr-fixums
  {:glft_fgas_estd_rte   (du/dblcol "calib-fgas-rate-list")
   :glft_oil_estd_rte    (du/dblcol "calib-oil-rate-list")
   :glft_wtr_estd_rte    (du/dblcol "calib-water-rate-list")
   :lgas_nput_qty        (du/dblcol "lift-gas-rate-list")
   :lgas_op_mnrl_ndx     (du/intcol "op-mandrel-ndx-list")
   :lgas_inj_prsr_qty    (du/dblcol "op-inj-press-list")
   :lgas_prod_prsr_qty   (du/dblcol "op-prod-press-list")
   :lgas_upst_prsr_qty   (du/dblcol "op-upst-press-list")
   :lgas_thpt_max_qty    (du/dblcol "op-flow-rate-list")
   :lift_dpth_est_qty    (du/dblcol "lift-meas-depth-list")
   :lgas_stbl_qty        (du/dblcol "stability-num-list")
   :glft_diff_oil_rte    (du/dblcol "diff-oil-rate-list")
   :glft_diff_iguf       (du/dblcol "diff-iguf-list")
   :man_prsr_qty         (du/dblcol "manifold-press-list")
   :lgas_curv_type       (du/intcol "curve-type-list")
   :lgas_curv_nbr        (du/intcol "curve-num-list")})

(defn get-stored-lgr
  "gets the stored lgr curve for the well"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :lgas_prfm_dtl_tb)
            (sqlh/where [:and
                         [:= :pwi_nbr pwi]
                         [:= :lgas_curv_type 5]])
            (sqlh/order-by [:lgas_nput_qty]))
        rows (map #(du/apply-fixums % stored-lgr-fixums) (du/query-rows dbc qry))]
    (if rows (du/vector-slices rows))))

(def lgas-prfm-fixums
  {:glft_dpth_clcn_cde      (du/intcol "lift-depth-pt")
   :lgas_inj_prsr_qty       (du/dblcol "inj-press")
   :lgas_max_qty            (du/dblcol "max-lgas-rate")
   :lgas_mnm_enft_flg       (du/wgccol "enable-min-rate")
   :lgas_mnm_qty            (du/dblcol "min-lgas-rate")
   :dstm_fln_prsr_qty       (du/dblcol "manifold-press")
   :goal_var_pct            (du/dblcol "goal-variation")
   :goal_export_sel_cde     (du/wgccol "goal-export-sel-code")
   :batch_calc_cde          (du/wgccol "batch-calc-code")
   :oil_curv_slope_crtn_qty (du/dblcol "oil-slope-correction")
   :oil_curv_crtn_qty       (du/dblcol "oil-rate-correction")
   :goal_export_fname       (du/strcol "goal-export-fname")
   :goal_export_stamp       (du/dtecol "goal-export-timestamp")
   :lgas_gen_stamp          (du/dtecol "goal-gen-timestamp")
   :lgas_num_curves         (du/dblcol "num-curves")})

(def lgas-perf-settings-key-redos
  {:lift-depth-pt           :glft_dpth_clcn_cde
   :inj-press               :lgas_inj_prsr_qty
   :max-lgas-rate           :lgas_max_qty
   :enable-min-rate         :lgas_mnm_enft_flg
   :min-lgas-rate           :lgas_mnm_qty
   :manifold-press          :dstm_fln_prsr_qty
   :goal-variation          :goal_var_pct
   :goal-export-sel-code    :goal_export_sel_cde
   :oil-slope-correction    :oil_curv_slope_crtn_qty
   :oil-rate-correction     :oil_curv_crtn_qty
   :goal-export-fname       :goal_export_fname
   :goal-export-timestamp   :goal_export_stamp
   :goal-gen-timestamp      :lgas_gen_stamp
   :num-curves              :lgas_num_curves})

(defn get-lgas-perf-settings
  "gets the lgr curve settings for the well (lift gas response control panel)
   the map returned is configured for calibrated glue lgas curves"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :lgas_prfm_tb)
            (sqlh/where [:= :pwi_nbr pwi]))
        row (first (du/query-rows dbc qry))]
    (if row (assoc (du/apply-fixums row lgas-prfm-fixums) :curve-type 1)
            ;; else
            { :lift-depth-pt             1
             :inj-press                 0
             :max-lgas-rate             0
             :enable-min-rate           false
             :min-lgas-rate             0
             :manifold-press            0
             :goal-variation            0
             :goal-export-sel-code      false
             :batch-calc-code           false
             :oil-slope-correction      1
             :oil-rate-correction       0
             :goal-export-fname         ""
             :goal-export-timestamp     0
             :goal-gen-timestamp        0
             :num-curves                0
             :curve-type                1})))


(defn update-lgas-perf-settings
  [dsn pwi updates]
  (let [ok-keys #{(keys lgas-perf-settings-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-lgas-perf-settings: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :lgas_prfm_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/sset (util/rename-map-keys updates lgas-perf-settings-key-redos)))]
      (execute-well-update dsn stmt pwi))))

;;------------------------------------------------------------------------------
;; alternate temperature details
;;------------------------------------------------------------------------------

(def alt-temp-fixums
  {:temp_modl_md_qty   (du/dblcol "temp-modl-md")
   :temp_modl_temp_qty (du/dblcol "temp-modl-temp")})

(def alt-temp-key-redos
  {:temp-modl-md   :temp_modl_md_qty
   :temp-modl-temp :temp_modl_temp_qty})

(defn get-alt-temp-table
  "gets the alternate temp model table as entered by the user"
  [dbc pwi]
  (let [qry
        (-> (sqlh/select :*)
            (sqlh/from :alt_temp_dtl_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/order-by [:temp_modl_md_qty :desc]))
        rows (map #(du/apply-fixums % alt-temp-fixums) (du/query-rows dbc qry))]
    (du/vector-slices rows)))

(defn update-alt-temp-table
  [dsn pwi updates]
  (let [ok-keys #{(keys alt-temp-key-redos)}
        up-keys (set (keys updates))
        bad-keys (cset/difference up-keys ok-keys)]
    (if-not (empty? bad-keys)
      (throw (Exception.
               (format "unsupported/invalid keys update-alt-temp-table: %s"
                       bad-keys))))
    (if (empty? up-keys)
      (throw (Exception.
               (format "At least one of %s must be provided" ok-keys))))
    (let [stmt
          (->
            (sqlh/update :alt_temp_dtl_tb)
            (sqlh/where [:= :pwi_nbr pwi])
            (sqlh/sset (util/rename-map-keys updates alt-temp-key-redos)))]
      (jdbc/execute dsn (sql/format stmt)))))

;;------------------------------------------------------------------------------
;; calibration inputs
;;------------------------------------------------------------------------------
(defn- getdefval
  "Check the var for nil and return the value if not nil, else the default"
  [v default]
  (if (nil? v) default v))

(defn get-calibration-inputs
  "Construct a default record for calibration inputs and results."
  [wtest]
  (if wtest
    (let [wt (mref/realize-refstr wtest)
          calib-in {:max-iterations  {:above 100 :below 100}
                    :grad-pct-limit-map {:above 5 :below 5}
                    :form-gas-oil-ratio-limit-map {:minimum 50.0 :maximum 2000000.0}
                    :lift-gas-rate-limit-map {:minimum 50.0 :maximum (* 8.0 (getdefval (:meas-lift-gas-rate wt) 5000.0))}
                    :enable-multipoint false
                    :traverse-tolerance (if (> (getdefval (:est-fbhp wt) 0) 0) (/ (getdefval (:est-fbhp wt) 0) 50.0) 100.0)}]

      calib-in)))


;;------------------------------------------------------------------------------
;; mime-fetchers for wgdb/well
;;------------------------------------------------------------------------------
(defn- tally-datey-well-things
  "generate the map of things for a well, args requires :dsn, :pwi"
  [dbc colky tblky mimetype args]
  (let [cvtfn (fn [sqlts] (-> sqlts (du/sqltime->jodatime) (du/jodatime->str)))
        qry (-> (sqlh/select colky) (sqlh/from tblky)
                (sqlh/where [:= :pwi_nbr (:pwi args)])
                (sqlh/order-by [colky :desc]))
        times (map #(cvtfn (colky %)) (du/query-rows dbc qry))
        refargs (select-keys args [:dsn :pwi :cmpl])]
    (zipmap
      times
      (map #(mref/->refstr
              (mykw mimetype)
              (assoc refargs :time %)) times))))

(defn- tally-welltests
  [dbc args]
  (tally-datey-well-things dbc :well_ptst_dte :well_ptst_tb
                           "welltest-map" args))

(defn- tally-flowing-gradient-surveys
  [dbc args]
  (tally-datey-well-things dbc :fbhp_surv_dte :fbhp_surv_tb
                           "flowing-gradient-survey-map" args))

(defn- tally-static-surveys
  [dbc args]
  (tally-datey-well-things dbc :bhp_surv_dte :sttc_surv_tb
                           "static-survey" args))

(defn- tally-buildup-surveys
  [dbc args]
  (tally-datey-well-things dbc :bldup_surv_dte :bldup_surv_tb
                           "buildup-survey" args))

(defn- tally-reservoir-surveys
  [dbc args]
  (tally-datey-well-things dbc :rsvr_surv_dte :rsvr_surv_tb
                           "reservoir-survey" args))

(defn- tally-pvt-samples
  [dbc args]
  (tally-datey-well-things dbc :pvt_sample_dte :pvt_properties_tb
                           "pvt-sample-map" args))

(defn- tally-scada-surveys
  [dbc args]
  (tally-datey-well-things dbc :scada_surv_dte :scada_surv_tb
                           "scada-survey" args))

(defn- tally-mandrel-surveys
  [dbc args]
  (tally-datey-well-things dbc :mnrl_surv_dte :mnrl_surv_tb
                           "mandrel-survey-map" args))

(defn- tally-welltracer-surveys
  [dbc args]
  (tally-datey-well-things dbc :trcr_surv_dte :trcr_surv_tb
                           "welltracer-survey" args))

(defn- compose-well-document
  "composes the well document from database.  the document will be
   primarily composed of references"
  [dbc dsn pwi cmpl]
  (let [pwi-args {:dsn dsn :pwi pwi}
        cmpl-args {:dsn dsn :cmpl cmpl}
        ->rs mref/->refstr
        ;; doc is basically a bunch of refs, or tally's of refs for
        ;; historical items.
        doc {:well-mstr-map               (->rs (mykw "well-mstr-map") pwi-args)
             :modl-ctrl-map               (->rs (mykw "modl-ctrl-map") pwi-args)
             :lgas-props-map              (->rs (mykw "lgas-props-map") pwi-args)
             :rsvr-map                    (->rs (mykw "rsvr-map") pwi-args)
             :dsvy-map                    (->rs (mykw "dsvy-map") cmpl-args)
             :flow-line-map               (->rs (mykw "flow-line-map") pwi-args)
             :inj-mech-map                (->rs (mykw "inj-mech-map") pwi-args)
             :prod-mech-map               (->rs (mykw "prod-mech-map") pwi-args)
             :lgas-perf-settings-map      (->rs (mykw "lgas-perf-settings-map") pwi-args)
             :alt-temps-map               (->rs (mykw "alt-temps-map") pwi-args)
             :stored-lgas-response-map    (->rs (mykw "stored-lgas-response-map") pwi-args)
             :welltest-hist-map           (tally-welltests dbc pwi-args)
             :flowing-gradient-survey-hist-map     (tally-flowing-gradient-surveys dbc  {:dsn dsn :pwi pwi :cmpl cmpl})
             :static-survey-hist-map      (tally-static-surveys dbc pwi-args)
             :buildup-survey-hist-map     (tally-buildup-surveys dbc pwi-args)
             :reservoir-survey-hist-map   (tally-reservoir-surveys dbc pwi-args)
             :pvt-sample-hist-map         (tally-pvt-samples dbc pwi-args)
             :scada-survey-hist-map       (tally-scada-surveys dbc pwi-args)
             :mandrel-survey-hist-map     (tally-mandrel-surveys dbc pwi-args)
             :welltracer-survey-hist-map  (tally-welltracer-surveys dbc pwi-args)}
        latest (fn [key] (get (key doc) (last (sort (keys (key doc))))))]
    ;; add in the latest of the historicals in current slots
    (assoc doc
      :welltest-map            (latest :welltest-hist-map)
      :flowing-gradient-survey-map      (latest :flowing-gradient-survey-hist-map)
      :static-survey       (latest :static-survey-hist-map)
      :buildup-survey      (latest :buildup-survey-hist-map)
      :reservoir-survey    (latest :reservoir-survey-hist-map)
      :pvt-sample-map      (latest :pvt-sample-hist-map)
      :scada-survey        (latest :scada-survey-hist-map)
      :mandrel-survey-map  (latest :mandrel-survey-hist-map)
      :welltracer-survey   (latest :welltracer-survey-hist-map)
      :calibration-map     (get-calibration-inputs (latest :welltest-hist-map)))))

(defn get-well
  "gets the well document from either a refstr or a wellident"
  [arg]
  (cond
    ;; creating from a ref string (which must be for a well)
    (mref/refstr? arg)
    (let [objref (mref/str->ref arg)]
      (if (= mykw("well") (:mimetype objref))
        (mref/realize-objref objref)
        nil))

    ;; creating from a well ident, which is a map with expected keys
    (and (map? arg)
         (cset/subset? #{:dsn :field :lease :well :cmpl}
                       (set (keys arg))))
    (with-open [dbc (jdbc/connection (du/dbspec-of (:dsn arg)))]
      (if-let [arg (get-well-db-keys dbc arg)]
        (compose-well-document dbc (:dsn arg) (:pwi arg) (:cmpl arg))
        nil))

    :else nil))

(defn delay-loadify-well
  "transforms a well document such that it has delay loaders in it
   for everything that might be delay loaded.  you touch something, it
   will be loaded first..."
  [w]
  (let [svs [;; references to the well bits
             :well-mstr-map :modl-ctrl-map :lgas-props-map :rsvr-map
             :dsvy-map :flow-line-map :inj-mech-map :prod-mech-map
             :lgas-perf-settings-map :alt-temps-map :stored-lgas-response-map
             ;; latest of historicals
             :welltest-map :flowing-gradient-survey-map :static-survey
             :buildup-survey :reservoir-survey
             :pvt-sample-map :scada-survey
             :mandrel-survey-map :welltracer-survey]

        sms [;; historical collections (maps of refs)
             :welltest-hist-map :flowing-gradient-survey-hist-map
             :static-survey-hist-map :buildup-survey-hist-map
             :reservoir-survey-hist-map :pvt-sample-hist-map
             :scada-survey-hist-map :mandrel-survey-hist-map
             :welltracer-survey-hist-map]
        strs (zipmap svs (map (fn [x] (mref/delay-load-str (x w))) svs))
        maps (zipmap sms (map (fn [x] (mref/delay-load-strmap (x w))) sms))]
    (merge w strs maps)))


(defn with-dsn
  "for automating database via :dsn key in map to supplied function"
  [args f]
  (with-open [dbc (jdbc/connection (du/dbspec-of (:dsn args)))]
    (f dbc args)))

;;------------------------------------------------------------------------------

;; register our mime fetchers
(swap! mref/mime-fetchers assoc
       (mykw "well")
       (fn [args] (with-dsn args
                            #(compose-well-document
                               %1
                               (:dsn args)
                               (:pwi args)
                               (:cmpl args))))

       (mykw "well-mstr-map")
       (fn [args] (with-dsn args #(get-well-master %1 (:pwi %2) (:dsn args))))

       (mykw "modl-ctrl-map")
       (fn [args] (with-dsn args #(get-model-control %1 (:pwi %2))))

       (mykw "dsvy-map")
       (fn [args] (with-dsn args #(get-dsvy %1 (:cmpl %2))))

       (mykw "flow-line-map")
       (fn [args] (with-dsn args #(get-flowline-mech %1 (:pwi %2))))

       (mykw "inj-mech-map")
       (fn [args] (with-dsn args #(get-injection-mech %1 (:pwi %2))))

       (mykw "prod-mech-map")
       (fn [args] (with-dsn args #(get-prod-mechanical %1 (:pwi %2))))

       (mykw "rsvr-map")
       (fn [args] (with-dsn args #(get-reservoir %1 (:pwi %2))))

       (mykw "lgas-props-map")
       (fn [args] (with-dsn args #(get-lgas-props %1 (:pwi %2))))

       (mykw "alt-temps-map")
       (fn [args] (with-dsn args #(get-alt-temp-table %1 (:pwi %2))))

       (mykw "stored-lgas-response-map")
       (fn [args] (with-dsn args #(get-stored-lgr %1 (:pwi %2))))

       (mykw "welltest-map")
       (fn [args] (with-dsn args
                            #(get-welltest
                               %1
                               (:pwi %2)
                               (du/str->sqltime (:time %2)))))

       (mykw "flowing-gradient-survey-map")
       (fn [args] (with-dsn args
                            #(get-flowing-gradient-survey
                               %1
                               (:pwi %2)
                               (du/str->sqltime (:time %2))
                               (:cmpl %2))))

       (mykw "static-survey")
       (fn [args] nil) ;; do me

       (mykw "buildup-survey")
       (fn [args] nil) ;; do me

       (mykw "reservoir-survey")
       (fn [args] (with-dsn args
                            #(get-reservoir-survey
                               %1
                               (:pwi %2)
                               (du/str->sqltime (:time %2)))))

       (mykw "reservoir-survey-hist-map")
       (fn [args] (with-dsn args
                            #(get-reservoir-survey-history
                               %1 (:pwi %2))))

       (mykw "pvt-sample-map")
       (fn [args] (with-dsn args
                            #(get-pvt-sample
                               %1
                               (:pwi %2)
                               (du/str->sqltime (:time %2)))))

       (mykw "scada-survey")
       (fn [args] nil) ;; do me

       (mykw "mandrel-survey-map")
       (fn [args] (with-dsn args
                            #(get-installed-mandrels
                               %1
                               (:pwi %2)
                               (du/str->sqltime (:time %2)))))

       (mykw "welltracer-survey")
       (fn [args] nil) ;; do me

       (mykw "lgas-perf-settings-map")
       (fn [args] (with-dsn args #(get-lgas-perf-settings %1 (:pwi %2)))))

;; register our mime updaters
(swap! mref/mime-updaters assoc
       (mykw "welltest-map")
       (fn [ref args updates]
         (update-welltest (:dsn args) (:pwi args) (:time args) updates))

       (mykw "well-mstr-map")
       (fn [ref args updates]
         (update-well-master (:dsn args) (:pwi args) updates))

       (mykw "modl-ctrl-map")
       (fn [ref args updates]
         (update-model-control (:dsn args) (:pwi args) updates))

       (mykw "rsvr-map")
       (fn [ref args updates]
         (update-reservoir (:dsn args) (:pwi args) updates))

       (mykw "lgas-props-map")
       (fn [ref args updates]
         (update-lgas-props (:dsn args) (:pwi args) updates))

       (mykw "dsvy-map")
       (fn [ref args updates]
         (update-dsvy (:dsn args) (:pwi args) (:cmpl args) updates))

       (mykw "flowline")
       (fn [ref args updates]
         (update-flowline-mech (:dsn args) (:pwi args) updates))

       (mykw "inj-mech-list")
       (fn [ref args updates]
         (update-injection-mech (:dsn args) (:pwi args) updates))

       (mykw "prod-mech-list")
       (fn [ref args updates]
         (update-prod-mech (:dsn args) (:pwi args) updates))

       (mykw "pvt-sample-map")
       (fn [ref args updates]
         (update-pvt-sample (:dsn args) (:pwi args) (:time args) updates))

       (mykw "mandrel-survey-map")
       (fn [ref args updates]
         (update-installed-mandrels (:dsn args) (:pwi args) (:time args) updates))

       (mykw "lgas-perf-settings-map")
       (fn [ref args updates]
         (update-lgas-perf-settings (:dsn args) (:pwi args) updates)))

