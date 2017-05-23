(ns getwelldoc.dbcore
  (:require [getwelldoc.config :as config]
            [getwelldoc.dbutils :as du]
            [getwelldoc.mimerefs :as mref]
            [clojure.set :as cset]
            [jdbc.core :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :as sqlh]))

;;;------------------------------------------------------------------------------
;;; mime-fetchers for wgdb/well
;;;------------------------------------------------------------------------------
(defn- mykw
  "creates a keyword in my namespace"
  [nm]
  (keyword (format "com.appsmiths.wgdb.%s" nm)))

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

;;------------------------------------------------------------------------------

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


;;------------------------------------------------------------------------------