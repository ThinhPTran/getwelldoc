(ns getwelldoc.database.utils
  (:require [getwelldoc.config :as config]
            [clj-time.coerce :as time-coerce]
            [clj-time.core :as time-core]
            [clj-time.format :as time-format]
            [clojure.string :as cs]
            [honeysql.core :as sql]
            [jdbc.core :as jdbc]))

;; ---------------------------------------------------------------------------
;; this batch of fun has to do with timestamps in database.  This stuff
;; is hard to come up with and get right.
;; - jdbc kicks out java.sql.timestamp classes, and needs these put into
;;   parameters in queries.
;; - we have timestamps in firebird, dates in oracle (go figure)
;; - we need a convert to string fn which has a stable inverse.  strings
;;   are stored in the refs, so we need to get back to what we got.
;; - clj-time (joda) times have good to-str from-str conversions that
;;   don't lose stuff.  Java is a migraine, so we sidestep it.
;; - we want to handle joda times in clojure code, strings outside

(defn sqltime->jodatime
  [sql-time]
  (time-coerce/from-sql-time sql-time))

(defn jodatime->sqltime
  [joda-time]
  (time-coerce/to-sql-time joda-time))

(defn jodatime->str
  [joda-time]
  (time-format/unparse
    (time-format/formatters :basic-date-time-no-ms) joda-time))

(defn str->jodatime
  [str]
  (time-format/parse
    (time-format/formatters :basic-date-time-no-ms) str))

(defn sqltime->str
  [sql-time]
  (-> sql-time (sqltime->jodatime) (jodatime->str)))

(defn str->sqltime
  [str]
  (-> str (str->jodatime) (jodatime->sqltime)))

;; next are little function makers that are used to create
;; functions used in the fixums.  A goofy DSL if you will...
(defn strcol
  "makes a fn that renames a column and strips string result"
  [new-name]
  (fn [k v]
    [(keyword new-name) (if v (cs/trimr v) v)]))

(defn intcol
  "makes a fn that renames a column and converts value to integer"
  [new-name]
  (fn [k v]
    [(keyword new-name) (if v (int v) v)]))

(defn dtecol
  "makes a fn that renames a column and converts to date"
  [new-name]
  (fn [k v] (do
              [(keyword new-name)
               (if v (sqltime->str v) v)])))

(defn dblcol
  "makes a fn that renames a column and converts value to double"
  [new-name]
  (fn [k v]
    [(keyword new-name) (if v (double v) v)]))

(defn fltcol
  "makes a fn that renames a column and converts value to float"
  [new-name]
  (fn [k v]
    [(keyword new-name) (if v (float v) v)]))

(defn wgccol
  "makes a fn that renames a column and translates the
  single char WinGLUE code.
  nil/0/F => False
  1,T     => True"
  [new-name]
  (fn [k v]
    [(keyword new-name) (or (= v 1) (= v "T"))]))

;;------------------------------------------------------------------------
;; fixum dictionaries:
;;  - key is the column name in the winglue database
;;  - value is a fixum closure that does the repair - usually cleaning up
;;    a datatype and renaming the column.
;; These also guide what we pull from a row, if a column is missing, it is
;; not included.
;;----------------------------------------------------------------------------

(defn apply-fixums
  "Applies a fixums dictionary to a row"
  [row fixums]
  (let [rvals (for [[k v]
                    (filter #(contains? fixums (key %)) row)]
                ((get fixums k) k v))
        keys (map #(keyword (first %)) rvals)
        vals (map #(second %) rvals)]
    (zipmap keys vals)))

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

(defn- vector-slice
  "given array of similarly keyed maps, yield a vector of all elements
  in the maps with key 'kw'"
  [kw mapvec]
  (into [] (map #(kw %) mapvec)))

(defn vector-slices
  "given array of similarly keyed maps, yield all the vector slices
  in the map by looking at the first element's keys"
  [v]
  (let [kys (keys (first v))]
    (into {} (map #(identity [% (vector-slice % v)]) kys))))

(defn reform-as-table
  "reform an array of homogenous maps  [ {:col [val ...] }] into table form
   {:cols=[] :vals=[]}.  kwds is vector of keywords from the query
   to be included, rset is the query results"
  [kwds rset]
  (let [ncols  (count kwds)
        nrows  (count ((first kwds) rset))
        cols   (into [] (map (fn [ix] {:name (name (get kwds ix))}))
                     (range ncols))
        parts  (partition ncols
                          (for [ix (range nrows) ky kwds]
                            (get (ky rset) ix)))
        vals   (into [] (map (fn [r] (into [] r)) parts))]
    {:cols cols :vals vals}))

(defn reform-inverted-data
  "Corrects a data inversion problem with valves-models-list"
  [vm]
  (into [] (for [vcnt (range (count (:status-list (first vm))))]
             (let [vmap (into {} (for [k (keys (first vm))]
                                   {k (into [] (for [idx (range (count vm))]
                                                 (nth (k (nth vm idx)) vcnt)))}))]
               vmap))))