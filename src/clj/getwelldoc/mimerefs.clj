(ns getwelldoc.mimerefs
  (:gen-class)
  (:require [clojure.edn :as edn]))

;; record type of an objref
(defrecord objref
  [^clojure.lang.Keyword mimetype
   ^clojure.lang.PersistentArrayMap args])

;; here is where we house then mime-type fetchers for objrefs
(defonce mime-fetchers (atom {}))
(defonce mime-updaters (atom {}))

(defn- loader-for
  "return the loader for the given mime ref, or die trying"
  [ref]
  (let [mtype (:mimetype ref)
        lmap @mime-fetchers
        loader (mtype lmap)]
    (if-not loader (throw (Exception.
                            (format "No registered loader for type: %s"
                                    (name mtype)))))
    loader))

(defn- updater-for
  "return the updater for the given mime type"
  [mtype]
  (mtype @mime-updaters))

(defn ->refstr
  "creates a recognizable string representation of objref arguments,
   bypassing actual ref creation"
  [mtype args]
  (format "#^ref:%s" (pr-str [mtype args])))

(defn refstr?
  "test if x appears to be an objref string"
  [x]
  (and (string? x) (.startsWith x "#^ref:")))

(defn str->ref
  "creates an object ref from a string or nil if failure"
  [x]
  (try
    (let [args (edn/read-string (.substring x 6))]
      (apply ->objref args))
    (catch Exception e nil)))

(defn realize-objref
  "returns the realized version of the objref"
  [objref]
  ((loader-for objref) (:args objref)))

(defn realize-refstr
  "returns the realized version from a ref string"
  [refstr]
  (realize-objref (str->ref refstr)))

(defn realize-refstrs
  "returns realized versions for sequence of ref-strings"
  [refstrs]
  (loop [orefs refstrs acc '[]]
    (if-not (seq orefs)
      acc
      (recur (rest orefs)
             (conj acc
                   (let [oref (first orefs)]
                     (if (refstr? oref)
                       (realize-refstr oref))))))))

(defn update-refstr
  [refstr updates]
  (if-let [objref (str->ref refstr)]
    (if-let [mimetype (:mimetype objref)]
      (if-let [update-fn (updater-for mimetype)]
        (update-fn objref (:args objref) updates)
        (throw (Exception. (format "no updater for type: %s" mimetype))))
      (throw (Exception. (format "invalid refstr: %s" refstr))))))

(defn delay-load
  "returns a delay that will realize the objectref when dereferenced
   effectively making them lazy and cached"
  [ref]
  (delay (realize-objref ref)))

(defn delay-load-str
  "if obj is a ref str, return a delay-loader for said obj, else obj"
  [obj]
  (if (refstr? obj)
    (delay-load (str->ref obj))
    (delay obj)))

(defn delay-load-strmap
  "transforms values of m to delay loaders if they are refstrs"
  [strmap]
  (zipmap
    (keys strmap)
    (map #(delay-load-str %) (vals strmap))))