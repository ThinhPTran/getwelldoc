(ns getwelldoc.config
  (:require [clojure.java.io :as io])
  (:import   [java.io PushbackReader])
  (:import   [java.io File])
  (:import   [java.io FileNotFoundException]))

(def cfg-base "tao2-config.clj")

(def default-config
  {
   ;; define the data sources
   ;; http://firebirdsql.org/file/Jaybird_2_1_JDBC_driver_manual.pdf firebird
   :data-sources
   {:pioneer
    {:description "Tao2 Firebird Database"
     :classname   "org.firebirdsql.jdbc.FBDriver"
     :subprotocol "firebirdsql"
     :subname     "//localhost:3050//home/setup/databases/glue.fdb"
     :user "glueuser"
     :password "glue"}}})

(def tao2-cfg (atom default-config))

;; util functions

(defn- root-dir
  "return the name of the root directory"
  []
  (.getAbsolutePath (first (File/listRoots))))

(defn- path-join
  "join pathname parts"
  [arg & args]
  (.getAbsolutePath (reduce (fn [f g] (File. f g)) arg args)))

(defn- via-file
  "tries to get config from a file"
  [path]
  (try
    (with-open [r (io/reader path)]
      (read (PushbackReader. r)))
    (catch FileNotFoundException _ nil)))

(defn- via-env
  "sees if file can be found via TAO2_CONFIG"
  []
  (if-let [pn (System/getenv "TAO2_CONFIG")]
    (via-file pn)))

(defn- via-etc
  "sees if we have file in /etc"
  []
  (via-file (path-join (root-dir) "etc" cfg-base)))

(defn- via-cwd
  "sees if we have file in local directory"
  []
  (via-file (path-join (System/getProperty "user.dir") cfg-base)))

(defn- via-jar
  "trys for config from JAR resources"
  []
  (io/resource cfg-base))

(defn set-db-connections
  [data-sources]
  (let [cfg (merge @tao2-cfg {:data-sources data-sources})]
    (reset! tao2-cfg cfg)))

(defn set-config!
  "Validates and sets the tao2-cfg atom with the provided config"
  [config]
  (reset! tao2-cfg config))

(defn load-config!
  "Attemps to load the TAO2 configuration from various places.
  Loads default configuration if no configuration file is found"
  []
  (binding [*read-eval* false] ;; nothing executable please
    (if-let [config (or (via-env) (via-etc) (via-cwd) (via-jar))]
      (set-config! config)
      (do
        (println "No configuration file found. Using default configuration")
        (set-config! default-config)))))

(defn get-data-sources
  "returns a map containing the data sources"
  [config]
  (let [dsmap (:data-sources config)
        dskeys (keys dsmap)]
    (zipmap dskeys (map #(:description (% dsmap)) dskeys))))




