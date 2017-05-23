(ns getwelldoc.core
  (:gen-class)
  (:require [getwelldoc.config :as config]
            [getwelldoc.utils :as utils]
            [getwelldoc.dbcore :as dbcore]
            [clojure.pprint :as pp]
            [environ.core :refer [env]]))

(defn tao2-version
  "retrieves the TAO2 version declared in project.clj"
  []
  ;; In debug, obtain with environ
  ;; In release (ubjerjar), obtain from project properties
  (utils/get-version))


(defn -main [& args]
  (println (format "===================================\nTao2 Version : %s\n===================================\n" (tao2-version)))
  (config/load-config!)
  (println "tao2-cfg:")
  (pp/pprint @config/tao2-cfg)
  (println "source revs: ")
  (pp/pprint (dbcore/get-data-source-revs))
  (println "config: ")
  (config/set-db-connections (dbcore/get-data-source-revs))
  (pp/pprint @config/tao2-cfg))
