(ns getwelldoc.core
  (:gen-class)
  (:require [getwelldoc.config :as config]
            [getwelldoc.utils :as utils]
            [getwelldoc.dbcore :as dbcore]
            [clojure.pprint :as pp]
            [environ.core :refer [env]]))


(defn -main [& args]
  (println (format "===================================\nTao2 Version : %s\n===================================\n" (utils/tao2-version)))
  (config/load-config!)
  (println "tao2-cfg:")
  (pp/pprint @config/tao2-cfg)
  (println "source revs: ")
  (pp/pprint (dbcore/get-data-source-revs))
  (println "config: ")
  (config/set-db-connections (dbcore/get-data-source-revs))
  (pp/pprint @config/tao2-cfg)
  (println "datasources: ")
  (pp/pprint (config/get-data-sources @config/tao2-cfg))
  (println "All wells: ")
  (pp/pprint (dbcore/get-matching-wells :pioneer {:select-set #{:field :lease :well :cmpl}
                                                  :where-map {}}))
  (println "Pick a well: "))

