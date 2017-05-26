(ns getwelldoc.common
  (:require [getwelldoc.config :as config]
            [getwelldoc.utils :as utils]
            [getwelldoc.database.core :as dbcore]
            [getwelldoc.mimerefs :as mref :refer [str->ref realize-refstr]]
            [clojure.pprint :as pp]
            [clojure.data :as da :refer [diff]]))

(def app-state (atom {}))

(add-watch app-state :watcher
           (fn [key atom old-state new-state]
             (prn "-- Atom Changed --")
             (pp/pprint (nth (diff old-state new-state) 1))))

(defn initinfo []
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
  (swap! app-state assoc :dsn (config/get-data-sources @config/tao2-cfg))
  (pp/pprint (:dsn @app-state))
  (println "All wells: ")
  (println "dsn: " (first (keys (:dsn @app-state))))
  (swap! app-state assoc :all-well (->> (dbcore/get-matching-wells (first (keys (:dsn @app-state))) {:select-set #{:field :lease :well :cmpl}
                                                                                                     :where-map {}})
                                        (vec)
                                        (map vec)
                                        (map #(zipmap [:field :lease :well :cmpl] %))))
  (swap! app-state assoc :current-well (first (:all-well @app-state)))
  (pp/pprint (:all-well @app-state))
  (println "Pick a well: ")
  (pp/pprint (:current-well @app-state))
  (swap! app-state assoc :welldoc (dbcore/get-well {:dsn :pioneer
                                                    :field (:field (:current-well @app-state))
                                                    :lease (:lease (:current-well @app-state))
                                                    :well (:well (:current-well @app-state))
                                                    :cmpl (:cmpl (:current-well @app-state))})))

(defn get-well-mstr-map []
  (println "Get :well-mstr-map")
  (swap! app-state assoc-in [:welldoc :well-mstr-map] (realize-refstr (:well-mstr-map (:welldoc @app-state)))))

(defn get-modl-ctrl-map []
  (println "Get :modl-ctrl-map")
  (swap! app-state assoc-in [:welldoc :modl-ctrl-map] (realize-refstr (:modl-ctrl-map (:welldoc @app-state)))))

(defn get-lgas-props-map []
  (println "Get :lgas-props-map")
  (swap! app-state assoc-in [:welldoc :lgas-props-map] (realize-refstr (:lgas-props-map (:welldoc @app-state)))))

(defn get-rsvr-map []
  (println "Get :rsvr-map")
  (swap! app-state assoc-in [:welldoc :rsvr-map] (realize-refstr (:rsvr-map (:welldoc @app-state)))))

(defn get-dsvy-map []
  (println "Get :dsvy-map")
  (swap! app-state assoc-in [:welldoc :dsvy-map] (realize-refstr (:dsvy-map (:welldoc @app-state)))))

(defn get-flow-line-map []
  (println "Get :flow-line-map")
  (swap! app-state assoc-in [:welldoc :flow-line-map] (realize-refstr (:flow-line-map (:welldoc @app-state)))))

(defn get-inj-mech-map []
  (println "Get :inj-mech-map")
  (swap! app-state assoc-in [:welldoc :inj-mech-map] (realize-refstr (:inj-mech-map (:welldoc @app-state)))))

(defn get-prod-mech-map []
  (println "Get :prod-mech-map")
  (swap! app-state assoc-in [:welldoc :prod-mech-map] (realize-refstr (:prod-mech-map (:welldoc @app-state)))))

(defn get-lgas-perf-settings-map []
  (println "Get :lgas-perf-settings-map ")
  (swap! app-state assoc-in [:welldoc :lgas-perf-settings-map ] (realize-refstr (:lgas-perf-settings-map  (:welldoc @app-state)))))

(defn get-alt-temps-map []
  (println "Get :alt-temps-map  ")
  (swap! app-state assoc-in [:welldoc :alt-temps-map  ] (realize-refstr (:alt-temps-map   (:welldoc @app-state)))))

(defn get-stored-lgas-response-map []
  (println "Get :stored-lgas-response-map   ")
  (swap! app-state assoc-in [:welldoc :stored-lgas-response-map] (realize-refstr (:stored-lgas-response-map (:welldoc @app-state)))))

(defn get-welltest-map []
  (println "Get :welltest-map")
  (swap! app-state assoc-in [:welldoc :welltest-map] (realize-refstr (:welltest-map (:welldoc @app-state)))))

(defn get-flowing-gradient-survey-map []
  (println "Get :flowing-gradient-survey-map")
  (swap! app-state assoc-in [:welldoc :flowing-gradient-survey-map] (realize-refstr (:flowing-gradient-survey-map (:welldoc @app-state)))))

(defn get-reservoir-survey []
  (println "Get :reservoir-survey")
  (swap! app-state assoc-in [:welldoc :reservoir-survey] (realize-refstr (:reservoir-survey (:welldoc @app-state)))))

(defn get-scada-survey []
  (println "Get :scada-survey")
  (swap! app-state assoc-in [:welldoc :scada-survey] (realize-refstr (:scada-survey (:welldoc @app-state)))))

(defn get-mandrel-survey-map []
  (println "Get :mandrel-survey-map")
  (swap! app-state assoc-in [:welldoc :mandrel-survey-map] (realize-refstr (:mandrel-survey-map (:welldoc @app-state)))))

(defn get-welltest-hist-map []
  (println "Get :welltest-hist-map ")
  (swap! app-state assoc-in [:welldoc :welltest-hist-map (first (keys (:welltest-hist-map  (:welldoc @app-state))))] (realize-refstr (first (vals (:welltest-hist-map  (:welldoc @app-state)))))))

(defn get-flowing-gradient-survey-hist-map []
  (println "Get :flowing-gradient-survey-hist-map ")
  (swap! app-state assoc-in [:welldoc :flowing-gradient-survey-hist-map (first (keys (:flowing-gradient-survey-hist-map  (:welldoc @app-state))))] (realize-refstr (first (vals (:flowing-gradient-survey-hist-map  (:welldoc @app-state)))))))

(defn get-reservoir-survey-hist-map []
  (println "Get :reservoir-survey-hist-map  ")
  (swap! app-state assoc-in [:welldoc :reservoir-survey-hist-map  (first (keys (:reservoir-survey-hist-map   (:welldoc @app-state))))] (realize-refstr (first (vals (:reservoir-survey-hist-map   (:welldoc @app-state)))))))

(defn get-scada-survey-hist-map []
  (println "Get :scada-survey-hist-map ")
  (swap! app-state assoc-in [:welldoc :scada-survey-hist-map  (first (keys (:scada-survey-hist-map   (:welldoc @app-state))))] (realize-refstr (first (vals (:scada-survey-hist-map   (:welldoc @app-state)))))))

(defn get-mandrel-survey-hist-map []
  (println "Get :mandrel-survey-hist-map ")
  (swap! app-state assoc-in [:welldoc :mandrel-survey-hist-map  (first (keys (:mandrel-survey-hist-map   (:welldoc @app-state))))] (realize-refstr (first (vals (:mandrel-survey-hist-map   (:welldoc @app-state)))))))



