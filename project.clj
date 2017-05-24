(defproject getwelldoc "0.1.1"
  :main getwelldoc.core
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/clojurescript "1.9.229"]
                 [funcool/clojure.jdbc "0.9.0"]
                 [org.firebirdsql.jdbc/jaybird-jdk18 "2.2.10"]
                 [clj-time "0.12.0"]
                 [honeysql "0.8.0"]
                 [reagent "0.6.1"]
                 [environ "1.1.0"]]
  
  :min-lein-version "2.5.3"

  :source-paths ["src/clj"]

  :plugins [[lein-cljsbuild "1.1.4"]
            [lein-environ "1.1.0"]]
  
  :clean-targets ^{:protect false} ["resources/public/js/compiled"
                                    "target"]

  :figwheel {:css-dirs ["resources/public/css"]}

  :profiles
  {:dev
   {:dependencies []
    :plugins      [[lein-figwheel "0.5.10"]]
    :uberjar {:aot :all}}}
    

  :cljsbuild
  {:builds
   [{:id           "dev"
     :source-paths ["src/cljs"]
     :figwheel     {:on-jsload "getwelldoc.core/reload"}
     :compiler     {:main                 getwelldoc.core
                    :optimizations        :none
                    :output-to            "resources/public/js/compiled/app.js"
                    :output-dir           "resources/public/js/compiled/dev"
                    :asset-path           "js/compiled/dev"
                    :source-map-timestamp true}}

    {:id           "min"
     :source-paths ["src/cljs"]
     :compiler     {:main            getwelldoc.core
                    :optimizations   :advanced
                    :output-to       "resources/public/js/compiled/app.js"
                    :output-dir      "resources/public/js/compiled/min"
                    :elide-asserts   true
                    :closure-defines {goog.DEBUG false}
                    :pretty-print    false}}]})

    
