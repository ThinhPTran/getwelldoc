(ns getwelldoc.utils
  (:require [clojure.java.io :as io]
            [getwelldoc.config :as config])
  (:import java.io.StringWriter         
           java.util.Properties))

;; http://stackoverflow.com/a/33070806
(defn get-version  
  "gets the version of the requested dependency, 
  can be used to get the project   version. 
  Only works in uberjars." 
  []  
  "0.1.0")



