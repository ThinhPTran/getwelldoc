(ns getwelldoc.db
  (:require [reagent.core :as r]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Vars

(defonce app-state
         (r/atom {:input-text ""}))