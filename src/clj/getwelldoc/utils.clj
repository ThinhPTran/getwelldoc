(ns getwelldoc.utils
  (:require [clojure.string :as cs]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [environ.core :refer [env]])
  (:import java.io.StringWriter         
           java.util.Properties))

;; http://stackoverflow.com/a/33070806
(defn get-version
  "gets the version of the requested dependency, can be used to get the project
   version. Only works in uberjars."
  [project]
  (let [path (str "META-INF/maven/" project "/pom.properties")
        props (io/resource path)]
    (when props
      (with-open [stream (io/input-stream props)]
        (let [props (doto (Properties.) (.load stream))]
          (.getProperty props "version"))))))

(defn tao2-version
  "retrieves the TAO2 version declared in project.clj"
  []
  ;; In debug, obtain with environ
  ;; In release (ubjerjar), obtain from project properties
  (some #(if (some? %) %)
        [(:getwelldoc-version env)
         (get-version "com.AppSmiths/winglue-well")]))

(declare getList cvtVal getStruct)

(defn to-string [v]
  (let [sw (StringWriter.)] (pp/pprint v sw)(.toString sw)))

(defn cvt-if-bool [v] (if (instance? Boolean v) (if v 1 0) v))
(defn cvt-types [[k v]] [k (cvt-if-bool v)])

(defn rename-map-keys
  "renames keys in map [m] using map [xlt] as a from->to specification
   (rename-keys {:a 42 :b 69 :z 19} {:a :fizzle :b :whizzle :c :slap})
     => {:fizzle 42 :whizzle 69 :z 19}
   keys in [m] & not in [xlt] are unaffected
   keys in [xlt] & not in [m] are ignored"
  [m xlt]
  (let [mapfn (fn [[k v]] [(if (contains? xlt k) (k xlt) k) v])]
    (into {} (map #(cvt-types %) (map #(mapfn %) m)))))

(defn str-invoke
  "from:
  https://en.wikibooks.org/wiki/Clojure_Programming/Examples#Invoking_Java_method_through_method_name_as_a_String"
  [instance method-str & args]
  (clojure.lang.Reflector/invokeInstanceMethod
    instance method-str (to-array args)))

(defn str-invoke-static
  ""
  [class-name method-str & args]
  (clojure.lang.Reflector/invokeStaticMethod
    class-name method-str (to-array args)))

(defn- protoName
  "get the protobuf name of a clojure key"
  [k]
  (clojure.string/join ""
                       (for [sp (cs/split (cs/replace (subs (str k) 1) #"([^a-zA-Z]+)" " $1 ")
                                          #" +")]
                         (let [sp2 (cs/replace sp #"-" "")]
                           (if (< 0 (count sp2)) (cs/capitalize sp2))))))

(defn- getRawVal
  "get the raw protobuf value"
  [var-type pbuf pname]
  (case var-type
    :list (str-invoke pbuf (format "get%sList" pname))
    :enum (let [enum-val (str-invoke pbuf (format "get%s" pname))]
            (str-invoke enum-val "getNumber"))
    (str-invoke pbuf (format "get%s" pname))))

(defn- getList
  "populate a vector from a protobuf list"
  [schema rawval pname]
  (into [] (let [list-schema (:values schema)
                 var-type (:type list-schema)]
             (for [v rawval]
               (case var-type
                 :enum (cvtVal list-schema (str-invoke v "getNumber") pname)
                 (cvtVal list-schema v pname))))))

(defn- cvtVal
  "convert a protobuf value to clojure given the raw value and type"
  [schema rawval pname]
  (let [val
        (case (:type schema)
          :struct  (getStruct schema rawval)
          :list    (getList schema rawval pname)
          :float   (float   rawval)
          :enum    (int     rawval)
          :int     (int     rawval)
          :boolean (boolean rawval)
          :long    (bigint  rawval)
          :string  (str     rawval)
          :double  (double  rawval))]
    val))

(defn- getStruct
  "populate a map from a protobuf struct"
  [var-schema pbuf]
  (into {}
        (let [fields (:fields var-schema)]
          (for [[kw schema] fields]
            (let [var-type (:type schema)
                  pname    (protoName kw)
                  raw-val  (getRawVal var-type pbuf pname)]
              [kw (cvtVal schema raw-val pname)])))))

(defn getdefval
  "Check the var for nil and return the value if not nil, else the default"
  [v default]
  (if (nil? v) default v))


(defn augment-welltest
  "add calculated quatities to welltest"
  [wt]
  (let [calib-water (getdefval (:calib-water-rate wt) 0.0)
        calib-oil   (getdefval (:calib-oil-rate wt) 0.0)
        meas-water  (getdefval (:meas-water-rate wt) 0.0)
        meas-oil    (getdefval (:meas-oil-rate wt) 0.0)
        calib-fgas  (getdefval (:calib-formation-gas-rate wt) 0.0)
        calib-lgas  (getdefval (:calib-lift-gas-rate wt) 0.0)
        est-fbhp    (getdefval (:est-fbhp wt) 0.0)
        calib-liq   (+ calib-water calib-oil)
        calib-form-gor (if (zero? calib-oil) 1000000
                                             (/ (* 1000.0 calib-fgas) calib-oil))
        calib-form-glr (if (zero? calib-liq) 0.0
                                             (/ (* 1000.0 calib-fgas) calib-liq))
        calib-wc    (if (zero? calib-liq) 0.0
                                          (* 100.0 (/ calib-water calib-liq)))
        calib-total-gas (+ calib-fgas calib-lgas)
        calib-total-glr (if (zero? calib-liq) 0.0
                                              (/ (* 1000.0 calib-total-gas) calib-liq))]

    (merge wt
           {:calib-water-rate calib-water
            :calib-oil-rate calib-oil
            :meas-water-rate meas-water
            :meas-oil-rate meas-oil
            :meas-liquid-rate  (+ meas-water meas-oil)
            :calib-liquid-rate calib-liq
            :calib-form-gor calib-form-gor
            :calib-form-glr calib-form-glr
            :calib-wc calib-wc
            :calib-total-gas calib-total-gas
            :est-fbhp est-fbhp
            :calib-total-glr calib-total-glr})))

;; from https://github.com/incanter ...
(defn calc-line
  "Finds value in point x.
   Given:
     f(xl) = yl
     f(xr) = yr
     xl <= x <= xr"
  [xl yl xr yr x]
  (let [xl (double xl)
        yl (double yl)
        xr (double xr)
        yr (double yr)
        x (double x)
        coef (/ (- x xl) (- xr xl))]
    (+ (* (- 1.0 coef) yl)
       (* coef yr))))

