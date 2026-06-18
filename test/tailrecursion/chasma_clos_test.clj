(ns tailrecursion.chasma-clos-test
  (:require [clojure.test :refer [deftest is]]
            [tailrecursion.chasma.clos :as clos]))

(clos/defgeneric dispatch)

(clos/defmethod dispatch [(x Object)] :object)
(clos/defmethod dispatch [(x String)] :string)
(clos/defmethod dispatch [(x :k)] :value)

(deftest basic-dispatch
  (is (= :string (dispatch "a")))
  (is (= :object (dispatch :x)))
  (is (= :value (dispatch :k))))

(clos/defgeneric next-method)

(clos/defmethod next-method [(x Number)] (str "num-" x))
(clos/defmethod next-method [(x Long)] (str "long-" (clos/call-next-method)))

(deftest call-next-method-chain
  (is (= "long-num-1" (next-method 1)))
  (is (= "num-1.0" (next-method 1.0))))

(defstruct person :name :age)
(defstruct account :name :age)

(clos/defgeneric struct-kind)

(clos/defmethod struct-kind [(p person)] :person)
(clos/defmethod struct-kind [(m {:name "Ada"})] :name-map)
(clos/defmethod struct-kind [(m clojure.lang.IPersistentMap)] :map)

(deftest defstruct-specializer-matches-exact-struct
  (is (= :person (struct-kind (struct person "Ada" 37))))
  (is (= :name-map (struct-kind {:name "Ada", :age 37})))
  (is (= :map (struct-kind (struct account "Grace" 37)))))

(clos/defgeneric struct-precedence)

(clos/defmethod struct-precedence [(m {:name "Ada"})] :map-literal)
(clos/defmethod struct-precedence [(p person)] :person)

(deftest defstruct-specializer-beats-map-patterns
  (is (= :person (struct-precedence (struct person "Ada" 37))))
  (is (= :map-literal (struct-precedence {:name "Ada", :age 37}))))
