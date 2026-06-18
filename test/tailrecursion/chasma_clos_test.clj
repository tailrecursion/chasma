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

(clos/defgeneric map-dominance)

(clos/defmethod map-dominance [(m {:a 1})] :a)
(clos/defmethod map-dominance [(m {:a 1, :b 2})] :ab)

(deftest map-literal-dominance
  (is (= :ab (map-dominance {:a 1, :b 2})))
  (is (= :a (map-dominance {:a 1, :b 3}))))

(clos/defgeneric has-keys-dominance)

(clos/defmethod has-keys-dominance [(m (has-keys :a))] :a)
(clos/defmethod has-keys-dominance [(m (has-keys :a :b))] :ab)

(deftest has-keys-subset-dominance
  (is (= :ab (has-keys-dominance {:a 1, :b 2})))
  (is (= :a (has-keys-dominance {:a 1}))))

(clos/defgeneric map-over-key-and-keys)

(clos/defmethod map-over-key-and-keys [(m (has-keys :a))] :has-a)
(clos/defmethod map-over-key-and-keys [(m (key= :a 1))] :key-a)
(clos/defmethod map-over-key-and-keys [(m {:a 1, :b 2})] :map-ab)

(deftest map-family-dominance
  (is (= :map-ab (map-over-key-and-keys {:a 1, :b 2})))
  (is (= :key-a (map-over-key-and-keys {:a 1, :b 3})))
  (is (= :has-a (map-over-key-and-keys {:a 2, :b 2}))))

(clos/defgeneric keys-exact-dominance)

(clos/defmethod keys-exact-dominance [(m (has-keys :a))] :has-a)
(clos/defmethod keys-exact-dominance [(m (keys= :a :b))] :keys-ab)

(deftest keys=-dominates-has-keys
  (is (= :keys-ab (keys-exact-dominance {:a 1, :b 2})))
  (is (= :has-a (keys-exact-dominance {:a 1, :b 2, :c 3}))))

(clos/defgeneric ambiguous-map-specificity)

(clos/defmethod ambiguous-map-specificity [(m (keys= :a :b))] :keys-ab)
(clos/defmethod ambiguous-map-specificity [(m {:a 1})] :map-a)

(deftest ambiguous-map-cases-use-fallback-order
  (is (= :map-a (ambiguous-map-specificity {:a 1, :b 2})))
  (is (= :keys-ab (ambiguous-map-specificity {:a 2, :b 2}))))

(clos/defgeneric equal-specializers)

(clos/defmethod equal-specializers [(m {:a 1})] :first)
(clos/defmethod equal-specializers [(m {:a 1})] :second)

(deftest equal-specializers-use-definition-order
  (is (= :first (equal-specializers {:a 1}))))

(clos/defgeneric multi-arg-dominance)

(clos/defmethod multi-arg-dominance [(m {:a 1}) (x String)] :first-arg)
(clos/defmethod multi-arg-dominance [(m {:a 1, :b 2}) (x Object)] :dominates-left)
(clos/defmethod multi-arg-dominance [(m {:a 1}) (x Object)] :fallback)

(deftest multi-arg-specificity-is-left-to-right
  (is (= :dominates-left (multi-arg-dominance {:a 1, :b 2} "x")))
  (is (= :first-arg (multi-arg-dominance {:a 1} "x"))))
