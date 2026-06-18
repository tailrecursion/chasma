(ns hooks.tailrecursion.chasma.clos
  (:refer-clojure :exclude [defmethod])
  (:require [clj-kondo.hooks-api :as api]))

(defn defgeneric
  [{:keys [node]}]
  (let [[_ name] (:children node)]
    {:node (api/list-node [(api/token-node 'def) name (api/token-node nil)])}))

(defn- param-symbol
  [node]
  (let [form (api/sexpr node)] (if (symbol? form) form (first form))))

(defn- body-uses?
  [sym body]
  (some #{sym} (tree-seq coll? seq (map api/sexpr body))))

(defn defmethod
  [{:keys [node]}]
  (let [[_ _ & body] (:children node)
        body (if (keyword? (api/sexpr (first body))) (rest body) body)
        params (first body)
        method-body (rest body)
        arg-syms (mapv (fn [param]
                         (let [sym (param-symbol param)]
                           (api/token-node
                             (if (body-uses? sym method-body) sym '_))))
                   (:children params))]
    {:node (api/list-node (vec (concat [(api/token-node 'fn)
                                        (api/vector-node arg-syms)]
                                       method-body)))}))
