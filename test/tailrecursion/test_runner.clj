(ns tailrecursion.test-runner
  (:require [clojure.test :as t]
            tailrecursion.chasma-clos-test
            tailrecursion.chasma-test))

(defn -main
  [& _]
  (let [{:keys [fail error]} (t/run-tests 'tailrecursion.chasma-test
                                          'tailrecursion.chasma-clos-test)]
    (shutdown-agents)
    (when (pos? (+ fail error))
      (System/exit 1))))
