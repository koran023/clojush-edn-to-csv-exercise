(ns edn2csv.core
  (require [clojure.core.reducers :as r]
           [clojure.edn :as edn]
           [clojure.java.io :as io]
           [clojure.pprint :as pp]
           [iota]
           [me.raynes.fs :as fs])
  (:gen-class))

;;;; HEADER LINES FOR CSV FILES ;;;;
; for node files
(def individuals-header-line "UUID:ID(Individual),Generation:int,Location:int,:LABEL")
(def semantics-header-line "UUID:ID(Semantics),TotalError:int,:LABEL")
(def errors-header-line "UUID:ID(Error),ErrorValue:int,Position:int,:LABEL")
; for edge files
(def parent-of-edges-header-line ":START_ID(Individual),GeneticOperator,:END_ID(Individual),:TYPE")
(def individual-semantics-header-line ":START_ID(Individual),:END_ID(Semantics),:TYPE")
(def semantics-error-header-line ":START_ID(Semantics),:END_ID(Error),:TYPE") ; in the README, START_ID has no colon : here. I inserted it because all the other header lines have one, and I figured it's a typo.

(defn individual-reader
    [t v]
    (when (= t 'clojush/individual) v))

; nifty function I grabbed off the interwebs for mapping a function across only
; the values of a map. Returns map with old keys, new values.
; http://blog.jayfields.com/2011/08/clojure-apply-function-to-each-value-of.html
(defn map-vals
  [m f & args]
  (reduce (fn [r [k v]] (assoc r k (apply f v args))) {} m))

(defn safe-println [output-stream & more]
  (.write output-stream (str (clojure.string/join "," more) "\n")))

;;;; BEGIN CODE FOR PROCESSING & WRITING INDIVIDUALS ;;;;

; resulting lists of lines to be printed to csv files
(def individuals-result (atom ()))
(def semantics-result (atom ()))
; etc

(defn dump-to-CSVs
  [csv-filenames]
  (with-open [individuals-out (io/writer (get csv-filenames :individuals))]
    (safe-println individuals-out individuals-header-line)
    (r/map (partial safe-println individuals-out) @individuals-result)))

(defn process-individual
  [line]
    (as-> line $
      (map $ [:uuid :generation :location])
      (concat $ ["Individual"])
      (swap! individuals-result conj $)))

(defn build-csv-filenames
  [edn-filename]
  (let [csv-file-postfixes {:individuals "_Individuals.csv", :semantics "_Semantics.csv",
                            :errors "_Errors.csv", :parent-of "_parent-of_edges.csv",
                            :individuals-semantics "_Individual_Semantics_edges.csv",
                            :semantics-error "_Semantics_Error_edges.csv"},
        edn-file-base-name (str (fs/parent edn-filename) "/"
                           (fs/base-name edn-filename ".edn"))]
    (map-vals csv-file-postfixes (partial str edn-file-base-name))))

(defn read-edn
  [edn-filename]
  (->>
    (iota/seq edn-filename)
    (r/map (partial edn/read-string {:default individual-reader}))
    (r/filter identity)))

(defn -main
  [edn-filename]
  (let [edn-seq (read-edn edn-filename), csv-filenames (build-csv-filenames edn-filename)]
    (r/map process-individual edn-seq)
    (dump-to-CSVs csv-filenames)
(shutdown-agents)))
