(ns edn2csv.core
  (require [clojure.core.reducers :as r]
           [clojure.edn :as edn]
           [clojure.java.io :as io]
           [clojure.pprint :as pp]
           [iota]
           [me.raynes.fs :as fs]
           [clj-uuid :as uuid])
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

; lists of lines to be printed to csv files
(def individuals-result (atom ()))
(def semantics-result (atom hash-map))
(def errors-result (atom ()))
(def parent-of-result (atom ()))
(def individual-semantics-result (atom ()))
(def semantics-error-result (atom()))

; hulking function that opens 6 files, prints a header line in each of them, and dumps the result atoms into their respective files
(defn dump-to-CSVs
  [csv-filenames]
  (with-open [individuals-out (io/writer (get csv-filenames :individuals)),
              semantics-out (io/writer (get csv-filenames :semantics)),
              errors-out (io/writer (get csv-filenames :errors)),
              parent-of-out (io/writer (get csv-filenames :parent-of)),
              individuals-semantics-out (io/writer (get csv-filenames :individuals-semantics)),
              semantics-error-out (io/writer (get csv-filenames :semantics-error))]
    ; print all header lines
    (safe-println individuals-out           individuals-header-line)
    (safe-println semantics-out             semantics-header-line)
    (safe-println errors-out                errors-header-line)
    (safe-println parent-of-out             parent-of-edges-header-line)
    (safe-println individuals-semantics-out individual-semantics-header-line)
    (safe-println semantics-error-out       semantics-error-header-line)
    ; dump atoms containing processed lines to their respective CSV files
    (r/map (partial safe-println individuals-out)               @individuals-result)
    (r/map (partial safe-println semantics-out)                 @semantics-result)
    (r/map (partial safe-println errors-out)                    @errors-result)
    (r/map (partial safe-println parent-of-out)                 @parent-of-result)
    (r/map (partial safe-println individuals-semantics-out)     @individual-semantics-result)
    (r/map (partial safe-println semantics-error-out)           @semantics-error-result)))

;;;; LINE PROCESSING FUNCTIONS ;;;;

; construct line for Individuals.csv
(defn make-individuals-line
  [line]
  (swap! individuals-result conj
    (as-> line $
      (doall (map $ [:uuid :generation :location])
      (concat $ ["Individual"])))))
      ;(swap! individuals-result conj $))

; construct line for Semantics.csv
(defn make-semantics-line
  [line]
  (swap! semantics-result assoc (line :errors)
    (as-> () $
      (concat $ uuid/v1)
      (concat $ (line :total-error))
      (concat $ ["Semantics"]))))

; construct line for Errors.csv
(defn make-errors-line
  [line]
  ;(map (fn [error-vector]
  (let [error-vector (line :errors)]
    (map (fn [error-value]
            (swap! errors-result
              (as-> () $
                (concat $ uuid/v1)
                (concat $ error-value)
                (concat $ (.indexOf error-vector error-value))
                (concat ["Error"])))) error-vector)))
                ;@semantics-result)
    
; construct line for ParentOf_edges.csv
(defn make-parent-of-line
  [line]

  (let [parents (get line :parent-uuids)]
    (swap! parent-of-result conj
      (doall (map (fn [parent]
                (as-> () $
                  (concat $ (line :uuid))
                  (concat $ (line :genetic-operators))
                  (concat $ parent)
                  (concat $ ["PARENT_OF"]))) parents)))))
    
; construct line for Individual_Semantics_edges.csv
(defn make-individuals-semantics-line
  [line]

  (swap! individual-semantics-result conj
    (as-> () $
      (concat $ (line :uuid))
      (concat $ (first (@semantics-result (line :errors))))
      (concat $ ["HAS_SEMANTICS"]))))
    
; construct line for Semantics_Error_edges.csv
(defn make-semantics-error-line
  [line]
  (let [error-vector (line :errors)]
    (map (fn [error-value]
            (swap! semantics-error-result conj
              (as-> () $
                (concat $ (first (@semantics-result error-vector)))
                (concat $ (first (@errors-result error-value))) ; this doesn't seem right -- won't there be many items in the errors-result atom that will have the same error-value?
                (concat ["HAS_ERROR"])))) error-vector)))
                ;@semantics-result)
                
;;;; END LINE PROCESSING FUNCTIONS ;;;;

; takes an individual line from the edn-seq and processes for each of the 6 needed files
(defn process-individual
  [line]
  (make-individuals-line line)
  (make-semantics-line line)
  (make-errors-line line)
  (make-parent-of-line line)
  (make-individuals-semantics-line line)
  (make-semantics-error-line line)
)

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
