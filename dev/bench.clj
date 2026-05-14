(ns bench
  "Ducktape insert/query benchmarks.

  Usage from REPL:
    (require '[bench :as b])
    (b/run-all)            ; full suite
    (b/bench-insert)       ; insert only
    (b/bench-query)        ; query only
    (b/bench-roundtrip)    ; insert + query"
  (:require [ducktape.core :as duck]
            [ducktape.ffi :as ffi]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dt]
            [tech.v3.datatype.datetime :as dtype-dt])
  (:import [java.time LocalDate]
           [java.util UUID]))

;; ---------------------------------------------------------------------------
;; Test datasets
;; ---------------------------------------------------------------------------

(def ^:const default-n 100000)

(defn make-numeric-ds
  "Pure numeric dataset — isolates bulk memcpy perf."
  ([] (make-numeric-ds default-n))
  ([^long n]
   (-> (ds/->dataset {:longs   (long-array (range n))
                      :doubles (double-array (map #(* % 1.23456789) (range n)))
                      :ints    (int-array (range n))
                      :floats  (float-array (map float (range n)))})
       (vary-meta assoc :name "bench_numeric"))))

(defn make-string-ds
  "String-heavy dataset — isolates string encode/decode perf."
  ([] (make-string-ds default-n))
  ([^long n]
   (-> (ds/->dataset {:short_str (mapv #(str "s" (rem % 9999)) (range n))
                      :long_str  (mapv #(str "a-longer-string-value-" (rem % 99999)) (range n))
                      :id        (long-array (range n))})
       (vary-meta assoc :name "bench_string"))))

(defn make-mixed-ds
  "Mixed types — realistic workload."
  ([] (make-mixed-ds default-n))
  ([^long n]
   (let [base-date (LocalDate/of 2000 1 1)]
     (-> (ds/->dataset {:longs   (long-array (range n))
                        :doubles (double-array (map #(* % 1.23456789) (range n)))
                        :strings (mapv #(str "str" (rem % 9999)) (range n))
                        :dates   (mapv #(.plusDays base-date (long (rem % 3650))) (range n))})
         (vary-meta assoc :name "bench_mixed")))))

(defn make-uuid-ds
  "UUID dataset — isolates UUID encode/decode."
  ([] (make-uuid-ds default-n))
  ([^long n]
   (-> (ds/->dataset {:id   (long-array (range n))
                      :uuid (repeatedly n #(UUID/randomUUID))})
       (vary-meta assoc :name "bench_uuid"))))

;; ---------------------------------------------------------------------------
;; Benchmark harness
;; ---------------------------------------------------------------------------

(defn- warmup-gc! []
  (System/gc)
  (Thread/sleep 100))

(defn- time-ms [f]
  (let [t0 (System/nanoTime)
        result (f)
        elapsed (/ (- (System/nanoTime) t0) 1e6)]
    {:ms elapsed :result result}))

(defn- bench-fn
  "Run `f` n-warmup + n-timed times, return stats."
  [f & {:keys [warmup timed] :or {warmup 3 timed 5}}]
  (dotimes [_ warmup] (f))
  (warmup-gc!)
  (let [times (mapv (fn [_] (:ms (time-ms f))) (range timed))
        sorted (sort times)]
    {:mean   (/ (reduce + times) (count times))
     :median (nth sorted (quot (count sorted) 2))
     :min    (first sorted)
     :max    (last sorted)
     :times  times}))

(defn- fmt [ms] (format "%.2f" (double ms)))

(defn- print-result [label {:keys [mean median min max]}]
  (println (format "  %-20s  mean=%s  median=%s  min=%s  max=%s ms"
                   label (fmt mean) (fmt median) (fmt min) (fmt max))))

;; ---------------------------------------------------------------------------
;; Benchmark functions
;; ---------------------------------------------------------------------------

(defonce db*   (delay (duck/initialize!) (duck/open-db)))
(defonce conn* (delay (duck/connect @db*)))

(defn- ensure-conn [] @conn*)

(defn- drop-quietly [table-name]
  (try (duck/run-query! (ensure-conn) (str "DROP TABLE IF EXISTS " table-name))
       (catch Exception _ nil)))

(defn bench-insert
  "Benchmark insert for a given dataset."
  [& {:keys [ds-fn label n warmup timed]
      :or   {ds-fn make-mixed-ds label "mixed" n default-n warmup 3 timed 5}}]
  (let [conn (ensure-conn)
        ds   (ds-fn n)]
    (drop-quietly (ds/dataset-name ds))
    (duck/create-table! conn ds)
    (println (str "\n▸ INSERT " label " (" n " rows, " (ds/column-count ds) " cols)"))
    (let [result (bench-fn #(do (duck/run-query! conn (str "DELETE FROM " (ds/dataset-name ds)))
                                (duck/insert-dataset! conn ds))
                           :warmup warmup :timed timed)]
      (print-result "insert" result)
      (println (format "  %-20s  %,.0f rows/sec" "" (/ n (/ (:median result) 1000.0))))
      result)))

(defn bench-query
  "Benchmark query for a table that's already loaded."
  [& {:keys [ds-fn label n warmup timed]
      :or   {ds-fn make-mixed-ds label "mixed" n default-n warmup 3 timed 5}}]
  (let [conn (ensure-conn)
        ds   (ds-fn n)
        tbl  (ds/dataset-name ds)]
    ;; Ensure table exists and is populated
    (drop-quietly tbl)
    (duck/create-table! conn ds)
    (duck/insert-dataset! conn ds)
    (println (str "\n▸ QUERY " label " (" n " rows, " (ds/column-count ds) " cols)"))
    (let [result (bench-fn #(duck/sql->dataset conn (str "SELECT * FROM " tbl))
                           :warmup warmup :timed timed)]
      (print-result "query" result)
      (println (format "  %-20s  %,.0f rows/sec" "" (/ n (/ (:median result) 1000.0))))
      result)))

(defn bench-roundtrip
  "Insert + query timing."
  [& {:keys [ds-fn label n warmup timed]
      :or   {ds-fn make-mixed-ds label "mixed" n default-n warmup 3 timed 5}}]
  (let [conn (ensure-conn)
        ds   (ds-fn n)
        tbl  (ds/dataset-name ds)]
    (drop-quietly tbl)
    (duck/create-table! conn ds)
    (println (str "\n▸ ROUNDTRIP " label " (" n " rows, " (ds/column-count ds) " cols)"))
    (let [result (bench-fn #(do (duck/run-query! conn (str "DELETE FROM " tbl))
                                (duck/insert-dataset! conn ds)
                                (duck/sql->dataset conn (str "SELECT * FROM " tbl)))
                           :warmup warmup :timed timed)]
      (print-result "roundtrip" result)
      result)))

(defn run-all
  "Run the full benchmark suite."
  [& {:keys [n warmup timed] :or {n default-n warmup 3 timed 5}}]
  (ensure-conn) ;; force init
  (println (str "═══════════════════════════════════════════════════════"))
  (println (str " ducktape benchmark  (" n " rows, " warmup " warmup, " timed " timed)"))
  (println (str " DuckDB " (ffi/read-c-str (ffi/duckdb_library_version))
                "  JDK " (System/getProperty "java.version")))
  (println (str "═══════════════════════════════════════════════════════"))
  (let [opts {:n n :warmup warmup :timed timed}]
    (doseq [[ds-fn label] [[make-numeric-ds "numeric"]
                           [make-string-ds  "string"]
                           [make-uuid-ds    "uuid"]
                           [make-mixed-ds   "mixed"]]]
      (bench-insert  (assoc opts :ds-fn ds-fn :label label))
      (bench-query   (assoc opts :ds-fn ds-fn :label label)))
    (println "\n═══════════════════════════════════════════════════════")
    (println "Done.")))
