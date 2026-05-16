(ns tmducken-comparison
  "Head-to-head benchmark: ducktape vs tmducken in the same JVM.

  Usage from REPL:
    (require '[tmducken-comparison :as cmp] :reload)
    (cmp/compare-all)            ; full suite
    (cmp/compare-uuid)           ; just UUID
    (cmp/compare-string)         ; just string
    (cmp/compare-numeric)        ; just numeric
    (cmp/compare-mixed)          ; just mixed
    (cmp/compare-wide-numeric)   ; 8-column all-numeric (exercises pmap)
    (cmp/compare-wide-mixed)     ; 8 numeric + 2 string

  IMPORTANT: only call ONE compare-* at a time. Running concurrently makes
  results unreliable since both libraries compete for CPU/memory bandwidth.

  Benchmark methodology (v2 — reliable statistics):
  * Time-based JIT warmup (default 2s per fn).
  * `System/gc` + small pause before every sample to clear allocation noise.
  * Per-sample alternation between the two libraries to remove position bias
    (even iterations time ducktape first, odd time tmducken first).
  * Default 50 samples per phase.
  * Reports mean ± 95% CI, plus p10/p50/p90 and RSD%.
  * Significance flag based on 95%-CI overlap of the means."
  (:require [bench-util :as bu]
            [ducktape.core :as duck]
            [ducktape.ffi :as ffi]
            [tmducken.duckdb :as tddb]
            [tech.v3.dataset :as ds])
  (:import [java.time LocalDate]
           [java.util UUID]))

(def ^:const N 1000000)

;; ---------------------------------------------------------------------------
;; Dataset constructors
;; ---------------------------------------------------------------------------

(defn make-numeric [n]
  (-> (ds/->dataset {:longs (long-array (range n))
                     :doubles (double-array (map #(* % 1.23456789) (range n)))
                     :ints (int-array (range n))
                     :floats (float-array (map float (range n)))})
      (vary-meta assoc :name "bench_numeric")))

(defn make-string [n]
  (-> (ds/->dataset {:short_str (mapv #(str "s" (rem % 9999)) (range n))
                     :long_str (mapv #(str "a-longer-string-value-" (rem % 99999)) (range n))
                     :id (long-array (range n))})
      (vary-meta assoc :name "bench_string")))

(defn make-uuid [n]
  (-> (ds/->dataset {:id (long-array (range n))
                     :uuid (repeatedly n #(UUID/randomUUID))})
      (vary-meta assoc :name "bench_uuid")))

(defn make-mixed [n]
  (let [base-date (LocalDate/of 2000 1 1)]
    (-> (ds/->dataset {:longs (long-array (range n))
                       :doubles (double-array (map #(* % 1.23456789) (range n)))
                       :strings (mapv #(str "str" (rem % 9999)) (range n))
                       :dates (mapv #(.plusDays base-date (long (rem % 3650))) (range n))})
        (vary-meta assoc :name "bench_mixed"))))

(defn make-wide-numeric
  "8-column all-numeric/temporal table.  Exercises the partitioned fast-path's
  parallel pmap across enough columns to fully utilise typical core counts."
  [n]
  (let [base-date (LocalDate/of 2000 1 1)]
    (-> (ds/->dataset {:l1 (long-array (range n))
                       :l2 (long-array (map #(* 2 %) (range n)))
                       :d1 (double-array (map #(* % 1.234) (range n)))
                       :d2 (double-array (map #(* % 5.678) (range n)))
                       :i1 (int-array (range n))
                       :i2 (int-array (map #(* 3 %) (range n)))
                       :dt1 (mapv #(.plusDays base-date (long (rem % 3650))) (range n))
                       :dt2 (mapv #(.plusDays base-date (long (rem % 1825))) (range n))})
        (vary-meta assoc :name "bench_wide_numeric"))))

(defn make-wide-mixed
  "10-column table: 8 numeric/temporal + 2 string.  Tests the partition+stitch
  path on a realistic OLAP-style fact table shape."
  [n]
  (let [base-date (LocalDate/of 2000 1 1)]
    (-> (ds/->dataset {:l1 (long-array (range n))
                       :l2 (long-array (map #(* 2 %) (range n)))
                       :d1 (double-array (map #(* % 1.234) (range n)))
                       :d2 (double-array (map #(* % 5.678) (range n)))
                       :i1 (int-array (range n))
                       :i2 (int-array (map #(* 3 %) (range n)))
                       :dt1 (mapv #(.plusDays base-date (long (rem % 3650))) (range n))
                       :dt2 (mapv #(.plusDays base-date (long (rem % 1825))) (range n))
                       :s1 (mapv #(str "x" (rem % 9999)) (range n))
                       :s2 (mapv #(str "y" (rem % 9999)) (range n))})
        (vary-meta assoc :name "bench_wide_mixed"))))

;; ---------------------------------------------------------------------------
;; Init — connections lazily started on first use, cached for reuse
;; ---------------------------------------------------------------------------

(defonce duck-state
  (delay
    (duck/initialize!)
    (let [db (duck/open-db)
          conn (duck/connect db)]
      {:db db :conn conn})))

(defonce tmd-state
  (delay
    (tddb/initialize!)
    (let [db (tddb/open-db)
          conn (tddb/connect db)]
      {:db db :conn conn})))

(defn duck-conn [] (:conn @duck-state))
(defn tmd-conn  [] (:conn @tmd-state))

;; ---------------------------------------------------------------------------
;; Workload runner
;;
;; Statistics, GC pause, warmup, interleaved sampling, and the format-stats
;; helper all live in `bench-util` (shared with appender-comparison).
;; ---------------------------------------------------------------------------

(defn- compare-workload
  "Run a head-to-head benchmark with full statistical reporting.

  Options:
  * :n          row count (default N)
  * :warmup-ms  JIT warmup duration per fn (default 2000)
  * :n-samples  sample count per phase per library (default 50)

  Returns a result map. Positive Δ% means ducktape is faster."
  ([label ds-fn] (compare-workload label ds-fn {}))
  ([label ds-fn opts]
   (let [{:keys [n warmup-ms n-samples]
          :as opts} (merge {:n N :warmup-ms 2000 :n-samples 50}
                           opts)
         dataset (ds-fn n)
         tbl (ds/dataset-name dataset)
         duck-c (duck-conn)
         tmd-c  (tmd-conn)
         _ (do (try (duck/run-query! duck-c (str "DROP TABLE IF EXISTS " tbl)) (catch Exception _))
               (duck/create-table! duck-c dataset)
               (try (tddb/run-query! tmd-c (str "DROP TABLE IF EXISTS " tbl)) (catch Exception _))
               (tddb/create-table! tmd-c dataset))
         ;; Bind the timed fns. INSERT clears the table then inserts; QUERY just
         ;; reads. Both libraries pay the same DELETE cost so it cancels in the
         ;; comparison.
         duck-i (fn []
                  (duck/run-query! duck-c (str "DELETE FROM " tbl))
                  (duck/insert-dataset! duck-c dataset))
         tmd-i  (fn []
                  (tddb/run-query! tmd-c (str "DELETE FROM " tbl))
                  (tddb/insert-dataset! tmd-c dataset))
         duck-q (fn [] (duck/sql->dataset duck-c (str "SELECT * FROM " tbl)))
         tmd-q  (fn [] (tddb/sql->dataset tmd-c (str "SELECT * FROM " tbl)))
         _ (println (format "\n▸ %s (%,d rows, %d samples, %dms warmup)"
                            label n n-samples warmup-ms))

         ;; Warmup INSERT, then sample INSERT
         _ (bu/warmup! warmup-ms duck-i)
         _ (bu/warmup! warmup-ms tmd-i)
         [duck-i-samples tmd-i-samples] (bu/bench-pair-interleaved duck-i tmd-i opts)
         di (bu/stats duck-i-samples)
         ti (bu/stats tmd-i-samples)
         insert-pct (* 100.0 (/ (- (:mean-ms ti) (:mean-ms di)) (:mean-ms ti)))
         insert-trim-pct (* 100.0 (/ (- (:trim-mean-ms ti) (:trim-mean-ms di))
                                     (:trim-mean-ms ti)))
         insert-sig? (not (bu/ci-overlap? di ti))

         ;; Warmup QUERY, then sample QUERY
         _ (bu/warmup! warmup-ms duck-q)
         _ (bu/warmup! warmup-ms tmd-q)
         [duck-q-samples tmd-q-samples] (bu/bench-pair-interleaved duck-q tmd-q opts)
         dq (bu/stats duck-q-samples)
         tq (bu/stats tmd-q-samples)
         query-pct  (* 100.0 (/ (- (:mean-ms tq) (:mean-ms dq)) (:mean-ms tq)))
         query-trim-pct (* 100.0 (/ (- (:trim-mean-ms tq) (:trim-mean-ms dq))
                                    (:trim-mean-ms tq)))
         query-sig? (not (bu/ci-overlap? dq tq))

         pos-or-neg #(if (pos? %) "+" "")]
     (println (format "  ducktape  INSERT  %s" (bu/format-stats di)))
     (println (format "  tmducken  INSERT  %s" (bu/format-stats ti)))
     (println (format "  Δ         INSERT  mean %s%.1f%%  (trim %s%.1f%%)  %s"
                      (pos-or-neg insert-pct) insert-pct
                      (pos-or-neg insert-trim-pct) insert-trim-pct
                      (if insert-sig? "(significant, 95%CI)" "(NS, CI overlaps)")))
     (println (format "  ducktape  QUERY   %s" (bu/format-stats dq)))
     (println (format "  tmducken  QUERY   %s" (bu/format-stats tq)))
     (println (format "  Δ         QUERY   mean %s%.1f%%  (trim %s%.1f%%)  %s"
                      (pos-or-neg query-pct) query-pct
                      (pos-or-neg query-trim-pct) query-trim-pct
                      (if query-sig? "(significant, 95%CI)" "(NS, CI overlaps)")))
     {:label label
      :n n
      :insert {:duck di :tmd ti :pct insert-pct :trim-pct insert-trim-pct :sig? insert-sig?}
      :query  {:duck dq :tmd tq :pct query-pct  :trim-pct query-trim-pct  :sig? query-sig?}
      :samples-ns {:duck-insert duck-i-samples
                   :tmd-insert  tmd-i-samples
                   :duck-query  duck-q-samples
                   :tmd-query   tmd-q-samples}})))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn compare-numeric
  ([] (compare-numeric {}))
  ([opts] (compare-workload "numeric" make-numeric opts)))

(defn compare-string
  ([] (compare-string {}))
  ([opts] (compare-workload "string" make-string opts)))

(defn compare-uuid
  ([] (compare-uuid {}))
  ([opts] (compare-workload "uuid" make-uuid opts)))

(defn compare-mixed
  ([] (compare-mixed {}))
  ([opts] (compare-workload "mixed" make-mixed opts)))

(defn compare-wide-numeric
  ([] (compare-wide-numeric {}))
  ([opts] (compare-workload "wide-numeric" make-wide-numeric opts)))

(defn compare-wide-mixed
  ([] (compare-wide-mixed {}))
  ([opts] (compare-workload "wide-mixed" make-wide-mixed opts)))

(defn compare-all
  "Run all workloads sequentially. Returns a vector of result maps."
  ([] (compare-all {}))
  ([opts]
   (println "\n═══════════════════════════════════════════════════════")
   (println (format " Head-to-head: ducktape vs tmducken (%,d rows)" N))
   (println (format " DuckDB %s  JDK %s"
                    (ffi/read-c-str (ffi/duckdb_library_version))
                    (System/getProperty "java.version")))
   (println (format " %s" (System/getProperty "java.vm.name")))
   (println "═══════════════════════════════════════════════════════")
   (let [results [(compare-numeric opts)
                  (compare-string opts)
                  (compare-uuid opts)
                  (compare-mixed opts)
                  (compare-wide-numeric opts)
                  (compare-wide-mixed opts)]]
     (println "\n═══════════════════════════════════════════════════════")
     (println " Summary  (Δ % = ducktape vs tmducken; + = duck faster)")
     (println " mean = arithmetic mean; trim = 10%-trimmed mean (robust to GC outliers)")
     (println "═══════════════════════════════════════════════════════")
     (println (format "  %-13s  %-22s  %-22s" "" "INSERT" "QUERY"))
     (println (format "  %-13s  %-22s  %-22s" "" "mean / trim" "mean / trim"))
     (doseq [r results]
       (println (format "  %-13s  %+5.1f%% %s / %+5.1f%%       %+5.1f%% %s / %+5.1f%%"
                        (:label r)
                        (:pct (:insert r)) (if (:sig? (:insert r)) "*" " ")
                        (:trim-pct (:insert r))
                        (:pct (:query r))  (if (:sig? (:query r))  "*" " ")
                        (:trim-pct (:query r)))))
     (println " (* = statistically significant at 95% CI on mean)\n")
     results)))
