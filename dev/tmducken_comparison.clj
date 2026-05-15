(ns tmducken-comparison
  "Head-to-head benchmark: ducktape vs tmducken in the same JVM.

  Usage from REPL:
    (require '[tmducken-comparison :as cmp] :reload)
    (cmp/compare-all)        ; full suite
    (cmp/compare-uuid)       ; just UUID
    (cmp/compare-string)     ; just string
    (cmp/compare-numeric)    ; just numeric
    (cmp/compare-mixed)      ; just mixed

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
  (:require [ducktape.core :as duck]
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
;; Statistics
;; ---------------------------------------------------------------------------

(defn- ns->ms ^double [ns] (/ (double ns) 1e6))

(defn- stats
  "Compute descriptive statistics for a sequence of sample times (ns).
  Returns a map of ms-converted measurements including a 10%-trimmed mean
  (robust against GC outliers in the tail)."
  [samples]
  (let [v (vec (sort samples))
        n (count v)
        mean (/ (double (reduce + v)) (max 1 n))
        var (/ (double (reduce + (mapv (fn [^long x]
                                         (let [d (- (double x) mean)]
                                           (* d d)))
                                       v)))
               (max 1 (dec n)))
        std (Math/sqrt var)
        sem (/ std (Math/sqrt (max 1 n)))
        ci95 (* 1.96 sem)
        pick (fn [^double q] (nth v (min (dec n) (long (* q n)))))
        ;; Trimmed mean: drop top/bottom 10% of samples then average.
        ;; Resists GC-induced outliers without throwing away signal.
        trim-lo (long (* 0.10 n))
        trim-hi (max 1 (- n trim-lo))
        trimmed (subvec v trim-lo trim-hi)
        trimmed-mean (/ (double (reduce + trimmed)) (max 1 (count trimmed)))]
    {:n            n
     :mean-ms      (ns->ms mean)
     :trim-mean-ms (ns->ms trimmed-mean)
     :ci95-ms      (ns->ms ci95)
     :std-ms       (ns->ms std)
     :rsd-pct      (if (zero? mean) 0.0 (* 100.0 (/ std mean)))
     :median-ms    (ns->ms (pick 0.50))
     :p10-ms       (ns->ms (pick 0.10))
     :p25-ms       (ns->ms (pick 0.25))
     :p75-ms       (ns->ms (pick 0.75))
     :p90-ms       (ns->ms (pick 0.90))
     :p95-ms       (ns->ms (pick 0.95))
     :min-ms       (ns->ms (first v))
     :max-ms       (ns->ms (last v))}))

(defn- ci-overlap?
  "Do the 95% confidence intervals around the means overlap?"
  [a b]
  (let [a-lo (- (:mean-ms a) (:ci95-ms a))
        a-hi (+ (:mean-ms a) (:ci95-ms a))
        b-lo (- (:mean-ms b) (:ci95-ms b))
        b-hi (+ (:mean-ms b) (:ci95-ms b))]
    (and (< a-lo b-hi) (< b-lo a-hi))))

;; ---------------------------------------------------------------------------
;; Bench primitives
;; ---------------------------------------------------------------------------

(defn- gc-pause!
  "Force GC and pause briefly to let the collector settle.
  Two passes to encourage compaction in concurrent collectors."
  []
  (System/gc) (Thread/sleep 30)
  (System/gc) (Thread/sleep 20))

(defn- time-ns ^long [f]
  (let [t (System/nanoTime)]
    (f)
    (- (System/nanoTime) t)))

(defn- warmup!
  "Run `f` repeatedly until `warmup-ms` of wallclock time has elapsed.
  Discards all results (purely for JIT warmup)."
  [^long warmup-ms f]
  (let [deadline (+ (System/nanoTime) (* warmup-ms 1000000))]
    (loop []
      (when (< (System/nanoTime) deadline)
        (f)
        (recur)))))

(defn- bench-pair-interleaved
  "Sample-level interleaved sampling. Returns [duck-samples-ns tmd-samples-ns].

  Approach (criterium-inspired, but simpler):
    1. One GC + pause at the very start.
    2. For 2*n-samples iterations, alternate timing duck-fn and tmd-fn.
    3. No GC between samples — let allocation pressure naturalize so the
       reported mean reflects the *typical* per-call cost (incl. GC overhead).

  Even iteration index → duck-fn. Odd → tmd-fn. Each library ends up with
  n-samples measurements, interleaved with the other across wallclock time.
  This minimises position bias (background CPU/GC drift affects both libs
  symmetrically) without injecting artificial cold-cache penalties from
  per-sample GCs."
  [duck-fn tmd-fn {:keys [n-samples]
                   :or {n-samples 50}}]
  (gc-pause!)
  (let [total (* 2 n-samples)
        duck-samples (long-array n-samples)
        tmd-samples  (long-array n-samples)]
    (dotimes [i total]
      (let [t (time-ns (if (even? i) duck-fn tmd-fn))
            idx (quot i 2)]
        (if (even? i)
          (aset duck-samples idx t)
          (aset tmd-samples  idx t))))
    [(vec duck-samples) (vec tmd-samples)]))

;; ---------------------------------------------------------------------------
;; Workload runner
;; ---------------------------------------------------------------------------

(defn- format-stats [s]
  (format "%6.2f ± %.2f ms  (trim-mean %.2f)  [p10 %.2f / p50 %.2f / p90 %.2f, RSD %4.1f%%]"
          (:mean-ms s) (:ci95-ms s) (:trim-mean-ms s)
          (:p10-ms s) (:median-ms s) (:p90-ms s) (:rsd-pct s)))

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
         _ (warmup! warmup-ms duck-i)
         _ (warmup! warmup-ms tmd-i)
         [duck-i-samples tmd-i-samples] (bench-pair-interleaved duck-i tmd-i opts)
         di (stats duck-i-samples)
         ti (stats tmd-i-samples)
         insert-pct (* 100.0 (/ (- (:mean-ms ti) (:mean-ms di)) (:mean-ms ti)))
         insert-trim-pct (* 100.0 (/ (- (:trim-mean-ms ti) (:trim-mean-ms di))
                                     (:trim-mean-ms ti)))
         insert-sig? (not (ci-overlap? di ti))

         ;; Warmup QUERY, then sample QUERY
         _ (warmup! warmup-ms duck-q)
         _ (warmup! warmup-ms tmd-q)
         [duck-q-samples tmd-q-samples] (bench-pair-interleaved duck-q tmd-q opts)
         dq (stats duck-q-samples)
         tq (stats tmd-q-samples)
         query-pct  (* 100.0 (/ (- (:mean-ms tq) (:mean-ms dq)) (:mean-ms tq)))
         query-trim-pct (* 100.0 (/ (- (:trim-mean-ms tq) (:trim-mean-ms dq))
                                    (:trim-mean-ms tq)))
         query-sig? (not (ci-overlap? dq tq))

         pos-or-neg #(if (pos? %) "+" "")]
     (println (format "  ducktape  INSERT  %s" (format-stats di)))
     (println (format "  tmducken  INSERT  %s" (format-stats ti)))
     (println (format "  Δ         INSERT  mean %s%.1f%%  (trim %s%.1f%%)  %s"
                      (pos-or-neg insert-pct) insert-pct
                      (pos-or-neg insert-trim-pct) insert-trim-pct
                      (if insert-sig? "(significant, 95%CI)" "(NS, CI overlaps)")))
     (println (format "  ducktape  QUERY   %s" (format-stats dq)))
     (println (format "  tmducken  QUERY   %s" (format-stats tq)))
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
                  (compare-mixed opts)]]
     (println "\n═══════════════════════════════════════════════════════")
     (println " Summary  (Δ % = ducktape vs tmducken; + = duck faster)")
     (println " mean = arithmetic mean; trim = 10%-trimmed mean (robust to GC outliers)")
     (println "═══════════════════════════════════════════════════════")
     (println (format "  %-8s  %-22s  %-22s" "" "INSERT" "QUERY"))
     (println (format "  %-8s  %-22s  %-22s" "" "mean / trim" "mean / trim"))
     (doseq [r results]
       (println (format "  %-8s  %+5.1f%% %s / %+5.1f%%       %+5.1f%% %s / %+5.1f%%"
                        (:label r)
                        (:pct (:insert r)) (if (:sig? (:insert r)) "*" " ")
                        (:trim-pct (:insert r))
                        (:pct (:query r))  (if (:sig? (:query r))  "*" " ")
                        (:trim-pct (:query r)))))
     (println " (* = statistically significant at 95% CI on mean)\n")
     results)))
