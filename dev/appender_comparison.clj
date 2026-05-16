(ns appender-comparison
  "Streaming-insert benchmark: ducktape's stateful appender API vs many
  one-shot insert-dataset! calls.

  Models the canonical streaming-ingest scenario — a producer feeds the
  database mini-batches (Kafka messages, paginated pages, file shards) —
  and quantifies how much amortizing the per-call appender setup actually
  saves at varying batch frequencies.

  Usage:
    (require '[appender-comparison :as ac] :reload)
    (ac/compare-all)                            ; full sweep
    (ac/compare-streaming :string 100000 1000)  ; one configuration

  IMPORTANT: only call ONE compare-* at a time. Running concurrently makes
  results unreliable since the two paths compete for CPU and memory bandwidth.

  Methodology mirrors tmducken-comparison (shared infrastructure in
  bench-util): time-based JIT warmup, per-sample alternation between the
  two paths, 95% CI on the mean, 10%-trimmed mean for robustness."
  (:require [bench-util :as bu]
            [ducktape.core :as duck]
            [ducktape.ffi :as ffi]
            [tech.v3.dataset :as ds])
  (:import [java.time LocalDate]
           [java.lang AutoCloseable]))

(def ^:const default-total-rows 100000)

;; ---------------------------------------------------------------------------
;; Per-batch dataset constructors
;;
;; Each builds a single batch of `n` rows.  Different `seed` values are used
;; across batches so we don't accidentally test cached object identity.
;; ---------------------------------------------------------------------------

(defn- batch-numeric [seed n]
  (let [s (long seed)]
    (ds/->dataset {:longs   (long-array (map #(+ s %) (range n)))
                   :doubles (double-array (map #(* (+ s %) 1.23456789) (range n)))
                   :ints    (int-array (map #(+ s %) (range n)))
                   :floats  (float-array (map #(* (+ s %) 0.5) (range n)))})))

(defn- batch-string [seed n]
  (let [s (long seed)]
    (ds/->dataset {:short_str (mapv #(str "s" (rem (+ s %) 9999)) (range n))
                   :long_str  (mapv #(str "a-longer-string-value-" (rem (+ s %) 99999)) (range n))
                   :id        (long-array (map #(+ s %) (range n)))})))

(defn- batch-mixed [seed n]
  (let [s (long seed)
        base-date (LocalDate/of 2000 1 1)]
    (ds/->dataset {:longs   (long-array (map #(+ s %) (range n)))
                   :doubles (double-array (map #(* (+ s %) 1.23456789) (range n)))
                   :strings (mapv #(str "str" (rem (+ s %) 9999)) (range n))
                   :dates   (mapv #(.plusDays base-date (long (rem (+ s %) 3650))) (range n))})))

(def ^:private workload->batch-fn
  {:numeric batch-numeric
   :string  batch-string
   :mixed   batch-mixed})

(defn- build-batches
  "Pre-build `n-batches` × `batch-size` batches for `workload`, so dataset
  construction isn't part of the timed window."
  [workload n-batches batch-size]
  (let [batch-fn (workload->batch-fn workload)]
    (vec (for [i (range n-batches)]
           (batch-fn (* (long i) (long batch-size)) (long batch-size))))))

;; ---------------------------------------------------------------------------
;; Connection — lazy, shared, cached across calls
;; ---------------------------------------------------------------------------

(defonce ^:private duck-state
  (delay
    (duck/initialize!)
    (let [db (duck/open-db)
          conn (duck/connect db)]
      {:db db :conn conn})))

(defn- duck-conn [] (:conn @duck-state))

;; ---------------------------------------------------------------------------
;; Workload runner
;; ---------------------------------------------------------------------------

(defn- drop-table-quietly! [conn tbl]
  (try (duck/run-query! conn (str "DROP TABLE IF EXISTS " tbl))
       (catch Exception _)))

(defn compare-streaming
  "Time both write paths over a stream of `n-batches` batches summing to
  `total-rows` rows.  Each sample writes the full stream after clearing
  the table.

  `workload` is one of `:numeric`, `:string`, `:mixed`.

  Options:
    :warmup-ms  per-fn JIT warmup wall-clock (default 1500)
    :n-samples  samples per path             (default 30)

  Returns a result map. `:speedup` is appender ÷ insert (>1 = appender wins)."
  ([workload total-rows n-batches]
   (compare-streaming workload total-rows n-batches {}))
  ([workload total-rows n-batches opts]
   (let [{:keys [warmup-ms n-samples]
          :or {warmup-ms 1500 n-samples 30}} opts
         batch-size  (long (max 1 (Math/floor (/ (double total-rows) n-batches))))
         actual-rows (* batch-size n-batches)
         batches     (build-batches workload n-batches batch-size)
         sample-ds   (first batches)
         conn        (duck-conn)
         tbl         (format "stream_%s_b%d_n%d" (name workload) batch-size n-batches)
         schema-ds   (vary-meta sample-ds assoc :name tbl)
         _ (drop-table-quietly! conn tbl)
         _ (duck/create-table! conn schema-ds)
         ;; insert-dataset! path: full setup+write+teardown per batch
         insert-fn (fn []
                     (duck/run-query! conn (str "DELETE FROM " tbl))
                     (let [n (count batches)]
                       (dotimes [i n]
                         (duck/insert-dataset! conn (vary-meta (nth batches i) assoc :name tbl)))))
         ;; appender path: one open + N appends + close
         append-fn (fn []
                     (duck/run-query! conn (str "DELETE FROM " tbl))
                     (with-open [^AutoCloseable app (duck/open-appender conn schema-ds)]
                       (let [n (count batches)]
                         (dotimes [i n]
                           (duck/append-dataset! app (nth batches i))))))
         _ (println (format "\n▸ %s (%,d rows ÷ %,d batches = %,d rows/batch, %d samples, %dms warmup)"
                            (name workload) actual-rows n-batches batch-size n-samples warmup-ms))
         _ (bu/warmup! warmup-ms append-fn)
         _ (bu/warmup! warmup-ms insert-fn)
         [app-samples ins-samples] (bu/bench-pair-interleaved append-fn insert-fn
                                                              {:n-samples n-samples})
         app (bu/stats app-samples)
         ins (bu/stats ins-samples)
         speedup      (/ (:mean-ms ins) (:mean-ms app))
         trim-speedup (/ (:trim-mean-ms ins) (:trim-mean-ms app))
         pct          (* 100.0 (/ (- (:mean-ms ins) (:mean-ms app)) (:mean-ms ins)))
         trim-pct     (* 100.0 (/ (- (:trim-mean-ms ins) (:trim-mean-ms app))
                                  (:trim-mean-ms ins)))
         sig?         (not (bu/ci-overlap? app ins))
         app-rps      (long (/ actual-rows (/ (:mean-ms app) 1000.0)))
         ins-rps      (long (/ actual-rows (/ (:mean-ms ins) 1000.0)))]
     (println (format "  appender         %s  %,d rows/s" (bu/format-stats app) app-rps))
     (println (format "  insert-dataset!  %s  %,d rows/s" (bu/format-stats ins) ins-rps))
     (println (format "  Δ                speedup %.2f×  (trim %.2f×)  saves %+.1f%%  %s"
                      speedup trim-speedup pct
                      (if sig? "(significant, 95%CI)" "(NS, CI overlaps)")))
     (drop-table-quietly! conn tbl)
     {:workload workload
      :total-rows actual-rows
      :n-batches n-batches
      :batch-size batch-size
      :appender app
      :insert ins
      :speedup speedup
      :trim-speedup trim-speedup
      :pct pct
      :trim-pct trim-pct
      :sig? sig?
      :appender-rps app-rps
      :insert-rps ins-rps})))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn compare-all
  "Sweep workloads × batch counts. Default: 100k total rows over batch
  counts of 10, 100, 1000, 10000 — covering one large batch (no real
  amortization) through to 10-row mini-batches where setup dominates.

  Options:
    :total-rows     default 100000
    :batch-counts   default [10 100 1000 10000]
    :workloads      default [:numeric :string :mixed]
    :warmup-ms      default 1500
    :n-samples      default 30

  Returns a vector of result maps. Prints a final summary table."
  ([] (compare-all {}))
  ([opts]
   (let [{:keys [total-rows batch-counts workloads]
          :or {total-rows   default-total-rows
               batch-counts [10 100 1000 10000]
               workloads    [:numeric :string :mixed]}} opts]
     ;; Force lazy init so the header can read the DuckDB version.
     (duck-conn)
     (println "\n═══════════════════════════════════════════════════════")
     (println (format " Streaming ingest: appender vs insert-dataset! (%,d rows total)"
                      total-rows))
     (println (format " DuckDB %s  JDK %s"
                      (ffi/read-c-str (ffi/duckdb_library_version))
                      (System/getProperty "java.version")))
     (println (format " %s" (System/getProperty "java.vm.name")))
     (println "═══════════════════════════════════════════════════════")
     (let [results (vec (for [w  workloads
                              nb batch-counts]
                          (compare-streaming w total-rows nb opts)))]
       (println "\n═══════════════════════════════════════════════════════")
       (println " Summary  (speedup × = how much faster the appender API is)")
       (println " mean × / trimmed-mean ×; * = significant at 95% CI on mean")
       (println "═══════════════════════════════════════════════════════")
       (let [header (apply str
                           (format "  %-9s" "workload")
                           (for [nb batch-counts]
                             (format "  %14s" (format "%d batches" nb))))]
         (println header))
       (let [by-workload (group-by :workload results)]
         (doseq [w workloads]
           (let [row (sort-by :n-batches (get by-workload w))]
             (println (apply str
                             (format "  %-9s" (name w))
                             (for [r row]
                               (format "  %6.2f× %s / %5.2f×"
                                       (:speedup r)
                                       (if (:sig? r) "*" " ")
                                       (:trim-speedup r))))))))
       (println)
       results))))
