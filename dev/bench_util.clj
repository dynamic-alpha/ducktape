(ns bench-util
  "Shared benchmark infrastructure for ducktape comparison harnesses.

  Provides:
    - Descriptive statistics (mean ± 95% CI, trimmed mean, p10/p50/p90, RSD)
    - Confidence-interval overlap test for significance flagging
    - GC pause + JIT warmup helpers
    - Interleaved paired sampling — alternates timing between two fns to
      cancel out drift in CPU/GC background load

  Methodology notes:
    * Time-based JIT warmup (caller passes a wall-clock duration).
    * One GC pause at the start of a sampling run, none between samples,
      so the mean reflects typical per-call cost including GC pressure.
    * Per-sample alternation between the two fns under test removes
      position bias (background drift hits both fns symmetrically across
      wallclock time).
    * Trimmed mean drops the top/bottom 10% of samples — resists GC-induced
      tail outliers without throwing away signal.")

;; ---------------------------------------------------------------------------
;; Statistics
;; ---------------------------------------------------------------------------

(defn ns->ms ^double [ns] (/ (double ns) 1e6))

(defn stats
  "Compute descriptive statistics for a sequence of sample times (ns).
  Returns a map of ms-converted measurements including a 10%-trimmed mean."
  [samples]
  (let [v (vec (sort samples))
        n (count v)
        mean (/ (double (reduce + v)) (max 1 n))
        var  (/ (double (reduce + (mapv (fn [^long x]
                                          (let [d (- (double x) mean)]
                                            (* d d)))
                                        v)))
                (max 1 (dec n)))
        std  (Math/sqrt var)
        sem  (/ std (Math/sqrt (max 1 n)))
        ci95 (* 1.96 sem)
        pick (fn [^double q] (nth v (min (dec n) (long (* q n)))))
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

(defn ci-overlap?
  "Do the 95% confidence intervals around the means overlap? When false,
  the two means differ at the 95% level."
  [a b]
  (let [a-lo (- (:mean-ms a) (:ci95-ms a))
        a-hi (+ (:mean-ms a) (:ci95-ms a))
        b-lo (- (:mean-ms b) (:ci95-ms b))
        b-hi (+ (:mean-ms b) (:ci95-ms b))]
    (and (< a-lo b-hi) (< b-lo a-hi))))

;; ---------------------------------------------------------------------------
;; Bench primitives
;; ---------------------------------------------------------------------------

(defn gc-pause!
  "Two-pass GC + brief sleep to let concurrent collectors compact."
  []
  (System/gc) (Thread/sleep 30)
  (System/gc) (Thread/sleep 20))

(defn time-ns
  "Run `f` once, return its wall-clock elapsed time in nanoseconds."
  ^long [f]
  (let [t (System/nanoTime)]
    (f)
    (- (System/nanoTime) t)))

(defn warmup!
  "Run `f` repeatedly until `warmup-ms` of wallclock time has elapsed.
  Discards results — purely for JIT warmup."
  [^long warmup-ms f]
  (let [deadline (+ (System/nanoTime) (* warmup-ms 1000000))]
    (loop []
      (when (< (System/nanoTime) deadline)
        (f)
        (recur)))))

(defn bench-pair-interleaved
  "Alternate timing `fn-a` and `fn-b` across `2 × n-samples` iterations.
  Returns `[a-samples b-samples]` (each a vector of `n-samples` ns).

  Even iterations time fn-a, odd time fn-b — both fns see the same
  background drift across wallclock time, so any CPU/GC noise cancels
  symmetrically in the comparison.

  Options:
    :n-samples  default 50"
  [fn-a fn-b {:keys [n-samples] :or {n-samples 50}}]
  (gc-pause!)
  (let [total (* 2 n-samples)
        a-samples (long-array n-samples)
        b-samples (long-array n-samples)]
    (dotimes [i total]
      (let [t   (time-ns (if (even? i) fn-a fn-b))
            idx (quot i 2)]
        (if (even? i)
          (aset a-samples idx t)
          (aset b-samples idx t))))
    [(vec a-samples) (vec b-samples)]))

;; ---------------------------------------------------------------------------
;; Formatting
;; ---------------------------------------------------------------------------

(defn format-stats
  "One-line statistics summary suitable for benchmark output."
  [s]
  (format "%6.2f ± %.2f ms  (trim-mean %.2f)  [p10 %.2f / p50 %.2f / p90 %.2f, RSD %4.1f%%]"
          (:mean-ms s) (:ci95-ms s) (:trim-mean-ms s)
          (:p10-ms s) (:median-ms s) (:p90-ms s) (:rsd-pct s)))

(defn sign-prefix
  "Empty string for non-negative, \"\" for `+` already-prefixed values
  printed via `%+f`. Useful when emitting Δ% lines manually."
  [^double x]
  (if (pos? x) "+" ""))
