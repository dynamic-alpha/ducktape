(ns profile
  "Head-to-head profiling helpers for ducktape vs tmducken via clj-async-profiler.

  Requires JVM started with the :dev alias opts (`-Djdk.attach.allowAttachSelf`
  and friends — set in deps.edn). After REPL is up, from any namespace:

    (require '[profile :as p])

    ;; Single-library CPU flamegraph (returns path to .html in /tmp)
    (p/profile-numeric :ducktape)
    (p/profile-numeric :tmducken)

    ;; Differential flamegraph: red = ducktape spends more time,
    ;; blue = tmducken spends more time
    (p/diff-numeric)
    (p/diff-string)
    (p/diff-uuid)
    (p/diff-mixed)

    ;; Allocation profile (find object-allocation hotspots)
    (p/profile-numeric :ducktape {:event :alloc})

    ;; Interactive UI at http://localhost:8080 — browse all captured profiles
    (p/serve)

  All profile files land in /tmp/clj-async-profiler/results/."
  (:require [clj-async-profiler.core :as prof]
            [clojure.string :as str]
            [tmducken-comparison :as cmp]
            [ducktape.core :as duck]
            [tmducken.duckdb :as tddb]
            [tech.v3.dataset :as ds]))

;; ---------------------------------------------------------------------------
;; Library dispatch
;; ---------------------------------------------------------------------------

(def ^:private libs
  {:ducktape {:run-query!     duck/run-query!
              :create-table!  duck/create-table!
              :insert!        duck/insert-dataset!
              :query          duck/sql->dataset
              :conn           cmp/duck-conn}
   :tmducken {:run-query!     tddb/run-query!
              :create-table!  tddb/create-table!
              :insert!        tddb/insert-dataset!
              :query          tddb/sql->dataset
              :conn           cmp/tmd-conn}})

(defn- ensure-table!
  "Drop+recreate+populate the table for `ds-fn` under the given library.
  Returns [conn sql] ready to be queried."
  [lib ds]
  (let [{:keys [run-query! create-table! insert! conn]} (get libs lib)
        tbl (ds/dataset-name ds)
        conn (conn)]
    (try (run-query! conn (str "DROP TABLE IF EXISTS " tbl)) (catch Exception _))
    (create-table! conn ds)
    (insert! conn ds)
    [conn (str "SELECT * FROM " tbl)]))

;; ---------------------------------------------------------------------------
;; Profile a single workload on a single library
;; ---------------------------------------------------------------------------

(defn- profile-workload*
  "Runs warmup then `iters` iterations of (query conn sql) under the profiler.
  Returns the path to the generated flamegraph HTML file."
  [lib workload-name ds-fn
   {:keys [iters rows warmup event interval]
    :or {iters 100 rows 1000000 warmup 10 event :cpu interval 1000000}}]
  (let [{:keys [query]} (get libs lib)
        ds (ds-fn rows)
        [conn sql] (ensure-table! lib ds)
        title (format "%s-%s-query-%dx%d" (name lib) workload-name iters rows)]
    ;; Warmup so JIT settles
    (dotimes [_ warmup] (query conn sql))
    (System/gc)
    (Thread/sleep 200)
    ;; Manual start/stop so we get the flamegraph path back. The `profile`
    ;; macro returns the body's value (nil from dotimes) and only prints the
    ;; path as a side effect.
    (prof/start {:event event :interval interval})
    (let [path (volatile! nil)]
      (try
        (dotimes [_ iters] (query conn sql))
        (finally
          (vreset! path (prof/stop {:title title}))))
      (println (format "✓ %s → %s" title @path))
      @path)))

(defn profile-numeric
  ([lib] (profile-numeric lib {}))
  ([lib opts] (profile-workload* lib "numeric" cmp/make-numeric opts)))

(defn profile-string
  ([lib] (profile-string lib {:iters 30}))
  ([lib opts] (profile-workload* lib "string" cmp/make-string (merge {:iters 30} opts))))

(defn profile-uuid
  ([lib] (profile-uuid lib {:iters 30}))
  ([lib opts] (profile-workload* lib "uuid" cmp/make-uuid (merge {:iters 30} opts))))

(defn profile-mixed
  ([lib] (profile-mixed lib {:iters 30}))
  ([lib opts] (profile-workload* lib "mixed" cmp/make-mixed (merge {:iters 30} opts))))

;; ---------------------------------------------------------------------------
;; Differential profiles — show where ducktape spends MORE/LESS time
;; than tmducken on the same workload
;; ---------------------------------------------------------------------------

(defn- html->txt
  "Convert a flamegraph HTML path to its collapsed-stacks .txt sibling.
  clj-async-profiler writes both files with matching prefixes."
  [html-path]
  (-> (str html-path)
      (str/replace #"-flamegraph\.html$" "-collapsed.txt")))

(defn- diff*
  [workload-name ds-fn opts]
  (let [tm-html (profile-workload* :tmducken workload-name ds-fn opts)
        dk-html (profile-workload* :ducktape workload-name ds-fn opts)
        tm-txt (html->txt tm-html)
        dk-txt (html->txt dk-html)
        diff-out (prof/generate-diffgraph
                  tm-txt dk-txt
                  {:title (format "diff-%s: ducktape vs tmducken" workload-name)})]
    (println (format "✓ diff → %s" diff-out))
    diff-out))

(defn diff-numeric
  ([] (diff-numeric {}))
  ([opts] (diff* "numeric" cmp/make-numeric opts)))

(defn diff-string
  ([] (diff-string {:iters 30}))
  ([opts] (diff* "string" cmp/make-string (merge {:iters 30} opts))))

(defn diff-uuid
  ([] (diff-uuid {:iters 30}))
  ([opts] (diff* "uuid" cmp/make-uuid (merge {:iters 30} opts))))

(defn diff-mixed
  ([] (diff-mixed {:iters 30}))
  ([opts] (diff* "mixed" cmp/make-mixed (merge {:iters 30} opts))))

;; ---------------------------------------------------------------------------
;; UI / utilities
;; ---------------------------------------------------------------------------

(defn serve
  "Start the interactive flamegraph UI. Defaults to localhost:8080."
  ([] (serve 8080))
  ([port] (prof/serve-ui port)))

(defn clear!
  "Wipe all captured profiles from /tmp/clj-async-profiler/."
  []
  (prof/clear-results))

(defn list-events
  "Print all sampling events the kernel/JVM exposes (e.g. :cpu, :alloc, :wall…)."
  []
  (prof/list-event-types))
