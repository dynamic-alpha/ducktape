(ns hooks.ducktape.ffi
  "clj-kondo hooks for the FFI macros in `ducktape.ffi`.

  These macros generate a bunch of top-level vars that clj-kondo can't see
  through (definterface forms, defns built from a spec table, and a map of
  DUCKDB_TYPE_* constants). The hooks below produce stub forms that expose
  the same var names to clj-kondo so downstream code resolves cleanly."
  (:require [clj-kondo.hooks-api :as api]))

;; ---------------------------------------------------------------------------
;; Small builders — clj-kondo's hooks-api only exposes node constructors
;; (`list-node`, `vector-node`, `token-node`, `string-node`, ...), so we
;; assemble expansions out of those rather than going through a Clojure
;; reader.
;; ---------------------------------------------------------------------------

(defn- defn-stub
  "Emit `(defn <fname> [& args] args)` as a node. The body references
  `args` so clj-kondo doesn't flag the binding as unused."
  [fname]
  (api/list-node
   [(api/token-node 'defn)
    (api/token-node (symbol fname))
    (api/vector-node [(api/token-node '&) (api/token-node 'args)])
    (api/token-node 'args)]))

(defn- def-stub
  "Emit `(def <vname> <init>)` as a node."
  [vname init-node]
  (api/list-node
   [(api/token-node 'def)
    (api/token-node (symbol vname))
    init-node]))

;; ---------------------------------------------------------------------------
;; (define-ffi-fns! [[c-name ret [arg-types...] coerce] ...])
;;
;; Emits one `defn` per spec row plus the top-level `define-datatypes!`.
;; We don't model real arities — callers always invoke through the wrapper
;; name and arity-checking the generated stubs would just produce noise.
;; ---------------------------------------------------------------------------

(defn define-ffi-fns!
  [{:keys [node]}]
  (let [;; node = (define-ffi-fns! [[...] [...] ...])
        specs-node (second (:children node))
        specs      (when (and specs-node (api/vector-node? specs-node))
                     (:children specs-node))
        fn-names   (keep (fn [spec]
                           (when (api/vector-node? spec)
                             (let [cname-node (first (:children spec))
                                   v          (api/sexpr cname-node)]
                               (when (string? v) v))))
                         specs)
        stubs      (mapv defn-stub fn-names)
        define-ddt (api/list-node
                    [(api/token-node 'defn)
                     (api/token-node 'define-datatypes!)
                     (api/vector-node [(api/token-node 'duckdb-home)])
                     (api/token-node 'duckdb-home)])
        new-node   (api/list-node
                    (into [(api/token-node 'do) define-ddt] stubs))]
    {:node new-node}))

;; ---------------------------------------------------------------------------
;; (define-type-constants!)
;;
;; The real macro reads the (private) `duckdb-types` map in the same
;; namespace and emits (def DUCKDB_TYPE_X <int>) for every entry plus a
;; reverse-lookup `duckdb-type-map`. We hardcode the constant names here;
;; if the upstream type list grows, add the new names here too.
;; ---------------------------------------------------------------------------

(def ^:private duckdb-type-constants
  ["DUCKDB_TYPE_INVALID"
   "DUCKDB_TYPE_BOOLEAN"
   "DUCKDB_TYPE_TINYINT"
   "DUCKDB_TYPE_SMALLINT"
   "DUCKDB_TYPE_INTEGER"
   "DUCKDB_TYPE_BIGINT"
   "DUCKDB_TYPE_UTINYINT"
   "DUCKDB_TYPE_USMALLINT"
   "DUCKDB_TYPE_UINTEGER"
   "DUCKDB_TYPE_UBIGINT"
   "DUCKDB_TYPE_FLOAT"
   "DUCKDB_TYPE_DOUBLE"
   "DUCKDB_TYPE_TIMESTAMP"
   "DUCKDB_TYPE_DATE"
   "DUCKDB_TYPE_TIME"
   "DUCKDB_TYPE_INTERVAL"
   "DUCKDB_TYPE_HUGEINT"
   "DUCKDB_TYPE_VARCHAR"
   "DUCKDB_TYPE_BLOB"
   "DUCKDB_TYPE_DECIMAL"
   "DUCKDB_TYPE_TIMESTAMP_S"
   "DUCKDB_TYPE_TIMESTAMP_MS"
   "DUCKDB_TYPE_TIMESTAMP_NS"
   "DUCKDB_TYPE_ENUM"
   "DUCKDB_TYPE_LIST"
   "DUCKDB_TYPE_STRUCT"
   "DUCKDB_TYPE_MAP"
   "DUCKDB_TYPE_UUID"
   "DUCKDB_TYPE_UNION"
   "DUCKDB_TYPE_TIMESTAMP_TZ"
   "DUCKDB_TYPE_ARRAY"])

(defn define-type-constants!
  [_]
  (let [const-defs (mapv #(def-stub % (api/token-node 0)) duckdb-type-constants)
        type-map   (def-stub "duckdb-type-map" (api/token-node nil))
        new-node   (api/list-node
                    (into [(api/token-node 'do) type-map] const-defs))]
    {:node new-node}))
