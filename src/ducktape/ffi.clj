(ns ducktape.ffi
  (:import [java.lang.foreign
            AddressLayout Arena Linker Linker$Option
            MemoryLayout MemorySegment FunctionDescriptor SymbolLookup
            ValueLayout ValueLayout$OfByte ValueLayout$OfInt
            ValueLayout$OfLong ValueLayout$OfDouble ValueLayout$OfFloat
            ValueLayout$OfShort]
           [java.lang.invoke MethodHandle]
           [java.nio.file Paths]))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; Typed ValueLayout constants — critical for avoiding reflection on .set/.get
;; ---------------------------------------------------------------------------

(def ^ValueLayout$OfLong   VL-LONG   ValueLayout/JAVA_LONG)
(def ^ValueLayout$OfDouble VL-DOUBLE ValueLayout/JAVA_DOUBLE)
(def ^ValueLayout$OfFloat  VL-FLOAT  ValueLayout/JAVA_FLOAT)
(def ^ValueLayout$OfInt    VL-INT    ValueLayout/JAVA_INT)
(def ^ValueLayout$OfShort  VL-SHORT  ValueLayout/JAVA_SHORT)
(def ^ValueLayout$OfByte   VL-BYTE   ValueLayout/JAVA_BYTE)
(def ^AddressLayout        VL-ADDR   ValueLayout/ADDRESS)

;; ---------------------------------------------------------------------------
;; Library loading
;; ---------------------------------------------------------------------------

(def ^:private ^SymbolLookup the-lib nil)
(def ^:private ^Linker the-linker nil)

(defn- load-lib ^SymbolLookup [^String duckdb-home]
  (let [lib-name (System/mapLibraryName "duckdb")
        path (if (empty? duckdb-home)
               lib-name
               (str (Paths/get duckdb-home (into-array String [lib-name]))))]
    (SymbolLookup/libraryLookup path (Arena/global))))

(defn- sym ^MemorySegment [^String name]
  (let [opt (.find the-lib name)]
    (when-not (.isPresent opt)
      (throw (RuntimeException. (str "Symbol not found: " name))))
    (.get opt)))

(defn- fn-handle ^MethodHandle [^String name ^FunctionDescriptor descriptor]
  (.downcallHandle ^Linker the-linker
                   (sym name)
                   descriptor
                   (into-array Linker$Option [])))

;; ---------------------------------------------------------------------------
;; duckdb_result struct layout — 6 × JAVA_LONG (48 bytes on 64-bit)
;; ---------------------------------------------------------------------------

(def result-layout
  (MemoryLayout/structLayout
   (into-array MemoryLayout (repeatedly 6 (constantly ValueLayout/JAVA_LONG)))))

;; ---------------------------------------------------------------------------
;; MethodHandle invocation helper
;; ---------------------------------------------------------------------------

(defn mh-invoke [^MethodHandle mh & args]
  (.invokeWithArguments mh ^java.util.List (java.util.Arrays/asList (object-array args))))

;; Specialized arities — avoid varargs object-array + Arrays.asList allocation
(let [^java.util.List e (java.util.Collections/emptyList)]
  (defn mh-invoke0 [^MethodHandle mh]
    (.invokeWithArguments mh e)))

(defn mh-invoke1 [^MethodHandle mh a0]
  (.invokeWithArguments mh ^java.util.List (java.util.List/of a0)))

(defn mh-invoke2 [^MethodHandle mh a0 a1]
  (.invokeWithArguments mh ^java.util.List (java.util.List/of a0 a1)))

(defn mh-invoke3 [^MethodHandle mh a0 a1 a2]
  (.invokeWithArguments mh ^java.util.List (java.util.List/of a0 a1 a2)))

(defn mh-invoke4 [^MethodHandle mh a0 a1 a2 a3]
  (.invokeWithArguments mh ^java.util.List (java.util.List/of a0 a1 a2 a3)))

;; ---------------------------------------------------------------------------
;; DuckDB type enum + reverse map  (data-driven, macro-generated)
;; ---------------------------------------------------------------------------

(def ^:private duckdb-types
  '{DUCKDB_TYPE_INVALID      0
    DUCKDB_TYPE_BOOLEAN      1
    DUCKDB_TYPE_TINYINT      2
    DUCKDB_TYPE_SMALLINT     3
    DUCKDB_TYPE_INTEGER      4
    DUCKDB_TYPE_BIGINT       5
    DUCKDB_TYPE_UTINYINT     6
    DUCKDB_TYPE_USMALLINT    7
    DUCKDB_TYPE_UINTEGER     8
    DUCKDB_TYPE_UBIGINT      9
    DUCKDB_TYPE_FLOAT       10
    DUCKDB_TYPE_DOUBLE      11
    DUCKDB_TYPE_TIMESTAMP   12
    DUCKDB_TYPE_DATE        13
    DUCKDB_TYPE_TIME        14
    DUCKDB_TYPE_INTERVAL    15
    DUCKDB_TYPE_HUGEINT     16
    DUCKDB_TYPE_VARCHAR     17
    DUCKDB_TYPE_BLOB        18
    DUCKDB_TYPE_DECIMAL     19
    DUCKDB_TYPE_TIMESTAMP_S  20
    DUCKDB_TYPE_TIMESTAMP_MS 21
    DUCKDB_TYPE_TIMESTAMP_NS 22
    DUCKDB_TYPE_ENUM        23
    DUCKDB_TYPE_LIST        24
    DUCKDB_TYPE_STRUCT      25
    DUCKDB_TYPE_MAP         26
    DUCKDB_TYPE_UUID        27
    DUCKDB_TYPE_UNION       28
    DUCKDB_TYPE_TIMESTAMP_TZ 31
    DUCKDB_TYPE_ARRAY       33})

(defmacro ^:private define-type-constants! []
  `(do ~@(for [[sym val] duckdb-types]
           `(def ~(with-meta sym {:const true}) ~val))
       (def ~'duckdb-type-map
         ~(into {} (map (fn [[sym val]] [val (keyword sym)]) duckdb-types)))))

(define-type-constants!)

(def ^:const DuckDBSuccess 0)

;; ---------------------------------------------------------------------------
;; FFI function specs — the single source of truth
;;
;; Each entry: [c-name ret arg-layouts coerce]
;;   ret       — a MemoryLayout, or :void
;;   arg-layouts — vector of MemoryLayout expressions
;;   coerce    — nil (raw return), `int`, or `long`
;; ---------------------------------------------------------------------------

(def ^:private layout-alias
  {:addr   'VL-ADDR
   :int    'ValueLayout/JAVA_INT
   :long   'ValueLayout/JAVA_LONG
   :byte   'ValueLayout/JAVA_BYTE
   :short  'ValueLayout/JAVA_SHORT
   :float  'ValueLayout/JAVA_FLOAT
   :double 'ValueLayout/JAVA_DOUBLE
   :result 'result-layout})

(defn- resolve-layout [k]
  (if (keyword? k) (get layout-alias k) k))

(defmacro ^:private define-ffi-fns!
  "From a spec vector, generates for each entry:
     1. (defonce ^:private -name (atom nil))
     2. A (reset! ...) form inside define-datatypes!
     3. (defn name [args...] (coerce (mh-invoke @-name args...)))"
  [specs]
  (let [atom-defs   (for [[cname _ _ _] specs]
                      (let [aname (symbol (str "-" cname))]
                        `(defonce ^:private ~aname (atom nil))))
        reset-forms (for [[cname ret args _] specs]
                      (let [aname  (symbol (str "-" cname))
                            ret-l  (resolve-layout ret)
                            arg-ls (mapv resolve-layout args)]
                        `(reset! ~aname
                                 (fn-handle ~cname
                                            ~(if (= ret :void)
                                               `(FunctionDescriptor/ofVoid (into-array MemoryLayout ~arg-ls))
                                               `(FunctionDescriptor/of ~ret-l (into-array MemoryLayout ~arg-ls)))))))
        wrapper-fns (for [[cname _ args coerce] specs]
                      (let [fname  (symbol cname)
                            aname  (symbol (str "-" cname))
                            params (mapv #(symbol (str "a" %)) (range (count args)))
                            invoke (case (count args)
                                     0 'mh-invoke0
                                     1 'mh-invoke1
                                     2 'mh-invoke2
                                     3 'mh-invoke3
                                     4 'mh-invoke4
                                     'mh-invoke)
                            call   `(~invoke (deref ~aname) ~@params)]
                        `(defn ~fname ~params
                           ~(case coerce
                              :int  `(int ~call)
                              :long `(long ~call)
                              call))))]
    `(do
       ~@atom-defs

       (defn ~'define-datatypes! [^String ~'duckdb-home]
         (let [~'lib    (load-lib ~'duckdb-home)
               ~'linker (Linker/nativeLinker)]
           (alter-var-root #'the-lib (constantly ~'lib))
           (alter-var-root #'the-linker (constantly ~'linker))
           ~@reset-forms))

       ~@wrapper-fns)))

;; ---------------------------------------------------------------------------
;; The spec table — one row per C function
;; ---------------------------------------------------------------------------

(define-ffi-fns!
  [;; Database lifecycle
   ["duckdb_open_ext"           :int  [:addr :addr :addr :addr]  :int]
   ["duckdb_close"              :void [:addr]                    nil]
   ["duckdb_connect"            :int  [:addr :addr]              :int]
   ["duckdb_disconnect"         :void [:addr]                    nil]
   ["duckdb_library_version"    :addr []                         nil]
   ["duckdb_free"               :void [:addr]                    nil]
   ;; Config
   ["duckdb_config_count"       :long []                         :long]
   ["duckdb_create_config"      :int  [:addr]                    :int]
   ["duckdb_get_config_flag"    :int  [:long :addr :addr]        :int]
   ["duckdb_set_config"         :int  [:addr :addr :addr]        :int]
   ["duckdb_destroy_config"     :void [:addr]                    nil]
   ;; Query execution
   ["duckdb_query"              :int  [:addr :addr :addr]        :int]
   ["duckdb_destroy_result"     :void [:addr]                    nil]
   ["duckdb_result_error"       :addr [:addr]                    nil]
   ;; Result metadata
   ["duckdb_column_count"       :long [:addr]                    :long]
   ["duckdb_column_name"        :addr [:addr :long]              nil]
   ["duckdb_column_logical_type" :addr [:addr :long]             nil]
   ["duckdb_get_type_id"        :int  [:addr]                    :int]
   ["duckdb_destroy_logical_type" :void [:addr]                  nil]
   ;; Chunk fetch  (fetch_chunk takes result struct BY VALUE)
   ["duckdb_fetch_chunk"        :addr [:result]                  nil]
   ["duckdb_data_chunk_get_size" :long [:addr]                   :long]
   ["duckdb_data_chunk_get_vector" :addr [:addr :long]           nil]
   ["duckdb_vector_get_data"    :addr [:addr]                    nil]
   ["duckdb_vector_get_validity" :addr [:addr]                   nil]
   ["duckdb_destroy_data_chunk" :void [:addr]                    nil]
   ;; Chunk write / appender
   ["duckdb_appender_create"    :int  [:addr :addr :addr :addr]  :int]
   ["duckdb_appender_destroy"   :int  [:addr]                    :int]
   ["duckdb_appender_error"     :addr [:addr]                    nil]
   ["duckdb_append_data_chunk"  :int  [:addr :addr]              :int]
   ["duckdb_vector_size"        :long []                         :long]
   ["duckdb_create_data_chunk"  :addr [:addr :long]              nil]
   ["duckdb_data_chunk_set_size" :void [:addr :long]             nil]
   ["duckdb_data_chunk_reset"   :void [:addr]                    nil]
   ["duckdb_create_logical_type" :addr [:int]                    nil]
   ["duckdb_vector_ensure_validity_writable" :void [:addr]       nil]
   ;; Prepared statements
   ["duckdb_prepare"            :int  [:addr :addr :addr]        :int]
   ["duckdb_destroy_prepare"    :void [:addr]                    nil]
   ["duckdb_prepare_error"      :addr [:addr]                    nil]
   ["duckdb_nparams"            :long [:addr]                    :long]
   ["duckdb_param_type"         :int  [:addr :long]              :int]
   ["duckdb_pending_prepared"   :int  [:addr :addr]              :int]
   ["duckdb_execute_pending"    :int  [:addr :addr]              :int]
   ["duckdb_destroy_pending"    :void [:addr]                    nil]
   ["duckdb_pending_error"      :addr [:addr]                    nil]
   ;; Bind params
   ["duckdb_bind_null"          :int  [:addr :long]              :int]
   ["duckdb_bind_boolean"       :int  [:addr :long :byte]        :int]
   ["duckdb_bind_int8"          :int  [:addr :long :byte]        :int]
   ["duckdb_bind_int16"         :int  [:addr :long :short]       :int]
   ["duckdb_bind_int32"         :int  [:addr :long :int]         :int]
   ["duckdb_bind_int64"         :int  [:addr :long :long]        :int]
   ["duckdb_bind_uint8"         :int  [:addr :long :byte]        :int]
   ["duckdb_bind_uint16"        :int  [:addr :long :short]       :int]
   ["duckdb_bind_uint32"        :int  [:addr :long :int]         :int]
   ["duckdb_bind_uint64"        :int  [:addr :long :long]        :int]
   ["duckdb_bind_float"         :int  [:addr :long :float]       :int]
   ["duckdb_bind_double"        :int  [:addr :long :double]      :int]
   ["duckdb_bind_date"          :int  [:addr :long :int]         :int]
   ["duckdb_bind_time"          :int  [:addr :long :long]        :int]
   ["duckdb_bind_timestamp"     :int  [:addr :long :long]        :int]
   ["duckdb_bind_varchar_length" :int [:addr :long :addr :long]  :int]
   ;; Logical type introspection — DECIMAL
   ["duckdb_decimal_internal_type" :int  [:addr]              :int]
   ["duckdb_decimal_scale"         :int  [:addr]              :int]
   ["duckdb_decimal_width"         :int  [:addr]              :int]
   ;; Logical type introspection — ENUM
   ["duckdb_enum_internal_type"    :int  [:addr]              :int]
   ["duckdb_enum_dictionary_size"  :int  [:addr]              :int]
   ["duckdb_enum_dictionary_value" :addr [:addr :long]        nil]
   ["duckdb_create_enum_type"      :addr [:addr :long]        nil]
   ;; Logical type introspection — LIST
   ["duckdb_list_vector_get_child" :addr [:addr]              nil]
   ["duckdb_list_vector_get_size"  :long [:addr]              :long]
   ["duckdb_list_vector_set_size"  :int  [:addr :long]        :int]
   ["duckdb_list_type_child_type"  :addr [:addr]              nil]
   ["duckdb_create_list_type"      :addr [:addr]              nil]
   ;; Logical type introspection — STRUCT
   ["duckdb_struct_type_child_count" :long [:addr]            :long]
   ["duckdb_struct_type_child_name"  :addr [:addr :long]      nil]
   ["duckdb_struct_type_child_type"  :addr [:addr :long]      nil]
   ["duckdb_struct_vector_get_child" :addr [:addr :long]      nil]
   ["duckdb_create_struct_type"      :addr [:addr :addr :long] nil]
   ;; Logical type introspection — ARRAY
   ["duckdb_array_type_child_type"   :addr [:addr]            nil]
   ["duckdb_array_vector_get_child"  :addr [:addr]            nil]
   ["duckdb_array_type_array_size"   :long [:addr]            :long]
   ["duckdb_create_array_type"       :addr [:addr :long]      nil]
   ;; Appender column introspection
   ["duckdb_appender_column_type"    :addr [:addr :long]      nil]
   ["duckdb_appender_column_count"   :long [:addr]            :long]])

;; ---------------------------------------------------------------------------
;; Helper functions
;; ---------------------------------------------------------------------------

(defn alloc-c-str ^MemorySegment [^Arena arena ^String s]
  (let [bytes (.getBytes s "UTF-8")
        n (alength bytes)
        seg (.allocate arena (inc n))]
    (MemorySegment/copy bytes 0 seg VL-BYTE 0 n)
    (.set seg VL-BYTE (long n) (byte 0))
    seg))

(defn read-c-str ^String [^MemorySegment ptr]
  (when (and ptr (not= 0 (.address ptr)))
    (let [^MemorySegment seg (.reinterpret ptr Long/MAX_VALUE)]
      (loop [n (long 0)]
        (if (== 0 (.get seg VL-BYTE n))
          (let [arr (byte-array n)]
            (MemorySegment/copy seg VL-BYTE (long 0) arr 0 (int n))
            (String. arr "UTF-8"))
          (recur (unchecked-inc n)))))))

(defn destroy-ptr!
  "Allocate a pointer-to-pointer, store `seg`'s address, call `destroy-fn` on it.
  Common pattern for duckdb_destroy_* functions that take T* (pointer to handle)."
  [destroy-fn ^MemorySegment seg]
  (with-open [a (Arena/ofConfined)]
    (let [p (.allocate a VL-ADDR)]
      (.set p VL-ADDR 0 seg)
      (destroy-fn p))))

(defn destroy-ptrs!
  "Batch destroy: one arena for multiple pointer-to-pointer destroy calls.
  `pairs` is a sequence of [destroy-fn segment]."
  [pairs]
  (with-open [a (Arena/ofConfined)]
    (doseq [[destroy-fn ^MemorySegment seg] pairs]
      (let [p (.allocate a VL-ADDR)]
        (.set p VL-ADDR 0 seg)
        (destroy-fn p)))))
