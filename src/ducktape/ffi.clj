(ns ducktape.ffi
  (:import [java.lang.foreign
            AddressLayout Arena Linker Linker$Option
            MemoryLayout MemorySegment FunctionDescriptor SymbolLookup
            ValueLayout ValueLayout$OfByte ValueLayout$OfInt
            ValueLayout$OfLong ValueLayout$OfDouble ValueLayout$OfFloat
            ValueLayout$OfShort]
           [java.lang.invoke MethodHandle MethodHandleProxies MethodHandles MethodType]
           [java.lang.reflect Field]
           [java.nio.file Paths]
           [sun.misc Unsafe]))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; Typed ValueLayout constants — critical for avoiding reflection on .set/.get
;; ---------------------------------------------------------------------------

;; UNALIGNED variants skip the alignment check on every access (~5ns saved per call).
;; Our reads/writes against DuckDB native memory are always aligned in practice, but
;; the JIT can't prove that, so we use UNALIGNED to skip the runtime check.
(def ^ValueLayout$OfLong   VL-LONG   ValueLayout/JAVA_LONG_UNALIGNED)
(def ^ValueLayout$OfDouble VL-DOUBLE ValueLayout/JAVA_DOUBLE_UNALIGNED)
(def ^ValueLayout$OfFloat  VL-FLOAT  ValueLayout/JAVA_FLOAT_UNALIGNED)
(def ^ValueLayout$OfInt    VL-INT    ValueLayout/JAVA_INT_UNALIGNED)
(def ^ValueLayout$OfShort  VL-SHORT  ValueLayout/JAVA_SHORT_UNALIGNED)
(def ^ValueLayout$OfByte   VL-BYTE   ValueLayout/JAVA_BYTE)
(def ^AddressLayout        VL-ADDR   ValueLayout/ADDRESS)

;; ---------------------------------------------------------------------------
;; sun.misc.Unsafe — used for hot read/write paths on raw native memory we own.
;; Panama's MemorySegment.get performs bounds + scope checks per call which the
;; JIT can't fully hoist on JDK 21. For pointers DuckDB has already validated,
;; raw Unsafe.getLong/putLong is ~2× faster.
;;
;; Panama is still used for all FFI calls (downcall handles, library lookup,
;; struct allocation). Unsafe only touches memory we've already obtained.
;; ---------------------------------------------------------------------------

(def ^Unsafe UNSAFE
  (let [^Field f (.getDeclaredField Unsafe "theUnsafe")]
    (.setAccessible f true)
    (.get f nil)))

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
;; FFI invocation strategy — MethodHandleProxies + per-signature SAMs
;;
;; Calling Panama MethodHandles via `.invokeWithArguments` is *slow* — it
;; boxes args into a `List`, forces a per-call `Arena.ofConfined` allocation
;; inside the downcall stub, and fragments the C heap so DuckDB's own mallocs
;; slow down.  Measured: ~106 ns/call.
;;
;; Clojure 1.12 does NOT compile `(.invokeExact ^MethodHandle mh arg)` as a
;; signature-polymorphic call — it falls back to varargs Object[], yielding
;; the same `ClassCastException` you'd see from a stale descriptor.
;;
;; The workaround: wrap each MethodHandle as a typed SAM via
;; `MethodHandleProxies.asInterfaceInstance(IFace.class, mh)`, then call
;; `IFace.apply(...)` directly — a normal interface dispatch with no boxing,
;; no varargs, no per-call arena.  Measured: ~16 ns/call (~7× faster).
;;
;; One `definterface` is auto-generated for each unique (return-type, arg-types)
;; signature in the FFI spec table below.  Interface names follow a
;; Java-descriptor convention: `MhM_MJ` is `(MemorySegment, long) -> MemorySegment`.
;; ---------------------------------------------------------------------------

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

(def ^:private type-info
  "Per-FFI-type info used to build SAM interfaces + wrapper code.

  Note on widening: Clojure's `definterface` (and primitive fn args/returns)
  only support `long`, `double`, and Object — `int`/`byte`/`short`/`float`
  are *not* allowed at the JVM bytecode level for Clojure-generated methods.
  We therefore widen narrow primitives to `long` (or `double` for float)
  in the SAM interface signature, and rely on `MethodHandleProxies.asType`
  (called internally by `asInterfaceInstance`) to narrow them back to the
  MethodHandle's actual primitive type at call time.

  :char    Java descriptor letter used to name the SAM interface.  All
           narrow integer types collapse to `J` and all float types to `D`
           so that signatures sharing the same widened Java method type
           share one interface (e.g. `bind_int8` and `bind_int64` both
           use `MhJ_MJJ`).
  :tag     Clojure type hint attached to fn args / return / deref'd proxy.
  :coerce  Form transformer for the call site — widens a wrapper param
           to the SAM's expected primitive type."
  {:addr   {:char "M" :tag 'java.lang.foreign.MemorySegment :coerce identity}
   :result {:char "M" :tag 'java.lang.foreign.MemorySegment :coerce identity}
   :int    {:char "J" :tag 'long   :coerce (fn [x] (list 'long x))}
   :long   {:char "J" :tag 'long   :coerce (fn [x] (list 'long x))}
   :byte   {:char "J" :tag 'long   :coerce (fn [x] (list 'long x))}
   :short  {:char "J" :tag 'long   :coerce (fn [x] (list 'long x))}
   :float  {:char "D" :tag 'double :coerce (fn [x] (list 'double x))}
   :double {:char "D" :tag 'double :coerce (fn [x] (list 'double x))}
   :void   {:char "V" :tag 'void   :coerce identity}})

(defn- iface-name
  "Build the SAM interface symbol for the signature.
  `(M)->M`            -> `MhM_M`
  `(M, J) -> long`    -> `MhJ_MJ`
  `() -> M`           -> `MhM_0`"
  [ret args]
  (symbol (str "Mh" (:char (type-info ret)) "_"
               (if (empty? args)
                 "0"
                 (apply str (map #(:char (type-info %)) args))))))

(defmacro ^:private define-ffi-fns!
  "From a spec vector of `[c-name return-type [arg-types...] coerce]` entries,
  generates for each entry:

  1. A SAM interface `MhX_YZ` (one shared per unique signature) so the
     MethodHandle can be invoked via a direct interface call.
  2. `(defonce ^:private -cname (atom nil))` — slot for the proxy instance.
  3. Code in `define-datatypes!` that creates the MethodHandle, wraps it as
     a SAM proxy, and stores it in the atom.
  4. A typed wrapper `(defn cname [args...] (.apply ^MhX_YZ @-cname args...))`
     with appropriate primitive coercions."
  [specs]
  (let [;; Unique (ret, args) signatures — one interface per signature
        unique-sigs (->> specs
                         (map (fn [[_ ret args _]] [ret args]))
                         distinct)
        iface-forms
        (for [[ret args] unique-sigs]
          (let [iname (iface-name ret args)
                params (mapv (fn [i arg-kw]
                               (with-meta (symbol (str "a" i))
                                 {:tag (:tag (type-info arg-kw))}))
                             (range (count args))
                             args)]
            `(definterface ~iname
               (~(with-meta 'apply {:tag (:tag (type-info ret))})
                ~params))))
        atom-defs
        (for [[cname _ _ _] specs]
          `(defonce ^:private ~(symbol (str "-" cname)) (atom nil)))
        ;; Map FFI type → java.lang.Class form for building a `MethodType`.
        ;; Widened primitives (int/byte/short → long, float → double) match
        ;; the widened SAM signatures we generate below.
        type-class (fn [t]
                     (case t
                       (:addr :result) 'java.lang.foreign.MemorySegment
                       (:int :long :byte :short) 'Long/TYPE
                       (:float :double) 'Double/TYPE
                       :void 'Void/TYPE))
        reset-forms
        (for [[cname ret args _] specs]
          (let [aname (symbol (str "-" cname))
                ret-l (resolve-layout ret)
                arg-ls (mapv resolve-layout args)
                iname (iface-name ret args)
                ret-cls (type-class ret)
                arg-clses (mapv type-class args)
                desc-form (if (= ret :void)
                            `(FunctionDescriptor/ofVoid (into-array MemoryLayout ~arg-ls))
                            `(FunctionDescriptor/of ~ret-l (into-array MemoryLayout ~arg-ls)))]
            ;; Build the raw MethodHandle, adapt its type to match the (widened)
            ;; SAM signature via `explicitCastArguments` (which permits
            ;; long→int narrowing that `asType` rejects), then wrap as a SAM
            ;; proxy and store in the atom.
            `(let [mh#       (fn-handle ~cname ~desc-form)
                   arg-arr#  ^"[Ljava.lang.Class;" (into-array Class [~@arg-clses])
                   sam-type# (MethodType/methodType ~(with-meta ret-cls {:tag 'java.lang.Class}) arg-arr#)
                   adapted#  (MethodHandles/explicitCastArguments mh# sam-type#)
                   proxy#    (MethodHandleProxies/asInterfaceInstance ~iname adapted#)]
               (reset! ~aname proxy#))))
        wrapper-fns
        (for [[cname ret args coerce] specs]
          (let [fname  (symbol cname)
                aname  (symbol (str "-" cname))
                iname  (iface-name ret args)
                hinted-params
                (mapv (fn [i arg-kw]
                        (with-meta (symbol (str "a" i))
                          {:tag (:tag (type-info arg-kw))}))
                      (range (count args))
                      args)
                coerced-args (mapv (fn [p arg-kw] ((:coerce (type-info arg-kw)) p))
                                   hinted-params args)
                call `(.apply ~(with-meta `(deref ~aname) {:tag iname})
                              ~@coerced-args)
                with-coerce (case coerce
                              :int  `(int ~call)
                              :long `(long ~call)
                              call)
                ;; Only put a fn return tag on MemorySegment-returning fns —
                ;; for primitive returns the `with-coerce` form already
                ;; produces the right boxed type via `(int ...)` / `(long ...)`.
                fn-meta (case ret
                          (:addr :result) {:tag 'java.lang.foreign.MemorySegment}
                          nil)]
            (if fn-meta
              `(defn ~(with-meta fname fn-meta) ~hinted-params ~with-coerce)
              `(defn ~fname ~hinted-params ~with-coerce))))]
    `(do
       ;; All interfaces first — must compile before any code referencing them
       ~@iface-forms
       ;; One atom per FFI fn to hold its SAM proxy
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
   ["duckdb_appender_flush"     :int  [:addr]                    :int]
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
