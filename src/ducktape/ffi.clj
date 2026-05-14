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
;; 1. Typed ValueLayout constants — CRITICAL for avoiding reflection on .set/.get
;; ---------------------------------------------------------------------------

(def ^ValueLayout$OfLong   VL-LONG   ValueLayout/JAVA_LONG)
(def ^ValueLayout$OfDouble VL-DOUBLE ValueLayout/JAVA_DOUBLE)
(def ^ValueLayout$OfFloat  VL-FLOAT  ValueLayout/JAVA_FLOAT)
(def ^ValueLayout$OfInt    VL-INT    ValueLayout/JAVA_INT)
(def ^ValueLayout$OfShort  VL-SHORT  ValueLayout/JAVA_SHORT)
(def ^ValueLayout$OfByte   VL-BYTE   ValueLayout/JAVA_BYTE)
(def ^AddressLayout        VL-ADDR   ValueLayout/ADDRESS)

;; ---------------------------------------------------------------------------
;; 2. Library loading
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
;; 3. duckdb_result struct layout — 6 × JAVA_LONG (48 bytes on 64-bit)
;; ---------------------------------------------------------------------------

(def result-layout
  (MemoryLayout/structLayout
    (into-array MemoryLayout (repeatedly 6 (constantly ValueLayout/JAVA_LONG)))))

;; ---------------------------------------------------------------------------
;; 4. MethodHandle invocation helper
;; ---------------------------------------------------------------------------

(defn mh-invoke [^MethodHandle mh & args]
  (.invokeWithArguments mh ^java.util.List (java.util.Arrays/asList (object-array args))))

;; ---------------------------------------------------------------------------
;; 5. DuckDB type enum constants
;; ---------------------------------------------------------------------------

(def ^:const DUCKDB_TYPE_INVALID    0)
(def ^:const DUCKDB_TYPE_BOOLEAN    1)
(def ^:const DUCKDB_TYPE_TINYINT    2)
(def ^:const DUCKDB_TYPE_SMALLINT   3)
(def ^:const DUCKDB_TYPE_INTEGER    4)
(def ^:const DUCKDB_TYPE_BIGINT     5)
(def ^:const DUCKDB_TYPE_UTINYINT   6)
(def ^:const DUCKDB_TYPE_USMALLINT  7)
(def ^:const DUCKDB_TYPE_UINTEGER   8)
(def ^:const DUCKDB_TYPE_UBIGINT    9)
(def ^:const DUCKDB_TYPE_FLOAT     10)
(def ^:const DUCKDB_TYPE_DOUBLE    11)
(def ^:const DUCKDB_TYPE_TIMESTAMP 12)
(def ^:const DUCKDB_TYPE_DATE      13)
(def ^:const DUCKDB_TYPE_TIME      14)
(def ^:const DUCKDB_TYPE_HUGEINT   16)
(def ^:const DUCKDB_TYPE_VARCHAR   17)
(def ^:const DUCKDB_TYPE_UUID      27)
(def ^:const DuckDBSuccess          0)

(def duckdb-type-map
  {0  :DUCKDB_TYPE_INVALID
   1  :DUCKDB_TYPE_BOOLEAN
   2  :DUCKDB_TYPE_TINYINT
   3  :DUCKDB_TYPE_SMALLINT
   4  :DUCKDB_TYPE_INTEGER
   5  :DUCKDB_TYPE_BIGINT
   6  :DUCKDB_TYPE_UTINYINT
   7  :DUCKDB_TYPE_USMALLINT
   8  :DUCKDB_TYPE_UINTEGER
   9  :DUCKDB_TYPE_UBIGINT
   10 :DUCKDB_TYPE_FLOAT
   11 :DUCKDB_TYPE_DOUBLE
   12 :DUCKDB_TYPE_TIMESTAMP
   13 :DUCKDB_TYPE_DATE
   14 :DUCKDB_TYPE_TIME
   16 :DUCKDB_TYPE_HUGEINT
   17 :DUCKDB_TYPE_VARCHAR
   27 :DUCKDB_TYPE_UUID})

;; ---------------------------------------------------------------------------
;; 6. All function handle atoms
;; ---------------------------------------------------------------------------

;; Database lifecycle
(defonce ^:private -duckdb_open_ext (atom nil))
(defonce ^:private -duckdb_close (atom nil))
(defonce ^:private -duckdb_connect (atom nil))
(defonce ^:private -duckdb_disconnect (atom nil))
(defonce ^:private -duckdb_library_version (atom nil))
(defonce ^:private -duckdb_free (atom nil))

;; Config
(defonce ^:private -duckdb_config_count (atom nil))
(defonce ^:private -duckdb_create_config (atom nil))
(defonce ^:private -duckdb_get_config_flag (atom nil))
(defonce ^:private -duckdb_set_config (atom nil))
(defonce ^:private -duckdb_destroy_config (atom nil))

;; Query execution
(defonce ^:private -duckdb_query (atom nil))
(defonce ^:private -duckdb_destroy_result (atom nil))
(defonce ^:private -duckdb_result_error (atom nil))

;; Result metadata
(defonce ^:private -duckdb_column_count (atom nil))
(defonce ^:private -duckdb_column_name (atom nil))
(defonce ^:private -duckdb_column_logical_type (atom nil))
(defonce ^:private -duckdb_get_type_id (atom nil))
(defonce ^:private -duckdb_destroy_logical_type (atom nil))

;; Chunk fetch
(defonce ^:private -duckdb_fetch_chunk (atom nil))
(defonce ^:private -duckdb_data_chunk_get_size (atom nil))
(defonce ^:private -duckdb_data_chunk_get_vector (atom nil))
(defonce ^:private -duckdb_vector_get_data (atom nil))
(defonce ^:private -duckdb_vector_get_validity (atom nil))
(defonce ^:private -duckdb_destroy_data_chunk (atom nil))

;; Chunk write (appender)
(defonce ^:private -duckdb_appender_create (atom nil))
(defonce ^:private -duckdb_appender_destroy (atom nil))
(defonce ^:private -duckdb_appender_error (atom nil))
(defonce ^:private -duckdb_append_data_chunk (atom nil))
(defonce ^:private -duckdb_vector_size (atom nil))
(defonce ^:private -duckdb_create_data_chunk (atom nil))
(defonce ^:private -duckdb_data_chunk_set_size (atom nil))
(defonce ^:private -duckdb_data_chunk_reset (atom nil))
(defonce ^:private -duckdb_create_logical_type (atom nil))
(defonce ^:private -duckdb_vector_ensure_validity_writable (atom nil))

;; Prepared statements
(defonce ^:private -duckdb_prepare (atom nil))
(defonce ^:private -duckdb_destroy_prepare (atom nil))
(defonce ^:private -duckdb_prepare_error (atom nil))
(defonce ^:private -duckdb_nparams (atom nil))
(defonce ^:private -duckdb_param_type (atom nil))
(defonce ^:private -duckdb_pending_prepared (atom nil))
(defonce ^:private -duckdb_execute_pending (atom nil))
(defonce ^:private -duckdb_destroy_pending (atom nil))
(defonce ^:private -duckdb_pending_error (atom nil))

;; Bind params
(defonce ^:private -duckdb_bind_null (atom nil))
(defonce ^:private -duckdb_bind_boolean (atom nil))
(defonce ^:private -duckdb_bind_int8 (atom nil))
(defonce ^:private -duckdb_bind_int16 (atom nil))
(defonce ^:private -duckdb_bind_int32 (atom nil))
(defonce ^:private -duckdb_bind_int64 (atom nil))
(defonce ^:private -duckdb_bind_uint8 (atom nil))
(defonce ^:private -duckdb_bind_uint16 (atom nil))
(defonce ^:private -duckdb_bind_uint32 (atom nil))
(defonce ^:private -duckdb_bind_uint64 (atom nil))
(defonce ^:private -duckdb_bind_float (atom nil))
(defonce ^:private -duckdb_bind_double (atom nil))
(defonce ^:private -duckdb_bind_date (atom nil))
(defonce ^:private -duckdb_bind_time (atom nil))
(defonce ^:private -duckdb_bind_timestamp (atom nil))
(defonce ^:private -duckdb_bind_varchar_length (atom nil))

;; ---------------------------------------------------------------------------
;; define-datatypes! — initialize library + all function handles
;; ---------------------------------------------------------------------------

(defn define-datatypes! [^String duckdb-home]
  (let [lib (load-lib duckdb-home)
        linker (Linker/nativeLinker)]
    (alter-var-root #'the-lib (constantly lib))
    (alter-var-root #'the-linker (constantly linker))

    ;; -- Database lifecycle --------------------------------------------------
    (reset! -duckdb_open_ext
      (fn-handle "duckdb_open_ext"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR VL-ADDR VL-ADDR VL-ADDR]))))

    (reset! -duckdb_close
      (fn-handle "duckdb_close"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_connect
      (fn-handle "duckdb_connect"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR VL-ADDR]))))

    (reset! -duckdb_disconnect
      (fn-handle "duckdb_disconnect"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_library_version
      (fn-handle "duckdb_library_version"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout []))))

    (reset! -duckdb_free
      (fn-handle "duckdb_free"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    ;; -- Config --------------------------------------------------------------
    (reset! -duckdb_config_count
      (fn-handle "duckdb_config_count"
        (FunctionDescriptor/of ValueLayout/JAVA_LONG
          (into-array MemoryLayout []))))

    (reset! -duckdb_create_config
      (fn-handle "duckdb_create_config"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_get_config_flag
      (fn-handle "duckdb_get_config_flag"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [ValueLayout/JAVA_LONG VL-ADDR VL-ADDR]))))

    (reset! -duckdb_set_config
      (fn-handle "duckdb_set_config"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR VL-ADDR VL-ADDR]))))

    (reset! -duckdb_destroy_config
      (fn-handle "duckdb_destroy_config"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    ;; -- Query execution -----------------------------------------------------
    (reset! -duckdb_query
      (fn-handle "duckdb_query"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR VL-ADDR VL-ADDR]))))

    (reset! -duckdb_destroy_result
      (fn-handle "duckdb_destroy_result"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_result_error
      (fn-handle "duckdb_result_error"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [VL-ADDR]))))

    ;; -- Result metadata -----------------------------------------------------
    (reset! -duckdb_column_count
      (fn-handle "duckdb_column_count"
        (FunctionDescriptor/of ValueLayout/JAVA_LONG
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_column_name
      (fn-handle "duckdb_column_name"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_column_logical_type
      (fn-handle "duckdb_column_logical_type"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_get_type_id
      (fn-handle "duckdb_get_type_id"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_destroy_logical_type
      (fn-handle "duckdb_destroy_logical_type"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    ;; -- Chunk fetch ---------------------------------------------------------
    (reset! -duckdb_fetch_chunk
      (fn-handle "duckdb_fetch_chunk"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [result-layout]))))

    (reset! -duckdb_data_chunk_get_size
      (fn-handle "duckdb_data_chunk_get_size"
        (FunctionDescriptor/of ValueLayout/JAVA_LONG
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_data_chunk_get_vector
      (fn-handle "duckdb_data_chunk_get_vector"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_vector_get_data
      (fn-handle "duckdb_vector_get_data"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_vector_get_validity
      (fn-handle "duckdb_vector_get_validity"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_destroy_data_chunk
      (fn-handle "duckdb_destroy_data_chunk"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    ;; -- Chunk write (appender) ----------------------------------------------
    (reset! -duckdb_appender_create
      (fn-handle "duckdb_appender_create"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR VL-ADDR VL-ADDR VL-ADDR]))))

    (reset! -duckdb_appender_destroy
      (fn-handle "duckdb_appender_destroy"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_appender_error
      (fn-handle "duckdb_appender_error"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_append_data_chunk
      (fn-handle "duckdb_append_data_chunk"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR VL-ADDR]))))

    (reset! -duckdb_vector_size
      (fn-handle "duckdb_vector_size"
        (FunctionDescriptor/of ValueLayout/JAVA_LONG
          (into-array MemoryLayout []))))

    (reset! -duckdb_create_data_chunk
      (fn-handle "duckdb_create_data_chunk"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_data_chunk_set_size
      (fn-handle "duckdb_data_chunk_set_size"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_data_chunk_reset
      (fn-handle "duckdb_data_chunk_reset"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_create_logical_type
      (fn-handle "duckdb_create_logical_type"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [ValueLayout/JAVA_INT]))))

    (reset! -duckdb_vector_ensure_validity_writable
      (fn-handle "duckdb_vector_ensure_validity_writable"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    ;; -- Prepared statements -------------------------------------------------
    (reset! -duckdb_prepare
      (fn-handle "duckdb_prepare"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR VL-ADDR VL-ADDR]))))

    (reset! -duckdb_destroy_prepare
      (fn-handle "duckdb_destroy_prepare"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_prepare_error
      (fn-handle "duckdb_prepare_error"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_nparams
      (fn-handle "duckdb_nparams"
        (FunctionDescriptor/of ValueLayout/JAVA_LONG
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_param_type
      (fn-handle "duckdb_param_type"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_pending_prepared
      (fn-handle "duckdb_pending_prepared"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR VL-ADDR]))))

    (reset! -duckdb_execute_pending
      (fn-handle "duckdb_execute_pending"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR VL-ADDR]))))

    (reset! -duckdb_destroy_pending
      (fn-handle "duckdb_destroy_pending"
        (FunctionDescriptor/ofVoid
          (into-array MemoryLayout [VL-ADDR]))))

    (reset! -duckdb_pending_error
      (fn-handle "duckdb_pending_error"
        (FunctionDescriptor/of VL-ADDR
          (into-array MemoryLayout [VL-ADDR]))))

    ;; -- Bind params ---------------------------------------------------------
    (reset! -duckdb_bind_null
      (fn-handle "duckdb_bind_null"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_bind_boolean
      (fn-handle "duckdb_bind_boolean"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_BYTE]))))

    (reset! -duckdb_bind_int8
      (fn-handle "duckdb_bind_int8"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_BYTE]))))

    (reset! -duckdb_bind_int16
      (fn-handle "duckdb_bind_int16"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_SHORT]))))

    (reset! -duckdb_bind_int32
      (fn-handle "duckdb_bind_int32"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_INT]))))

    (reset! -duckdb_bind_int64
      (fn-handle "duckdb_bind_int64"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_bind_uint8
      (fn-handle "duckdb_bind_uint8"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_BYTE]))))

    (reset! -duckdb_bind_uint16
      (fn-handle "duckdb_bind_uint16"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_SHORT]))))

    (reset! -duckdb_bind_uint32
      (fn-handle "duckdb_bind_uint32"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_INT]))))

    (reset! -duckdb_bind_uint64
      (fn-handle "duckdb_bind_uint64"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_bind_float
      (fn-handle "duckdb_bind_float"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_FLOAT]))))

    (reset! -duckdb_bind_double
      (fn-handle "duckdb_bind_double"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_DOUBLE]))))

    (reset! -duckdb_bind_date
      (fn-handle "duckdb_bind_date"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_INT]))))

    (reset! -duckdb_bind_time
      (fn-handle "duckdb_bind_time"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_bind_timestamp
      (fn-handle "duckdb_bind_timestamp"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG ValueLayout/JAVA_LONG]))))

    (reset! -duckdb_bind_varchar_length
      (fn-handle "duckdb_bind_varchar_length"
        (FunctionDescriptor/of ValueLayout/JAVA_INT
          (into-array MemoryLayout [VL-ADDR ValueLayout/JAVA_LONG VL-ADDR ValueLayout/JAVA_LONG]))))))

;; ---------------------------------------------------------------------------
;; 7. Helper functions
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
    (.getString (.reinterpret ptr Long/MAX_VALUE) 0)))

;; ---------------------------------------------------------------------------
;; 8. Public API wrappers
;; ---------------------------------------------------------------------------

;; -- Database lifecycle ------------------------------------------------------

(defn duckdb_library_version []
  (mh-invoke @-duckdb_library_version))

(defn duckdb_open_ext [path out-db config out-error]
  (int (mh-invoke @-duckdb_open_ext path out-db config out-error)))

(defn duckdb_close [db-ptr]
  (mh-invoke @-duckdb_close db-ptr))

(defn duckdb_connect [db out-conn]
  (int (mh-invoke @-duckdb_connect db out-conn)))

(defn duckdb_disconnect [conn-ptr]
  (mh-invoke @-duckdb_disconnect conn-ptr))

(defn duckdb_free [ptr]
  (mh-invoke @-duckdb_free ptr))

;; -- Config ------------------------------------------------------------------

(defn duckdb_config_count []
  (long (mh-invoke @-duckdb_config_count)))

(defn duckdb_create_config [out-config]
  (int (mh-invoke @-duckdb_create_config out-config)))

(defn duckdb_get_config_flag [idx out-name out-desc]
  (int (mh-invoke @-duckdb_get_config_flag idx out-name out-desc)))

(defn duckdb_set_config [config name option]
  (int (mh-invoke @-duckdb_set_config config name option)))

(defn duckdb_destroy_config [config-ptr]
  (mh-invoke @-duckdb_destroy_config config-ptr))

;; -- Query execution ---------------------------------------------------------

(defn duckdb_query [conn sql out-result]
  (int (mh-invoke @-duckdb_query conn sql out-result)))

(defn duckdb_destroy_result [result-ptr]
  (mh-invoke @-duckdb_destroy_result result-ptr))

(defn duckdb_result_error [result-ptr]
  (mh-invoke @-duckdb_result_error result-ptr))

;; -- Result metadata ---------------------------------------------------------

(defn duckdb_column_count [result-ptr]
  (long (mh-invoke @-duckdb_column_count result-ptr)))

(defn duckdb_column_name [result-ptr col]
  (mh-invoke @-duckdb_column_name result-ptr col))

(defn duckdb_column_logical_type [result-ptr col]
  (mh-invoke @-duckdb_column_logical_type result-ptr col))

(defn duckdb_get_type_id [logical-type]
  (int (mh-invoke @-duckdb_get_type_id logical-type)))

(defn duckdb_destroy_logical_type [type-ptr]
  (mh-invoke @-duckdb_destroy_logical_type type-ptr))

;; -- Chunk fetch -------------------------------------------------------------

(defn duckdb_fetch_chunk [result-seg]
  (mh-invoke @-duckdb_fetch_chunk result-seg))

(defn duckdb_data_chunk_get_size [chunk]
  (long (mh-invoke @-duckdb_data_chunk_get_size chunk)))

(defn duckdb_data_chunk_get_vector [chunk col]
  (mh-invoke @-duckdb_data_chunk_get_vector chunk col))

(defn duckdb_vector_get_data [vec]
  (mh-invoke @-duckdb_vector_get_data vec))

(defn duckdb_vector_get_validity [vec]
  (mh-invoke @-duckdb_vector_get_validity vec))

(defn duckdb_destroy_data_chunk [chunk-ptr]
  (mh-invoke @-duckdb_destroy_data_chunk chunk-ptr))

;; -- Chunk write (appender) --------------------------------------------------

(defn duckdb_appender_create [conn schema table out-appender]
  (int (mh-invoke @-duckdb_appender_create conn schema table out-appender)))

(defn duckdb_appender_destroy [appender-ptr]
  (int (mh-invoke @-duckdb_appender_destroy appender-ptr)))

(defn duckdb_appender_error [appender]
  (mh-invoke @-duckdb_appender_error appender))

(defn duckdb_append_data_chunk [appender chunk]
  (int (mh-invoke @-duckdb_append_data_chunk appender chunk)))

(defn duckdb_vector_size []
  (long (mh-invoke @-duckdb_vector_size)))

(defn duckdb_create_data_chunk [types n-cols]
  (mh-invoke @-duckdb_create_data_chunk types n-cols))

(defn duckdb_data_chunk_set_size [chunk size]
  (mh-invoke @-duckdb_data_chunk_set_size chunk size))

(defn duckdb_data_chunk_reset [chunk]
  (mh-invoke @-duckdb_data_chunk_reset chunk))

(defn duckdb_create_logical_type [type-id]
  (mh-invoke @-duckdb_create_logical_type type-id))

(defn duckdb_vector_ensure_validity_writable [vec]
  (mh-invoke @-duckdb_vector_ensure_validity_writable vec))

;; -- Prepared statements -----------------------------------------------------

(defn duckdb_prepare [conn sql out-stmt]
  (int (mh-invoke @-duckdb_prepare conn sql out-stmt)))

(defn duckdb_destroy_prepare [stmt-ptr]
  (mh-invoke @-duckdb_destroy_prepare stmt-ptr))

(defn duckdb_prepare_error [stmt]
  (mh-invoke @-duckdb_prepare_error stmt))

(defn duckdb_nparams [stmt]
  (long (mh-invoke @-duckdb_nparams stmt)))

(defn duckdb_param_type [stmt idx]
  (int (mh-invoke @-duckdb_param_type stmt idx)))

(defn duckdb_pending_prepared [stmt out-pending]
  (int (mh-invoke @-duckdb_pending_prepared stmt out-pending)))

(defn duckdb_execute_pending [pending out-result]
  (int (mh-invoke @-duckdb_execute_pending pending out-result)))

(defn duckdb_destroy_pending [pending-ptr]
  (mh-invoke @-duckdb_destroy_pending pending-ptr))

(defn duckdb_pending_error [pending]
  (mh-invoke @-duckdb_pending_error pending))

;; -- Bind params -------------------------------------------------------------

(defn duckdb_bind_null [stmt idx]
  (int (mh-invoke @-duckdb_bind_null stmt idx)))

(defn duckdb_bind_boolean [stmt idx val]
  (int (mh-invoke @-duckdb_bind_boolean stmt idx val)))

(defn duckdb_bind_int8 [stmt idx val]
  (int (mh-invoke @-duckdb_bind_int8 stmt idx val)))

(defn duckdb_bind_int16 [stmt idx val]
  (int (mh-invoke @-duckdb_bind_int16 stmt idx val)))

(defn duckdb_bind_int32 [stmt idx val]
  (int (mh-invoke @-duckdb_bind_int32 stmt idx val)))

(defn duckdb_bind_int64 [stmt idx val]
  (int (mh-invoke @-duckdb_bind_int64 stmt idx val)))

(defn duckdb_bind_uint8 [stmt idx val]
  (int (mh-invoke @-duckdb_bind_uint8 stmt idx val)))

(defn duckdb_bind_uint16 [stmt idx val]
  (int (mh-invoke @-duckdb_bind_uint16 stmt idx val)))

(defn duckdb_bind_uint32 [stmt idx val]
  (int (mh-invoke @-duckdb_bind_uint32 stmt idx val)))

(defn duckdb_bind_uint64 [stmt idx val]
  (int (mh-invoke @-duckdb_bind_uint64 stmt idx val)))

(defn duckdb_bind_float [stmt idx val]
  (int (mh-invoke @-duckdb_bind_float stmt idx val)))

(defn duckdb_bind_double [stmt idx val]
  (int (mh-invoke @-duckdb_bind_double stmt idx val)))

(defn duckdb_bind_date [stmt idx val]
  (int (mh-invoke @-duckdb_bind_date stmt idx val)))

(defn duckdb_bind_time [stmt idx val]
  (int (mh-invoke @-duckdb_bind_time stmt idx val)))

(defn duckdb_bind_timestamp [stmt idx val]
  (int (mh-invoke @-duckdb_bind_timestamp stmt idx val)))

(defn duckdb_bind_varchar_length [stmt idx val len]
  (int (mh-invoke @-duckdb_bind_varchar_length stmt idx val len)))
