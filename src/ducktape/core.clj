(ns ducktape.core
  (:require [ducktape.ffi :as ffi]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype :as dt]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.sql :as sql]
            [ham-fisted.api :as hamf]
            [clojure.tools.logging :as log])
  (:import [java.lang.foreign Arena MemoryLayout MemorySegment]
           [java.math BigInteger BigDecimal]
           [java.util UUID]
           [java.util.concurrent.atomic LongAdder]
           [java.util.function Supplier]
           [java.time LocalDate LocalTime Instant]
           [ham_fisted ITypedReduce IFnDef]
           [tech.v3.datatype ObjectReader]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang Seqable]
           [tech.v3.dataset.impl.column Column]
           [tech.v3.dataset.impl.dataset Dataset]
           [java.lang AutoCloseable]))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; SQL datatype mappings for DuckDB (register at load time)
;; ---------------------------------------------------------------------------

(sql/set-datatype-mapping! "duckdb" :boolean "bool" -7
                           sql/generic-sql->column sql/generic-column->sql)
(sql/set-datatype-mapping! "duckdb" :string "varchar" 12
                           sql/generic-sql->column sql/generic-column->sql)
(sql/set-datatype-mapping! "duckdb" :uuid "UUID" 1111
                           sql/generic-sql->column sql/generic-column->sql)

;; ---------------------------------------------------------------------------
;; 1. Initialization
;; ---------------------------------------------------------------------------

(defonce ^:private initialize* (atom false))

(defn initialized? [] @initialize*)

(defn initialize!
  "Initialize the duckdb ffi system. Must be called first."
  ([{:keys [duckdb-home]}]
   (swap! initialize*
          (fn [is-init?]
            (when-not is-init?
              (let [duckdb-home (or duckdb-home
                                    (System/getenv "DUCKDB_HOME")
                                    "")]
                (log/infof "Attempting to load duckdb from \"%s\"" duckdb-home)
                (ffi/define-datatypes! duckdb-home)))
       ;; Check version
            (let [ver (ffi/read-c-str (ffi/duckdb_library_version))
                  [_ major] (re-find #"^v?(\d+)" ver)
                  major (when major (Long/parseLong major))]
              (when (and major (< major 1))
                (throw (RuntimeException. (str "Unsupported DuckDB version: " ver)))))
            true)))
  ([] (initialize! nil)))

;; ---------------------------------------------------------------------------
;; 2. open-db
;; ---------------------------------------------------------------------------

(defn open-db
  "Open a database. path may be nil for in-memory."
  (^long [^String path config-options]
   (with-open [arena (Arena/ofConfined)]
     (let [path (or path "")
           config-ptr (when-not (empty? config-options)
                        (let [cp (.allocate arena ffi/VL-ADDR)]
                          (ffi/duckdb_create_config cp)
                          (let [cfg (.get cp ffi/VL-ADDR 0)]
                            (doseq [[k v] config-options]
                              (ffi/duckdb_set_config cfg
                                                     (ffi/alloc-c-str arena (str k))
                                                     (ffi/alloc-c-str arena (str v))))
                            cfg)))
           db-ptr (.allocate arena ffi/VL-ADDR)
           err-ptr (.allocate arena ffi/VL-ADDR)
           rc (ffi/duckdb_open_ext
               (ffi/alloc-c-str arena path)
               db-ptr
               (if config-ptr config-ptr MemorySegment/NULL)
               err-ptr)]
       ;; Destroy config if we created one
       (when config-ptr
         (ffi/destroy-ptr! ffi/duckdb_destroy_config config-ptr))
       (when-not (== rc ffi/DuckDBSuccess)
         (let [err-seg (.get err-ptr ffi/VL-ADDR 0)
               err-str (ffi/read-c-str err-seg)]
           (when err-seg (ffi/duckdb_free err-seg))
           (throw (Exception. (format "Error opening database: %s" err-str)))))
       (.address (.get db-ptr ffi/VL-ADDR 0)))))
  (^long [^String path] (open-db path nil))
  (^long [] (open-db "")))

;; ---------------------------------------------------------------------------
;; 3. close-db
;; ---------------------------------------------------------------------------

(defn close-db [^long db]
  (with-open [arena (Arena/ofConfined)]
    (let [p (.allocate arena ffi/VL-ADDR)]
      (.set p ffi/VL-ADDR 0 (MemorySegment/ofAddress db))
      (ffi/duckdb_close p))))

;; ---------------------------------------------------------------------------
;; 4. connect / disconnect
;; ---------------------------------------------------------------------------

(defn connect ^long [^long db]
  (with-open [arena (Arena/ofConfined)]
    (let [p (.allocate arena ffi/VL-ADDR)]
      (ffi/duckdb_connect (MemorySegment/ofAddress db) p)
      (.address (.get p ffi/VL-ADDR 0)))))

(defn disconnect [^long conn]
  (with-open [arena (Arena/ofConfined)]
    (let [p (.allocate arena ffi/VL-ADDR)]
      (.set p ffi/VL-ADDR 0 (MemorySegment/ofAddress conn))
      (ffi/duckdb_disconnect p))))

;; ---------------------------------------------------------------------------
;; 5. run-query!
;; ---------------------------------------------------------------------------

(defn run-query!
  "Execute a SQL statement, ignoring results. Used for DDL."
  [^long conn ^String sql]
  (with-open [arena (Arena/ofConfined)]
    (let [result (.allocate arena (.byteSize ^MemoryLayout ffi/result-layout))
          rc (ffi/duckdb_query
              (MemorySegment/ofAddress conn)
              (ffi/alloc-c-str arena sql)
              result)]
      (when-not (== rc ffi/DuckDBSuccess)
        (let [err (ffi/read-c-str ^MemorySegment (ffi/duckdb_result_error result))]
          (ffi/duckdb_destroy_result result)
          (throw (Exception. err))))
      (ffi/duckdb_destroy_result result)
      :ok)))

;; ---------------------------------------------------------------------------
;; 6. create-table! / drop-table!
;; ---------------------------------------------------------------------------

(defn create-table!
  ([conn dataset options]
   (let [sql-str (sql/create-sql "duckdb" dataset)]
     (run-query! conn sql-str)
     (sql/table-name dataset options)))
  ([conn dataset] (create-table! conn dataset nil)))

(defn drop-table! [conn dataset]
  (let [ds-name (sql/table-name dataset)]
    (run-query! conn (format "drop table %s" ds-name))
    ds-name))

;; ---------------------------------------------------------------------------
;; 7. insert-dataset! — write hot path
;; ---------------------------------------------------------------------------

(def ^:private dtype->duckdb-type
  {:boolean    ffi/DUCKDB_TYPE_BOOLEAN
   :int8       ffi/DUCKDB_TYPE_TINYINT
   :uint8      ffi/DUCKDB_TYPE_UTINYINT
   :int16      ffi/DUCKDB_TYPE_SMALLINT
   :uint16     ffi/DUCKDB_TYPE_USMALLINT
   :int32      ffi/DUCKDB_TYPE_INTEGER
   :uint32     ffi/DUCKDB_TYPE_UINTEGER
   :int64      ffi/DUCKDB_TYPE_BIGINT
   :uint64     ffi/DUCKDB_TYPE_UBIGINT
   :float32    ffi/DUCKDB_TYPE_FLOAT
   :float64    ffi/DUCKDB_TYPE_DOUBLE
   :local-date ffi/DUCKDB_TYPE_DATE
   :local-time ffi/DUCKDB_TYPE_TIME
   :instant    ffi/DUCKDB_TYPE_TIMESTAMP
   :string     ffi/DUCKDB_TYPE_VARCHAR
   :uuid       ffi/DUCKDB_TYPE_UUID})

;; -- Write helpers (insert-dataset! building blocks) -------------------------

(defn- wrap-native
  "Wrap a native address as a typed native-buffer."
  [^long addr ^long n-bytes dtype]
  (-> (native-buffer/wrap-address addr n-bytes nil)
      (native-buffer/set-native-datatype dtype)))

(def ^:private write-col-specs
  "dtype → [byte-width target-dtype needs-pack?]
  Covers every column type that can be bulk-copied via dt/copy!."
  {:boolean          [1 :int8   false]
   :int8             [1 :int8   false]
   :uint8            [1 :uint8  false]
   :int16            [2 :int16  false]
   :uint16           [2 :uint16 false]
   :int32            [4 :int32  false]
   :uint32           [4 :uint32 false]
   :float32          [4 :float32 false]
   :int64            [8 :int64  false]
   :uint64           [8 :uint64 false]
   :float64          [8 :float64 false]
   :local-date       [4 :int32  true]
   :packed-local-date [4 :int32 false]
   :local-time       [8 :int64  true]
   :packed-local-time [8 :int64 false]
   :instant          [8 :int64  true]
   :packed-instant   [8 :int64  false]})

(defn- write-validity!
  "Write the validity bitmap for `missing` into the DuckDB validity segment."
  [^MemorySegment validity-seg ^RoaringBitmap missing ^long row-count]
  (let [n-valid (long (Math/ceil (/ (double row-count) 64)))
        missing-card (long (.getCardinality missing))]
    (cond
      (== missing-card row-count)
      (dotimes [i n-valid]
        (.set validity-seg ffi/VL-LONG (long (* i 8)) (long 0)))

      (== missing-card 0)
      (dotimes [i n-valid]
        (.set validity-seg ffi/VL-LONG (long (* i 8)) (long -1)))

      :else
      (do
        (dotimes [i n-valid]
          (.set validity-seg ffi/VL-LONG (long (* i 8)) (long -1)))
        (let [miter (.getIntIterator missing)]
          (loop []
            (when (.hasNext miter)
              (let [ne (Integer/toUnsignedLong (.next miter))
                    vidx (quot ne 64)
                    bit-idx (rem ne 64)
                    cv (.get validity-seg ffi/VL-LONG (long (* vidx 8)))]
                (.set validity-seg ffi/VL-LONG (long (* vidx 8))
                      (bit-and-not cv (bit-shift-left 1 bit-idx)))
                (recur)))))))))

(defn- write-uuid!
  [^MemorySegment data-ptr subcol ^long row-count]
  ;; Fill a JVM long array (no bounds-check overhead), then bulk-copy to native memory
  (let [n2 (long (* 2 row-count))
        ^longs bits (long-array n2)]
    (dotimes [i row-count]
      (let [^UUID uuid (subcol i)
            off (long (* 2 i))]
        (when uuid
          (aset bits off (.getLeastSignificantBits uuid))
          (aset bits (unchecked-inc off) (.getMostSignificantBits uuid)))))
    (let [^MemorySegment dseg (.reinterpret data-ptr (* 16 row-count))]
      (MemorySegment/copy bits 0 dseg ffi/VL-LONG (long 0) (int n2)))))

(defn- write-string!
  [^Arena arena ^MemorySegment data-ptr subcol ^long row-count]
  ;; Phase 1: encode all strings to byte arrays + compute total pointer-string bytes
  (let [^objects encoded (object-array row-count)
        total-ptr-bytes (loop [i 0 acc 0]
                          (if (< i row-count)
                            (let [v (subcol i)
                                  ^bytes b (.getBytes (if (string? v) ^String v (str v)) "UTF-8")]
                              (aset encoded i b)
                              (recur (unchecked-inc i)
                                     (if (> (alength b) 12) (+ acc (alength b)) acc)))
                            acc))
        ;; Phase 2: allocate one slab for all pointer-string data
        ^MemorySegment slab (when (pos? total-ptr-bytes)
                              (.allocate arena (long total-ptr-bytes)))
        slab-offset (java.util.concurrent.atomic.AtomicLong. 0)
        base-addr (.address data-ptr)]
    ;; Phase 3: parallel write duckdb_string_t entries
    (dorun
     (hamf/pgroups row-count
                   (fn [^long sidx ^long eidx]
                     (let [^MemorySegment dseg (.reinterpret (MemorySegment/ofAddress (+ base-addr (* sidx 16)))
                                                             (* (- eidx sidx) 16))]
                       (dotimes [i (- eidx sidx)]
                         (let [^bytes sbytes (aget encoded (int (+ sidx i)))
                               slen (alength sbytes)
                               off (long (* i 16))]
                           (.set dseg ffi/VL-INT off slen)
                           (if (<= slen 12)
                             (MemorySegment/copy sbytes 0 dseg ffi/VL-BYTE (+ off 4) slen)
                             ;; Bump-allocate from slab (lock-free via AtomicLong)
                             (let [slab-off (.getAndAdd slab-offset (long slen))
                                   ^MemorySegment buf (.asSlice slab slab-off (long slen))]
                               (MemorySegment/copy sbytes 0 buf ffi/VL-BYTE 0 slen)
                               (.set dseg ffi/VL-LONG (+ off 8) (.address buf))))))))))))

(defn- write-blob!
  "Write byte[] values into DuckDB BLOB vector (same string_t format as VARCHAR)."
  [^Arena arena ^MemorySegment data-ptr subcol row-count]
  (let [row-count (long row-count)
        ;; Phase 1: compute total pointer-blob bytes
        total-ptr-bytes (loop [i 0 acc 0]
                          (if (< i row-count)
                            (let [v (subcol i)]
                              (if (nil? v)
                                (recur (unchecked-inc i) acc)
                                (let [^bytes b v
                                      blen (alength b)]
                                  (recur (unchecked-inc i)
                                         (if (> blen 12) (+ acc blen) acc)))))
                            acc))
        ^MemorySegment slab (when (pos? total-ptr-bytes)
                              (.allocate arena (long total-ptr-bytes)))
        slab-offset (java.util.concurrent.atomic.AtomicLong. 0)
        base-addr (.address data-ptr)
        ^MemorySegment dseg (.reinterpret (MemorySegment/ofAddress base-addr) (* 16 row-count))]
    (dotimes [i row-count]
      (let [v (subcol i)]
        (when v
          (let [^bytes b v
                blen (alength b)
                off (long (* i 16))]
            (.set dseg ffi/VL-INT off blen)
            (if (<= blen 12)
              (MemorySegment/copy b 0 dseg ffi/VL-BYTE (+ off 4) blen)
              (let [slab-off (.getAndAdd slab-offset (long blen))
                    ^MemorySegment buf (.asSlice slab slab-off (long blen))]
                (MemorySegment/copy b 0 buf ffi/VL-BYTE 0 blen)
                (.set dseg ffi/VL-LONG (+ off 8) (.address buf))))))))))

(defn- write-hugeint!
  "Write BigInteger values into a DuckDB HUGEINT vector (16 bytes: lower uint64 + upper int64)."
  [^MemorySegment data-ptr subcol row-count]
  (let [row-count (long row-count)
        base-addr (.address data-ptr)
        ^MemorySegment seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 16 row-count))]
    (dotimes [i row-count]
      (let [v (subcol i)]
        (when v
          (let [^BigInteger bi (if (instance? BigInteger v) v (BigInteger/valueOf (long v)))
                off (long (* i 16))
                ;; Extract lower 64 bits and upper 64 bits
                lower (.longValue bi)
                upper (.longValue (.shiftRight bi 64))]
            (.set seg ffi/VL-LONG off lower)
            (.set seg ffi/VL-LONG (+ off 8) upper)))))))

(defn- write-interval!
  "Write interval maps {:months :days :micros} into a DuckDB INTERVAL vector."
  [^MemorySegment data-ptr subcol row-count]
  (let [row-count (long row-count)
        base-addr (.address data-ptr)
        ^MemorySegment seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 16 row-count))]
    (dotimes [i row-count]
      (let [v (subcol i)]
        (when v
          (let [off (long (* i 16))]
            (.set seg ffi/VL-INT off (int (or (:months v) 0)))
            (.set seg ffi/VL-INT (+ off 4) (int (or (:days v) 0)))
            (.set seg ffi/VL-LONG (+ off 8) (long (or (:micros v) 0)))))))))

(defn- write-decimal!
  "Write BigDecimal values into a DuckDB DECIMAL vector based on internal type."
  [^MemorySegment data-ptr subcol row-count scale internal-kw]
  (let [row-count (long row-count)
        scale (int scale)
        base-addr (.address data-ptr)]
    (case internal-kw
      :DUCKDB_TYPE_SMALLINT
      (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 2 row-count))]
        (dotimes [i row-count]
          (let [v (subcol i)]
            (when v
              (let [^BigDecimal bd (if (instance? BigDecimal v) v (BigDecimal/valueOf (double v)))]
                (.set seg ffi/VL-SHORT (long (* i 2)) (short (.longValue (.unscaledValue (.setScale bd scale))))))))))
      :DUCKDB_TYPE_INTEGER
      (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 4 row-count))]
        (dotimes [i row-count]
          (let [v (subcol i)]
            (when v
              (let [^BigDecimal bd (if (instance? BigDecimal v) v (BigDecimal/valueOf (double v)))]
                (.set seg ffi/VL-INT (long (* i 4)) (int (.longValue (.unscaledValue (.setScale bd scale))))))))))
      :DUCKDB_TYPE_BIGINT
      (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 8 row-count))]
        (dotimes [i row-count]
          (let [v (subcol i)]
            (when v
              (let [^BigDecimal bd (if (instance? BigDecimal v) v (BigDecimal/valueOf (double v)))]
                (.set seg ffi/VL-LONG (long (* i 8)) (.longValue (.unscaledValue (.setScale bd scale)))))))))
      :DUCKDB_TYPE_HUGEINT
      (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 16 row-count))]
        (dotimes [i row-count]
          (let [v (subcol i)]
            (when v
              (let [^BigDecimal bd (if (instance? BigDecimal v) v (BigDecimal/valueOf (double v)))
                    ^BigInteger bi (.unscaledValue (.setScale bd scale))
                    off (long (* i 16))
                    lower (.longValue bi)
                    upper (.longValue (.shiftRight bi 64))]
                (.set seg ffi/VL-LONG off lower)
                (.set seg ffi/VL-LONG (+ off 8) upper)))))))))

(defn- instant->micros ^long [v]
  (if (instance? Instant v)
    (let [^Instant inst v]
      (+ (* (.getEpochSecond inst) 1000000)
         (quot (.getNano inst) 1000)))
    (long v)))

(defn- write-timestamp-converted!
  "Write Instant values as TIMESTAMP_S/MS/NS by converting to target precision."
  [^MemorySegment data-ptr subcol row-count mode]
  (let [row-count (long row-count)
        base-addr (.address data-ptr)
        ^MemorySegment seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 8 row-count))]
    (dotimes [i row-count]
      (let [v (subcol i)]
        (when v
          (let [micros (instant->micros v)]
            (.set seg ffi/VL-LONG (long (* i 8))
                  (case mode
                    :micros  micros
                    :seconds (quot micros 1000000)
                    :millis  (quot micros 1000)
                    :nanos   (* micros 1000)))))))))

(def ^:private duckdb-type->write-dtype
  "Map DuckDB type keywords to dataset dtype keywords for STRUCT child writing."
  {:DUCKDB_TYPE_BOOLEAN   :boolean
   :DUCKDB_TYPE_TINYINT   :int8
   :DUCKDB_TYPE_SMALLINT  :int16
   :DUCKDB_TYPE_INTEGER   :int32
   :DUCKDB_TYPE_BIGINT    :int64
   :DUCKDB_TYPE_UTINYINT  :uint8
   :DUCKDB_TYPE_USMALLINT :uint16
   :DUCKDB_TYPE_UINTEGER  :uint32
   :DUCKDB_TYPE_UBIGINT   :uint64
   :DUCKDB_TYPE_FLOAT     :float32
   :DUCKDB_TYPE_DOUBLE    :float64
   :DUCKDB_TYPE_DATE      :local-date
   :DUCKDB_TYPE_TIME      :local-time
   :DUCKDB_TYPE_TIMESTAMP :instant
   :DUCKDB_TYPE_TIMESTAMP_TZ :instant
   :DUCKDB_TYPE_VARCHAR   :string
   :DUCKDB_TYPE_UUID      :uuid})

(defn- write-enum!
  "Write string values as ENUM dictionary indices."
  [^MemorySegment data-ptr subcol row-count dict-reverse internal-kw]
  (let [row-count (long row-count)
        base-addr (.address data-ptr)]
    (case internal-kw
      :DUCKDB_TYPE_UTINYINT
      (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress base-addr) row-count)]
        (dotimes [i row-count]
          (let [v (subcol i)
                idx (int (get dict-reverse (if (string? v) v (str v)) 0))]
            (.set seg ffi/VL-BYTE (long i) (byte idx)))))
      :DUCKDB_TYPE_USMALLINT
      (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 2 row-count))]
        (dotimes [i row-count]
          (let [v (subcol i)
                idx (int (get dict-reverse (if (string? v) v (str v)) 0))]
            (.set seg ffi/VL-SHORT (long (* i 2)) (short idx)))))
      :DUCKDB_TYPE_UINTEGER
      (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 4 row-count))]
        (dotimes [i row-count]
          (let [v (subcol i)
                idx (int (get dict-reverse (if (string? v) v (str v)) 0))]
            (.set seg ffi/VL-INT (long (* i 4)) idx)))))))

(defn- write-column!
  "Write `subcol` data into DuckDB vector memory at `data-ptr`."
  [^Arena arena ^MemorySegment data-ptr subcol row-count col-dt daddr]
  (let [row-count (long row-count)
        daddr (long daddr)]
    (if-let [[^long bw target-dt pack?] (get write-col-specs col-dt)]
      ;; Numeric / temporal — bulk copy through native-buffer (dtype's fast path)
      (let [src (if pack? (packing/pack subcol) subcol)]
        (dt/copy! src (wrap-native daddr (* bw row-count) target-dt)))
      (case col-dt
        :uuid           (write-uuid! data-ptr subcol row-count)
        (:string :text) (write-string! arena data-ptr subcol row-count)))))

(defn- write-struct!
  "Write a column of maps into a DuckDB STRUCT vector's child vectors."
  [^Arena arena ^MemorySegment dvec subcol row-count child-names child-type-ids]
  (let [row-count (long row-count)
        n-children (count child-names)]
    (dotimes [ci n-children]
      (let [child-key (keyword (child-names ci))
            child-tid (long (child-type-ids ci))
            child-type-kw (get ffi/duckdb-type-map child-tid)
            child-dtype (get duckdb-type->write-dtype child-type-kw)
            ^MemorySegment cvec (ffi/duckdb_struct_vector_get_child dvec (long ci))
            _ (ffi/duckdb_vector_ensure_validity_writable cvec)
            ^MemorySegment cdata (ffi/duckdb_vector_get_data cvec)
            ^MemorySegment cvalid (ffi/duckdb_vector_get_validity cvec)
            ;; Create reader extracting field values from maps
            field-reader (reify ObjectReader
                           (lsize [_] row-count)
                           (readObject [_ idx]
                             (let [m (subcol idx)]
                               (when m (get m child-key)))))
            ;; Compute child validity
            child-missing (RoaringBitmap.)]
        (dotimes [i row-count]
          (when (nil? (field-reader i))
            (.add child-missing (int i))))
        (write-validity! (.reinterpret cvalid (* (long (Math/ceil (/ (double row-count) 64))) 8))
                         child-missing row-count)
        (when child-dtype
          (write-column! arena cdata field-reader row-count child-dtype (.address cdata)))))))

(defn- write-list!
  "Write a column of vectors/lists into a DuckDB LIST vector."
  [^Arena arena ^MemorySegment dvec subcol row-count child-type-info]
  (let [row-count (long row-count)
        ;; Compute total child count and list entries
        total-children (loop [i 0 acc 0]
                         (if (< i row-count)
                           (let [v (subcol i)]
                             (recur (unchecked-inc i)
                                    (if (and v (sequential? v)) (+ acc (count v)) acc)))
                           acc))
        ;; Write list_entry_t array (offset + length)
        ^MemorySegment entry-data (ffi/duckdb_vector_get_data dvec)
        ^MemorySegment entry-seg (.reinterpret entry-data (* 16 row-count))
        _ (ffi/duckdb_list_vector_set_size dvec (long total-children))
        ^MemorySegment child-vec (ffi/duckdb_list_vector_get_child dvec)
        _ (ffi/duckdb_vector_ensure_validity_writable child-vec)
        ^MemorySegment child-data (ffi/duckdb_vector_get_data child-vec)
        ^MemorySegment child-validity (ffi/duckdb_vector_get_validity child-vec)
        child-type-kw (get ffi/duckdb-type-map (long (:type-id child-type-info)))
        child-dtype (get duckdb-type->write-dtype child-type-kw)
        flat-vals (object-array total-children)
        child-missing (RoaringBitmap.)]
    ;; Phase 1: write list_entry_t offsets/lengths and collect flat child data
    (loop [i 0 offset 0]
      (when (< i row-count)
        (let [v (subcol i)
              entry-off (long (* i 16))]
          (if (and v (sequential? v))
            (let [n (count v)]
              (.set entry-seg ffi/VL-LONG entry-off (long offset))
              (.set entry-seg ffi/VL-LONG (+ entry-off 8) (long n))
              (dotimes [j n]
                (let [elem (nth v j)
                      ci (+ offset j)]
                  (if (nil? elem)
                    (.add child-missing (int ci))
                    (aset flat-vals ci elem))))
              (recur (unchecked-inc i) (+ offset n)))
            (do
              (.set entry-seg ffi/VL-LONG entry-off (long offset))
              (.set entry-seg ffi/VL-LONG (+ entry-off 8) (long 0))
              (recur (unchecked-inc i) offset))))))
    ;; Phase 2: write child validity
    (let [n-valid-words (long (Math/ceil (/ (double total-children) 64)))
          ^MemorySegment cval-seg (.reinterpret child-validity (* n-valid-words 8))]
      (write-validity! cval-seg child-missing total-children))
    ;; Phase 3: write child data using existing infrastructure
    (when (and child-dtype (pos? total-children))
      (let [flat-reader (reify ObjectReader
                          (lsize [_] total-children)
                          (readObject [_ idx]
                            ;; Return default (0) for nil entries — validity bitmap handles nulls
                            (let [v (aget flat-vals idx)]
                              (if (nil? v) (long 0) v))))]
        (write-column! arena child-data flat-reader total-children child-dtype (.address child-data))))))

(defn- write-map!
  "Write a column of Clojure maps into a DuckDB MAP vector (LIST<STRUCT{key,value}>)."
  [^Arena arena ^MemorySegment dvec subcol row-count key-type-info val-type-info]
  (let [row-count (long row-count)
        ;; Compute total entries across all maps
        total-entries (loop [i 0 acc 0]
                        (if (< i row-count)
                          (let [v (subcol i)]
                            (recur (unchecked-inc i)
                                   (if (and v (map? v)) (+ acc (count v)) acc)))
                          acc))
        ;; Write list_entry_t for the parent LIST vector
        ^MemorySegment entry-data (ffi/duckdb_vector_get_data dvec)
        ^MemorySegment entry-seg (.reinterpret entry-data (* 16 row-count))
        _ (ffi/duckdb_list_vector_set_size dvec (long total-entries))
        ;; Child is STRUCT{key, value} — get its two sub-children
        ^MemorySegment struct-vec (ffi/duckdb_list_vector_get_child dvec)
        ^MemorySegment key-vec (ffi/duckdb_struct_vector_get_child struct-vec 0)
        ^MemorySegment val-vec (ffi/duckdb_struct_vector_get_child struct-vec 1)
        _ (ffi/duckdb_vector_ensure_validity_writable key-vec)
        _ (ffi/duckdb_vector_ensure_validity_writable val-vec)
        ^MemorySegment key-data (ffi/duckdb_vector_get_data key-vec)
        ^MemorySegment val-data (ffi/duckdb_vector_get_data val-vec)
        ^MemorySegment key-validity (ffi/duckdb_vector_get_validity key-vec)
        ^MemorySegment val-validity (ffi/duckdb_vector_get_validity val-vec)
        key-type-kw (get ffi/duckdb-type-map (long (:type-id key-type-info)))
        val-type-kw (get ffi/duckdb-type-map (long (:type-id val-type-info)))
        key-dtype (get duckdb-type->write-dtype key-type-kw)
        val-dtype (get duckdb-type->write-dtype val-type-kw)
        ;; Flatten keys and values, write list entries
        ^objects flat-keys (object-array total-entries)
        ^objects flat-vals (object-array total-entries)
        key-missing (RoaringBitmap.)
        val-missing (RoaringBitmap.)]
    (loop [i 0 offset 0]
      (when (< i row-count)
        (let [v (subcol i)
              entry-off (long (* i 16))]
          (if (and v (map? v))
            (let [entries (seq v)
                  n (count entries)]
              (.set entry-seg ffi/VL-LONG entry-off (long offset))
              (.set entry-seg ffi/VL-LONG (+ entry-off 8) (long n))
              (loop [es entries j 0]
                (when es
                  (let [[k vv] (first es)
                        ci (+ offset j)]
                    (if (nil? k) (.add key-missing (int ci)) (aset flat-keys ci k))
                    (if (nil? vv) (.add val-missing (int ci)) (aset flat-vals ci vv))
                    (recur (next es) (unchecked-inc j)))))
              (recur (unchecked-inc i) (+ offset n)))
            (do
              (.set entry-seg ffi/VL-LONG entry-off (long offset))
              (.set entry-seg ffi/VL-LONG (+ entry-off 8) (long 0))
              (recur (unchecked-inc i) offset))))))
    ;; Write key/value validity
    (when (pos? total-entries)
      (let [nw (long (Math/ceil (/ (double total-entries) 64)))]
        (write-validity! (.reinterpret key-validity (* nw 8)) key-missing total-entries)
        (write-validity! (.reinterpret val-validity (* nw 8)) val-missing total-entries)))
    ;; Write key data
    (when (and key-dtype (pos? total-entries))
      (let [kr (reify ObjectReader (lsize [_] total-entries)
                 (readObject [_ idx] (let [v (aget flat-keys idx)] (if (nil? v) "" v))))]
        (write-column! arena key-data kr total-entries key-dtype (.address key-data))))
    ;; Write value data
    (when (and val-dtype (pos? total-entries))
      (let [vr (reify ObjectReader (lsize [_] total-entries)
                 (readObject [_ idx] (let [v (aget flat-vals idx)] (if (nil? v) (long 0) v))))]
        (write-column! arena val-data vr total-entries val-dtype (.address val-data))))))

;; -- insert-dataset! --------------------------------------------------------

(defn insert-dataset!
  ([^long conn dataset options]
   ;; ofShared: allows parallel string/uuid writes from hamf/pgroups fork-join threads
   (with-open [arena (Arena/ofShared)]
     (let [table-name (sql/table-name dataset options)
           conn-seg   (MemorySegment/ofAddress conn)
           app-ptr    (.allocate arena ffi/VL-ADDR)
           app-rc     (ffi/duckdb_appender_create conn-seg
                                                  (ffi/alloc-c-str arena "")
                                                  (ffi/alloc-c-str arena table-name)
                                                  app-ptr)
           ^MemorySegment appender (.get app-ptr ffi/VL-ADDR 0)
           check-error (fn [status]
                         (when-not (== (int status) ffi/DuckDBSuccess)
                           (let [err (ffi/read-c-str ^MemorySegment (ffi/duckdb_appender_error appender))]
                             (throw (Exception. (str "Appender error: " err))))))
           _          (check-error app-rc)
           n-rows     (ds/row-count dataset)
           n-cols     (ds/column-count dataset)
           colvec     (vec (ds/columns dataset))
           dtypes     (mapv (comp packing/unpack-datatype dt/elemwise-datatype) colvec)
           duckdb-ids (mapv dtype->duckdb-type dtypes)
           chunk-size (ffi/duckdb_vector_size)
           n-chunks   (long (Math/ceil (/ (double n-rows) chunk-size)))
           ;; Only probe appender column types when dataset has columns that
           ;; may need special handling. String columns could be ENUM, object
           ;; columns could be BLOB/HUGEINT/DECIMAL/INTERVAL/LIST/MAP/STRUCT.
           ;; Pure numeric/temporal/uuid datasets skip all probe FFI calls.
           needs-probe? (some (fn [ci]
                                (let [dt (dtypes ci)]
                                  (or (nil? (duckdb-ids ci))
                                      (= dt :string))))
                              (range n-cols))
           col-overrides
           (if-not needs-probe?
             {}
             (reduce (fn [m ci]
                       (let [^MemorySegment lt (ffi/duckdb_appender_column_type appender (long ci))
                             tid (ffi/duckdb_get_type_id lt)
                             kw (get ffi/duckdb-type-map tid)]
                         (case kw
                           :DUCKDB_TYPE_ENUM
                           (let [dict-size (ffi/duckdb_enum_dictionary_size lt)
                                 dict (mapv (fn [i]
                                              (let [^MemorySegment s (ffi/duckdb_enum_dictionary_value lt (long i))
                                                    v (ffi/read-c-str s)]
                                                (ffi/duckdb_free s) v))
                                            (range dict-size))
                                 dict-reverse (into {} (map-indexed (fn [i v] [v i]) dict))
                                 internal-kw (get ffi/duckdb-type-map (ffi/duckdb_enum_internal_type lt))]
                             (assoc m ci {:kw kw :lt lt :dict-reverse dict-reverse :internal-kw internal-kw}))
                           :DUCKDB_TYPE_STRUCT
                           (let [n (ffi/duckdb_struct_type_child_count lt)
                                 child-names (mapv (fn [i]
                                                     (let [^MemorySegment s (ffi/duckdb_struct_type_child_name lt (long i))
                                                           v (ffi/read-c-str s)]
                                                       (ffi/duckdb_free s) v))
                                                   (range n))
                                 child-type-ids (mapv (fn [i]
                                                        (let [^MemorySegment clt (ffi/duckdb_struct_type_child_type lt (long i))
                                                              tid (ffi/duckdb_get_type_id clt)]
                                                          (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type clt)
                                                          tid))
                                                      (range n))]
                             (assoc m ci {:kw kw :lt lt :child-names child-names :child-type-ids child-type-ids}))
                           (:DUCKDB_TYPE_TIMESTAMP_TZ :DUCKDB_TYPE_TIMESTAMP_S :DUCKDB_TYPE_TIMESTAMP_MS :DUCKDB_TYPE_TIMESTAMP_NS)
                           (assoc m ci {:kw kw :lt lt})
                           :DUCKDB_TYPE_BLOB
                           (assoc m ci {:kw kw :lt lt})
                           :DUCKDB_TYPE_HUGEINT
                           (assoc m ci {:kw kw :lt lt})
                           :DUCKDB_TYPE_INTERVAL
                           (assoc m ci {:kw kw :lt lt})
                           :DUCKDB_TYPE_DECIMAL
                           (let [scale-val (ffi/duckdb_decimal_scale lt)
                                 width-val (ffi/duckdb_decimal_width lt)
                                 internal-type (ffi/duckdb_decimal_internal_type lt)
                                 internal-kw (get ffi/duckdb-type-map internal-type)]
                             (assoc m ci {:kw kw :lt lt :scale scale-val :width width-val :internal-kw internal-kw}))
                           :DUCKDB_TYPE_LIST
                           (let [^MemorySegment child-lt (ffi/duckdb_list_type_child_type lt)
                                 child-tid (ffi/duckdb_get_type_id child-lt)
                                 child-info {:type-id child-tid}]
                             (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type child-lt)
                             (assoc m ci {:kw kw :lt lt :child-type child-info}))
                           :DUCKDB_TYPE_MAP
                         ;; MAP is LIST<STRUCT{key, value}> — extract key/value types
                           (let [^MemorySegment list-child-lt (ffi/duckdb_list_type_child_type lt)
                               ;; list-child is STRUCT{key, value}
                                 ^MemorySegment key-lt (ffi/duckdb_struct_type_child_type list-child-lt 0)
                                 ^MemorySegment val-lt (ffi/duckdb_struct_type_child_type list-child-lt 1)
                                 key-info {:type-id (ffi/duckdb_get_type_id key-lt)}
                                 val-info {:type-id (ffi/duckdb_get_type_id val-lt)}]
                             (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type key-lt)
                             (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type val-lt)
                             (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type list-child-lt)
                             (assoc m ci {:kw kw :lt lt :key-type key-info :val-type val-info}))
                         ;; Non-special type — destroy and skip
                           (do (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type lt)
                               m))))
                     {} (range n-cols)))
           types-seg  (.allocate arena (* n-cols 8))
           _          (dotimes [ci n-cols]
                        (if-let [ovr (get col-overrides ci)]
                          ;; Use appender's logical type (e.g. ENUM)
                          (.set types-seg ffi/VL-ADDR (long (* ci 8)) ^MemorySegment (:lt ovr))
                          ;; Normal: create from dataset dtype
                          (.set types-seg ffi/VL-ADDR (long (* ci 8))
                                ^MemorySegment (ffi/duckdb_create_logical_type (int (duckdb-ids ci))))))
           ^MemorySegment write-chunk (ffi/duckdb_create_data_chunk types-seg (long n-cols))
           ;; Pre-fetch vector handles — stable across duckdb_data_chunk_reset
           vecs  (object-array (map #(ffi/duckdb_data_chunk_get_vector write-chunk (long %)) (range n-cols)))
           ;; Pre-pack temporal columns once (not per-chunk)
           prepped-cols (object-array
                         (map-indexed
                          (fn [ci col]
                            (let [dt (dtypes ci)]
                              (if (#{:local-date :local-time :instant} dt)
                                (packing/pack col)
                                col)))
                          colvec))
           ;; Pre-resolve full-column missing bitmaps (avoid per-chunk ds/missing)
           ^objects full-missings (object-array (map ds/missing colvec))]
       (try
         (dotimes [chunk-idx n-chunks]
           (let [row-off   (long (* chunk-idx chunk-size))
                 row-count (long (min chunk-size (- n-rows row-off)))]
             (ffi/duckdb_data_chunk_set_size write-chunk row-count)
             (dotimes [col-idx n-cols]
               (let [^MemorySegment dvec (aget vecs col-idx)
                     _                   (ffi/duckdb_vector_ensure_validity_writable dvec)
                     ^MemorySegment dp   (ffi/duckdb_vector_get_data dvec)
                     ^MemorySegment vp   (ffi/duckdb_vector_get_validity dvec)
                     col-dt  (dtypes col-idx)
                     ;; Clip full-column missing bitmap to this chunk's range
                     ^RoaringBitmap full-missing (aget full-missings col-idx)
                     chunk-missing (if (.isEmpty full-missing)
                                     full-missing
                                     (let [bm (RoaringBitmap/bitmapOfRange row-off (+ row-off row-count))]
                                       (.and bm full-missing)
                                       ;; Shift to 0-based for this chunk
                                       (RoaringBitmap/addOffset bm (- row-off))))
                     subcol  (dt/sub-buffer (aget prepped-cols col-idx) row-off row-count)]
                 (write-validity! (.reinterpret vp (* (long (Math/ceil (/ (double row-count) 64))) 8))
                                  chunk-missing
                                  row-count)
                 (if-let [ovr (get col-overrides col-idx)]
                   ;; Special column type
                   (case (:kw ovr)
                     :DUCKDB_TYPE_ENUM
                     (write-enum! dp subcol row-count (:dict-reverse ovr) (:internal-kw ovr))
                     :DUCKDB_TYPE_STRUCT
                     (write-struct! arena dvec subcol row-count (:child-names ovr) (:child-type-ids ovr))
                     :DUCKDB_TYPE_TIMESTAMP_TZ
                     (write-timestamp-converted! dp subcol row-count :micros)
                     :DUCKDB_TYPE_TIMESTAMP_S
                     (write-timestamp-converted! dp subcol row-count :seconds)
                     :DUCKDB_TYPE_TIMESTAMP_MS
                     (write-timestamp-converted! dp subcol row-count :millis)
                     :DUCKDB_TYPE_TIMESTAMP_NS
                     (write-timestamp-converted! dp subcol row-count :nanos)
                     :DUCKDB_TYPE_BLOB
                     (write-blob! arena dp subcol row-count)
                     :DUCKDB_TYPE_HUGEINT
                     (write-hugeint! dp subcol row-count)
                     :DUCKDB_TYPE_INTERVAL
                     (write-interval! dp subcol row-count)
                     :DUCKDB_TYPE_DECIMAL
                     (write-decimal! dp subcol row-count (:scale ovr) (:internal-kw ovr))
                     :DUCKDB_TYPE_LIST
                     (write-list! arena dvec subcol row-count (:child-type ovr))
                     :DUCKDB_TYPE_MAP
                     (write-map! arena dvec subcol row-count (:key-type ovr) (:val-type ovr)))
                   ;; Normal column
                   (write-column! arena dp subcol row-count
                                  (if (#{:local-date :local-time :instant} col-dt)
                                    (keyword (str "packed-" (name col-dt)))
                                    col-dt)
                                  (.address dp)))))
             (check-error (ffi/duckdb_append_data_chunk appender write-chunk))
             (ffi/duckdb_data_chunk_reset write-chunk)))
         n-rows
         (finally
           (ffi/destroy-ptrs!
            (concat [[ffi/duckdb_appender_destroy appender]
                     [ffi/duckdb_destroy_data_chunk write-chunk]]
                    (map (fn [ci] [ffi/duckdb_destroy_logical_type
                                   (.get types-seg ffi/VL-ADDR (long (* ci 8)))])
                         (range n-cols)))))))))
  ([conn dataset] (insert-dataset! conn dataset nil)))

;; ---------------------------------------------------------------------------
;; 7b. Read-path instrumentation (dev-only, off by default; zero overhead off)
;; ---------------------------------------------------------------------------

(def ^:dynamic *instrument-stats*
  "When bound to a Map<Keyword, LongAdder>, per-phase timings/counters
  accumulate here. nil by default — single var-deref + nil-check overhead."
  nil)

(defmacro ^:private instr-time
  "Time `body`, accumulate ns into the LongAdder at `phase-kw` if instrumentation
  is enabled. When disabled, just runs body."
  [phase-kw & body]
  `(let [m# *instrument-stats*]
     (if (nil? m#)
       (do ~@body)
       (let [t0# (System/nanoTime)
             r#  (do ~@body)
             t1# (System/nanoTime)]
         (when-let [^LongAdder a# (.get ^java.util.Map m# ~phase-kw)]
           (.add a# (- t1# t0#)))
         r#))))

(defn- instr-inc!
  "Increment a counter by 1 (or n)."
  ([phase-kw] (instr-inc! phase-kw 1))
  ([phase-kw n]
   (when-let [^java.util.Map m *instrument-stats*]
     (when-let [^LongAdder a (.get m phase-kw)]
       (.add a (long n))))))

(defn new-instr-buf
  "Build a fresh instrumentation buffer with all known phase counters."
  ^java.util.Map []
  (doto (java.util.HashMap.)
    ;; ns timings
    (.put :fetch-ns             (LongAdder.))
    (.put :realize-wallclock-ns (LongAdder.))
    (.put :validity-ns          (LongAdder.))
    (.put :buffer-ns            (LongAdder.))
    (.put :clone-cpu-ns         (LongAdder.))
    (.put :deref-wait-ns        (LongAdder.))
    (.put :concat-ns            (LongAdder.))
    ;; counters
    (.put :chunks               (LongAdder.))
    (.put :columns              (LongAdder.))
    (.put :clones               (LongAdder.))))

(defn instr-snapshot
  "Snapshot the instrumentation buffer to a sorted map of {phase sum}."
  [^java.util.Map m]
  (into (sorted-map)
        (map (fn [[k ^LongAdder a]] [k (.sum a)]))
        m))

(defmacro with-instrumentation
  "Bind a fresh instrumentation buffer for `body`. Returns [result snapshot]."
  [& body]
  `(let [buf# (new-instr-buf)]
     (binding [*instrument-stats* buf#]
       (let [r# (do ~@body)]
         [r# (instr-snapshot buf#)]))))

;; ---------------------------------------------------------------------------
;; 8. Read path — validity->missing and coldata->buffer
;; ---------------------------------------------------------------------------

(defn- validity->missing
  ^RoaringBitmap [^long n-rows ^MemorySegment nmask]
  (if (or (nil? nmask) (== 0 (.address nmask)))
    (bitmap/->bitmap)
    (let [nvals (long (Math/ceil (/ (double n-rows) 64)))
          ^MemorySegment seg (.reinterpret nmask (* nvals 8))
          rval (bitmap/->bitmap)]
      (dotimes [idx nvals]
        (let [lval (.get seg ffi/VL-LONG (long (* idx 8)))]
          (when-not (== lval -1)
            (let [logical-idx (unchecked-long (* idx 64))]
              ;; Iterate only set bits in the inverted mask (O(nulls) not O(64))
              (loop [inv (bit-not lval)]
                (when-not (== 0 inv)
                  (let [bit-idx (Long/numberOfTrailingZeros inv)]
                    (.add rval (unchecked-int (+ bit-idx logical-idx)))
                    (recur (bit-and inv (unchecked-dec inv))))))))))
      rval)))

(def ^:private numeric-col-specs
  "Map from duckdb-type-map keyword → [byte-width native-dtype].
  Types here are handled by a single wrap-address + set-native-datatype path."
  {:DUCKDB_TYPE_BOOLEAN   [1  :int8]   ;; post-processed via elemwise-cast
   :DUCKDB_TYPE_TINYINT   [1  :int8]
   :DUCKDB_TYPE_SMALLINT  [2  :int16]
   :DUCKDB_TYPE_INTEGER   [4  :int32]
   :DUCKDB_TYPE_BIGINT    [8  :int64]
   :DUCKDB_TYPE_UTINYINT  [1  :uint8]
   :DUCKDB_TYPE_USMALLINT [2  :uint16]
   :DUCKDB_TYPE_UINTEGER  [4  :uint32]
   :DUCKDB_TYPE_UBIGINT   [8  :uint64]
   :DUCKDB_TYPE_FLOAT     [4  :float32]
   :DUCKDB_TYPE_DOUBLE    [8  :float64]
   :DUCKDB_TYPE_DATE      [4  :packed-local-date]
   :DUCKDB_TYPE_TIME      [8  :packed-local-time]
   :DUCKDB_TYPE_TIMESTAMP [8  :packed-instant]
   :DUCKDB_TYPE_TIMESTAMP_TZ [8  :packed-instant]})

(defn- coldata->buffer
  [^RoaringBitmap missing n-rows type-info data-ptr ^MemorySegment vector-seg]
  (let [n-rows (long n-rows)
        data-ptr (long data-ptr)
        type-kw (:type-kw type-info)]
    (if-let [[^long byte-width dtype] (get numeric-col-specs type-kw)]
      ;; Numeric / temporal — zero-copy wrap, dt/clone does memcpy
      (let [buf (-> (native-buffer/wrap-address data-ptr (* byte-width n-rows) nil)
                    (native-buffer/set-native-datatype dtype))]
        (if (= type-kw :DUCKDB_TYPE_BOOLEAN) (dt/elemwise-cast buf :boolean) buf))

      ;; Complex types
      (case type-kw
        :DUCKDB_TYPE_UUID
        (let [base-addr data-ptr
              ^sun.misc.Unsafe u ffi/UNSAFE]
          (reify
            ObjectReader
            (elemwiseDatatype [_] :uuid)
            (lsize [_] n-rows)
            (readObject [_ idx]
              (let [addr (+ base-addr (* idx 16))]
                (UUID. (.getLong u (+ addr 8))
                       (.getLong u addr))))
            dt-proto/PClone
            (clone [_]
              ;; Unsafe raw reads; cross-column parallelism via futures in realize-chunk
              (let [^objects out (make-array UUID n-rows)]
                (dotimes [i n-rows]
                  (let [addr (+ base-addr (* i 16))]
                    (aset out (int i) (UUID. (.getLong u (+ addr 8))
                                             (.getLong u addr)))))
                (hamf/wrap-array out)))))

        :DUCKDB_TYPE_VARCHAR
        ;; Single reinterpreted segment for the whole string_t array — avoid per-element ofAddress
        (let [base-addr data-ptr
              ^MemorySegment full-seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 16 n-rows))
              has-missing? (not (.isEmpty missing))]
          (reify
            ObjectReader
            (elemwiseDatatype [_] :string)
            (lsize [_] n-rows)
            (readObject [_ idx]
              ;; NULL entries may have garbage string_t data — guard against it
              (when-not (and has-missing? (.contains missing (int idx)))
                (let [off (long (* idx 16))
                      slen (int (.get full-seg ffi/VL-INT off))]
                  (if (<= slen 12)
                    (let [arr (byte-array slen)]
                      (MemorySegment/copy full-seg ffi/VL-BYTE (+ off 4) arr 0 slen)
                      (String. arr "UTF-8"))
                    (let [ptr-addr (.get full-seg ffi/VL-LONG (+ off 8))
                          ^MemorySegment ptr-seg (.reinterpret (MemorySegment/ofAddress ptr-addr) slen)
                          arr (byte-array slen)]
                      (MemorySegment/copy ptr-seg ffi/VL-BYTE (long 0) arr 0 slen)
                      (String. arr "UTF-8"))))))
            dt-proto/PClone
            (clone [_this]
              ;; Parallel decode via hamf/pgroups — each group works on a sub-range
              (let [ne n-rows
                    ^objects out (make-array String ne)]
                (dorun
                 (hamf/pgroups ne
                               (if has-missing?
                                 ;; Slow path: check missing per element
                                 (fn [^long sidx ^long eidx]
                                   (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress (+ base-addr (* sidx 16)))
                                                                          (* (- eidx sidx) 16))]
                                     (dotimes [i (- eidx sidx)]
                                       (when-not (.contains missing (int (+ sidx i)))
                                         (let [off (long (* i 16))
                                               slen (int (.get seg ffi/VL-INT off))]
                                           (aset out (int (+ sidx i))
                                                 (if (<= slen 12)
                                                   (let [arr (byte-array slen)]
                                                     (MemorySegment/copy seg ffi/VL-BYTE (+ off 4) arr 0 slen)
                                                     (String. arr "UTF-8"))
                                                   (let [ptr-addr (.get seg ffi/VL-LONG (+ off 8))
                                                         ^MemorySegment ps (.reinterpret (MemorySegment/ofAddress ptr-addr) slen)
                                                         arr (byte-array slen)]
                                                     (MemorySegment/copy ps ffi/VL-BYTE (long 0) arr 0 slen)
                                                     (String. arr "UTF-8")))))))))
                                 ;; Fast path: no nulls — skip missing check entirely
                                 (fn [^long sidx ^long eidx]
                                   (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress (+ base-addr (* sidx 16)))
                                                                          (* (- eidx sidx) 16))]
                                     (dotimes [i (- eidx sidx)]
                                       (let [off (long (* i 16))
                                             slen (int (.get seg ffi/VL-INT off))]
                                         (aset out (int (+ sidx i))
                                               (if (<= slen 12)
                                                 (let [arr (byte-array slen)]
                                                   (MemorySegment/copy seg ffi/VL-BYTE (+ off 4) arr 0 slen)
                                                   (String. arr "UTF-8"))
                                                 (let [ptr-addr (.get seg ffi/VL-LONG (+ off 8))
                                                       ^MemorySegment ps (.reinterpret (MemorySegment/ofAddress ptr-addr) slen)
                                                       arr (byte-array slen)]
                                                   (MemorySegment/copy ps ffi/VL-BYTE (long 0) arr 0 slen)
                                                   (String. arr "UTF-8")))))))))))
                (hamf/wrap-array out)))))

        :DUCKDB_TYPE_TIMESTAMP_S
        (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 8 n-rows))]
          (reify ObjectReader
            (elemwiseDatatype [_] :instant)
            (lsize [_] n-rows)
            (readObject [_ idx]
              (let [secs (.get seg ffi/VL-LONG (long (* idx 8)))]
                (Instant/ofEpochSecond secs)))))

        :DUCKDB_TYPE_TIMESTAMP_MS
        (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 8 n-rows))]
          (reify ObjectReader
            (elemwiseDatatype [_] :instant)
            (lsize [_] n-rows)
            (readObject [_ idx]
              (let [ms (.get seg ffi/VL-LONG (long (* idx 8)))]
                (Instant/ofEpochMilli ms)))))

        :DUCKDB_TYPE_TIMESTAMP_NS
        (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 8 n-rows))]
          (reify ObjectReader
            (elemwiseDatatype [_] :instant)
            (lsize [_] n-rows)
            (readObject [_ idx]
              (let [ns-val (.get seg ffi/VL-LONG (long (* idx 8)))
                    secs (quot ns-val 1000000000)
                    nanos (rem ns-val 1000000000)]
                (Instant/ofEpochSecond secs nanos)))))

        :DUCKDB_TYPE_HUGEINT
        (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 16 n-rows))]
          (reify ObjectReader
            (elemwiseDatatype [_] :object)
            (lsize [_] n-rows)
            (readObject [_ idx]
              (let [off (long (* idx 16))
                    lower (.get seg ffi/VL-LONG off)
                    upper (.get seg ffi/VL-LONG (+ off 8))
                    lower-bi (if (neg? lower)
                               (.add (BigInteger/valueOf lower) (.shiftLeft BigInteger/ONE 64))
                               (BigInteger/valueOf lower))]
                (.add (.shiftLeft (BigInteger/valueOf upper) 64) lower-bi)))))

        :DUCKDB_TYPE_BLOB
        (let [base-addr data-ptr
              ^MemorySegment full-seg (.reinterpret (MemorySegment/ofAddress base-addr) (* 16 n-rows))]
          (reify ObjectReader
            (elemwiseDatatype [_] :object)
            (lsize [_] n-rows)
            (readObject [_ idx]
              (when-not (.contains missing (int idx))
                (let [off (long (* idx 16))
                      slen (int (.get full-seg ffi/VL-INT off))]
                  (if (<= slen 12)
                    (let [arr (byte-array slen)]
                      (MemorySegment/copy full-seg ffi/VL-BYTE (+ off 4) arr 0 slen)
                      arr)
                    (let [ptr-addr (.get full-seg ffi/VL-LONG (+ off 8))
                          ^MemorySegment ptr-seg (.reinterpret (MemorySegment/ofAddress ptr-addr) slen)
                          arr (byte-array slen)]
                      (MemorySegment/copy ptr-seg ffi/VL-BYTE (long 0) arr 0 slen)
                      arr)))))))

        :DUCKDB_TYPE_INTERVAL
        (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 16 n-rows))]
          (reify ObjectReader
            (elemwiseDatatype [_] :object)
            (lsize [_] n-rows)
            (readObject [_ idx]
              (let [off (long (* idx 16))
                    months (int (.get seg ffi/VL-INT off))
                    days (int (.get seg ffi/VL-INT (+ off 4)))
                    micros (.get seg ffi/VL-LONG (+ off 8))]
                {:months months :days days :micros micros}))))

        :DUCKDB_TYPE_DECIMAL
        (let [scale (long (:scale type-info))
              internal-type (long (:internal-type type-info))
              internal-kw (get ffi/duckdb-type-map internal-type)]
          (case internal-kw
            :DUCKDB_TYPE_SMALLINT
            (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 2 n-rows))]
              (reify ObjectReader
                (elemwiseDatatype [_] :object)
                (lsize [_] n-rows)
                (readObject [_ idx]
                  (BigDecimal/valueOf (long (.get seg ffi/VL-SHORT (long (* idx 2)))) (int scale)))))
            :DUCKDB_TYPE_INTEGER
            (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 4 n-rows))]
              (reify ObjectReader
                (elemwiseDatatype [_] :object)
                (lsize [_] n-rows)
                (readObject [_ idx]
                  (BigDecimal/valueOf (long (.get seg ffi/VL-INT (long (* idx 4)))) (int scale)))))
            :DUCKDB_TYPE_BIGINT
            (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 8 n-rows))]
              (reify ObjectReader
                (elemwiseDatatype [_] :object)
                (lsize [_] n-rows)
                (readObject [_ idx]
                  (BigDecimal/valueOf (.get seg ffi/VL-LONG (long (* idx 8))) (int scale)))))
            :DUCKDB_TYPE_HUGEINT
            (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 16 n-rows))]
              (reify ObjectReader
                (elemwiseDatatype [_] :object)
                (lsize [_] n-rows)
                (readObject [_ idx]
                  (let [off (long (* idx 16))
                        lower (.get seg ffi/VL-LONG off)
                        upper (.get seg ffi/VL-LONG (+ off 8))
                        lower-bi (if (neg? lower)
                                   (.add (BigInteger/valueOf lower) (.shiftLeft BigInteger/ONE 64))
                                   (BigInteger/valueOf lower))]
                    (BigDecimal. ^BigInteger (.add (.shiftLeft (BigInteger/valueOf upper) 64) lower-bi)
                                 (int scale))))))
            (throw (RuntimeException. (str "Unsupported DECIMAL internal type: " internal-kw)))))

        :DUCKDB_TYPE_ENUM
        (let [dict (:enum-dict type-info)
              internal-type (long (:internal-type type-info))
              internal-kw (get ffi/duckdb-type-map internal-type)]
          (case internal-kw
            :DUCKDB_TYPE_UTINYINT
            (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) n-rows)]
              (reify ObjectReader
                (elemwiseDatatype [_] :string)
                (lsize [_] n-rows)
                (readObject [_ idx]
                  (when-not (.contains missing (int idx))
                    (let [i (Byte/toUnsignedInt (.get seg ffi/VL-BYTE (long idx)))]
                      (nth dict i))))))
            :DUCKDB_TYPE_USMALLINT
            (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 2 n-rows))]
              (reify ObjectReader
                (elemwiseDatatype [_] :string)
                (lsize [_] n-rows)
                (readObject [_ idx]
                  (when-not (.contains missing (int idx))
                    (let [i (Short/toUnsignedInt (.get seg ffi/VL-SHORT (long (* idx 2))))]
                      (nth dict i))))))
            :DUCKDB_TYPE_UINTEGER
            (let [^MemorySegment seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 4 n-rows))]
              (reify ObjectReader
                (elemwiseDatatype [_] :string)
                (lsize [_] n-rows)
                (readObject [_ idx]
                  (when-not (.contains missing (int idx))
                    (let [i (Integer/toUnsignedLong (.get seg ffi/VL-INT (long (* idx 4))))]
                      (nth dict i))))))
            (throw (RuntimeException. (str "Unsupported ENUM internal type: " internal-kw)))))

        :DUCKDB_TYPE_LIST
        (let [^MemorySegment child-vec (ffi/duckdb_list_vector_get_child vector-seg)
              child-size (ffi/duckdb_list_vector_get_size vector-seg)
              ^MemorySegment child-data (ffi/duckdb_vector_get_data child-vec)
              ^MemorySegment child-validity (ffi/duckdb_vector_get_validity child-vec)
              child-missing (validity->missing child-size child-validity)
              child-buf (coldata->buffer child-missing child-size (:child-type type-info) (.address child-data) child-vec)
              ;; list_entry_t: offset (uint64) + length (uint64) = 16 bytes per entry
              ^MemorySegment entry-seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 16 n-rows))]
          (reify ObjectReader
            (elemwiseDatatype [_] :object)
            (lsize [_] n-rows)
            (readObject [_ idx]
              ;; NULL list entries have garbage offset/length — skip them
              (when-not (.contains missing (int idx))
                (let [off (long (* idx 16))
                      list-offset (.get entry-seg ffi/VL-LONG off)
                      list-length (.get entry-seg ffi/VL-LONG (+ off 8))]
                  (mapv (fn [i]
                          (let [ci (+ list-offset i)]
                            (if (.contains ^RoaringBitmap child-missing (int ci))
                              nil
                              (child-buf ci))))
                        (range list-length)))))))

        :DUCKDB_TYPE_MAP
        ;; MAP is internally LIST<STRUCT{key, value}>
        (let [^MemorySegment child-vec (ffi/duckdb_list_vector_get_child vector-seg)
              child-size (ffi/duckdb_list_vector_get_size vector-seg)
              ^MemorySegment child-data (ffi/duckdb_vector_get_data child-vec)
              ^MemorySegment child-validity (ffi/duckdb_vector_get_validity child-vec)
              child-missing (validity->missing child-size child-validity)
              child-buf (coldata->buffer child-missing child-size (:child-type type-info) (.address child-data) child-vec)
              ^MemorySegment entry-seg (.reinterpret (MemorySegment/ofAddress data-ptr) (* 16 n-rows))]
          (reify ObjectReader
            (elemwiseDatatype [_] :object)
            (lsize [_] n-rows)
            (readObject [_ idx]
              (when-not (.contains missing (int idx))
                (let [off (long (* idx 16))
                      list-offset (.get entry-seg ffi/VL-LONG off)
                      list-length (.get entry-seg ffi/VL-LONG (+ off 8))]
                  (persistent!
                   (reduce (fn [m i]
                             (let [ci (+ list-offset i)
                                   entry (child-buf ci)]
                               (assoc! m (:key entry) (:value entry))))
                           (transient {})
                           (range list-length))))))))

        :DUCKDB_TYPE_STRUCT
        (let [child-count (long (:child-count type-info))
              child-names (:child-names type-info)
              child-types (:child-types type-info)
              children (mapv (fn [ci]
                               (let [^MemorySegment cvec (ffi/duckdb_struct_vector_get_child vector-seg (long ci))
                                     ^MemorySegment cdata (ffi/duckdb_vector_get_data cvec)
                                     ^MemorySegment cvalid (ffi/duckdb_vector_get_validity cvec)
                                     cmissing (validity->missing n-rows cvalid)]
                                 {:buf (coldata->buffer cmissing n-rows (child-types ci) (.address cdata) cvec)
                                  :missing cmissing
                                  :name (keyword (child-names ci))}))
                             (range child-count))]
          (reify ObjectReader
            (elemwiseDatatype [_] :object)
            (lsize [_] n-rows)
            (readObject [_ idx]
              (persistent!
               (reduce (fn [m {:keys [buf missing name]}]
                         (assoc! m name
                                 (if (.contains ^RoaringBitmap missing (int idx))
                                   nil
                                   (buf idx))))
                       (transient {})
                       children)))))

        (throw (RuntimeException. (format "Unsupported column type: %s" type-kw)))))))

;; ---------------------------------------------------------------------------
;; 9. ResultChunks type
;; ---------------------------------------------------------------------------

(defn- supplier-seq [^Supplier s]
  (when-let [item (.get s)]
    (cons item (lazy-seq (supplier-seq s)))))

(deftype ^:private ResultChunks [sql result-seg destroy-result* realize-chunk reduce-type]
  AutoCloseable
  (close [_] @destroy-result*)
  Supplier
  (get [_]
    (let [^MemorySegment chunk (ffi/duckdb_fetch_chunk result-seg)]
      (when (and chunk (not= 0 (.address chunk)))
        (let [ds (realize-chunk chunk true)]
          (ffi/destroy-ptr! ffi/duckdb_destroy_data_chunk chunk)
          ds))))
  ITypedReduce
  (reduce [_ rfn acc]
    (loop [acc acc]
      (let [^MemorySegment chunk (instr-time :fetch-ns (ffi/duckdb_fetch_chunk result-seg))]
        (if (and chunk (not= 0 (.address chunk)) (not (reduced? acc)))
          (do
            (instr-inc! :chunks)
            (let [result (case reduce-type
                           :clone
                           (let [ds (realize-chunk chunk true)]
                             (ffi/destroy-ptr! ffi/duckdb_destroy_data_chunk chunk)
                             (rfn acc ds))
                           :zero-copy
                           (rfn acc (realize-chunk chunk false)))]
              (recur result)))
          (do
            ;; Destroy last null chunk if needed
            (when (and chunk (not= 0 (.address chunk)))
              (ffi/destroy-ptr! ffi/duckdb_destroy_data_chunk chunk))
            (if (reduced? acc) @acc acc))))))
  Iterable
  (iterator [this] (clojure.lang.SeqIterator. (.seq this)))
  Seqable
  (seq [this] (supplier-seq this))
  Object
  (toString [_] (str "#duckdb-result[\"" sql "\"]")))

;; ---------------------------------------------------------------------------
;; 10. extract-type-info and results->datasets helper
;; ---------------------------------------------------------------------------

(defn- extract-type-info
  "Extract type metadata from a DuckDB logical type.
  Returns a map with :type-id, :type-kw, and type-specific keys."
  [^MemorySegment lt]
  (let [tid (ffi/duckdb_get_type_id lt)
        tkw (get ffi/duckdb-type-map tid)
        base {:type-id tid :type-kw tkw}]
    (case tkw
      :DUCKDB_TYPE_DECIMAL
      (assoc base
             :scale (ffi/duckdb_decimal_scale lt)
             :width (ffi/duckdb_decimal_width lt)
             :internal-type (ffi/duckdb_decimal_internal_type lt))

      :DUCKDB_TYPE_ENUM
      (let [dict-size (ffi/duckdb_enum_dictionary_size lt)
            dict (mapv (fn [i]
                         (let [^MemorySegment s (ffi/duckdb_enum_dictionary_value lt (long i))
                               v (ffi/read-c-str s)]
                           (ffi/duckdb_free s)
                           v))
                       (range dict-size))]
        (assoc base
               :enum-dict dict
               :internal-type (ffi/duckdb_enum_internal_type lt)))

      :DUCKDB_TYPE_LIST
      (let [^MemorySegment child-lt (ffi/duckdb_list_type_child_type lt)
            child-info (extract-type-info child-lt)]
        (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type child-lt)
        (assoc base :child-type child-info))

      :DUCKDB_TYPE_MAP
      (let [^MemorySegment child-lt (ffi/duckdb_list_type_child_type lt)
            child-info (extract-type-info child-lt)]
        (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type child-lt)
        (assoc base :child-type child-info))

      :DUCKDB_TYPE_STRUCT
      (let [n (ffi/duckdb_struct_type_child_count lt)
            names (mapv (fn [i]
                          (let [^MemorySegment s (ffi/duckdb_struct_type_child_name lt (long i))
                                v (ffi/read-c-str s)]
                            (ffi/duckdb_free s)
                            v))
                        (range n))
            child-infos (mapv (fn [i]
                                (let [^MemorySegment clt (ffi/duckdb_struct_type_child_type lt (long i))
                                      ci (extract-type-info clt)]
                                  (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type clt)
                                  ci))
                              (range n))]
        (assoc base
               :child-count n
               :child-names names
               :child-types child-infos))

      :DUCKDB_TYPE_ARRAY
      (let [^MemorySegment child-lt (ffi/duckdb_array_type_child_type lt)
            child-info (extract-type-info child-lt)
            arr-size (ffi/duckdb_array_type_array_size lt)]
        (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type child-lt)
        (assoc base :child-type child-info :array-size arr-size))

      ;; Default: just the type ID
      base)))

(defn- results->datasets
  ^AutoCloseable [sql result-seg destroy-result* options]
  (let [n-cols (ffi/duckdb_column_count result-seg)
        names (mapv (fn [i] (ffi/read-c-str ^MemorySegment (ffi/duckdb_column_name result-seg (long i)))) (range n-cols))
        type-infos (mapv (fn [i]
                           (let [^MemorySegment lt (ffi/duckdb_column_logical_type result-seg (long i))
                                 info (extract-type-info lt)]
                             (ffi/destroy-ptr! ffi/duckdb_destroy_logical_type lt)
                             info))
                         (range n-cols))
        key-fn (get options :key-fn identity)
        realize-chunk (fn [^MemorySegment data-chunk clone?]
                        (instr-time :realize-wallclock-ns
                         (let [n-rows (ffi/duckdb_data_chunk_get_size data-chunk)
                               colmap (hamf/mut-map)
                               ;; Build delays that construct the Column. Each future runs
                               ;; clone eagerly in the agent pool, so cloning across columns
                               ;; overlaps. Forcing them via mapv deref awaits all in parallel.
                               columns
                               (hamf/mapv
                                (fn [cidx]
                                  (instr-inc! :columns)
                                  (let [^MemorySegment vdata (ffi/duckdb_data_chunk_get_vector data-chunk (long cidx))
                                        ^MemorySegment data-ptr (ffi/duckdb_vector_get_data vdata)
                                        ^MemorySegment val-ptr (ffi/duckdb_vector_get_validity vdata)
                                        missing (instr-time :validity-ns
                                                  (validity->missing n-rows val-ptr))
                                        coldata (instr-time :buffer-ns
                                                  (coldata->buffer missing n-rows (type-infos cidx) (.address data-ptr) vdata))
                                        cname (key-fn (names cidx))
                                        delayed-data (if clone?
                                                       (do (instr-inc! :clones)
                                                           (future (instr-time :clone-cpu-ns
                                                                     (dt/clone coldata))))
                                                       (delay coldata))]
                                    (.put colmap cname cidx)
                                    (delay (Column. missing @delayed-data {:name cname} nil))))
                                (hamf/range n-cols))
                               col-data (instr-time :deref-wait-ns
                                          (hamf/mapv deref columns))]
                           (Dataset. col-data
                                     (persistent! colmap)
                                     {:name :_unnamed}
                                     0 0))))
        reduce-type (get options :reduce-type :clone)]
    (ResultChunks. sql result-seg destroy-result* realize-chunk reduce-type)))

;; ---------------------------------------------------------------------------
;; 11. datasets->dataset
;; ---------------------------------------------------------------------------

(defn- datasets->dataset [^AutoCloseable results]
  (let [dsdata (vec results)]
    (if (== 1 (count dsdata))
      (instr-time :concat-ns (dt/clone (dsdata 0)))
      (instr-time :concat-ns (apply ds/concat dsdata)))))

;; ---------------------------------------------------------------------------
;; 12. Prepared statements
;; ---------------------------------------------------------------------------

(defn- bind-prepare-param [^MemorySegment stmt ^long idx v ^Arena arena]
  (cond
    (nil? v) (ffi/duckdb_bind_null stmt idx)
    (instance? Boolean v) (ffi/duckdb_bind_boolean stmt idx (byte (if v 1 0)))
    (integer? v) (ffi/duckdb_bind_int64 stmt idx (long v))
    (float? v) (ffi/duckdb_bind_double stmt idx (double v))
    (double? v) (ffi/duckdb_bind_double stmt idx (double v))
    (string? v) (let [^bytes bval (.getBytes ^String v "UTF-8")
                      seg (let [s (.allocate arena (alength bval))]
                            (MemorySegment/copy bval 0 s ffi/VL-BYTE 0 (alength bval))
                            s)]
                  (ffi/duckdb_bind_varchar_length stmt idx seg (long (alength bval))))
    (instance? LocalDate v) (ffi/duckdb_bind_date stmt idx (int (.toEpochDay ^LocalDate v)))
    (instance? LocalTime v) (ffi/duckdb_bind_time stmt idx (long (quot (.toNanoOfDay ^LocalTime v) 1000)))
    (instance? Instant v) (ffi/duckdb_bind_timestamp stmt idx
                                                     (long (+ (* (.getEpochSecond ^Instant v) 1000000)
                                                              (quot (.getNano ^Instant v) 1000))))
    :else (throw (RuntimeException. (str "Cannot bind value: " v)))))

(deftype ^:private PrepStatement0 [sql destroy-prep* finalize-stmt]
  Object (toString [_] (str "#prepared-stmt-0[\"" sql "\"]"))
  IFnDef (invoke [_] (finalize-stmt))
  AutoCloseable (close [_] @destroy-prep*))

(deftype ^:private PrepStatement1 [sql ^MemorySegment stmt param-types destroy-prep* finalize-stmt]
  Object (toString [_] (str "#prepared-stmt-1[\"" sql "\"]"))
  IFnDef
  (invoke [_ v0]
    (with-open [a (Arena/ofConfined)]
      (bind-prepare-param stmt 1 v0 a)
      (finalize-stmt)))
  AutoCloseable (close [_] @destroy-prep*))

(declare prepare)

(defn sql->datasets
  (^AutoCloseable [^long conn ^String sql options]
   (with-open [^AutoCloseable stmt (prepare conn sql options)]
     (stmt)))
  (^AutoCloseable [conn sql] (sql->datasets conn sql nil)))

(defn sql->dataset
  ([conn sql options]
   (sql->datasets conn sql (assoc options :result-type :single)))
  ([conn sql] (sql->dataset conn sql nil)))

(defn prepare
  (^AutoCloseable [^long conn ^String sql] (prepare conn sql nil))
  (^AutoCloseable [^long conn ^String sql options]
   (let [arena (Arena/ofConfined)
         conn-seg (MemorySegment/ofAddress conn)
         stmt-ptr (.allocate arena ffi/VL-ADDR)
         rc (ffi/duckdb_prepare conn-seg (ffi/alloc-c-str arena sql) stmt-ptr)
         ^MemorySegment stmt (.get stmt-ptr ffi/VL-ADDR 0)
         destroy-prep* (delay
                         (ffi/destroy-ptr! ffi/duckdb_destroy_prepare stmt)
                         (.close arena))
         _ (when-not (== rc 0)
             (let [err (ffi/read-c-str ^MemorySegment (ffi/duckdb_prepare_error stmt))]
               @destroy-prep*
               (throw (RuntimeException. (str "Error preparing: " err)))))
         result-type (get options :result-type :streaming)
         finalize-stmt
         (fn []
           (let [pend-arena (Arena/ofConfined)]
             (try
               (let [pending-ptr (.allocate pend-arena ffi/VL-ADDR)
                     prc (ffi/duckdb_pending_prepared stmt pending-ptr)
                     ^MemorySegment pending (.get pending-ptr ffi/VL-ADDR 0)
                     _ (when-not (== prc 0)
                         (let [err (or (ffi/read-c-str ^MemorySegment (ffi/duckdb_pending_error pending))
                                       "Unknown Error")]
                           (ffi/destroy-ptr! ffi/duckdb_destroy_pending pending)
                           (throw (RuntimeException. (str "Pending error: " err)))))
                     result-seg (.allocate pend-arena (.byteSize ^MemoryLayout ffi/result-layout))
                     erc (ffi/duckdb_execute_pending pending result-seg)
                     _ (when-not (== erc 0)
                         (let [err (or (ffi/read-c-str ^MemorySegment (ffi/duckdb_pending_error pending))
                                       "Unknown Error")]
                           (ffi/destroy-ptr! ffi/duckdb_destroy_pending pending)
                           (ffi/duckdb_destroy_result result-seg)
                           (throw (RuntimeException. (str "Execute error: " err)))))
                     _ (ffi/destroy-ptr! ffi/duckdb_destroy_pending pending)
                     destroy-result* (delay (ffi/duckdb_destroy_result result-seg)
                                            (.close pend-arena))
                     opts (if (= result-type :single)
                            (assoc options :reduce-type :zero-copy)
                            options)
                     res-data (results->datasets sql result-seg destroy-result* opts)]
                 (case result-type
                   :streaming res-data
                   :realized res-data
                   :single (with-open [^AutoCloseable res-data res-data]
                             (datasets->dataset res-data))))
               (catch Exception e
                 (try (.close pend-arena) (catch IllegalStateException _))
                 (throw e)))))
         n-params (ffi/duckdb_nparams stmt)
         param-types (mapv #(ffi/duckdb_param_type stmt (long (inc %))) (range n-params))]
     (case (int n-params)
       0 (PrepStatement0. sql destroy-prep* finalize-stmt)
       1 (PrepStatement1. sql stmt param-types destroy-prep* finalize-stmt)
       ;; For 2+ params, use a general N-arity version
       (reify
         Object (toString [_] (str "#prepared-stmt-" n-params "[\"" sql "\"]"))
         IFnDef
         (applyTo [_ args]
           (with-open [a (Arena/ofConfined)]
             (doall (map-indexed (fn [i v]
                                   (bind-prepare-param stmt (long (inc i)) v a))
                                 args))
             (finalize-stmt)))
         AutoCloseable (close [_] @destroy-prep*))))))
