(ns ducktape.core
  (:require [ducktape.ffi :as ffi]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype :as dt]
            [tech.v3.datatype.datetime :as dt-datetime]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.resource :as resource]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.sql :as sql]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [clojure.tools.logging :as log])
  (:import [java.lang.foreign Arena MemoryLayout MemorySegment ValueLayout]
           [java.util UUID ArrayList]
           [java.util.function Supplier]
           [java.time LocalDate LocalTime Instant]
           [ham_fisted ITypedReduce IFnDef$LO IFnDef Casts]
           [tech.v3.datatype ObjectReader UnsafeUtil]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang Seqable IReduceInit]
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
         (let [cp (.allocate arena ffi/VL-ADDR)]
           (.set cp ffi/VL-ADDR 0 config-ptr)
           (ffi/duckdb_destroy_config cp)))
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
  ([^long conn ^String sql options]
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
  ([conn sql] (run-query! conn sql nil)))

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

(defn- dtype-type->duckdb [dt]
  (case dt
    :boolean   ffi/DUCKDB_TYPE_BOOLEAN
    :int8      ffi/DUCKDB_TYPE_TINYINT
    :uint8     ffi/DUCKDB_TYPE_UTINYINT
    :int16     ffi/DUCKDB_TYPE_SMALLINT
    :uint16    ffi/DUCKDB_TYPE_USMALLINT
    :int32     ffi/DUCKDB_TYPE_INTEGER
    :uint32    ffi/DUCKDB_TYPE_UINTEGER
    :int64     ffi/DUCKDB_TYPE_BIGINT
    :uint64    ffi/DUCKDB_TYPE_UBIGINT
    :float32   ffi/DUCKDB_TYPE_FLOAT
    :float64   ffi/DUCKDB_TYPE_DOUBLE
    :local-date ffi/DUCKDB_TYPE_DATE
    :local-time ffi/DUCKDB_TYPE_TIME
    :instant   ffi/DUCKDB_TYPE_TIMESTAMP
    :string    ffi/DUCKDB_TYPE_VARCHAR
    :uuid      ffi/DUCKDB_TYPE_UUID))

(defn insert-dataset!
  ([^long conn dataset options]
   (with-open [arena (Arena/ofConfined)]
     (let [table-name (sql/table-name dataset options)
           conn-seg (MemorySegment/ofAddress conn)
             app-ptr (.allocate arena ffi/VL-ADDR)
             app-rc (ffi/duckdb_appender_create
                      conn-seg
                      (ffi/alloc-c-str arena "")
                      (ffi/alloc-c-str arena table-name)
                      app-ptr)
             ^MemorySegment appender (.get app-ptr ffi/VL-ADDR 0)
             check-error (fn [status]
                           (when-not (== (int status) ffi/DuckDBSuccess)
                             (let [err (ffi/read-c-str ^MemorySegment (ffi/duckdb_appender_error appender))]
                               (throw (Exception. (str "Appender error: " err))))))
             _ (check-error app-rc)
             n-rows (ds/row-count dataset)
             n-cols (ds/column-count dataset)
             colvec (vec (ds/columns dataset))
             dtypes (mapv (comp packing/unpack-datatype dt/elemwise-datatype) colvec)
             duckdb-type-ids (mapv dtype-type->duckdb dtypes)
             chunk-size (ffi/duckdb_vector_size)
             n-chunks (long (Math/ceil (/ (double n-rows) chunk-size)))
             ;; Create logical types array
             types-seg (.allocate arena (* n-cols 8))
             _ (dotimes [ci n-cols]
                 (.set types-seg ffi/VL-ADDR (long (* ci 8))
                   ^MemorySegment (ffi/duckdb_create_logical_type (int (duckdb-type-ids ci)))))
             ^MemorySegment write-chunk (ffi/duckdb_create_data_chunk types-seg (long n-cols))]
         (try
           (dotimes [chunk-idx n-chunks]
             (let [row-off (long (* chunk-idx chunk-size))
                   row-count (long (min chunk-size (- n-rows row-off)))
                   n-valid (long (Math/ceil (/ (double row-count) 64)))]
               (ffi/duckdb_data_chunk_set_size write-chunk (long row-count))
               (dotimes [col-idx n-cols]
                 (let [^MemorySegment dvec (ffi/duckdb_data_chunk_get_vector write-chunk (long col-idx))
                       _ (ffi/duckdb_vector_ensure_validity_writable dvec)
                       ^MemorySegment data-ptr (ffi/duckdb_vector_get_data dvec)
                       ^MemorySegment validity-ptr (ffi/duckdb_vector_get_validity dvec)
                       validity-seg (.reinterpret validity-ptr (* n-valid 8))
                       subcol (dt/sub-buffer (colvec col-idx) row-off row-count)
                       missing (ds/missing subcol)
                       missing-card (long (.getCardinality ^RoaringBitmap missing))
                       col-dt (dtypes col-idx)
                       daddr (.address data-ptr)]
                   ;; Write validity
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
                       (let [miter (.getIntIterator ^RoaringBitmap missing)]
                         (loop []
                           (when (.hasNext miter)
                             (let [ne (Integer/toUnsignedLong (.next miter))
                                   vidx (quot ne 64)
                                   bit-idx (rem ne 64)
                                   cv (.get validity-seg ffi/VL-LONG (long (* vidx 8)))]
                               (.set validity-seg ffi/VL-LONG (long (* vidx 8))
                                 (bit-and-not cv (bit-shift-left 1 bit-idx)))
                               (recur)))))))
                   ;; Write column data
                   (let [wrap-addr (fn [addr n-bytes dtype]
                                     (-> (native-buffer/wrap-address addr n-bytes nil)
                                         (native-buffer/set-native-datatype dtype)))]
                     (case col-dt
                       (:int8 :uint8 :boolean)
                       (dt/copy! subcol (wrap-addr daddr row-count (if (= :boolean col-dt) :int8 col-dt)))
                       (:int16 :uint16)
                       (dt/copy! subcol (wrap-addr daddr (* 2 row-count) col-dt))
                       (:int32 :uint32 :float32)
                       (dt/copy! subcol (wrap-addr daddr (* 4 row-count) col-dt))
                       (:int64 :uint64 :float64)
                       (dt/copy! subcol (wrap-addr daddr (* 8 row-count) col-dt))
                       (:local-date :packed-local-date)
                       (dt/copy! (if (= col-dt :local-date) (packing/pack subcol) subcol)
                                 (wrap-addr daddr (* 4 row-count) :int32))
                       (:local-time :packed-local-time)
                       (dt/copy! (if (= col-dt :local-time) (packing/pack subcol) subcol)
                                 (wrap-addr daddr (* 8 row-count) :int64))
                       (:instant :packed-instant)
                       (dt/copy! (if (= col-dt :instant) (packing/pack subcol) subcol)
                                 (wrap-addr daddr (* 8 row-count) :int64))
                       :uuid
                       (let [^MemorySegment dseg (.reinterpret data-ptr (* 16 row-count))]
                         (dotimes [i row-count]
                           (let [^UUID uuid (subcol i)
                                 off (long (* i 16))]
                             (if uuid
                               (do (.set dseg ffi/VL-LONG off (.getLeastSignificantBits uuid))
                                   (.set dseg ffi/VL-LONG (+ off 8) (.getMostSignificantBits uuid)))
                               (do (.set dseg ffi/VL-LONG off (long 0))
                                   (.set dseg ffi/VL-LONG (+ off 8) (long 0)))))))
                       (:string :text)
                       (let [^MemorySegment dseg (.reinterpret data-ptr (* 16 row-count))]
                         (dotimes [i row-count]
                           (let [sval (str (subcol i))
                                 ^bytes sbytes (.getBytes sval "UTF-8")
                                 slen (alength sbytes)
                                 off (long (* i 16))]
                             (.set dseg ffi/VL-INT off (int slen))
                             (if (<= slen 12)
                               (MemorySegment/copy sbytes 0 dseg ffi/VL-BYTE (+ off 4) slen)
                               (let [^MemorySegment buf (.allocate arena (long slen))]
                                 (MemorySegment/copy sbytes 0 buf ffi/VL-BYTE 0 slen)
                                 (.set dseg ffi/VL-LONG (+ off 8) (.address buf)))))))))))
               ;; end of col loop — append chunk and reset
               (check-error (ffi/duckdb_append_data_chunk appender write-chunk))
               (ffi/duckdb_data_chunk_reset write-chunk)))
           n-rows
           (finally
             ;; Destroy appender
             (let [p (.allocate arena ffi/VL-ADDR)]
               (.set p ffi/VL-ADDR 0 appender)
               (ffi/duckdb_appender_destroy p))
             ;; Destroy write chunk
             (let [p (.allocate arena ffi/VL-ADDR)]
               (.set p ffi/VL-ADDR 0 write-chunk)
               (ffi/duckdb_destroy_data_chunk p))
             ;; Destroy logical types
             (dotimes [ci n-cols]
               (let [lt (.get types-seg ffi/VL-ADDR (long (* ci 8)))
                     p (.allocate arena ffi/VL-ADDR)]
                 (.set p ffi/VL-ADDR 0 lt)
                 (ffi/duckdb_destroy_logical_type p))))))))
  ([conn dataset] (insert-dataset! conn dataset nil)))

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
            (let [logical-idx (* idx 64)]
              (dotimes [bit-idx 64]
                (when (== 0 (bit-and lval (bit-shift-left 1 bit-idx)))
                  (.add rval (unchecked-int (+ bit-idx logical-idx)))))))))
      rval)))

(defn- coldata->buffer
  [^RoaringBitmap missing ^long n-rows ^long duckdb-type ^long data-ptr]
  (case (get ffi/duckdb-type-map duckdb-type)
    :DUCKDB_TYPE_BOOLEAN
    (-> (native-buffer/wrap-address data-ptr n-rows nil)
        (dt/elemwise-cast :boolean))

    :DUCKDB_TYPE_TINYINT
    (native-buffer/wrap-address data-ptr n-rows nil)

    :DUCKDB_TYPE_SMALLINT
    (-> (native-buffer/wrap-address data-ptr (* 2 n-rows) nil)
        (native-buffer/set-native-datatype :int16))

    :DUCKDB_TYPE_INTEGER
    (-> (native-buffer/wrap-address data-ptr (* 4 n-rows) nil)
        (native-buffer/set-native-datatype :int32))

    :DUCKDB_TYPE_BIGINT
    (-> (native-buffer/wrap-address data-ptr (* 8 n-rows) nil)
        (native-buffer/set-native-datatype :int64))

    :DUCKDB_TYPE_UTINYINT
    (-> (native-buffer/wrap-address data-ptr n-rows nil)
        (native-buffer/set-native-datatype :uint8))

    :DUCKDB_TYPE_USMALLINT
    (-> (native-buffer/wrap-address data-ptr (* 2 n-rows) nil)
        (native-buffer/set-native-datatype :uint16))

    :DUCKDB_TYPE_UINTEGER
    (-> (native-buffer/wrap-address data-ptr (* 4 n-rows) nil)
        (native-buffer/set-native-datatype :uint32))

    :DUCKDB_TYPE_UBIGINT
    (-> (native-buffer/wrap-address data-ptr (* 8 n-rows) nil)
        (native-buffer/set-native-datatype :uint64))

    :DUCKDB_TYPE_FLOAT
    (-> (native-buffer/wrap-address data-ptr (* 4 n-rows) nil)
        (native-buffer/set-native-datatype :float32))

    :DUCKDB_TYPE_DOUBLE
    (-> (native-buffer/wrap-address data-ptr (* 8 n-rows) nil)
        (native-buffer/set-native-datatype :float64))

    :DUCKDB_TYPE_DATE
    (-> (native-buffer/wrap-address data-ptr (* 4 n-rows) nil)
        (native-buffer/set-native-datatype :packed-local-date))

    :DUCKDB_TYPE_TIME
    (-> (native-buffer/wrap-address data-ptr (* 8 n-rows) nil)
        (native-buffer/set-native-datatype :packed-local-time))

    :DUCKDB_TYPE_TIMESTAMP
    (-> (native-buffer/wrap-address data-ptr (* 8 n-rows) nil)
        (native-buffer/set-native-datatype :packed-instant))

    :DUCKDB_TYPE_UUID
    (let [seg (-> (MemorySegment/ofAddress data-ptr)
                  (.reinterpret (* 16 n-rows)))]
      (reify ObjectReader
        (elemwiseDatatype [_] :uuid)
        (lsize [_] n-rows)
        (readObject [_ idx]
          (let [off (long (* idx 16))
                low (.get seg ffi/VL-LONG off)
                high (.get seg ffi/VL-LONG (+ off 8))]
            (UUID. high low)))))

    :DUCKDB_TYPE_VARCHAR
    (let [seg (-> (MemorySegment/ofAddress data-ptr)
                  (.reinterpret (* 16 n-rows)))]
      (reify ObjectReader
        (elemwiseDatatype [_] :string)
        (lsize [_] n-rows)
        (readObject [_ idx]
          (if (.contains missing (unchecked-int idx))
            ""
            (let [off (long (* idx 16))
                  slen (int (.get seg ffi/VL-INT off))]
              (if (<= slen 12)
                (let [arr (byte-array slen)]
                  (MemorySegment/copy seg ffi/VL-BYTE (+ off 4) arr 0 slen)
                  (String. arr "UTF-8"))
                (let [ptr-addr (.get seg ffi/VL-LONG (+ off 8))
                      ^MemorySegment ptr-seg (.reinterpret (MemorySegment/ofAddress ptr-addr) slen)
                      arr (byte-array slen)]
                  (MemorySegment/copy ptr-seg ffi/VL-BYTE 0 arr 0 slen)
                  (String. arr "UTF-8"))))))))

    (throw (RuntimeException. (format "Unsupported column type: %d" duckdb-type)))))

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
          (with-open [a (Arena/ofConfined)]
            (let [p (.allocate a ffi/VL-ADDR)]
              (.set p ffi/VL-ADDR 0 chunk)
              (ffi/duckdb_destroy_data_chunk p)))
          ds))))
  ITypedReduce
  (reduce [_ rfn acc]
    (loop [acc acc]
      (let [^MemorySegment chunk (ffi/duckdb_fetch_chunk result-seg)]
        (if (and chunk (not= 0 (.address chunk)) (not (reduced? acc)))
          (let [result (case reduce-type
                         :clone
                         (let [ds (realize-chunk chunk true)]
                           (with-open [a (Arena/ofConfined)]
                             (let [p (.allocate a ffi/VL-ADDR)]
                               (.set p ffi/VL-ADDR 0 chunk)
                               (ffi/duckdb_destroy_data_chunk p)))
                           (rfn acc ds))
                         :zero-copy
                         (rfn acc (realize-chunk chunk false)))]
            (recur result))
          (do
            ;; Destroy last null chunk if needed
            (when (and chunk (not= 0 (.address chunk)))
              (with-open [a (Arena/ofConfined)]
                (let [p (.allocate a ffi/VL-ADDR)]
                  (.set p ffi/VL-ADDR 0 chunk)
                  (ffi/duckdb_destroy_data_chunk p))))
            (if (reduced? acc) @acc acc))))))
  Iterable
  (iterator [this] (clojure.lang.SeqIterator. (.seq this)))
  Seqable
  (seq [this] (supplier-seq this))
  Object
  (toString [_] (str "#duckdb-result[\"" sql "\"]")))

;; ---------------------------------------------------------------------------
;; 10. results->datasets helper
;; ---------------------------------------------------------------------------

(defn- results->datasets
  ^AutoCloseable [sql result-seg destroy-result* options]
  (let [n-cols (ffi/duckdb_column_count result-seg)
        names (mapv (fn [i] (ffi/read-c-str ^MemorySegment (ffi/duckdb_column_name result-seg (long i)))) (range n-cols))
        type-ids (mapv (fn [i]
                         (let [^MemorySegment lt (ffi/duckdb_column_logical_type result-seg (long i))
                               tid (ffi/duckdb_get_type_id lt)]
                           ;; Destroy the logical type
                           (with-open [a (Arena/ofConfined)]
                             (let [p (.allocate a ffi/VL-ADDR)]
                               (.set p ffi/VL-ADDR 0 lt)
                               (ffi/duckdb_destroy_logical_type p)))
                           tid))
                       (range n-cols))
        key-fn (get options :key-fn identity)
        realize-chunk (fn [^MemorySegment data-chunk clone?]
                        (let [n-rows (ffi/duckdb_data_chunk_get_size data-chunk)
                              colmap (hamf/mut-map)
                              columns
                              (mapv (fn [cidx]
                                      (let [^MemorySegment vdata (ffi/duckdb_data_chunk_get_vector data-chunk (long cidx))
                                            ^MemorySegment data-ptr (ffi/duckdb_vector_get_data vdata)
                                            ^MemorySegment val-ptr (ffi/duckdb_vector_get_validity vdata)
                                            missing (validity->missing n-rows val-ptr)
                                            coldata (coldata->buffer missing n-rows (long (type-ids cidx)) (.address data-ptr))
                                            cname (key-fn (names cidx))
                                            cloned (if clone? (dt/clone coldata) coldata)]
                                        (.put colmap cname cidx)
                                        (Column. missing cloned {:name cname} nil)))
                                    (range n-cols))]
                          (Dataset. (vec columns)
                                    (persistent! colmap)
                                    {:name :_unnamed}
                                    0 0)))
        reduce-type (get options :reduce-type :clone)]
    (ResultChunks. sql result-seg destroy-result* realize-chunk reduce-type)))

;; ---------------------------------------------------------------------------
;; 11. datasets->dataset
;; ---------------------------------------------------------------------------

(defn- datasets->dataset [^AutoCloseable results]
  (let [dsdata (vec results)]
    (if (== 1 (count dsdata))
      (dt/clone (dsdata 0))
      (apply ds/concat dsdata))))

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
                         (let [a (Arena/ofConfined)
                               p (.allocate a ffi/VL-ADDR)]
                           (.set p ffi/VL-ADDR 0 stmt)
                           (ffi/duckdb_destroy_prepare p)
                           (.close a))
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
                           (let [p (.allocate pend-arena ffi/VL-ADDR)]
                             (.set p ffi/VL-ADDR 0 pending)
                             (ffi/duckdb_destroy_pending p))
                           (throw (RuntimeException. (str "Pending error: " err)))))
                     result-seg (.allocate pend-arena (.byteSize ^MemoryLayout ffi/result-layout))
                     erc (ffi/duckdb_execute_pending pending result-seg)
                     _ (when-not (== erc 0)
                         (let [err (or (ffi/read-c-str ^MemorySegment (ffi/duckdb_pending_error pending))
                                       "Unknown Error")]
                           (let [p (.allocate pend-arena ffi/VL-ADDR)]
                             (.set p ffi/VL-ADDR 0 pending)
                             (ffi/duckdb_destroy_pending p))
                           (ffi/duckdb_destroy_result result-seg)
                           (throw (RuntimeException. (str "Execute error: " err)))))
                     ;; Destroy pending
                     _ (let [p (.allocate pend-arena ffi/VL-ADDR)]
                         (.set p ffi/VL-ADDR 0 pending)
                         (ffi/duckdb_destroy_pending p))
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
                 (.close pend-arena)
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
