(ns ducktape.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [ducktape.core :as duck]
            [ducktape.ffi :as ffi]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dt]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.resource :as resource])
  (:import [java.util UUID]
           [java.time Instant]
           [java.math BigDecimal BigInteger]))

(duck/initialize!)

(def db* (delay (duck/initialize!)
                (duck/open-db)))

(def conn* (delay (duck/connect @db*)))

(deftest trivial
  (try
    (duck/drop-table! @conn* "trivial")
    (catch Throwable _e nil))
  (let [ds (-> (ds/->dataset [{:a 1}])
               (ds/set-dataset-name "trivial"))]
    (duck/create-table! @conn* ds)
    (duck/insert-dataset! @conn* ds)
    (is (= 1 (-> (duck/sql->dataset @conn* "select * from trivial;")
                 (ds/row-count))))))

(defn supported-datatype-ds
  []
  (-> (ds/->dataset {:boolean [true false true true false false true false false true]
                     :bytes (byte-array (range 10))
                     :shorts (short-array (range 10))
                     :ints (int-array (range 10))
                     :longs (long-array (range 10))
                     :floats (float-array (range 10))
                     :doubles (double-array (range 10))
                     :strings (map str (range 10))
                     :uuids (repeatedly 10 #(UUID/randomUUID))
                     :instants (repeatedly 10 dtype-dt/instant)
                     :local_dates (repeatedly 10 dtype-dt/local-date)
                     :local_times (->> (repeatedly 10 dtype-dt/local-time))})
      (vary-meta assoc
                 :primary-key :longs
                 :name :testtable)))

(deftest basic-datatype-test
  (try
    (let [ds (supported-datatype-ds)]
      (duck/create-table! @conn* ds)
      (duck/insert-dataset! @conn* ds)
      (let [sql-ds (duck/sql->dataset @conn* "select * from testtable"
                                      {:key-fn keyword})]
        (doseq [column (vals ds)]
          (is (= (vec column)
                 (vec (sql-ds (:name (meta column)))))))))
    (finally
      (try (duck/drop-table! @conn* "testtable")
           (catch Throwable _e nil)))))

(defonce stocks-src* (delay
                       (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                                     {:key-fn keyword
                                      :dataset-name :stocks})))

(deftest basic-stocks-test
  (try
    (let [stocks @stocks-src*
          _ (do (duck/create-table! @conn* stocks)
                (duck/insert-dataset! @conn* stocks))
          sql-stocks (duck/sql->dataset @conn* "select * from stocks")]
      (is (= (ds/row-count stocks)
             (ds/row-count sql-stocks)))
      (is (= (vec (stocks :symbol))
             (vec (sql-stocks "symbol"))))
      (is (= (vec (stocks :date))
             (vec (sql-stocks "date"))))
      (is (dfn/equals (stocks :price)
                      (sql-stocks "price"))))
    (finally
      (try
        (duck/drop-table! @conn* "stocks")
        (catch Throwable _e nil)))))

(deftest filter-stonks-test
  (let [stonks (-> (apply ds/concat (repeat 10 @stocks-src*))
                   (ds/row-map (fn [m]
                                 (cond
                                   (< (:price m) 37.) (update m :price (constantly nil))
                                   :else m)))
                   (vary-meta assoc :name :stonks))]
    (try
      (do (duck/create-table! @conn* stonks)
          (duck/insert-dataset! @conn* stonks))
      (let [sql-stocks (duck/sql->dataset @conn* "select * from stonks")]
        (is (= (ds/row-count stonks)
               (ds/row-count sql-stocks)))
        (is (= (vec (stonks :symbol))
               (vec (sql-stocks "symbol"))))
        (is (= (vec (stonks :date))
               (vec (sql-stocks "date"))))
        (is (= (vec (bitmap/->random-access (ds/missing stonks)))
               (vec (bitmap/->random-access (ds/missing sql-stocks)))))
        (is (dfn/equals (stonks :price)
                        (sql-stocks "price"))))
      (finally
        (try
          (duck/drop-table! @conn* stonks)
          (catch Throwable _e nil))))))

(deftest prepared-statements-test
  (try
    (let [stocks @stocks-src*
          _ (do (duck/create-table! @conn* stocks)
                (duck/insert-dataset! @conn* stocks))]
      (resource/stack-resource-context
       (let [prep-stmt (duck/prepare @conn* "select * from stocks" {:result-type :single})]
         (is (== 560 (ds/row-count (prep-stmt))) "single")))
      (resource/stack-resource-context
       (let [prep-stmt (duck/prepare @conn* "select * from stocks" {:result-type :streaming})]
         (is (== 560 (ds/row-count (first (prep-stmt)))) "streaming")))
      (resource/stack-resource-context
       (let [prep-stmt (duck/prepare @conn* "select * from stocks where symbol = $1")]
         (is (== (ds/row-count (ds/filter-column @stocks-src* :symbol "AAPL"))
                 (ds/row-count (first (prep-stmt "AAPL")))) "single arg"))))
    (finally
      (try
        (duck/drop-table! @conn* "stocks")
        (catch Throwable _e nil)))))

(deftest missing-instant-test
  (try
    (let [ds (-> (ds/->dataset {:a [1 2 nil 4 nil 6]
                                :b [(dtype-dt/instant) nil nil (dtype-dt/instant) nil (dtype-dt/instant)]})
                 (vary-meta assoc :name "testdb"))
          _ (do (duck/create-table! @conn* ds)
                (duck/insert-dataset! @conn* ds))
          sql-ds (duck/sql->dataset @conn* "select * from testdb" {:key-fn keyword})]
      (is (= (ds/missing ds)
             (ds/missing sql-ds)))
      (is (= (vec (ds :a))
             (vec (sql-ds :a))))
      (is (= (vec (ds :b))
             (vec (sql-ds :b)))))
    (finally
      (try
        (duck/drop-table! @conn* "testdb")
        (catch Throwable _e nil)))))

(deftest insert-test
  (let [cn 4
        rn 1024
        ds-fn #(-> (into {} (for [i (range cn)] [(str "c" i)
                                                 (for [_ (range rn)] (str (random-uuid)))]))
                   (ds/->dataset {:dataset-name "t"})
                   (ds/select-columns (for [i (range cn)] (str "c" i))))]
    (try
      (duck/drop-table! @conn* "t")
      (catch Throwable _e nil))
    (duck/create-table! @conn* (ds-fn))
    (duck/insert-dataset! @conn* (ds-fn))
    (duck/insert-dataset! @conn* (ds-fn))
    (is (= (* 2 rn) (-> (duck/sql->dataset @conn* "from t")
                        (ds/row-count))))))

(deftest insert-chunk-size-test
  (let [cn 4
        rn (ffi/duckdb_vector_size)
        ds-fn #(-> (into {} (for [i (range cn)] [(str "c" i)
                                                 (for [_ (range rn)] (random-uuid))]))
                   (ds/->dataset {:dataset-name "t"})
                   (ds/select-columns (for [i (range cn)] (str "c" i))))]
    (try
      (duck/drop-table! @conn* "t")
      (catch Throwable _e nil))
    (duck/create-table! @conn* (ds-fn))
    (duck/insert-dataset! @conn* (ds-fn))
    (duck/insert-dataset! @conn* (ds-fn))
    (is (= (* 2 rn) (-> (duck/sql->dataset @conn* "from t")
                        (ds/row-count))))))

;; ---------------------------------------------------------------------------
;; Extended type tests — new DuckDB types
;; ---------------------------------------------------------------------------

(defn- with-fresh-conn
  "Run `f` with a fresh db + connection, cleaning up after."
  [f]
  (let [db (duck/open-db)
        cn (duck/connect db)]
    (try (f cn)
         (finally
           (duck/disconnect cn)
           (duck/close-db db)))))

(deftest timestamp-tz-test
  (with-fresh-conn
    (fn [cn]
      (testing "read"
        (duck/run-query! cn "CREATE TABLE ts_tz_r (val TIMESTAMPTZ)")
        (duck/run-query! cn "INSERT INTO ts_tz_r VALUES ('2024-01-15 10:30:00+05:00'), ('2024-06-15 12:00:00Z'), (NULL)")
        (let [r (duck/sql->dataset cn "SELECT * FROM ts_tz_r" {:key-fn keyword})]
          (is (= 3 (ds/row-count r)))
          (is (= (Instant/parse "2024-01-15T05:30:00Z") (first (r :val))))
          (is (= (Instant/parse "2024-06-15T12:00:00Z") (second (r :val))))
          (is (= #{2} (set (ds/missing r))))))
      (testing "write"
        (duck/run-query! cn "CREATE TABLE ts_tz_w (val TIMESTAMPTZ)")
        (let [instants [(Instant/parse "2024-01-15T10:30:00Z") (Instant/parse "2024-06-15T12:00:00Z") nil]
              test-ds (-> (ds/->dataset {:val instants}) (vary-meta assoc :name "ts_tz_w"))]
          (duck/insert-dataset! cn test-ds)
          (let [r (duck/sql->dataset cn "SELECT * FROM ts_tz_w" {:key-fn keyword})]
            (is (= (Instant/parse "2024-01-15T10:30:00Z") (first (r :val))))
            (is (= (Instant/parse "2024-06-15T12:00:00Z") (second (r :val))))
            (is (= #{2} (set (ds/missing r))))))))))

(deftest timestamp-precision-test
  (with-fresh-conn
    (fn [cn]
      (testing "TIMESTAMP_S read"
        (duck/run-query! cn "CREATE TABLE ts_s_r (val TIMESTAMP_S)")
        (duck/run-query! cn "INSERT INTO ts_s_r VALUES ('2024-01-15 10:30:00'), ('2024-06-15 12:00:00'), (NULL)")
        (let [r (duck/sql->dataset cn "SELECT * FROM ts_s_r" {:key-fn keyword})]
          (is (= (Instant/parse "2024-01-15T10:30:00Z") (first (r :val))))
          (is (= #{2} (set (ds/missing r))))))
      (testing "TIMESTAMP_S write"
        (duck/run-query! cn "CREATE TABLE ts_s_w (val TIMESTAMP_S)")
        (let [test-ds (-> (ds/->dataset {:val [(Instant/parse "2024-01-15T10:30:00Z") nil]})
                          (vary-meta assoc :name "ts_s_w"))]
          (duck/insert-dataset! cn test-ds)
          (let [r (duck/sql->dataset cn "SELECT * FROM ts_s_w" {:key-fn keyword})]
            ;; TIMESTAMP_S truncates sub-second precision
            (is (= (Instant/parse "2024-01-15T10:30:00Z") (first (r :val))))
            (is (= #{1} (set (ds/missing r)))))))

      (testing "TIMESTAMP_MS read"
        (duck/run-query! cn "CREATE TABLE ts_ms_r (val TIMESTAMP_MS)")
        (duck/run-query! cn "INSERT INTO ts_ms_r VALUES ('2024-01-15 10:30:00.123'), (NULL)")
        (let [r (duck/sql->dataset cn "SELECT * FROM ts_ms_r" {:key-fn keyword})]
          (is (= (Instant/parse "2024-01-15T10:30:00.123Z") (first (r :val))))))
      (testing "TIMESTAMP_MS write"
        (duck/run-query! cn "CREATE TABLE ts_ms_w (val TIMESTAMP_MS)")
        (let [test-ds (-> (ds/->dataset {:val [(Instant/parse "2024-01-15T10:30:00.123Z") nil]})
                          (vary-meta assoc :name "ts_ms_w"))]
          (duck/insert-dataset! cn test-ds)
          (let [r (duck/sql->dataset cn "SELECT * FROM ts_ms_w" {:key-fn keyword})]
            (is (= (Instant/parse "2024-01-15T10:30:00.123Z") (first (r :val)))))))

      (testing "TIMESTAMP_NS read"
        (duck/run-query! cn "CREATE TABLE ts_ns_r (val TIMESTAMP_NS)")
        (duck/run-query! cn "INSERT INTO ts_ns_r VALUES ('2024-01-15 10:30:00.123456789'), (NULL)")
        (let [r (duck/sql->dataset cn "SELECT * FROM ts_ns_r" {:key-fn keyword})]
          (is (= (Instant/parse "2024-01-15T10:30:00.123456789Z") (first (r :val))))))
      (testing "TIMESTAMP_NS write"
        (duck/run-query! cn "CREATE TABLE ts_ns_w (val TIMESTAMP_NS)")
        (let [test-ds (-> (ds/->dataset {:val [(Instant/parse "2024-01-15T10:30:00.123456Z") nil]})
                          (vary-meta assoc :name "ts_ns_w"))]
          (duck/insert-dataset! cn test-ds)
          (let [r (duck/sql->dataset cn "SELECT * FROM ts_ns_w" {:key-fn keyword})]
            ;; Microsecond precision preserved (ns write multiplies by 1000)
            (is (= (Instant/parse "2024-01-15T10:30:00.123456Z") (first (r :val))))))))))

(deftest hugeint-test
  (with-fresh-conn
    (fn [cn]
      (testing "read - positive"
        (let [r (duck/sql->dataset cn "SELECT 170141183460469231731687303715884105727::HUGEINT AS h" {:key-fn keyword})]
          (is (= (BigInteger. "170141183460469231731687303715884105727") (first (r :h))))))
      (testing "read - negative"
        (let [r (duck/sql->dataset cn "SELECT (-42)::HUGEINT AS h" {:key-fn keyword})]
          (is (= (BigInteger/valueOf -42) (first (r :h))))))
      (testing "read - auto SUM overflow"
        (duck/run-query! cn "CREATE TABLE big_nums (v BIGINT)")
        (duck/run-query! cn "INSERT INTO big_nums VALUES (9223372036854775807), (9223372036854775807)")
        (let [r (duck/sql->dataset cn "SELECT SUM(v) AS s FROM big_nums" {:key-fn keyword})]
          (is (= (BigInteger. "18446744073709551614") (first (r :s))))))
      (testing "write"
        (duck/run-query! cn "CREATE TABLE huge_w (v HUGEINT)")
        (let [test-ds (-> (ds/->dataset {:v [(BigInteger. "123456789012345678901234") nil (BigInteger/valueOf 42)]})
                          (vary-meta assoc :name "huge_w"))]
          (duck/insert-dataset! cn test-ds)
          (let [r (duck/sql->dataset cn "SELECT * FROM huge_w" {:key-fn keyword})]
            (is (= (BigInteger. "123456789012345678901234") (first (r :v))))
            (is (= #{1} (set (ds/missing r))))
            (is (= (BigInteger/valueOf 42) (nth (vec (r :v)) 2)))))))))

(deftest blob-test
  (with-fresh-conn
    (fn [cn]
      (testing "read"
        (duck/run-query! cn "CREATE TABLE blobs_r (data BLOB)")
        (duck/run-query! cn "INSERT INTO blobs_r VALUES (encode('hello')), (encode('world')), (NULL)")
        (let [r (duck/sql->dataset cn "SELECT * FROM blobs_r" {:key-fn keyword})]
          (is (= 3 (ds/row-count r)))
          (is (= "hello" (String. ^bytes (first (r :data)) "UTF-8")))
          (is (= "world" (String. ^bytes (second (r :data)) "UTF-8")))
          (is (= #{2} (set (ds/missing r)))))
        ;; Large blob (>12 bytes, pointer-style string_t)
        (duck/run-query! cn "INSERT INTO blobs_r VALUES (encode('this is a longer blob value exceeding 12 bytes'))")
        (let [r (duck/sql->dataset cn "SELECT * FROM blobs_r WHERE octet_length(data) > 12" {:key-fn keyword})]
          (is (= "this is a longer blob value exceeding 12 bytes"
                 (String. ^bytes (first (r :data)) "UTF-8")))))
      (testing "write"
        (duck/run-query! cn "CREATE TABLE blobs_w (data BLOB)")
        (let [short-blob (.getBytes "hello" "UTF-8")
              long-blob (.getBytes "this is a longer blob exceeding twelve bytes" "UTF-8")
              test-ds (-> (ds/->dataset {:data [short-blob nil long-blob]})
                          (vary-meta assoc :name "blobs_w"))]
          (duck/insert-dataset! cn test-ds)
          (let [r (duck/sql->dataset cn "SELECT * FROM blobs_w" {:key-fn keyword})]
            (is (= "hello" (String. ^bytes (first (r :data)) "UTF-8")))
            (is (= #{1} (set (ds/missing r))))
            (is (= "this is a longer blob exceeding twelve bytes"
                   (String. ^bytes (nth (vec (r :data)) 2) "UTF-8")))))))))

(deftest decimal-test
  (with-fresh-conn
    (fn [cn]
      (testing "read - small decimal (INTEGER-backed)"
        (let [r (duck/sql->dataset cn "SELECT 123.45::DECIMAL(10,2) AS d" {:key-fn keyword})]
          (is (= (BigDecimal. "123.45") (first (r :d))))))
      (testing "read - huge decimal (HUGEINT-backed)"
        (let [r (duck/sql->dataset cn "SELECT 123456789012345678901234567.89::DECIMAL(30,2) AS d" {:key-fn keyword})]
          (is (= (BigDecimal. "123456789012345678901234567.89") (first (r :d))))))
      (testing "write"
        (duck/run-query! cn "CREATE TABLE decs_w (val DECIMAL(10,3))")
        (let [test-ds (-> (ds/->dataset {:val [(BigDecimal. "1.234") nil (BigDecimal. "999.999")]})
                          (vary-meta assoc :name "decs_w"))]
          (duck/insert-dataset! cn test-ds)
          (let [r (duck/sql->dataset cn "SELECT * FROM decs_w" {:key-fn keyword})]
            (is (= (BigDecimal. "1.234") (first (r :val))))
            (is (= #{1} (set (ds/missing r))))
            (is (= (BigDecimal. "999.999") (nth (vec (r :val)) 2)))))))))

(deftest interval-test
  (with-fresh-conn
    (fn [cn]
      (testing "read"
        (let [r (duck/sql->dataset cn "SELECT INTERVAL '14' MONTH + INTERVAL '3' DAY + INTERVAL '45' SECOND AS iv" {:key-fn keyword})]
          (is (= {:months 14 :days 3 :micros 45000000} (first (r :iv))))))
      (testing "write"
        (duck/run-query! cn "CREATE TABLE iv_w (val INTERVAL)")
        (let [test-ds (-> (ds/->dataset {:val [{:months 14 :days 3 :micros 45000000}
                                               nil
                                               {:months 0 :days 0 :micros 0}]})
                          (vary-meta assoc :name "iv_w"))]
          (duck/insert-dataset! cn test-ds)
          (let [r (duck/sql->dataset cn "SELECT * FROM iv_w" {:key-fn keyword})]
            (is (= {:months 14 :days 3 :micros 45000000} (first (r :val))))
            (is (= #{1} (set (ds/missing r))))
            (is (= {:months 0 :days 0 :micros 0} (nth (vec (r :val)) 2)))))))))

(deftest enum-read-test
  (with-fresh-conn
    (fn [cn]
      (duck/run-query! cn "CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
      (duck/run-query! cn "CREATE TABLE colors (name VARCHAR, fav color)")
      (duck/run-query! cn "INSERT INTO colors VALUES ('Alice', 'red'), ('Bob', 'blue'), ('Carol', NULL)")
      (let [r (duck/sql->dataset cn "SELECT * FROM colors" {:key-fn keyword})]
        (is (= ["Alice" "Bob" "Carol"] (vec (r :name))))
        (is (= ["red" "blue"] (vec (remove nil? (r :fav)))))
        (is (= #{2} (set (ds/missing (r :fav)))))))))

(deftest enum-write-test
  (with-fresh-conn
    (fn [cn]
      (duck/run-query! cn "CREATE TYPE size AS ENUM ('small', 'medium', 'large')")
      (duck/run-query! cn "CREATE TABLE items (name VARCHAR, sz size)")
      (let [test-ds (-> (ds/->dataset {:name ["shirt" "pants" "hat" "jacket" nil]
                                       :sz ["small" "large" "medium" "small" "large"]})
                        (vary-meta assoc :name "items"))]
        (duck/insert-dataset! cn test-ds)
        (let [r (duck/sql->dataset cn "SELECT * FROM items" {:key-fn keyword})]
          (is (= 5 (ds/row-count r)))
          (is (= ["shirt" "pants" "hat" "jacket"] (vec (remove nil? (r :name)))))
          (is (= ["small" "large" "medium" "small" "large"] (vec (r :sz)))))))))

(deftest list-test
  (with-fresh-conn
    (fn [cn]
      (testing "read - integers"
        (duck/run-query! cn "CREATE TABLE int_lists_r (vals INTEGER[])")
        (duck/run-query! cn "INSERT INTO int_lists_r VALUES ([1, 2, 3]), ([4, 5]), (NULL), ([]), ([NULL, 7])")
        (let [r (duck/sql->dataset cn "SELECT * FROM int_lists_r" {:key-fn keyword})
              vals (vec (r :vals))]
          (is (= [1 2 3] (first vals)))
          (is (= [4 5] (second vals)))
          (is (nil? (nth vals 2)))
          (is (= [] (nth vals 3)))
          (is (= [nil 7] (nth vals 4)))))

      (testing "read - strings"
        (duck/run-query! cn "CREATE TABLE str_lists_r (vals VARCHAR[])")
        (duck/run-query! cn "INSERT INTO str_lists_r VALUES (['hello', 'world']), (['a', 'b', 'c'])")
        (let [r (duck/sql->dataset cn "SELECT * FROM str_lists_r" {:key-fn keyword})]
          (is (= ["hello" "world"] (first (r :vals))))
          (is (= ["a" "b" "c"] (second (r :vals))))))

      (testing "write - integers"
        (duck/run-query! cn "CREATE TABLE int_lists_w (vals INTEGER[])")
        (let [test-ds (-> (ds/->dataset {:vals [[1 2 3] [4 5] nil [] [nil 7]]})
                          (vary-meta assoc :name "int_lists_w"))]
          (duck/insert-dataset! cn test-ds)
          (let [r (duck/sql->dataset cn "SELECT * FROM int_lists_w" {:key-fn keyword})
                vals (vec (r :vals))]
            (is (= [1 2 3] (first vals)))
            (is (= [4 5] (second vals)))
            (is (nil? (nth vals 2)))
            (is (= [] (nth vals 3)))
            (is (= [nil 7] (nth vals 4)))))))))

(deftest struct-read-test
  (with-fresh-conn
    (fn [cn]
      (duck/run-query! cn "CREATE TABLE structs (id INTEGER, person STRUCT(name VARCHAR, age INTEGER))")
      (duck/run-query! cn "INSERT INTO structs VALUES (1, {'name': 'Alice', 'age': 30}), (2, {'name': 'Bob', 'age': 25}), (3, NULL)")
      (let [r (duck/sql->dataset cn "SELECT * FROM structs" {:key-fn keyword})]
        (is (= [1 2 3] (vec (r :id))))
        (is (= {:name "Alice" :age 30} (first (r :person))))
        (is (= {:name "Bob" :age 25} (second (r :person))))
        (is (= #{2} (set (ds/missing (r :person)))))))))

(deftest struct-write-test
  (with-fresh-conn
    (fn [cn]
      (duck/run-query! cn "CREATE TABLE struct_w (id INTEGER, point STRUCT(x DOUBLE, y DOUBLE))")
      (let [test-ds (-> (ds/->dataset {:id [1 2 3]
                                       :point [{:x 1.0 :y 2.0}
                                               {:x 3.0 :y 4.0}
                                               {:x 5.0 :y 6.0}]})
                        (ds/set-dataset-name "struct_w"))]
        (duck/insert-dataset! cn test-ds)
        (let [r (duck/sql->dataset cn "SELECT * FROM struct_w" {:key-fn keyword})]
          (is (= [1 2 3] (vec (r :id))))
          (is (= {:x 1.0 :y 2.0} (first (r :point))))
          (is (= {:x 5.0 :y 6.0} (nth (vec (r :point)) 2))))))))

(deftest map-test
  (with-fresh-conn
    (fn [cn]
      (testing "read"
        (let [r (duck/sql->dataset cn "SELECT MAP {'a': 1, 'b': 2, 'c': 3} AS m" {:key-fn keyword})]
          (is (= {"a" 1 "b" 2 "c" 3} (first (r :m)))))
        (let [r (duck/sql->dataset cn "SELECT MAP {'x': NULL, 'y': 42} AS m" {:key-fn keyword})]
          (is (= {"x" nil "y" 42} (first (r :m))))))
      (testing "write"
        (duck/run-query! cn "CREATE TABLE map_w (data MAP(VARCHAR, INTEGER))")
        (let [test-ds (-> (ds/->dataset {:data [{"a" 1 "b" 2} {"x" 10} nil]})
                          (vary-meta assoc :name "map_w"))]
          (duck/insert-dataset! cn test-ds)
          (let [r (duck/sql->dataset cn "SELECT * FROM map_w" {:key-fn keyword})]
            (is (= {"a" 1 "b" 2} (first (r :data))))
            (is (= {"x" 10} (second (r :data))))
            (is (= #{2} (set (ds/missing r))))))))))

;; ---------------------------------------------------------------------------
;; Multi-chunk fast-path tests
;;
;; sql->dataset's :single result-type uses a partitioned parallel-memcpy
;; concat fast-path when there are >=2 numeric/temporal columns and >=8
;; chunks (~16K rows at DuckDB's default vector_size=2048).  These tests
;; exercise the fast path on workloads large enough to trigger it, focusing
;; on the corner cases that are easy to get wrong: missing-bitmap offset
;; merging, packed-temporal type tagging, unsigned-int tagging, and
;; numeric/non-numeric partition stitching.
;; ---------------------------------------------------------------------------

(def ^:private ^:const fast-path-rows
  "Comfortably above 8 chunks (8 * 2048 = 16384 rows minimum)."
  20000)

(deftest fast-path-numeric-with-nulls-test
  (with-fresh-conn
    (fn [cn]
      (let [n fast-path-rows
            ;; Sprinkle nulls at non-uniform chunk offsets to exercise the
            ;; per-chunk missing-bitmap offset merge across chunk boundaries.
            null-rows #{0 1 100 2047 2048 2049 4097 (- n 1)}
            longs (mapv (fn [i] (when-not (null-rows i) (long i)))   (range n))
            doubles (mapv (fn [i] (when-not (null-rows i) (* i 1.5))) (range n))
            ints (mapv (fn [i] (when-not (null-rows i) (int i)))     (range n))
            floats (mapv (fn [i] (when-not (null-rows i) (float i))) (range n))
            ds-in (-> (ds/->dataset {:longs longs :doubles doubles :ints ints :floats floats})
                      (vary-meta assoc :name "fp_numeric_null"))]
        (duck/create-table! cn ds-in)
        (duck/insert-dataset! cn ds-in)
        (let [out (duck/sql->dataset cn "SELECT * FROM fp_numeric_null ORDER BY longs NULLS LAST")
              card (fn [^org.roaringbitmap.RoaringBitmap bm] (.getCardinality bm))]
          (is (= n (ds/row-count out)))
          ;; Missing rows should match the null count (positions differ post-ORDER BY,
          ;; but cardinality must be preserved across the chunk-boundary union)
          (is (= (count null-rows) (card (ds/missing (get out "longs"))))
              "longs column missing-bitmap survived the chunk-boundary merge")
          (is (= (count null-rows) (card (ds/missing (get out "doubles")))))
          (is (= (count null-rows) (card (ds/missing (get out "ints")))))
          (is (= (count null-rows) (card (ds/missing (get out "floats")))))
          ;; Non-null values are intact — first non-null is at row index 2
          ;; (rows 0 and 1 are in null-rows)
          (is (= 2 (first (filter some? (get out "longs"))))))))))

(deftest fast-path-mixed-partition-stitch-test
  (with-fresh-conn
    (fn [cn]
      (let [n fast-path-rows
            ;; Column order: numeric, string, numeric, string, numeric.
            ;; The stitch step must re-interleave numeric (fast-path) and
            ;; string (fallback) columns back into this original positional order.
            ds-in (-> (ds/->dataset {:a_long (long-array (range n))
                                     :b_str  (mapv #(str "x" %) (range n))
                                     :c_dbl  (double-array (map #(* 0.5 %) (range n)))
                                     :d_str  (mapv #(str "y" %) (range n))
                                     :e_int  (int-array (range n))})
                      (vary-meta assoc :name "fp_mixed_order"))]
        (duck/create-table! cn ds-in)
        (duck/insert-dataset! cn ds-in)
        (let [out (duck/sql->dataset cn "SELECT * FROM fp_mixed_order ORDER BY a_long")]
          (is (= n (ds/row-count out)))
          (is (= ["a_long" "b_str" "c_dbl" "d_str" "e_int"]
                 (mapv (comp :name meta) (ds/columns out)))
              "column order preserved after partition+stitch")
          (is (= [0 1 2] (vec (take 3 (get out "a_long")))))
          (is (= ["x0" "x1" "x2"] (vec (take 3 (get out "b_str")))))
          (is (= [0.0 0.5 1.0] (vec (take 3 (get out "c_dbl")))))
          (is (= ["y0" "y1" "y2"] (vec (take 3 (get out "d_str")))))
          (is (= [0 1 2] (vec (take 3 (get out "e_int"))))))))))

(deftest fast-path-wide-numeric-test
  (with-fresh-conn
    (fn [cn]
      (let [n fast-path-rows
            base-date (java.time.LocalDate/of 2020 1 1)
            ;; 8-column wide table: exercises the parallel pmap across many
            ;; columns simultaneously plus a packed temporal type.
            ds-in (-> (ds/->dataset {:l1 (long-array (range n))
                                     :l2 (long-array (map #(* 2 %) (range n)))
                                     :d1 (double-array (map #(* 0.5 %) (range n)))
                                     :d2 (double-array (map #(* 1.5 %) (range n)))
                                     :i1 (int-array (range n))
                                     :i2 (int-array (map #(* 3 %) (range n)))
                                     :f1 (float-array (map float (range n)))
                                     :dt (mapv #(.plusDays base-date (long %)) (range n))})
                      (vary-meta assoc :name "fp_wide_num"))]
        (duck/create-table! cn ds-in)
        (duck/insert-dataset! cn ds-in)
        (let [out (duck/sql->dataset cn "SELECT * FROM fp_wide_num ORDER BY l1")]
          (is (= n (ds/row-count out)))
          (is (= 8 (ds/column-count out)))
          (is (= 0 (first (get out "l1"))))
          (is (= (long (* 2 (dec n))) (last (get out "l2"))))
          (is (= 0.0 (first (get out "d1"))))
          ;; Packed-local-date roundtrip — auto-unpacks to LocalDate on read
          (is (= base-date (first (get out "dt"))))
          (is (= (.plusDays base-date (dec n)) (last (get out "dt")))))))))

(deftest fast-path-unsigned-types-test
  (with-fresh-conn
    (fn [cn]
      (let [n fast-path-rows]
        (duck/run-query! cn "CREATE TABLE fp_unsigned (
                               u8  UTINYINT,
                               u16 USMALLINT,
                               u32 UINTEGER,
                               u64 UBIGINT,
                               i64 BIGINT)")
        (duck/run-query! cn
                         (format "INSERT INTO fp_unsigned SELECT
                                    (i %% 200)::UTINYINT,
                                    (i %% 60000)::USMALLINT,
                                    (i * 2)::UINTEGER,
                                    (i * 3)::UBIGINT,
                                    i::BIGINT
                                  FROM range(0, %d) t(i)" n))
        (let [out (duck/sql->dataset cn "SELECT * FROM fp_unsigned ORDER BY i64")]
          (is (= n (ds/row-count out)))
          (is (= :uint8  (dt/elemwise-datatype (get out "u8"))))
          (is (= :uint16 (dt/elemwise-datatype (get out "u16"))))
          (is (= :uint32 (dt/elemwise-datatype (get out "u32"))))
          (is (= :uint64 (dt/elemwise-datatype (get out "u64"))))
          ;; Spot-check round-trip values
          (is (= 0 (long (first (get out "u8")))))
          (is (= 199 (long (nth (get out "u8") 199))))
          (is (= (long (* 2 (dec n))) (long (last (get out "u32")))))
          (is (= (long (* 3 (dec n))) (long (last (get out "u64"))))))))))

(deftest fast-path-packed-temporal-test
  (with-fresh-conn
    (fn [cn]
      (let [n fast-path-rows
            base-date (java.time.LocalDate/of 2020 1 1)
            base-time (java.time.LocalTime/of 10 30 0)
            base-inst (java.time.Instant/parse "2020-01-01T10:00:00Z")
            ds-in (-> (ds/->dataset
                       {:id (long-array (range n))
                        :d  (mapv #(.plusDays base-date (long %)) (range n))
                        :t  (mapv #(.plusNanos base-time (long (* % 1000))) (range n))
                        :i  (mapv #(.plusSeconds base-inst (long %)) (range n))})
                      (vary-meta assoc :name "fp_temporal"))]
        (duck/create-table! cn ds-in)
        (duck/insert-dataset! cn ds-in)
        (let [out (duck/sql->dataset cn "SELECT * FROM fp_temporal ORDER BY id")]
          (is (= n (ds/row-count out)))
          ;; All three packed types must auto-unpack on read
          (is (= base-date (first (get out "d"))))
          (is (= base-time (first (get out "t"))))
          (is (= base-inst (first (get out "i"))))
          (is (= (.plusDays base-date (dec n)) (last (get out "d"))))
          (is (= (.plusSeconds base-inst (dec n)) (last (get out "i")))))))))

(deftest fast-path-fallback-thresholds-test
  ;; The fast path is gated on (>= n-numeric 2) AND (>= n-chunks 8).  Below
  ;; either threshold the function falls back to apply ds/concat.  Both
  ;; paths must produce equivalent datasets.
  (with-fresh-conn
    (fn [cn]
      (testing "single numeric column — falls back, still correct"
        (let [n fast-path-rows
              ds-in (-> (ds/->dataset {:val (long-array (range n))})
                        (vary-meta assoc :name "fp_single_col"))]
          (duck/create-table! cn ds-in)
          (duck/insert-dataset! cn ds-in)
          (let [out (duck/sql->dataset cn "SELECT * FROM fp_single_col ORDER BY val")]
            (is (= n (ds/row-count out)))
            (is (= (vec (range n)) (vec (get out "val")))))))
      (testing "small query (< 8 chunks) — falls back, still correct"
        (let [n 100  ;; way below fast-path threshold
              ds-in (-> (ds/->dataset {:a (long-array (range n))
                                       :b (double-array (range n))})
                        (vary-meta assoc :name "fp_small"))]
          (duck/create-table! cn ds-in)
          (duck/insert-dataset! cn ds-in)
          (let [out (duck/sql->dataset cn "SELECT * FROM fp_small ORDER BY a")]
            (is (= n (ds/row-count out)))
            (is (= (vec (range n)) (vec (get out "a"))))))))))

;; ---------------------------------------------------------------------------
;; Streaming appender API tests
;; ---------------------------------------------------------------------------

(defn- make-stream-ds
  "Build a small batch with a stable schema for streaming tests.
  `n` must be >= 1 — element-dtypes can't be inferred from empty seqs.
  Pass the same table-name across batches to feed one appender."
  [table-name start n]
  (assert (>= n 1) "make-stream-ds requires at least 1 row for stable dtypes")
  (-> (ds/->dataset {:id (long-array (range start (+ start n)))
                     :name (mapv #(str "row-" %) (range start (+ start n)))
                     :score (double-array (map #(* 0.5 %) (range start (+ start n))))})
      (vary-meta assoc :name table-name)))

(defn- stream-schema-sample
  "A 1-row schema sample for `make-stream-ds`-shaped tables."
  [table-name]
  (make-stream-ds table-name 0 1))

(deftest appender-basic-lifecycle-test
  (testing "open + multiple appends + close round-trips correctly"
    (with-fresh-conn
      (fn [cn]
        (let [sample (stream-schema-sample "stream_basic")]
          (duck/create-table! cn sample)
          ;; Drop the schema sample's row so we get a clean count
          (duck/run-query! cn "DELETE FROM stream_basic")
          (with-open [app (duck/open-appender cn sample)]
            (let [r1 (duck/append-dataset! app (make-stream-ds "stream_basic" 0 10))
                  r2 (duck/append-dataset! app (make-stream-ds "stream_basic" 10 5))
                  r3 (duck/append-dataset! app (make-stream-ds "stream_basic" 15 25))]
              (is (= 10 r1))
              (is (= 5 r2))
              (is (= 25 r3))))
          ;; close should have flushed
          (let [r (duck/sql->dataset cn "SELECT * FROM stream_basic ORDER BY id" {:key-fn keyword})]
            (is (= 40 (ds/row-count r)))
            (is (= (vec (range 40)) (vec (r :id))))
            (is (= (mapv #(str "row-" %) (range 40)) (vec (r :name))))
            (is (= (mapv #(* 0.5 %) (range 40)) (vec (r :score))))))))))

(deftest appender-many-small-batches-test
  ;; The whole point of the streaming API: many tiny batches reuse the
  ;; appender's cached schema state and produce a correct table.
  (testing "100 × 7-row batches via one appender"
    (with-fresh-conn
      (fn [cn]
        (let [sample (stream-schema-sample "stream_many")]
          (duck/create-table! cn sample)
          (duck/run-query! cn "DELETE FROM stream_many")
          (with-open [app (duck/open-appender cn sample)]
            (dotimes [i 100]
              (duck/append-dataset! app (make-stream-ds "stream_many" (* i 7) 7))))
          (let [r (duck/sql->dataset cn "SELECT * FROM stream_many ORDER BY id" {:key-fn keyword})]
            (is (= 700 (ds/row-count r)))
            (is (= (vec (range 700)) (vec (r :id))))))))))

(deftest appender-flush-makes-rows-visible-test
  ;; Rows held in the appender's internal buffer are NOT visible. Calling
  ;; flush-appender! makes them visible mid-stream.
  (testing "buffered rows invisible until flush"
    (with-fresh-conn
      (fn [cn]
        (let [sample (stream-schema-sample "stream_flush")
              count-rows #(first ((duck/sql->dataset cn "SELECT count(*) AS n FROM stream_flush"
                                                    {:key-fn keyword}) :n))]
          (duck/create-table! cn sample)
          (duck/run-query! cn "DELETE FROM stream_flush")
          (with-open [app (duck/open-appender cn sample)]
            (is (= 0 (count-rows)) "no rows before any append")
            (duck/append-dataset! app (make-stream-ds "stream_flush" 0 50))
            (is (= 0 (count-rows)) "appended rows still buffered, not visible")
            (is (= :ok (duck/flush-appender! app)))
            (is (= 50 (count-rows)) "flush makes buffered rows visible")
            ;; flushing again with nothing buffered is a no-op
            (is (= :ok (duck/flush-appender! app)))
            (is (= 50 (count-rows)))))))))

(deftest appender-multiple-open-simultaneously-test
  ;; The whole "single worker, N tables" use case.
  (testing "two appenders open in parallel on the same connection write to different tables"
    (with-fresh-conn
      (fn [cn]
        (let [sample-a (stream-schema-sample "stream_multi_a")
              sample-b (-> (ds/->dataset {:label ["x"] :weight (float-array [1.0])})
                           (vary-meta assoc :name "stream_multi_b"))]
          (duck/create-table! cn sample-a)
          (duck/create-table! cn sample-b)
          (duck/run-query! cn "DELETE FROM stream_multi_a")
          (with-open [app-a (duck/open-appender cn sample-a)
                      app-b (duck/open-appender cn sample-b)]
            (dotimes [i 5]
              (duck/append-dataset! app-a (make-stream-ds "stream_multi_a" (* i 3) 3))
              (duck/append-dataset! app-b (-> (ds/->dataset
                                               {:label (mapv #(str "l-" %) (range (* i 2) (+ (* i 2) 2)))
                                                :weight (float-array (map #(* 0.25 %)
                                                                          (range (* i 2) (+ (* i 2) 2))))})
                                              (vary-meta assoc :name "stream_multi_b")))))
          (let [ra (duck/sql->dataset cn "SELECT * FROM stream_multi_a ORDER BY id" {:key-fn keyword})
                rb (duck/sql->dataset cn "SELECT * FROM stream_multi_b ORDER BY label" {:key-fn keyword})]
            (is (= 15 (ds/row-count ra)))
            (is (= (vec (range 15)) (vec (ra :id))))
            ;; create-table! only creates schema (no insert), so rb has only the 10 we appended.
            (is (= 10 (ds/row-count rb)))
            (is (= (sort (map #(str "l-" %) (range 10)))
                   (vec (rb :label))))))))))

(deftest appender-schema-mismatch-test
  (with-fresh-conn
    (fn [cn]
      (let [sample (stream-schema-sample "stream_sm")]
        (duck/create-table! cn sample)
        (duck/run-query! cn "DELETE FROM stream_sm")
        (with-open [app (duck/open-appender cn sample)]
          (testing "extra column rejected"
            (let [bad (-> (ds/->dataset {:id (long-array [0])
                                         :name ["x"]
                                         :score (double-array [0.0])
                                         :extra (long-array [1])})
                          (vary-meta assoc :name "stream_sm"))]
              (is (thrown-with-msg? IllegalArgumentException
                                    #"Batch schema does not match"
                                    (duck/append-dataset! app bad)))))
          (testing "missing column rejected"
            (let [bad (-> (ds/->dataset {:id (long-array [0])
                                         :name ["x"]})
                          (vary-meta assoc :name "stream_sm"))]
              (is (thrown-with-msg? IllegalArgumentException
                                    #"Batch schema does not match"
                                    (duck/append-dataset! app bad)))))
          (testing "wrong dtype rejected"
            (let [bad (-> (ds/->dataset {:id (long-array [0])
                                         :name ["x"]
                                         :score (long-array [0])}) ;; int instead of double
                          (vary-meta assoc :name "stream_sm"))]
              (is (thrown-with-msg? IllegalArgumentException
                                    #"Batch schema does not match"
                                    (duck/append-dataset! app bad)))))
          (testing "matching batch still works after rejected ones"
            (is (= 3 (duck/append-dataset! app (make-stream-ds "stream_sm" 0 3))))))))))

(deftest appender-closed-throws-test
  (with-fresh-conn
    (fn [cn]
      (let [sample (stream-schema-sample "stream_closed")
            _ (duck/create-table! cn sample)
            _ (duck/run-query! cn "DELETE FROM stream_closed")
            app (duck/open-appender cn sample)]
        (duck/append-dataset! app (make-stream-ds "stream_closed" 0 2))
        (.close ^java.lang.AutoCloseable app)
        (is (thrown-with-msg? IllegalStateException
                              #"Appender is closed"
                              (duck/append-dataset! app (make-stream-ds "stream_closed" 2 2))))
        (is (thrown-with-msg? IllegalStateException
                              #"Appender is closed"
                              (duck/flush-appender! app)))
        ;; double-close is safe (CAS protects against double-destroy)
        (.close ^java.lang.AutoCloseable app)
        ;; rows from before close should still be there
        (let [r (duck/sql->dataset cn "SELECT count(*) AS n FROM stream_closed"
                                   {:key-fn keyword})]
          (is (= 2 (first (r :n)))))))))

(deftest appender-poisoned-by-failed-flush-test
  ;; Constraint violation at flush time invalidates buffered data and
  ;; poisons the appender. close-after-poison must be a safe no-op
  ;; (the failed flush already destroyed the native state).
  (with-fresh-conn
    (fn [cn]
      (duck/run-query! cn "CREATE TABLE stream_pk (id INTEGER PRIMARY KEY, v VARCHAR)")
      (let [batch-1 (-> (ds/->dataset {:id (int-array [1 2]) :v ["a" "b"]})
                        (vary-meta assoc :name "stream_pk"))
            dup (-> (ds/->dataset {:id (int-array [1]) :v ["x"]})
                    (vary-meta assoc :name "stream_pk"))
            app (duck/open-appender cn batch-1)]
        (try
          ;; First batch commits cleanly.
          (duck/append-dataset! app batch-1)
          (is (= :ok (duck/flush-appender! app)))
          ;; Duplicate key — flush fails, appender becomes poisoned.
          (duck/append-dataset! app dup)
          (is (thrown-with-msg? Exception
                                #"flush failed.*[Dd]uplicate"
                                (duck/flush-appender! app)))
          ;; Subsequent ops fail fast.
          (is (thrown-with-msg? IllegalStateException
                                #"Appender is closed"
                                (duck/append-dataset! app batch-1)))
          (is (thrown-with-msg? IllegalStateException
                                #"Appender is closed"
                                (duck/flush-appender! app)))
          ;; The first batch's data survived (it was committed by the first flush).
          (let [r (duck/sql->dataset cn "SELECT count(*) AS n FROM stream_pk"
                                     {:key-fn keyword})]
            (is (= 2 (first (r :n)))))
          (finally
            ;; close-after-poison must not throw or double-destroy
            (try (.close ^java.lang.AutoCloseable app)
                 (catch Throwable t
                   (is false (str "close-after-poison threw: " t))))))))))

(deftest appender-equivalence-with-insert-dataset-test
  ;; Same input dataset, two write paths, identical output table.
  (with-fresh-conn
    (fn [cn]
      (duck/run-query! cn "CREATE TABLE eq_insert (id BIGINT, name VARCHAR, score DOUBLE)")
      (duck/run-query! cn "CREATE TABLE eq_appender (id BIGINT, name VARCHAR, score DOUBLE)")
      (let [batches (vec (for [i (range 5)] (make-stream-ds "_" (* i 20) 20)))]
        ;; insert-dataset! path
        (doseq [b batches]
          (duck/insert-dataset! cn (vary-meta b assoc :name "eq_insert")))
        ;; open-appender path
        (with-open [app (duck/open-appender cn (vary-meta (first batches) assoc :name "eq_appender"))]
          (doseq [b batches]
            (duck/append-dataset! app (vary-meta b assoc :name "eq_appender"))))
        (let [r1 (duck/sql->dataset cn "SELECT * FROM eq_insert   ORDER BY id" {:key-fn keyword})
              r2 (duck/sql->dataset cn "SELECT * FROM eq_appender ORDER BY id" {:key-fn keyword})]
          (is (= (ds/row-count r1) (ds/row-count r2)))
          (is (= (vec (r1 :id))    (vec (r2 :id))))
          (is (= (vec (r1 :name))  (vec (r2 :name))))
          (is (= (vec (r1 :score)) (vec (r2 :score)))))))))

(deftest appender-handles-special-types-test
  ;; The shared helpers must cover the same type matrix as insert-dataset!.
  ;; Spot-check string + UUID + instant via the streaming API.
  (with-fresh-conn
    (fn [cn]
      (let [base (-> (ds/->dataset {:s   (mapv #(str "v-" %) (range 5))
                                    :uid (repeatedly 5 #(UUID/randomUUID))
                                    :t   (repeatedly 5 #(java.time.Instant/now))})
                     (vary-meta assoc :name "stream_special"))]
        (duck/create-table! cn base)
        (with-open [app (duck/open-appender cn base)]
          (duck/append-dataset! app base)
          (duck/append-dataset! app base)
          (duck/append-dataset! app base))
        (let [r (duck/sql->dataset cn "SELECT count(*) AS n FROM stream_special"
                                   {:key-fn keyword})]
          (is (= 15 (first (r :n)))))))))
