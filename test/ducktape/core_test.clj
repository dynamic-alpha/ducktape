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
           [java.time LocalTime Instant]
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
