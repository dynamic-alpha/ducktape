# Ducktape

Connect [tech.v3.dataset](https://github.com/techascent/tech.ml.dataset) to [DuckDB](https://duckdb.org).

A near drop-in replacement for [tmducken](https://github.com/techascent/tmducken) that uses Java's [Panama Foreign Function & Memory API](https://openjdk.org/jeps/454) instead of JNA.

## Differences from tmducken

**Project Panama instead of JNA.** Ducktape calls DuckDB's C API through `java.lang.foreign.*` (JDK 22+) rather than JNA/dtype-next FFI. This means fewer dependencies, deterministic native memory management via `Arena` scoping, and no marshalling overhead on FFI calls. The entire FFI layer is a single 250-line file of data-driven macro-generated bindings.

**More DuckDB types.** Read and write support for BLOB, HUGEINT, DECIMAL, INTERVAL, ENUM, LIST, STRUCT, MAP, and all timestamp precision variants — types tmducken does not handle.

**Performance tuned.** Signature-polymorphic FFI dispatch via `MethodHandles/explicitCastArguments` + `MethodHandleProxies` (no Panama `invokeWithArguments` allocation), parallel string/UUID encode and decode via `hamf/pgroups`, lock-free slab allocation for pointer-style strings, RoaringBitmap validity scanning, pre-packed temporal columns, and two-phase column cloning. Beats tmducken on all measured workloads — see [Benchmarks](#benchmarks).

## Requirements

- **JDK 22+** (Panama FFM is a final API as of JDK 22)
- **DuckDB 1.5+** (tested against 1.5.2)

> `--enable-native-access=ALL-UNNAMED` must be passed as a JVM option.

## Quick start

```clojure
(require '[ducktape.core :as duck]
         '[tech.v3.dataset :as ds])

(duck/initialize!)

(def db (duck/open-db))           ;; in-memory, or (open-db "/tmp/my.db")
(def conn (duck/connect db))

;; Create + insert
(def my-ds (ds/->dataset {:name  ["Alice" "Bob" "Carol"]
                          :age   [30 25 35]
                          :score [9.5 8.2 9.8]}
                         {:dataset-name "people"})

(duck/create-table! conn my-ds)
(duck/insert-dataset! conn my-ds)

;; Query back
(duck/sql->dataset conn "SELECT * FROM people WHERE score > 9.0" {:key-fn keyword})
;; => :_unnamed [2 3]:
;; |  :name | :age | :score |
;; |--------|-----:|-------:|
;; |  Alice |   30 |    9.5 |
;; |  Carol |   35 |    9.8 |

;; Cleanup
(duck/disconnect conn)
(duck/close-db db)
```

## API

| Function | Description |
|----------|-------------|
| `initialize!` | Load the DuckDB shared library. Call once at startup. |
| `open-db` / `close-db` | Open/close a database (path or in-memory) |
| `connect` / `disconnect` | Create/destroy a connection |
| `run-query!` | Execute SQL, ignore results (DDL, DML) |
| `create-table!` / `drop-table!` | Create/drop a table from a dataset schema |
| `insert-dataset!` | Bulk insert via DuckDB's data chunk appender API |
| `sql->dataset` | Query → single dataset |
| `sql->datasets` | Query → lazy sequence of chunk datasets |
| `prepare` | Prepared statement (0-arity, 1-arity, or N-arity) |

`initialize!` searches for the DuckDB shared library in this order:
1. `:duckdb-home` option (directory path)
2. `DUCKDB_HOME` environment variable
3. Default system library paths

## Supported DuckDB types

| DuckDB Type | Clojure | Read | Write |
|-------------|---------|:----:|:-----:|
| BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT | primitives | ✓ | ✓ |
| UTINYINT, USMALLINT, UINTEGER, UBIGINT | primitives | ✓ | ✓ |
| FLOAT, DOUBLE | primitives | ✓ | ✓ |
| VARCHAR | String | ✓ | ✓ |
| BLOB | byte[] | ✓ | ✓ |
| UUID | java.util.UUID | ✓ | ✓ |
| DATE | LocalDate | ✓ | ✓ |
| TIME | LocalTime | ✓ | ✓ |
| TIMESTAMP | Instant | ✓ | ✓ |
| TIMESTAMP WITH TIME ZONE | Instant | ✓ | ✓ |
| TIMESTAMP_S / _MS / _NS | Instant | ✓ | ✓ |
| HUGEINT | BigInteger | ✓ | ✓ |
| DECIMAL | BigDecimal | ✓ | ✓ |
| INTERVAL | `{:months :days :micros}` | ✓ | ✓ |
| ENUM | String | ✓ | ✓ |
| LIST | vector | ✓ | ✓ |
| STRUCT | map (keyword keys) | ✓ | ✓ |
| MAP | map | ✓ | ✓ |

## Why Panama over JNA

tmducken uses [JNA](https://github.com/java-native-access/jna) (via [dtype-next](https://github.com/cnuernber/dtype-next)'s FFI layer) to call DuckDB's C API. Panama eliminates several layers of overhead:

- **No marshalling.** JNA copies arguments through `libffi` for every call. Panama generates direct `MethodHandle` downcalls that the JIT compiles to ordinary machine code.
- **No reflection.** JNA resolves signatures at runtime. Panama resolves `FunctionDescriptor` layouts at link time and produces typed handles the JIT can inline.
- **No global lock.** JNA's library loading holds a global synchronization lock. Panama's `SymbolLookup` is lock-free after initial load.
- **Deterministic memory.** JNA relies on `Memory.finalize` for native allocations (GC-dependent cleanup). Panama's `Arena` scoping guarantees deterministic deallocation with `with-open`.
- **Typed memory access.** JNA's `Pointer.getLong(offset)` goes through a general-purpose accessor. Panama's `MemorySegment.get(ValueLayout.JAVA_LONG, offset)` carries the layout statically, enabling the JIT to emit a single `mov` instruction.

## Benchmarks

1M rows, JDK 25, DuckDB 1.5.2, Apple M-series. Same JVM, same datasets, 1.5s JIT warmup per fn, 20–30 samples per phase per library, interleaved per-sample alternation. Δ% is `(tmducken_mean − ducktape_mean) / tmducken_mean` — positive means ducktape is faster. `*` indicates statistical significance at 95% CI; `trim` is the 10%-trimmed mean (robust to GC outliers).

|           | INSERT (mean / trim) | QUERY (mean / trim) |
|-----------|----------------------|---------------------|
| Numeric   | **+10.7%** * / +10.9% | **+54.9%** * / +55.5% |
| String    | **+36.2%** * / +36.0% | **+26.1%** * / +23.2% |
| UUID      | **+33.4%** * / +33.5% | **+35.4%** * / +35.5% |
| Mixed     | **+30.4%** * / +30.2% | +21.2% / +23.3%       |

Ducktape wins all 8 metrics (7 statistically significant); the mixed-query CI overlaps due to higher tail variance but the trimmed mean still favours ducktape. The biggest win is numeric query (+55%): tmducken's `apply ds/concat` over 489 native-buffer chunks is the dominant cost in both libraries, but ducktape's chunk-realization phase is meaningfully leaner.

The bench harness lives in `dev/tmducken_comparison.clj`. Run `(require '[tmducken-comparison :as cmp])` then `(cmp/compare-all)`.

## Development

### Nix

The included `flake.nix` provides DuckDB and sets `DUCKDB_HOME` automatically:

```bash
nix develop
```

### deps.edn

```clojure
{:deps {techascent/tech.ml.dataset    {:mvn/version "8.021"}
        techascent/tech.ml.dataset.sql {:mvn/version "7.029"}
        com.cnuernber/ham-fisted       {:mvn/version "3.029"}
        org.roaringbitmap/RoaringBitmap {:mvn/version "1.6.14"}}
 :aliases
 {:dev {:jvm-opts ["--enable-native-access=ALL-UNNAMED"]}}}
```

## License

EPL-2.0
