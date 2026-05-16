# Ducktape

[![CI](https://github.com/dynamic-alpha/ducktape/actions/workflows/ci.yaml/badge.svg)](https://github.com/dynamic-alpha/ducktape/actions/workflows/ci.yaml)
[![Clojars Project](https://img.shields.io/clojars/v/ai.dyal/ducktape.svg)](https://clojars.org/ai.dyal/ducktape)
[![cljdoc badge](https://cljdoc.org/badge/ai.dyal/ducktape)](https://cljdoc.org/d/ai.dyal/ducktape/CURRENT)

Connect [tech.v3.dataset](https://github.com/techascent/tech.ml.dataset) to [DuckDB](https://duckdb.org).

A near drop-in replacement for [tmducken](https://github.com/techascent/tmducken) that uses Java's [Panama Foreign Function & Memory API](https://openjdk.org/jeps/454) instead of JNA.

## Differences from tmducken

**Better stability.** Built on JDK 22+ Project Panama (`java.lang.foreign.*`) instead of JNA:

- Native memory lives in scoped `Arena`s — released deterministically, not when the GC runs.
- GC-race use-after-free segfaults are ruled out by construction.

**More DuckDB types.** Read and write support for BLOB, HUGEINT, DECIMAL,
INTERVAL, ENUM, LIST, STRUCT, MAP, and all timestamp precision variants — types
tmducken does not handle.

**Streaming appender API.** `open-appender` / `append-dataset!` /
`flush-appender!` keep DuckDB's appender alive across batches, amortizing setup
cost — up to **10× faster** than repeated `insert-dataset!` for small-batch
ingest (see [Streaming
inserts](#streaming-inserts-appender-vs-many-one-shot-inserts)).

**Performance tuned.** Parallel column encode/decode, direct `MethodHandle` FFI
dispatch, partitioned parallel-concat for multi-chunk reads — up to **4×
faster** than tmducken (see [Benchmarks](#benchmarks)).

## Requirements

- **JDK 22+** (Panama FFM is a final API as of JDK 22)
- **DuckDB 1.5+** (tested against 1.5.2)

## Installation

Add the dependency and the required JVM option to your `deps.edn`:

```clojure
{:deps {ai.dyal/ducktape {:mvn/version "0.1.0-SNAPSHOT"}}
 :aliases
 {:dev {:jvm-opts ["--enable-native-access=ALL-UNNAMED"]}}}
```

The `--enable-native-access=ALL-UNNAMED` JVM option is required — Panama's
FFI refuses native downcalls without it.

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

## Streaming inserts

For producers that feed the database many small batches (Kafka consumers,
paginated API ingest, file shards), use the stateful appender API to
amortize DuckDB's per-call setup costs across batches:

```clojure
(with-open [app (duck/open-appender conn sample-ds)]
  (doseq [batch dataset-stream]
    (duck/append-dataset! app batch))
  ;; close flushes; or call (duck/flush-appender! app) for explicit
  ;; commit points if you need bounded data-loss windows.
  )
```

`sample-ds` is a `tech.v3.dataset` whose column dtypes (and `:name`
metadata) define the schema every batch must match. Multiple appenders
can be open simultaneously on the same connection — typically one per
destination table.

See [Benchmarks](#benchmarks) for a quantitative comparison vs repeated
`insert-dataset!` calls (up to **10×** faster for tiny batches).

## API

| Function | Description |
|----------|-------------|
| `initialize!` | Load the DuckDB shared library. Call once at startup. |
| `open-db` / `close-db` | Open/close a database (path or in-memory) |
| `connect` / `disconnect` | Create/destroy a connection |
| `run-query!` | Execute SQL, ignore results (DDL, DML) |
| `create-table!` / `drop-table!` | Create/drop a table from a dataset schema |
| `insert-dataset!` | Bulk insert via DuckDB's data chunk appender API |
| `open-appender` / `append-dataset!` / `flush-appender!` | Long-lived streaming appender — amortizes setup across many batches |
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

1M rows, JDK 25, DuckDB 1.5.2, Apple M-series. Same JVM, same datasets, 1.5s JIT warmup per fn, 30 samples per phase per library, interleaved per-sample alternation. **Speedup** is `tmducken_mean / ducktape_mean`; values above 1.0× mean ducktape is faster. All twelve metrics are statistically significant at 95% CI.

| Workload         |        | tmducken rows/s | ducktape rows/s | **Speedup** |
|------------------|--------|----------------:|----------------:|:-----------:|
| **numeric**      | INSERT |      25,636,285 |      28,864,127 |  **1.13×**  |
|                  | QUERY  |      48,066,662 |     170,902,963 |  **3.56×**  |
| **string**       | INSERT |       2,626,336 |       4,190,803 |  **1.60×**  |
|                  | QUERY  |       4,677,947 |       8,327,285 |  **1.78×**  |
| **uuid**         | INSERT |      21,876,992 |      38,133,634 |  **1.74×**  |
|                  | QUERY  |      19,504,444 |      30,279,061 |  **1.55×**  |
| **mixed**        | INSERT |       6,341,387 |       9,288,231 |  **1.46×**  |
|                  | QUERY  |       9,254,418 |      18,987,116 |  **2.05×**  |
| **wide-numeric** | INSERT |      16,916,895 |      18,984,929 |  **1.12×**  |
|                  | QUERY  |      21,564,755 |      86,642,974 |  **4.02×**  |
| **wide-mixed**   | INSERT |       3,611,626 |       4,681,157 |  **1.30×**  |
|                  | QUERY  |       5,387,781 |       9,697,254 |  **1.80×**  |

**Workload schemas** (1M rows each):

- **numeric** — 4 columns: `int64`, `float64`, `int32`, `float32`.
- **string** — 3 columns: short string (~5 chars), long string (~25 chars), `int64` id.
- **uuid** — 2 columns: `int64` id, `UUID`.
- **mixed** — 4 columns: `int64`, `float64`, string, `LocalDate`.
- **wide-numeric** — 8 numeric/temporal columns: 2× `int64`, 2× `float64`, 2× `int32`, 2× `LocalDate`. Exercises the partitioned parallel-concat fast-path with enough columns to fully utilise typical core counts.
- **wide-mixed** — 10 columns: the 8 from `wide-numeric` plus 2 string columns. Realistic OLAP fact-table shape, mixing fast-path numeric columns with fallback-path string columns.

The bench harness lives in `dev/tmducken_comparison.clj`. Run `(require '[tmducken-comparison :as cmp])` then `(cmp/compare-all)`, or invoke individual workloads via `(cmp/compare-numeric)`, `(cmp/compare-wide-numeric)`, etc.

### Streaming inserts: appender vs many one-shot inserts

The streaming `open-appender` / `append-dataset!` API amortizes the per-call
DuckDB FFI setup (appender create/destroy, column-type probe, data chunk
allocation, logical type creation/destruction) across many batches. Below,
**100k total rows** split into varying numbers of batches; each cell is
`speedup-mean × / trimmed-mean ×` for `insert-dataset! ÷ appender`.

| Workload    | 10 batches × 10k rows | 100 batches × 1k rows | 1000 batches × 100 rows | 10000 batches × 10 rows |
|-------------|----------------------:|----------------------:|------------------------:|------------------------:|
| **numeric** | 1.15× / 1.27×         | **2.75×** \* / 2.98×  | **8.94×** \* / 9.14×    | **10.62×** \* / 10.54×  |
| **string**  | **1.09×** \* / 1.09×  | **1.53×** \* / 1.54×  | **4.82×** \* / 4.81×    | **9.19×** \* / 9.20×    |
| **mixed**   | **1.23×** \* / 1.18×  | **2.05×** \* / 2.01×  | **6.03×** \* / 6.12×    | **8.27×** \* / 9.28×    |

`*` = statistically significant at 95% CI on the mean. Same JVM (JDK 25,
DuckDB 1.5.2, Apple M-series), 1.5s warmup per fn, 30 interleaved samples.

The amortization scales with batch frequency. At 10 × 10k-row batches there
is little setup to amortize and per-batch encoding work dominates (1.1–1.3×).
At 10000 × 10-row batches the per-batch setup overhead dominates the
`insert-dataset!` path, so the streaming API wins by roughly an order of
magnitude. Numeric workloads see the largest relative gains because the
actual encoding work is cheapest, making setup overhead proportionally larger.

The bench harness lives in `dev/appender_comparison.clj`. Run
`(require '[appender-comparison :as ac])` then `(ac/compare-all)`, or
`(ac/compare-streaming :string 100000 1000)` for a single configuration.

## Development

### Nix

The included `flake.nix` provides DuckDB and sets `DUCKDB_HOME` automatically:

```bash
nix develop
```

### deps.edn

See [Installation](#installation) for the dependency coordinate and required
JVM option. Snapshots are published to Clojars; nothing else needs to be
configured.

## Releasing

Ducktape publishes to Clojars as `ai.dyal/ducktape`. The build script lives
in [`dev/build.clj`](dev/build.clj) and runs via the `:build` alias.

### Setting the version

The version is resolved in this order: `:version` CLI arg → `VERSION` env var →
`0.1.0-SNAPSHOT` default. So all three of these work:

```bash
VERSION=0.2.0-SNAPSHOT clj -T:build deploy
clj -T:build deploy :version '"0.2.0-SNAPSHOT"'
clj -T:build deploy                              # → 0.1.0-SNAPSHOT
```

### Local tasks

| Command                | What it does                                     |
|------------------------|--------------------------------------------------|
| `clj -T:build jar`     | Build the jar under `target/`                    |
| `clj -T:build install` | Install to `~/.m2` for local consumption         |
| `clj -T:build deploy`  | Publish to Clojars (needs credentials, below)    |
| `clj -T:build clean`   | Remove `target/`                                 |

### Deploy credentials

`deploy` reads two env vars:

- `CLOJARS_USERNAME` — your Clojars username
- `CLOJARS_PASSWORD` — a [Clojars deploy token](https://clojars.org/tokens),
  ideally scoped to `ai.dyal/*`. **Not** your account password.

### Snapshot via GitHub Actions

Run the **Release** workflow from the repo's Actions tab. The default
version is `0.1.0-SNAPSHOT`; override it in the workflow input if needed.

### Tagged release

The release flow has two steps: stamp the changelog, then tag.

```bash
# 1. Prepend the new release section to CHANGELOG.md
git cliff --tag v0.1.0 --unreleased --prepend CHANGELOG.md

# 2. Commit, tag, push
git add CHANGELOG.md
git commit -m "docs: changelog for v0.1.0"
git tag v0.1.0
git push origin main v0.1.0
```

The Release workflow then:

1. Runs the test suite.
2. Publishes `ai.dyal/ducktape 0.1.0` to Clojars.
3. Re-runs [`git-cliff`](https://git-cliff.org) for release notes (same content
   as the new `CHANGELOG.md` section).
4. Creates a GitHub Release at the tag with those notes as the body.

Preview the notes before stamping:

```bash
git cliff --unreleased    # what would land in the next release
git cliff --latest        # what landed in the most recent release
```

Sections in `CHANGELOG.md` are grouped by Conventional Commit type (`feat:` →
Features, `fix:` → Bug Fixes, `perf:` → Performance, etc.) per the rules in
[`cliff.toml`](cliff.toml).

## License

MIT — Copyright © 2026 Dynamic Alpha Technologies Inc. See [`LICENSE`](LICENSE).
