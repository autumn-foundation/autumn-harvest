# `entity_sql::run`: the fixed cost of a query and the cost per row

`Worker::query_entity_sql` (`worker.rs:36626`) runs read-only user SQL over
the state of one keyed entity (issue #2260). It takes a permit of
`QUERY_SLOTS` (`worker.rs:36637`), so at most `MAX_CONCURRENT_QUERIES` (4)
queries run at a time (`entity_sql.rs:84`). It finds the snapshot through
`entity_record` (`worker.rs:36647`), then calls `run_cancellable` in
`spawn_blocking` (`worker.rs:36668`). The engine never calls it on its own.
User code calls it on demand, and each call runs one query. The harness
drives the public wrapper `run` (`entity_sql.rs:207`), which calls
`run_cancellable` (`entity_sql.rs:226`) with a new cancel flag. Each query
opens a fresh in-memory database in `open` (`entity_sql.rs:387`). That
builds the schema, encodes the state as JSON, inserts the row, sets the
limits and installs the authorizer (`entity_sql.rs:426`). The query then runs
under a progress handler (`entity_sql.rs:238`). `to_json`
(`entity_sql.rs:503`) converts each result value (`entity_sql.rs:286`).

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`). Wall-clock timing is not admissible evidence
on this shared-vCPU machine.

**Outcome: baseline only. No code change.**

One query has a fixed cost of 411,663 instructions. The open, the setup
statements and the close are 86.57% of a point query. A scan then costs
about 8,179 instructions per array item, and SQLite JSON work is most of
that. The cost is linear in the item count. The authorizer and the progress handler are
below 0.3% in every shape.

## Workload

[`benches/entity_sql_profile.rs`](../autumn-harvest/benches/entity_sql_profile.rs)
calls [`entity_sql::run`](../autumn-harvest/src/entity_sql.rs) with an
`EntityRecord` of fixed ids. It opens no Postgres connection.
`ENTITY_SQL_PROFILE_SHAPE` picks the state and the SQL:

| Shape | State | SQL | Asserted outcome |
|---|---|---|---|
| `point` (default) | The state of the `entity_sql` unit tests: two items and an owner. | `SELECT json_extract(state, '$.owner') FROM entity` | One row, `"ada"`. |
| `scan` | N items of three fields: `sku`, `qty` and `price_cents`. | `count(*)` and `sum` of `price_cents` over `json_each(entity.state, '$.items')`, where `qty >= ?1`. | The count and the sum that the harness computes. |
| `rows` | As `scan`. | `json_extract` of `sku` and `qty` for each item. | N rows, and the last row matches. |

The `scan` filter keeps 60% of the items. The `rows` shape returns N rows of
two values, so `to_json` runs 2N times. N must not pass `MAX_ROWS`.

The harness builds the record once. Each query runs inside the
`#[inline(never)]` wrapper `measure_run`, so its inclusive cost leaves the
setup out. After the setup, `settle_heap` asks for one 4 KiB block. That
makes glibc merge the small chunks that the setup freed, so the first query
does not pay for them. `ENTITY_SQL_PROFILE_REPS` (default 1) repeats the
query. The first query of a process also runs the one-time SQLite
initialization.

The progress handler reads the clock and the SQLite memory counter at each
check. The number of checks depends only on the virtual machine steps.
`MAX_QUERY_TIME` is a wall-clock deadline. A query that passed it under
valgrind would fail the run with `TooCostly`. No run did. Every pair of runs
below gave the same total, to the instruction.

## Running

```bash
BIN=$(cargo bench -p autumn-harvest --features owner-disk-cache \
  --bench owner_disk_cache_profile --bench entity_sql_profile --no-run \
  --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="entity_sql_profile") | .executable')
ENTITY_SQL_PROFILE_SHAPE=scan ENTITY_SQL_PROFILE_N=1000 valgrind --tool=callgrind \
  --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --inclusive=yes --threshold=99 cg.out
ENTITY_SQL_PROFILE_SHAPE=point ENTITY_SQL_PROFILE_REPS=101 valgrind --tool=callgrind \
  --branch-sim=no --cache-sim=no --toggle-collect='entity_sql_profile::measure_run' \
  --callgrind-out-file=cg.point.out "$BIN"
callgrind_annotate --threshold=90 cg.point.out
ENTITY_SQL_PROFILE_SHAPE=scan ENTITY_SQL_PROFILE_N=1000 valgrind --tool=dhat \
  --num-callers=64 --dhat-out-file=dhat.json "$BIN"
```

Set `ENTITY_SQL_PROFILE_SHAPE` to `point`, `scan` or `rows`. The flat self
views below use `--toggle-collect` on `measure_run`. That run collects only
the cost inside the wrapper.

## Profile

Callgrind, total `Ir` at the headline sizes. Two runs each:

| Shape | N | Reps | Run 1 | Run 2 | `measure_run` | Share |
|---|---:|---:|---:|---:|---:|---:|
| `point` | | 101 | 42,281,711 | 42,281,711 | 41,613,589 | 98.42% |
| `scan` | 1000 | 1 | 16,196,104 | 16,196,104 | 8,657,007 | 53.45% |
| `rows` | 1000 | 1 | 18,948,880 | 18,948,880 | 10,980,264 | 57.95% |

The rest of each total is the harness: it builds the state, drops it and
checks the result. A rebuild of the harness after a comment edit moved each
total by 4 `Ir`. The `measure_run` costs stayed the same. A review rerun of
the same binary in another environment moved each total by 47 `Ir` and each
`measure_run` by 9. Its two runs of each shape also matched.

### The fixed cost of one query

The `point` shape ran at three repetition counts:

| Reps | Total Ir | `measure_run` |
|---:|---:|---:|
| 1 | 976,344 | 433,457 |
| 11 | 5,119,409 | 4,563,958 |
| 101 | 42,281,711 | 41,613,589 |

Between the 11-query run and the 101-query run, each added query costs
411,663. A check run of 51 queries puts `measure_run` at 21,044,159, so each
query from 51 to 101 costs 411,388. The first query costs 433,457, with
22,886 of one-time `sqlite3_initialize`. The split below
divides the 101-query run by 101:

| Part | Ir per query | Share |
|---|---:|---:|
| `measure_run` | 412,016 | 100.00% |
| Setup statements in `execute_batch`: two `PRAGMA`s and `CREATE TABLE` | 248,987 | 60.43% |
| `sqlite3StartTable`, with the schema load `sqlite3Init` (90,732) | 93,969 | 22.81% |
| `sqlite3EndTable` | 44,843 | 10.88% |
| `sqlite3AddColumn` | 16,038 | 3.89% |
| `Connection::open_in_memory`, as `open_with_flags` | 74,617 | 18.11% |
| FTS3, FTS5, R*Tree and DBSTAT registration inside the open | 51,357 | 12.46% |
| `Connection` drop | 33,081 | 8.03% |
| Row `INSERT` | 26,526 | 6.44% |
| Prepare the user SQL | 14,766 | 3.58% |
| Step and read the result | 6,445 | 1.56% |
| State encode, `serde_json::to_writer` | 1,727 | 0.42% |
| Authorizer callbacks | 802 | 0.19% |

The `sqlite3StartTable`, `sqlite3EndTable` and `sqlite3AddColumn` rows are
part of the `execute_batch` row. They include the parse of the built-in
schema table, which `sqlite3Init` runs at the first statement. The
registration row is part of the open row.

The top self costs of the `point` run are `_int_malloc` (2,587,570, 6.22%),
`pthread_mutex_lock` (2,079,039, 5.00%) and `sqlite3RunParser` (1,943,442,
4.67%). `pthread_mutex_unlock` adds 1,292,425 (3.11%). SQLite takes a global
mutex for its memory count. `entity_sql` reads that count for the memory
budget.

### The cost per item, `scan`, N=10000

| Part | Ir | Per item | Share |
|---|---:|---:|---:|
| `measure_run` | 82,267,356 | | 100.00% |
| SQLite execution, `Rows::advance` | 70,507,203 | 7,050.7 | 85.70% |
| `json_extract`, 16,000 calls | 35,158,761 | 3,515.9 | 42.74% |
| The parse of each `value` inside it, `jsonParseFuncArg` | 21,201,110 | 2,120.1 | 25.77% |
| The `json_each` parse of the whole state, `jsonEachFilter` | 10,472,078 | 1,047.2 | 12.73% |
| State encode in `open`, `serde_json::to_writer` | 9,561,085 | 956.1 | 11.62% |
| Row `INSERT` | 1,684,981 | 168.5 | 2.05% |
| Progress handler callbacks | 240,045 | 24.0 | 0.29% |
| Setup statements, open, close and prepare | 506,683 | | 0.62% |

The rows are inclusive and overlap. The JSON rows run inside
`Rows::advance`. The encode and the `INSERT` run in `open`. `json_each` renders each item back to JSON text as `value`. Each
`json_extract(value, ...)` then parses that text again. The top self costs
of the `scan` run at N=1000 are `jsonTranslateTextToBlob` (1,230,669,
14.22%) and `jsonTranslateBlobToText` (630,400, 7.28%).

### The cost per result row, `rows`, N=10000

`measure_run` costs 105,741,379. `Rows::advance` is 82,096,881 (77.64%). The
conversion of the 10,000 result rows costs 11,894,954 (11.25%), or 1,189.5
per row of two values:

| Part | Ir | Per row |
|---|---:|---:|
| `Row::get_ref`, 20,000 calls | 5,130,000 | 513.0 |
| Allocations of the row `Vec` and the text | 3,322,764 | 332.3 |
| `run_cancellable` self, with the inlined `to_json` | 1,510,546 | 151.1 |
| `String::from_utf8_lossy` | 1,210,000 | 121.0 |
| `Vec::push` of the row | 591,644 | 59.2 |
| `memcpy` of the text | 130,000 | 13.0 |

## Scaling

Callgrind, total `Ir` of the harness and the inclusive cost of `measure_run`.
The ratio is against the row above.

| Shape | N | Total Ir | `measure_run` | Ratio |
|---|---:|---:|---:|---:|
| `scan` | 100 | 2,510,099 | 1,304,729 | |
| `scan` | 1000 | 16,196,104 | 8,657,007 | 6.64 |
| `scan` | 10000 | 152,172,617 | 82,267,356 | 9.50 |
| `rows` | 100 | 2,762,847 | 1,530,049 | |
| `rows` | 1000 | 18,948,880 | 10,980,264 | 7.18 |
| `rows` | 10000 | 179,944,948 | 105,741,379 | 9.63 |

Both shapes are linear. The `scan` slope is 8,169 per item from N=100 to
N=1000, and 8,179 from N=1000 to N=10000. The `rows` slopes are 10,500 and
10,529. A check run at N=3000 put `measure_run` at 25,007,953 for `scan` and
32,025,328 for `rows`. Each is within 0.05% of the line through N=1000 and
N=10000. A line through N=1000 and N=10000 meets N=0 at 478,079 for `scan`
and at 451,251 for `rows`. That is the fixed cost, with the first-query
initialization and a longer SQL text. No term is superlinear.

## Allocations (dhat)

Dhat with `--num-callers=64`. The `measure_run` columns sum the allocation
points whose stack holds the wrapper.

| Shape | N | Reps | Total bytes | Blocks | `measure_run` bytes | `measure_run` blocks | Peak bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| `point` | | 101 | 10,292,535 | 29,422 | 10,276,830 | 29,090 | 85,317 |
| `scan` | 1000 | 1 | 1,956,218 | 13,373 | 543,694 | 1,348 | 1,381,185 |
| `rows` | 1000 | 1 | 2,078,066 | 15,380 | 665,565 | 3,354 | 1,381,185 |

One `point` query allocates 101,751 bytes in 288 blocks. The peak of that
run is 85,317 bytes. The `rows` query allocates 2,006 more blocks than
`scan` for 1,000 rows: one `Vec` and one `String` per row. In `scan` and `rows`, the harness
state sets the peak. Each run ends with 548 bytes in 2 blocks.

## Findings

### Where the cost goes

- **The fixed cost dominates a small query.** The open, the setup
  statements and the close take 86.57% of a point query. The user SQL,
  prepared and stepped, takes 5.15%.
- **SQLite JSON work dominates a large query.** In a scan, 85.70% is SQLite
  execution. Most of that is JSON work: one parse of the whole state, then a
  parse of each item for each `json_extract` call.
- **The state encode runs on every query.** It costs 956 per item, 11.62% of
  the scan at N=10000, whatever the SQL reads.
- **The guards are cheap.** The authorizer costs 802 per point query
  (0.19%). The progress handler costs 0.29% of the scan at N=10000.

The query cost is linear in the state size and in the result rows. The
caller pays more before `run`: `entity_record` loads the snapshot. On a file
hit, that is `load_entity`. Its decode costs about 78 instructions per state
byte, as
[`performance-owner-disk-cache.md`](performance-owner-disk-cache.md)
measures.

### Ideas, not implemented

| Idea | Location | Upper bound |
|---|---|---|
| Build the bundled SQLite without FTS3, FTS5, R*Tree and DBSTAT. The open then registers none of them. | The workspace `Cargo.toml:71` | 12.46% of a point query (51,357 `Ir`). |
| Keep the JSON text of the snapshot in the record, and bind it as is. A file hit then skips the decode in `load_entity` and the encode in `open`. | `entity_sql.rs:392` | 11.62% of a scan at N=10000, for the encode alone. |
| Replace the `CREATE TABLE` of each query with a prepared schema, such as a template database image. | `entity_sql.rs:391` | 37.58% of a point query (154,850 `Ir`). |

Each bound is the measured cost of the code that the idea removes. The
replacement work is not subtracted.

The first idea is a build flag of the bundled `libsqlite3-sys`. Every crate
of the workspace that uses `rusqlite` shares it, so it needs a check of those
crates. The second idea changes the public `EntityRecord`. The third bound is
loose, because a new database must still load some schema. A fresh database
for each query is also a rule of
[ADR 0012](adr/0012-owner-disk-cache.md). A template image must keep that
rule.

None of the ideas changes the order of growth. The JSON parse inside SQLite
depends on the user SQL, and the engine cannot remove it.

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.0, measured 2026-10-26.
