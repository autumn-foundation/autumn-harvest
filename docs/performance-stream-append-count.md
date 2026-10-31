# Durable stream append: skip the stored-row count (issue #2083)

`store::append_stream_chunks` admits chunks against a per-run cap. Before this
change it counted every stored chunk of the run on each append. This note
records the profile and the change.

## 🎯 Workload

`tests/integration/stream_append_count_perf.rs` seeds running executions with
no chunks. It replays the whole lifetime of each stream through the public
`append_stream_chunks`, in ten-chunk appends, round by round across all live
streams. So chunks of different runs interleave in the heap, as concurrent
streams write them. A stream of `n` chunks makes `n / 10` appends, and append
`k` finds `10 k` chunks stored. Long streams therefore dominate.

Stream lengths are heavy-tailed and derive from `md5` of the ordinal: the
median run emits about 90 chunks and the p95 run about 770. One run in fifty
emits just over the 10,000 cap. The 1,000-run fixture stores 449,498 chunks
over 45,438 appends. Three sizes run: 100, 300 and 1,000 runs.

The `vacuumed` variant runs `VACUUM` whenever autovacuum's insert rule would
fire (`1000 + 0.2 * reltuples` inserts). A real autovacuum lags, so this is the
best case for the old count. The other variant never vacuums. Autovacuum is off
on the table, so both runs are deterministic.

## 📈 Profile

Baseline, 1,000 runs, vacuumed (`before-pg_stat_statements.txt`):

| Statement | Calls | Buffers | Share of buffers | Share of calls |
| --- | --: | --: | --: | --: |
| `INSERT` of a ten-chunk batch | 44,524 | 6,760,229 | 57.8% | 12.2% |
| **`SELECT COUNT(*)` of the run's chunks** | **45,438** | **4,332,498** | **37.0%** | **12.5%** |
| `SELECT max(stream_offset)` | 45,438 | 213,885 | 1.8% | 12.5% |
| `SELECT state, input` of the execution | 45,438 | 181,752 | 1.6% | 12.5% |

Total: 363,519 calls and 11,700,024 buffers. Never vacuumed, the count is
16,853,503 of 40,848,609 buffers (41.3%).

## 💡 Hypothesis

The count reads every stored chunk of the run, so its cost grows with the run's
length. The planner has no cheaper plan: `COUNT(*)` must visit each index entry
for the run, and without a fresh visibility map it fetches each heap row too. A
`min`/`max` probe reads about four buffers.

The count is only needed when the new chunks might not fit. Stored offsets are
distinct, so the stored count never exceeds `highest - lowest + 1`. When that
span plus the new chunks fits the cap, the room is at least the new chunks, and
the admitted count is the new chunk count, whatever the stored count is.

## 🔧 Change

`append_stream_chunks_in_tx` reads `min` and `max` of the real offsets in one
statement. `admitted_count` returns the new chunk count when the span plus the
new chunks fits the cap. Otherwise it runs the count as before. No migration,
no index and no schema change. The advisory lock, the terminal-run check and the
marker rule are unchanged.

## 📊 Measurement

`pg_stat_statements`, same harness and fixture, 1,000 runs:

| Counter | Before | After | Delta |
| --- | --: | --: | --: |
| Count statement calls (vacuumed) | 45,438 | 20 | -99.96% |
| Count statement buffers (vacuumed) | 4,332,498 | 7,228 | -99.83% |
| Total buffers (vacuumed) | 11,700,024 | 7,549,098 | -35.5% |
| Total buffers (never vacuumed) | 40,848,609 | 24,019,572 | -41.2% |
| Statements | 363,519 | 318,101 | -12.5% |
| WAL bytes | 168,229,686 | 168,229,716 | +30 bytes |

The merged `min`/`max` read costs 388,229 buffers against 213,885 for `max`
alone. Taking that into account, the two reads fall from 4,546,383 to 395,457
buffers (-91.3%). The 100 and 300 run sizes move the same way
(`after-summary.txt`).

## ✅ Equivalence

The table digest is the md5 of every stored `(exec, offset, chunk)` in a total
order. It is identical before and after at all six fixture points, for example
`b8d995d2fe4e94de67b362af945eb64b` at 1,000 runs. The stored chunk counts and
the number of inserted chunks match too.

`append_admission_edge_cases` runs on the old and the new code with the same
expectations: holes that never fill, negative offsets that still count, a
re-driven batch with no marker, the exact cap, a batch cut at the room, and an
empty run. The 25 `durable_stream` tests pass unchanged.

## 💸 Write cost

None. No index is added. WAL bytes match to 30 bytes.

## Not changed: the batch insert

The ten-row `INSERT` is 57.8% of buffers after the count goes, at about 150
buffers per statement. It is the larger remaining cost. It was not touched here.
A lead for a later pass: how much of it is the foreign-key check per row.

## 🔬 Reproduce

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
PERF_LABEL=after cargo test -p autumn-harvest --test integration \
  zz_capture_stream_append_evidence -- --ignored --nocapture
```

The server needs `pg_stat_statements` in `shared_preload_libraries` and a
superuser. Artifacts are in `docs/perf-artifacts/stream-append-count/`.
