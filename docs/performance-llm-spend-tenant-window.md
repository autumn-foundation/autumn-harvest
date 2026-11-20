# LLM spend: the tenant window reads the index only (issue #1997)

A tenant LLM cap runs `llm_budget::load_llm_spend` before each LLM step. The
tenant sum read every ledger row of each in-window run from the heap. It then
dropped the rows outside the window. A covering index on
`(workflow_exec_id, recorded_at)` makes the window a key range, and the sum
reads the index. Migration `20261120174554_harvest_llm_ledger_run_recorded_index`
adds it.

## 🎯 Workload

The database half of an agent fleet's LLM steps. Each step of a type with a
tenant cap calls `ActivityContext::check_llm_budget`. That check calls
`load_llm_spend(conn, exec_id, 86_400, true)`, which runs `LLM_SPEND_SQL` once.
Each completed step writes one `harvest_llm_ledger` row.

The harness is
`autumn-harvest/tests/integration/llm_spend_tenant_window_perf.rs`. Its
capture test seeds a fresh database with a production-shaped fixture:

| Item | Value |
|---|---|
| Runs over 30 days | 200,004 |
| Runs with a quota key | 179,984, cubic skew over 500 tenants |
| Workflow types | 3, at 70%, 20% and 10% of the runs |
| Run length | 90% 1–30 min, 9.7% 1–12 h, 0.3% 1–10 days |
| Calls per run | 70% 1–5, 25% 6–40, 5% 100–600, multi-day runs 500–3,000 |
| Ledger rows | 6,259,781, written in time order |
| Ledger rows in the 1-day window | 295,417 |
| Heap pages, all-visible | 94,480, of which 90,023 |

The rows before the window go in first, and a `VACUUM` marks their pages
all-visible. The rows in the window go in after it, with autovacuum off for
the ledger. On a live shard, autovacuum has not yet reached the newest rows.
So the fixture is the worst case for an index-only scan.

Three target runs read the spend. Each one is a running run of one tenant
scope. The scopes differ only in their in-window volume:

| Scope | Runs in the window | Ledger rows of those runs | Rows in the window |
|---|---|---|---|
| large (`support_agent` / `tenant-0`) | 571 | 36,229 | 18,420 |
| medium (`support_agent` / `tenant-1`) | 154 | 11,969 | 5,157 |
| small (`support_agent` / `tenant-3`) | 79 | 2,714 | 1,991 |

One capture runs 10 spend reads for each target, and 200 ledger inserts.

## 📈 Profile

`pg_stat_statements` over one capture, before the change:

| Statement | Calls | % calls | Buffers | % buffers |
|---|---|---|---|---|
| `LLM_SPEND_SQL` | 30 | 9.4 | 470,880 | 99.6 |
| ledger `INSERT` | 200 | 62.5 | 2,020 | 0.4 |
| `BEGIN`, `COMMIT`, `SET LOCAL jit = off` | 90 | 28.1 | 0 | 0.0 |

The spend read is the whole read cost of the workload. No statement spills.

## 🧭 Plan

Before, for the large scope (`before.explain.txt`):

```text
->  Aggregate (actual rows=1 loops=571)
      Buffers: shared hit=24396 read=9831
      ->  Bitmap Heap Scan on harvest_llm_ledger l (actual rows=32 loops=571)
            Recheck Cond: (l.workflow_exec_id = w.id)
            Filter: (l.recorded_at >= (now() - '24:00:00'::interval))
            Rows Removed by Filter: 31
            Heap Blocks: exact=31681
            ->  Bitmap Index Scan on harvest_llm_ledger_pkey (actual rows=63 loops=571)
```

After (`after.explain.txt`):

```text
->  Aggregate (actual rows=1 loops=571)
      Buffers: shared hit=16308
      ->  Index Only Scan using harvest_llm_ledger_run_recorded on harvest_llm_ledger l
            (actual rows=32 loops=571)
            Index Cond: ((workflow_exec_id = w.id) AND (recorded_at >= (now() - '24:00:00'::interval)))
            Heap Fetches: 18419
```

After a `VACUUM` (`after-vacuumed.explain.txt`), the same node reads
2,437 buffers with `Heap Fetches: 0`. The run scan, the tenant index scan and
the migration check do not change.

## 💡 Hypothesis

The primary key `(workflow_exec_id, event_id, call_index)` has no time
column. So the read takes every row of each run, and filters on
`recorded_at` after the heap fetch. The ledger takes rows in time order from
many runs at once. So each row of a run is on its own heap page:
31,681 heap blocks for 36,229 rows. A long run that is still active pays for
its whole history, although the window holds a small part of it.

An index keyed on `(workflow_exec_id, recorded_at)` reads only the rows in
the window. With the three summed columns in `INCLUDE`, the rows of a run are
adjacent leaf entries, so the sum reads few pages.

## 🔧 Change

Migration `20261120174554_harvest_llm_ledger_run_recorded_index`:

```sql
CREATE INDEX harvest_llm_ledger_run_recorded
    ON harvest_llm_ledger (workflow_exec_id, recorded_at)
    INCLUDE (input_tokens, output_tokens, cost_usd_micros);
```

It uses the guarded build of `docs/upgrading/online-migrations.md`. It
accepts an index that an operator prebuilt with `CONCURRENTLY`. It refuses
an invalid index or one with another definition. The query text does not
change.

**Lock:** the plain build takes `SHARE` on `harvest_llm_ledger`, which blocks
the ledger inserts of completing LLM steps. It took 4.4 s on the 6.26M-row
fixture. `lock_timeout` is 5 s for the wait. On a large ledger, prebuild it
with `CONCURRENTLY`. The ledger is not on the hot table list, and no other
table is locked. `down.sql` drops the index.

## 📊 Measurement

All counters come from one run of the harness, on one fixture, in one
session. A repeat run of the baseline gave 34,845 buffers for the large
scope, against 34,833 here.

| Counter | Tool | Before | After | Delta |
|---|---|---|---|---|
| Buffers per read, large | `EXPLAIN (ANALYZE, BUFFERS)` | 34,833 | 16,915 | −51% |
| Buffers per read, medium | `EXPLAIN (ANALYZE, BUFFERS)` | 10,343 | 3,488 | −66% |
| Buffers per read, small | `EXPLAIN (ANALYZE, BUFFERS)` | 1,912 | 1,191 | −38% |
| Buffers per read, large, after `VACUUM` | `EXPLAIN (ANALYZE, BUFFERS)` | 34,833 | 3,024 | −91% |
| Ledger rows read, large | `actual rows × loops` | 35,973 | 18,272 | −49% |
| Heap blocks, large | `EXPLAIN` | 31,681 | 18,419 heap fetches | −42% |
| Spend read buffers, 30 calls | `pg_stat_statements` | 470,880 | 215,940 | −54% |
| Ledger heap blocks, workload | `pg_statio_user_tables` | 427,097 | 203,700 | −52% |
| Temp blocks written | `pg_stat_statements` | 0 | 0 | — |
| Statements per spend read | `pg_stat_statements` | 4 | 4 | — |

The heap fetches that remain are the in-window rows on pages that are not
all-visible yet. Autovacuum removes them over time. So a read costs between
the cold and the vacuumed figure. The saving grows with the in-window
volume of the tenant: 721 buffers for the small scope, 17,918 for the large.

`pg_stat_user_indexes` shows the new index used: `idx_scan` 0 → 8,070 for
the capture. The primary key went from 8,070 scans to 0.

## ✅ Equivalence

`compare_with_and_without_index` reads each spend without the index, builds
it, and reads again. Both reads run in one transaction, so `NOW()` and the
window are the same instant. The transaction rolls back.

- On the production-shaped fixture, the 6 spends of the 3 targets, with and
  without the tenant sum, are identical (`result-rows.txt`). The test also
  checks that only the second read uses the index.
- `llm_spend_covering_index_does_not_change_the_spend` always runs. Its
  fixture has a row exactly on the window start and one 1 µs before it, a
  run that ended on the window start and one 1 µs before it, unpriced calls,
  a `MIGRATING` copy, a `MIGRATED` source with and without a committed move,
  a run with no key, a run of another tenant, a run with no rows, and the
  largest token and cost values that the ledger accepts. All 20 spends are
  identical, and the test pins the expected sums.

The existing `llm_budget_tests` and `llm_ledger_tests` pass unchanged. Run
together on one shared database, two `llm_budget_tests` fail with and
without the index. Each suite passes on its own.

## 💸 Write cost

| Counter | Tool | Before | After |
|---|---|---|---|
| WAL per ledger insert | `pg_stat_statements.wal_bytes`, 200 inserts | 442 B | 607 B (+37%) |
| Buffers per ledger insert | `pg_stat_statements` | 10.1 | 11.2 |
| Index size, 6.26M rows | `pg_relation_size` | — | 423 MB (primary key: 402 MB) |

One LLM step writes one ledger row per call. So one step with a tenant cap
pays about 165 WAL bytes more and saves 721 to 17,918 buffers per read in
this fixture. A deployment with no tenant cap never runs the tenant read. It
pays the write cost only.

## 🔬 Reproduce

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo xtask perf llm-spend-tenant-window
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest --features db,testing --test integration \
  -- llm_spend_tenant_window_perf
```

The server needs `pg_stat_statements` in `shared_preload_libraries`. The
artifacts are in `docs/perf-artifacts/llm-spend-tenant-window/`.
