# `POST /ui/schedules/bulk-pause` and `bulk-resume` — the audit-insert N+1

`schedule_bulk_pause_ui` and `schedule_bulk_resume_ui` (in
`autumn-harvest-plugin/src/ui.rs`) each batch their own row selection and
`UPDATE ... RETURNING id` per shard, then audit the outcome one row at a
time: a `for id in &updated_ids` loop calling `audit::insert_audit` once per
updated schedule. Bulk-pausing or bulk-resuming N schedules on one shard
issued 1 read query, 1 batched update, and N single-row audit inserts —
every one of them on every bulk action.

This is the Vantage schedules management page's "select all matching, pause
them" button (issue #951): an operator filters to a workflow family and
pauses every schedule that matches, which is exactly the shape that makes N
large.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it — the harness is in the repo
> precisely so you can.

## TL;DR

* Against a 500-schedule fixture (300 matching a `target=` filter, 200
  non-matching noise schedules on the same shard), one
  `POST /ui/schedules/bulk-pause` request issued **300 audit-insert
  statements** — **98.4% of the whole request's SQL calls** (300 of 305).
  Buffers were a smaller share (43.3%, 2 590 of 5 980) — the N+1 shows up in
  the `calls` ranking, not the buffers ranking, exactly the pattern this
  repo's own performance playbook calls out.
* **The fix collects the per-row `NewAuditRecord`s into a `Vec` and issues
  one multi-row insert per shard** via the new `audit::insert_audit_batch`.
  Measured on the identical fixture: **1 SQL call**, a **99.67% reduction
  in calls** (300x fewer). Buffers moved only slightly (2 590 → 2 503,
  -3.4%): the same rows are written either way, so the win is round trips,
  not bytes.
* **`bulk-resume` shows the identical pattern and the identical fix** — it
  shares the same per-row audit loop, batched the same way.
* **No new index, no schema change, no migration.** `insert_audit` itself is
  untouched and still used by every single-row mutation (pause one
  schedule, delete one schedule, trigger one schedule). Only the two bulk
  handlers' audit loop changed, from N calls to `insert_audit_batch` once
  per shard.
* **Result-equivalence is exact**, verified by comparing every batched row
  against what the original per-row loop would have written, field by
  field, on a disjoint fixture in the same test
  (`insert_audit_batch_matches_per_row_insert_audit_loop`). The one
  disclosed difference: a single multi-row `INSERT` shares one `NOW()`
  across its rows, where N separate statements each got their own —
  every row in one bulk action now carries the exact same `occurred_at`,
  which is arguably more correct for an atomic bulk action, not less.

## Reference environment

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest-plugin --test schedule_bulk_audit_perf -- --test-threads=1
```

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16.13 (Ubuntu), default `shared_buffers`, `pg_stat_statements` preloaded |
| Harness | `autumn-harvest-plugin/tests/schedule_bulk_audit_perf.rs` |

`HARVEST_TEST_DATABASE_URL` is an **admin** URL: a fresh, uniquely-named
database is created and migrated per run, matching the convention in
`schedule_overdue_aux_perf.rs` and `claim_bench_support.rs`. With the
variable unset the harness falls back to a `postgres:16` testcontainer.

## Profile

One `POST /ui/schedules/bulk-pause target=bulk_perf_target` request, 300
matching + 200 non-matching schedules on one shard, `pg_stat_statements`
reset immediately before the request:

| statement shape | calls | buffers | share of request calls | share of request buffers |
|:--|--:|--:|--:|--:|
| `INSERT INTO harvest_audit_log ...` (per-row, before) | 300 | 2 590 | 98.4% | 43.3% |
| whole request (before) | 305 | 5 980 | 100% | 100% |

98.4% of calls is nowhere near the "under 5% of both calls and buffers,
stop" floor — this is squarely the class of N+1 the profiling step exists
to catch.

## The fix

```rust
// Before: one insert_audit(&mut conn, &ar).await per updated_ids entry.
for id in &updated_ids {
    let ar = NewAuditRecord { /* ... */ target_id: Some(id_str.as_str()), /* ... */ };
    let _ = insert_audit(&mut conn, &ar).await;
}

// After: one insert_audit_batch(&mut conn, &records).await per shard.
let records: Vec<NewAuditRecord<'_>> = id_strs
    .iter()
    .map(|id_str| NewAuditRecord { /* ... */ target_id: Some(id_str.as_str()), /* ... */ })
    .collect();
let _ = insert_audit_batch(&mut conn, &records).await;
```

`audit::insert_audit_batch` (`autumn-harvest/src/audit.rs`) is a thin
addition alongside the existing `insert_audit`: a multi-row
`insert_into(harvest_audit_log::table).values(records)`, returning the
generated ids in the same order. An empty slice never sends a statement —
an empty `VALUES` list has no `Insertable` representation — so a bulk
action that matched zero rows still does zero audit inserts, exactly as
before.

Every record in one bulk action's batch shares the same
actor/operation/route/status/shard; only `target_id` varies row to row, so
the batched statement is a straightforward
`INSERT ... VALUES ($1,...), ($9,...), ...` with the identical column
values the per-row loop would have written.

## Measurement

Same fixture, same request, after the fix:

| statement shape | calls | buffers |
|:--|--:|--:|
| `INSERT INTO harvest_audit_log ...` (batched, after) | 1 | 2 503 |
| whole request (after) | 6 | 5 899 |

| | before | after | Δ |
|:--|--:|--:|--:|
| audit-insert calls | 300 | 1 | **-99.67%** (300x fewer) |
| audit-insert buffers | 2 590 | 2 503 | -3.4% |
| whole-request calls | 305 | 6 | **-98.03%** |
| whole-request buffers | 5 980 | 5 899 | -1.35% |

`bulk-resume` (300 already-paused matching schedules, same 200 noise rows):

| | before | after | Δ |
|:--|--:|--:|--:|
| audit-insert calls | 300 | 1 | **-99.67%** |
| audit-insert buffers | 2 590 | 2 503 | -3.4% |
| whole-request calls | 305 | 6 | **-98.03%** |
| whole-request buffers | 6 578 | 6 491 | -1.3% |

The call-count elimination alone clears the impact floor ("statement count
per request drops from O(n) to O(1)" needs no other justification). The
buffer delta is small and reported honestly as such: the same 300 rows are
written to the same table either way, so most of the buffer cost is
inherent heap/index writes, not round-trip overhead.

Full before/after artifacts (`pg_stat_statements` snapshots, the whole
request's statement list, and fixture summaries for both actions) are
committed under
[`docs/perf-artifacts/schedule-bulk-audit/`](perf-artifacts/schedule-bulk-audit/).

## Equivalence

`insert_audit_batch_matches_per_row_insert_audit_loop` inserts 25 rows
through the original per-row `insert_audit` loop and 25 rows (disjoint
target ids) through `insert_audit_batch` in the same test run, then
compares every field except `id` and `occurred_at`: actor, operation,
target_type, route_or_command, request_id, idempotency_key, status,
error_summary, shard_id, source. All match.

`id` legitimately differs (a fresh UUID per row either way).
`occurred_at` legitimately differs in one specific way: every row written
by the batched insert shares exactly one `occurred_at`, proven in the same
test by collecting the batch's timestamps into a set and asserting its
size is 1 — a single multi-row `INSERT` runs inside one transaction, and
`NOW()` is stable per transaction. The N-statement loop it replaces got a
fresh `NOW()` per autocommitted statement instead. This is disclosed, not
hidden: every audit row produced by one bulk action now carries the
timestamp of that one atomic action, which better reflects what actually
happened than N slightly-different timestamps for rows that all resulted
from the same click.

`insert_audit_batch_on_empty_slice_is_a_no_op` covers the zero-match edge
case: an empty slice sends no statement and returns `Ok(vec![])`.

The pre-existing end-to-end regression tests in `ui_integration.rs` —
`ui_schedules_bulk_pause_pauses_matching_rows`,
`ui_schedules_bulk_pause_respects_the_health_filter`,
`ui_schedules_bulk_resume_reaches_auto_paused_rows`,
`ui_schedules_every_mutation_is_audited_as_ui_sourced`,
`ui_schedules_pause_redirect_resolves_to_the_list` — pass unchanged against
the fix: they assert on audit row counts and content (source, operation),
never on statement count, so the batched insert satisfies them exactly as
the per-row loop did.

## Write cost

No new index. No schema change. `harvest_audit_log`'s existing indexes
(`occurred_at`, `actor, occurred_at`, `target_type, target_id, occurred_at`,
`operation, occurred_at`) are unaffected — the batched statement inserts
the same rows into the same table, so their maintenance cost per row is
identical to before. The only thing that changed is the number of
round trips and parse/plan cycles Postgres spends getting those rows in.

## Reproduce

```bash
# Equivalence tests (fast, always-run, no pg_stat_statements needed):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest-plugin --test schedule_bulk_audit_perf -- \
  insert_audit_batch_matches_per_row_insert_audit_loop \
  insert_audit_batch_on_empty_slice_is_a_no_op

# Full evidence capture (seeds 500 schedules; a couple of seconds):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_LABEL=after \
  cargo test -p autumn-harvest-plugin --test schedule_bulk_audit_perf -- \
  --ignored --exact zz_capture_schedule_bulk_pause_audit_perf_evidence \
  zz_capture_schedule_bulk_resume_audit_perf_evidence --nocapture
```

To reproduce the "before" numbers, revert
`autumn-harvest-plugin/src/ui.rs` to its pre-fix per-row loop (the new
`audit::insert_audit_batch` in `autumn-harvest/src/audit.rs` can stay — it
is simply unused by the reverted handlers), run the capture with
`PERF_LABEL=before`, then restore the file and re-run with `PERF_LABEL=after`.
