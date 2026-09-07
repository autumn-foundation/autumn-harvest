# `POST /dead-letters/discard` — the per-row delete N+1

`bulk_discard_dead_letters_for_selector` (in `autumn-harvest-plugin/src/
api.rs`) batches its own row selection (`query_dead_letters_for_api_bulk`) up
to `dlq::MAX_BULK_LIMIT` (1,000) rows in one query, then discarded the
matches **one row at a time**: a `for row in rows` loop issuing one
`DELETE FROM harvest_dead_letters WHERE id = $1` per row. The crate's own
embedder-facing library function, `dlq::bulk_discard_dead_letters` — "the
external-embedder surface" its own doc comment names it, for callers that
use this crate directly rather than through the plugin's HTTP layer — had
the identical per-row loop.

Purging every dead letter left behind by one broken activity is exactly the
shape `MAX_BULK_LIMIT` is sized for (issue #1421): an operator narrows the
DLQ view to `activity_name = <broken activity>` and clicks "discard all
matching," and every one of those up-to-1,000 rows issued its own `DELETE`
statement.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it — the harness is in the repo
> precisely so you can.

## TL;DR

* Against a 5,000-row DLQ fixture (1,000 matching `activity_name =
  'dlq_bulk_perf_target'`, 4,000 noise dead letters spread across 25 other
  activities and 4 queues), one `POST /dead-letters/discard` request at the
  endpoint's own row cap issued **1,000 single-row `DELETE` statements** —
  see the profile numbers below for calls/buffers share.
* **The fix adds `dlq::discard_dead_letters_batch`**, which replaces the
  loop at both call sites with one `DELETE ... WHERE id = ANY($1)
  RETURNING id`. Unlike `audit::insert_audit_batch` (a multi-row `INSERT`
  that binds one parameter per column per row, and so has to chunk to stay
  under PostgreSQL's 65,535-parameter wire-protocol cap), `id = ANY($1)`
  binds the entire id list as a **single array parameter** — there is no
  bound-parameter ceiling to chunk against here even at the 1,000-row
  bulk-discard cap, or at 10x that (proven directly, see Equivalence).
* **No new index, no schema change, no migration.** `harvest_dead_letters`'
  primary-key index already serves `id = ANY(...)` exactly as it served
  `id = $1`.
* **Both call sites fixed from one shared primitive**: the live HTTP path
  (`api.rs::bulk_discard_dead_letters_for_selector`) and the library's own
  `dlq::bulk_discard_dead_letters`, whose doc comment already says to "keep
  the two in sync." Only the measured, real-workload path — the HTTP
  endpoint — is profiled below; the library function shares the identical
  fix mechanically, not as a second, separately-measured change.
* **Result-equivalence is exact on the happy path**, proven by comparing
  the batch against a per-row delete loop over the same ids, including a
  mixed set where some ids never existed (`discard_dead_letters_batch_matches_per_row_delete_loop`).
  One disclosed difference on the *unhappy* path: the old per-row loop was
  best-effort (a mid-loop connection failure left earlier deletes
  committed and abandoned the rest); the batched `DELETE` is one statement,
  so a failure now leaves *zero* rows deleted rather than an arbitrary
  prefix. See "Equivalence" below for why that is a strengthening, not a
  weakening, of the prior guarantee.

## Reference environment

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest-plugin --test dlq_bulk_discard_perf -- --test-threads=1
```

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16.13 (Ubuntu), default `shared_buffers`, `pg_stat_statements` preloaded |
| Harness | `autumn-harvest-plugin/tests/dlq_bulk_discard_perf.rs` |

`HARVEST_TEST_DATABASE_URL` is an **admin** URL: a fresh, uniquely-named
database is created and migrated per run, matching the convention in
`schedule_bulk_audit_perf.rs` and `claim_bench_support.rs`. With the
variable unset the harness falls back to a `postgres:16` testcontainer.

## Profile

One `POST /dead-letters/discard {"activity_name": "dlq_bulk_perf_target",
"limit": 1000}` request, 1,000 matching + 4,000 noise dead letters,
`pg_stat_statements` reset immediately before the request and snapshotted
immediately after it (before, i.e. the per-row loop):

| statement shape | calls | buffers | share of request calls | share of request buffers |
|:--|--:|--:|--:|--:|
| `DELETE FROM harvest_dead_letters WHERE id = $1` (per-row, before) | 1 000 | 4 000 | 99.4% | 92.4% |
| whole request (before) | 1 006 | 4 329 | 100% | 100% |

99.4% of calls is nowhere near the "under 5% of both calls and buffers,
stop" floor — this is squarely the class of N+1 the profiling step exists
to catch, the same pattern `schedule_bulk_pause_ui`/`schedule_bulk_resume_ui`'s
audit-insert loop showed (`docs/performance-schedule-bulk-audit.md`). The
rest of the request is one audit-log insert, one `SELECT` for the matching
page, and one `COUNT(*)` for `matched` — none of them the target here.

## The fix

```rust
// Before: one diesel::delete(...find(id)).execute(conn).await per row.
for row in rows {
    let id = row.id;
    let deleted = diesel::delete(harvest_dead_letters::table.find(id))
        .execute(conn)
        .await
        .map_err(database_error)?;
    if deleted > 0 {
        result.acted_on += 1;
        result.ids.push(id.to_string());
    } else {
        result.skipped += 1;
    }
}

// After: one discard_dead_letters_batch(conn, &ids).await per page.
let ids: Vec<uuid::Uuid> = rows.iter().map(|row| row.id).collect();
let deleted: HashSet<uuid::Uuid> = dlq::discard_dead_letters_batch(conn, &ids)
    .await?
    .into_iter()
    .collect();
result.ids = ids.iter().filter(|id| deleted.contains(id)).map(ToString::to_string).collect();
result.acted_on = result.ids.len();
result.skipped = ids.len() - result.acted_on;
```

`dlq::discard_dead_letters_batch` (`autumn-harvest/src/dlq.rs`):

```rust
pub async fn discard_dead_letters_batch(
    conn: &mut AsyncPgConnection,
    ids: &[Uuid],
) -> HarvestResult<Vec<Uuid>> {
    use crate::schema::harvest_dead_letters::dsl;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    diesel::delete(dsl::harvest_dead_letters.filter(dsl::id.eq_any(ids)))
        .returning(dsl::id)
        .get_results(conn)
        .await
        .map_err(crate::error::database_error)
}
```

`dlq::bulk_discard_dead_letters` (the library's own embedder-facing bulk
function) is rewritten to call the same primitive, replacing its own
identical per-row loop.

## Measurement

Same fixture, same request, after the fix:

| statement shape | calls | buffers |
|:--|--:|--:|
| `DELETE ... WHERE id = ANY($1) RETURNING id` (batched, after) | 1 | 2 104 |
| whole request (after) | 7 | 2 433 |

| | before | after | Δ |
|:--|--:|--:|--:|
| discard-delete calls | 1 000 | 1 | **-99.9%** (1,000x fewer) |
| discard-delete buffers | 4 000 | 2 104 | **-47.4%** |
| whole-request calls | 1 006 | 7 | **-99.3%** |
| whole-request buffers | 4 329 | 2 433 | **-43.8%** |

The call-count elimination alone clears the impact floor ("statement count
per request drops from O(n) to O(1)" needs no other justification). The
buffer count also dropped substantially and was not the target — with the
per-row loop, each single-row `DELETE ... WHERE id = $1` cost 4 buffers on
average (a primary-key btree descent plus a heap fetch, paid 1,000 times);
the batched form still visits the same rows through the same primary-key
index, but a single `= ANY($1)` scan reuses buffer pins across matches
instead of re-walking the index root/branch pages on every call, so the
same logical work costs fewer total buffer touches. This is a bonus, not
the claim: the statement-count elimination is what clears the floor.

Full before/after artifacts (`pg_stat_statements` snapshots and the whole
request's statement list) are committed under
[`docs/perf-artifacts/dlq-bulk-discard/`](perf-artifacts/dlq-bulk-discard/).

## Equivalence

`discard_dead_letters_batch_matches_per_row_delete_loop`
(`autumn-harvest-plugin/tests/dlq_bulk_discard_perf.rs`) covers two cases in
one test:

* **Every id exists.** 25 dead letters inserted, all 25 ids passed to
  `discard_dead_letters_batch` in one call. Asserts the returned `Vec`,
  sorted, equals the input ids sorted, and that zero rows matching those
  ids remain in `harvest_dead_letters` afterward.
* **A mixed id set.** 10 real dead-letter ids plus 10 freshly-generated
  UUIDs that were never inserted, passed together. Asserts the function
  returns exactly the 10 real ids — the missing ones are silently absent,
  matching the old loop's `Ok(0) => skipped += 1` branch rather than
  erroring.

`discard_dead_letters_batch_on_empty_slice_is_a_no_op` covers the
zero-match edge case: an empty slice sends no statement and returns
`Ok(vec![])`.

`discard_dead_letters_batch_handles_a_large_id_list_unchunked` seeds 10,001
dead letters and deletes all of them in one `discard_dead_letters_batch`
call, asserting every one comes back deleted. This is the direct rebuttal
to the obvious wrong guess ("surely this needs chunking like
`insert_audit_batch`"): `id = ANY($1)` binds one array parameter regardless
of how many ids are inside it, so there is no per-row bind-parameter cost
to hit a wire-protocol ceiling with.

The end-to-end evidence capture itself (`zz_capture_dlq_bulk_discard_perf_evidence`)
asserts `matched == acted_on == 1000` and `skipped == 0` against the live
HTTP handler, and separately confirms all 4,000 noise rows remain
untouched — the filter's selectivity is exact, not just the delete count.

The pre-existing `dlq_bulk_integration.rs` regression tests —
`bulk_discard_with_empty_filter_returns_400`,
`bulk_discard_removes_entries_without_enqueueing`,
`bulk_discard_error_class_deletes_only_matching`,
`bulk_cause_post_filter_precedes_limit`,
`bulk_cause_across_shards_honors_global_limit`,
`bulk_cause_dry_run_count_equals_aggregate_facet` — pass unchanged against
the fix: none of them assert on statement count, and the response shape
(`matched`/`acted_on`/`skipped`/`ids`/`dry_run`/`failures`) is unchanged.

**Disclosed behavior change, unhappy path only.** The old per-row loop was
best-effort under a mid-operation failure: a connection error on row *k*
left rows `1..k-1` deleted and committed, rows `k..N` untouched, and
reported nothing about the abandoned tail beyond a hard error from the
whole handler. A single `DELETE ... WHERE id = ANY($1)` is one statement:
if it fails, it fails atomically, and *zero* of the targeted rows are
deleted. This is a strengthening (an operator retrying a failed bulk
discard no longer has to wonder which prefix of the page already landed),
not a weakening — and the `failures` field of `BulkDlqResult`, which the
old discard loop never actually populated in practice either (a
primary-key `DELETE` has no per-row business-logic failure mode the way
`replay_dead_letter` does), stays in the struct for API-shape compatibility
but is now always empty for discard specifically.

## Write cost

No new index. No schema change. `harvest_dead_letters`' primary-key index
is the only index this delete path touches either way — the batched
statement deletes the same rows through the same index, just via one
statement instead of N.

## Scope: why `bulk_replay` is untouched

`bulk_replay_dead_letters_for_selector` and `dlq::bulk_replay_dead_letters`
have the identical per-row loop shape, calling `replay_dead_letter` once
per row. That function is not a simple delete: it is its own transaction
doing a `SELECT ... FOR UPDATE`, an optional `harvest_workflow_executions`
lookup, a task-queue `INSERT`, and the same-row `DELETE` — real per-row
business logic (including a distinct code path for `callback`-type dead
letters), not a repeated identical statement. Batching it is a
substantially different, higher-risk change and is out of scope here; see
the "Banned changes" and "Ask before" sections of this repo's Ledger
charter on why a real per-row transaction is not blindly flattened into
one statement.

## Reproduce

```bash
# Equivalence tests (fast, always-run, no pg_stat_statements needed):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest-plugin --test dlq_bulk_discard_perf -- \
  discard_dead_letters_batch_matches_per_row_delete_loop \
  discard_dead_letters_batch_on_empty_slice_is_a_no_op \
  discard_dead_letters_batch_handles_a_large_id_list_unchunked

# Full evidence capture (seeds 5,000 dead letters; a few seconds):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_LABEL=after \
  cargo test -p autumn-harvest-plugin --test dlq_bulk_discard_perf -- \
  --ignored --exact zz_capture_dlq_bulk_discard_perf_evidence --nocapture
```

To reproduce the "before" numbers, revert `autumn-harvest-plugin/src/api.rs`'s
`bulk_discard_dead_letters_for_selector` to its pre-fix per-row loop (the new
`dlq::discard_dead_letters_batch` can stay defined — it is simply unused by
the reverted handler), run the capture with `PERF_LABEL=before`, then
restore the file and re-run with `PERF_LABEL=after`.
