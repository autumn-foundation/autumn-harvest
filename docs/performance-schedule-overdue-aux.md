# `GET /admin/schedules` — the overdue-aux N+1

`load_schedule_overdue_aux_by_shard` (in `autumn-harvest-plugin/src/api.rs`)
computes, for every schedule on a shard, the tick-exact `at_capacity`
suppression basis and the calendar-adjusted `effective_fire_at` the overdue
read depends on (issue #696 / Codex round 3). It did that **one schedule row
at a time**: for every row it called `scheduler::schedule_running_basis` (a
`COUNT(*)` on `harvest_workflow_executions` plus
`throttle::pending_throttle_count_for_workflow` — itself a `to_regclass`
existence check *and* a second count query, unconditionally, on every call)
and, for calendar-bearing schedules, `scheduler::resolve_effective_fire_at`,
which re-queries `calendar::load_exclusions_for_calendar` from scratch even
when several schedules share the same calendar. Up to four round trips per
schedule row, on every single `GET /admin/schedules` request — the exact
class of bug this repo's own performance playbook names directly: "workflow/
activity bookkeeping queries that are individually trivial but collectively
dominant... these won't show up in a buffers ranking, only in a `calls`
ranking."

This endpoint backs the Vantage schedules management page (issue #951),
shipped recently enough that it had not yet been profiled.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it — the harness is in the repo
> precisely so you can.

## TL;DR

* **The per-row loop issues up to four queries per *schedule*, not per
  *shard*.** Against a 500-schedule fixture (3 shared calendars, every 10th
  schedule referencing one; `RUNNING`/`PAUSED` executions seeded for every
  4th schedule's workflow; pending-throttle rows for every 7th), one
  `GET /admin/schedules` request issued **1,550 SQL calls** touching
  **2,185 buffers** in the aux-lookup statement shapes alone —
  **99.55% of the whole request's calls and 98.51% of its buffers**
  (1,557 calls / 2,218 buffers total).
* **The fix batches all three lookups per shard**: one grouped
  `RUNNING`/`PAUSED` count query (`scheduler::schedule_running_basis_batch`),
  one grouped pending-throttle count query
  (`throttle::pending_throttle_counts_for_workflows`, plus a single
  `to_regclass` check instead of one per schedule), and one grouped
  calendar-exclusions query keyed by *distinct* calendar name
  (`calendar::load_exclusions_for_calendars`) — each covering every schedule
  row on the shard at once. Measured on the identical fixture: **4 SQL
  calls**, **16 buffers** — a **99.74% reduction in calls** (387.5x fewer)
  and a **99.27% reduction in buffers** (136.6x fewer). The whole request's
  totals fall from 1,557 calls / 2,218 buffers to 11 calls / 49 buffers.
* **No new index, no schema change, no migration.** The per-schedule decision
  logic (`scheduler::resolve_effective_fire_at_pure`) is unchanged, pure,
  DB-free code, extracted verbatim from the existing
  `resolve_effective_fire_at`; only how its inputs are fetched changed. The
  original single-item functions (`schedule_running_basis`,
  `resolve_effective_fire_at`, `pending_throttle_count_for_workflow`,
  `load_exclusions_for_calendar`) are untouched and still used by every other
  caller (the single-schedule `GET /admin/schedules/{id}` read, the schedule
  tick's own `overdue_schedule_pass` sampler).
* **Result-equivalence is exact, verified two ways**: a direct function-level
  comparison (the batched functions' output against the original functions'
  output, schedule-by-schedule, same fixture, same run) and the three
  pre-existing `GET /admin/schedules` overdue/at-capacity/calendar
  integration tests, run unmodified against the fixed code.

## Reference environment

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16 (Ubuntu), default `shared_buffers` |
| Harness | `autumn-harvest-plugin/tests/schedule_overdue_aux_perf.rs` |
| Artifacts | `docs/perf-artifacts/schedule-overdue-aux/` (committed, this page's source) |

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_LABEL=before \
  cargo test -p autumn-harvest-plugin --test schedule_overdue_aux_perf \
    -- --ignored --exact zz_capture_schedule_overdue_aux_perf_evidence --nocapture
```

`HARVEST_TEST_DATABASE_URL` is treated as an **admin** URL: a fresh,
uniquely-named database is created off it, migrated via
`autumn_harvest::test_init_sql()`, seeded, measured, and left in place for
inspection. `pg_stat_statements` must be preloaded via
`shared_preload_libraries = 'pg_stat_statements'` on the target instance;
without it a Docker/testcontainers fallback is used for the two always-run
equivalence tests only (they need no `pg_stat_statements`) — the
`#[ignore]`d evidence-capture test needs a real target with the extension
preloaded.

The "before" snapshot (this page's numbers) was captured by running the
harness against the pre-fix `load_schedule_overdue_aux_by_shard` — the
harness itself only calls the public HTTP entry point and reads
`pg_stat_statements`, so it is unchanged between the before/after runs; only
the code behind the endpoint moved.

## Profile

The fixture's `pg_stat_statements` snapshot after one real
`GET /admin/schedules` request shows three statement shapes dominating the
whole request:

| statement | calls | buffers |
|:--|--:|--:|
| `COUNT(*) FROM harvest_workflow_executions WHERE workflow_name = $1 AND state = ANY($2)` | 500 | 1,130 |
| `COUNT(*) FROM harvest_start_throttle WHERE workflow_name = $1` | 500 | 1,000 |
| `to_regclass($1) IS NOT NULL` | 500 | 5 |
| `SELECT excluded_date FROM harvest_calendar_exclusions WHERE calendar_name = $1` | 50 | 50 |
| **aux-lookup total** | **1,550** | **2,185** |
| **whole-request total** | 1,557 | 2,218 |
| **aux-lookup share** | **99.55%** | **98.51%** |

The remaining ~0.45%/1.49% is the single `SELECT * FROM harvest_schedules`
list query and the best-effort recent-backfill batch load
(`load_recent_backfills`, already batched via `schedule_id = ANY($1)` — not
touched by this change). The aux lookups are not a fraction of this
endpoint's cost; they are effectively the entire cost.

## The problem

```rust
for s in schedules {
    let name = s.dag_name.as_deref().or(s.workflow_name.as_deref()).unwrap_or("");
    let at_capacity = match scheduler::schedule_running_basis(&mut conn, name, s.id).await {
        Ok(basis) => basis >= i64::from(s.max_active_runs),
        Err(_) => false,
    };
    let effective_fire_at = scheduler::resolve_effective_fire_at(
        &mut conn, s.calendar_name.as_deref(), &s.skip_policy,
        s.schedule_expr.as_deref(), s.next_run_at,
    ).await.unwrap_or(None);
    aux.insert(s.id, ScheduleOverdueAux { at_capacity, effective_fire_at });
}
```

`schedule_running_basis` itself issues two queries
(`harvest_workflow_executions` count, then
`pending_throttle_count_for_workflow`'s existence check + count), and
`resolve_effective_fire_at` issues one more whenever the schedule has a
calendar. None of this is a mistake in any single query — every individual
statement is cheap (a handful of buffers) — the cost is purely that it runs
once per schedule row instead of once per shard.

## The fix

Three new batched functions, one per lookup, each doing exactly what the
per-item version did but for a whole `&[&str]` of names/calendars at once:

```rust
// autumn-harvest/src/scheduler.rs
pub async fn schedule_running_basis_batch(
    conn: &mut AsyncPgConnection, schedules: &[(Uuid, &str)],
) -> HarvestResult<HashMap<Uuid, i64>> { /* one GROUP BY query + one batched throttle call;
                                             keyed by schedule_id since #1160 (see addendum) */ }

pub fn resolve_effective_fire_at_pure(
    excluded: &[NaiveDate], exclude_weekends: bool, skip_policy_db: &str,
    schedule_expr: Option<&str>, next_run_at: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> { /* pure, DB-free -- the exact decision logic
                                 resolve_effective_fire_at already had */ }

// autumn-harvest/src/throttle.rs
pub async fn pending_throttle_counts_for_workflows(
    conn: &mut AsyncPgConnection, workflow_names: &[&str],
) -> HarvestResult<HashMap<String, i64>> { /* one to_regclass check + one GROUP BY query */ }

// autumn-harvest/src/calendar.rs
pub async fn load_exclusions_for_calendars(
    conn: &mut AsyncPgConnection, calendar_names: &[&str],
) -> HarvestResult<HashMap<String, Vec<NaiveDate>>> { /* one query, grouped in memory */ }
```

`load_schedule_overdue_aux_by_shard` collects the distinct names and distinct
calendar names present on the shard's schedules, calls each batched function
once, then loops over the schedules **in memory** (no DB access in the loop)
to build the same `ScheduleOverdueAux` map it always did.

The original single-item functions
(`schedule_running_basis`, `resolve_effective_fire_at`,
`pending_throttle_count_for_workflow`, `load_exclusions_for_calendar`) are
**not modified** — they are still used, unchanged, by the single-schedule
`GET /admin/schedules/{id}` read and by the scheduler tick's own
`overdue_schedule_pass` sampler (a separate, not-yet-profiled per-schedule
loop on a periodic background pass rather than a per-HTTP-request path — out
of scope for this change; see "Known limitations" below).

## Measurement

| | before | after | Δ |
|:--|--:|--:|--:|
| aux-lookup calls | 1,550 | 4 | **-99.74%** (387.5x fewer) |
| aux-lookup buffers | 2,185 | 16 | **-99.27%** (136.6x fewer) |
| whole-request calls | 1,557 | 11 | -99.29% |
| whole-request buffers | 2,218 | 49 | -97.79% |
| statement shapes (aux) | 4 | 4 | same shapes, O(1) instead of O(n) calls each |

Tool: `pg_stat_statements`, reset and snapshotted **once** around one real
`GET /admin/schedules` request against the identical 500-schedule fixture in
the identical test run (before/after captured as separate runs of the same
harness, with only `autumn-harvest-plugin/src/api.rs` toggled between the
pre-fix and post-fix version — see "Reproduce" below). The aux-lookup and
whole-request views are both derived from that single snapshot in-process,
not from two separate `pg_stat_statements` queries: `pg_stat_statements`
tracks queries against itself like any other statement, so a first snapshot
query would appear as a new row a second snapshot query could then pick up,
inflating the total (caught in review — an earlier version of this harness
queried it twice and over-counted the whole-request total by the first
query's own footprint, a few calls/buffers out of the small "after" totals).
Clears the impact floor several times over: this is an N+1 elimination
(statement count per request drops from O(schedules) to O(1) per shard),
which alone clears the floor, and the buffer reduction separately clears the
≥20% threshold by more than two orders of magnitude (136.6x fewer aux-lookup
buffers).

Full artifacts: `docs/perf-artifacts/schedule-overdue-aux/`
(`{before,after}-pg_stat_statements.txt` for the filtered aux-lookup shapes,
`{before,after}-all-statements.txt` for the whole-request ranking,
`{before,after}-fixture-summary.txt` for the exact fixture shape and
totals).

## Equivalence

Two independent checks, both passing:

1. **Function-level, exact.** `schedule_running_basis_batch_matches_per_schedule_loop`
   and `resolve_effective_fire_at_pure_matches_resolve_effective_fire_at`
   (in the harness file, always-run, not `#[ignore]`d) seed a 60-schedule
   fixture and assert the batched functions' output equals the *original,
   unmodified* per-item functions' output, called in a loop, against the
   identical fixture and connection — schedule-by-schedule for both the
   calendar check and (since #1160) the running-basis check. The calendar check
   additionally asserts at least 5 schedules land in the real rebasing
   branch (`Some(adjusted_fire_at)`), so the comparison isn't vacuously
   passing on an all-`None` fixture.
2. **End-to-end, pre-existing.** The three `GET /admin/schedules`
   overdue/at-capacity/calendar integration tests already in
   `api_scheduler_integration.rs` —
   `schedule_read_reports_overdue_fields`,
   `schedule_create_response_at_capacity_is_not_overdue`,
   `schedule_read_honors_calendar_deferred_fire` — pass **unmodified**
   against the fixed code. These exercise the real HTTP handler end to end
   (wedged/healthy overdue flags, at-capacity suppression, calendar-deferred
   `effective_fire_at`), independent of the harness above.

## Write cost

None — no index added, no schema change, read-path-only rewrite of how
existing lookups are batched.

## Known limitations

* **`overdue_schedule_pass`** (`autumn-harvest/src/scheduler.rs`), the
  scheduler tick's own periodic overdue-gauge sampler, has the identical
  per-schedule-loop shape calling `schedule_running_basis` and
  `resolve_effective_fire_at`. It is a background pass on a timer, not a
  per-HTTP-request path, so it was out of scope for this investigation
  (different workload, different profile), but it could use the same batched
  functions this change adds. Left as a follow-up rather than folded in here
  — this change is scoped to one measured workload.
* The fixture's calendar/throttle/running-basis distributions (every 10th /
  7th / 4th schedule) are synthetic, chosen to exercise every code path
  non-trivially, not fit to a specific production fleet's shape. The O(n)
  vs. O(1) call-count result does not depend on the exact ratios — it holds
  for any nonzero population of schedules per shard.

## Addendum (issue #1160): `schedule_id`-scoped counting

`schedule_running_basis`/`schedule_running_basis_batch` gained a `schedule_id`
parameter after this investigation shipped. A `ctx.continue_as_new_as(...)`
(#803) successor carries its predecessor's `schedule_id`/`scheduled_for` but
runs as the target `workflow_name`, so the name-only `COUNT(*)` this doc
describes could never see it — the schedule's `max_active_runs` cap silently
stopped covering the rest of a run once it changed type mid-chain. The fix
adds `OR schedule_id = $schedule_id` to the same single `COUNT(*)` (additive,
not a replacement: the existing `workflow_name = $name` clause still covers
every same-type run, manual triggers included, so a same-type schedule's
behavior is unchanged). This changed both functions' signatures and
`schedule_running_basis_batch`'s return type from `HashMap<String, i64>`
(keyed by name) to `HashMap<Uuid, i64>` (keyed by schedule id) — two
schedules can no longer be assumed to have independent, name-only bases, so
callers look results up by the schedule they asked about, not by name. Every
call site in this doc's own code (`load_schedule_overdue_aux_by_shard`,
`get_schedule`, `upsert_workflow_schedule_and_read_back`) and in
`overdue_schedule_pass` was updated accordingly; no new query round trip was
added (the disjunct is one extra `OR` clause on the existing `COUNT(*)`), so
this addendum does not change the O(1)-per-shard call-count result above.

## Reproduce

```bash
# Equivalence tests (fast, always-run, no pg_stat_statements needed):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest-plugin --test schedule_overdue_aux_perf -- \
  schedule_running_basis_batch_matches_per_schedule_loop \
  resolve_effective_fire_at_pure_matches_resolve_effective_fire_at

# Pre-existing end-to-end overdue/capacity/calendar tests, run against the fixed code:
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest-plugin --test api_scheduler_integration -- \
  schedule_read_reports_overdue_fields \
  schedule_create_response_at_capacity_is_not_overdue \
  schedule_read_honors_calendar_deferred_fire

# Full evidence capture (seeds 500 schedules; a few seconds):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_LABEL=after \
  cargo test -p autumn-harvest-plugin --test schedule_overdue_aux_perf -- \
  --ignored --exact zz_capture_schedule_overdue_aux_perf_evidence --nocapture
```

To reproduce the "before" numbers, `git stash` (or otherwise revert) just
`autumn-harvest-plugin/src/api.rs` (the batched helper functions in
`scheduler.rs`/`throttle.rs`/`calendar.rs` can stay — they are simply unused
by the reverted `load_schedule_overdue_aux_by_shard`), run the capture with
`PERF_LABEL=before`, then restore the file and re-run with `PERF_LABEL=after`.
