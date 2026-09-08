# `scheduler::overdue_schedule_pass` — the sampler's own N+1 (the aux-lookup fix's follow-up)

`docs/performance-schedule-overdue-aux.md` batched three per-schedule DB
lookups out of `GET /admin/schedules`'s `load_schedule_overdue_aux_by_shard`
and explicitly named a second, unfixed call site carrying the identical
shape: `scheduler::overdue_schedule_pass`, the scheduler tick's own periodic
overdue-gauge sampler. That page's "Known limitations" section reads:

> **`overdue_schedule_pass`** (`autumn-harvest/src/scheduler.rs`), the
> scheduler tick's own periodic overdue-gauge sampler, has the identical
> per-schedule-loop shape calling `schedule_running_basis` and
> `resolve_effective_fire_at`. It is a background pass on a timer, not a
> per-HTTP-request path, so it was out of scope for this investigation
> (different workload, different profile), but it could use the same
> batched functions this change adds. Left as a follow-up rather than
> folded in here — this change is scoped to one measured workload.

This page is that follow-up.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it — the harness is in the repo
> precisely so you can.

## 🎯 Workload

`overdue_schedule_pass` runs on every worker's adaptive-interval sampler
tick (`worker.rs`'s `overdue_schedule_pass` call site), once per shard, over
every schedule row on that shard — a background pass, not a per-request
handler, so it runs continuously for the lifetime of a worker process
rather than once per operator page load. For each schedule it called
`scheduler::schedule_running_basis` (a `COUNT(*)` on
`harvest_workflow_executions` plus
`throttle::pending_throttle_count_for_workflow`, itself a `to_regclass`
existence check *and* a second count query, unconditionally, on every call)
and, for calendar-bearing schedules, `scheduler::resolve_effective_fire_at`,
which re-queries `calendar::load_exclusions_for_calendar` from scratch even
when several schedules share the same calendar. Up to three round trips per
schedule row, on every sampler tick, forever.

The harness is
`autumn-harvest/tests/integration/scheduler_overdue_pass_perf.rs`, reusing
the sibling investigation's exact fixture shape (500 schedules headline
depth, 3 shared calendars referenced by every 10th schedule, `RUNNING`/
`PAUSED` executions for every 4th schedule's workflow, pending-throttle rows
for every 7th) against a real Postgres 16 with `pg_stat_statements`
preloaded, calling `overdue_schedule_pass` directly (the one real public
entry point every worker's sampler tick calls — no HTTP layer needed, since
this is a library function, not an endpoint). Unlike the aux-lookup fix's
harness, this one sweeps three schedule-population sizes (n=50/200/500) in
one run so the artifact shows the O(n) → O(1) call-count shape directly, not
one point on a curve.

## 📈 Profile

`pg_stat_statements` call counts for the aux-lookup statement shapes
(running-basis `COUNT`, throttle `to_regclass` + `COUNT`, calendar-exclusions
`SELECT`) against the pre-fix code, one `overdue_schedule_pass` call per
size:

| n | aux_calls | aux_buffers | schedules_list_calls |
|--:|--:|--:|--:|
| 50 | 155 | 110 | 1 |
| 200 | 620 | 1,025 | 1 |
| 500 | 1,550 | 3,185 | 1 |

Calls scale exactly linearly at **3.1 calls per schedule** (155/50 =
620/200 = 1,550/500 = 3.1) — confirming the O(n) shape, not just asserting
it. At the 500-schedule headline depth the aux-lookup statements are
1,550 of the pass's 1,551 total calls (the `+1` is the single
`SELECT * FROM harvest_schedules` list query) — **99.94% of the pass's SQL
calls**. This is not a fraction of the workload worth weighing against a
floor; on this workload the aux lookups *are* the workload.

## 💡 Hypothesis

The mechanism is identical to the aux-lookup fix, because this is the exact
sibling code path that fix's own doc named as unfixed: replace the
per-schedule loop's individual `schedule_running_basis`/
`resolve_effective_fire_at` calls with the already-built, already-tested
batched forms (`schedule_running_basis_batch`,
`resolve_effective_fire_at_pure` fed by
`calendar::load_exclusions_for_calendars`), loaded once per shard instead of
once per schedule row.

## 🔧 Change

`scheduler::overdue_schedule_pass` now loads the running-basis batch and the
calendar-exclusions batch once, before the per-schedule loop, and the loop
reads both from the preloaded maps instead of issuing a query per row:

```rust
let schedule_names: Vec<(uuid::Uuid, &str)> = schedules
    .iter()
    .map(|s| (s.id, s.dag_name.as_deref().or(s.workflow_name.as_deref()).unwrap_or("")))
    .collect();
let basis = schedule_running_basis_batch(conn, &schedule_names).await?;

let calendar_names: Vec<&str> = schedules
    .iter()
    .filter_map(|s| s.calendar_name.as_deref())
    .collect::<std::collections::BTreeSet<_>>()
    .into_iter()
    .collect();
let exclusions = crate::calendar::load_exclusions_for_calendars(conn, &calendar_names).await?;

for s in schedules {
    let at_capacity = basis.get(&s.id).copied().unwrap_or(0) >= i64::from(s.max_active_runs);
    let effective_fire_at = s.calendar_name.as_deref().and_then(|cal_name| {
        let excluded = exclusions.get(cal_name).unwrap_or(&Vec::new());
        let exclude_weekends = crate::calendar::calendar_excludes_weekends(cal_name);
        resolve_effective_fire_at_pure(excluded, exclude_weekends, &s.skip_policy,
            s.schedule_expr.as_deref(), s.next_run_at)
    });
    // ... unchanged: schedule_overdue(&OverdueInputs { at_capacity, effective_fire_at, .. })
}
```

**Error semantics are unchanged, not just "close enough".** The old loop's
`schedule_running_basis(..).await?` and `resolve_effective_fire_at(..).await?`
each propagated a query failure as an `Err` that failed the whole pass (see
`worker.rs`'s call site: `Err(error) => { pass_complete = false; ... }`).
The batched calls use the identical `?` propagation at the top of the
function, so a failed batch still fails the whole pass — this deliberately
does **not** adopt the aux-lookup endpoint's degrade-to-`Default` fallback,
because that behavior change was not needed here and this investigation's
mandate is the smallest change that clears the floor.

**No new index, no schema change, no migration.** `schedule_running_basis`
and `resolve_effective_fire_at` (the original, unmodified single-item
functions) are untouched and still used by every other caller — the
single-schedule `GET /admin/schedules/{id}` read. Only `overdue_schedule_pass`
changed, from N calls to two batched queries per shard.

## 📊 Measurement

`pg_stat_statements` call/buffer counts for the aux-lookup statement shapes,
same fixture, same harness, before vs. after:

| n | aux_calls (before) | aux_calls (after) | Δ calls | aux_buffers (before) | aux_buffers (after) | Δ buffers |
|--:|--:|--:|--:|--:|--:|--:|
| 50 | 155 | 4 | **-97.4%** | 110 | 8 | -92.7% |
| 200 | 620 | 4 | **-99.4%** | 1,025 | 11 | -98.9% |
| 500 | 1,550 | 4 | **-99.7%** | 3,185 | 17 | -99.5% |

Calls collapse to a **constant 4** regardless of `n` (matching the
aux-lookup fix's own "4 SQL calls" result exactly, as expected — it is the
same three batched lookups plus the one schedule-list query) — the O(n) → O(1)
asymptotic shape is demonstrated directly across the three swept sizes, not
inferred from a single before/after pair. This clears the impact floor
several times over: an N+1 elimination (statement count drops from O(schedules)
to O(1) per shard) alone clears it, and the buffer reduction separately
clears the ≥20% threshold by roughly two orders of magnitude at every
tested size.

Full artifacts: `docs/perf-artifacts/schedule-overdue-pass/before-sweep.txt`,
`docs/perf-artifacts/schedule-overdue-pass/after-sweep.txt`.

## ✅ Equivalence

`overdue_schedule_pass_matches_the_original_per_schedule_loop` (in the
harness file, always-run, not `#[ignore]`d) seeds a 60-schedule fixture and
asserts the batched inputs (`schedule_running_basis_batch` +
`resolve_effective_fire_at_pure` fed by the batched exclusions) equal the
original, unmodified per-schedule functions' output, schedule-by-schedule,
same fixture, same connection — mirroring
`schedule_overdue_aux_perf.rs`'s own equivalence-test pattern for the
sibling fix. It asserts at least 5 schedules land in the real calendar
rebasing branch, so the comparison is not vacuous on an all-`None` fixture.

The pre-existing DB integration suite for this function,
`scheduler_overdue_tests.rs` (13 tests covering wedged/healthy/paused/
exhausted/at-capacity/calendar/throttle/backfill scenarios via
`overdue_schedule_pass` and its `sample_overdue_schedules` wrapper), passes
**unmodified** against the fixed code.

## Write cost

None — read-path-only rewrite of how existing lookups are batched; no
index added, no schema change.

## 🔬 Reproduce

```bash
service postgresql start   # local Postgres 16 with pg_stat_statements
                            # in shared_preload_libraries

# Equivalence + pre-existing regression suite (fast, always-run):
HARVEST_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres \
  cargo test -p autumn-harvest --features db --test integration -- \
  scheduler_overdue overdue_schedule_pass_matches --test-threads=1

# Evidence sweep (writes docs/perf-artifacts/schedule-overdue-pass/<label>-sweep.txt):
HARVEST_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres \
  PERF_LABEL=after \
  cargo test -p autumn-harvest --features db --test integration -- \
  zz_capture_overdue_schedule_pass_perf_evidence --ignored --nocapture
```

`HARVEST_TEST_DATABASE_URL` is treated as an **admin** URL, matching
`schedule_overdue_aux_perf.rs`'s convention: a fresh, uniquely-named
database is created off it per measured size, migrated via
`autumn_harvest::test_init_sql()`, seeded, and measured. Without it, the
harness falls back to a per-test testcontainers Postgres — sufficient for
the equivalence tests, but the `#[ignore]`d evidence-capture test needs a
target with `pg_stat_statements` preloaded.

## Reference environment

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16 (Ubuntu), default `shared_buffers` |
| Harness | `autumn-harvest/tests/integration/scheduler_overdue_pass_perf.rs` |
| Artifacts | `docs/perf-artifacts/schedule-overdue-pass/` (committed, this page's source) |

## Verification

- `cargo fmt --all` — clean.
- `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` — no
  Tier A findings, no Tier B regressions.
- `cargo clippy -p autumn-harvest --no-default-features --features db
  --all-targets -- -D warnings` — fails on six pre-existing errors in
  `context.rs`/`executor.rs` (a `#[cfg(any(test, feature = "testing"))]`
  branch clippy's pedantic/nursery lints flag under this toolchain's clippy
  version), the same failure `docs/performance-sqlite-runtime-drive.md` and
  PR #1217 already document for the identical unrelated cause. Confirmed
  zero findings on either changed file (`scheduler.rs`,
  `scheduler_overdue_pass_perf.rs`).
- `cargo test -p autumn-harvest --no-default-features --features testing
  --lib` — full unit suite passes unchanged.
- `cargo test -p autumn-harvest --features db --test integration --
  scheduler_overdue overdue_schedule_pass_matches --test-threads=1` — 13
  pre-existing DB tests plus the new equivalence test, all pass.

Checked for duplicate/overlapping work first: no open PR or issue touches
`overdue_schedule_pass`; `docs/performance-schedule-overdue-aux.md`
explicitly named it as the follow-up this page closes.
