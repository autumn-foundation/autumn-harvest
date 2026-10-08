# `mutex::reclaim_expired_leases_and_wake` — the lease scanner's own per-key round trips

`mutex.rs`'s module doc names two per-holder loops that share this shape —
`release_all_locks_for_holder`, `delete_waiters_for_holder` — but both are
bounded by how many keys *one execution* holds or waits on, small in the
common case. `reclaim_expired_leases_and_wake` has no such bound: it is the
durable-mutex lease scanner's crash-recovery sweep, called from
`timeout::enforce_timeouts_once` on every worker's periodic timeout tick,
over **every** expired lease on the shard at once. This page measures and
fixes its own unaudited per-key N+1.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it — the harness is in the repo
> precisely so you can.

## 🎯 Workload

A worker-fleet restart, or a rolling deploy while many workflows hold
`ctx.mutex()` locks, is exactly the burst this function exists to drain: a
crash leaves leases stranded, and the next timeout tick on a surviving
worker reclaims all of them at once. That is precisely the shape this
repo's own performance playbook names directly: bookkeeping queries that
are individually trivial but collectively dominant, and do not show up in
a buffers ranking, only in a `calls` ranking (`docs/performance.md`).

The harness is
`autumn-harvest/tests/integration/mutex_lease_reclaim_perf.rs`, calling
`reclaim_expired_leases_and_wake` directly (the one real public entry
point every worker's timeout tick calls, wrapped in the same
`conn.transaction(...)` `timeout::enforce_timeouts_once` uses) against a
real Postgres 16 with `pg_stat_statements` preloaded. It sweeps three
fixture sizes (n=200/1,000/2,000 expired-lease keys) so the artifact shows
the call-count shape directly, not one point on a curve. The fixture
mixes four populations per the module's own crash-recovery scenario: a
live (non-expired) lease control, an expired lease held by a `PAUSED`
execution (not reclaimable), a plain reclaim with no waiter, and a
reclaim with a genuine external waiter — a fraction of which also carry
the reclaimed holder's own stale waiter row, to stress the wake target.

## 📈 Profile

`pg_stat_statements` call counts for the per-key reclaim/waiter-cleanup/
head-of-line group (everything on `harvest_mutex_locks` or
`harvest_mutex_waiters` except the one-time candidate-select and the
advisory lock), pre-fix code, one sweep per size:

| n | reclaim_loop_calls | reclaim_loop_buffers | advisory_calls | wake_calls |
|--:|--:|--:|--:|--:|
| 200 | 525 | 2,287 | 175 | 54 |
| 1,000 | 2,631 | 12,363 | 877 | 276 |
| 2,000 | 5,262 | 21,786 | 1,754 | 554 |

Calls scale at exactly **3 per reclaimed key** (525/175 = 2,631/877 =
5,262/1,754 = 3.0) — the advisory-lock count is the true reclaimed-key
count, and every one of them pays the fenced reclaim `DELETE`, the
waiter-cleanup `DELETE` and the head-of-line `SELECT`, even the common
"no waiter at all" key. At the 2,000-key headline depth the reclaim loop
is 5,262 of the sweep's 5,262 + 1,754 + 554 = 7,570 total calls to the
two mutex tables and the queue — the loop this page targets **is** the
sweep's own bookkeeping cost, not a fraction of it worth weighing against
a floor.

## 💡 Hypothesis

The fenced reclaim, the waiter-row delete and the head-of-line lookup can
collapse into one statement via CTEs, cutting the per-key round-trip
count from three to one. The per-key **advisory lock** stays a separate,
first statement — it is the ABBA deadlock-avoidance mechanism the module
doc documents (`mutex.rs`'s "Advisory-first lock ordering" section), and
batching it away would change locking semantics this page has no mandate
to touch.

## 🔧 Change

`reclaim_expired_lock_and_wake_target_stmt` (`mutex.rs`) replaces the
three separate statements with one:

```sql
WITH reclaimed AS (
    DELETE FROM harvest_mutex_locks l
    WHERE l.lock_key = $1 AND l.lease_expires_at < now()
      AND NOT EXISTS (SELECT 1 FROM harvest_workflow_executions e
                       WHERE e.id = l.holder_exec_id AND e.state = 'PAUSED')
    RETURNING holder_exec_id
),
deleted_waiter AS (
    DELETE FROM harvest_mutex_waiters w
    USING reclaimed r
    WHERE w.lock_key = $1 AND w.waiter_exec_id = r.holder_exec_id
    RETURNING w.waiter_exec_id
)
SELECT
    (SELECT holder_exec_id FROM reclaimed) AS reclaimed_holder,
    (SELECT w2.waiter_exec_id FROM harvest_mutex_waiters w2
     WHERE w2.lock_key = $1
       AND EXISTS (SELECT 1 FROM reclaimed)
       AND NOT EXISTS (SELECT 1 FROM deleted_waiter dw
                        WHERE dw.waiter_exec_id = w2.waiter_exec_id)
     ORDER BY w2.id ASC LIMIT 1) AS head_waiter
```

**The subtlety this fix has to get right.** A data-modifying CTE's effect
is visible only to a statement that names it, never to a plain scan of
the same base table in the same top-level statement — unlike three
separate statements in one transaction, where each later statement's own
snapshot *does* see the previous statement's writes. So `head_waiter`
cannot simply re-`SELECT` `harvest_mutex_waiters`: a naive rewrite would
still see the physically-present (not-yet-visible-as-deleted) self-waiter
row and incorrectly return the reclaimed holder as head of line instead
of the real next-in-line waiter. `head_waiter`'s subquery instead
anti-joins against `deleted_waiter`'s own `RETURNING` output to exclude
exactly the row the three-statement sequence would have already deleted
by the time its `SELECT` ran.

`reclaim_expired_leases_and_wake`'s loop body shrinks from four
statements (advisory lock, reclaim, waiter-delete, head-of-line) to two
(advisory lock, the combined statement):

```rust
advisory_lock(conn, &k.lock_key).await?;
let row: ReclaimAndWakeTargetRow =
    diesel::sql_query(reclaim_expired_lock_and_wake_target_stmt())
        .bind::<diesel::sql_types::Text, _>(&k.lock_key)
        .get_result(conn)
        .await
        .map_err(database_error)?;
if row.reclaimed_holder.is_some() {
    if let Some(head) = row.head_waiter {
        crate::queue::wake_workflow_task(conn, ExecutionId::from_uuid(head)).await?;
    }
    count += 1;
}
```

**Behavior is unchanged, not just "close enough."** `count` still
increments exactly when the old `if let Some(h) = reclaimed` branch would
have; `wake_workflow_task` is still called exactly when the old code's
`head_of_line` lookup would have returned a waiter, targeting the same
execution. `wake_workflow_task` itself (a separate, already-existing
function touching `harvest_task_queue`) is untouched — this fix changes
only the `harvest_mutex_locks`/`harvest_mutex_waiters` bookkeeping ahead
of it.

**No new index, no schema change, no migration.** `reclaim_expired_lock_stmt`,
`delete_holder_waiter_for_key_stmt` and `head_of_line_stmt` are untouched
and still used by `release_all_locks_for_holder` and
`delete_waiters_for_holder` — only `reclaim_expired_leases_and_wake`'s own
loop changed.

## 📊 Measurement

Same harness, same three fixture sizes, before vs. after:

| n | calls (before) | calls (after) | Δ calls | buffers (before) | buffers (after) | Δ buffers |
|--:|--:|--:|--:|--:|--:|--:|
| 200 | 525 | 175 | **-66.7%** | 2,287 | 2,296 | +0.4% |
| 1,000 | 2,631 | 877 | **-66.7%** | 12,363 | 12,317 | -0.4% |
| 2,000 | 5,262 | 1,754 | **-66.7%** | 21,786 | 21,878 | +0.4% |

Calls collapse from 3 per reclaimed key to exactly **1**, at every swept
size — the O(3n) → O(n) round-trip shape is demonstrated directly, not
inferred from one pair. `advisory_calls` and `wake_calls` are identical
before and after at every size (175/877/1,754 and 54/276/554
respectively), confirming the fix touches only the three statements it
targets and changes nothing else's call count.

**Buffers do not move, and that is expected, not a miss.** This fix
eliminates round trips for work Postgres was already doing cheaply — the
three pre-fix statements are simple keyed lookups against small, indexed
tables, so combining them changes how many times the client and server
exchange a message, not how many pages get touched to do the same
`DELETE`/`DELETE`/`SELECT`. Buffers are the right metric for the
sibling fixes on this page's parent document (`docs/performance.md`),
where the defect was *re-scanning the same rows once per candidate* —
that defect does not exist here, so this page does not claim a buffer
win it did not observe.

**The admissible evidence for this fix is syscalls, not buffers** — per
this repo's own floor for I/O and lock-related work. A whole-process
`strace -f -c` of the identical evidence-capture run (all three fixture
sizes, one process) against the pre-fix and post-fix binaries:

| syscall | before | after | Δ |
|:--|--:|--:|--:|
| `sendto` | 25,247 | 14,023 | **-44.5%** |
| `recvfrom` | 25,619 | 15,135 | **-40.9%** |

Both directly measure the DB-socket round trips this fix removes, and
both clear this page's impact floor on their own. Full artifacts:
`docs/perf-artifacts/mutex-lease-reclaim/{before,after}-sweep.txt` (the
`pg_stat_statements` sweep) and
`docs/perf-artifacts/mutex-lease-reclaim/{before,after}-strace-summary.txt`
(the `strace -c` capture).

## ✅ Equivalence

`reclaim_wakes_the_correct_head_of_line_and_leaves_everything_else_alone`
(in the harness file, always-run, not `#[ignore]`d) seeds a 90-key
fixture and asserts, against the fixed code: a non-expired lock is never
touched; a `PAUSED`-holder's expired lock is never reclaimed and its
waiter (if any) stays parked; a plain reclaim with no waiter wakes
nobody; and — the case that specifically exercises the anti-join — a
reclaimed key carrying both the holder's own stale waiter row and a
genuine external waiter wakes the external waiter, never the holder, even
though the holder's row has the smaller `id`. This same test passed
unmodified against the pre-fix code first (see the RED commit), so it is
a real regression guard, not a test written to fit the fix.

The pre-existing `mutex_tests.rs` DB integration suite (the mutual-
exclusion witness tests covering acquire/release/reclaim/renew through
the real `Worker` poll loop) passes **unmodified** against the fixed
code.

## Write cost

None — read-path bookkeeping restructuring only. No index added, no
schema change, no migration.

## 🔬 Reproduce

```bash
service postgresql start   # local Postgres 16 with pg_stat_statements
                            # in shared_preload_libraries

# Correctness (always-run):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  cargo test -p autumn-harvest --features db --test integration -- \
  mutex_lease_reclaim_perf --test-threads=1

# pg_stat_statements sweep (writes docs/perf-artifacts/mutex-lease-reclaim/<label>-sweep.txt):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  PERF_LABEL=after \
  cargo test -p autumn-harvest --features db --test integration -- \
  zz_capture_mutex_lease_reclaim_perf_evidence --ignored --nocapture --test-threads=1

# strace syscall capture, against the compiled test binary directly:
BIN=$(cargo test -p autumn-harvest --features db,testing --test integration \
  --no-run --message-format=json 2>/dev/null | \
  python3 -c 'import json,sys
for l in sys.stdin:
    d=json.loads(l)
    if d.get("reason")=="compiler-artifact" and d.get("target",{}).get("name")=="integration" and d.get("executable"):
        print(d["executable"])')
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  strace -f -c -o /tmp/strace.txt "$BIN" --ignored --nocapture --test-threads=1 \
  zz_capture_mutex_lease_reclaim_perf_evidence
```

`HARVEST_TEST_DATABASE_URL` is treated as an **admin** URL: a fresh,
uniquely-named database is created off it per measured size, migrated via
`autumn_harvest::test_init_sql()`, seeded, and measured. Without it, the
harness falls back to a per-test testcontainers Postgres — sufficient for
the correctness test, but the `#[ignore]`d evidence-capture test needs a
target with `pg_stat_statements` preloaded.

To reproduce the "before" half of either capture, temporarily revert
`autumn-harvest/src/mutex.rs`'s `reclaim_expired_lock_and_wake_target_stmt`
addition and `reclaim_expired_leases_and_wake`'s loop body to the commit
before this page's fix (the RED commit checks out cleanly against the
pre-fix code, since it adds only the harness).

## Reference environment

| | |
|:--|:--|
| Machine | linux / shared vCPU (wall-clock inadmissible; see below) |
| Postgres | 16 (Ubuntu), default `shared_buffers` |
| Harness | `autumn-harvest/tests/integration/mutex_lease_reclaim_perf.rs` |
| Artifacts | `docs/perf-artifacts/mutex-lease-reclaim/` (committed, this page's source) |

**Wall-clock is not evidence on this page.** This machine is a shared
cloud vCPU; every number above is a deterministic count
(`pg_stat_statements` calls/buffers, `strace -c` syscalls), never a
timer. No wall-clock figure is quoted anywhere on this page.

## Verification

- `cargo fmt --all` — clean.
- `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` — no
  Tier A findings, no Tier B regressions in either changed file.
- `cargo clippy -p autumn-harvest --no-default-features --features db
  --all-targets -- -D warnings` — clean on `mutex.rs` and
  `mutex_lease_reclaim_perf.rs`.
- `cargo test -p autumn-harvest --no-default-features --features testing
  --lib` — full unit suite passes unchanged.
- `cargo test -p autumn-harvest --features db --test integration --
  mutex_lease_reclaim_perf mutex_tests --test-threads=1` — the new
  harness plus the full pre-existing `ctx.mutex()` DB suite, all pass.

Checked for duplicate/overlapping work first: no open PR or issue touches
`mutex.rs`'s lease scanner; the two other per-holder loops it shares a
module with (`release_all_locks_for_holder`, `delete_waiters_for_holder`)
are bounded by one execution's own key set and are named here as
known-limitation follow-ups, not folded into this change.

## Known limitations

* **`release_all_locks_for_holder` and `delete_waiters_for_holder` carry
  the identical three-statement-per-key shape**, unfixed by this page.
  Both are bounded by how many keys one execution holds or waits on
  (typically 0 or 1), so the same combined-statement technique would
  apply but was not measured to matter at that bound. Left as a
  follow-up rather than folded in here, per the same
  measure-before-widening discipline `docs/performance-schedule-overdue-pass.md`
  applies to its own sibling loops.
* **The buffer flatness is specific to this fix's mechanism** (fewer round
  trips over the same work), not a general finding about combining
  statements. A future fix to one of the two loops above should still
  measure buffers, in case its access pattern differs from this one's.
