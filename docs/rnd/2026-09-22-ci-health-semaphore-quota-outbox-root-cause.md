# 🚦 Semaphore CI health — `quota_enforcement_tests`' outbox-retry timeout:
# root cause found and fixed. It was never CPU-scheduling delay under load —
# the test's own worker config never wired `sharded_pool` into
# `WorkerRuntimeConfig`, so the background scanner that is supposed to
# retry a quota-deferred outbox row could never resolve a target shard's
# connection pool and could never succeed. The test only ever passed on a
# separate, undocumented one-shot race. **Corrected post-review (Codex on
# this PR):** an earlier draft over-claimed this fix also explains PR
# #1697's OTHER panic site (the already-30s source-completion wait) --
# it does not; that remained a distinct, unexplained flake at the time,
# retracted explicitly below rather than left standing. **UPDATE (Codex
# review, a later PR on this branch): that source-completion panic is now
# explained and fixed too.** #1713 (cherry-picked here as `8292b07`) found
# it: `integration_e2e.rs`'s testcontainers schema bundle was missing
# migration `20260920215812_harvest_completion_trigger_fires_target`, so
# every Docker-backed test database lacked a column `completion_trigger.rs`
# writes inside the SAME transaction that completes the source workflow --
# rolling back the source's own completion on every attempt. That is a
# second, independent fix in this PR's history, not something this
# report's own outbox-scanner fix explains; see `8292b07`'s commit message
# and #1713 for the full writeup. A second, separate flake in the same file
# is recorded further below, corrected in place rather than left standing.

**Status:** two fixes shipped on this branch. This session's own fix
touches `autumn-harvest/tests/integration/quota_enforcement_tests.rs` only
— no `ci.yml`, timeout-constant, or sharding-manifest change — and closes
out issue #1685's own recommendation ("whoever next has Docker available
in-session should point the ≥20x rerun campaign at the outbox-retry-loop
panic specifically"), continuing the series from
`docs/rnd/2026-09-21-ci-health-semaphore-quota-outbox-recurrence.md`. A
second, later, unrelated fix (`8292b07`, cherry-picked from #1713) closes
the source-completion panic this report originally left open -- see the
UPDATE above.

## 🎯 Starting point

Between the 09-21 report and today, two more data points landed on the same
test, `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`,
both on PR #1697 (`claude/trusting-ritchie-nml6ud`, unmerged as of this
writing):

1. `Test DB (linux, shard 0)` failed on commit `860a424` at the
   outbox-retry-loop panic (`quota_enforcement_tests.rs`, "target row was
   never created by the outbox retry; last count was 1") — the SAME
   signature the 09-16 and 09-21 reports tracked, on a branch whose own diff
   never touches this file (confirmed byte-identical to `origin/trunk-dev`
   at that point).
2. PR #1697 then found and fixed a second, genuinely separate, previously
   unfixed hardcoded `10s` in the SAME test — the outbox-retry poll loop a
   few lines below the already-widened (`#1673`) `30s` source-completion
   wait — and pushed that fix as commit `26d67fa`.
3. The SAME job failed AGAIN on `26d67fa`, but at the *other* wait: `Elapsed(())`
   from `wait_for_execution_state_with_timeout` at `quota_enforcement_tests.rs:3757`,
   the first wait, already at `30s` since `#1673`. So even a bound that had
   already been widened once for this exact flake was not always enough.

That is where this session started: two waits in the same test, both
already widened once, both still failing intermittently, no confirmed
mechanism after three reports (09-16, 09-21, and PR #1697's own two rounds).

## 🔬 Method: reproduce first, then instrument, then fix

No Docker in this session's sandbox either, but local Postgres 16
(`pg_ctlcluster 16 main start`) plus `HARVEST_TEST_DATABASE_URL` against a
freshly migrated database worked directly. A GitHub-hosted `ubuntu-latest`
runner is a 4-core box; this session's sandbox is also 4 cores, so CPU
oversubscription is directly comparable.

**Reproduction, not guessed at — measured:**

```sh
# 8x CPU oversubscription: 32 busy `yes` loops pinned across 4 cores,
# load average settles around 30+. Run the whole block in a subshell with
# its OWN `EXIT` trap -- pasted into an existing interactive shell, a bare
# top-level `trap ... EXIT` only fires when that shell itself later exits,
# leaving all 32 `yes` processes saturating the host in the meantime. The
# subshell's `EXIT` fires the moment this block finishes, regardless of
# how the surrounding shell is used afterward.
(
  STRESS_PIDS=()
  for i in $(seq 1 32); do yes > /dev/null & STRESS_PIDS+=("$!"); done
  trap 'kill "${STRESS_PIDS[@]}" 2>/dev/null' EXIT

  export HARVEST_TEST_DATABASE_URL="postgres://harvest:harvest@127.0.0.1:5432/harvest_test"
  for i in $(seq 1 20); do
    cargo test -p autumn-harvest --test integration -- \
      completion_trigger_defers_to_outbox_when_target_quota_exceeded \
      --test-threads=1
  done
)
```

Against `trunk-dev` HEAD (before this session's fix), this reproduced the
exact CI panic (`"target row was never created by the outbox retry; last
count was 1"`, at the then-still-`10s` retry loop) in roughly **5 of every
20 runs** (~25%) on a clean, single-run database — no cross-test state
needed. This is the first time in the series a session obtained a real,
repeatable local reproduction of this flake rather than only CI's own
occurrences.

## 🩺 Diagnosis: instrumented, not inferred

Temporary `eprintln!`-based tracing (gated behind an env var, added to
`autumn-harvest/src/completion_trigger.rs` for this investigation and fully
reverted before the fix below — none of it ships) at three points: the
scanner's tick entry and its pending-row count
(`enforce_completion_triggers_outbox_with_codecs`), the per-row relay
outcome (`relay_gate_checked_start`), and each of the four backoff-stamping
call sites in the scanner's per-task loop. Re-running the same stress
reproduction with this instrumentation on a clean-per-run database (deleting
`harvest_completion_trigger_outbox`/`harvest_workflow_executions` between
runs, to rule out cross-run leakage as a confound — see the unrelated
finding at the end) isolated the mechanism precisely.

**The scanner tick cadence itself was healthy** — `outbox_tick_enter` fired
every ~45-55ms under this stress level (versus the test's configured
`poll_interval` of 25ms — real but modest stretching, not starvation).

**The failing runs' scanner NEVER succeeded at the retry, on any tick, for
the entire 10s window** — but not because it could not reach the row in
time. Every attempt (there were 2-3 across a typical 10s failing window,
each ~5.0s apart, matching `QUOTA_REDEFER_BACKOFF`) hit the **same
early-continue branch**, `backoff_site=missing_pool`:

```rust
let Some(target_pool) = sharded_pool
    .as_ref()
    .and_then(|sp| sp.exact_pool_for(target_shard).cloned())
else {
    // ...
    stamp_outbox_relay_backoff(conn, task.id).await;
    continue;
};
```

`sharded_pool` here is the `Option<ShardedDbPool>` parameter threaded all
the way from `WorkerRuntimeConfig::sharded_pool`
(`worker.rs`, the `spawn_timeout_checker_for_shard` call site: `self.config.sharded_pool.clone()`)
through `enforce_timeouts_once` into
`enforce_completion_triggers_outbox_with_codecs`. It is **not** the same
thing as the `crate::shard::GLOBAL_SHARDED_POOL` static.

`quota_enforcement_tests.rs`'s shared `runtime_config()` test helper (used
by `build_runtime_worker`, in turn used by dozens of tests in this file)
hardcodes:

```rust
sharded_pool: None,
```

This test builds its own `ShardedDbPool::single(build_test_pool(&url))` —
previously bound to `_sharded_pool`, an **underscore-prefixed, never-read**
local — which self-installs into `GLOBAL_SHARDED_POOL` as `ShardedDbPool::single`'s
documented side effect (`shard.rs`). But that global is never the same
thing the worker's own scanner reads. **The scanner's `target_pool`
resolution was therefore guaranteed to fail on every single tick, for the
entire lifetime of this test, unconditionally — not a timing-dependent
near-miss.**

**So why does the test usually pass?** A second, separate mechanism:
`DeferredTriggerStart::spawn()` (`completion_trigger.rs`), a one-shot
`tokio::spawn`'d task fired *inline*, synchronously with the trigger
evaluation that defers the row to the outbox in the first place. Unlike the
scanner, `spawn()`'s body reads `GLOBAL_SHARDED_POOL` directly — the global
the test *does* populate — so it resolves the pool correctly and can
actually attempt the relay. But it is **one-shot**: exactly one
`relay_gate_checked_start` call, no retry loop of its own. Whether that one
attempt sees the blocker's quota slot already freed depends entirely on
**tokio scheduling order** between this spawned task and the test's own
subsequent code (two DB-query awaits, then `mark_terminal`) — all
cooperatively multiplexed onto one OS thread, since this test is a plain
`#[tokio::test]` (`current_thread` flavor). Under normal load, the test's
own awaits usually yield enough opportunities for the runtime to run the
spawned task *late* — after `mark_terminal` — so the one shot usually lands
clean and the test passes in ~1-2s, which matches this session's own clean
baseline runs exactly. Under CPU contention, that ordering is no longer
reliable, and the one-shot task can run *before* `mark_terminal`, see the
blocker still occupying the slot, defer again with a fresh 5s backoff — and
then there is **no fallback that can ever work**, because the scanner that
exists specifically to retry a deferred row can never resolve a pool at
all. The test then burns out its full 10s deadline deterministically.

**Correction (post-review, Codex on this PR).** An earlier draft of this
paragraph claimed this also explains the *other* panic site PR #1697 hit
(the already-`30s` first wait, `Elapsed(())` at
`wait_for_execution_state_with_timeout`, watching the SOURCE reach
`COMPLETED`). Wrong, and worth retracting explicitly rather than leaving it
to mislead the next reader: `evaluate_triggers_for_execution`'s
`QuotaExceeded` arm inserts the outbox row and `continue`s — it never
returns an `Err` that could roll back or stall the source's own terminal
transaction, and its own comment says so directly ("deferring the start to
the outbox for retry rather than blocking the source execution's own
completion"). The source's transition to `COMPLETED` does not read the
target's quota state, the outbox table, or `sharded_pool` at all, so a
fix to the scanner's pool resolution cannot affect how long that wait
takes. **That panic site was not fixed by this session's change** -- it
needed a different fix, found later on this same branch. **Update (Codex
review, a later PR on this branch): it is no longer unexplained.** #1713
(cherry-picked here as `8292b07`) found it: a missing migration in
`integration_e2e.rs`'s testcontainers schema meant `harvest_completion_trigger_fires`
was missing columns `completion_trigger.rs` writes inside the source's own
completion transaction, so that insert's failure rolled the whole
transaction back on every Docker-backed attempt -- not a worker
decision-cycle timing issue at all. See `8292b07`'s commit message and
#1713 for the full writeup.

**A live occurrence landed on this PR's own CI while this correction was
being written** (`Test DB (linux, shard 0)`, commit `4ce17a5`, run
`35781489366`): the exact panic site predicted above,
`integration_e2e.rs:1383:6` from `quota_enforcement_tests.rs:3786`, the
SOURCE-completion wait — not the mechanism this session's fix addresses.
Every other test in the module passed (46/46) on that run.

Tried to reproduce it locally with the same harness used for the fixed
flake, to see if it is the same "single-VM CPU oversubscription" story:
**it is not, at least not at the stress levels this session could apply.**
16x oversubscription (64 `yes` loops), 15 runs — 0 failures. Doubled to 32x
(128 loops), 15 more runs — 0 failures, 30/30 total. The fixed flake
reproduced reliably at 8x; the scanner-driven retry path (the ~6s runs)
stayed comfortably bounded even at 32x. This is a genuine negative result,
not an absence of trying: whatever makes a single decision cycle exceed 30s
on a real GitHub Actions runner is not reproduced by same-VM CPU
oversubscription of the kind that reliably broke the other panic site.
Candidates this narrows toward, none confirmed: real hypervisor-level
steal time (categorically different from same-kernel oversubscription — a
stolen vCPU cannot run anything on the VM, including a local busy-loop
stress harness, so this session's method cannot simulate it), I/O-bound
latency (Postgres round-trip tail latency under real disk/network
conditions this sandbox's local Postgres does not reproduce), or a genuine
retry/correctness issue in the decision-cycle path (e.g., poison-pill
strikes accumulating, or a transient DB error triggering more than one
attempt) that would need worker-level tracing captured from a real CI run,
not a local one, to catch.

## 🔧 Treatment

One-line-of-intent fix, in `completion_trigger_defers_to_outbox_when_target_quota_exceeded`
only: give the worker's own `WorkerRuntimeConfig` the SAME `ShardedDbPool`
the test already constructs (and already relies on for the one-shot path),
instead of leaving it `None`.

```rust
let sharded_pool = autumn_harvest::shard::ShardedDbPool::single(build_test_pool(&url));
// ...
let mut worker_cfg = runtime_config("w-946-trigger-quota", 2, 1, std::time::Duration::from_secs(10));
worker_cfg.sharded_pool = Some(sharded_pool);
let worker = Arc::new(Worker::new(worker_cfg, reg).expect("worker should build"));
```

No timeout constant changed. No `ci.yml` change. No sharding-manifest
change.

## 📊 Measurement: before/after, same harness, same stress level

| | Before (`trunk-dev` HEAD) | After (this fix) |
|---|---|---|
| Clean, single run, no stress | pass, ~1.1-1.7s | pass, ~1.1-1.7s |
| 8x CPU-oversubscription stress, 20 runs | **5 failures** (panic at the outbox-retry-loop assertion, ~11-12s each) | 0 failures |
| Same stress, 30 more runs (extended confirmation) | not re-run (already had a clean fail rate estimate) | **0 failures** (70/70 total across both batches) |
| Scanner-driven retries under stress (the runs that do not win the one-shot race) | never completed within the 10s deadline | complete in ~6.0-6.5s, comfortably inside the ORIGINAL, unchanged 10s bound |

The margin in the last row is the actual signal that this is a mechanism
fix, not a masked timing coincidence: the scanner now succeeds well inside
the bound PR #1673/#1697 were widening *around* the symptom to accommodate,
using the *same* stress level that reliably broke it before.

Also re-ran, unmodified: `cargo clippy -p autumn-harvest --all-features
--tests -- -D warnings` (clean), `cargo fmt --check` (clean after one
formatting pass), `python3 docs/audits/comment-hygiene.py --base
origin/trunk-dev` (clean once committed — the script scopes to committed
diff against the merge base, not the working tree, so this only resolves
after commit, not before; re-checked post-commit).

## 🧭 What this means for PR #1697's own widen

PR #1697's `10s → 30s` widen of the SAME outbox-retry loop is a reasonable
defensive margin in isolation, and its own root-cause note (an asymmetry
between the two waits, one already widened and one not) was correct as far
as it went. But it was chasing a symptom of the mechanism above, not the
mechanism itself — no amount of widening that specific bound helps once the
one-shot race is lost, since the scanner it is waiting on can never
succeed regardless of how long it waits. Once this report's fix lands on
`trunk-dev`, that widen is very likely unnecessary (this session's own
measurement: ~6.5s under the same stress that used to exceed even the
original 10s bound, a comfortable margin). Not reverting or otherwise
touching PR #1697 from this session — different branch, different
author-of-record — but flagging this for whoever reviews it next, so the
widen is not miscredited as the actual fix.

## ⚠️ A second, separate flake in the same file — status corrected below

**Correction (Codex review, a later PR on this same branch): "the way real
CI does it" below is wrong, and it matters.** This section's reproduction
used `HARVEST_TEST_DATABASE_URL` pointing at ONE persistent local database
for the whole module run, so every test in the module really did share one
database and could leak rows into a later test. Real CI does not run this
way: `setup_test_database_url_or_env()` starts a **fresh testcontainer per
test function** whenever `HARVEST_TEST_DATABASE_URL` is unset (CI's actual
mode — `.github/workflows/ci.yml` never sets it), regardless of
`--test-threads`. So real CI never shares one database across the module,
and the row-leakage hypothesis below cannot be the CI-observed mechanism —
it is, at most, an artifact specific to this local persistent-database
reproduction style, not a CI-shaped flake. Confirmed independently: a later
PR on this branch (#1713, cherry-picked here as `8292b07`) ran this exact
full module via testcontainers — every test its own fresh database — and
got **47/47 passing**, including
`quota_blocked_outbox_retry_row_is_not_starved_by_a_flood_of_fresh_rows`,
the specific test this section reported failing deterministically. That
fix (a missing migration in `integration_e2e.rs`'s test-schema bundle) is
unrelated to the row-leakage hypothesis below, so its passing does not
confirm or refute that hypothesis either way -- it only confirms this
module does not fail under CI's real per-test-isolated-container model.

The original text is kept below for the record, with this correction
governing how to read it:

Running the **whole** `quota_enforcement_tests` module against a shared
persistent database (`cargo test -p autumn-harvest --test integration --
quota_enforcement_tests --test-threads=1` with `HARVEST_TEST_DATABASE_URL`
set, all ~58 tests, one shared database) fails **deterministically** — 3 of
3 local reproductions, **zero CPU stress needed** — on
`quota_blocked_outbox_retry_row_is_not_starved_by_a_flood_of_fresh_rows`
(and, in one of the three runs, two additional `quota_blocked_outbox_*`
tests failed instead/also). **Confirmed present on unmodified `trunk-dev`
HEAD** (verified by stashing this session's fix and re-running the same
full-module command 3x before restoring it) — this is not caused by, or
related to, the fix above, and (per the correction above) not established
to reproduce under CI's actual per-test-container model either.

Not root-caused to this report's own confidence bar (that would need its
own instrumented investigation), but the shape is suspicious and worth a
named candidate for the next session investigating THIS specific
reproduction mode: several `quota_blocked_outbox_*`
tests in this same file (`quota_blocked_outbox_never_attempted_rows_outrank_expired_quota_retries`
is one concrete example) insert dozens of rows directly into
`harvest_completion_trigger_outbox` targeting shard 0, backed by a quota
blocker that is deliberately **never freed** for the rest of the test — and
never delete those rows before the test function returns. Under
`--test-threads=1` with a shared persistent database, every test in the
module shares one process and one database for the module's whole run. A
later, alphabetically-sorted test in the same module
(`completion_trigger_defers_to_outbox_when_target_quota_exceeded` sorts
BEFORE all of these by alphabetical test order — confirmed via `cargo test
--list`, so it is not itself exposed to this — but several
`quota_blocked_outbox_*` tests sort near each other and could plausibly
crowd one another's `OUTBOX_CLAIM_BATCH_LIMIT`/`OUTBOX_RETRY_RESERVED_SLOTS`
budgets) is a concrete, testable hypothesis for the next session, not a
confirmed mechanism yet.

## 🔬 Reproduce

```sh
# Local Postgres 16, fresh migrated test database:
pg_ctlcluster 16 main start
sudo -u postgres psql -c "CREATE USER harvest WITH SUPERUSER PASSWORD 'harvest';"
sudo -u postgres psql -c "CREATE DATABASE harvest_test OWNER harvest;"
cargo build -p autumn-harvest --lib   # generates target/debug/build/autumn-harvest-*/out/all_migrations_bundle.sql
PGPASSWORD=harvest psql -U harvest -h 127.0.0.1 -d harvest_test \
  -f target/debug/build/autumn-harvest-*/out/all_migrations_bundle.sql
export HARVEST_TEST_DATABASE_URL="postgres://harvest:harvest@127.0.0.1:5432/harvest_test"

# CPU oversubscription (8x on a 4-core box). Capture the PIDs -- the
# no-stress section below needs these actually stopped, not just
# outlived, or its own "no stress needed" claim does not hold.
STRESS_PIDS=()
for i in $(seq 1 32); do yes > /dev/null & STRESS_PIDS+=("$!"); done

# Reproduce the pre-fix flake (run against trunk-dev HEAD, before this
# session's fix, to confirm the failure rate below still holds):
for i in $(seq 1 20); do
  cargo test -p autumn-harvest --test integration -- \
    completion_trigger_defers_to_outbox_when_target_quota_exceeded \
    --test-threads=1
done
# -> ~5/20 failures, "target row was never created by the outbox retry;
#    last count was 1", ~11-12s each.

# Confirm the fix (after this session's change):
for i in $(seq 1 40); do
  cargo test -p autumn-harvest --test integration -- \
    completion_trigger_defers_to_outbox_when_target_quota_exceeded \
    --test-threads=1
done
# -> 0/40 failures (this session's own run; a second 30-run batch also
#    0/30, 70/70 total).

# Stop the CPU burners before anything claiming "no stress needed" --
# otherwise they are still running underneath it and that claim is false.
kill "${STRESS_PIDS[@]}" 2>/dev/null

# The unrelated, pre-existing full-module flake (no stress needed):
for i in 1 2 3; do
  cargo test -p autumn-harvest --test integration -- \
    quota_enforcement_tests --test-threads=1
done
# -> fails on quota_blocked_outbox_retry_row_is_not_starved_by_a_flood_of_fresh_rows
#    (and, once, two more quota_blocked_outbox_* tests) on EVERY run, with
#    or without this session's fix applied -- confirmed pre-existing.

# Confirm test execution order (alphabetical, not declaration order):
cargo test -p autumn-harvest --test integration -- quota_enforcement_tests --test-threads=1 --list
```
