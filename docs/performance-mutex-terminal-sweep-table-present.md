# The terminal-sweep `table_present` triple-check — real, and not worth fixing

🎯 **Workload.** `WorkflowContext::mutex` (issue #691, `autumn-harvest/src/mutex.rs`)
is guarded end to end by `table_present()` — an uncached
`SELECT to_regclass('harvest_mutex_locks') IS NOT NULL` — so that a shard that
has not yet applied the mutex migration during a staggered rollout no-ops
instead of erroring. `sweep_terminal_holder_and_wake` is the single
per-terminal-transition chokepoint (`completion_trigger.rs`, `reset.rs`,
`worker.rs`): it runs inside the terminal-seal transaction of **every**
workflow/activity execution that finishes anywhere in the engine — complete,
fail, cancel, terminate, timeout, poison-pill — whether or not that execution
ever touched a mutex.

That single function calls `table_present()` three times in the same
transaction, all evaluating to the same answer:

```
sweep_terminal_holder_and_wake            table_present()   #1  (mutex.rs:817)
  -> release_all_locks_for_holder(..)     table_present()   #2  (mutex.rs:654)
  -> delete_waiters_for_holder(..)        table_present()   #3  (mutex.rs:705)
```

`release_all_locks_for_holder` and `delete_waiters_for_holder` are `pub` and
re-exported (mutex.rs:849-850), but a workspace-wide grep found **no call
site anywhere other than `sweep_terminal_holder_and_wake`** — so today this
triple-check is pure internal redundancy, not defensive coding for a real
second caller.

📈 **Profile.** `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` of the exact
statement `table_present()` issues, against a freshly-migrated fixture (all
112 `autumn-harvest/migrations/*/up.sql` applied; see
`docs/perf-artifacts/mutex-terminal-sweep-table-present/fixture-summary.txt`):

```
Result  (actual rows=1 loops=1)
  Buffers: shared hit=6
```

`docs/perf-artifacts/mutex-terminal-sweep-table-present/table_present.explain.txt`
has the full output. A `pg_stat_statements` snapshot of three back-to-back
executions (matching the current per-sweep call count) confirms the linear
count and cost (`pg_stat_statements.txt`):

| calls | shared_blks_hit | shared_blks_read |
|--:|--:|--:|
| 3 | 18 | 0 |

So each call costs 6 buffers, all cache hits — a `pg_class`/relcache lookup,
not a heap or index access on `harvest_mutex_locks` itself.

**Where this sits in the terminal-transition workload.** This page does not
have a full-transaction buffer count for `evaluate_triggers_for_execution` to
divide by, but an existing sub-piece of that same transaction is already
measured: `docs/perf-artifacts/completion-trigger-outbox-queue/*.txt` shows a
single completion-trigger batch of 50 items touching 150-285 buffers on its
own — before the workflow-execution row update, the event append, and
whatever else the terminal-seal transaction does. Three redundant 6-buffer
catalog lookups are nowhere close to 5% of that, let alone of the full
transaction.

🧭 **Plan.** No plan-shape question here — `to_regclass` is a catalog
function, not a table scan; the "plan" is `Result` with no scan node either
way.

💡 **Hypothesis.** Collapsing the three calls into one (check `table_present`
once in `sweep_terminal_holder_and_wake`, then have it invoke unguarded
private cores for the two sub-steps, leaving the public
`release_all_locks_for_holder` / `delete_waiters_for_holder` entry points and
their own guards untouched for any future external caller) would cut this
statement's buffers-per-terminal-transition from 18 to 6 (-67%) and its call
count from 3 to 1.

🔧 **Change.** **Not made.** Prototyped locally (private `_unchecked` core +
guarded public wrappers, matching the shape `reclaim_expired_leases_and_wake`
already uses for its own combined-statement optimization at mutex.rs:759-764)
and reverted after measurement, per the process below.

📊 **Measurement — why this doesn't clear the floor.**

| | before | after (prototype) | Δ |
|:--|--:|--:|--:|
| calls (this statement) | 3 / terminal transition | 1 / terminal transition | -67% |
| buffers (this statement) | 18 | 6 | -12 buffers |
| buffers (this statement) as % of total workload buffers | ≪5% | — | — |

This is real, reproducible waste — but it fails the page's own gate on both
available floor items:

- **Buffer floor:** the floor requires ≥20% reduction *on a statement that is
  ≥5% of the workload's buffers*. At 6 buffers/call this statement cannot be
  5% of any transaction that also writes `harvest_workflow_executions`,
  `harvest_events`, and the completion-trigger/outbox rows the linked
  artifact above shows costing 150-285 buffers on its own.
- **N+1 floor:** the floor's N+1 bullet is "statement count per request drops
  from O(n) to O(1)" — i.e. a count that *grows* with batch/row size. This
  redundancy is a fixed 3 → 1 regardless of workload size, not an
  asymptotic elimination, so it doesn't fit that bullet either, even though
  it is the same "statement count per unit of work" evidence category the
  N+1 bullet draws on.

No other floor item (rows read, spills, plan shape, WAL, locks) applies to a
catalog lookup.

✅ **Equivalence.** N/A — no code changed.

💸 **Write cost.** N/A — no index added.

**Verdict: do not fix, as a Ledger-gated change.** The waste is real and the
fix is small and safe (it doesn't touch the "no cross-call caching" rule this
file documents at mutex.rs:474-477 — that rule is about caching an answer
*across* separate calls/transactions under a staggered migration, not about
three re-checks inside one already-open transaction; collapsing them cannot
observe a mid-transaction schema change any differently than the current
code already risks by checking three times under `READ COMMITTED`). But at
6 buffers/call, "the review attention and permanent maintenance cost" this
page's own rules weigh against a habit of shipping sub-floor changes is not
worth it *for buffer/row reasons*. The one argument this page's own evidence
rules cannot certify is latency: three sequential synchronous round trips
inside the terminal-seal transaction, on literally the highest-frequency
transaction boundary in the engine, serialized rather than parallelizable.
Wall-clock is inadmissible here by this page's own rules, so that argument is
named but not scored — it is recorded for a human to weigh against the
near-zero buffer cost, not decided by this page.

🔬 **Reproduce.**

```sh
PGPASSWORD=postgres psql -h localhost -U postgres -c \
  "CREATE DATABASE harvest_ledger_probe;"
cd autumn-harvest/migrations
for d in $(ls -d */ | sort); do
  [ -f "${d}up.sql" ] && PGPASSWORD=postgres psql -h localhost -U postgres \
    -d harvest_ledger_probe -v ON_ERROR_STOP=1 -f "${d}up.sql"
done
PGPASSWORD=postgres psql -h localhost -U postgres -d harvest_ledger_probe \
  -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
PGPASSWORD=postgres psql -h localhost -U postgres -d harvest_ledger_probe \
  -c "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) SELECT to_regclass('harvest_mutex_locks') IS NOT NULL AS present;"
PGPASSWORD=postgres psql -h localhost -U postgres -d harvest_ledger_probe \
  -c "SELECT pg_stat_statements_reset();"
for i in 1 2 3; do
  PGPASSWORD=postgres psql -h localhost -U postgres -d harvest_ledger_probe \
    -c "SELECT to_regclass('harvest_mutex_locks') IS NOT NULL AS present;" > /dev/null
done
PGPASSWORD=postgres psql -h localhost -U postgres -d harvest_ledger_probe \
  -c "SELECT query, calls, shared_blks_hit, shared_blks_read FROM pg_stat_statements WHERE query ILIKE '%to_regclass%';"
```
