# The terminal-sweep `table_present` triple-check — real calls, ~zero buffers

> **Corrected after review, twice.** The first version of this page
> mismeasured the buffer cost (three separate `psql` processes, each a cold
> connection, when production issues all three calls on one already-warm
> pooled connection) and mis-scoped the workload (claimed "every finished
> workflow/activity execution"; activities never reach this code). A second
> review round caught a narrower scope gap in that same fix: a workflow
> that **fails and is successfully retried** also skips this sweep on the
> failing attempt. All three are fixed below. The corrected numbers make the
> no-fix verdict *stronger*, not weaker: the real buffer cost of the
> redundancy this page found is zero in the deployment shape that matters,
> not merely under 5%.

🎯 **Workload.** `WorkflowContext::mutex` (issue #691, `autumn-harvest/src/mutex.rs`)
is guarded end to end by `table_present()` — an uncached
`SELECT to_regclass('harvest_mutex_locks') IS NOT NULL` — so that a shard that
has not yet applied the mutex migration during a staggered rollout no-ops
instead of erroring. `sweep_terminal_holder_and_wake` runs on workflow
terminal transitions that reach one of three call sites, all workflow-scoped
— not every execution, and not activities, which never call it (the trigger
evaluator below loads its row from `harvest_workflow_executions`, which an
activity has none of):

* `evaluate_triggers_for_execution_collecting_with_codecs`
  (`completion_trigger.rs:1539`) — fires on complete/cancel/terminate/
  timeout/poison-pill, and on fail **only when no workflow-level retry is
  committed**. `persist_workflow_failure` (`worker.rs:8294-8440`) marks the
  failing execution `FAILED` first, then starts the retry attempt; only once
  that succeeds does it set `retry_committed = true` and gate the evaluator
  call behind `if !retry_committed`. So a `FAILED` predecessor whose retry
  was successfully scheduled reaches a terminal *state* without reaching
  this sweep at all. This page originally guessed that a later attempt's own
  sweep would eventually release the lock; it doesn't. The retry runs under
  a **new** `ExecutionId` (`rid`), `harvest_mutex_locks.holder_exec_id`
  matches against the exact id passed to the sweep, and nothing transfers
  `holder_exec_id` from the predecessor to `rid` (no write site does). The
  predecessor's lock, if it held one, sits until `reclaim_expired_leases_
  and_wake`'s lease-expiry sweep reclaims it — not until any later attempt's
  own terminal sweep. That is a real mutex-semantics question (a lock a
  failed-and-retried workflow held is not released until its lease expires,
  not at the retry decision), out of scope to fix from a Ledger performance
  pass — noted here only because this page's own table_present() accounting
  must not misstate what the surrounding function actually does.
* continue-as-new sealing (`worker.rs:18719`)
* workflow reset (`reset.rs:1335`)

Still a high-frequency chokepoint — most workflow terminal transitions,
including every plain success/cancel/terminate/timeout/poison-pill and every
non-retried failure — just narrower than "every workflow terminal
transition" (the first correction's wording) or the original "every
finished workflow/activity execution."

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

📈 **Profile.** Three probes, all against a freshly-migrated fixture (all
112 `autumn-harvest/migrations/*/up.sql` applied; see
`docs/perf-artifacts/mutex-terminal-sweep-table-present/fixture-summary.txt`).

**Probe 1 — cold connection, single call**
(`cold-connection-first-call.explain.txt`): `shared hit=6`. This is
Postgres's catalog/syscache resolving `harvest_mutex_locks` by name for the
first time on that backend (`RELNAMENSP` syscache lookup) — a `pg_class`
lookup, not a heap/index access on the table itself.

**Probe 2 — the methodology the first version of this page used, and why it
was wrong** (`pg_stat_statements.INVALID-separate-connections.txt`): three
separate `psql` invocations, each opening its own connection/backend,
report `calls=3, shared_blks_hit=18` — i.e. `6` **every** time, because each
process starts with a cold syscache. Codex's review correctly flagged this
as not representative: `sweep_terminal_holder_and_wake`'s three calls run on
one already-open `AsyncPgConnection`, not three fresh ones. Kept in the
artifact directory as a labeled negative example, not as evidence for
anything in this page's conclusion.

**Probe 3 — one connection, three calls in the same transaction**
(`same-connection-same-transaction.explain.txt`, matching production
exactly): `Buffers: shared hit=6` on call 1, **no `Buffers:` line at all**
(zero shared-buffer touches) on calls 2 and 3 — the backend's syscache
already has the answer, so Postgres never consults the shared buffer pool
again. `pg_stat_statements.same-connection.txt` confirms it in aggregate:
`calls=3, shared_blks_hit=6` for the whole three-call sweep, not 18.

**Probe 4 — one connection, three separate transactions**
(`same-connection-new-transactions.explain.txt`): same result, `6, 0, 0` —
the cache entry backing this lookup is a backend-local syscache entry, not
transaction-scoped, so it survives across `COMMIT`. It is **not** the same
thing as a guarantee for "the connection's lifetime": `to_regclass`
resolves through Postgres's catalog/syscache, and a syscache entry is
invalidated by relevant DDL against that relation (or a broader
invalidation event), not merely by time or transaction boundaries. This
probe only demonstrates reuse across three adjacent transactions with no
intervening DDL — it does not show, and this page does not claim, that
*only* the very first sweep a pooled connection ever handles pays the cost
for that connection's entire remaining life. The narrower, supported
reading: within any span with no DDL against `harvest_mutex_locks` (which
in practice is effectively the whole life of a connection that predates and
outlives a migration, since nothing else touches that table's schema),
`table_present()` after the first call in that span costs 0 buffers,
regardless of whether it's called once or three times per sweep.

🧭 **Plan.** No plan-shape question — `to_regclass` is a catalog function
with no scan node, cold or warm.

💡 **Hypothesis (revised).** The original hypothesis — collapsing the three
calls to one saves buffers — is **refuted** by probes 3 and 4: after the
first call on a given connection, the second and third already cost zero
shared buffers, so there is nothing left to save. What collapsing three
calls to one *does* still do is drop the statement-count contribution from
3 to 1 per sweep (still true — `pg_stat_statements` reports `calls=3` either
way in probes 2-4) and remove two round trips' worth of planner/executor
overhead not visible to buffer accounting (parse, plan, dispatch) — real,
but the kind of cost this page's own rules classify as wall-clock and
therefore inadmissible without a >2x corroborating buffer or row change,
which probes 3-4 show does not exist here.

🔧 **Change.** None shipped, for a stronger reason than the original
version of this page gave: not merely sub-floor, but a measured buffer
delta of **zero** in the deployment shape (pooled, reused connections) that
matters.

📊 **Measurement**

| | separate connections (invalid) | same connection, same tx | same connection, new tx |
|:--|--:|--:|--:|
| calls | 3 | 3 | 3 |
| shared_blks_hit (call 1 / 2 / 3) | 6 / 6 / 6 | 6 / 0 / 0 | 6 / 0 / 0 |
| **total shared_blks_hit** | **18** | **6** | **6** |

Collapsing 3 calls to 1 on an already-warm connection: **6 → 6, Δ = 0
buffers.** On a cold connection's very first sweep (or the first sweep after
any DDL invalidates the syscache entry): 6 → 6 either way (a single call
still has to resolve it once). There is no buffer regime in which the fix
saves anything.

Neither floor item available to a catalog lookup is cleared, and now for an
even more direct reason than "the statement is under 5% of the workload":

- **Buffer floor** (≥20% reduction on a statement ≥5% of workload buffers):
  the measured reduction is **0%**, not merely under the 5%-of-workload
  bar.
- **N+1 floor** (statement count drops O(n) → O(1)): the redundancy is a
  fixed 3 → 1 regardless of workload size, not an asymptotic elimination.

✅ **Equivalence.** N/A — no code changed.

💸 **Write cost.** N/A — no index added.

**Verdict: do not fix.** The three calls are real and `pg_stat_statements`
will keep reporting `calls=3` at this chokepoint until someone changes it,
but the buffer cost of removing two of them is not "small" — it is
measured zero, because Postgres's own backend-local syscache already
eliminates the repeat work `table_present()`'s redundancy appears to cause.
The only remaining argument is round-trip latency (three sequential
synchronous awaits on the hottest transaction boundary in the engine
instead of one), which this page's own rules name but cannot score, since
wall-clock is inadmissible without a corroborating buffer/row change this
investigation now shows does not exist.

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

# Probe 3: one connection, three calls in one transaction (matches production).
cat <<'SQL' | PGPASSWORD=postgres psql -h localhost -U postgres -d harvest_ledger_probe
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) SELECT to_regclass('harvest_mutex_locks') IS NOT NULL AS present;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) SELECT to_regclass('harvest_mutex_locks') IS NOT NULL AS present;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) SELECT to_regclass('harvest_mutex_locks') IS NOT NULL AS present;
COMMIT;
SQL

# Probe 4: one connection, three separate transactions.
cat <<'SQL' | PGPASSWORD=postgres psql -h localhost -U postgres -d harvest_ledger_probe
BEGIN; EXPLAIN (ANALYZE, BUFFERS) SELECT to_regclass('harvest_mutex_locks') IS NOT NULL AS present; COMMIT;
BEGIN; EXPLAIN (ANALYZE, BUFFERS) SELECT to_regclass('harvest_mutex_locks') IS NOT NULL AS present; COMMIT;
BEGIN; EXPLAIN (ANALYZE, BUFFERS) SELECT to_regclass('harvest_mutex_locks') IS NOT NULL AS present; COMMIT;
SQL
```
