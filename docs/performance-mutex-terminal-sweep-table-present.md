# The terminal-sweep `table_present` triple-check — real calls, ~zero buffers

> **Corrected after review.** The first version of this page mismeasured the
> buffer cost (three separate `psql` processes, each a cold connection, when
> production issues all three calls on one already-warm pooled connection)
> and mis-scoped the workload (claimed "every finished workflow/activity
> execution"; activities never reach this code). Both are fixed below. The
> corrected numbers make the no-fix verdict *stronger*, not weaker: the real
> buffer cost of the redundancy this page found is zero in the deployment
> shape that matters, not merely under 5%.

🎯 **Workload.** `WorkflowContext::mutex` (issue #691, `autumn-harvest/src/mutex.rs`)
is guarded end to end by `table_present()` — an uncached
`SELECT to_regclass('harvest_mutex_locks') IS NOT NULL` — so that a shard that
has not yet applied the mutex migration during a staggered rollout no-ops
instead of erroring. `sweep_terminal_holder_and_wake` runs inside the
terminal-seal transaction of every **workflow** terminal transition — not
every execution, and not activities, which never call it. Its three call
sites are all workflow-scoped: `evaluate_triggers_for_execution_collecting_with_codecs`
(`completion_trigger.rs:1539`, the terminal-trigger evaluator that fires on
complete/fail/cancel/terminate/timeout/poison-pill — it loads the row from
`harvest_workflow_executions`, so an activity, which has no row in that
table, cannot reach it), continue-as-new sealing (`worker.rs:18719`), and
workflow reset (`reset.rs:1335`). Still a very high-frequency chokepoint —
every workflow that ever finishes, regardless of whether it touched a
mutex — just not the even-higher-frequency "every execution" this page
originally claimed.

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
(`cold-connection-first-call.explain.txt`): `shared hit=6`. This is the
Postgres relation-cache (`relcache`) resolving `harvest_mutex_locks` for the
first time on that backend — a `pg_class` lookup, not a heap/index access on
the table itself.

**Probe 2 — the methodology the first version of this page used, and why it
was wrong** (`pg_stat_statements.INVALID-separate-connections.txt`): three
separate `psql` invocations, each opening its own connection/backend,
report `calls=3, shared_blks_hit=18` — i.e. `6` **every** time, because each
process starts with a cold relcache. Codex's review correctly flagged this
as not representative: `sweep_terminal_holder_and_wake`'s three calls run on
one already-open `AsyncPgConnection`, not three fresh ones. Kept in the
artifact directory as a labeled negative example, not as evidence for
anything in this page's conclusion.

**Probe 3 — one connection, three calls in the same transaction**
(`same-connection-same-transaction.explain.txt`, matching production
exactly): `Buffers: shared hit=6` on call 1, **no `Buffers:` line at all**
(zero shared-buffer touches) on calls 2 and 3 — the backend's relcache
already has the answer, so Postgres never consults the shared buffer pool
again. `pg_stat_statements.same-connection.txt` confirms it in aggregate:
`calls=3, shared_blks_hit=6` for the whole three-call sweep, not 18.

**Probe 4 — one connection, three separate transactions**
(`same-connection-new-transactions.explain.txt`): same result, `6, 0, 0` —
the relcache entry is backend-scoped, not transaction-scoped, so it survives
across `COMMIT`. In a pooled-connection deployment, this means only the
*very first* terminal-transition sweep a given pooled connection ever
handles pays the 6-buffer cost, once, for the lifetime of that connection —
regardless of whether `table_present()` is called once or three times per
sweep, and regardless of how many thousands of terminal transitions that
connection goes on to seal afterward.

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
buffers.** On a cold connection's very first sweep: 6 → 6 either way (a
single call still has to resolve the relcache once). There is no buffer
regime in which the fix saves anything.

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
measured zero, because Postgres's own backend-local relcache already
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
