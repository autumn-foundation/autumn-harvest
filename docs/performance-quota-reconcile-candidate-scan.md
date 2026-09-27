# Ledger: `quota_reconcile` candidate-scan cost under mixed-deployment skew

Findings, not a fix. Issue #1226 follow-up, closing the open question
migration `20260910192721_harvest_quota_reconcile_candidate_index`'s own
comment raises, with a root cause more specific than the migration
guessed. No code, index, or query change ships in this PR — see
"Why no fix ships" below.

**Correction history** (all from `Codex Review` on
[PR #1691](https://github.com/autumn-foundation/autumn-harvest/pull/1691),
before merge — read the review thread for the full exchange):

1. The original pass reported a negative result ("no code change is
   warranted"), from a fixture that seeded no terminal history for the
   quota'd workflow type itself. That made an unrelated index look
   artificially cheap. Re-measured with realistic terminal history; the
   conclusion reverses (below).
2. The first correction claimed `= ANY($1)` structurally cannot use a
   leading-column index for ordered access. That overstated it: the
   index CAN deliver a cheap plan, confirmed by forcing the planner to
   use it. The real mechanism is a cost-estimation problem, not a hard
   limitation — see "Root cause," below, which replaces that claim.
3. That correction's own three-way comparison table mixed a cold
   first-tick timing (from the plan-table capture above) with warm,
   back-to-back timings (from the three-variant capture). The buffer
   counts were already apples-to-apples; the timings were not.
4. The attempted fix for (3) was itself incomplete: the three variants
   in that capture still run sequentially on one connection, so later
   variants reuse pages earlier ones already pulled into cache -- the
   timing comparison stayed uncontrolled no matter which numbers were
   quoted. The exec-time comparison is dropped entirely; buffers alone
   carry the claim (see "Root cause," below, for why that is sufficient
   under this agent's own evidence rules). The plan table's own
   first-tick timings were also out of sync with the committed
   artifacts (stale numbers from an earlier run) and are now refreshed
   to match exactly.
5. The "why no fix ships" section wrongly argued that a per-registered-
   name rewrite would generate unbounded distinct SQL text, defeating
   the prepared-statement cache. It would not: one static parameterized
   query, executed once per name, keeps one query shape regardless of
   registry size -- corrected below.

Also fixed: the fixture originally used `gen_random_uuid()`, so exact
buffer counts jittered a few percent run to run. Row ids are now
deterministic (`md5()` of a row-unique seed, cast to `uuid`), confirmed
by running the full capture twice and diffing: `Rows Removed by Filter`
and the full-pass `pg_stat_statements` total (503,105 buffers) matched
bit-for-bit both times; only the cache-dependent hit/read split moved a
little, exactly the "total is stable, the split is not" property this
agent's own admissible-evidence rules already name.

## 🎯 Workload

`reconcile_quota_keys_from` runs on `worker_heartbeat_interval` cadence,
once per assigned shard, on every worker process. Reproduce:
`HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh`.

## 📈 Fixture

Seeded, deterministic, production-shaped:

- One quota'd workflow type (`billing_saga`): 1,000 non-terminal rows
  with `quota_key IS NULL` (a post-rollout backfill population), plus a
  fixed 50,000-row terminal (`COMPLETED`) history — a realistic
  accumulated history for a workflow type important enough to have a
  declared `QuotaPolicy`.
- A large population of 50 distinct NON-quota'd workflow types, swept
  across three sizes: 20,000 / 100,000 / 500,000 non-terminal rows, each
  with a 10%-of-noise terminal population too.
- `batch_size` = `QUOTA_RECONCILE_DEFAULT_BATCH` (200) throughout.
- Row ids: `md5('qr-<namespace>-' || i)::uuid`, not `gen_random_uuid()`.

## 🧭 Plan — first tick at each noise size

| noise rows | plan chosen | buffers | rows removed by filter |
|---:|---|---:|---:|
| 20,000 | Index Scan on `idx_harvest_we_quota_reconcile_candidates`, residual filter | 3,870 (all hit) | 3,657 |
| 100,000 | same | 18,970 (all hit) | 18,682 |
| 500,000 | same | 95,091 (hit+read) | 94,467 |

Buffers and rows-removed match the committed `noise-*.explain.txt` files
exactly and reproduce bit-for-bit run to run (fixture ids are
deterministic). Execution time is deliberately not tabulated here: every
re-run of this capture (this file was captured six times over the course
of this investigation, for reasons unrelated to this table) regenerates
`noise-*.explain.txt` with a fresh, slightly different `Execution Time:`,
and a table claiming exact millisecond figures needs updating on every
such re-run to stay honest -- read the committed `noise-*.explain.txt`
files directly for the timing that produced them, or the "Root cause"
section below, whose measurements are all buffer-only for the same
reason.

Unlike the first (incorrect) pass, `idx_harvest_wfx_workflow_identity`
is **never** picked here at any size. That index is not partial by
state — it covers `billing_saga`'s full history, terminal rows
included. Once that history is realistic (50,000 rows), scanning it
costs as much as scanning the id-ordered candidate index, so the
planner has no incentive to switch. Cost scales with the non-quota'd
population as the migration comment originally warned, reaching a
genuinely expensive ~150ms / ~95,000-buffer single batch at 500,000
noise rows — with real disk reads (`read` in the thousands), not just
cache churn.

Full plans: `docs/perf-artifacts/quota-reconcile-candidate-scan/noise-*.explain.txt`.

## 🔬 Root cause: cost misestimation, not a hard limitation

The migration comment proposes and rejects a `(workflow_name, id)`
partial index, reasoning it could not serve `ORDER BY id LIMIT $3`
without an extra sort. Built and tested directly against the 500,000
fixture, with `CANDIDATE_SQL` unmodified: the unforced planner does
**not** pick it up — same ~95,000-buffer cost as the id-only index
(`alternative-index-explain.txt`).

The first correction to this document stopped there and concluded the
index was structurally unusable under `= ANY($1)`. Review pushed back,
correctly: a `ScalarArrayOpExpr` CAN still be an index condition
followed by a sort, and the row estimate for the alternative plan
(`rows=38,875`) is wildly off from the actual matching rows (1,000) — a
38x overestimate, caused by `workflow_name` and the partial predicate
(`quota_key IS NULL AND state IN (...)`) being correlated in a way
ordinary column statistics do not capture. That is worth testing
directly rather than reasoning about, so three variants were captured
against the identical fixture:

| variant | mechanism | buffers |
|---|---|---:|
| unmodified `CANDIDATE_SQL` (`= ANY($1)`), nothing forced | what production runs | 95,091 |
| same query, `enable_indexscan = off` for one transaction | forces the planner onto the alternative index via Bitmap Index Scan + Sort | 46 |
| same predicate rewritten to literal `workflow_name = $1` | plain equality lets Postgres serve `ORDER BY id` directly from the index, no sort | 189 |

Buffers only. `explain_with_alternative_index` runs these three
variants sequentially on one connection, so each later variant can
reuse pages the earlier ones already pulled into shared_buffers. That
makes any timing comparison between them uncontrolled -- a real finding
from review, not addressed by reordering or repeating the capture,
since the effect is inherent to running multiple variants on one warm
connection. Buffers do not have this problem: `EXPLAIN (..., BUFFERS)`
counts the pages this specific execution actually touched regardless of
whether the OS or Postgres already had them cached, which is exactly
why this agent's own evidence rules treat the buffer total as the
admissible, cache-independent gate and `actual time=` as inadmissible
alone. No exec-time claim is made for this comparison.

Forcing the plan (row 2) proves the index itself is not the problem:
once selected, it is **~2,000x cheaper in buffers** than what the
unforced planner picks. The literal-equality form (row 3) is still
**~500x cheaper** than the unforced plan, though not cheaper than the
forced bitmap-plus-sort form (row 2) by buffers -- 189 versus 46. Its
advantage is structural, not a lower buffer count: a plain `=` lets
Postgres recognize the index already returns `id`-ordered output and
skip the sort and the parallel bitmap machinery entirely, so it needs
no forcing to reach a cheap plan at all.

So the verdict is narrower and more actionable than either earlier
draft claimed: the composite index **would help enormously**. What
blocks it is that the default cost-based planner, working from a
misestimated row count for the `= ANY($1)` condition against this
partial index, ranks the (actually catastrophic) id-only-index plan as
cheaper than the (actually excellent) alternative-index-plus-sort plan.
A quick attempt at `CREATE STATISTICS (dependencies, ndistinct) ON
workflow_name, quota_key, state` did not change the unforced plan
choice in this fixture — extended statistics on the base table do not
straightforwardly fix a partial index's own selectivity estimate for a
`ScalarArrayOpExpr` condition. That was not pursued further; it is a
side note, not a ruled-out avenue.

## 📊 Measurement — one full reconcile pass (largest fixture)

Real `reconcile_quota_keys_from` calls, driven to completion (6 ticks,
1,000 rows backfilled), `pg_stat_statements`-scoped to this database,
reproduced twice with identical totals:

| calls | shared_blks_hit | shared_blks_read | total_buffers |
|---:|---:|---:|---:|
| 6 | ~489,500-489,600 | ~13,500-13,600 | **503,105** (both runs) |

This is six calls, not one. `spawn_quota_key_reconciler_for_shard` makes
exactly one `reconcile_quota_keys_from` call per `worker_heartbeat_interval`
tick, so 503,105 is the total cost of a full pass over the candidate set --
six heartbeat intervals here, not a single one. A single interval's cost
during the backfill is the "Plan" table above: 95,091 buffers for one
first-tick call at this same 500,000-noise size, the number that actually
clears the impact floor's 5%-of-workload-buffers bar for a single-shard,
single-heartbeat-interval sweep.

**Steady state is not zero. It is worse than the backfill itself, for a
sustained workload.** Only the 1,000 target rows ever leave the candidate
index because *this sweep* backfilled them -- their `quota_key` gets set,
so they stop matching `quota_key IS NULL`. A non-quota'd row leaves the
index too, but only when its own execution goes terminal (the index
predicate is `state IN ('RUNNING', 'PAUSED')`, not `quota_key IS NULL`
alone) -- nothing about *this sweep* ever touches it. The fixture never
advances that population, which is deliberate: it models a live
production system's steady state, where new non-quota'd starts
continuously replace completions and the active non-quota'd population
stays roughly constant, not a workload that drains to zero and stays
there. Once the target backlog is fully backfilled, `CANDIDATE_SQL` has
zero rows left it can match, but it does not know that in advance: it
still walks the id-ordered candidate index from the last cursor position
(which wraps to the start once a batch comes back short) looking for a
`billing_saga` row that no longer exists, scanning every one of the
500,000 noise rows before giving up and returning zero rows. Measured
directly, immediately after the backfill pass completes, against the
identical fixture:

| calls | buffers | rows removed by filter | rows returned | exec time |
|---:|---:|---:|---:|---:|
| 1 | 504,201 (hit/read split varies, total reproduced identically twice) | 500,000 | 0 | 267-314ms |

That is not a one-time rollout expense. Under a sustained workload -- the
active non-quota'd population never draining to zero and staying there --
it is the **permanent** cost of *every* heartbeat tick, for as long as
this deployment has both a quota'd workflow type and a non-quota'd
population sharing the table, worse than any single tick measured during
the backfill itself.

## 💡 Verdict

The scaling risk the migration comment names is real, confirmed at
production-shaped scale, worse than a rollout-window cost under a
sustained non-quota'd workload (it does not end on its own), and the
specific reason the proposed index does not help in practice is
now precisely diagnosed: a planner cardinality misestimate under
`= ANY($1)` against a partial index whose predicate correlates with the
leading column, not an inherent inability to use the index at all. The
buffer gap between the plan Postgres picks and the plan it could pick is
500x-2,000x, measured against the identical fixture (the admissible,
cache-independent gate; see "Root cause" above for why no exec-time claim
is made alongside it).

## 🔧 Why no fix ships in this PR

Two directions exist, both evidenced above, neither attempted here:

1. **Rewrite the query per registered name.** Instead of one query
   filtered by `workflow_name = ANY($1)`, issue one bounded,
   `ORDER BY id LIMIT $3`-scoped query per registered quota'd workflow
   name (each a plain `=`, each able to use the index cleanly per the
   literal-equality measurement above), merged by `id` before applying
   the outer `LIMIT`.
2. **Fix the statistics**, so the existing single query picks the good
   plan on its own. Not solved here; the one attempt made
   (`CREATE STATISTICS` on the base table) did not change the outcome,
   and finding what would needs its own investigation.

Neither is this pass's "smallest change that moves the counter":

- Route 1 does not need dynamically generated SQL text. The same static
  parameterized query (`workflow_name = $1`, exactly what
  `explain_literal_equality` already demonstrates) can execute once per
  registered name; only the number of *executions* scales with the
  registry, and the prepared-statement cache still sees one query
  shape, not an unbounded one. Two real costs remain, though: one round
  trip per registered name per tick instead of one round trip total,
  and merging N per-name result streams by `id` while preserving the
  keyset cursor's anti-starvation guarantee this module's doc comment
  describes at length -- "every tick moves strictly past whatever it
  just examined" now has to hold across N cursors, not one. That merge
  design is the real design question, not a mechanical rewrite.
- Route 2 is architecturally smaller in principle but was not reduced
  to a working, verified statistics change in this pass — reporting
  "try `CREATE STATISTICS`" without a measured before/after would
  itself violate this agent's own evidence rules.

Per this agent's process, a change like either is an "ask before": the
author decides. This PR stops at diagnosis, with the mechanism nailed
down precisely enough that whoever picks up the fix does not need to
re-run this investigation. Given the steady-state cost measured above
recurs for as long as the non-quota'd workload stays active, this
diagnosis is worth prioritizing sooner rather than later in any
deployment that mixes quota'd and non-quota'd workflow types at this
population scale.

## 🔬 Reproduce

```sh
sudo systemctl start postgresql   # or any reachable Postgres 13+ with
                                   # pg_stat_statements preloaded
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh
```
