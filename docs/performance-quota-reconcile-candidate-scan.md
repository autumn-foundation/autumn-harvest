# Ledger: `quota_reconcile` candidate-scan cost under mixed-deployment skew

Findings, not a fix. Issue #1226 follow-up, closing the open question
migration `20260910192721_harvest_quota_reconcile_candidate_index`'s own
comment raises, with a root cause more specific than the migration
guessed. No code, index, or query change ships in this PR — see
"Why no fix ships" below.

**Correction history** (both from `Codex Review` on
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

| noise rows | plan chosen | buffers | rows removed by filter | exec time |
|---:|---|---:|---:|---:|
| 20,000 | Index Scan on `idx_harvest_we_quota_reconcile_candidates`, residual filter | 3,870 (all hit) | 3,657 | 1.8ms |
| 100,000 | same | 18,970 (all hit) | 18,682 | 12.0ms |
| 500,000 | same | ~95,100 (hit+read) | 94,467 | ~142-152ms |

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
index was structurally unusable under `= ANY($1)`. Round 2 of review
pushed back, correctly: a `ScalarArrayOpExpr` CAN still be an index
condition followed by a sort, and the row estimate for the alternative
plan (`rows=38,875`) is wildly off from the actual matching rows
(1,000) — a 38x overestimate, caused by `workflow_name` and the partial
predicate (`quota_key IS NULL AND state IN (...)`) being correlated in
a way ordinary column statistics do not capture. That is worth testing
directly rather than reasoning about, so three variants were captured
against the identical fixture and connection:

| variant | mechanism | buffers | exec time |
|---|---|---:|---:|
| unmodified `CANDIDATE_SQL` (`= ANY($1)`), nothing forced | what production runs | ~95,100 | ~142-152ms |
| same query, `enable_indexscan = off` for one transaction | forces the planner onto the alternative index via Bitmap Index Scan + Sort | 46 | 6.5ms |
| same predicate rewritten to literal `workflow_name = $1` | plain equality lets Postgres serve `ORDER BY id` directly from the index, no sort | 189 | 0.11ms |

Forcing the plan (row 2) proves the index itself is not the problem:
once selected, it is **~2,000x cheaper** than what the unforced planner
picks. The literal-equality form (row 3) is cheaper still, since a
plain `=` lets Postgres recognize the index already returns `id`-ordered
output and skip the sort entirely.

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

Half a million buffers for one pass, on a fixture whose non-quota'd
population (500,000 rows) is well within plausible production scale.
This clears the impact floor's 5%-of-workload-buffers bar for even a
single-shard, single-heartbeat-interval reconcile sweep.

## 💡 Verdict

The scaling risk the migration comment names is real, confirmed at
production-shaped scale, and the specific reason the proposed index
does not help in practice is now precisely diagnosed: a planner
cardinality misestimate under `= ANY($1)` against a partial index whose
predicate correlates with the leading column, not an inherent inability
to use the index at all. The gap between the plan Postgres picks and
the plan it could pick is roughly 500x-2,000x depending on measure.

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

- Route 1 changes `CANDIDATE_SQL` from one static, cacheable query to a
  dynamically-built one scaling with the registered quota'd workflow
  count — exactly the "unbounded distinct statement texts defeating the
  prepared-statement cache" pattern this agent's own process calls out
  to avoid, unless the fan-out is capped and reasoned about explicitly.
  More importantly, it touches the keyset cursor's anti-starvation
  guarantee this module's doc comment describes at length: merging N
  per-name cursors while preserving "every tick moves strictly past
  whatever it just examined" is a real design question, not a
  mechanical rewrite.
- Route 2 is architecturally smaller in principle but was not reduced
  to a working, verified statistics change in this pass — reporting
  "try `CREATE STATISTICS`" without a measured before/after would
  itself violate this agent's own evidence rules.

Per this agent's process, a change like either is an "ask before": the
author decides. This PR stops at diagnosis, with the mechanism nailed
down precisely enough that whoever picks up the fix does not need to
re-run this investigation.

## 🔬 Reproduce

```sh
sudo systemctl start postgresql   # or any reachable Postgres 13+ with
                                   # pg_stat_statements preloaded
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh
```
