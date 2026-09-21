# Ledger: `quota_reconcile` candidate-scan cost under mixed-deployment skew

Findings, not a fix. Issue #1226 follow-up, closing the open question
migration `20260910192721_harvest_quota_reconcile_candidate_index`'s own
comment raises, with a root cause more specific than the migration
guessed. No code, index, or query change ships in this PR — see
"Why no fix ships" below.

**Correction history:** this document originally shipped a negative
result ("no code change is warranted"), based on a fixture that seeded
no terminal history for the quota'd workflow type itself. `Codex Review`
caught the gap in [PR #1691](https://github.com/autumn-foundation/autumn-harvest/pull/1691)
before merge. Re-measured with a realistic terminal population under the
target workflow type; the conclusion below reverses the original one.

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

## 🧭 Plan — first tick at each noise size

| noise rows | plan chosen | buffers | rows removed by filter | exec time |
|---:|---|---:|---:|---:|
| 20,000 | Index Scan on `idx_harvest_we_quota_reconcile_candidates`, residual filter | ~3,800 | ~3,575 | ~1.4ms |
| 100,000 | same | ~20,100 | ~19,800 | ~12ms |
| 500,000 | same | ~86,800 hit + ~11,000 read | ~97,100 | ~148ms |

Unlike the first (incorrect) pass, `idx_harvest_wfx_workflow_identity`
is **never** picked here at any size. That index is not partial by
state — it covers `billing_saga`'s full history, terminal rows
included. Once that history is realistic (50,000 rows, matched here to
the noise population's own terminal share), scanning it costs as much
as scanning the id-ordered candidate index, so the planner has no
incentive to switch. Cost scales with the non-quota'd population as the
migration comment originally warned, reaching a genuinely expensive
148ms / ~98,000-buffer single batch at 500,000 noise rows — some real
disk reads (`read=10982`), not just cache churn.

Full plans: `docs/perf-artifacts/quota-reconcile-candidate-scan/noise-*.explain.txt`.

## 🔬 Root cause of why the rejected index does not help

The migration comment proposes and rejects a `(workflow_name, id)`
partial index, reasoning that it could not serve `ORDER BY id LIMIT $3`
without an extra sort. Built and tested directly against the 500,000
fixture, with `CANDIDATE_SQL` unmodified: Postgres does **not** pick it
up. It keeps using the id-only index, at the same ~98,000-buffer cost
(`docs/perf-artifacts/quota-reconcile-candidate-scan/alternative-index-explain.txt`).

That is not the sort-cost trade-off the migration predicted. It is a
narrower, more specific planner limitation, confirmed by testing the
same index against three query forms on the same fixture and connection:

| predicate form | plan | buffers |
|---|---|---:|
| `workflow_name = ANY($1)` (what `CANDIDATE_SQL` binds) | id-only index, residual filter | ~97,800 |
| `workflow_name = 'billing_saga'` (literal equality) | `(workflow_name, id)` index, no residual filter | ~197 |
| `workflow_name IN ('billing_saga')` (literal, single-element) | same as equality (Postgres folds it) | ~197 |

`= ANY($1::text[])` — a `ScalarArrayOpExpr` — does not get ordered index
access from a leading-column index in this Postgres version, even for a
single-element array. A plain `=` on the exact same index, against the
exact same data, is **~500x cheaper**. This is the real reason the
composite index "is not a strict win" as written: it is not a
sort-versus-scan trade-off at all, it is that `CANDIDATE_SQL`'s query
shape cannot reach the index's benefit no matter how it is indexed.

## 📊 Measurement — one full reconcile pass (largest fixture)

Real `reconcile_quota_keys_from` calls, driven to completion (6 ticks,
1,000 rows backfilled), `pg_stat_statements`-scoped to this database:

| calls | shared_blks_hit | shared_blks_read | total_buffers |
|---:|---:|---:|---:|
| 6 | 488,556 | 13,548 | 502,104 |

Half a million buffers for one pass, on a fixture whose non-quota'd
population (500,000 rows) is well within plausible production scale.
This clears the impact floor's 5%-of-workload-buffers bar for even a
single-shard, single-heartbeat-interval reconcile sweep, and the `= ANY`
mechanism above says exactly why a same-shape index will not fix it.

## 💡 Verdict

The scaling risk the migration comment names is real, confirmed at
production-shaped scale, and worse than the migration's own worst case
described: a 500x gap exists between the query shape `CANDIDATE_SQL`
uses and the query shape that lets an index actually help. Buffer cost
grows with the non-quota'd population with no natural ceiling, unlike
the first (incorrect) pass's finding.

## 🔧 Why no fix ships in this PR

A structural fix exists in principle: instead of one query filtered by
`workflow_name = ANY($1)`, issue one bounded, `ORDER BY id LIMIT
$3`-scoped query per registered quota'd workflow name (each a plain `=`,
each able to use a `(workflow_name, id)` partial index), and merge the
per-name results by `id` before applying the outer `LIMIT`. Postgres can
do this efficiently for a small, bounded name count via a Merge Append
over already-sorted per-name scans.

This is not this pass's "smallest change that moves the counter":

- It changes `CANDIDATE_SQL` from one static, cacheable query to a
  dynamically-built one scaling with the registered quota'd workflow
  count — exactly the "unbounded distinct statement texts defeating the
  prepared-statement cache" pattern this agent's own process calls out
  to avoid, unless the fan-out is capped and reasoned about explicitly.
- More importantly, it touches the keyset cursor's anti-starvation
  guarantee this module's doc comment describes at length: merging N
  per-name cursors while preserving "every tick moves strictly past
  whatever it just examined, across every registered name" is a real
  design question, not a mechanical rewrite. Getting it wrong
  reintroduces the starvation this module exists to prevent.

Per this agent's process, a change like that is an "ask before": the
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
