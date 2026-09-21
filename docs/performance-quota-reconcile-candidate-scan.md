# Ledger: `quota_reconcile` candidate-scan cost under mixed-deployment skew

Negative result. No code, index, or query change shipped. Issue #1226
follow-up, closing the open question migration
`20260910192721_harvest_quota_reconcile_candidate_index`'s own comment
raises.

## 🎯 Workload

`quota_reconcile::reconcile_quota_keys_from` runs on
`worker_heartbeat_interval` cadence, once per assigned shard, on every
worker process (see `quota_reconcile.rs`'s module doc). Its candidate scan
(`CANDIDATE_SQL`) finds non-terminal `harvest_workflow_executions` rows
with `quota_key IS NULL`, restricted to workflow types with a currently
registered `QuotaPolicy`.

The migration that added `idx_harvest_we_quota_reconcile_candidates` (a
partial index on `(id) WHERE quota_key IS NULL AND state IN ('RUNNING',
'PAUSED')`) flags an unresolved trade-off in its own comment: the query's
`workflow_name = ANY($1)` clause is not part of that index, so it runs as
a residual filter, not a seek. In a mixed deployment with a large
non-quota'd population and few matching rows, the migration warns "one
tick's scan can touch many discarded rows before filling `batch_size`."
It proposes and rejects a `(workflow_name, id)` index as an alternative,
stating confirmation "needs `EXPLAIN ANALYZE` against production-scale
data" and marking it "tracked as a follow-up, not silently accepted."

Reproduce: `HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh`

## 📈 Fixture

Seeded, deterministic, production-shaped: one quota'd workflow type
(`billing_saga`, 1,000 non-terminal rows with `quota_key IS NULL` — a
realistic post-rollout backfill population) alongside a large population
of 50 distinct non-quota'd workflow types sharing the same table and the
same `quota_key IS NULL` / non-terminal predicate, swept across three
sizes: 20,000 / 100,000 / 500,000 rows. A 10%-of-noise terminal
(`COMPLETED`) population is seeded at each size too, for a realistic
terminal/non-terminal mix. `batch_size` is
`QUOTA_RECONCILE_DEFAULT_BATCH` = 200 throughout. Row ids are
server-generated UUIDs, so target and noise rows interleave uniformly in
`id` order with no extra scattering step.

## 🧭 Plan — first tick at each noise size

| noise rows | plan chosen | buffers | rows removed by filter |
|---:|---|---:|---:|
| 20,000 | Index Scan on `idx_harvest_we_quota_reconcile_candidates`, residual `workflow_name` filter | 4,690 (all hit) | 4,469 |
| 100,000 | Bitmap Heap Scan via `idx_harvest_wfx_workflow_identity` (unrelated, pre-existing index on `workflow_name`) + Sort + top-N Limit | 35 | 0 |
| 500,000 | same as 100,000 | 44 | 0 |

At 20,000 noise rows the planner picks exactly the plan the migration
comment warned about: an id-ordered index scan filtering
`workflow_name` row by row, discarding 4,469 non-matching rows to fill
one 200-row batch. Above roughly 100,000 noise rows, the planner
switches to a different plan entirely — a Bitmap Index Scan on
`idx_harvest_wfx_workflow_identity`, an unrelated index added by
migration `20260710000002_harvest_workflow_continue_chain` for
continue-as-new chain lookups, which happens to make `workflow_name`
itself selective enough to seek on directly. It then sorts only the
matched ~1,000 `billing_saga` rows in memory (`Sort Method: top-N
heapsort`) and takes the top 200. Buffer cost **drops** as the noise
population grows past that crossover, the opposite of what the "large
non-quota'd population" framing predicts.

Full plans: `docs/perf-artifacts/quota-reconcile-candidate-scan/noise-*.explain.txt`.

### The rejected `(workflow_name, id)` alternative, tested directly

Creating the exact partial index the migration comment proposes and
rejects (`(workflow_name, id) WHERE quota_key IS NULL AND state IN
('RUNNING', 'PAUSED')`) against the 500,000-noise fixture, then
re-running the **same unmodified** `CANDIDATE_SQL`: the planner does pick
it up (Bitmap Index Scan on the new index, 23 hit + 9 read = 32 buffers)
instead of `idx_harvest_wfx_workflow_identity` (35 buffers). The
difference is noise — both plans do the same shape of work (seek by
`workflow_name`, sort ~1,000 rows, take top 200) and land within a few
buffers of each other. The migration's own call — "not a strict win" —
is confirmed, not just asserted: the existing unrelated index already
gets Postgres most of the way there once the table is large enough to
need it, so the extra write-amplification of the composite index buys
close to nothing at read time.

Plan: `docs/perf-artifacts/quota-reconcile-candidate-scan/alternative-index-explain.txt`.

## 📊 Measurement — one full reconcile pass (largest fixture)

Real `reconcile_quota_keys_from` calls, driven to completion (6 ticks,
1,000 rows backfilled), `pg_stat_statements`-scoped to this database:

| calls | shared_blks_hit | shared_blks_read | total_buffers |
|---:|---:|---:|---:|
| 6 | 614 | 43 | 657 |

657 buffers for a full backfill pass over a 501,000-row candidate
population is not a workload cost worth optimizing against — nowhere
near the impact floor's 5%-of-workload-buffers threshold for even
considering a change, let alone the 20% reduction floor a change would
need to clear.

## 💡 Verdict

The scaling risk the migration comment names is real, but only in a
narrow, low-severity band: a moderate non-quota'd population (tested at
20,000 rows) before the planner's cost model crosses over to the
`idx_harvest_wfx_workflow_identity` plan. Even there, the extra cost is
4,690 buffers, entirely cache hits, no reads — not an I/O concern, and
one tick, not a per-request cost. Above that band the concern
self-resolves through an unrelated existing index, not through anything
`quota_reconcile.rs` does on purpose.

No change clears the impact floor. The composite index the migration
comment proposed was built and measured directly rather than reasoned
about, and confirmed not worth its permanent write cost. This closes
issue #1226's "tracked as a follow-up, not silently accepted" note with a
measured answer: no code change is warranted, and the existing design
should stay as shipped.

## 🔬 Reproduce

```sh
sudo systemctl start postgresql   # or any reachable Postgres 13+ with
                                   # pg_stat_statements preloaded
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh
```
