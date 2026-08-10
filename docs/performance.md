# Task-claim and enqueue performance

Harvest publishes a CPU-path budget for replay (a 10 000-event history replays in
under 200 ms, issue #135), but until issue #786 it published nothing at all for
`queue::claim_task` — the single most scalability-critical query in the engine,
and the one that has accreted roughly a `WHERE` predicate per phase since 3.7:

| Predicate | Issue |
|:--|:--|
| build-id routing | #171 |
| per-key concurrency | #247 |
| rate-limit gate | #332 / #699 |
| circuit-breaker tracked set | #369 |
| `schedule_to_close` | #378 |
| PAUSED-execution skip | #383 |
| worker sessions | #606 |
| queue pauses | #619 |
| capability labels | #382 |
| sticky routing | #235 |

Each was added for correctness. None was measured. This page is the measurement.

> **These are starter reference numbers, not an SLO.** They were taken on one
> machine with one Postgres configuration (below). Your hardware, your
> `shared_buffers`, your backlog shape, and your queue count all move them.
> Reproduce them on your own hardware before designing against them — the
> benchmark is in the repo precisely so you can.

## TL;DR

* **Claim latency scales superlinearly with pending-backlog depth.** 1k → 10k
  rows (10x) costs ~23x latency; 10k → 100k (10x) costs a further ~14x. Claim
  cost is a function of how deep your queue is, not how much work you dispatch.
* **The cause is structural, not incidental.** The claim query's `ORDER BY`
  leads with a non-indexable `CASE` expression, so `idx_harvest_tq_poll` cannot
  serve the ordering. Postgres sequentially scans and sorts every eligible
  pending row on every single claim. See [the plan](#the-plan) below.
* **The two expensive gates are per-key concurrency (+306% p50) and the
  PAUSED-execution skip (+1319% p50).** Build-id routing, the rate-limit gate,
  and the circuit-breaker tracked set are all within noise of free.
* **Enqueue is not the problem.** ~4 000 rows/s sustained at p50 1.5 ms,
  essentially flat from 1k to 100k backlog.
* Issue #786 deliberately **measures without tuning**: the claim query is
  byte-for-byte unchanged by this work.

## Reference environment

The tables on this page came from these two commands:

```bash
# The full exploratory report (the tables on this page).
# Expect this to take 15-30 minutes: it sweeps three backlog depths, eight gate
# scenarios and three enqueue depths, and a 100k-row scenario is slow on purpose.
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
HARVEST_BENCH_SCENARIO_SECS=180 \
  cargo bench -p autumn-harvest --features db --bench claim_bench

# The CI gate (the headline scenario only), ~2 minutes:
cargo test -p autumn-harvest --features db --test integration -- \
  claim_budget_tests --test-threads=1
```

Two figures on this page are **not** from a single run of those commands, and
say so where they appear: the budget derivation (a distribution over 8 runs) and
the debug-vs-release comparison (two profiles, back to back).

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16.13 (Ubuntu), default `shared_buffers` |
| Profile | `bench` (release). Debug was measured too — see [profile](#profile-does-not-matter) |
| Harness | `autumn-harvest/tests/integration/claim_bench_support.rs` |

`HARVEST_TEST_DATABASE_URL` is treated as an **admin** URL, not a target
database: the role it names **must be able to `CREATE DATABASE`**, because a
freshly-named database is created and migrated per run so a 100k-row backlog can
never leak into a shared one. A role without that privilege makes the harness
report a skip and exit 0 — a silent no-result rather than an error, so check the
privilege before concluding "the benchmark produced nothing".

Two more things worth knowing before pointing this at a server you care about:

* **Setup sweeps stale `harvest_claim_bench_%` databases**, not just teardown —
  that is what reclaims databases orphaned by a run that panicked, which a
  teardown hook can never do. It skips any database whose owning pid is still
  alive, so concurrent runs cannot delete each other's working set. The liveness
  check is Linux-only (`/proc/{pid}`); everywhere else it conservatively assumes
  the pid is alive, so on macOS and Windows stale databases accumulate until
  removed by hand.
* **`HARVEST_BENCH_SCENARIO_SECS`** caps each scenario's measured phase
  (default 240 s). It is the knob to raise when a row comes back marked `⚠` or
  `‡` — see [measurement hygiene](#measurement-hygiene).

With the variable unset the benchmark starts a `postgres:16` testcontainer
instead; with neither available it prints a skip notice and exits 0.

## Claim latency vs backlog depth

Baseline gate (no build policy, no concurrency key, no rate limit, no pauses),
8 concurrent claimers across 4 queues:

| backlog | p50 ms | p99 ms | max ms | claims/s |
|--:|--:|--:|--:|--:|
| 1 000 | 9.35 | 19.39 | 20.92 | 626 |
| 10 000 | 219.69 | 264.11 | 284.01 | 29 |
| 100 000 † | 3 142.97 | 3 442.05 | 3 453.71 | 2 |

† Cut short by the per-scenario wall-clock budget (60 s, override with
`HARVEST_BENCH_SCENARIO_SECS`). The percentiles describe the 144 claims it did
observe. That the scenario *cannot* finish 800 claims in a minute is itself the
finding.

These rows run at 8 concurrent claimers against a 4-core box, so they measure
the claim path **under contention** — the operational number a worker actually
waits for, not the isolated query cost. The tail columns in particular carry
run-queue scheduling as well as query time; the p50 column is the more stable
comparison, and the per-gate table below deliberately runs below saturation for
the same reason.

**Read this table as a sharding trigger.** A queue that stays around a thousand
pending rows claims in single-digit milliseconds. A queue that sits at ten
thousand claims in a fifth of a second — still workable, but each worker poll is
now a real cost. A queue parked at a hundred thousand pending rows spends
multiple seconds per claim, and the fleet's throughput collapses to a couple of
claims per second regardless of how many workers you add. If your steady-state
backlog is trending toward the 100k row, the answer is to shard
(`docs/sharding.md`) or to shed the backlog — adding workers will not help,
because every worker pays the full scan.

## Incremental cost of the accreted gates

10 000-row backlog, 4 queues, **2 claimers**.

Attribution deliberately runs below the headline scenario's 8 claimers. At 8
claimers on a 4-core box the tail columns stopped reproducing between runs (an
identical baseline scenario reported a 285 ms max in one run and 3 093 ms in the
next) even though the p50 deltas held steady — the tails were measuring
run-queue scheduling, not the query. Below saturation the numbers isolate
predicate cost.

**`p50 vs baseline` is the attribution statistic.**

| gate | seeded rows | claimable | n | p50 ms | p50 vs baseline |
|:--|--:|--:|--:|--:|--:|
| `baseline` | 10 000 | 10 000 | 504 | 100.89 | — |
| `circuit_breaker_set` | 10 000 | 10 000 | 532 | 104.27 | +3% |
| `rate_limited` | 10 000 | 10 000 | 490 | 108.30 | +7% |
| `build_policy` | 10 000 | 10 000 | 454 | 116.10 | +15% |
| `concurrency_key` | 10 000 | 10 000 | 174 | 409.26 | **+306%** |
| `paused_rows` | 20 000 | 10 000 | 77 | 1 431.27 | **+1319%** |
| `all_gates` | 20 000 | 10 000 | 66 | 1 695.00 | **+1580%** |

Reproducibility: a second independent run gave +1%, −3%, +13%, +283%, +1346%,
+1462% for the same rows — the ordering and the order of magnitude are stable,
the exact percentages are not. Treat these as "free / cheap / expensive", not as
three significant figures.

What each row exercises, and what it means:

* **`circuit_breaker_set` (+3%)** — the worker passes a populated tracked-activity
  set, so the rate-limit gate and debit are skipped via `= ANY($5)` (#369).
  Free, as designed.
* **`rate_limited` (+7%)** — rows carry a `rate_limit_key` with a funded bucket,
  exercising the candidate-side `EXISTS` gate and the `rate_limit_debit` CTE
  (#332 / #699). Effectively free at this backlog.
* **`build_policy` (+15%)** — rows carry `required_build_id` and the worker's
  build matches only through a `harvest_build_compat` declaration, forcing the
  `EXISTS` branch rather than the cheap `required_build_id = $3` equality
  (#171). A real but modest cost; safe-deploy ramps (#604) are not expensive.
* **`concurrency_key` (+306%)** — rows carry `concurrency_key` + `concurrency_cap`,
  exercising both the candidate-side `COUNT(*)` subquery and the
  `pg_try_advisory_xact_lock` re-check in the `claimed` CTE (#247). This is
  measured with 256 distinct keys and a cap high enough never to block, so it is
  the *predicate* cost with contention deliberately minimised. **Per-key
  concurrency is not free — budget for it.**
* **`paused_rows` (+1319%)** — the scenario seeds 10 000 rows belonging to PAUSED
  executions *in addition to* the claimable backlog, so the claimable pool is
  identical to baseline and the only variable is the extra rows the scan walks
  past through the `NOT EXISTS` anti-join (#383). This is the single most
  important operational finding on this page: **pausing executions does not take
  their work out of the claim path.** The rows stay `PENDING`, they stay in the
  scan, and every claim by every worker pays for them. A fleet with a large
  paused population is paying that cost continuously.
* **`all_gates` (+1580%)** — everything at once, for an upper bound.

## The plan

`EXPLAIN (ANALYZE, BUFFERS)` of a single headline claim, trimmed to the nodes
that matter (the benchmark prints it in full). The worker-id literal is
shortened to `'worker-0'` for width; the real bind is
`'harvest-bench-worker-0'`. Nothing else in the shown nodes is edited — the
absence of `cost=`/`actual time=` is `COSTS OFF, TIMING OFF`, and the folded
`priority DESC` is the planner constant-folding an unused bind.

Note that this plan is for the **claim statement**, which is the dominant part
of — but not all of — the operation the tables above time (see
[what is actually timed](#what-is-actually-timed)).

```text
->  Sort (actual rows=1 loops=1)
      Sort Key: (CASE WHEN ((sticky_worker_id = 'worker-0') AND (sticky_until > now()))
                 THEN 1 ELSE 0 END) DESC, priority DESC, scheduled_at
      Sort Method: quicksort  Memory: 1400kB
      Buffers: shared hit=20223
      ->  Nested Loop Anti Join (actual rows=10000 loops=1)
            ->  Seq Scan on harvest_task_queue (actual rows=10000 loops=1)
                  Buffers: shared hit=223
            ->  Index Scan using harvest_queue_pauses_pkey on harvest_queue_pauses qp
                  (actual rows=0 loops=10000)
                  Buffers: shared hit=20000
```

Two things to read here:

1. **`Seq Scan` + `Sort`, not an index scan.** `idx_harvest_tq_poll` is
   `(queue_name, state, priority DESC, scheduled_at) WHERE state = 'PENDING'` —
   exactly the right index for the *filter*, but it cannot serve the *ordering*,
   because the claim query sorts by a `CASE` expression on
   `sticky_worker_id`/`sticky_until` first (#235) and by a second `CASE` for
   priority aging. Neither is indexable. So every claim reads and sorts all
   eligible pending rows to return one. That is the superlinear scaling in the
   table above.
2. **`loops=10000` on the queue-pause anti-join.** The `NOT EXISTS` against
   `harvest_queue_pauses` (#619) runs once per candidate row — 20 000 of the
   20 223 buffer hits. It is an index lookup, so it is cheap *per row*, but it is
   paid per row.

In the full plan (which the benchmark prints, and which is trimmed out of the
excerpt above) the `SubPlan` nodes for the concurrency, rate-limit, capability
and PAUSED predicates all show `never executed` on a baseline seed. Each is
guarded by a cheap leading test that is false for every baseline row, so the
subplan never runs: the concurrency and rate-limit guards short-circuit on
`concurrency_key IS NULL` / `rate_limit_key IS NULL`, while the PAUSED and
capability guards short-circuit on a **type** test (`task_type <> 'workflow'`),
not a NULL test — the baseline seeds activity rows. That is why the cheap gates
in the attribution table are cheap: you only pay for a predicate when you
actually use the feature.

**None of this is fixed by issue #786.** Per that issue's scope, the claim query
is left byte-for-byte unchanged; measuring it and tuning it are separate pieces
of work, and tuning without a published baseline is how you get an unfalsifiable
"optimisation". This page is the baseline.

## Enqueue throughput

8 concurrent writers enqueueing into an already-populated queue:

| backlog | rows | p50 ms | p99 ms | max ms | rows/s |
|--:|--:|--:|--:|--:|--:|
| 1 000 | 800 | 1.75 | 22.00 | 22.73 | 3 108 |
| 10 000 | 800 | 1.51 | 3.10 | 4.54 | 4 269 |
| 100 000 | 800 | 1.52 | 3.48 | 7.05 | 4 246 |

Enqueue is **flat in backlog depth** — the write side does not degrade as the
queue fills, which is the expected and desired asymmetry. A start-storm is
bounded by your connection pool and by Postgres write throughput, not by
anything Harvest does.

Two caveats on this table. `queue::enqueue` is not a bare `INSERT`: it resolves
defaults and writes one row inside its own transaction, so the per-row latency
includes transaction and round-trip overhead — which is exactly why it is worth
measuring rather than assuming. And the throughput column is a **floor**, not a
peak: it divides all rows by the whole wall clock including warmup and task
spawn/join.

## The CI gate

`claim_budget_tests::claim_p50_at_headline_scenario_is_within_budget` runs via
the `linux` row in `.github/ci/integration-suites.txt` — so, on Linux, on
code-touching changes — and fails the build when **p50** claim latency at the
**headline scenario** (10 000 pending rows, 8 concurrent claimers, 4 queues)
exceeds its budget.

| | |
|:--|:--|
| Statistic | **p50** (see below — deliberately not p99) |
| Reference p50 | ~220 ms idle, ~516 ms under load |
| Budget | **1 500 ms** (~2.9x the worst observation, ~6.8x the idle one) |
| Override | `HARVEST_CLAIM_BUDGET_P99_MS` (historical name; overrides whichever statistic the gate asserts) |

### Why the gate asserts p50, not p99

The headline scenario runs 8 concurrent claimers against a 4-core box on
purpose — contention is the point of the scenario. But that makes the database
oversubscribed, and an oversubscribed tail measures the run queue rather than
the claim path. Measured across repeated runs on the reference machine:

| statistic | idle box | loaded box | spread |
|:--|--:|--:|--:|
| p50 | ~220 ms | ~516 ms | **~2.3x** |
| p99 | ~300 ms | ~4 665 ms | **~15x** |

A p99 gate at this budget failed roughly **one run in three** during review, on
the same hardware class and Postgres version that produced the published
numbers. It was not detecting regressions; it was detecting the scheduler. (The
sweep table above, captured on a moderately busy box, shows the same thing at
the headline depth: p50 283 ms against a p99 of 1 652 ms — a number that would
have failed a 1 500 ms p99 gate while nothing about the claim path had changed.)

p99 is still measured, still published above, and printed in the gate's failure
message so a genuine tail regression is visible to whoever reads it. It is
simply not the assertion.

**The budget is a cliff detector, not a drift detector.** It will not catch a
predicate that makes claims 50% slower — the reference p50 itself moves 2.3x
with machine load, so no threshold on this hardware could. It catches the kind
of change that adds another per-row subplan to a scan already walking the whole
pending backlog. For drift, run the benchmark and compare against the per-gate
table above, which runs below saturation precisely so it can resolve smaller
differences.

The gate also asserts three soundness properties, each of which fails loudly
rather than reporting a meaningless percentile:

* **at least 100 samples were collected** — a severe regression could otherwise
  leave the gate defending a two-sample percentile;
* **the run was not truncated** by the wall-clock ceiling — a partial run's
  percentiles describe a shorter, differently-warmed window than the published
  ones, so the gate defends a complete scenario or says so;
* **at least 90% of measured operations actually claimed a task.** Note what this
  does and does not prove: it rules out "the harness measured an empty queue",
  which is the failure mode that would silently make the gate pass. It does not
  prove each gate scenario put its *predicate* on the execution path — a
  scenario that stopped setting its trigger column would still claim 100% of its
  operations, just via the cheap `IS NULL` leg. That is what the separate
  seed-census test asserts;
* **the backlog was not drained below 80%.** `claim_task` is destructive, so a
  run that emptied the queue would be timing claims against an increasingly
  empty table. The bound is enforced two ways: the planned operation count is
  capped at `backlog / 5`, and the per-claimer split is exact, so the claimers
  between them can never execute more than that plan.

Locally, with neither Docker nor `HARVEST_TEST_DATABASE_URL`, the gate skips with
a notice. Under `CI` it **fails** instead: a performance gate that silently
no-ops when its dependency is missing is not a gate.

## Methodology

### Why not criterion

`claim_task` is **destructive** — it moves a row `PENDING → RUNNING`. Criterion
runs the measured closure thousands of times, which would drain the seeded
backlog and end up timing "claim against an empty queue", the opposite of the
thing under test. The harness instead seeds a backlog of N, performs a bounded
number of claims (never more than N/5), and reports true percentiles over the
collected per-claim latencies.

### What is actually timed

One `queue::claim_task(...)` call, wall clock, from the client. That call is **a
whole transaction**, not the single statement the `EXPLAIN` below shows: it opens
a transaction, runs the claim CTE, and — depending on the row it lands on —
performs follow-up work such as the rate-limit debit and the per-key concurrency
advisory-lock re-check, before committing. So a published number includes
transaction overhead and one client↔server round trip, and the `EXPLAIN` plan
explains the *dominant* statement rather than the whole measured operation.

That is the right thing to measure — it is what a worker actually waits for —
but it means these numbers are not directly comparable to a bare `EXPLAIN
ANALYZE` of the claim query.

### Measurement hygiene

* **Percentiles are nearest-rank**, so a reported p99 is an actually-observed
  claim someone waited for, not an interpolation between two.
* **A tenth of each claimer's observations is discarded as warmup**, applied
  after collection rather than by planned index. This matters more than it
  sounds: an earlier revision discarded a flat 3 samples per claimer, and the
  headline p99 read ~2 900 ms instead of ~300 ms — the metric had become a
  cold-start measurement. Applying the fraction post-hoc also means a scenario
  cut short by its wall-clock budget still reports the samples it took instead
  of discarding all of them and printing a confident-looking `0.00 ms`.
* **`ANALYZE` runs after every seed.** Without it the planner works from stale
  statistics on a freshly bulk-loaded table and picks plans that are neither
  representative nor stable.
* **Seeding is set-based** (`INSERT ... SELECT FROM generate_series`), so a
  100k-row backlog costs one round trip.
* **The pool is always sized above the claimer count**, so a measured claim never
  includes pool-checkout queueing.
* **Every scenario truncates first**, so scenarios cannot contaminate each other.
* **Zero samples renders as `n/a`, never `0.00`.** A scenario that measured
  nothing must not publish a number that looks instantaneous. A row cut short by
  the wall-clock budget is marked `⚠`, and a row with fewer than 100 samples —
  below the floor the CI gate itself will accept — is marked `‡` and should be
  read as directional only.
* **The enqueue table gets the same warmup trim** as the claim tables, so its
  `n` column is below its `rows` column. `rows/s` deliberately divides *all*
  rows by the *whole* wall clock, warmup included, so it is a conservative floor
  on sustained throughput rather than a peak.

### Profile does not matter

The gate runs in the `test` (debug) profile and the benchmark in `bench`
(release). Measured back to back at the headline scenario, they agree: p50
228–256 ms in debug against 242–256 ms in release. The work is server-side, so
the client build profile is not a meaningful variable. Numbers from the gate and
from the benchmark are directly comparable.

### Known limitations

* **Single-shard, single-host.** Multi-shard distributed load is explicitly out
  of scope for issue #786. Per-shard numbers are what this page reports; a
  sharded deployment multiplies claim capacity by shard count, which is the
  entire point of `docs/sharding.md`.
* **Tail columns in the attribution table pick up background stalls** (autovacuum,
  the OS scheduler) and are reported for completeness only.
* **The seeded backlog has a degenerate sort-key distribution.** Every seeded row
  gets `priority = 0` and the same `scheduled_at`. Since the page's central
  finding is that Postgres sorts every eligible row by
  `(CASE …, priority DESC, scheduled_at)`, sorting N *identical* keys is not
  what a production backlog looks like. A real queue with mixed priorities and
  spread arrival times may sort differently — probably not cheaper, but this is
  measured on the degenerate case and should be read that way.
* **Not every claim-path predicate is varied.** The attribution table covers
  build-id routing (#171), per-key concurrency (#247), the rate-limit gate
  (#332/#699), the circuit-breaker tracked set (#369) and the PAUSED skip
  (#383). Four more are present in the query but held constant: capability
  labels (#382) — a real `NOT EXISTS` + `jsonb_array_elements` subplan, and the
  most defensible next scenario to add — plus `schedule_to_close` (#378), worker
  sessions (#606) and sticky routing (#235), which are cheap inline column
  tests. Queue pauses (#619) *is* exercised but is folded into the baseline, so
  its cost shows up in the `EXPLAIN` buffer counts rather than as a table row.
* **The scheduler tick and the timeout scanner are not benchmarked here.** They
  are separate hot paths and separate work.

## See also

* `benches/claim_bench.rs` — the report generator.
* `tests/integration/claim_bench_support.rs` — the harness, shared verbatim
  between the benchmark and the gate so published and gated numbers can never be
  produced by different code.
* `tests/integration/claim_budget_tests.rs` — the gate.
* `docs/sharding.md` — what to do when the backlog table says you have outgrown
  one shard.
