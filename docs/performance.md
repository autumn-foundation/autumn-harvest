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

Each was added for correctness. None was measured. This page is the measurement
— **for five of them**. The attribution table below varies build-id routing,
per-key concurrency, the rate-limit gate, the circuit-breaker tracked set and
the PAUSED skip. The other five are present in the query and held constant, so
this page says nothing about what they cost; see
[known limitations](#known-limitations).

> **These are starter reference numbers, not an SLO.** They were taken on one
> machine with one Postgres configuration (below). Your hardware, your
> `shared_buffers`, your backlog shape, and your queue count all move them.
> Reproduce them on your own hardware before designing against them — the
> benchmark is in the repo precisely so you can.

## TL;DR

* **Claim latency scales superlinearly with pending-backlog depth.** 1k → 10k
  rows (10x) costs ~24x latency; 10k → 100k (10x) costs a further ~15x. Claim
  cost is a function of how deep your queue is, not how much work you dispatch.
  **This is the single biggest lever on this page** — bigger than any individual
  predicate, and it dominates the per-gate table below.
* **The cause is structural, not incidental.** The claim query's `ORDER BY`
  leads with a non-indexable `CASE` expression, so `idx_harvest_tq_poll` cannot
  serve the ordering. Postgres sequentially scans and sorts every eligible
  pending row on every single claim. See [the plan](#the-plan) below.
* **Only one predicate is genuinely expensive: per-key concurrency (+590% p50).**
  Build-id routing (+15%), the rate-limit gate (+5%) and the circuit-breaker
  tracked set (+1%) are cheap or free.
* **Paused executions cost as much as live ones (+1321% p50), but not because
  the PAUSED predicate is expensive.** An equal-depth control with *no* PAUSED
  predicate costs the same (+1357%). The anti-join is free; the rows sitting in
  the table are what you pay for. See
  [the control that changed the conclusion](#the-control-that-changed-the-conclusion).
* **Enqueue is not the problem.** ~3 500 rows/s sustained at p50 ~1.8 ms, flat
  from 1k to 100k backlog (7% spread, inside run-to-run noise). At 100k the write
  side sustains ~3 650 rows/s while the read side manages ~3 claims/s — **a queue
  that deep does not drain.**
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

# The CI gate, byte-for-byte as `.github/ci/integration-suites.txt` runs it.
# `claim_budget_tests` is a substring filter, so this runs the whole gate
# module, not just the budget check: the headline scenario, the eight-scenario
# coverage sweep, the 250k-row enqueue cutoff, and the sweep/lease probes.
# ~80s here against a local server; longer in CI, which starts a container.
cargo test -p autumn-harvest --features db --test integration -- \
  claim_budget_tests --test-threads=1

# Just the headline p50-vs-budget check — the one assertion that fails when a
# regression lands. ~50s here.
cargo test -p autumn-harvest --features db --test integration -- \
  claim_budget_tests::claim_p50_at_headline_scenario_is_within_budget \
  --test-threads=1
```

Every table on this page is from **one** run of the first command, on an
otherwise-idle box. Figures that are *not* from that run say so where they
appear: the budget derivation (a distribution over repeated runs), the
reproducibility paragraph (three independent runs), the p50-vs-p99 comparison
(idle and loaded), and the debug-vs-release comparison (two profiles, back to
back).

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

* **Setup sweeps stale benchmark databases**, not just teardown — that is what
  reclaims databases orphaned by a run that panicked, which a teardown hook can
  never do. Only names this harness could itself have minted are ever eligible:
  the full shape `harvest_claim_bench_{pid}_{token}_{seq}`, where `pid` and
  `seq` are decimal and `token` is exactly 16 lowercase hex digits. Sharing the
  prefix is **not** enough — a database of your own called, say,
  `harvest_claim_bench_123_production` fails the token and sequence checks and
  is never a candidate. (The SQL prefilter matches on the prefix, but `_` is a
  single-character wildcard in `LIKE`, so the prefilter is deliberately not the
  authority; every candidate is re-checked against the whole shape before
  anything destructive runs.) The sole liveness authority is the **server**: the
  sweep drops a database only when nothing holds a backend against it, and each
  run keeps one idle connection open for the whole life of its database
  precisely so that question has an answer even between scenarios. That lease
  disables `idle_session_timeout` on its own session: the lease defends the run
  by *being* an idle backend, which is exactly what the reaper introduced in
  PostgreSQL 14 kills, so on a server that sets it the lease would otherwise be
  terminated mid-run and a concurrent sweep would then see the live database as
  abandoned. The override is session-local, needs no privilege, and touches
  nothing else on the server; if it cannot be applied on a 14+ server the run
  warns rather than proceeding quietly, since an unarmed lease is not a lease.
  A local pid
  is deliberately *not* consulted — it cannot answer for a run on another host,
  it cannot answer at all on non-Linux, and two containerised runs on different
  hosts both report pid 1, so treating it as a liveness signal would either veto
  every reclaim or protect nothing. There is one window the server cannot see —
  between `CREATE DATABASE` and the first connection to it, the database exists
  with zero backends and looks abandoned — so setup holds an advisory lock
  across that whole span, and every sweep takes the same lock first. The lock
  lives on its own connection to the `postgres` database rather than on the
  admin connection, because Postgres advisory locks are scoped to the session's
  database, not the cluster: taken on the admin connection, two runs reaching
  one server through different admin databases would not serialize at all. For
  the same reason there is no fallback — **a role that cannot connect to
  `postgres` makes the run refuse to start**, naming the missing grant, rather
  than proceeding with a lock that only coordinates a subset of clients.
  Credentials never reach a log line: the URL is redacted to
  `scheme://***@host:port/db` before it is printed.
* **Each scenario is bounded by a wall clock**, on the write path as well as
  the read path: every pool checkout, claim and enqueue is bounded by the
  scenario deadline, so a stalled server ends the scenario at the ceiling
  instead of hanging the run. A scenario that stops early is marked `⚠` in the
  report and fails the CI gate as unsound rather than publishing a percentile
  over a partial window.
* **`HARVEST_BENCH_SCENARIO_SECS`** caps each scenario's measured phase
  (default 240 s). It is the knob to raise when a row comes back marked `⚠` or
  `‡` — see [measurement hygiene](#measurement-hygiene).

With the variable unset the benchmark starts a `postgres:16` testcontainer
instead; with neither available it prints a skip notice and exits 0.

## Claim latency vs backlog depth

Baseline gate (no build policy, no concurrency key, no rate limit, no pauses),
8 concurrent claimers across 4 queues:

| backlog | n | p50 ms | p99 ms | max ms | claims/s |
|--:|--:|--:|--:|--:|--:|
| 1 000 | 184 | 10.15 | 19.29 | 22.63 | 681 |
| 10 000 | 720 | 208.06 | 267.98 | 308.08 | 25 |
| 100 000 ⚠ | 432 | 2 988.02 | 3 857.67 | 4 192.23 | 3 |

⚠ Cut short by the per-scenario wall-clock budget (180 s for this run; the
default is 240 s, override with `HARVEST_BENCH_SCENARIO_SECS`). The percentiles
describe the 432 claims it did observe. That the scenario *cannot* finish its
planned 800 claims in three minutes is itself the finding.

These rows run at 8 concurrent claimers against a 4-core box, so they measure
the claim path **under contention** — the operational number a worker actually
waits for, not the isolated query cost. The tail columns in particular carry
run-queue scheduling as well as query time; the p50 column is the more stable
comparison, and the per-gate table below deliberately runs below saturation for
the same reason.

**Read this table as a sharding trigger.** A queue that stays around a thousand
pending rows claims in single-digit milliseconds. A queue that sits at ten
thousand claims in roughly a quarter of a second — still workable, but each worker
poll is now a real cost. A queue parked at a hundred thousand pending rows spends
several seconds per claim, and the fleet's throughput collapses to a handful of
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

**`p50 vs` is the attribution statistic**, and it is measured against the row
named in `vs what` — which is not always `baseline`. See
[the control that changed the conclusion](#the-control-that-changed-the-conclusion).

| gate | seeded rows | claimable | n | p50 ms | p50 vs | vs what |
|:--|--:|--:|--:|--:|--:|:--|
| `baseline` | 10 000 | 10 000 | 720 | 106.67 | — | |
| `circuit_breaker_set` | 10 000 | 10 000 | 720 | 107.91 | +1% | `baseline` |
| `rate_limited` | 10 000 | 10 000 | 720 | 111.56 | +5% | `baseline` |
| `build_policy` | 10 000 | 10 000 | 720 | 122.91 | +15% | `baseline` |
| `concurrency_key` ⚠ | 10 000 | 10 000 | 450 | 736.44 | **+590%** | `baseline` |
| `double_backlog` ⚠ | 20 000 | 20 000 | 196 | 1 553.66 | **+1357%** | `baseline` |
| `paused_rows` ⚠ | 20 000 | 10 000 | 215 | 1 516.05 | **−2%** | `double_backlog` |
| `all_gates` ⚠ | 20 000 | 10 000 | 173 | 1 908.90 | **+1690%** | `baseline` |

⚠ Cut short by the per-scenario wall-clock budget; the percentiles describe the
`n` samples shown. A scenario that cannot finish 800 claims in three minutes is
itself a finding.

`double_backlog` is a **control, not a gate** — it seeds no predicate at all.

**How much of this reproduces.** Across four independent runs on the reference
machine, `build_policy` is the only row that repeats to the point: +15%, +13%,
+15%, +13%. `rate_limited` and `circuit_breaker_set` each land within a few
points of zero and have **swapped rank with each other** between runs (one run
put `rate_limited` at −3%, another put `circuit_breaker_set` at −1% — i.e. below
`baseline`), so read them as "free", not as an ordering: the gap between them is
smaller than the noise, and either can measure faster than a claim path that
does strictly less work. `concurrency_key` was +306%, +283%, +590% and +518%;
`paused_rows` vs `baseline` was +1319%, +1346%, +1321% and +1343%. So: the
*classification* into free / modest / expensive is stable, and the expensive
rows are reproducibly expensive, but only `build_policy` and the paused-vs-
baseline figure are reproducible to better than a factor of two. Treat every
percentage here as one significant figure.

The table above is one representative run, not an average — averaging truncated
scenarios with different `n` would be worse than quoting one honestly. The
fourth run's every row fell inside the ranges quoted here, which is the property
that matters: the table is representative, not lucky.

### The control that changed the conclusion

An earlier revision of this page compared `paused_rows` against `baseline`,
measured +1319%, and concluded that the PAUSED-execution skip was the most
expensive predicate on the claim path. That conclusion was wrong, and the
`double_backlog` control is what caught it.

`paused_rows` seeds its PAUSED ballast *in addition to* the claimable backlog,
so it walks a table twice as deep as `baseline`. Claim latency is strongly
superlinear in depth (see [the sweep table](#claim-latency-vs-backlog-depth)),
so any delta measured against `baseline` charges the predicate for the extra
rows too. `double_backlog` removes that confound: same 20 000 total rows, all of
them plain and claimable, **no PAUSED predicate anywhere**.

It costs **+1357%** — slightly *more* than `paused_rows` does. Measured against
that honest comparand the anti-join costs **−2%**, which at n=196/215 truncated
samples is indistinguishable from zero.

So there are two different questions and two different numbers:

| comparison | question it answers | answer |
|:--|:--|--:|
| `paused_rows` vs `baseline` | what does a paused population cost an operator? | **+1321%** |
| `paused_rows` vs `double_backlog` | is the PAUSED anti-join predicate expensive? | **−2%** |

**The operational finding survives; the attribution does not.** Pausing
executions still does not take their work out of the claim path — the rows stay
`PENDING`, every worker still scans them on every claim, and a fleet with a large
paused population still pays roughly the same 14x that an equal number of *live*
rows would cost. But it pays that because the rows are *there*, not because
`NOT EXISTS (… state = 'PAUSED')` is expensive to evaluate. Deleting the
predicate would buy you nothing; draining the rows would buy you everything.

What each row exercises, and what it means:

* **`circuit_breaker_set` (+1%)** — the worker passes a populated tracked-activity
  set, so the rate-limit gate and debit are skipped via `= ANY($5)` (#369).
  Free, as designed.
* **`rate_limited` (+5%)** — rows carry a `rate_limit_key` with a funded bucket,
  exercising the candidate-side `EXISTS` gate and the `rate_limit_debit` CTE
  (#332 / #699). Effectively free at this backlog.
* **`build_policy` (+15%)** — rows carry `required_build_id` and the worker's
  build matches only through a `harvest_build_compat` declaration, forcing the
  `EXISTS` branch rather than the cheap `required_build_id = $3` equality
  (#171). A real but modest cost; safe-deploy ramps (#604) are not expensive.
* **`concurrency_key` (+590%)** — rows carry `concurrency_key` + `concurrency_cap`,
  exercising both the candidate-side `COUNT(*)` subquery and the
  `pg_try_advisory_xact_lock` re-check in the `claimed` CTE (#247). This is
  measured with 256 distinct keys and a cap high enough never to block, so it is
  the *predicate* cost with contention deliberately minimised. **This is the one
  genuinely expensive predicate on the claim path — budget for it.** Its measured
  multiplier is also the least stable on this page: it grows as a run progresses
  (a shorter earlier run reported +306% over 174 samples; this 450-sample run
  reports +590%), which is consistent with the `COUNT(*)` subquery counting a
  `RUNNING` population that the benchmark itself is growing. Read it as
  "expensive and load-dependent", not as a fixed multiplier.
* **`double_backlog` (+1357%)** — the control described above. Not a predicate:
  the cost of doubling table depth, full stop.
* **`paused_rows` (−2% vs the control)** — 10 000 rows belonging to PAUSED
  executions on top of the claimable backlog, exercising the `NOT EXISTS`
  anti-join (#383). Free as a predicate; expensive as table depth.
* **`all_gates` (+1690%)** — every gate at once. Read it as "a deployment using
  all of these features", **not** as a strict upper bound on the claim path: the
  circuit-breaker tracked set short-circuits the rate-limit `EXISTS` and the
  debit CTE (`= ANY($5)` wins, #369), so a deployment with rate limiting and
  *no* breaker executes strictly more work per claim than this row does. Note
  what happens when depth is controlled for: `all_gates` runs at the same 20 000
  rows as `double_backlog`, and against *that* comparand every predicate in the
  engine combined costs **+23%**. At this depth the scan and sort dominate so
  completely that the predicates are a rounding error on top of them.

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
->  Limit (actual rows=1 loops=1)
      Buffers: shared hit=20224
      ->  LockRows (actual rows=1 loops=1)
            ->  Sort (actual rows=1 loops=1)
                  Sort Key: (CASE WHEN ((sticky_worker_id = 'worker-0')
                             AND (sticky_until > now())) THEN 1 ELSE 0 END) DESC,
                            priority DESC, scheduled_at
                  Sort Method: quicksort  Memory: 1400kB
                  Buffers: shared hit=20223
                  ->  Nested Loop Anti Join (actual rows=10000 loops=1)
                        ->  Seq Scan on harvest_task_queue (actual rows=10000 loops=1)
                              Buffers: shared hit=223
                        ->  Index Scan using harvest_queue_pauses_pkey
                              on harvest_queue_pauses qp (actual rows=0 loops=10000)
                              Buffers: shared hit=20000
```

Three things to read here:

1. **`Seq Scan` + `Sort`, not an index scan.** `idx_harvest_tq_poll` is
   `(queue_name, state, priority DESC, scheduled_at) WHERE state = 'PENDING'` —
   exactly the right index for the *filter*, but it cannot serve the *ordering*,
   because the claim query's leading sort key is a `CASE` expression on
   `sticky_worker_id`/`sticky_until` (#235), which is not indexable. One
   non-indexable leading key is enough: the remaining `priority DESC,
   scheduled_at` cannot rescue it. So every claim reads and sorts all eligible
   pending rows to return one. That is the superlinear scaling in the table
   above.
2. **`actual rows=10000` feeding a `Limit 1`.** The plan materialises and sorts
   ten thousand rows in order to return a single task. That ratio — not the
   absolute time — is the shape of the problem, and it is why doubling the
   backlog doubles the work per claim.
3. **`loops=10000` on the queue-pause anti-join.** The `NOT EXISTS` against
   `harvest_queue_pauses` (#619) runs once per candidate row — 20 000 of the
   20 223 buffer hits. It is an index lookup, so it is cheap *per row*, but it is
   paid per row.

One number in the full output is deliberately **not** comparable to the tables
above: this plan's `Execution Time` was 1 625 ms, against a measured p50 of
107 ms for the same scenario. The `EXPLAIN` is a single *cold* claim on a fresh
connection against a freshly-seeded table — precisely the plan-cache and
buffer-cache cost the warmup trim exists to exclude. Read the plan for its
*shape*; read the tables for timing.

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

| backlog | rows | n | p50 ms | p99 ms | max ms | rows/s |
|--:|--:|--:|--:|--:|--:|--:|
| 1 000 | 800 | 720 | 1.70 | 3.19 | 4.70 | 3 668 |
| 10 000 | 800 | 720 | 1.83 | 5.10 | 7.15 | 3 425 |
| 100 000 | 800 | 720 | 1.72 | 4.10 | 5.88 | 3 653 |

Enqueue is **flat in backlog depth** — a 100x deeper queue moves p50 by 0.1 ms
and throughput by 7%, which is inside this box's run-to-run noise. That is the
expected and desired asymmetry: reads pay for depth, writes do not. A
start-storm is bounded by your connection pool and by Postgres write throughput,
not by anything Harvest does.

Put the two sides together and the operational picture is stark: at a 100 000-row
backlog this machine sustains ~3 650 enqueues/s against ~3 claims/s — three
orders of magnitude apart. **A queue that deep does not drain.** Nothing in the
write path warns you about it; the backlog table above is the warning.

Two caveats on this table. `queue::enqueue` is not a bare `INSERT`: it resolves
defaults and writes one row inside its own transaction, so the per-row latency
includes transaction and round-trip overhead — which is exactly why it is worth
measuring rather than assuming. And the throughput column is a **floor**, not a
peak: it divides all rows by the whole wall clock including warmup and task
spawn/join, so the `n` column (post-warmup samples behind the latency columns)
is below the `rows` column by design.

## The CI gate

`claim_budget_tests::claim_p50_at_headline_scenario_is_within_budget` runs via
the `linux` row in `.github/ci/integration-suites.txt` — so, on Linux, on
code-touching changes — and fails the build when **p50** claim latency at the
**headline scenario** (10 000 pending rows, 8 concurrent claimers, 4 queues)
exceeds its budget.

| | |
|:--|:--|
| Statistic | **p50** (see below — deliberately not p99) |
| Reference p50 | 200–234 ms across runs on a quiet reference box; ~516 ms observed on a loaded one |
| Budget | **1 500 ms** (~2.9x the worst observation, ~6.4–7.5x the quiet ones) |
| Override | `HARVEST_CLAIM_BUDGET_MS` |

### Why the gate asserts p50, not p99

The headline scenario runs 8 concurrent claimers against a 4-core box on
purpose — contention is the point of the scenario. But that makes the database
oversubscribed, and an oversubscribed tail measures the run queue rather than
the claim path. Measured across repeated runs on the reference machine:

| statistic | quiet box | loaded box | spread |
|:--|--:|--:|--:|
| p50 | ~200–234 ms | ~516 ms | **~2.3x** |
| p99 | ~300 ms | ~4 665 ms | **~15x** |

A p99 gate at this budget failed **2 runs out of 6** during review, on the same
hardware class and Postgres version that produced the published numbers. It was
not detecting regressions; it was detecting the scheduler. A separate run on a
moderately busy box measured p50 283 ms against a p99 of 1 652 ms at this same
scenario — the p99 alone would have failed a 1 500 ms gate while nothing about
the claim path had changed.

p99 is still measured, still published above, and printed in the gate's failure
message so a genuine tail regression is visible to whoever reads it. It is
simply not the assertion.

**The budget is a cliff detector, not a drift detector.** It will not catch a
predicate that makes claims 50% slower — the reference p50 itself moves 2.3x
with machine load, so no threshold on this hardware could. It catches the kind
of change that adds another per-row subplan to a scan already walking the whole
pending backlog. Being precise about how big that cliff has to be, since the
quiet-box reference spans 200–234 ms and the budget is therefore 6.4–7.5x it:

| regression scale | example | quiet box (200 ms) | quiet box (234 ms) | loaded box (516 ms) |
|:--|:--|:--|:--|:--|
| ~6.9x | a second `concurrency_key`-class subplan | 1 380 ms — **misses** | 1 615 ms — trips | 3 560 ms — trips |
| ~14.6x | doubling the rows every claim walks | 2 914 ms — trips | 3 409 ms — trips | 7 519 ms — trips |

So a depth-class regression trips everywhere, while a single-subplan-class one
trips on a loaded box and at the slow end of the quiet range but can slip
through on the fastest quiet runs. That is inherent rather than a tuning miss:
the reference moves 2.3x with load, so no single threshold separates 6.9x from
noise on this hardware. It also matters less than it looks, because the gate
runs in CI, and CI is the loaded case — 2.9x headroom, where even the smaller
cliff clears the budget by more than 2x. For drift below either cliff, run the
benchmark and compare against the per-gate table above, which runs below
saturation precisely so it can resolve smaller differences.

**The budget was derived on the reference machine, not on a CI runner.** CI
hardware is slower and shared, so the Linux CI runs are the real calibration.
The first such run (2026-08-10, `ubuntu-latest`, Docker-backed Postgres 16)
passed all seven gate tests in 108 s, with the headline scenario itself taking
about 78 s of that — comfortably inside both the 1 500 ms p50 budget and the
240 s per-scenario ceiling. So the budget derived here holds on CI hardware as
published; it has not been widened for it. If a later run proves flaky rather
than catching anything, the fix is to re-derive the number from CI observations
— not to widen it by guesswork and not to delete it; `HARVEST_CLAIM_BUDGET_MS`
exists for the one-off, and the failure message always carries the full stat
line.

Note that the measured stat line is only *printed* when the gate fails: the
manifest runner does not pass `--nocapture`, and Rust's test harness shows
captured output for failing tests only. That is the right default for a gate —
silence means "within budget" — but it does mean CI logs carry no trend data
between failures. Run the benchmark for that.

The gate also asserts five soundness properties, each of which fails loudly
rather than reporting a meaningless percentile:

* **at least 100 samples were collected** — a severe regression could otherwise
  leave the gate defending a two-sample percentile;
* **the run was not truncated** by the wall-clock ceiling — a partial run's
  percentiles describe a shorter, differently-warmed window than the published
  ones, so the gate defends a complete scenario or says so;
* **the scenario finished inside its wall-clock ceiling** (+30 s slack for task
  join). The truncation flag above is only set where the harness *checks* the
  deadline, so it cannot catch an `await` that never returns to a check; this
  assertion measures the clock directly and so does;
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
* **The wall-clock ceiling bounds each claim, not just the loop.** `claim_task`
  is an unbounded `await`, so checking the deadline only between calls would let
  a single stalled claim — exactly the regression or database stall the ceiling
  exists to catch — run for minutes past the advertised cap. Each call is
  wrapped in a timeout derived from the remaining budget; expiry marks the run
  truncated, which the gate treats as "measurement unsound" rather than
  publishing a percentile from a partial run.
* **One deadline for the scenario, established before any claimer starts.**
  Deriving it inside each claimer — after its pool checkout — would restart the
  clock behind an unbounded `await`: a stalled or exhausted pool parks every
  claimer with no deadline yet in existence. The checkout is bounded by the same
  deadline as the claims, and the gate additionally asserts the scenario's
  measured wall clock against that ceiling, because `truncated` is only set
  where the harness *checks* the deadline and so cannot catch an `await` that
  never returns to a check.
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
* **Throughput and latency use different windows, on purpose.** `claims/s` and
  `rows/s` count *every* successful operation over the *whole* wall clock,
  warmup included, because the clock starts at the first warmup call — a
  fraction whose numerator and denominator cover different spans is not a rate.
  The percentile columns exclude warmup, because there the first calls on a
  fresh connection are exactly the unrepresentative ones. The `n` column belongs
  to the latency window, so it is below the operation count the throughput
  column divides.
* **Every worker starts together, and the clock starts with them.** Each claimer
  (or writer) checks out its pooled connection and then waits at a start
  barrier, so a row labelled "8 concurrent claimers" measures eight of them
  contending, not a ramp-up in which the first is already sampling while the
  last is still connecting. The throughput denominator is the span from that
  release to the slowest worker finishing, so pool construction is not counted
  as measured work. This is a *different* clock from the per-scenario ceiling
  below, which deliberately starts *before* checkout — `pool.get()` is an
  unbounded await, so a ceiling that started after it would not bound it.

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
* **Half the claim-path predicates are varied; the other half are not measured
  at all.** The attribution table covers five: build-id routing (#171), per-key
  concurrency (#247), the rate-limit gate (#332/#699), the circuit-breaker
  tracked set (#369) and the PAUSED skip (#383). Five more are present in the
  query on every claim but are never given anything to match, so their subplans
  run against empty or null input and this page reports nothing about their
  cost. Ranked by how much that omission is likely to matter:
  * **Capability labels (#382)** — a `NOT EXISTS` + `jsonb_array_elements`
    subplan, so the one whose real cost is least predictable from the query
    text, and the most defensible next scenario to add. The seed leaves
    `required_capabilities` null.
  * **Queue pauses (#619)** — a `NOT EXISTS` against `harvest_queue_pauses`,
    which the harness only ever `TRUNCATE`s. The predicate is evaluated on every
    claim, but always against an *empty* relation, which is the cheapest path it
    has. Being in the plan is not the same as being measured.
  * **`schedule_to_close` (#378), worker sessions (#606), sticky routing
    (#235)** — cheap inline column tests, against columns the seed leaves null.

  Adding these is scenario work, not query work: each needs a seed variant and a
  report row, on a bench that already runs 15-30 minutes.
* **Queue count is a parameter, but it is not swept.** `Scenario.queues`
  parameterizes how many distinct queues the backlog spreads across, and every
  published row holds it at 4. Backlog depth and claimer count *are* varied.
  Spreading the same backlog over more queues does not obviously help — the
  claim filter is `queue_name = ANY($1)`, so a worker bound to all four still
  scans all four — but that is an expectation, not a measurement.
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
