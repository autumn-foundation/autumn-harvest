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

> **Looking for end-to-end numbers?** This page measures the claim and enqueue
> path in isolation. [`benchmarks.md`](benchmarks.md) publishes what the engine
> does end to end — workflows/sec, dispatch and signal latency, replay
> throughput — at 1, 2 and 4 shards, with a one-command reproduction. The two
> are complements: when an end-to-end number there moves, this page is where you
> find out whether the claim path is why.

> **These are starter reference numbers, not an SLO.** They were taken on one
> machine with one Postgres configuration (below). Your hardware, your
> `shared_buffers`, your backlog shape, and your queue count all move them.
> Reproduce them on your own hardware before designing against them — the
> benchmark is in the repo precisely so you can.

## TL;DR

* **Claim latency scales superlinearly with pending-backlog depth.** 1k → 10k
  rows (10x) costs ~19x latency; 10k → 100k (10x) costs a further ~15x. Claim
  cost is a function of how deep your queue is, not how much work you dispatch.
  **This is the single biggest lever on this page** — bigger than any individual
  predicate, and it dominates the per-gate table below.
* **The cause is structural, not incidental.** The claim query's `ORDER BY`
  leads with a non-indexable `CASE` expression, so `idx_harvest_tq_poll` cannot
  serve the ordering. Postgres sequentially scans and sorts every eligible
  pending row on every single claim. See [the plan](#the-plan) below.
* **Only one predicate is genuinely expensive: per-key concurrency (+644% p50).**
  Build-id routing (+13%), the rate-limit gate (+2%) and the circuit-breaker
  tracked set (+4%) are cheap or free.
* **The per-key concurrency predicate (#247) — flagged above as the one
  genuinely expensive gate — has since been fixed.** Its candidate-side gate
  was a correlated `COUNT(*)` subquery, re-evaluated once per candidate row a
  claim visits rather than once per claim. Materializing the `RUNNING` count
  once per claim into a small pre-aggregated CTE cuts total buffers touched
  across a real 10 000-row drain **-99.23%** at the headline scenario
  (1 385 001 432 → 10 727 317). See
  [the concurrency-key gate fix](#the-concurrency-key-gate-fix).
* **Paused executions cost as much as live ones (+1403% p50), and the cost is
  table depth rather than anything specific to pausing.** An equal-*depth*
  control with no PAUSED rows costs the same (+1383%), so the operational
  finding is about rows in the table. That control does **not** isolate the
  anti-join predicate itself — the two scenarios take different query plans —
  so this page does not publish a cost for the predicate in isolation. See
  [the control that changed the conclusion](#the-control-that-changed-the-conclusion).
* **The queue-pause anti-join (#619) — flagged above as accreted-but-unmeasured
  — has since been measured with an actively paused queue, and fixed.** It is
  a different predicate from the PAUSED-*execution* skip above (that one
  checks `harvest_workflow_executions.state`; this one checks
  `harvest_queue_pauses`, an operator-facing "pause this whole queue" switch).
  Pre-filtering it into a small array instead of re-probing it once per
  candidate row cuts buffers touched by a single claim **-98.05%** at the
  headline 10k-backlog scenario (12 743 → 248) with one of four polled queues
  paused. See
  [the queue-pause anti-join fix](#the-queue-pause-anti-join-fix).
* **Enqueue is not the problem.** ~4 800 rows/s sustained at p50 ~1.5 ms, flat
  from 1k to 100k backlog (inside run-to-run noise). At 100k the write side
  sustains ~4 600 rows/s while the read side manages ~3 claims/s — **a queue
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
| 1 000 | 184 | 10.44 | 23.49 | 26.53 | 640 |
| 10 000 | 720 | 200.03 | 239.90 | 283.73 | 29 |
| 100 000 ⚠ | 583 | 2 919.99 | 3 516.16 | 3 658.11 | 3 |

⚠ Cut short by the per-scenario wall-clock budget (180 s for this run; the
default is 240 s, override with `HARVEST_BENCH_SCENARIO_SECS`). The percentiles
describe the 583 claims it did observe. That the scenario *cannot* finish its
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
| `baseline` | 10 000 | 10 000 | 720 | 89.32 | — | |
| `rate_limited` | 10 000 | 10 000 | 720 | 90.68 | +2% | `baseline` |
| `circuit_breaker_set` | 10 000 | 10 000 | 720 | 92.97 | +4% | `baseline` |
| `build_policy` | 10 000 | 10 000 | 720 | 100.65 | +13% | `baseline` |
| `concurrency_key` ⚠ | 10 000 | 10 000 | 657 | 664.49 | **+644%** | `baseline` |
| `double_backlog` ⚠ | 20 000 | 20 000 | 325 | 1 324.88 | **+1383%** | `baseline` |
| `paused_rows` ⚠ | 20 000 | 10 000 | 322 | 1 342.69 | **+1%** | `double_backlog` |
| `all_gates` ⚠ | 20 000 | 10 000 | 257 | 1 694.20 | **+1797%** | `baseline` |

⚠ Cut short by the per-scenario wall-clock budget; the percentiles describe the
`n` samples shown. A scenario that cannot finish 800 claims in three minutes is
itself a finding.

`double_backlog` is a **control, not a gate** — it seeds no predicate at all.

**How much of this reproduces.** Across six independent runs on the reference
machine, `build_policy` is the only row that repeats to the point: +15%, +13%,
+15%, +13%, +12%, +13%. `rate_limited` and `circuit_breaker_set` each land
within a few points of zero and have **swapped rank with each other** between
runs (one run put `rate_limited` at −3%, two put `circuit_breaker_set` at −1% —
i.e. below `baseline`; the sixth reversed them again, +2% against +4%), so read
them as "free", not as an ordering: the gap between them is smaller than the
noise, and either can measure faster than a claim path that does strictly less
work. `concurrency_key` was +306%, +283%, +590%, +518%, +532% and +644%;
`paused_rows` vs `baseline` was +1319%, +1346%, +1321%, +1343%, +1301% and
+1403%. So: the
*classification* into free / modest / expensive is stable, and the expensive
rows are reproducibly expensive, but only `build_policy` and the paused-vs-
baseline figure are reproducible to better than a factor of two. Treat every
percentage here as one significant figure.

The table above is one representative run, not an average — averaging truncated
scenarios with different `n` would be worse than quoting one honestly. Every row
of the fourth and sixth runs fell inside the ranges quoted here, which is the
property that matters: the table is representative, not lucky.

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

It costs **+1383%**, within about a percent of what `paused_rows` costs. So the
operational finding stands on a depth-controlled comparison: at equal table
depth, a paused population costs what a live one does.

#### What that control does *not* establish

It is tempting to read the remaining **+1%** as "the anti-join predicate is
free". That reading does not survive looking at the two plans, and this page
does not make it.

`double_backlog` controls for *total rows in the table*. It does not control for
the population that reaches the sort, and that is where the claim query spends
its time (see [the plan](#the-plan)). The PAUSED anti-join is a `WHERE`
predicate, so it is evaluated *before* the `ORDER BY`:

| | `double_backlog` (control) | `paused_rows` |
|:--|:--|:--|
| rows scanned | 20 000 | 20 000 |
| rows surviving the filter | 20 000 | 10 000 |
| **rows fed to the sort** | **20 000** | **10 000** |
| PAUSED `SubPlan` | **never executed** | executed |

Two consequences. First, the control does not merely lack a PAUSED *population*
— its PAUSED `SubPlan` never runs at all, because every row it seeds is
`task_type = 'activity'` and the guard short-circuits on the type test (the same
mechanism described [at the end of the plan section](#the-plan)). Second, and
more importantly, the two scenarios sort *different numbers of rows*: the
anti-join removes half the table before the sort in `paused_rows` and removes
nothing in the control.

So the +1% is the sum of two effects with opposite signs — the anti-join's probe
cost, minus the sort saving from 10 000 fewer sorted rows — and this
measurement cannot separate them. (On a single-shot `EXPLAIN ANALYZE` the two
scenarios do not even take the same plan: the control gets a sequential scan
with a hash anti-join, `paused_rows` gets an index scan with a merge anti-join
and a hashed subplan.) The *sign* of the +1% also flips between runs — an
earlier run measured the control as the slower of the two, putting the figure at
−1% — which is what you would expect from two effects that roughly cancel,
measured at n=325/322 truncated samples. Do not read the ±1% as a direction, and
do not read it as a predicate cost.

**Isolating the predicate would need a control that matches the post-filter
population, not the pre-filter one** — same 20 000 rows scanned, same 10 000
sorted, excluded by a mechanism cheap enough not to be the thing under test.
That control does not exist yet, so no cost for the predicate in isolation is
published here.

So there are two different questions, and this page answers only one of them:

| comparison | question it answers | answer |
|:--|:--|--:|
| `paused_rows` vs `baseline` | what does a paused population cost an operator? | **+1403%** |
| `paused_rows` vs `double_backlog` | what does that cost at equal table depth? | **+1%** |

The second row is *not* the cost of the anti-join predicate; see the subsection
above for why the comparison cannot support that reading.

**The operational finding survives; the attribution is the part that does not.**
Pausing executions still does not take their work out of the claim path — the
rows stay `PENDING`, every worker still scans them on every claim, and a fleet
with a large paused population still pays roughly the same 15x that an equal
number of *live* rows would cost. That is the depth-controlled result, and it is
the actionable one: **drain the rows.** Whether the `NOT EXISTS (… state =
'PAUSED')` predicate is *additionally* expensive to evaluate is not settled by
these two scenarios, and it is not settled by the `all_gates` row either — that
row carries the same confound, in the same direction. `all_gates` also seeds
PAUSED ballast, so it too sorts 10 000 rows where `double_backlog` sorts 20 000;
comparing the two charges the predicates while *crediting* them with a sort half
the size. The +28% that comparison yields therefore bounds nothing in either
direction. Deriving a direction for it would mean adding the unmeasured
10 000-row sort saving back to the difference, which assumes the difference
decomposes into predicate cost plus sort cost. It does not: the two scenarios
also filter to different post-filter populations and can reach different plans,
so there is neither a measured term to add back nor an established direction for
the bias.

What *is* population-matched is the top of the table. `rate_limited`,
`circuit_breaker_set`, `build_policy` and `concurrency_key` all seed exactly the
10 000 claimable rows `baseline` does, so their deltas are clean — and they do
not support a "predicates are a rounding error" reading either:
`concurrency_key` costs **+644%** on an identical population. The defensible
statement is narrower than the one this page used to make: *depth* is the
dominant cost (~15x from 10k to 20k rows), *one* predicate is separately
expensive at this depth (`concurrency_key`, ~7.4x), and the combined cost of all
of them is not something these scenarios can bound in either direction.

What each row exercises, and what it means:

* **`circuit_breaker_set` (+4%)** — the worker passes a populated tracked-activity
  set, so the rate-limit gate and debit are skipped via `= ANY($5)` (#369).
  Free, as designed.
* **`rate_limited` (+2%)** — rows carry a `rate_limit_key` with a funded bucket,
  exercising the candidate-side `EXISTS` gate and the `rate_limit_debit` CTE
  (#332 / #699). Effectively free at this backlog.
* **`build_policy` (+13%)** — rows carry `required_build_id` and the worker's
  build matches only through a `harvest_build_compat` declaration, forcing the
  `EXISTS` branch rather than the cheap `required_build_id = $3` equality
  (#171). A real but modest cost; safe-deploy ramps (#604) are not expensive.
* **`concurrency_key` (+644%)** — rows carry `concurrency_key` + `concurrency_cap`,
  exercising both the candidate-side `COUNT(*)` subquery and the
  `pg_try_advisory_xact_lock` re-check in the `claimed` CTE (#247). This is
  measured with 256 distinct keys and a cap high enough never to block, so it is
  the *predicate* cost with contention deliberately minimised. **This is the one
  genuinely expensive predicate on the claim path — budget for it.** Its measured
  multiplier is also the least stable on this page: it grows as a run progresses
  (a shorter earlier run reported +306% over 174 samples; this 657-sample run
  reports +644%), which is consistent with the `COUNT(*)` subquery counting a
  `RUNNING` population that the benchmark itself is growing. Read it as
  "expensive and load-dependent", not as a fixed multiplier. **This
  predicate's candidate-side gate has since been fixed** — see
  [the concurrency-key gate fix](#the-concurrency-key-gate-fix).
* **`double_backlog` (+1383%)** — the control described above. Not a predicate:
  the cost of doubling table depth, full stop.
* **`paused_rows` (+1% vs the control)** — 10 000 rows belonging to PAUSED
  executions on top of the claimable backlog, exercising the `NOT EXISTS`
  anti-join (#383). Expensive as table depth. The +1% is *not* the predicate's
  isolated cost — the control sorts 20 000 rows where this scenario sorts
  10 000, so the two effects cancel to an unknown degree; see
  [what that control does not establish](#what-that-control-does-not-establish).
* **`all_gates` (+1797%)** — every gate at once. Read it as "a deployment using
  all of these features", **not** as a strict upper bound on the claim path: the
  circuit-breaker tracked set short-circuits the rate-limit `EXISTS` and the
  debit CTE (`= ANY($5)` wins, #369), so a deployment with rate limiting and
  *no* breaker executes strictly more work per claim than this row does. It is
  reported against `baseline` because there is no comparand that would make a
  depth-controlled reading sound: `all_gates` seeds the same PAUSED ballast
  `paused_rows` does, so it *scans* 20 000 rows like `double_backlog` but
  *sorts* only 10 000. Comparing the two (which yields +28%) charges this row
  for every predicate while crediting it with half the sort, so that figure is
  not interpretable as a bound in either direction and is not quoted as one.
  See [what that control does not
  establish](#what-that-control-does-not-establish).

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

   > **Follow-up (issue #619 fix):** this node describes the *pre-fix* shape of
   > `claim_task_query()`. The scenario above — like every scenario on this
   > page — evaluates the predicate against an always-*empty*
   > `harvest_queue_pauses` (see [known limitations](#known-limitations)), so
   > even this `loops=10000` never ran against a queue that was genuinely
   > paused. [The queue-pause anti-join fix](#the-queue-pause-anti-join-fix)
   > below measures the identical `loops=N` pattern against an *actively
   > paused* queue directly, confirms the mechanism, and replaces the
   > correlated anti-join with a one-time prefilter. Points 1 and 2 above are
   > unaffected by that fix and remain accurate for the current query.

One number in the full output is deliberately **not** comparable to the tables
above: this plan's `Execution Time` was 1 287 ms, against a measured p50 of
89 ms for the same scenario. The `EXPLAIN` is a single *cold* claim on a fresh
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

## The queue-pause anti-join fix

Point 3 above — `loops=10000` on the queue-pause anti-join — describes a real
cost, but every scenario elsewhere on this page evaluates it against an empty
`harvest_queue_pauses` (see [known limitations](#known-limitations)), so it was
flagged, never measured. This section closes that gap with a dedicated harness
variant that seeds an *active* pause on one of the worker's polled queues, then
fixes the predicate the measurement indicts.

**Mechanism.** `claim_task_query()`'s pre-fix anti-join
(`NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp WHERE qp.queue_name =
harvest_task_queue.queue_name)`) is *correlated*: Postgres re-evaluates it once
per candidate row the outer scan visits, not once per claim. The fix replaces
it with a `MATERIALIZED` CTE that reads the (small, low-cardinality) pause
table once per claim into an array, then tests membership with a plain
`<> ALL(...)`:

```sql
paused_queues AS MATERIALIZED (
    SELECT COALESCE(array_agg(queue_name), ARRAY[]::text[]) AS names
    FROM harvest_queue_pauses
    WHERE queue_name = ANY($2)
)
...
CROSS JOIN paused_queues
...
NOT (harvest_task_queue.queue_name = ANY(paused_queues.names))
```

The array is bounded by `$2` — the worker's own polled-queue list, typically
single digits — never a scan of the whole pause table.

**Measurement.** One cold claim (`EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS,
TIMING OFF)`), four polled queues, one of the four actively paused, against the
reference environment above. Full artifacts (before/after `EXPLAIN` at each
backlog depth, plus a `pg_stat_statements` snapshot) are committed under
[`docs/perf-artifacts/queue-pause-claim-anti-join/`](perf-artifacts/queue-pause-claim-anti-join/).

| Backlog | Buffers (before) | Buffers (after) | Δ |
|--:|--:|--:|--:|
| 1 000 | 1 292 | 47 | **-96.36%** |
| 10 000 (headline) | 12 743 | 248 | **-98.05%** |
| 100 000 | 9 008 | 2 251 | **-75.01%** |

At 1k and 10k the mechanism is exactly the one the plan above describes: the
anti-join subnode itself goes from `loops=10000, Buffers: shared hit=12500`
(98.1% of the statement's own buffer cost at 10k) to a CTE evaluated
`loops=1, Buffers: shared hit=5`. At 100k the picture is more interesting:
Postgres's *own* planner already escapes the `loops=N` shape in the **pre-fix**
plan — it switches to a `Merge Anti Join`, since one side (the one-row pause
set) sorts for free — so the anti-join itself is cheap there either way (~2
buffer hits before, ~5 after). The 100k delta instead comes from a secondary,
plan-shape effect the rewrite enables: once the correlated form is gone, the
planner no longer needs *sorted* input from the base table to support a merge,
and switches the base scan from an index scan (`idx_harvest_tq_poll`, 8 983
buffers) to a plain sequential scan (2 223 buffers) — cheaper here because the
table fits comfortably in a handful of large sequential reads. This is reported
honestly rather than folded into "the same mechanism at every scale": the fix
is unambiguously good everywhere measured, but *why* varies with the
scale-dependent plan Postgres already chooses.

**Corroboration.** Wall-clock execution time moved in the same direction as
buffers by more than 2x only at the headline scale (1 372 ms → 123 ms, 11.2x)
— the bar this page's own methodology sets for treating wall-clock as
corroborating evidence. At 1k it improved modestly (2.16 ms → 1.56 ms, well
under 2x) and at 100k it was flat (1 401 ms → 1 421 ms) — the "after" plan
spills *more* to a temp-file external sort at that scale (810 → 1 417 pages
written), because the wider intermediate row (each candidate row now carries
`paused_queues.names` through the join before the anti-join filter narrows it)
costs more to sort even though fewer buffers are touched to produce it.
Buffers, not wall-clock, is what this fix is measured against; the 100k
wall-clock flatness is reported for completeness, not hidden.

**Cumulative, real-claim-loop evidence.** A `pg_stat_statements` snapshot of
7 501 real `claim_task()` calls draining the full 10k-row headline backlog (one
queue paused throughout): total buffers **18 671 000 → 3 773 247**
(**-79.79%**). Lower than the single-cold-claim -98.05% because later claims in
the drain face a shrinking, already-less-pathological candidate set on both
sides of the fix — expected, not a discrepancy.

**Equivalence.** Both before and after runs claim the identical 7 500 of 10 000
rows and never touch the actively paused queue's rows (proven end-to-end by
`tests/integration/queue_pause_tests.rs::claim_query_excludes_paused_queues_end_to_end`,
which also covers resuming the queue). Reproduce with
`autumn-harvest/scripts/queue_pause_claim_perf_repro.sh`, which needs either
`HARVEST_TEST_DATABASE_URL` (an admin connection string) or a reachable Docker
daemon for its testcontainer fallback — not both.

## The concurrency-key gate fix

The `concurrency_key` row above — flagged as "the one genuinely expensive
predicate on the claim path" — was measured, not yet fixed, when this page
first shipped. This section closes that gap: it fixes the predicate the
attribution table indicts and measures the result against the same headline
scenario the +644% figure came from.

**Mechanism.** `claim_task_query()`'s pre-fix per-key concurrency gate
(issue #247) is a *correlated* `COUNT(*)` subquery on the candidate side:

```sql
( concurrency_key IS NULL
  OR concurrency_cap IS NULL
  OR ( SELECT COUNT(*) FROM harvest_task_queue inner_q
       WHERE inner_q.concurrency_key = harvest_task_queue.concurrency_key
         AND inner_q.task_type = harvest_task_queue.task_type
         AND inner_q.state = 'RUNNING'
         AND inner_q.worker_id IS NOT NULL
     ) < harvest_task_queue.concurrency_cap
)
```

Postgres re-evaluates this once per candidate row the outer scan visits, not
once per claim — the same anti-pattern as the queue-pause predicate above, but
here the subquery scans `harvest_task_queue` for `RUNNING` rows sharing the
same key rather than a small operator-facing pause table, so its cost is
*load*-dependent (it grows with the `RUNNING` population), not just
*depth*-dependent. The fix replaces it with two `MATERIALIZED` CTEs, computed
once per claim rather than once per candidate row:

```sql
concurrency_pending_keys AS MATERIALIZED (
    SELECT DISTINCT concurrency_key, task_type
    FROM harvest_task_queue
    WHERE queue_name = ANY($2)
      AND state = 'PENDING'
      AND scheduled_at <= NOW()
      AND concurrency_key IS NOT NULL
      AND concurrency_cap IS NOT NULL
),
concurrency_running_counts AS MATERIALIZED (
    SELECT t.concurrency_key, t.task_type, COUNT(*) AS running_count
    FROM harvest_task_queue t
    WHERE t.state = 'RUNNING'
      AND t.worker_id IS NOT NULL
      AND t.concurrency_key IN (SELECT concurrency_key FROM concurrency_pending_keys)
    GROUP BY t.concurrency_key, t.task_type
)
...
( concurrency_key IS NULL
  OR concurrency_cap IS NULL
  OR COALESCE((
       SELECT rc.running_count FROM concurrency_running_counts rc
       WHERE rc.concurrency_key = harvest_task_queue.concurrency_key
         AND rc.task_type = harvest_task_queue.task_type
     ), 0) < harvest_task_queue.concurrency_cap
)
```

`concurrency_pending_keys` bounds the second CTE's join to only the keys
actually present in the current backlog (never the whole `RUNNING`
population), and both CTEs are evaluated once regardless of how many
candidate rows are later filtered against them. The `claimed` CTE's
authoritative, race-safe recheck — the same correlated `COUNT(*)`, guarded by
`pg_try_advisory_xact_lock(hashtext(candidate.concurrency_key)::bigint)`, run
once on the single winning row after it is already locked — is untouched: the
fix rewrites only the *filtering* pass over many candidate rows, not the
*authoritative* recheck on the one row that wins the claim. Confirmed
byte-for-byte identical before/after: at the hot-contention scale below, both
plans show the identical
`Aggregate (actual rows=1 loops=1) Buffers: shared hit=10` /
`Bitmap Heap Scan ... Heap Blocks: exact=8` subtree for that recheck node.

**Measurement.** `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)`
of a single cold claim, isolated to the concurrency-check subtree specifically
— the part of the plan this fix changes — against the reference environment
above. Full artifacts (before/after `EXPLAIN` at each `BACKLOG_SWEEP` depth,
plus a hot-contention variant and a `pg_stat_statements` snapshot) are
committed under
[`docs/perf-artifacts/concurrency-key-claim-predicate/`](perf-artifacts/concurrency-key-claim-predicate/).

| Scenario | Buffers, concurrency-check subtree (before) | Buffers (after) | Δ |
|:--|--:|--:|--:|
| idle, backlog=1 000 | 1 000 (`loops=1000`) | 1 (CTE never executed; the `RUNNING`-side probe short-circuits) | **-99.9%** |
| idle, backlog=10 000 (headline) | 10 000 (`loops=10000`) | 1 (never executed) | **-99.99%** |
| idle, backlog=100 000 | 100 000 (`loops=100000`) | 1 (never executed) | **-99.999%** |
| hot contention (10 000 backlog + 2 000 `RUNNING` rows spread across the same ~256 keys) | 98 124 (`loops=10000`) | 733 (`concurrency_pending_keys`: 333 + `concurrency_running_counts`: 400, both `loops=1`) | **-99.25%** |

At every idle depth, Postgres's own planner *lazily skips*
`concurrency_pending_keys` entirely — the plan reports it `(never executed)`
— because the cheaper `concurrency_running_counts` probe returns 0 rows first
via `COALESCE(..., 0)`, and nothing in an idle backlog has a `RUNNING` peer to
check. The pre-fix plan has no equivalent short-circuit: the correlated
subquery runs once per candidate row regardless of whether any `RUNNING` row
could possibly exist, so its buffer cost tracks the loop count 1:1 (1 000 →
10 000 → 100 000, exactly matching backlog depth). Under hot contention, where
the short-circuit can't fire (`concurrency_pending_keys` materializes
`actual rows=256 loops=1`), the pre-fix candidate-side aggregate instead costs
98 124 buffers across its 10 000 loops — each of those 10 000 independent
per-row probes now does real index-scan work against the populated `RUNNING`
rows — where the fixed version pays that cost exactly once, covering all 256
distinct keys in one pass.

A pre-existing external-merge sort spill at the 100 000-row idle depth
(`Sort Method: external merge  Disk: 18384kB`) is present, at the identical
disk size, in both the before and after plans — this fix does not introduce or
change it (see issue #1215, which targets a different part of the query).
Likewise the base-table scan feeding the `ORDER BY … LIMIT` is unchanged in
shape between before and after at every depth (see issue #1177) — this fix
touches only the concurrency-check predicate, not the candidate
ordering/pushdown. The specific scan type at the 100 000-row depth can itself
vary *across* separate script runs (an earlier run captured a `Seq Scan`
where this run captured an `Index Scan using idx_harvest_tq_poll`, purely
from `ANALYZE` statistics/row-layout differences between fresh fixture
builds — the same class of variance noted for the cumulative buffer count
below); what stays constant within a single run's before/after pair, and
what this claim actually depends on, is that the *before* and *after* halves
of the same run always match each other.

**Corroboration.** Wall-clock execution time for a *single* cold claim is
**not** admissible corroboration at idle scale: it is flat to slightly worse
at backlog=10 000 (116.7 ms → 143.4 ms) and backlog=100 000 (1 629.0 ms →
1 855.9 ms), neither >2x nor in the same direction as the buffer win — the
concurrency-check subtree shrinks from tens/hundreds of thousands of buffers
to essentially nothing, but that subtree is a small fraction of a single
query's total cost at idle scale, where the `ORDER BY`/sort work
[the plan](#the-plan) already identifies as the dominant cost still
dominates. Buffers, not wall-clock, is what the idle-scale rows above are
measured against; this page does not claim a single-query wall-clock win it
did not observe.

At hot contention, wall-clock moves the same direction as buffers by more
than 2x — the bar this page's own methodology sets for treating it as
corroborating evidence: **1 573.3 ms → 311.5 ms (5.05x)**, alongside the
-99.25% buffer reduction above.

**Cumulative, real-claim-loop evidence.** A `pg_stat_statements` snapshot of
10 001 real `claim_task()` calls draining the full 10 000-row headline
backlog (the same `ClaimGate::ConcurrencyKey` scenario the +644% p50 figure
was measured under — 256 distinct keys, cap high enough never to block):
total buffers **1 385 001 432 → 10 727 317** (**-99.23%**, 129.1x fewer
buffers for the identical drain). Far larger than the single-cold-claim
reduction above, because the pre-fix cost *compounds* across the drain: each
of the 10 000 sequential `claim_task()` calls independently re-scans the
remaining candidate backlog through its own correlated subquery, so the total
work across a full drain grows roughly with the *square* of backlog depth
(10 000 + 9 999 + … candidate-row evaluations, each paying its own per-row
subquery cost), where the fix's per-call cost stays flat — bounded by
distinct-key cardinality and the `RUNNING` population, not by the shrinking
remaining backlog.

The absolute *before* buffer count is sensitive to physical row/page layout
after `ANALYZE`: an earlier run of this same script measured
**2 492 987 808 → 10 938 903** on an identically-shaped fixture, roughly 1.8x
higher on the *before* side than the **1 385 001 432** figure above, because
the correlated subquery's cost depends on how `RUNNING`/`PENDING` rows happen
to land on disk, while the fixed version's flat, bounded CTE cost barely
moved between runs (10 938 903 → 10 727 317, -1.9%). The *relative* reduction
is what to trust across reruns: both landed in the same -99%/100x+ regime,
comfortably clearing this page's impact floor either way; reproduce it
yourself rather than pinning to either absolute number.

**Equivalence.** Both before and after runs claim the identical
10 000-of-10 000 claimable rows (`claimed=10000 of 10000 claimable` in both
`{before,after}-fixture-summary.txt`), with identical seeded/claimable row
counts at every swept depth and an identical hot-contention seed shape
(`running_rows_added=2000` in both). `calls=10001` matches exactly between
before and after in the `pg_stat_statements` snapshot — the fix changes
per-call cost, not the number of claim attempts needed to drain the backlog.
Correctness of the underlying enforcement is unchanged and covered by
`tests/integration/integration_e2e.rs`'s existing
`concurrency_cap_limits_concurrent_claims_cluster_wide`,
`concurrency_cap_shared_key_budget_is_not_doubled`,
`concurrency_cap_failure_frees_slot_and_does_not_wedge_queue`,
`concurrency_cap_null_key_tasks_are_unaffected_by_saturated_key` and
`per_key_concurrency_cap_enforced_across_fleet` (unmodified by this change; all
five pass unchanged against the fixed query). Two new unit tests —
`queue::tests::claim_query_concurrency_gate_matches_the_authoritative_recheck`
and
`queue::tests::concurrency_gate_ctes_are_defined_and_referenced_exactly_once_each`
— pin the query's shape directly. Reproduce with
`autumn-harvest/scripts/concurrency_key_claim_perf_repro.sh`, which needs
either `HARVEST_TEST_DATABASE_URL` (an admin connection string) or a reachable
Docker daemon for its testcontainer fallback — not both.

### Known limitation: cost scales with distinct concurrency-key cardinality

The committed hot-contention fixture above uses 256 distinct concurrency
keys — the harness's fixed `KEY_CARDINALITY` constant
(`tests/integration/claim_bench_support.rs`) — and at that cardinality the
fix is unambiguously a win (733 buffers vs 98 124, -99.25%). That number does
not generalize to arbitrarily many distinct keys, and this is a real,
confirmed limit of the fix, not a hypothetical one.

The candidate-side gate is a correlated scalar subquery —
`COALESCE((SELECT rc.running_count FROM concurrency_running_counts rc
WHERE rc.concurrency_key = … AND rc.task_type = …), 0) < concurrency_cap` —
evaluated once per candidate row. `concurrency_running_counts` is a
`MATERIALIZED` CTE, and a CTE has no index: PostgreSQL always resolves a
lookup against one with a linear `CTE Scan`, regardless of `MATERIALIZED` or
the CTE's own size. The committed
`after-claim-backlog-10000-hot-contention.explain.txt` already shows this
node — `CTE Scan on concurrency_running_counts rc (loops=10000)`, filtering
out 255 of 256 rows on every one of the 10 000 loops — it just costs little
enough at 256 keys (≈1-2 pages, fully cached) to stay invisible in the
subtree-total table above. Re-running the same shape with 5 000 distinct
keys instead of 256 (all other parameters held fixed: 10 000-row backlog,
2 000 `RUNNING` rows, `NON_BLOCKING_CAP`) reproduces a **1 600 ms** single
claim, worse than the pre-fix baseline's own hot-contention wall-clock
(1 573.3 ms, see above) — the fix's own per-candidate-row `CTE Scan` becomes
the dominant cost once distinct-key cardinality is large enough. The
underlying shape is O(candidate rows × distinct running key/type pairs);
256 keys keeps that product small, thousands of keys does not.

Three pure query rewrites were evaluated as replacements for the candidate-
side gate and rejected, each confirmed by `EXPLAIN (ANALYZE, BUFFERS,
VERBOSE, SETTINGS)` against a from-scratch reproduction of both the idle
(10 000-row, 256-key, zero `RUNNING`) and high-cardinality (5 000-key)
scenarios:

- **`LEFT JOIN` to a `GROUP BY`-aggregated running-count subquery.** Turns
  the per-row `CTE Scan` into a single hash lookup: 5 000-key hot-contention
  cost drops to 24.1 ms (66x faster than the correlated form, and fewer
  buffers: 377 vs 713). But it regresses the *idle* case from ~1-4 buffers
  (the correlated form's lazy `(never executed)` short-circuit) to ~232
  buffers at a 10 000-row idle backlog, because any `LEFT JOIN`-shaped
  formulation defeats PostgreSQL's ability to push `ORDER BY … LIMIT`
  through the ordered `idx_harvest_tq_poll` scan feeding the candidate
  selection.
- **`LEFT JOIN LATERAL`** — the same regression, same magnitude (idle-depth
  buffers move from ~1-4 to ~230, scaling linearly with backlog depth from
  there).
- **`LEFT JOIN LATERAL` with `enable_hashjoin`/`enable_mergejoin` disabled**
  (forcing a Nested Loop), and again with `enable_seqscan`/`enable_bitmapscan`
  also disabled (forcing the planner onto `idx_harvest_tq_poll` — the same
  index whose ordering the correlated form exploits) — neither recovers the
  idle-case short-circuit. Even driven by a plain, ordered `Index Scan` on
  the outer side, PostgreSQL still inserts an explicit `Sort` and evaluates
  the full joined result before `LIMIT` applies (buffers actually rose to
  916, since the plain Index Scan touches every leaf and heap page the
  Bitmap/Seq Scan alternatives could skip). This is not a planner-tuning
  gap: PostgreSQL cannot apply `LIMIT`-pushdown-through-ordered-scan to a
  join whose filter references the joined side, independent of which
  physical join algorithm is chosen — only a scalar subquery evaluated
  lazily in the outer `WHERE` clause gets that optimization, and a CTE
  cannot back one with an index.

The one formulation that would plausibly get both properties — a correlated
subquery in the same shape as the *authoritative* recheck in the `claimed`
CTE below (which already scans the base table, not a CTE, and is cheap
because it runs at most `LIMIT`-many times, not once per candidate) — needs
a new supporting partial index (e.g. on
`(concurrency_key, task_type) WHERE state = 'RUNNING' AND worker_id IS NOT
NULL`) to make each per-candidate-row lookup an indexed probe instead of a
CTE linear scan. Adding an index is outside what this PR changes
unilaterally; see the review discussion on this PR for the concrete proposal
and open question.

**That proposal was measured and killed:**
`docs/assays/0003-concurrency-gate-cardinality-index.md` (ledger #3) found
the partial-index rewrite fixes this exact 5,000-key blowup (~48.8x faster
than control) without regressing the 256-key case, but at zero `RUNNING`
rows it costs ~10,000 real per-candidate-row index probes where the current
fix costs ~10,000 near-free probes of a small, resident, empty CTE — 200x+
over its pre-set idle-cost line, at any key cardinality. Re-assaying this
exact formulation without new information is a re-dig; see that report for
what else remains untested. Until a fix clears all three of that assay's
lines, deployments with concurrency-key
cardinality in the low hundreds (the tested, committed range) get the full
measured win above; deployments with concurrency keys numbering in the
thousands or more should expect the candidate-side gate's cost to grow with
that cardinality and are not covered by this fix's evidence.

## Enqueue throughput

8 concurrent writers enqueueing into an already-populated queue:

| backlog | rows | n | p50 ms | p99 ms | max ms | rows/s |
|--:|--:|--:|--:|--:|--:|--:|
| 1 000 | 800 | 720 | 1.61 | 3.32 | 3.86 | 4 540 |
| 10 000 | 800 | 720 | 1.39 | 3.46 | 4.22 | 5 122 |
| 100 000 | 800 | 720 | 1.58 | 3.14 | 5.42 | 4 647 |

Enqueue is **flat in backlog depth** — a 100x deeper queue moves p50 by 0.2 ms,
and the throughput spread across the sweep is inside this box's run-to-run
noise (the *middle* backlog measured fastest here, and the deepest beat the
shallowest — throughput does not order by depth at all, which is the tell that
the variation is noise rather than depth). That is the expected and desired
asymmetry: reads pay for depth, writes do not. A
start-storm is bounded by your connection pool and by Postgres write throughput,
not by anything Harvest does.

Put the two sides together and the operational picture is stark: at a 100 000-row
backlog this machine sustains ~4 600 enqueues/s against ~3 claims/s — three
orders of magnitude apart. **A queue that deep does not drain.** Nothing in the
write path warns you about it; the backlog table above is the warning.

Two caveats on this table. `queue::enqueue` is not a bare `INSERT`: it resolves
defaults and writes one row inside its own transaction, so the per-row latency
includes transaction and round-trip overhead — which is exactly why it is worth
measuring rather than assuming. And the throughput column is a **floor**, not a
peak: it divides *all* rows — warmup included — by the shared
barrier-to-completion window defined under [Measurement
hygiene](#measurement-hygiene), which ends at the *slowest* writer, so a writer
that finishes early still counts toward the denominator. Task spawn and join sit
outside that window by construction, so read this as a floor on sustained
throughput, not as an end-to-end figure. The `n` column (post-warmup samples
behind the latency columns) is below the `rows` column by design.

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
| ~7.4x | a second `concurrency_key`-class subplan | 1 488 ms — **misses** | 1 741 ms — trips | 3 839 ms — trips |
| ~14.6x | doubling the rows every claim walks | 2 914 ms — trips | 3 409 ms — trips | 7 519 ms — trips |

So a depth-class regression trips everywhere, while a single-subplan-class one
trips on a loaded box and at the slow end of the quiet range but can slip
through on the fastest quiet runs. That is inherent rather than a tuning miss:
the reference moves 2.3x with load, so no single threshold separates 7.4x from
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
whole transaction**, not the single statement the `EXPLAIN` below shows — and not
a single round trip either. It issues, in order:

1. `BEGIN ISOLATION LEVEL READ COMMITTED`. The level is pinned on the `BEGIN`
   itself rather than inherited, so step 4 always gets a fresh snapshot.
2. **The claim CTE.** The rate-limit debit and the per-key concurrency
   advisory-lock re-check are branches *within* this statement, not extra ones —
   this is the statement the `EXPLAIN` below plans. With cross-region DR fencing
   enabled (issue #954, off by default) it carries one additional
   `MATERIALIZED` CTE probing `harvest_shard_generation` — still the same single
   statement and the same round-trip count, and the published figures below were
   measured with fencing off, which is the default and the configuration the
   plan applies to.
3. *(hit only)* `queue_pause::try_lock_queue_for_claim` — a
   `pg_try_advisory_xact_lock` on the queue. If it loses the race against a
   concurrent pause or resume, `queue_pause::release_claim` hands the row back
   and the call returns "no task": same round-trip count, no claim.
4. *(hit only)* `queue_pause::release_claim_if_queue_paused` — the authoritative
   queue-pause re-check. It is a *separate statement* precisely so it takes a
   snapshot the claim could not have; folding it into the CTE would defeat it.
5. `COMMIT`.

So a published number is **five** client↔server round trips when the claim lands
on a row and **three** when the queue is empty — plus transaction overhead — and
the `EXPLAIN` plan explains the *dominant* statement rather than the whole
measured operation. The seeded scenarios claim at most a fifth of the backlog
they seed, so their samples are overwhelmingly hits.

That distinction is the first thing to reason about when moving this workload to
a **remote** database: the round-trip count, not the query plan, is what network
latency multiplies. Five round trips at 1 ms of network RTT is 5 ms of floor per
claim that no amount of index tuning removes. Every number on this page was
measured against a loopback server, so that floor is ~0 here and the plan
dominates; that ordering inverts across a network.

Measuring the whole call is the right thing to do — it is what a worker actually
waits for — but it means these numbers are not directly comparable to a bare
`EXPLAIN ANALYZE` of the claim query.

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
  last is still connecting. The throughput denominator is **one** span shared by
  every worker — earliest resume after the barrier through the last completion —
  so pool construction is not counted as measured work and no worker's
  release-to-resume delay escapes the denominator. Timing each worker from its
  own resume and taking the widest of those would drop exactly that delay while
  keeping all of its claims in the numerator, reporting a rate the run never
  achieved; with more workers than runtime threads most of them are not polled
  at release, so the effect is not marginal. This is a *different* clock from
  the per-scenario ceiling below, which deliberately starts *before* checkout —
  `pool.get()` is an unbounded await, so a ceiling that started after it would
  not bound it.

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
  * **Capability labels (#382)** — measured directly:
    [`docs/performance-capability-labels.md`](performance-capability-labels.md) seeds `required_capabilities`
    (rather than leaving it null) and finds a real, +24–36% buffer cost on the
    claim query across the same backlog-depth sweep used everywhere else on
    this page, corroborated three independent ways (`EXPLAIN` buffers,
    `pg_relation_size` row-width growth, and an aggregate `pg_stat_statements`
    drain). The mechanism is heap-page growth from the wider stored JSONB
    payload, not a plan inefficiency — no query-shape fix applies; see that
    page for the full measurement and why.
  * **Queue pauses (#619)** — the attribution-table sweep above still only ever
    `TRUNCATE`s `harvest_queue_pauses`, so it still says nothing about this
    predicate's cost on its own. A dedicated harness variant that actively
    pauses a queue closed that specific gap and, as a direct result, replaced
    the correlated anti-join with a one-time prefilter — see
    [the queue-pause anti-join fix](#the-queue-pause-anti-join-fix).
  * **`schedule_to_close` (#378)** — measured directly:
    [`docs/performance-schedule-to-close.md`](performance-schedule-to-close.md) seeds `schedule_to_close_at`
    (rather than leaving it null) and **confirms this page's own suspicion on
    magnitude, but not on mechanism**: a small, real buffer cost (+2.6% to
    +7.5% across the three published backlog depths), corroborated by two
    standalone MVCC-bloat scripts, one bulk and one per-row (+5.2% both) —
    nowhere near the 20% impact floor, so no fix is proposed. Codex review
    caught that the predicate text alone (a plain inline column test) is not
    the whole story: `harvest_task_queue` carries a partial index on this
    column for the timeout scanner, and the claim `UPDATE` writes a new
    entry to it for every `schedule-to-close` row — a fixed, depth-independent
    +1 dirtied/+1 written page at every backlog depth tested, additive with a
    separate row-width effect on the candidate scan that *does* scale with
    depth. Review also caught that the harness's first seeded deadline gave
    every row the byte-identical value, letting B-tree deduplication
    understate the index's real growth by roughly 3x — fixed by seeding a
    distinct, per-row deadline instead. See that page's "Plan" and
    "Write-side cost" sections for the buffer- and storage-level evidence.
    One thing did **not** reproduce cleanly across this pass's several
    capture runs: the real 10,001-call `pg_stat_statements` drain's
    aggregate delta varied noticeably run to run (always positive, never
    converging on one value), but only the most recent run's artifacts are
    ever committed -- the repro script overwrites the same canonical
    filenames each time -- so that page states only the one auditable,
    committed number (**+15.5%**) rather than citing bounds from runs whose
    evidence no longer exists in the repository to audit. A markedly more expensive plan
    at the 100,000-row depth was also observed on earlier, pre-fix runs of
    this capture (never on the unpopulated side) but not on either
    fully-fixed run; that page's "100,000-row plan choice" section explains
    why it does not assert a frequency for this, including why an earlier
    revision's "N of M runs" framing had to be walked back once those
    runs' artifacts were no longer available to audit.
  * **Worker sessions (#606), sticky routing (#235)** — still cheap inline
    column tests, against columns the seed leaves null; not yet measured.

  Adding one of these is scenario work, not query work: each needs a seed
  variant and a report row, on a bench that already runs 15-30 minutes.
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
* `docs/perf-artifacts/queue-pause-claim-anti-join/` — committed before/after
  `EXPLAIN`/`pg_stat_statements` evidence for
  [the queue-pause anti-join fix](#the-queue-pause-anti-join-fix).
* `autumn-harvest/scripts/queue_pause_claim_perf_repro.sh` — regenerates that
  evidence from a clean checkout.
* `docs/perf-artifacts/concurrency-key-claim-predicate/` — committed
  before/after `EXPLAIN`/`pg_stat_statements` evidence for
  [the concurrency-key gate fix](#the-concurrency-key-gate-fix).
* `autumn-harvest/scripts/concurrency_key_claim_perf_repro.sh` — regenerates
  that evidence from a clean checkout.
* [`docs/performance-capability-labels.md`](performance-capability-labels.md) — the capability-labels claim
  predicate (#382) measurement referenced above.
* `docs/perf-artifacts/capability-labels-claim-predicate/` — committed
  `EXPLAIN`/`pg_stat_statements` evidence for that measurement.
* `autumn-harvest/scripts/capability_labels_claim_perf_repro.sh` — regenerates
  that evidence from a clean checkout.
* [`docs/performance-schedule-to-close.md`](performance-schedule-to-close.md) — the `schedule_to_close_at` claim
  predicate (#378) measurement referenced above.
* `docs/perf-artifacts/schedule-to-close-claim-predicate/` — committed
  `EXPLAIN`/`pg_stat_statements`/heap-growth evidence for that measurement.
* `autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh` — regenerates
  that evidence from a clean checkout.
* [`docs/performance-history-ceiling.md`](performance-history-ceiling.md) — a separate scanner, not part of
  `claim_task_query()`: the workflow-history-ceiling check
  (`timeout::enforce_workflow_history_ceiling`, issue #493) fixed a
  correlated `harvest_events` event-count subquery that was evaluated twice
  per RUNNING execution on every timeout-scanner tick.

### Other profiling notes

Instruction/allocation-count profiling passes over other hot paths, each a
standalone note rather than part of the claim-path attribution table above:

* [`docs/performance-replay.md`](performance-replay.md) — `WorkflowReplayer`'s
  in-memory replay path against issue #135's CPU-path budget; shipped fix.
* [`docs/performance-verify.md`](performance-verify.md) —
  `ReplayVerifier::verify_dir`'s opaque-payload guard fast-path; shipped under
  maintainer override after falling short of the autonomous gate.
* [`docs/performance-schema-validation-lazy-path.md`](performance-schema-validation-lazy-path.md)
  — lazy JSON-Pointer path construction in schema validation (issue #373).
* [`docs/performance-det-check.md`](performance-det-check.md) — fusing a
  redundant per-line comment scan in `harvest det-check` (issue #778).
* [`docs/performance-dag-graph.md`](performance-dag-graph.md) — hoisting a
  per-node rebuild out of `GET /dag-run-graph` (issue #690).
* [`docs/performance-dlq-aggregate.md`](performance-dlq-aggregate.md) — DLQ
  aggregate grouping (issue #385/#613); a measured fix that was reverted after
  review found a regressing input shape — a negative result.
* [`docs/performance-dlq-merge.md`](performance-dlq-merge.md) — the DLQ
  cross-shard merge stage that runs after the grouping above; redundant key
  clones removed.
* [`docs/performance-stall-diagnosis.md`](performance-stall-diagnosis.md) — an
  allocation-free ranking pass over `GET /api/harvest/workflows/{id}/diagnose`
  (issue #809).
* [`docs/performance-workflow-children-traversal.md`](performance-workflow-children-traversal.md)
  — batching the N+1 in `GET /workflows/{id}/children?depth=N` (issue #786-adjacent).
* [`docs/performance-schedule-overdue-aux.md`](performance-schedule-overdue-aux.md)
  — the same N+1 shape in `GET /admin/schedules`'s overdue-aux computation
  (issue #696).
* [`docs/performance-quota-history-bytes.md`](performance-quota-history-bytes.md)
  — measuring the `history_bytes` admission check's cost claim (issue #946
  AC7); partially inaccurate claim, no fix identified.
* [`docs/performance-codec-rotation-reencrypt.md`](performance-codec-rotation-reencrypt.md)
  — skipping a JSON round-trip in the codec-key-rotation re-encryption sweep
  (issue #948).
* [`docs/performance-sqlite-runtime-drive.md`](performance-sqlite-runtime-drive.md)
  — the first profiling harness for `autumn-harvest-sqlite`; findings only, no
  local fix cleared the floor.
