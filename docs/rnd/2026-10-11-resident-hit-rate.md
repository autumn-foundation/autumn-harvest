# Resident-state hit rate — first measurements (issue #2007)

> **Status:** measurement record. It answers the question of issue #2007
> for two workloads: how often does a warm decision resume the parked
> workflow (#1798)? It also records one finding: the second claim loop of
> #2106 adds cold decisions under load.

## Question

Resident workflow state skips replay only for some suspensions. Two bets
of epic #1970 depend on how often real workflows take that path:

- **Generalize the resident path.** This helps when many replays come from
  joins, races, mutexes or other shapes that the path does not cover.
- **Typed-state snapshots.** This helps when many replays are `cold`: no
  resident state on the worker, as on a first decision, a restart or an
  eviction.

The counter `harvest.workflow.resident{workflow, queue, outcome}` (#2114)
answers it. `outcome` is `hit`, or the reason why the decision replayed
history. See the `harvest.workflow.resident` row in
[`docs/telemetry.md`](../telemetry.md).

## Method

Each run uses one worker with the defaults: sticky routing on, resident
workflows on, a cache of 1,000 entries and two claim loops (#2106). A
recorder counts each `outcome`. Each decision records one sample.

### Run 1 — e2e bench, `throughput` scenario

The canonical workflow runs three activities in sequence. Each run makes
four decisions: the start and one per activity result. The bench prints the
outcomes of its measured loop in its notes. Its recorder returns `false`
from `is_enabled`, so the worker starts no metric sampler. The bench takes
each snapshot of the counts after they stop changing, so a decision attempt
in flight at a phase boundary falls on one side of it.

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
  cargo bench -p autumn-harvest --features db,testing --bench e2e_bench
```

`HARVEST_BENCH_CLAIM_LOOPS=1` runs the one-claim-loop control.

### Run 2 — agent loop, `examples/agent-loop-hit-rate`

The binary runs the real `autumn_harvest_agent::agent_loop` with an offline
model. The model asks for one tool call per turn, then answers. A run with
`TURNS` tool turns makes `2 × TURNS + 2` decisions. The binary sets the step
cap of each run to `TURNS`. It fails if the runs make fewer than
`RUNS × (2 × TURNS + 2)` decisions, and it reports any decisions above that.
Its recorder also returns `false` from `is_enabled`.

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/agent_hit_rate \
  RUNS=20 TURNS=4 cargo run -p agent-loop-hit-rate
```

### Environment

| | |
|:--|:--|
| Date | 2026-10-12 |
| Base | `trunk-dev` at `abdd301`, with #2106 and #2114 |
| CPUs | 4 logical, cloud container |
| Postgres | 16.15, local, one server |
| Bench profile | `bench` (release) |
| Agent profile | `dev` (debug). The hit rate does not depend on the profile. |

## Results

### Run 1 — e2e bench `throughput`, 1 shard, 1,200 measured runs

| Claim loops | Decisions | `hit` | `cold` | Hit rate | Workflows/s |
|---|---:|---:|---:|---:|---:|
| 2 (default) | 4,864 | 3,600 | 1,264 | 74.0% | 43.41 |
| 2 (default), repeat | 4,864 | 3,600 | 1,264 | 74.0% | 43.57 |
| 1 | 4,800 | 3,600 | 1,200 | 75.0% | 33.88 |

The one-loop row comes from a run before the snapshots settled and before
`HARVEST_BENCH_CLAIM_LOOPS` existed: a local edit set
`max_concurrent_claims = 1` in `start_fleet`, as that knob now does. It
counted exactly four decisions per run, so no boundary sample moved. Two earlier
two-loop runs without settled snapshots counted 4,873 and 4,861 decisions.

### Run 2 — `agent_loop`, 20 runs

| Tool turns | Runs of 20 | Decisions | `hit` | `cold` | Hit rate |
|---|---:|---:|---:|---:|---:|
| 4 | 5 | 200 | 180 | 20 | 90.0% |
| 4 | 1 | 201 | 180 | 21 | 89.6% |
| 12 | 1 | 520 | 500 | 20 | 96.2% |

No outcome other than `hit` and `cold` occurred in any run.

## Reading

- **Each run resumes warm after its first decision.** Every run had one
  `cold` first decision and a `hit` for each later decision that it needs.
  For a run with `n` such decisions on one worker, the hit rate is
  `(n − 1) / n`. The only other decisions are the extra ones below.
- **The resident path already covers these shapes.** A sequential workflow
  and the sequential agent loop await one command per cycle.
- **The second claim loop adds cold decisions.** With two claim loops, the
  bench made 64 decisions more than four per run, in both runs, and each
  extra decision was `cold`. With one claim loop, it made exactly four per
  run. The hits do not change, so the extra decisions are replays on top of
  the warm path. At its lower rate, the agent loop made one extra `cold`
  decision in one of six runs.
- **For these shapes, a wider resident path wins nothing.** Typed-state
  snapshots can reach a decision after a restart or an eviction. A run's
  first decision has no state to restore. The extra decisions under two
  claim loops need their cause found first.

## Limits

- One worker and no eviction. A fleet with failover, rolling deploys or a
  small cache adds `cold` replays that these runs do not show.
- No joins, races, approvals, conditions or push signal handlers. The agent
  run used no approval policy. An approval with a deadline is a race.
- The two workloads are synthetic. Each activity returns at once.
- These runs do not show why a second claim loop adds decisions. One
  explanation fits: a second workflow task for a run is claimed while the
  first decision holds the cache entry. The runs do not test it.
- The agent loop ran each 12-turn configuration once.
- A settled snapshot is a quiet interval, not proof of quiescence. A slow
  decision attempt can still cross a phase boundary. The worker runs at most
  eight decisions at once, so each boundary can move at most eight samples.
  That bound is far below the 64 extra decisions.

## Next step

- Find the cause of the extra cold decisions under two claim loops (#2106).
- Read the counter on a real fleet before choosing between the two bets.
