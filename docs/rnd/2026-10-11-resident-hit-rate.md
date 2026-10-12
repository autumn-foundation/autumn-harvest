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
from `is_enabled`, so the worker starts no metric sampler.

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
  cargo bench -p autumn-harvest --features db,testing --bench e2e_bench
```

### Run 2 — agent loop, `examples/agent-loop-hit-rate`

The binary runs the real `autumn_harvest_agent::agent_loop` with an offline
model. The model asks for one tool call per turn, then answers. A run with
`TURNS` tool turns makes `2 × TURNS + 2` decisions. The binary sets the step
cap of each run to `TURNS`. It fails if the decision total differs from
`RUNS × (2 × TURNS + 2)`.

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
| 2 (default) | 4,873 | 3,600 | 1,273 | 73.9% | 47.91 |
| 2 (default), repeat | 4,861 | 3,600 | 1,261 | 74.1% | 41.37 |
| 1 | 4,800 | 3,600 | 1,200 | 75.0% | 33.88 |

### Run 2 — `agent_loop`, 20 runs

| Tool turns | Decisions | `hit` | `cold` | Hit rate |
|---|---:|---:|---:|---:|
| 4 | 200 | 180 | 20 | 90.0% |
| 12 | 520 | 500 | 20 | 96.2% |

The 4-turn run gave the same counts in four repeats. No outcome other than
`hit` and `cold` occurred in any run.

## Reading

- **Each run resumes warm after its first decision.** Every run had one
  `cold` first decision and a `hit` for each later decision. For a run with
  `n` decisions on one worker, the hit rate is `(n − 1) / n`.
- **The resident path already covers these shapes.** A sequential workflow
  and the sequential agent loop await one command per cycle.
- **The second claim loop adds cold decisions under load.** With two claim
  loops, the bench made 61 to 73 decisions more than four per run, and each
  extra decision was `cold`. With one claim loop, it made exactly four per
  run. The hits do not change, so the extra decisions are replays on top of
  the warm path. The agent loop shows no extra decisions at its lower rate.
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
- An earlier 4-turn run counted 21 cache misses, one more than its runs.
  Its outcome table was not kept. The four later runs matched exactly.

## Next step

- Find the cause of the extra cold decisions under two claim loops (#2106).
- Read the counter on a real fleet before choosing between the two bets.
