# Resident-state hit rate — first measurements (issue #2007)

> **Status:** measurement record. No code change follows from it here. It
> answers the question of issue #2007 for two workloads: how often does a
> warm decision resume the parked workflow (#1798)?

## Question

Resident workflow state skips replay only for a narrow set of suspensions.
Two bets of epic #1970 depend on how often real workflows hit that path:

- **Generalize the resident path.** This helps when many misses come from
  joins, races, mutexes or other narrow-path limits.
- **Typed-state snapshots.** This helps when many misses are `cold`: no
  resident state on the worker, as on a first decision, a restart or an
  eviction.

The counters `harvest.workflow.resident_hit` and
`harvest.workflow.resident_miss{reason}` answer it. See
[Resident hit rate](../telemetry.md#resident-hit-rate-issue-2007).

## Method

Both runs use one worker with the defaults: sticky routing on, resident
workflows on, a cache of 1,000 entries. A recorder counts the two counters.
Each decision records one sample.

### Run 1 — e2e bench, `throughput` scenario

The canonical workflow runs three activities in sequence. Each run makes
four decisions: the start and one per activity result. The bench prints the
counts of the measured loop in its notes.

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
  cargo bench -p autumn-harvest --features db,testing --bench e2e_bench
```

### Run 2 — agent loop, `examples/agent-loop-hit-rate`

The binary runs the real `autumn_harvest_agent::agent_loop` with an offline
model. The model asks for one tool call per turn, then answers. A run with
`TURNS` tool turns makes `2 × TURNS + 2` decisions.

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/agent_hit_rate \
  RUNS=20 TURNS=4 cargo run -p agent-loop-hit-rate
```

### Environment

| | |
|:--|:--|
| Date | 2026-10-11 |
| Commit | `bac2d66` (PR #2109) |
| CPUs | 4 logical, cloud container |
| Postgres | 16.15, local, one server |
| Bench profile | `bench` (release) |
| Agent profile | `dev` (debug). The hit rate does not depend on the profile. |

## Results

### Run 1 — e2e bench `throughput`, 1 shard

1,200 measured completions at 31.72 workflows/s.

| Outcome | Decisions | Share |
|---|---:|---:|
| resident hit | 3,600 | 75.0% |
| miss: `cold` | 1,200 | 25.0% |
| **total** | 4,800 | |

### Run 2 — `agent_loop`

| Runs × tool turns | Decisions | Resident hits | Miss: `cold` | Hit rate |
|---|---:|---:|---:|---:|
| 20 × 4 | 200 | 180 | 20 | 90.0% |
| 20 × 12 | 360 | 340 | 20 | 94.4% |

No other miss reason occurred in either run.

## Reading

- **Every decision after the first resumed warm.** Each run had exactly one
  miss, its first decision, and that miss was `cold`. For a run with `n`
  decisions on one worker, the hit rate is `(n − 1) / n`.
- **The narrow path already covers these shapes.** A sequential workflow and
  the sequential agent loop await one command per cycle. The issue predicted
  this for agents, and the data confirms it.
- **For these shapes, the remaining misses are all `cold`.** A wider
  resident path would win nothing here. Typed-state snapshots could reach
  the first decision after a restart or an eviction. A run's very first
  decision has no earlier state to restore.

## Limits

- One worker and no eviction. A fleet with failover, rolling deploys or a
  small cache adds `cold` misses that these runs do not show.
- No joins, races, approvals, conditions or push signal handlers. The agent
  run used no approval policy. An approval with a deadline is a race, so it
  would add `race` misses. The production mix of these shapes needs a
  measurement on real traffic. The counters now make that possible.
- The two workloads are synthetic. Each activity returns at once.

## Next step

Read the counters on a real fleet before you choose between the two bets.
The [dashboard panels](../dashboards/starter-pack-v0.1.0.json) "Resident hit
ratio" and "Resident misses by reason" show both views.
