# agent-loop-hit-rate

Measures how often `autumn_harvest_agent::agent_loop` resumes warm on a
Postgres worker (issue #2007).

A warm decision can resume the parked workflow and replay nothing. The
worker counts each decision as `harvest.workflow.resident_hit` or as
`harvest.workflow.resident_miss{reason}`. This binary installs a recorder
that counts both, runs the agent loop, and prints a table.

The model is offline. It asks for one `lookup` tool call per turn, then
answers. The binary needs no API key and no network.

## Run

Use an empty database. The worker also polls the `default` queue, where the
agent activities run.

```sh
createdb agent_hit_rate
DATABASE_URL=postgres://postgres:postgres@localhost:5432/agent_hit_rate \
  RUNS=20 TURNS=4 cargo run -p agent-loop-hit-rate --release
```

| Variable | Default | Meaning |
|----------|---------|---------|
| `DATABASE_URL` | none | The Postgres database. The binary applies the Harvest migrations. |
| `RUNS` | 20 | The number of agent runs. |
| `TURNS` | 4 | The tool turns of each run. |

## Example output

```text
| Outcome | Decisions | Share |
|---|---:|---:|
| resident hit | 180 | 90.0% |
| miss: `cold` | 20 | 10.0% |
| **total** | 200 | |
```

A run with `TURNS` tool turns makes `2 × TURNS + 2` decisions: the start,
one per model turn and one per tool call. The first decision of each run is
always `cold`.

Recorded results are in
[`docs/rnd/2026-10-11-resident-hit-rate.md`](../../docs/rnd/2026-10-11-resident-hit-rate.md).
