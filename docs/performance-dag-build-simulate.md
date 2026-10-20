# `DagBuilder::build` and `DagSimulator::run`: negative result

Wall-clock timing is not admissible on the shared vCPU. Every number below is an
instruction count (`valgrind --tool=callgrind`) or an allocation count
(`valgrind --tool=dhat`). The figures are one session's. The proportions are the
portable claim.

## Workload

`benches/dag_simulator_profile.rs` builds a layered DAG of 40 layers of 50
tasks (2,000 tasks). Each task past layer 0 depends on three tasks of the
previous layer. It then calls `DagBuilder::build` and `DagSimulator::run` once.

## Profile (inclusive, 7,647,585 instructions total)

| Share | Cost |
|---|---|
| ~45% | Harness fixture: `DagBuilder::activity`, mostly `short_activity_name` (15.1%) |
| 40.1% | `DagBuilder::build` |
| 16.0% | `Vec<PendingDagTask>::clone` inside `build` |
| 14.9% | `simulate`, which includes dropping the `DagDefinition` |
| 7.9% | `DagSimulator::run` |

Whole program, dhat: 12,066 blocks, 2,254,678 bytes allocated.

## Findings

- `build` clones the pending task list because it takes `&self` and the builder
  stays usable afterwards. `DagTask` owns the same fields, so converting by
  reference would clone them anyway. The clone is inherent to the API.
- `run` allocates one `Vec<TaskStatus>` per task. `run` is 7.9% of the profile,
  so removing that allocation could save about 3%, below the impact floor.
  `DagSimulator` is also a test utility, not a production path.
- The fixture cost (`short_activity_name`) is harness work, not engine work.

No change made. A change to `build` that avoids the clone needs a decision on
the `&self` API shape.

## Reproduce

```text
cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench dag_simulator_profile --no-run
valgrind --tool=callgrind --callgrind-out-file=cg.out <binary>
callgrind_annotate --inclusive=yes cg.out
valgrind --tool=dhat --dhat-out-file=dhat.json <binary>
```
