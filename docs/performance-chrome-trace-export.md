# `export_chrome_trace` — return the events without a serde round trip

`trace_export::export_chrome_trace` turns a `DagProfile` into a Chrome trace:
a JSON array of trace events. It built the events as a `Vec<Value>` and
returned `json!(events)`. On a `Vec<Value>`, that macro expands to
`to_value(&events)`. It serialized the finished tree into a second, deep
copy, then dropped the first.

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count (`valgrind --tool=dhat`).
Wall-clock timing is not admissible evidence on this shared-vCPU machine.

**Outcome: shipped (PR #2279).** The harness costs 34.8% fewer instructions
and allocates 35.4% fewer blocks.

## Workload

`benches/dag_tooling_profile.rs` runs the static DAG tooling that authors run
in CI: `DagLinter::analyze`, `DagProfiler::profile` and
`export_chrome_trace`. The DAG has 40 layers of 50 tasks, 2,000 tasks in
all. Each layer reuses the same 50 activity names, and each task after
layer 0 depends on three tasks of the layer before it.

## Profile (before, callgrind, inclusive)

| Stage | Ir | Share |
|---|---:|---:|
| `export` (`export_chrome_trace` and its drop) | 38,684,204 | 68.3% |
| `lint` | 4,549,756 | 8.0% |
| `profile` | 4,087,979 | 7.2% |

Inside `export_chrome_trace`, serde's `collect_seq::<&Vec<Value>>` is 13.5M
`Ir`, 23.8% of the harness. Its dhat site is the top site by block count,
with 16,000 blocks.

## Change

One line in `trace_export.rs`: `json!(events)` becomes
`Value::Array(events)`. The array takes the vector by move, so no copy
exists. The output is identical, and the existing test passes unchanged.

## Measurement

| Counter (tool) | Before | After | Change |
|---|---:|---:|---:|
| Total Ir (callgrind) | 56,634,207 | 36,922,937 | -34.8% |
| `export` inclusive Ir (callgrind) | 38,684,204 | 18,973,220 | -51.0% |
| Blocks allocated (dhat) | 79,009 | 51,008 | -35.4% |
| Bytes allocated (dhat) | 10,563,366 | 7,879,766 | -25.4% |

### Re-measured on `trunk-dev` (2026-10-25)

The same harness ran again on `572c7ff`, with rustc 1.99.0 and valgrind
3.22.0. `trace_export.rs` has not changed since the fix. The allocation
counts are identical, and total `Ir` is 1.1% below the "After" column:

| Counter (tool) | After (PR #2279) | `572c7ff` |
|---|---:|---:|
| Total Ir (callgrind) | 36,922,937 | 36,529,341 |
| Blocks allocated (dhat) | 51,008 | 51,008 |
| Bytes allocated (dhat) | 7,879,766 | 7,879,766 |

## Not addressed

The linter and the profiler are about 8% and 7% of the workload. Neither has
one cost that a local change can remove.

## Reproduce

```text
cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench dag_tooling_profile --no-run
BIN=$(ls -t target/release/deps/dag_tooling_profile-* | grep -v '\.d$' | head -1)
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

For the "before" column, check out `2d9be05` and restore
`autumn-harvest/src/trace_export.rs` from `2d9be05^`. The squash merge holds
the harness and the fix together.
