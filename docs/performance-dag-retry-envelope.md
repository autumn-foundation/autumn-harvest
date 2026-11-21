# `DagRetryEnvelope::analyze` — total without an attempt list

`DagRetryEnvelope::analyze` called `retry_envelope::analyze` for every task.
That call builds a `Vec<AttemptWindow>` and scans for the saturation point.
The DAG analysis reads only `configured_attempts` and `worst_case_total()`.

Every number is a deterministic count from `valgrind`. Wall-clock timing is
not admissible evidence on this machine.

## Workload

`benches/dag_retry_envelope_profile.rs`: 2000 tasks (40 layers of 50), three
upstreams each, every jitter mode, 3 to 10 attempts, 50 repetitions.

```sh
cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench dag_retry_envelope_profile --no-run
RETRY_ENV_REPS=50 valgrind --tool=callgrind <binary>
RETRY_ENV_REPS=50 valgrind --tool=dhat <binary>
```

## Profile before the change

| Entry | Inclusive Ir | Share |
|---|---|---|
| `DagRetryEnvelope::analyze` | 341.3 M | 71.9% |
| `retry_envelope::analyze` | 247.4 M | 52.1% |
| `compute_retry_delay` (self) | 84.2 M | 17.7% |
| `_int_malloc` (self) | 62.3 M | 13.1% |

## Change

`retry_envelope::worst_case_without_deadline` folds the latest finish with a
loop and no `Vec`. It skips the `saturates_at` scan. A test proves that it
equals `analyze(..).worst_case_total()` over every jitter mode and bound.

## Result

| Counter | Before | After | Delta |
|---|---|---|---|
| callgrind total Ir | 474,868,861 | 306,354,034 | -35.5% |
| `DagRetryEnvelope::analyze` inclusive Ir | 341,342,664 | 172,825,994 | -49.4% |
| dhat blocks | 471,117 | 311,117 | -34.0% |
| dhat bytes | 152,724,654 | 54,164,654 | -64.5% |

The totals include the harness cost of building and cloning the DAG.
