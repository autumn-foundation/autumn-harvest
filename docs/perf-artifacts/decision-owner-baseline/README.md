# Decision-owner baseline: measurement artifacts

The tables in
[`docs/performance-decision-owner-baseline.md`](../../performance-decision-owner-baseline.md)
come from these files (issue #2248). Non-production. One box, one shape.

| file | what it is |
|:--|:--|
| `apparatus.patch` | a patch to assay #14's harvest apparatus. It reads the claim cap from `ASSAY14_MAX_CONCURRENT_CLAIMS`. It records each claim by kind and each decision persist phase, and prints them on the `cell` line. |
| `run.sh` | runs three rounds of the `postgres` arm at caps 1, 2 and 4 |
| `grade.py` | prints the tables from the raw output |
| `results/raw/` | the verbatim output of the sweep |
| `results/graded.md` | the output of `grade.py` |
| `results/fence-bench.txt` | the output of `benches/decision_fence_bench.rs` |

## Prerequisites

The same as assay #14's harvest arm. See
[its README](../../assays/apparatus/0014-harvest-vs-temporal-0.7.0/README.md).
In short: PostgreSQL 16 on `127.0.0.1:5432` with `fsync=off`,
`synchronous_commit=off`, `max_connections=300` and trust auth on loopback.
No Temporal arm runs.

## Running

Copy the apparatus four directories below the repository root, so its path
dependencies resolve. Apply the patch, then build one release binary.

```bash
mkdir -p target/dob/x
cp -r docs/assays/apparatus/0014-harvest-vs-temporal-0.7.0 target/dob/x/app
git apply --directory=target/dob/x/app docs/perf-artifacts/decision-owner-baseline/apparatus.patch
CARGO_TARGET_DIR=target/dob/tgt cargo build --release \
  --manifest-path target/dob/x/app/Cargo.toml

DOB_BIN=target/dob/tgt/release/harvest_vs_temporal_070_assay \
  ./docs/perf-artifacts/decision-owner-baseline/run.sh
```

The fence bench:

```bash
HARVEST_FENCE_BENCH_URL=postgres://postgres@127.0.0.1:5432/postgres \
  cargo bench -p autumn-harvest --features db --bench decision_fence_bench
```

Run nothing else on the box during a sweep. `run.sh` refuses to write into
a `results/raw` that is not empty.
