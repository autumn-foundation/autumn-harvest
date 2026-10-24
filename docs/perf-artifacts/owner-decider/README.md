# Owner decider: measurement artifacts

The tables in
[`docs/performance-owner-decider.md`](../../performance-owner-decider.md)
come from these files (issue #2255). Non-production. One box, one shape.

| file | what it is |
|:--|:--|
| `apparatus.patch` | a patch to assay #14's harvest apparatus. It holds the baseline patch of issue #2248. It adds `ASSAY14_OWNER_MODE=1`, which turns owner mode on after the schema, and it records each owner take by kind. |
| `run.sh` | runs rounds of the `postgres` arm in legacy mode and in owner mode, at each claim cap |
| `grade.py` | prints the tables from the raw output |
| `results/raw/` | the verbatim output of the sweep |
| `results/graded.md` | the output of `grade.py` |
| `results/fence-bench/` | the output of the fence bench runs |

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
mkdir -p target/odc/x
cp -r docs/assays/apparatus/0014-harvest-vs-temporal-0.7.0 target/odc/x/app
git apply --directory=target/odc/x/app docs/perf-artifacts/owner-decider/apparatus.patch
CARGO_TARGET_DIR=target/odc/tgt cargo build --release \
  --manifest-path target/odc/x/app/Cargo.toml

ODC_BIN=target/odc/tgt/release/harvest_vs_temporal_070_assay \
  ./docs/perf-artifacts/owner-decider/run.sh
```

The fence bench, one writer, in rotated mode orders:

```bash
for order in \
  generation,generation_owner,generation_owner_split,owner \
  owner,generation_owner_split,generation_owner,generation \
  generation_owner_split,generation,owner,generation_owner
do
  HARVEST_FENCE_BENCH_WRITERS=1 HARVEST_FENCE_BENCH_ORDER="$order" \
  HARVEST_FENCE_BENCH_URL=postgres://postgres@127.0.0.1:5432/postgres \
    cargo bench -p autumn-harvest --features db --bench decision_fence_bench -- --noplot
done
```

Run nothing else on the box during a sweep. `run.sh` refuses to write into
a `results/raw` that is not empty.
