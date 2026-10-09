# Claim concurrency: measurement artifacts

The tables in [`docs/performance.md` § "Claim concurrency"](../../performance.md#claim-concurrency)
come from these files. Non-production. Read assay #11's bounding section
before you quote a number: one box, one shape, one Temporal version.

| file | what it is |
|:--|:--|
| `apparatus.patch` | a patch to assay #14's harvest apparatus. It reads the claim cap from `ASSAY14_MAX_CONCURRENT_CLAIMS`. It also sets `tenant: None`, a field that trunk added after assay #14 (issue #1977). |
| `run.sh` | runs three rounds: Temporal at each depth, then caps 1, 2 and 4 |
| `grade.py` | prints the tables from the raw output |
| `results/raw/` | the verbatim output of the sweep |
| `results/graded.md` | the output of `grade.py` |

## Prerequisites

The same as assay #14's harvest and Temporal arms. See
[its README](../../assays/apparatus/0014-harvest-vs-temporal-0.7.0/README.md).
In short: PostgreSQL 16 on `127.0.0.1:5432` with `fsync=off`,
`synchronous_commit=off`, `max_connections=300` and trust auth on loopback.
Docker with `temporalio/auto-setup:1.25.2`. Go 1.24.7.

## Running

Copy the apparatus four directories below the repository root, so its path
dependencies resolve. Apply the patch, then build one release binary.

```bash
mkdir -p target/cc/x
cp -r docs/assays/apparatus/0014-harvest-vs-temporal-0.7.0 target/cc/x/app
git apply --directory=target/cc/x/app docs/perf-artifacts/claim-concurrency/apparatus.patch
CARGO_TARGET_DIR=target/cc/tgt cargo build --release --locked \
  --manifest-path target/cc/x/app/Cargo.toml
(cd docs/assays/apparatus/0011-harvest-vs-temporal && go build -o assay11 .)

CC_BIN=target/cc/tgt/release/harvest_vs_temporal_070_assay \
  ./docs/perf-artifacts/claim-concurrency/run.sh
```

Run nothing else on the box during the sweep. `run.sh` refuses to write
into a `results/raw` that is not empty.
