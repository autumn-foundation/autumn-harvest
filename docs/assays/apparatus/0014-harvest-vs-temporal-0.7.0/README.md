# Apparatus for assay #14: assay #11 rerun on 0.7.0, at every depth

Non-production. Not a workspace member. See
[`../../0014-harvest-vs-temporal-0.7.0-depth-sweep.md`](../../0014-harvest-vs-temporal-0.7.0-depth-sweep.md)
for the question, the pre-registration and the verdict.

Read assay #11's bounding section before you quote a number from here. Four
cores is near the bottom of Temporal's envelope and near the top of harvest's.

## Parts

| file | what it does |
|:--|:--|
| `src/main.rs` | the harvest arms, `postgres` and `redis_pg`, at every depth |
| `run.sh` | runs the registered matrix in the registered order |
| `grade.py` | grades the registered lines from the raw output |
| `results/` | verbatim output and the graded summary |

The Temporal arm is assay #11's
[`main.go`](../0011-harvest-vs-temporal/main.go) and
[`run.sh`](../0011-harvest-vs-temporal/run.sh), unchanged.

The harvest arm is assay #10's workload with two changes:

- One process sweeps every depth in `ASSAY14_DEPTHS`.
- A capture recorder keeps the per-event #1815 signals. It returns
  `is_enabled() = false`, so the worker runs no sampler SQL.

## Running

Build every binary before the first run. Do not build, run git or run any
other job while the sweep runs.

```bash
# One harvest binary per tree. The path dependencies resolve inside each
# checkout, so copy this directory into each one first.
for t in 0aeb887 513b7aa 9f444b7; do
  git worktree add --detach "/trees/$t" "$t"
  cp -r docs/assays/apparatus/0014-harvest-vs-temporal-0.7.0 \
    "/trees/$t/docs/assays/apparatus/"
  CARGO_TARGET_DIR="/tgt/$t" cargo build --release --manifest-path \
    "/trees/$t/docs/assays/apparatus/0014-harvest-vs-temporal-0.7.0/Cargo.toml"
done
(cd docs/assays/apparatus/0011-harvest-vs-temporal && go build -o assay11 .)

redis-server --daemonize yes --port 6379 --save "" --appendonly no
ASSAY14_BINS=0aeb887=/tgt/0aeb887/release/harvest_vs_temporal_070_assay,\
513b7aa=/tgt/513b7aa/release/harvest_vs_temporal_070_assay,\
9f444b7=/tgt/9f444b7/release/harvest_vs_temporal_070_assay \
  ./docs/assays/apparatus/0014-harvest-vs-temporal-0.7.0/run.sh
```

| variable | default | meaning |
|:--|:--|:--|
| `ASSAY14_TREE` | `unknown` | the tree label printed on every cell line |
| `ASSAY14_ROUND` | `0` | the round label printed on every cell line |
| `ASSAY14_DEPTHS` | `250,500,1000,2000` | backlog depths, in run order |
| `ASSAY14_ARMS` | `postgres,redis_pg` | harvest arms, in run order |
| `ASSAY14_INPUT_JSON` | assay #11's payload | the seeded workflow input |
| `ASSAY14_CAP_SECS` | `900` | cap on one run |
| `ASSAY14_DATABASE_URL` | `postgres://postgres@127.0.0.1:5432/assay14` | the assay database |
| `ASSAY14_ADMIN_URL` | `postgres://postgres@127.0.0.1:5432/postgres` | drops and creates it |
| `ASSAY14_REDIS_URL` | `redis://127.0.0.1:6379` | the dispatch Redis |
