# Group commit: measurement artifacts

The tables in
[`docs/performance-group-commit.md`](../../performance-group-commit.md)
come from these files (issue #2256). Non-production. One box, one shape.

| file | what it is |
|:--|:--|
| `grade.py` | prints the tables from the raw output |
| `results/sync-on-r<n>.txt` | one bench run with `synchronous_commit = on`, the server default |
| `results/sync-off-r<n>.txt` | one bench run with `synchronous_commit = off` on the bench database |

## Running

PostgreSQL 16 on `localhost:5432` with trust auth and `max_connections=300`.
Build the bench once, then run it once per round. Each round sets the mode
order, so drift hits each mode alike.

```bash
cargo bench -p autumn-harvest --features db --bench group_commit_bench --no-run

HARVEST_GROUP_BENCH_URL=postgres://postgres@localhost:5432/postgres \
HARVEST_GROUP_BENCH_ORDER=solo,group,direct \
  target/release/deps/group_commit_bench-<hash> --bench

python3 docs/perf-artifacts/group-commit/grade.py
```

Add `HARVEST_GROUP_BENCH_SYNC=off` for the `sync-off` rounds.
