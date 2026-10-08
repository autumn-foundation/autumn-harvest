# Apparatus for assay #2: matched-workload Redis vs Postgres

Non-production. Not a workspace member. See
[`../../0002-redis-matched-workload-vs-postgres.md`](../../0002-redis-matched-workload-vs-postgres.md)
for the question, pre-registration, and verdict this code was built to
answer.

```bash
redis-server --daemonize yes --port 6379 --save "" --appendonly no
BENCH_SCENARIO_SECS=120 cargo run --release --manifest-path docs/assays/apparatus/0002-redis-matched-workload/Cargo.toml
```

Set `BENCH_BACKLOGS` (comma-separated) to override the swept backlog depths
(default `1000,10000,100000`, matching `docs/performance.md`'s table).
