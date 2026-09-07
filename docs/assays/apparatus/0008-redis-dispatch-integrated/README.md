# Apparatus for assay #8: integrated Redis dispatch throughput

Non-production. Not a workspace member. See
[`../../0008-redis-dispatch-integrated-throughput.md`](../../0008-redis-dispatch-integrated-throughput.md)
for the question, the pre-registration and the verdict this code answers.

It runs a real four-`Worker` pool against a real Postgres, once with
`RedisDispatch` installed as the process-global dispatch channel and once
with no channel at all. Both arms use the same binary, the same pool shape
and the same workload.

```bash
psql -h 127.0.0.1 -U postgres -c 'create database assay6'
redis-server --daemonize yes --port 6379 --save "" --appendonly no
cargo run --release --manifest-path docs/assays/apparatus/0008-redis-dispatch-integrated/Cargo.toml
```

The binary drops and recreates its own database before every run, applies
`autumn_harvest::test_init_sql()`, and deletes only the Redis keys under its
own per-run prefix. It never calls `FLUSHALL`.

| variable | default | meaning |
|:--|:--|:--|
| `ASSAY6_DATABASE_URL` | `postgres://postgres@127.0.0.1:5432/assay6` | the assay database |
| `ASSAY6_ADMIN_URL` | `postgres://postgres@127.0.0.1:5432/postgres` | used to drop and create it |
| `ASSAY6_DB_NAME` | `assay6` | name of the database to reset |
| `ASSAY6_REDIS_URL` | `redis://127.0.0.1:6379` | the dispatch Redis |
| `ASSAY6_WORKFLOWS` | `10000` | seeded workflows per drain run |
| `ASSAY6_REPS` | `3` | repetitions per arm per shape |
| `ASSAY6_PACED_SECS` | `30` | length of the paced window |
| `ASSAY6_DRAIN_CAP_SECS` | `600` | cap on one Redis-arm run, after which it is truncated |
| `ASSAY6_CONTROL_CAP_SECS` | `ASSAY6_DRAIN_CAP_SECS` | the same cap for the control arm, which needs its own |
| `ASSAY6_WORKER_POLL_MS` | `25` | `WorkerRuntimeConfig.poll_interval`, which also drives the metrics samplers |
| `ASSAY6_PACED_RATE_MILLI` | unset | paced starts per second in thousandths, so the paced shape runs on its own |
| `ASSAY6_SEEDERS` | `16` | parallel connections used to seed |
| `ASSAY6_SHAPES` | `drain,paced` | narrow the matrix to one shape |
| `ASSAY6_ARMS` | `redis,control` | narrow the matrix to one arm |
