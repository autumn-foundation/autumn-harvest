# Apparatus for assay #10: cross-mode throughput

Non-production. Not a workspace member. See
[`../../0010-cross-mode-throughput.md`](../../0010-cross-mode-throughput.md)
for the question, the pre-registration and the verdict this code answers.

Three arms drain the same backlog of the same canonical 3-activity workflow:

| arm | persistence | dispatch |
|:--|:--|:--|
| `sqlite` | one `SQLite` file, embedded | caller-driven `run_until_idle` |
| `postgres` | PostgreSQL | the Postgres claim path |
| `redis_pg` | PostgreSQL | `RedisDispatch`, Postgres still the source of truth |

The same `wf_three_activities` function is the workflow handler on every arm.
The replay engine is backend-neutral, so only persistence and dispatch differ.

```bash
redis-server --daemonize yes --port 6379 --save "" --appendonly no
cargo run --release --manifest-path docs/assays/apparatus/0010-cross-mode-throughput/Cargo.toml
```

`ASSAY10_INPUT_JSON` defaults to the canonical empty object the published
harness seeds, which is what this assay's L1 compares against. Assay #11
registers a ~40-byte payload instead and overrides it. **A run with an
overridden input is a different workload, so the binary refuses to grade any
pre-registered line for it** and prints a "Not graded" notice in place of the
verdict block.

The binary drops and recreates its own Postgres database before every
Postgres-backed repetition, applies `autumn_harvest::test_init_sql()`, deletes
its own `SQLite` file before every embedded repetition, and deletes only the
Redis keys under its own per-run prefix. It never calls `FLUSHALL`.

| variable | default | meaning |
|:--|:--|:--|
| `ASSAY10_DATABASE_URL` | `postgres://postgres@127.0.0.1:5432/assay10` | the assay database |
| `ASSAY10_ADMIN_URL` | `postgres://postgres@127.0.0.1:5432/postgres` | used to drop and create it |
| `ASSAY10_DB_NAME` | `assay10` | name of the database to reset |
| `ASSAY10_REDIS_URL` | `redis://127.0.0.1:6379` | the dispatch Redis |
| `ASSAY10_SQLITE_DIR` | `/tmp/assay10-sqlite` | directory for the embedded arm's file |
| `ASSAY10_WORKFLOWS` | `2000` | seeded workflows per drain run |
| `ASSAY10_REPS` | `3` | repetitions per arm |
| `ASSAY10_SEEDERS` | `16` | parallel connections used to seed |
| `ASSAY10_INPUT_JSON` | `{}` | the seeded workflow input, as JSON text |
| `ASSAY10_CAP_SECS` | `900` | cap on one run, after which it is truncated |
| `ASSAY10_ARMS` | `sqlite,postgres,redis_pg` | narrow the matrix to one arm |
