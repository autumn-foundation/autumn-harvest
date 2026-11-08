# The claim refine by TID: e2e profile (issue #2547)

Each file is the `pg_stat_statements` or `pg_stat_user_tables` snapshot of
one shard database, for the `throughput` scenario at 1 shard. The bench
resets both views after its migrations, and teardown writes them before it
drops the database.

- `before1`, `before2`: `trunk-dev` at `373ec6f`.
- `after1`, `after2`: this change.
- Run 2 sampled 2% of plans with `auto_explain`. The plans are not
  committed. `docs/performance.md` gives their medians.

Reproduce:

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
HARVEST_BENCH_SCENARIOS=throughput HARVEST_BENCH_SHARDS=1 \
HARVEST_BENCH_STATS_DIR=/tmp/claim-refine-tid \
  cargo bench -p autumn-harvest --features db,testing --bench e2e_bench
```

The server needs `shared_preload_libraries = 'pg_stat_statements'`.
