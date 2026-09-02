# Assay ledger

Records of R&D spikes run against a falsifiable question, a named decider, and
numeric success/kill lines committed *before* the first measurement. See each
entry for its pre-registration commit, apparatus, and verdict.

| # | question | verdict | report |
|--:|:--|:--|:--|
| 1 | Does the built `autumn-harvest-redis` adapter clear its own spec's >10,000 ops/sec bar, on a machine where nothing else can slow it down? | **kill** on the registered claim (8,828 mean vs 10,000 ops/sec @ 8 workers, shared queue) — but 13.8x the measured Postgres control at matched concurrency, and unintegrated with the worker regardless | [0001-redis-adapter-throughput-ceiling.md](0001-redis-adapter-throughput-ceiling.md) |
