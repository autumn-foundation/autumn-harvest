# Assay ledger

Records of R&D spikes run against a falsifiable question, a named decider, and
numeric success/kill lines committed *before* the first measurement. See each
entry for its pre-registration commit, apparatus, and verdict.

| # | question | verdict | report |
|--:|:--|:--|:--|
| 1 | Does the built `autumn-harvest-redis` adapter clear its own spec's >10,000 ops/sec bar, on a machine where nothing else can slow it down? | **kill**, narrowly: an 8-worker/steady-state sub-question this assay added on its own (8,760 mean vs 10,000 ops/sec). The founding spec's actual, unconstrained claim looks achievable, not refuted — an exploratory backlog-drain check (post-hoc, not pre-registered, and not a verified match for the Postgres control's workload shape after two attempts) hit 12,004 mean on its own. No multiplier against Postgres is reported. Unintegrated with the worker regardless of either number. | [0001-redis-adapter-throughput-ceiling.md](0001-redis-adapter-throughput-ceiling.md) |
| 2 | Re-charter of #1: at `docs/performance.md`'s own matched scenario shape (8 claimers, 4 queues, bounded-fraction claim-only draw, ported by value from `claim_bench_support.rs`), does `RedisTaskQueue::claim` clear a decisive (10x) margin over Postgres's published claims/s at the 10,000-row headline cell? | **pursue** — mean 17,561.96 claims/s across 4 runs (range 15,319.64-19,140.83) vs. a 290 claims/s (10x) line, against a published Postgres control of 29 claims/s (~605x on the mean, ~53x on the worst individual run). `n` matched the published Postgres `n` exactly at both the 1,000- and 10,000-row cells, confirming the workload shapes now line up. Still unintegrated with the worker; a deployment-shaped follow-up remains a shelf entry until the integration refactor exists. | [0002-redis-matched-workload-vs-postgres.md](0002-redis-matched-workload-vs-postgres.md) |
