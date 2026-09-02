# Assay ledger

Records of R&D spikes run against a falsifiable question, a named decider, and
numeric success/kill lines committed *before* the first measurement. See each
entry for its pre-registration commit, apparatus, and verdict.

| # | question | verdict | report |
|--:|:--|:--|:--|
| 1 | Does the built `autumn-harvest-redis` adapter clear its own spec's >10,000 ops/sec bar, on a machine where nothing else can slow it down? | **kill**, narrowly: an 8-worker/steady-state sub-question this assay added on its own (8,828 mean vs 10,000 ops/sec). The founding spec's actual, unconstrained claim looks achievable, not refuted — an exploratory backlog-drain check (post-hoc, not pre-registered, and not a verified match for the Postgres control's workload shape after two attempts) hit 12,271 mean on its own. No multiplier against Postgres is reported. Unintegrated with the worker regardless of either number. | [0001-redis-adapter-throughput-ceiling.md](0001-redis-adapter-throughput-ceiling.md) |
