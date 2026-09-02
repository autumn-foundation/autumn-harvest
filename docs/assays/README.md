# Assay ledger

Records of R&D spikes run against a falsifiable question, a named decider, and
numeric success/kill lines committed *before* the first measurement. See each
entry for its pre-registration commit, apparatus, and verdict.

| # | question | verdict | report |
|--:|:--|:--|:--|
| 1 | Does the built `autumn-harvest-redis` adapter clear its own spec's >10,000 ops/sec bar, on a machine where nothing else can slow it down? | **kill**, narrowly: an 8-worker/steady-state sub-question this assay added on its own (8,828 mean vs 10,000 ops/sec). The founding spec's actual, unconstrained claim looks achievable, not refuted — a matched-workload backlog-drain check (post-hoc, not pre-registered) hit 12,271 mean, 19.2x the Postgres control. Unintegrated with the worker regardless of either number. | [0001-redis-adapter-throughput-ceiling.md](0001-redis-adapter-throughput-ceiling.md) |
