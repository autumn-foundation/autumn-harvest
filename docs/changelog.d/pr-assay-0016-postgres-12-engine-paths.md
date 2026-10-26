## Phase — Assay #16: the Postgres 12 floor at the engine layer

Assay ledger #16 tried to run the engine's database suites on PG 12.22, PG 14.24 and PG 16.15. The verdict is undetermined: the PG 16 control passed 79.0% of in-scope tests against a registered 90% validity line, so no arm was graded. The report names a per-test re-charter. See `docs/assays/0016-postgres-12-engine-paths.md`.

No engine change: no `WorkflowEvent` variant, no migration, no public API change.
