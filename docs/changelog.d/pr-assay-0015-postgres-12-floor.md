## Phase — Assay #15: the Postgres 12 floor at the schema layer

Assay ledger #15 applies all 139 migrations to fresh PG 12.22, PG 14.19 and PG 16.15 databases, three runs each. All 834 PG 12 and PG 14 applications succeed (417 per version), and the schema signature matches PG 16. Engine query paths are not covered. See `docs/assays/0015-postgres-12-floor.md`.

No engine change: no `WorkflowEvent` variant, no migration, no public API change.
