## Phase — Audit export disable marks shards unobserved (issue #1506)

A live process could clear its audit-export config. Both audit-export gauges
then kept their last value, and no alert fired. `set_global_audit_export_config`
now records the `Some` to `None` change. Both export tick paths then set
`harvest.audit.export_observed` to `0` for each shard they serve.

A process that never configured export emits nothing new (AC8). Tests write
the config lock directly, so they never set the mark. No migration. No
`WorkflowEvent` change. `harvest_events` is not touched.

Tests: six unit tests in `audit_export.rs` and one integration test,
`disabling_a_live_export_marks_the_default_shard_unobserved`.
