## Feature — Database guard on append-only `harvest_events` (issue #1817)

**Behavior change.** A `BEFORE UPDATE` trigger on `harvest_events` now
rejects history rewrites with `restrict_violation` (SQLSTATE `23001`). Before
this change, only a source grep test enforced the invariant. An operator
script or a new code path could still rewrite history.

**The rules, per updated row.**

- `event_data` changes only when the transaction sets
  `harvest.sanctioned_event_rewrite` to `erase` or `codec_rotation`.
  `erase.rs` and `codec_rotation.rs` set it through `append_only::sanction`
  and clear it through `append_only::revoke`.
- The `type` key inside `event_data` never changes.
- No other column changes, `cohort` included. The trigger compares whole
  rows, so a column that a later migration adds is guarded too.

The trigger checks who rewrites `event_data`, not what a writer changes under
`data`. Each exception's own tests still prove its scope.

**`cohort` is guarded.** The sweeper's fast drop gate assumes that no row's
cohort predates its execution. A row moved into an older cohort could be
dropped while its run is live. `disable_partitioning` resets `cohort` on the
new flat table before it reinstalls the guard.

**Partition layout changes.** `LIKE` copies no triggers. `enable_sql`,
`migration_plan_steps` and `disable_partitioning` now reinstall the guard.
`operator_triggers` exempts it by its exact shape. The conversion still
refuses an impostor trigger with the same name.

**Codec rotation cost.** Each compare-and-swap now runs in a transaction,
because a transaction-local setting needs one. Each row adds BEGIN, COMMIT and
two `set_config` round trips to the one UPDATE.

**Rolling upgrade.** A worker on an older release does not set the sanction.
Its erasure requests and codec-rotation sweep fail until it runs this release.

**Migration.** `20261004160009_harvest_events_append_only_guard`. No row is
read or written. No `WorkflowEvent` variant, no replay impact.

**Tests.**

- `append_only_guard_tests` (new) covers the rules. A plain `UPDATE` of
  `event_data` fails. Each sanction passes. An unknown or out-of-transaction
  sanction fails. Identity columns and `cohort` stay immutable under a
  sanction.
- `event_partitioning_tests` adds the guard round trip through `enable`, the
  large-table plan, `disable` and a `DEFAULT` drain. It also covers the
  migration on an already-partitioned shard and an impostor-trigger refusal.
- The erasure and codec-rotation suites pass with the guard on, including
  `replay_fidelity_is_byte_identical_across_a_sweep`.
- Fixtures that backdate or tamper rows call `append_only::with_guard_off`.
