## Feature — Database guard on append-only `harvest_events` (issue #1817)

**Behaviour change.** A `BEFORE UPDATE` trigger on `harvest_events` now
rejects history rewrites with `restrict_violation`. Before this change, only a
source grep test enforced the invariant. An operator script or a new code path
could still rewrite history.

**The rules, per updated row.**

- `event_data` changes only when the transaction sets
  `harvest.sanctioned_event_rewrite` to `erase` or `codec_rotation`.
  `erase.rs` and `codec_rotation.rs` set it through `append_only::sanction`
  and clear it through `append_only::revoke`.
- The `type` key inside `event_data` never changes.
- No other column changes, except `cohort`. The trigger compares whole rows,
  so a column that a later migration adds is guarded too.

**`cohort` is sanctioned.** It is storage placement, not history. Replay does
not read it. `disable_partitioning` resets it on the new flat table.

**Partition layout changes.** `LIKE` copies no triggers. `enable_sql`,
`migration_plan_steps` and `disable_partitioning` now reinstall the guard.
`operator_triggers` exempts it by its exact shape, so the conversion does not
refuse harvest's own trigger. An impostor with the same name still refuses.

**Codec rotation cost.** Each compare-and-swap now runs in its own transaction,
because a transaction-local setting needs one. The sweep is a bounded
background batch.

**Migration.** `20261004160009_harvest_events_append_only_guard`. Metadata
only. No `WorkflowEvent` variant, no replay impact.

**Tests.** `append_only_guard_tests` (new): a plain `UPDATE` of `event_data`
fails, each sanction passes, an unknown or out-of-transaction sanction fails,
identity columns stay immutable under a sanction, and `cohort` stays writable.
`event_partitioning_tests` adds the guard round trip through `enable`, the
large-table plan and `disable`, plus an impostor-trigger refusal. The erasure
and codec-rotation suites pass with the guard on, including
`replay_fidelity_is_byte_identical_across_a_sweep`. Fixtures that backdate or
tamper rows use `append_only::with_guard_off`.
