## Fix — no panics on scanner, `Drop` and request paths (issue #1821)

A panic on a path that every replica runs can disable that path across the
fleet. One bad row stops the same scanner loop on every replica. A panic in
`Drop` during unwinding aborts the process. This change removes those panics
and adds a lint that keeps them out.

**Breaking change.** `classify_workflow_timeout` now returns
`Option<(DateTime<Utc>, TimeoutKind)>`. A caller must handle `None`.

- **`ExecutionId` round-trip.** The open question was whether
  `ExecutionId` parsing can reject a valid UUID. It cannot.
  `ExecutionId::from_str` is `Uuid::parse_str`, and `Uuid` display always
  parses. The scanners in `worker`, `timeout`, `poison_pill` and `sessions`
  now call the total `ExecutionId::from_uuid`. The string round-trip, its
  allocation and its `expect` are gone. A row that is not a UUID already
  fails in Diesel `load` with an error, before this code runs. No skip metric
  ships. No stored UUID can fail the conversion, so no skip path exists.
- **`RetentionLeaseGuard::drop`.** It recovers a poisoned lock. It skips the
  release when no Tokio runtime exists.
- **Poisoned locks.** The retention and scheduler monitors, the retention
  lease list, every `HarvestApiState` cell and the plugin runtime slot
  recover with `PoisonError::into_inner`. No critical section can leave its
  value half-written, so a poisoned lock still holds valid data.
  `WorkflowContext` keeps its panics: a panic there fails one workflow task
  under `catch_unwind`, and replay builds a new context.
- **Row-driven values return errors.** The workflow-timeout scanner logs and
  skips a row with no fired deadline. The scan filter makes that branch
  unreachable, so it has no metric. The retention candidate `completed_at`
  and the parent-close policy columns load as non-null, so a NULL is a Diesel
  error, not a panic. `resolve_live_attempt`,
  `resolve_live_attempt_id_best_effort` and the history-cap path return
  `HarvestError` values.
- **Lint.** `clippy::expect_used` and `clippy::unwrap_used` are `warn` in the
  `autumn-harvest` and `autumn-harvest-plugin` libraries. `clippy.toml`
  exempts test code. Each remaining site has an `#[expect]` attribute with a
  reason. `context` and `testing` are whole-module exceptions in `lib.rs`.
  See "Panic policy" in `docs/architecture.md`.
- **Tests.** These tests failed before the fix:
  `lease_guard_drop_survives_a_poisoned_lock_1821`,
  `harvest_api_state_survives_a_poisoned_cell_1821` and
  `classify_workflow_timeout_without_a_fired_deadline_does_not_panic_1821`.
  `execution_id_parse_accepts_every_uuid_1821` is a property test that
  answers the open question.

No migration, no schema change, no new `WorkflowEvent` variant.
