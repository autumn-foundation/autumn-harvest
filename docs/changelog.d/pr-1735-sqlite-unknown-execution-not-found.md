## Fix — SQLite: `ExecutionNotFound` for an unknown id on every accessor (issue #1735)

`SqliteError::ExecutionNotFound` means an unknown id. Before this change, only
`outcome` and `send_signal` returned it. For an unknown `ExecutionId`:

- `run_until_blocked` (and `run_until_blocked_as_of`) returned
  `Sqlite(QueryReturnedNoRows)`. That error names no id.
- `load_history` and `activity_attempts` returned `Ok(vec![])`. A caller could
  not tell a wrong id from a run with no rows.

**What changed**

- `store::execution_state` returns `ExecutionNotFound(exec)` when no row
  matches. The drivers, `outcome` and `send_signal` share it. `outcome` and
  `send_signal` drop their separate existence query, so each makes one query
  fewer.
- `load_history` and `activity_attempts` check existence only when the result
  is empty. A non-empty result costs no extra query.
- A known execution with no attempts for a name still returns an empty list.
- The `# Errors` rustdoc of `run_until_blocked`, `load_history`,
  `activity_attempts` and `send_signal` now lists `ExecutionNotFound` and the
  other variants each can return. `docs/sqlite-backend.md` §7 states the rule.

**Behavior change.** `load_history` and `activity_attempts` now return `Err`
for an unknown id, where they returned `Ok(vec![])`. `run_until_blocked` and
`run_until_blocked_as_of` now return `ExecutionNotFound`, not
`Sqlite(QueryReturnedNoRows)`.

**Invariants.** No new `WorkflowEvent` variant. No migration. No write to
`harvest_events`.

**Tests.** `tests/integration/unknown_execution_id.rs` covers six
accessors on an unknown id. It also pins a known id with no attempts to an
empty `Ok` list.
