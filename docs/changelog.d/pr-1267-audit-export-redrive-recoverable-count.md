## Phase — audit-export redrive reports what it can actually deliver (issue #1267)

`POST /admin/audit-export/redrive` rewinds a shard's export cursor under
`SELECT … FOR UPDATE` on the cursor row. `audit::purge_old_audit_records`
does not take that lock — it reads the cursor through a plain, uncorrelated
subquery.

A retention sweep can race a redrive: it reads the old, higher
`last_acked_seq`, purges records the redrive is about to promise back, and
the redrive still commits its lower cursor. The `200` response then claimed
full recovery of a window retention had already narrowed. Filed as a P2
follow-up to issue #953 (PR #1261, Codex review round 7) — the bug is in the
report, not in data loss beyond what retention already permitted.

Fix: the redrive now counts, inside the same transaction as the rewind, how
many `(to, from]` records still exist. `count_redrive_recoverable` is the
new primitive in `autumn_harvest::audit_export`. The
`POST /admin/audit-export/redrive` handler calls it right after
`rewind_cursor_locked` and adds two fields to the response:

- `recoverable_records` — records in the window that still exist and will
  re-export.
- `already_purged_records` — the rest of the window, gone before this
  redrive could reach it. `0` on the common path.

The `audit_export.redrive` audit record also names the gap when
`already_purged_records > 0`, so the compliance trail matches the API
response.

This closes the false-success report. It does not add locking to the
retention path — the issue named that as a separate possible follow-up,
worth measuring on a large audit table before committing to it.

No new `WorkflowEvent` variant, no migration — a read/report addition to
the existing redrive transaction.

Tests:
- `redrive_recoverable_count_matches_the_full_window_when_nothing_was_purged`
  (`autumn-harvest/tests/integration/audit_export_tests.rs`, integration,
  real Postgres): no purge, no gap — the full window is recoverable.
- `redrive_recoverable_count_falls_short_when_a_purge_already_removed_part_of_the_window`
  (same file): reproduces the race's end state directly — purge deletes the
  aged, acknowledged records under the stale cursor before the redrive
  runs — and asserts the redrive reports exactly the survivors, not the
  full window it was asked for.
- `docs/api-contract.json`, `management_api_response_fields()`, and both
  `openapi.json` copies updated and regenerated
  (`scripts/regenerate-openapi.sh`); `contract_regression` and
  `openapi_spec` suites pass.
