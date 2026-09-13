## Phase 5.x — a coherent audit-export cursor lifecycle: close a revival race, audit retire/reactivate (issue #1273)

Two deferred findings from #1261's review (issue #953's audit-export cursor),
filed together because they are two symptoms of the same under-designed
mechanism: `decommission_cursor` retired a shard's export cursor by setting
`retired_at`, but the states a cursor can be in, who may transition it, and
what gets recorded were never written down.

- **Finding 1 (P1) fixed: a racing scanner pass can no longer revive a
  retired cursor.** `ensure_cursor_row`'s `ON CONFLICT DO UPDATE` used to
  clear `retired_at` unconditionally on every scanner tick, so a tick already
  under way when an operator retired a shard could silently un-retire it
  moments later. The `DO UPDATE` is now guarded by `WHERE retired_at IS
  NULL`, evaluated by Postgres under the same row lock that resolves the
  conflict — race-free by construction, not by convention. A retired cursor
  is now a complete no-op for `ensure_cursor_row`: no heartbeat, no
  un-retire.
- **Finding 2 (P2) fixed: retirement (and reactivation) are audited.**
  `decommission_cursor` was a bare library call with no route, no admin
  gate, and no audit trail — the one action that discards unexported audit
  records had no record of who authorised it. Two new admin routes,
  `POST /admin/audit-export/decommission` and `POST /admin/audit-export/reactivate`,
  are admin-gated and audited (`audit_export.decommission`,
  `audit_export.reactivate`) in the same one-transaction-one-connection
  shape as the existing redrive route: the mutation and its audit row commit
  together, on the target shard's own connection, so an
  applied-but-unaudited transition is not representable.
- **Reactivation is now explicit, not a side effect.** Resuming export used
  to happen implicitly on the next scanner tick after a re-enable — the same
  mechanism finding 1 closes. `POST /admin/audit-export/reactivate` is now
  the only way back from `RETIRED` to live, resuming from the preserved
  `last_assigned_seq` exactly as before, just as an explicit, audited
  operator action instead of an inferred one.
- **Bonus fix, surfaced by finding 1's own fix: `purge_old_audit_records`'s
  retention guard now treats a shard's own `retired_at` as authoritative.**
  Its guard used to OR a shard's live-cursor check together with the
  process-wide `is_configured()` flag, so decommissioning one shard released
  nothing as long as *any* shard in the process still had a sink configured
  — the ordinary fleet steady state. This made no practical difference
  before finding 1's fix, since `ensure_cursor_row` un-retired a cursor on
  the very next tick anyway; making retirement durable is what surfaced it.
  `is_configured()` now only stands in for a cursor row when none exists
  yet (the bootstrap window); once a row exists, its own `retired_at`
  decides. See `decommission_releases_its_shards_backlog_even_while_export_stays_configured_elsewhere`.
- **Second bonus fix, from automated review of this PR: the decommission
  route no longer traps its own audit record in the stream it just
  retired.** `POST /admin/audit-export/decommission`'s atomic audit row
  lands on the target shard's own connection (needed for atomicity with the
  mutation), but that shard's exporter is exactly what the call just
  stopped — combined with the retention fix above, that record could
  eventually be deleted from that shard's own database with no trace
  anywhere, defeating finding 2's whole purpose. A genuine retirement now
  also writes a second, best-effort audit record on the default shard,
  which keeps exporting. Skipped when the target already IS the default
  shard. Not yet covered by an automated test: this codebase has no
  plugin-level HTTP integration test for the audit-export admin routes at
  all (the pre-existing redrive route has the same gap), and a real test
  needs a multi-shard harness this sandbox could not run against Docker.
- **No migration.** `retired_at` and `claim_epoch` already existed on
  `harvest_audit_export_cursor`; the fix is a predicate on an existing
  `ON CONFLICT` statement plus two new locked-transaction functions
  (`decommission_cursor_locked`, `reactivate_cursor_locked`) reusing the
  existing columns.
- **Test evidence:** `autumn-harvest/tests/integration/audit_export_tests.rs`
  gained direct coverage for both findings —
  `ensure_cursor_row_never_revives_a_retired_cursor` and
  `a_scanner_tick_after_decommission_does_not_revive_the_cursor` pin finding
  1 at both the unit and scanner-tick level;
  `a_decommission_and_its_audit_record_land_together_on_the_target_shard`
  and its reactivate counterpart pin finding 2; idempotency
  (`AlreadyRetired`/`AlreadyActive`) and unconfigured-shard cases are
  covered separately, along with atomicity (`a_failed_audit_write_rolls_the_decommission_back`
  and its reactivate counterpart) and the epoch-fencing interaction across a
  decommission-then-reactivate cycle
  (`a_reactivate_does_not_reopen_a_claim_fenced_by_the_prior_decommission`).
  Two existing tests that relied on the old implicit reactivation
  (`a_recreated_cursor_continues_the_sequence_it_left_off_at`,
  `the_sequence_survives_a_decommission_that_purges_every_stamped_row`) were
  updated to call the new explicit `reactivate_cursor`.
