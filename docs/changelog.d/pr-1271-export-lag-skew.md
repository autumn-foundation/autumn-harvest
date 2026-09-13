## Phase — `harvest.audit.export_lag` no longer under-reports on commit-order skew (issue #1271)

**Bug fix. No new `WorkflowEvent` variant, no replay impact.** One migration:
widens an existing partial index; no new table, no new column.

### The bug

`export_lag_seconds` found the age of the oldest unacknowledged audit
record from two lookups: the minimum `occurred_at` among not-yet-sequenced
rows, and the `occurred_at` of the single **lowest-sequence** pending row.

The second lookup assumed the lowest-sequence pending row is also the
oldest. That does not hold: `export_seq` is assigned to rows the exporter
can *see*, and `occurred_at` is transaction start time. A long transaction
can commit, and so become visible, after a shorter one that started
later. The exporter then assigns the long transaction a *higher* sequence
while it carries an *older* `occurred_at`. When that row is pending, the
old lookup returned the newer row's timestamp instead, so the gauge — the
feature's outage detector — under-reported lag.

### The fix

The sequenced-but-unacknowledged lookup now takes `MIN(occurred_at)` over
the lowest `EXPORT_LAG_LOOKBACK_ROWS` (1000) pending sequences, not just
the single lowest one. This finds the true oldest row whenever the skew
resolves within that many rows, which covers every ordinary case, and its
cost stays fixed regardless of backlog size — the property the per-tick
gauge query must hold, since the backlog is largest during exactly the
sink outage the gauge exists to catch.

`harvest_audit_log_export_seq_idx` gains `occurred_at` as a second key
column (same leading column, same partial predicate), so the bounded scan
skips the heap fetch on a page the visibility map already covers. Every
existing use of the index — the claim scan's `ORDER BY export_seq LIMIT
n`, the redrive lookup's `MIN(export_seq)` — is unaffected. The migration
checks the index's key-column count before rebuilding it, so an operator's
out-of-band `CONCURRENTLY` pre-build (documented in the migration) makes
this migration's own statement a no-op instead of undoing it.

### Tests (TDD red → green → refactor)

* `audit_export_tests.rs`:
  `lag_is_not_fooled_by_a_late_committing_row_with_a_lower_sequence`
  reproduces the skew directly (a backdated row stamped with a *higher*
  `export_seq` than a recent row with a *lower* one) and asserts the
  reported lag tracks the older row. Confirmed failing pre-fix (~5s
  reported instead of ~200s). Also asserts the bounded gauge agrees with
  the exact admin-view query (`pending_and_lag`) on this input.
* `lag_window_still_catches_skew_at_its_last_covered_position` and
  `lag_window_does_not_reach_skew_just_beyond_it` pin the
  `EXPORT_LAG_LOOKBACK_ROWS` boundary itself: a skewed row at the window's
  last position is found, and one immediately past it is not — proving the
  bound is wired to the documented constant, not off by one in either
  direction.
