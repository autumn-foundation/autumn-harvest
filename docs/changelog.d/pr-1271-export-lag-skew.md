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
is index-only. Every existing use of the index — the claim scan's `ORDER
BY export_seq LIMIT n`, the redrive lookup's `MIN(export_seq)` — is
unaffected.

### Tests (TDD red → green → refactor)

* `audit_export_tests.rs`:
  `lag_is_not_fooled_by_a_late_committing_row_with_a_lower_sequence`
  reproduces the skew directly (a backdated row stamped with a *higher*
  `export_seq` than a recent row with a *lower* one) and asserts the
  reported lag tracks the older row. Confirmed failing pre-fix (~5s
  reported instead of ~200s).
