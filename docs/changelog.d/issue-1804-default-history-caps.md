## Engine — Default history event cap and byte cap (issue #1804)

A runaway loop used to grow `harvest_events` and replay CPU without bound.
The only default was the advisory `should_continue_as_new` at 10,000 events.
The worker now caps each run's history by default.

| Setting | Before | After |
|---|---|---|
| `WorkflowHistoryPolicy::event_hard_cap` | `None` | `Some(50_000)` |
| `WorkflowHistoryPolicy::byte_hard_cap` (new) | — | `Some(50 MiB)` |
| `DEFAULT_HISTORY_BLOAT_WARN_FRACTION` | `0.75` | `0.2` |

- A run at the event cap fails and moves to the DLQ with the existing typed
  `HistoryCapExceeded { count, cap, workflow_type }` reason.
- A run at the byte cap fails the same way with the new typed
  `DeadLetterReason::HistoryBytesCapExceeded { bytes, cap, workflow_type }`.
- `harvest.workflow.history_bloat` now fires at 10,000 events by default.
  The worker also logs a `tracing::warn!` when it fires.
- New overrides: `history_byte_hard_cap(n)`,
  `history_event_hard_cap_unlimited()` and
  `history_byte_hard_cap_unlimited()` on `HarvestBuilder`;
  `with_byte_hard_cap`, `without_event_hard_cap` and `without_byte_hard_cap`
  on `WorkflowHistoryPolicy`. New constants
  `DEFAULT_HISTORY_EVENT_HARD_CAP` and `DEFAULT_HISTORY_BYTE_HARD_CAP`.

Design decisions:

- The defaults follow Temporal: warn at 10,240 events, terminate at 51,200.
  The warning stays a fraction of the cap, so `0.2` puts it at 10,000.
- The byte measure is `pg_column_size(event_data)`, the same measure as the
  tenant `max_history_bytes` quota.
- The byte check runs once per decision, next to the event-cap preflight. It
  uses the same `continue_as_new` exemption. It can overshoot the cap by one
  decision's appends.
- The byte sum is incremental. `WorkflowCache` keeps a private
  `HistoryBytesMark` with each entry. A warm decision sums only the events at
  or after the mark, and a cold decision sums the whole history. The sum has
  an upper event-id bound, so a concurrent append is never counted twice.
- The history-bloat `COUNT(*)` now runs only when the in-memory prospective
  count crosses the threshold. That count never under-counts the worker's own
  appends, so small runs pay no extra query now that the warning is on by
  default.
- `try_build` logs a warning when the event cap is at or below
  `history_continue_as_new_threshold`. The advisory can then never fire
  before the cap.
- The scanner ceiling `max_workflow_history_events` (#493) stays opt-in. The
  worker cap covers the runaway-loop case without a per-tick scan.
- The SQLite backend does not enforce either cap.

### Upgrade note

This changes default behaviour. `docs/upgrading/0.7.0.md` §1.2 carries the
note: who is affected, how to raise a cap or restore `unlimited`, and a SQL
query that finds the runs a cap would fail.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` write.

Tests: `tests/integration/history_default_caps_tests.rs` runs a real worker
against Postgres. A run at 50,000 events fails with `HistoryCapExceeded`
under the default policy. A run at 10,000 events emits
`harvest.workflow.history_bloat` once and stays `RUNNING`. A signal loop
reaches a 64 KiB byte cap on the warm cache path and fails with
`HistoryBytesCapExceeded`; small signals in between prove the sum does not
double-count. An explicit `unlimited` keeps a run past 50,000 events alive.
Unit tests cover the defaults, the overrides, the new DLQ tags, the cache
mark and the 10,000-event boundary.
