## Engine — Default history event cap and byte cap (issue #1804)

Before this change, a runaway loop grew `harvest_events` and replay CPU
without bound. The only default was the advisory `should_continue_as_new` at
10,000 events. The worker now caps each run's history by default.

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
  `DEFAULT_HISTORY_EVENT_HARD_CAP` and `DEFAULT_HISTORY_BYTE_HARD_CAP`, and
  `WorkflowHistoryPolicy::history_bloat_warn_threshold()`. The crate root
  now re-exports the three history defaults.

Design decisions:

- The defaults follow Temporal: warn at 10,240 events, terminate at 51,200.
  The warning stays a fraction of the cap, so `0.2` puts it at 10,000.
- The byte measure is `pg_column_size(event_data)`, the same measure as the
  tenant `max_history_bytes` quota.
- The worker measures the stored bytes once, at the start of each decision.
  Two gates use the measure. The first stops a run before an inline local
  activity runs a side effect. The second sits next to the event-cap
  preflight and uses the same `continue_as_new` exemption. The run can
  overshoot the cap by one decision's appends.
- The byte sum is incremental. `WorkflowCache` keeps a crate-private
  `HistoryBytesMark` with each entry. A warm decision sums only the events at
  or after the mark, with one small indexed query. The sum has an upper
  event-id bound, so a concurrent append is never counted twice. A cold
  decision reads the sum from its full history load, so it scans no extra
  rows.
- A codec rotation rewrites rows in place and can change their size, so a
  warm mark can drift. Every 64 warm decisions the worker sums the full
  history again. An incremental sum at or above the cap never fails a run
  alone: the worker sums the full history first.
- A failed byte measure skips the byte check for that decision only. It
  logs a warning and does not fail the task.
- The history-bloat `COUNT(*)` now runs only when the in-memory prospective
  count crosses the threshold. Small runs pay no extra query now that the
  warning is on by default. The prospective count usually over-counts. When
  it is low, the warning fires one decision later.
- `try_build` logs a warning when the event cap is at or below
  `history_continue_as_new_threshold`. The advisory can then never fire
  before the cap. It also warns when the warning point is below the
  threshold, because healthy runs then page. The README and
  `examples/long_running.rs` now use caps that avoid both cases.
- The warn-threshold formula moves from `worker.rs` to
  `context::history_bloat_warn_threshold`, with its rationale. The worker,
  the builder check and the tests share it.
- `HistoryCapBreach` names the cap a run reached. The local-activity cap
  gates, the preflight and `fail_workflow_for_history_cap` all pass it.
- The scanner ceiling `max_workflow_history_events` (#493) stays opt-in. The
  worker cap covers the runaway-loop case without a per-tick scan.
- The SQLite backend does not enforce either cap.

### Upgrade note

This changes default behaviour. `docs/upgrading/0.7.0.md` §1.3 carries the
note: who is affected, how to raise a cap or restore `unlimited`, and a SQL
query that finds the runs a cap would fail.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` write.

Tests: `tests/integration/history_default_caps_tests.rs` runs a real worker
against Postgres:

- Under the default policy, a run at 50,000 events fails with
  `HistoryCapExceeded { count: 50_000, cap: 50_000 }` and emits the warning.
- Under the default policy, a run at 10,000 events emits
  `harvest.workflow.history_bloat` once and stays `RUNNING`. A run at 9,999
  events does not.
- Under the default policy, a run at 50 MiB fails with
  `HistoryBytesCapExceeded`.
- A signal loop reaches a 64 KiB cap on the warm path and on the cold path.
  The reason carries exactly the stored bytes. Small signals in between
  prove the sum does not double-count.
- A stale warm mark at or above the cap does not fail a run whose stored
  bytes are below it.
- A run over the byte cap runs no inline local activity.
- A run over the byte cap can still `continue_as_new`.
- Explicit `unlimited` caps keep a run past 50,000 events and 50 MiB alive.

Unit tests cover the defaults, the overrides, the warn threshold, both
build-time warnings, the breach helpers, the new DLQ tags and the cache mark.
