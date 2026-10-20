## Feature — fan-out failure tolerance and result writer (issue #1986)

Issue #1986 adds two options to the activity fan-out helpers. The new
`fan_out` module holds them.

- **Failure tolerance.** `FanOutOptions::with_tolerance(FailureTolerance)`
  takes a count or a percentage. The percentage rounds down. A fan-out
  completes when at most that many items fail. It fails at one more with
  `HarvestError::FanOutFailureThresholdExceeded { tolerated, total }`. A
  windowed fan-out then dispatches no further wave.
  - Each wave polls every slot before it decides. So a workflow can catch the
    error and go on without an issue #1791 drift block.
  - A fan-out that stops records a `fan_out_stop:{n}` marker with the number
    of slots it dispatched. Replay reads it ahead, so the fan-out never takes
    an activity that the workflow scheduled after the stop.
  - The stop consumes the start events of slots that still run, removes
    their waits and cancels them, as `ctx.race()` cancels its losers. So the
    next step and strict replay stay clean, and no result arrives after the
    workflow ends.
- **Result writer.** `FanOutOptions::with_result_writer(true)` tells the worker
  to write each item result through the `PayloadStore`.
  - The flag rides in the task row's `context_headers` as
    `x-harvest-result-writer`. No migration. The engine removes a copy that a
    caller supplies.
  - The worker encodes the result with the payload codecs and writes it once,
    before it takes any lock. The blob holds the activity id, so two runs
    never share a key. The completion transaction adds the
    `harvest_payload_refs` row.
  - History records a small `StoredResult`. Its size does not depend on the
    result. Replay fetches no blob. `StoredResult::fetch` reads one back, from
    an activity.
  - A fresh dispatch fails with `HarvestError::Config` when the workflow
    worker has no store. An activity worker without a store records the value
    inline, and fails a result that carries the reserved key.
  - With a store, the result cap (issue #252) does not apply to a writer row.
  - A transactional activity (`run_transactional`) writes its result the
    same way. Its reference row commits with its own transaction.
- New entry points: `execute_activity_fan_out_with` and
  `execute_activity_fan_out_raw_with`. They return `FanOutResults`, a
  serializable manifest.
- The SQLite runtime rejects a writer fan-out as unsupported.
- **Breaking.** `WorkflowCommand::ScheduleActivity` has a new field,
  `result_writer`. Outside the crate, a struct literal or a pattern without
  `..` must name it.
- History records the writer mode as `fan_out_writer:{n}`. Replay reads the
  mode from history, not from the current options.
- **Known gaps.** Each item still records its activity events, so event count
  grows with width. A windowed fan-out in a `join!` with other activity
  dispatch can mis-assign its resumed slots, as the `_windowed` helpers can. PII erasure and codec rotation do not touch result blobs.
  A manifest is valid only while the run that wrote it exists.

No migration. No new `WorkflowEvent` variant. No route change.

Tests: `fanout_tolerance_tests` (27 pure tests) and
`fanout_result_writer_db_tests` (a real worker on Postgres, with an
encrypting test codec). With the writer, completed-event bytes per item stay
within 8 bytes for 20 and 400 items, and for 1 KiB and 64 KiB results (477,
477 and 481 bytes; 87,581 inline). The store sees one upload per stored item
and no read during replay.
