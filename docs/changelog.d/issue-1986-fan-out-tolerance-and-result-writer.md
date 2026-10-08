## Feature — fan-out failure tolerance and result writer (issue #1986)

Two options for the activity fan-out helpers, in the new `fan_out` module.

- **Failure tolerance.** `FanOutOptions::tolerate(FailureTolerance)` takes a
  count or a percentage. The percentage rounds down. A fan-out completes when
  at most that many items fail. It fails at one more with
  `HarvestError::FanOutFailureThresholdExceeded { tolerated, total }`. A
  windowed fan-out then dispatches no further wave.
  - Each wave polls every slot before it decides. So a workflow can catch the
    error and complete without an issue #1791 drift block.
  - A windowed resume now matches the recorded prefix by name and input. A
    fan-out that stopped early never claims a later activity.
- **Result writer.** `FanOutOptions::write_results()` tells the worker to
  write each item result through the `PayloadStore`.
  - The flag rides in the task row's `context_headers` as
    `x-harvest-result-writer`. No migration.
  - The worker encodes the result with the payload codecs before the `put`.
    The completion transaction adds the `harvest_payload_refs` row.
  - History records a fixed-size `StoredResult`. Replay fetches no blob.
    `StoredResult::fetch` reads one back.
  - A fresh dispatch without a store fails with `HarvestError::Config`. A
    worker without a store records the value inline.
- New entry points: `execute_activity_fan_out_with` and
  `execute_activity_fan_out_raw_with`. They return `FanOutResults`, a
  serializable manifest.
- **Breaking.** `WorkflowCommand::ScheduleActivity` has a new field,
  `result_writer`. A struct literal outside the crate must set it.

No migration. No new `WorkflowEvent` variant. No route change.

Tests: `fanout_tolerance_tests` (18 pure tests) and
`fanout_result_writer_db_tests` (real worker and Postgres). With the writer,
completed-event bytes per item are the same for 20 and 400 items and for
1 KiB and 64 KiB results.
