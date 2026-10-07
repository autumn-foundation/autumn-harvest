## Fix — business-day scan bound and skip-scan date limits (issue #1968)

Three rough edges in `calendar.rs`, from the 2026-10-07 session report.

- **Monotone scan bound.** `add_business_days` used a total scan bound of
  `n * 7 + 30` days. The bound grew with `n`, so a long closure rejected a
  small `n` and accepted a larger `n`. The scan now rejects a run of more than
  30 consecutive non-business days, whatever `n` is. A smaller `n` thus never
  rejects where a larger `n` resolves.
  - **Behavior change.** A closure of more than 30 days now rejects every `n`.
    Before, a large `n` could cross it. `n = 0` keeps its old accept set.
  - `ScanExhausted { scanned_days }` now gives the length of the run with no
    business day (31). Recorded runs do not change: the workflow freezes each
    result, and replay does not run the scan.
- **Doc drift.** The docs gave the window as `n * 7 + 14`. They now give the
  run bound.
- **No panic at the date limits.** `apply_skip_policy` panicked at
  `NaiveDate::MAX` with `RunNextBusinessDay` and at `NaiveDate::MIN` with
  `RunPrevBusinessDay`. The scan now uses checked steps and returns `None`.

No migration. No new `WorkflowEvent` variant. No route change.

Tests: five unit tests in `calendar.rs` and the `business_day_props`
property suite (monotone in `n`; no panic at the range limits). Each failed
before the fix.
