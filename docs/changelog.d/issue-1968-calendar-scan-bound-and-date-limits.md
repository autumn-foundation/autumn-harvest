## Fix — business-day scan bound and skip-scan date limits (issue #1968)

Three defects in `calendar.rs`.

- **Monotone scan bound.** `add_business_days` used a total scan bound of
  `n * 7 + 30` days. The bound grew with `n`, so a long closure rejected a
  small `n` and accepted a larger `n`. The scan now rejects a run of more than
  30 consecutive non-business days, whatever `n` is. So a smaller `n` never
  rejects where a larger `n` resolves.
  - **Behavior change.** A closure of more than 30 days now rejects every `n`.
    Before, a large `n` could cross it. A large `n` across many closures of
    30 days or fewer now resolves. Before, it rejected when the total passed
    `n * 7 + 30`. `n = 0` keeps its old accept set.
  - `ScanExhausted { scanned_days }` is now always 31: the consecutive
    non-business days the scan examined.
  - `ctx.timer_business_days` results in recorded histories do not change. The
    workflow freezes each result, and replay does not run the scan. A direct
    caller of `add_business_days` gets the new accept set and the value 31.
  - The worst-case `skipped` list grows from 25,581 to 109,530 dates. Only a
    calendar with one business day in each 31 days for 300 years gets there.
- **Doc drift.** The docs gave the window as `n * 7 + 14`. They now give the
  run bound.
- **No panic at the date limits.** `apply_skip_policy` panicked at
  `NaiveDate::MAX` with `RunNextBusinessDay` and at `NaiveDate::MIN` with
  `RunPrevBusinessDay`. The scan now uses checked steps and returns `None`.

No migration. No new `WorkflowEvent` variant. No route change.

Tests: seven unit tests in `calendar.rs` and the `business_day_props`
property suite (monotone in `n`; no panic at the range limits). All except
`a_month_long_closure_resolves_for_every_n` fail on the old code.
