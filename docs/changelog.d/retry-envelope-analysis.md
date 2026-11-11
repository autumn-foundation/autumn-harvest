## Phase X.Y — Retry envelope analysis

`retry_envelope::analyze` checks a `RetryPolicy` against the activity
timeouts. It reports the earliest and the latest start of each attempt.

- `possible_attempts` counts the attempts that can start before the
  `schedule_to_close` deadline. `unreachable_attempts` counts the configured
  attempts that can never run.
- `guaranteed_attempts` counts the attempts that keep a full `start_to_close`
  window even in the worst case.
- `retry_delay_bounds` gives the jitter range of each retry. A test proves
  that every seeded delay falls inside it.
- `analyze_defaults` reads an `ActivityDefaults` value directly.
- The module is pure and sends no SQL.
