## Phase X.Y — Conformance: exact retry backoffs in the corpus (issue #2517)

`race_timer_wins` failed in the coverage nightly. On Postgres, the activity
won the race against a one-second timer. Its "60 s" retry backoff came from
`RetryPolicy::fixed`, which applies full jitter. The Postgres worker drew a
delay in `[0, 60 s]` from a seed of random ids. `SQLite` waits the base
delay. About one run in sixty drew a delay under the timer.

- The corpus wraps each positive backoff in `exact`, which sets
  `JitterPolicy::None`. That covers `slow_retry`, `very_slow_retry` and
  `gated_retry`.
- `corpus::all_activities` lists every corpus activity. `RACE_TIMER` names
  the timer of `race_timer_wins`.
- `timing.rs` adds guards that run without Docker. Each corpus activity
  has a retry policy and is in `all_activities`. Each corpus retry
  waits its base delay for 1000 seeds. The race loser retries after the race
  timer, with one second to spare.

Test-only change. No engine change, no new `WorkflowEvent` variant, no
migration. Both guards failed before the fix: seed 18 gave a 1.49 s retry.
The full suite passes on Postgres 16, and `race_timer_wins` passed 60 of 60
repeated runs.
