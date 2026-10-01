## Change — retry jitter on by default; engine backoffs and cron fires jittered (issue #1792)

Activities that failed together used to retry together, at 1 s, 2 s, 4 s.
Cron schedules all fired on the same instant. Jitter now spreads both.
Every jittered value comes from a stable seed, so replay stays deterministic.

| Site | Before | After |
|---|---|---|
| `JitterPolicy::default()`, `RetryPolicy::exponential`, `fixed`, `default` | `None` | `Full`, `[0, base]` |
| Activity retry with no retry policy | fixed 1 s | `Full` over 1 s |
| Local-activity retry | seed `0` for every activity | seed from execution id and activity id |
| Non-determinism block re-dispatch | `5s * 2^n`, cap 300 s | `Equal`, `[base/2, base]` |
| Contained handler-panic re-dispatch | `1s * 2^(n-1)`, cap 30 s | `Equal`, `[base/2, base]` |
| `WorkflowSchedule::new` and `#[dag(schedule)]` with a cron expression | 0 s fire jitter | `DEFAULT_CRON_JITTER` (10 s) |

Design decisions:

- User retries use `Full`. It has the lowest total work, and the attempt cap
  bounds an early retry.
- The two re-dispatch loops use `Equal`. A floor of half the base keeps them
  from a hot loop. The non-determinism block has no attempt cap.
- A local activity used seed `0`, so all of them drew the same fraction. With
  jitter on by default, that would keep them in lockstep.
- The cron default skips an expression with a seconds field. Such a schedule
  can fire more often than the 10 s window. Interval and manual schedules
  keep zero jitter.
- `DEFAULT_CRON_JITTER` is a whole number of seconds, because
  `harvest_schedules.jitter_secs` stores whole seconds.
- `capability_miss_backoff` is unchanged. Its purpose is to stop a worker
  from claiming its own release again.

### Upgrade note

This changes default behavior. The next `docs/upgrading/` guide must carry
this note.

- A retry can now fire earlier than its old exact backoff, down to zero. To
  keep exact timing, set `.with_jitter(JitterPolicy::None)`. A stored policy
  with no `jitter` key now reads as `Full`.
- A cron schedule now fires up to 10 s after its slot. To keep the exact
  slot, call `.with_jitter(Duration::ZERO)`, or set `jitter = "0s"` on
  `#[dag]`. On the next startup, a re-registered cron schedule writes
  `jitter_secs = 10` to its `harvest_schedules` row.
- A non-determinism block now re-dispatches at least every 150 s at the
  cap, not every 300 s.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` write.

Tests: unit tests in `policy.rs` and `worker.rs` show that 100 tasks get more
than one delay, and that each delay stays in its band. A new property test in
`tests/property/policy_props.rs` checks that a default delay stays in
`[0, cap]` and repeats for one seed and attempt. `macros_dag.rs` covers the
DAG default and the `"0s"` opt-out.
