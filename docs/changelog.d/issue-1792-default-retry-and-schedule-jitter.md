## Engine — Retry, re-dispatch and cron fire jitter on by default (issue #1792)

Activities that failed together used to retry together, at 1 s, 2 s, 4 s.
Cron schedules all fired on the same instant. Jitter now spreads both. Each
jittered value comes from a stable seed, so one build always computes the
same value.

| Site | Before | After |
|---|---|---|
| `JitterPolicy::default()`, `RetryPolicy::exponential`, `fixed`, `default` | `None` | `Full`, `[0, base]` |
| Activity retry and workflow retry (#523) under those policies | exact backoff | `Full` |
| Activity retry with no retry policy | fixed 1 s | `Full` over 1 s |
| Local-activity retry | seed `0` for every activity | seed from execution id and activity id |
| Non-determinism block re-dispatch | `5s * 2^n`, cap 300 s | `Equal`, `[base/2, base]` |
| Contained handler-panic re-dispatch | `1s * 2^(n-1)`, cap 30 s | `Equal`, `[base/2, base]` |
| `WorkflowSchedule::new` and `#[dag(schedule)]` with a cron | 0 s fire jitter | `DEFAULT_CRON_JITTER` (10 s) |
| `POST /admin/schedules/workflow` and `/preview` with no `jitter_secs`, cron | 0 s | 10 s |
| `PATCH /admin/schedules/{id}` that changes the cadence, no `jitter_secs` | keeps the stored jitter | re-derives a defaulted jitter |

Design decisions:

- User retries use `Full`. It gives the lowest total work. `Full` can retry
  with almost no delay, but the attempt cap limits how often that happens.
- The two re-dispatch loops use `Equal`. Each delay is at least half the
  base, so neither loop can become a hot loop. The non-determinism block has
  no attempt cap.
- Local activities now get a seed from the execution id and the activity id.
  With seed `0`, all local activities draw the same fraction and stay in
  lockstep.
- The cron default skips an expression with a seconds field. Such a schedule
  can fire more often than once per 10 s. Interval and manual schedules keep
  zero jitter.
- `DEFAULT_CRON_JITTER` is a whole number of seconds, because
  `harvest_schedules.jitter_secs` stores whole seconds.
- A PATCH that changes the cadence re-derives the jitter only when the stored
  value equals the default of the old cadence. Any other stored value is
  explicit and stays.
- `#[dag(jitter = "0s")]` opts out. The macro now checks the `jitter` string
  at compile time.
- `harvest schedule create-workflow` gains `--jitter-secs`.
- `JitterPolicy` is now re-exported from the crate root and the prelude.
- Three backoffs are unchanged. `capability_miss_backoff` stops a worker
  from claiming its own release again; no cohort retries together there. The
  completion-callback default delivery policy keeps `JitterPolicy::None`. The
  SQLite backend retries at the base delay, as before.

### Upgrade note

This changes default behavior. The next `docs/upgrading/` guide must carry
this note.

- **Workflow timers from a retry policy.** Replay compares each recorded
  timer duration with the duration the code computes. A workflow that sets a
  timer from `policy.next_delay(..)` or `next_delay_with_seed(..)` on a
  constructor-built policy now computes a different duration. An in-flight
  run recorded before the upgrade then blocks on a timer mismatch. Pin
  `.with_jitter(JitterPolicy::None)`, or gate the change with `ctx.patched`.
- A retry can now fire earlier than its old exact backoff, down to zero. To
  keep exact timing, set `.with_jitter(JitterPolicy::None)`.
- Stored task, execution and schedule rows keep their serialized `jitter`
  value. A policy that a client sends with no `jitter` key now reads as
  `Full`. That includes `retry_policy` on the schedule create and PATCH
  routes.
- A cron schedule now fires up to 10 s after its slot. To keep the exact
  slot, call `.with_jitter(Duration::ZERO)`, set `jitter = "0s"` on `#[dag]`,
  or send `"jitter_secs": 0`. The create route is an upsert, so a re-POST with
  no `jitter_secs` also sets 10 s.
- On the next startup, each re-registered cron schedule writes
  `jitter_secs = 10` once. A schedule whose `retry_policy` comes from a
  constructor also writes `"jitter":"Full"` once.
- At the cap, a non-determinism block now waits 150 s to 300 s between
  re-dispatches, not exactly 300 s.

No migration, no new `WorkflowEvent` variant, and no `harvest_events` write.

Tests: unit tests in `policy.rs` and `worker.rs` show that 100 tasks do not
all get the same delay. They also show that each delay stays in its band. A
property test in `tests/property/policy_props.rs` checks that a default delay
stays in `[0, cap]` and repeats for one seed and attempt. `nd_block_tests.rs`
checks each requeue delay against its `Equal` band. Other tests cover the
PATCH merge, the plugin `jitter_secs` default, the CLI flag, the macro
duration grammar and the DAG `"0s"` opt-out in `macros_dag.rs`.
