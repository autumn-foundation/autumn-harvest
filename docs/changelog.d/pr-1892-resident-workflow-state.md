## Feature — Resident workflow state on warm decisions (issue #1798, step 2)

**Behaviour change.** A warm cache entry now keeps the suspended workflow
itself, not only its events. The next decision on the same worker sends
the new result to the parked handler future and polls it again. It does not
replay history, so its replay work no longer grows with history length.
Step 1 (sticky routing on by default) shipped in #1869. Issue #1797
(deterministic readiness) made this step possible.

**Which suspensions stay resident.** A warm decision must decide exactly
as a cold replay would. The resident path therefore accepts a narrow set:

- The cycle awaits exactly one command: an activity, a timer or a signal.
  One awaited command means no race whose winner could differ.
- Each other command is a marker, a side effect, progress, current
  details, a log line or a search-attribute upsert.
- The context has no park token, push signal handler, held mutex, cancel
  request or non-determinism record, and no unread history.
- A signal wait is not for a name that a non-blocking claim probed with a
  scan that reached the end of history.
- The workflow is not hosted in a hot-swapped module.

**When a warm decision resumes.** A warm decision resumes only on an exact
delta. The delta holds the events of the last suspension, in order. Then it
holds exactly one event that resolves the awaited command: an activity
success, a timer fire or a signal. The start and heartbeat events of the
awaited activity may come before that event. The context inputs must not
have changed, for example the deadline, shard, build, queue, parent,
headers or handler. Any other delta declines. A decline drops the resident
state and runs a cold replay, which is always correct. Joins, races,
failures, updates, cancels and child workflows therefore still replay cold.

**Cache validation.** A hit now checks that the delta event ids run on
from the cached `next_event_id` with no gap. A gap drops the resident
state. A hit also takes the entry out of the cache. Only a committed
suspension, or a task that the wake ingest re-drives, puts it back. A stale
entry can no longer outlive an ND block, a panic, a deadlock, a pause park
or a rolled-back cycle. The take moves the snapshot instead of cloning it,
and the post-commit cache write moves it back. The write now comes before
the dispatch hints are published, so the next decision finds the entry. An
earlier snapshot never replaces a later one, and a displaced entry drops
outside the cache lock.

**Configuration.** `WorkerConfig::resident_workflows` (default `true`) and
`WorkerConfig::with_resident_workflows`. Turn it off to replay every
decision while the event cache stays warm. It has no effect when sticky
routing is off. The effective-config view reports the effective value.
`WorkerRuntimeConfig` gains the same field. A struct literal that lists
every field of either type must add it.

**Limits.** Resident state lives only in worker memory. A restart, a
failover, an LRU eviction or a decision that does not commit drops it, and
the next decision replays cold. A resident future keeps the state of a
foreign future, such as a raw `tokio::time::sleep`. Such a workflow is not
deterministic on a cold worker either.

**Metrics.** `harvest.workflow.cache_hit` and `harvest.workflow.cache_miss`
keep their meaning. A hit is a task served from the in-process cache with a
delta load. It counts whether the task resumes or replays. A miss is a full
history load. A resumed cycle records `harvest.replay = false` on its span,
and a decline logs its reason at `debug` level.

**Memory.** A resident entry also holds the parked future and its context.
The context keeps its own copy of the history, so a resident entry holds
two copies. The entry cap (`workflow_cache_size`) is unchanged.

**Internals.**

- `executor::drive_workflow` now builds the handler future as an
  `async move` block that owns an `Arc<WorkflowContext>`. The future is
  therefore `'static`, with no `unsafe` code. The strict and canary paths
  keep their borrowed future.
- The new `resident` module holds the resident state.
- A warm cycle appends the delta to the matcher as consumed events
  (`HistoryMatcher::append_consumed`). The replay position, `info()`,
  `history_event_count` and progress `seq` then match a cold replay.
- The matcher records signal names that a non-blocking claim probed at the
  end of history.

No migration. No new `WorkflowEvent` variant. No change to
`harvest_events`.

**Decision cost.** New `decision_cost_warm` group in
`benches/replay_bench.rs`. Measured on a 4-vCPU cloud container:

| History | `decision_cost` (cold replay) | `decision_cost_warm` (resident) |
|--------:|------------------------------:|--------------------------------:|
| 1,000 events | 0.13 ms | 2.8 µs |
| 5,000 events | 0.61 ms | 2.6 µs |
| 10,000 events | 1.25 ms | 2.8 µs |

A warm decision costs the same at every size. The group times the resume
only. The worker still loads the delta and persists the commands. The same
change skips a full-history scan in the end-of-cycle drift check when no
external request is stashed, which also helps cold decisions.

**Other doc.** `docs/rnd/sqlite-feasibility.md` now counts 121 core
modules, because `resident.rs` is new.

**Tests.**

- `resident.rs` unit tests drive the same workflow cold and warm and
  compare every decision. They then replay the warm history cold and
  compare the end. Fixtures cover an activity, timer and signal chain with
  side effects, progress and a history-length read, a replay-position read,
  a signal loop, a timer id reused in a loop, and a failure, a panic and a
  foreign wait after a resume. Two fixtures probe a signal before they wait
  for it, in one cycle and across cycles.
- Other unit tests show that joins, push handlers, conditions and unread
  history never stay resident. Each unsafe delta declines for its own
  reason, and so does a changed context key. A wait for a running activity
  after a cold replay stays resident.
- `cache.rs` unit tests cover the take, the resident switch and the
  later-snapshot rule.
- `tests/integration/sticky_default_tests.rs` runs real workers:
  - A warm worker runs the body from the top once in four decisions.
  - A two-signal delta falls back to a cold replay and completes.
  - With sticky routing off or resident workflows off, every decision
    replays.
  - LRU eviction drops the resident workflow, counts a miss and replays.
- `tests/integration/nd_block_tests.rs`: after a warm resident decision, a
  divergent build still replays cold and ND-blocks. A rollback completes
  the run.
