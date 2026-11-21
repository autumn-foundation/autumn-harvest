# Pre-deploy analyses

Harvest ships a set of pure analyses that answer capacity questions before
a deploy. Each one reads a definition or a policy and returns a report. None
of them sends SQL, writes an event or needs a migration. So you can call them
from a unit test, a CI check or a build script.

| Question | Module | Entry point |
|---|---|---|
| How long can each DAG task slip? | `dag_slack` | `DagSlackAnalyzer::analyze` |
| Which DAG task should get faster first? | `dag_leverage` | `DagLeverage::rank` |
| What is the cheapest way to meet a DAG deadline? | `dag_crash` | `DagCrashPlanner::plan` |
| How many worker slots does one DAG run need? | `dag_concurrency` | `profile`, `peak_bounds` |
| How long can a DAG run take when every task retries? | `dag_retry_envelope` | `DagRetryEnvelope::analyze` |
| Which configured attempts can a deadline let run? | `retry_envelope` | `analyze`, `analyze_defaults` |
| How much load does an outage add to a dependency? | `retry_storm` | `simulate` |
| How late does a throttled burst of starts run? | `throttle_forecast` | `forecast`, `forecast_schedules`, `steady_state` |
| Which run shapes are usual, and which are rare? | `shape_census` | `ShapeCensus` |
| Which schedules fire at the same moment? | `schedule_herd` | `forecast`, `forecast_shifted` |

Each module is public in the `autumn_harvest` crate and needs no cargo
feature. The schedule herd forecast also has a management route and a CLI
command. See [Schedule herds](#schedule-herds-schedule_herd).

## DAG analyses

The DAG analyses build on the critical path. `CriticalPathAnalyzer` in
`critical_path.rs` finds one longest path through a `DagDefinition`. See
[`performance-critical-path.md`](performance-critical-path.md) for its cost.
The modules below answer the next questions.

Each DAG analysis takes a `DagDefinition`. Build one with `DagBuilder`. A
`#[dag]` function exposes its builder function in `DagInfo::builder`.

### Task durations

`DagSlackAnalyzer` and `DagRetryEnvelope` pick the duration of each task in
this order:

1. the `start_to_close` of the task, from `.start_to_close(..)`;
2. a duration that you set with `mock_duration(name, duration)`;
3. the default duration, one second unless you call `with_default_duration`.

So a task with a timeout uses its timeout, not your mock. The name is the
activity name of the task. `CriticalPathAnalyzer` uses the same order.

### Slack (`dag_slack`)

`DagSlackAnalyzer` runs the critical path method. A forward pass finds the
earliest start of each task. A backward pass finds the latest start that
still meets the run time. The difference is the slack: how long the task can
slip before it delays the run.

- `DagSlackAnalyzer::new(dag)`, then `mock_duration` and
  `with_default_duration`, then `analyze()`.
- `DagSlackReport` holds `makespan` and one `TaskSlack` per task, in
  topological index order.
- `TaskSlack` holds `index`, `name`, `duration`, `earliest_start`,
  `earliest_finish`, `latest_start`, `latest_finish` and `slack`.
  `is_critical()` is `true` when the slack is zero.
- `DagSlackReport::critical_indices()` lists every zero-slack task in
  ascending order. Unlike `CriticalPathAnalyzer`, it reports all tied paths.
- `DagSlackReport::most_flexible()` returns the task with the most slack.
  It is the cheapest task to move to slower or cheaper capacity.

### Leverage (`dag_leverage`)

Slack says how far a task can slip. Leverage asks the reverse question: how
much faster does the run get when one task gets faster?

- `DagLeverage::new(&dag, &report)` takes the DAG and its slack report. It
  panics when the two hold a different number of tasks.
- `rank()` returns one `TaskLeverage` per task. Each one holds
  `makespan_if_free` and `saved`: the run time when the task takes no time,
  and the time that this saves. The largest saving comes first. Equal
  savings keep index order.
- `makespan_with(index, duration)` returns the run time for any other
  duration of one task. It returns `None` for an index out of range.

A task off the critical path saves nothing. A critical task saves up to the
gap to the next longest path.

### Crash plan (`dag_crash`)

A crash plan answers the cheapest way to make a run meet a deadline. It
uses greedy project crashing.

- `DagCrashPlanner::new(&dag, &report)` takes the DAG and its slack report.
  It panics on a task count mismatch, as `DagLeverage` does.
- `crashable(name, floor, cost_per_second)` lets an activity shrink to
  `floor`. Each second it gives up costs `cost_per_second`, in your own
  units. A task with no option keeps its duration.
- `plan(target)` returns a `CrashPlan`. Each step cuts the task that buys
  one second of run time at the lowest price. Equal prices keep index order.
  The plan buys only the useful part of a cut, so it never pays for extra
  slack.

`CrashPlan` holds `target`, `baseline`, `achieved`, `total_cost`, the new
`durations` and the `steps`. Each `CrashStep` holds `index`, `name`,
`shortened_by`, `cost` and `makespan_after`. A step cost rounds up to a
whole unit.

`meets_target()` is `false` when no single cut helps. Two tied parallel paths
cause this: a cut on one path leaves the other path as long as before. The
planner then stops. It does not cut two tasks together.

### Worker demand (`dag_concurrency`)

`dag_concurrency` turns a slack report into a step function of running
tasks. It answers how many worker slots a run needs to meet its makespan.

- `profile(&report, Schedule::Earliest)` starts every task at its earliest
  start. `Schedule::Latest` starts every task at its latest start.
  `Schedule` here is `dag_concurrency::Schedule`, not the cron type.
- `ConcurrencyProfile` holds `segments`. Each `Segment` holds `start`, `end`
  and `running`. Idle spans are omitted.
- `peak()` gives the most tasks that run at once. `peak_at()` gives the
  start of the first span that reaches the peak. `task_time()` gives the
  total task time, which is the same for both schedules.
- `peak_bounds(&report)` returns `(lower, upper)`: the smaller and the
  larger peak of the two schedules. The gap shows how much slack can level
  the load.

Back-to-back tasks do not count as concurrent. A task with zero duration
adds no demand. The two schedules are bounds only. A run with fewer than
`lower` slots can still meet its makespan when slack levels the load, but
these two schedules do not prove it.

### Retry envelope of a DAG (`dag_retry_envelope`)

The critical path assumes that every task succeeds at its first attempt.
`DagRetryEnvelope` asks how long the run takes when every task uses all of
its retries.

- `DagRetryEnvelope::new(dag)`, then `mock_duration`,
  `with_default_duration` and `with_fallback_policy`, then `analyze()`.
- The duration of a task bounds one attempt. Each task then gets the worst
  case of [`retry_envelope`](#retry-envelope-retry_envelope): every attempt
  runs to its bound, and jitter picks the longest backoff.
- `with_fallback_policy(policy)` sets the retry policy of each task that sets
  none. Pass the registered default policy of your worker. With no fallback,
  a task without `.retry(..)` runs once.
- A DAG task sets no `schedule_to_close`, so no deadline cuts the retries
  short.

`DagRetryEnvelopeReport` holds:

| Field or method | Meaning |
|---|---|
| `nominal_makespan`, `nominal_path` | The run time and one critical path when each task succeeds at once. |
| `worst_case_makespan`, `worst_case_path` | The same when each task uses all of its retries. |
| `tasks` | One `TaskRetryEnvelope` per task: `attempt_bound`, `attempts`, `worst_case` and `inflation()`. |
| `inflation()` | The time that retries can add to the whole run. |
| `path_shifts()` | `true` when retries move the critical path to other tasks. |
| `unbounded_indices()` | The tasks with no worst-case bound. |

A short task with a long retry budget can overtake a long task with no
retries. `path_shifts()` shows that case. A tie between two paths goes to
the lower index, so each path is deterministic.

A policy with more than `retry_envelope::MAX_LISTED_ATTEMPTS` (10,000)
attempts has no bound. That task then has `worst_case = None`. The
`worst_case_makespan` is `None` and the `worst_case_path` is empty.

The analysis calls `retry_envelope::worst_case_without_deadline`, so it
builds no attempt list per task. See
[`performance-dag-retry-envelope.md`](performance-dag-retry-envelope.md).

### Example

The diamond below runs `a`, then `b` and `c` in parallel, then `d`.

```rust
use std::time::Duration;
use autumn_harvest::dag::DagBuilder;
use autumn_harvest::dag_concurrency::peak_bounds;
use autumn_harvest::dag_crash::DagCrashPlanner;
use autumn_harvest::dag_leverage::DagLeverage;
use autumn_harvest::dag_retry_envelope::DagRetryEnvelope;
use autumn_harvest::dag_slack::DagSlackAnalyzer;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};

fn a() {}
fn b() {}
fn c() {}
fn d() {}

let secs = Duration::from_secs;
let mut builder = DagBuilder::new();
let ta = builder.activity(a);
let tb = builder.activity(b).upstream(&ta);
let tc = builder
    .activity(c)
    .upstream(&ta)
    .retry(RetryPolicy::fixed(4, secs(1)).with_jitter(JitterPolicy::None));
let _td = builder.activity(d).upstream(&tb).upstream(&tc);
let dag = builder.build().unwrap();

let report = DagSlackAnalyzer::new(dag.clone())
    .mock_duration("a", secs(2))
    .mock_duration("b", secs(10))
    .mock_duration("c", secs(3))
    .mock_duration("d", secs(1))
    .analyze();
assert_eq!(report.makespan, secs(13));
assert_eq!(report.critical_indices(), vec![0, 1, 3]);
assert_eq!(report.tasks[2].slack, secs(7));

// Making `b` free saves 7 s. Then the path through `c` sets the run time.
let ranking = DagLeverage::new(&dag, &report).rank();
assert_eq!((ranking[0].name.as_str(), ranking[0].saved), ("b", secs(7)));

// Cut `b` to meet an 8 s deadline, at 5 units per second.
let plan = DagCrashPlanner::new(&dag, &report)
    .crashable("b", secs(1), 5)
    .plan(secs(8));
assert!(plan.meets_target());
assert_eq!(plan.total_cost, 25);

// `b` and `c` overlap in both schedules.
assert_eq!(peak_bounds(&report), (2, 2));

// With all four attempts, `c` takes 3 + 3 * (1 + 3) = 15 s and overtakes `b`.
let envelope = DagRetryEnvelope::new(dag)
    .mock_duration("a", secs(2))
    .mock_duration("b", secs(10))
    .mock_duration("c", secs(3))
    .mock_duration("d", secs(1))
    .analyze();
assert_eq!(envelope.worst_case_makespan, Some(secs(18)));
assert!(envelope.path_shifts());
```

## Retry analyses

### Retry envelope (`retry_envelope`)

A `RetryPolicy` promises `max_attempts` attempts. A `schedule_to_close`
deadline bounds all attempts and backoffs together. The two settings live in
different places, so a policy can promise attempts that the deadline never
lets run. `retry_envelope::analyze` finds them.

- `analyze(&policy, start_to_close, schedule_to_close)` takes the two
  timeouts as `Option<Duration>`. Pass `None` for a timeout that the
  activity does not set.
- `analyze_defaults(&defaults)` reads an `ActivityDefaults` value. It
  returns `None` when the value sets no retry policy. `ActivityDefaults::of`
  builds the code values of an `ActivityInfo`.
- `worst_case_without_deadline(&policy, start_to_close)` returns the
  configured attempts and the worst-case total, with no attempt list.

`RetryEnvelope` lists one `AttemptWindow` per attempt that can start before
the deadline. Each window holds `attempt` (1-based), `min_backoff`,
`max_backoff`, `earliest_start`, `latest_start` and `latest_finish`. Offsets
count from the schedule time.

| Method | Meaning |
|---|---|
| `possible_attempts()` | The attempts that can start before the deadline. |
| `unreachable_attempts()` | The configured attempts that can never start. Above zero, the deadline cuts the policy short. |
| `guaranteed_attempts()` | The attempts whose latest finish is at or before the deadline, so each one gets a full `start_to_close` window. |
| `worst_case_total()` | The latest finish of the last attempt, capped at the deadline. `None` when neither timeout bounds it. |

The earliest start assumes that each attempt fails at once and that jitter
picks the shortest delay. The latest start assumes that each attempt runs
to its `start_to_close` bound and that jitter picks the longest delay. Both
bounds hold for every jitter seed, so the report needs no sampling.

`retry_delay_bounds(&policy, retry)` gives the jitter range of one retry.
`retry` is 1-based, as in `RetryPolicy::next_delay_with_seed`. A test proves
that every seeded delay falls inside the range. See
[`retry-jitter.md`](retry-jitter.md) for the jitter modes.

`saturates_at` names the first attempt whose base backoff reaches
`max_interval`. Later attempts gain nothing from the backoff coefficient.

Limits:

- The analysis covers the delays of the policy only. An
  `ActivityFailure::with_retry_after` hint replaces the policy delay, so the
  bounds do not apply to it.
- The list stops at `MAX_LISTED_ATTEMPTS` (10,000) attempts. `truncated` is
  then `true`, `unreachable_attempts()` is 0 and `worst_case_total()` is the
  deadline.

```rust
use std::time::Duration;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::retry_envelope::analyze;

let secs = Duration::from_secs;
// Backoffs of 1, 2, 4 and 8 s, 5 s per attempt, 20 s in total.
let policy = RetryPolicy::exponential(6, secs(1)).with_jitter(JitterPolicy::None);
let envelope = analyze(&policy, Some(secs(5)), Some(secs(20)));
assert_eq!(envelope.possible_attempts(), 5);
assert_eq!(envelope.unreachable_attempts(), 1);
assert_eq!(envelope.guaranteed_attempts(), 3);
assert_eq!(envelope.worst_case_total(), Some(secs(20)));
```

### Retry storm (`retry_storm`)

A `RetryPolicy` bounds the retries of one call. It does not bound the
retries of many calls together. `retry_storm::simulate` tells how much load
an outage adds to a dependency, and when that load lands.

`simulate(&policy, budget, &scenario)` replays a steady stream of calls
through one outage. `StormScenario` holds:

| Field | Meaning |
|---|---|
| `arrivals_per_sec` | First attempts each second. |
| `traffic` | Calls arrive over `[0, traffic)`. |
| `outage_start`, `outage_end` | An attempt that starts in `[outage_start, outage_end)` fails. Any other attempt succeeds. |
| `bucket` | The width of one report bucket. |
| `horizon` | The simulation stops here. Later work counts as pending. |

`StormReport` holds one `StormBucket` per bucket, with `first_attempts`,
`retries` and `failures`. It also holds `calls`, `retries`, `succeeded`,
`exhausted`, `deferrals`, `pending_at_horizon` and `last_attempt`.

| Method | Meaning |
|---|---|
| `peak()` | The bucket with the most attempts. A tie goes to the earliest bucket. |
| `peak_ratio()` | The peak bucket against the load with no outage. 3.0 means three times the normal load. |
| `amplification()` | Attempts per call. 1.0 means that no call retries. |
| `drained_at()` | The last attempt, or `None` when work is still pending at the horizon. |

`exhausted` counts the calls whose last permitted attempt fails.

Each retry delay comes from `RetryPolicy::next_delay_with_seed`, seeded by
the call index. So the forecast shows how much jitter flattens a herd.

Pass a `RetryBudgetPolicy` as `budget` to model the retry budget. It runs
through a real `RetryBudgetRegistry`. One budget covers all calls, as for one
activity type on one worker. A retry that the budget defers waits and keeps
its attempt, as on a worker. `deferrals` counts each deferral, and one retry
can defer often. See decision 11 in
[`architecture.md`](architecture.md#key-design-decisions) for the budget.

Model limits:

- An attempt takes no time. A failure schedules the next attempt at once.
- The model ignores `schedule_to_close`. Use `retry_envelope` for the
  attempts that a deadline cuts.
- A run processes at most `MAX_SIMULATED_EVENTS` (5,000,000) events. A larger
  scenario returns `StormError::TooManyEvents`. A bad rate, a zero bucket or
  an outage that ends before it starts also returns a `StormError`.

Each run is deterministic. The overflow deferral of the budget draws from a
seed, not from `rand`. The module reads the clock once, as the base instant
of the budget, and no result depends on that instant.

```rust
use std::time::Duration;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::retry_storm::{StormScenario, simulate};

let policy = RetryPolicy::fixed(5, Duration::from_secs(1)).with_jitter(JitterPolicy::None);
let scenario = StormScenario {
    arrivals_per_sec: 100.0,
    traffic: Duration::from_secs(20),
    outage_start: Duration::ZERO,
    outage_end: Duration::from_secs(10),
    bucket: Duration::from_secs(1),
    horizon: Duration::from_secs(600),
};
let report = simulate(&policy, None, &scenario).unwrap();
// Five attempts for each call: the outage sends five times the load.
assert_eq!(report.peak_ratio(), 5.0);
```

The alert `harvest_activity_retry_storm` watches the live retry rate. See
[its runbook](runbooks/harvest-alerts.md#harvest_activity_retry_storm).

## Throttle forecast (`throttle_forecast`)

A `ThrottlePolicy` paces workflow starts with a token bucket. Many schedules
of one workflow type often share one cron expression. They then reach one
bucket at the same instant. `throttle_forecast` tells how late the last
start runs, and whether `schedule_to_start` drops part of the burst.

- `forecast(&policy, &arrivals)` replays a list of `Arrival { at, count }`
  bursts through the bucket. The bursts may come in any order.
- `forecast_schedules(&policy, &schedules, from, until)` walks real cron and
  interval schedules with `SlotWalker`. Slots at one instant form one burst.
  All schedules share one bucket, as the schedules of one unkeyed workflow
  type do. The bucket is full at `from`.
- `steady_state(&policy, fan_out, period)` classifies a burst of `fan_out`
  starts that repeats each `period`. `SteadyState::Keeps { worst_wait }`
  means that the bucket refills before the next burst.
  `SteadyState::FallsBehind { lag_per_period }` means that the backlog grows
  each period.

`ThrottleForecast` holds one `BatchForecast` per burst. Each one holds `at`,
`count`, `admitted`, `dropped`, `first_admit`, `last_admit` and `max_wait`.
The totals are `admitted()`, `dropped()`, `max_wait()` and `drained_at()`.

Limits:

- **The waits are lower bounds.** The real scanner fires on a tick, and it
  fires at most `throttle::THROTTLE_FIRE_PER_KEY_CAP` rows of one key per
  tick. Both only add delay.
- `steady_state` refuses a policy with a `schedule_to_start` deadline, with
  `ForecastError::DeadlineDrops`. A drop bounds the backlog, so the closed
  form does not apply. Call `forecast` with the periodic bursts instead.
- A policy with a rate that is not above zero, or a burst below 1, returns
  `ForecastError::InvalidRate` or `ForecastError::InvalidBurst`.
- `forecast_schedules` reads at most `MAX_FORECAST_SLOTS` (100,000) slots.
  `truncated` is then `true`.
- The forecast models one bucket. A keyed throttle has one bucket per key.

```rust
use std::time::Duration;
use autumn_harvest::throttle::ThrottlePolicy;
use autumn_harvest::throttle_forecast::{SteadyState, steady_state};

// 300 tenants on one hourly cron under a 100/m throttle.
let policy = ThrottlePolicy::from_rate_str("100/m", None, None, None).unwrap();
let state = steady_state(&policy, 300, Duration::from_secs(3600)).unwrap();
// The burst of 100 runs at once. The other 200 drain at 0.6 s each.
assert_eq!(state, SteadyState::Keeps { worst_wait: Duration::from_secs(120) });
```

## Shape census (`shape_census`)

[`history_shape`](architecture.md#module-guide) fingerprints one history by
its ordered event type names. `shape_census` counts many of them. Feed it
the histories of one workflow type. It tells which path is the usual one,
which paths are rare, and how a new run compares.

- `ShapeCensus::new()`, then `observe(&events)` for each history.
  `observe` returns the `shape_fingerprint` of the history.
- `total()` counts the histories. `distinct()` counts the shapes.
- `shapes()` lists every shape, the most common first. `dominant()` returns
  the most common shape.
- `rare_shapes(max_share)` lists the shapes with a share of `max_share` or
  less, the rarest first.
- `judge(&events)` returns a `ShapeVerdict` for a new run. It does not
  change the census.

A `ShapeSummary` holds `fingerprint`, `count`, `share` and `path`. A
`ShapeVerdict` holds `fingerprint`, `seen`, `share` and `divergence`: the
first event index where the run leaves the dominant path. `divergence` is
`None` when the run follows the dominant path, and for an empty census.

The census keys each shape by its full name sequence. A fingerprint
collision therefore does not merge two shapes. A tie on count falls back to
the shape order, so two runs over one corpus give the same report. A payload
change does not move a shape.

```rust
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::shape_census::ShapeCensus;

let done = WorkflowEvent::WorkflowCompleted { output: serde_json::json!(1) };
let mut census = ShapeCensus::new();
census.observe(&[done.clone()]);
census.observe(&[done.clone()]);
census.observe(&[done.clone(), done.clone()]);

assert_eq!(census.dominant().map(|s| s.count), Some(2));
assert_eq!(census.rare_shapes(0.5).len(), 1);
assert_eq!(census.judge(&[done.clone(), done]).divergence, Some(1));
```

## Schedule herds (`schedule_herd`)

Each schedule passes validation alone. So nothing warns when twenty
schedules all say `0 * * * *`. At the top of each hour the scheduler then
starts twenty runs at once, and their activities reach the workers
together. The herd forecast finds these moments.

### The library

- `forecast(&schedules, from, until, bucket, min_herd)` walks each
  `(name, &Schedule)` pair over `[from, until)` with `SlotWalker`. It puts
  each fire into the bucket that starts at a whole multiple of `bucket`
  since the Unix epoch.
- A bucket becomes a `HerdSpot` when it holds at least `min_herd` distinct
  schedules. A `min_herd` of 0 counts as 2. A `bucket` under one second
  counts as one second.
- `forecast_shifted(.., lookback, shift)` calls `shift` for each slot.
  `shift` returns `Fire::Start(time)`, `Fire::Skip` or `Fire::Stop`. Pass
  the jitter offset of each schedule here, and its largest value as
  `lookback`.

`HerdForecast` holds `fires`, `spots`, `unanchored` and `truncated`. The
largest spot comes first, and spots of equal size are in time order.
`peak()` returns the largest spot. `offenders()` ranks each schedule by the
number of spots that include it. A schedule with a high count is the best
one to move to another minute.

Only cron schedules have a fixed phase. An interval schedule counts from the
end of its previous run, and a manual schedule never fires. The library
lists both as `unanchored` and leaves them out of every bucket. A schedule
that reaches `MAX_FIRES_PER_SCHEDULE` (100,000) fires in the window goes in
`truncated`.

```rust
use std::time::Duration;
use autumn_harvest::jiff::Timestamp;
use autumn_harvest::policy::Schedule;
use autumn_harvest::schedule_herd::forecast;

let hourly = Schedule::Cron("0 * * * *".to_string());
let offset = Schedule::Cron("17 * * * *".to_string());
let from = Timestamp::from_second(0).unwrap();
let until = Timestamp::from_second(3 * 3600).unwrap();
let report = forecast(
    &[("a", &hourly), ("b", &hourly), ("c", &offset)],
    from,
    until,
    Duration::from_secs(60),
    2,
);
// `a` and `b` meet at the top of each hour. `c` fires alone.
assert_eq!(report.spots.len(), 3);
assert_eq!(report.offenders()[0], ("a".to_string(), 3));
```

### The endpoint and the CLI

`GET /admin/schedules/herd` runs the forecast over every stored schedule.
`harvest schedule herd` calls it and prints a table. See
[`management-api.md`](management-api.md#schedule-herd-forecast)
for the parameters and the body.

The route applies the scheduler rules that the library leaves to its
caller:

- It applies the jitter of each schedule, as the scheduler does.
- It lists paused, auto-paused, exhausted, manual and unparsable schedules
  as `inactive`. It lists interval schedules as `unanchored`.
- Each schedule stops at its `end_at` and at the end of its `max_runs`
  budget.

Limits:

- **Calendar exclusions are not applied yet.** A slot that a calendar skips
  still counts. Use the [schedule preview](runbooks/schedule-preview.md) to
  see the calendar effect on one schedule.
- A schedule whose budget at the window is unknown goes in `truncated`. So
  does a schedule whose jitter can carry a run past its next slot.
- The SQLite backend answers `501` for the route. It has no port yet.

The slot walk parses each cron expression once per walk. See
[`performance-schedule-slot-walk.md`](performance-schedule-slot-walk.md).
