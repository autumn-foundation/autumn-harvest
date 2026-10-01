# Automatic load shedding (issue #1794)

The manual admission gate (issue #377) is an operator switch. Load shedding
is an automatic gate. It refuses new workflow starts on a queue while that
queue has an old backlog. A refused caller gets `429 Too Many Requests` with
a `Retry-After` header, so it can slow down. The manual gate answers `503`, so
a caller can tell overload from an operator halt.

Load shedding is opt-in per queue. A deployment with no policy runs no
sampler and no extra SQL.

## Signal

The signal is the **age of the oldest claimable `PENDING` task** on the
queue. This is the value of the `harvest.queue.oldest_pending_age` gauge.
The query is `queue::oldest_pending_ages`. It counts only tasks that a worker
may claim now. Paused queues, paused executions, saturated concurrency keys
and empty rate-limit buckets do not count.

A background sampler reads the signal every `sample_interval`. It reads every
physical shard pool at the same time and keeps the maximum age per queue. A
configured queue with no claimable task has age 0.

Schedule-to-start p99 is not a signal in this version. No central histogram
of it exists.

## Trip and clear conditions

Each queue has a `LoadShedPolicy` with `trip_age`, `clear_age` and
`retry_after`. The policy requires `0 < clear_age < trip_age` and
`retry_after > 0`.

| State | Sample | Next state |
|-------|--------|------------|
| admitting | `age >= trip_age` | **shedding** (trip) |
| admitting | `age < trip_age` | admitting |
| shedding | `age <= clear_age` | **admitting** (clear) |
| shedding | `age > clear_age` | shedding |

The gap between the two thresholds is the hysteresis. A queue that drains to
just under `trip_age` stays shed until its age falls to `clear_age`.

### Fail open

The gate fails **open**. This is the opposite of the manual gate.

- A sampler tick with any shard read failure changes no state.
- A sample that runs longer than two sample intervals counts as a failure.
- The gate ignores a state older than three sample intervals. It then admits
  starts.
- After such a gap, a shedding queue sheds again only at `trip_age`. That is
  a new trip.

A protective heuristic must not cause an outage when its own input is gone.

### Replicas

Each replica runs its own sampler against the same database. Replicas can
trip up to one sample interval apart. A replica that starts during an
incident admits starts until its own samples reach `trip_age`.

## What is shed

The start primitive checks the manual gate and then the shedder at one
point. At that point it decides to **create** a new execution. Only
`GateMode::Check` callers reach this check. The gate sheds these requests:

- `POST /workflows/{workflow_name}/start`: plain, keyed and auto-id starts.
- The same route for a throttled workflow. The route checks the shedder
  before the throttle can defer the start with `202`.
- `POST /workflows/{workflow_name}/signal-with-start` when no run exists.
- `POST /workflows/{workflow_name}/update-with-start` when no run exists.
- `POST /workflows/batch_start`. Each shed item is a per-item `rejected`
  result whose `error` starts with `load shed on queue`. An atomic batch whose
  only rejections are sheds answers `429` with `Retry-After` and inserts
  nothing. An atomic batch with a manual-gate block keeps its `409`.
- The queue can trip while an atomic batch starts its items. The batch then
  stops at the shed item and answers `429` with `Retry-After`. Items started
  before it stay started, as for any other atomic start failure.
- `POST /workflows/{id}/rerun`.

A start that both gates match gets the manual gate's `503`.

## Exemptions

The gate never sheds these requests:

- A signal, update, query, cancel or terminate on an existing run.
- A start that attaches to an existing run, for example a repeated
  `workflow_id` under `allow_duplicate`.
- Signal-with-start and update-with-start when the run exists.
- Continuations: workflow retry, continue-as-new, child start, reset.
- Internal producers: scheduler, completion triggers, outbox relay,
  debounce, throttle and event-batch scanner fires, transactional starts.
  They cannot act on `Retry-After`. The manual gate still applies to them.
- Operator triggers: DAG trigger, schedule trigger and schedule backfill.
- Debounce and event-batch HTTP arrivals. They coalesce into one start.

## Response

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 5
Content-Type: application/json

{
  "error": "load shed",
  "queue": "default",
  "oldest_pending_age_secs": 312,
  "retry_after_secs": 5
}
```

`Retry-After` is `retry_after` rounded up to whole seconds. The minimum is 1.
A shed start writes no execution, event or task row.

## Configuration

```rust
use std::time::Duration;

use autumn_harvest::HarvestBuilder;
use autumn_harvest::load_shed::{LoadShedConfig, LoadShedPolicy};

fn build() -> Result<(), Box<dyn std::error::Error>> {
    let policy = LoadShedPolicy::new(
        Duration::from_secs(300), // trip_age
        Duration::from_secs(60),  // clear_age
        Duration::from_secs(5),   // retry_after
    )?;
    let config = LoadShedConfig::new()
        .with_sample_interval(Duration::from_secs(5))
        .queue("default", policy);
    let _built = HarvestBuilder::new().load_shed(config).try_build()?;
    Ok(())
}
```

The default `sample_interval` is 5 seconds. The minimum is 1 second.

## Observability

| Signal | Kind | When |
|--------|------|------|
| `harvest.load_shed.active{queue}` | gauge | 1 while the queue sheds, 0 otherwise. The sampler sets it on every tick, also after a failed read. |
| `harvest.load_shed.rejected{queue}` | counter | One per shed start or shed batch item. |
| `load_shed.trip` audit row | audit | The queue trips. |
| `load_shed.clear` audit row | audit | The queue clears. |

Each audit row has these values:

- `actor`: `system`.
- `source`: `api`. The table accepts only `api`, `cli` and `ui`.
- `route_or_command`: `background.load_shed_sampler`.
- `target_type`: `queue`. `target_id` is the queue name.
- `error_summary`: the sampled age, for example `oldest pending age 312s`.

Each replica writes its own trip and clear rows. A failed sample logs a
warning and writes no row.

## Choosing thresholds

- Set `trip_age` above the normal oldest-pending age at peak load.
- Set `clear_age` well below `trip_age`. Half or less is a good start.
- Set `retry_after` near the time the queue needs to drain from `trip_age`
  to `clear_age`.
- Keep `sample_interval` well below `clear_age`.
