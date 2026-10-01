# Automatic load shedding (issue #1794)

The manual admission gate (issue #377) is an operator switch. Load shedding
is an automatic gate. It rejects new workflow starts on a queue while that
queue has an old backlog. A rejected caller gets `429 Too Many Requests` with
a `Retry-After` header, so it can slow down.

Load shedding is opt-in per queue. A deployment with no policy runs no
sampler and no extra SQL.

## Signal

The signal is the **age of the oldest claimable `PENDING` task** on the
queue. This is the value of the `harvest.queue.oldest_pending_age` gauge.
The query is `queue::oldest_pending_ages`. It counts only tasks that a worker
may claim now. Paused queues, paused executions, saturated concurrency keys
and empty rate-limit buckets do not count.

A background sampler reads the signal every `sample_interval`. It reads every
physical shard pool and keeps the maximum age per queue. A configured queue
with no claimable task has age 0.

Schedule-to-start p99 is not a signal in this version. No central histogram
of it exists.

## Trip and clear conditions

Each queue has a `LoadShedPolicy` with `trip_age`, `clear_age` and
`retry_after`. The policy requires `0 < clear_age < trip_age`.

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
- A state older than three sample intervals is ignored. Starts are admitted.

A protective heuristic must not cause an outage when its own input is gone.

## What is shed

Shedding applies where the manual gate applies with `GateMode::Check`. That
is the point in the start primitive where it decides to **create** a new
execution. These requests are shed:

- `POST /workflows/{name}/start`: plain, keyed, auto-id and throttled starts.
- `POST /workflows/{name}/signal-with-start` when no run exists.
- `POST /workflows/{name}/update-with-start` when no run exists.
- `POST /workflows/batch_start`: each item. A shed item is a per-item
  rejection.
- `POST /workflows/{id}/rerun`.

The manual gate is checked first. A start that both gates match gets `503`.

## Exemptions

These are never shed:

- A signal, update, query, cancel or terminate on an existing run.
- A start that attaches to an existing run, for example a repeated
  `workflow_id` under `allow_duplicate`.
- Signal-with-start and update-with-start when the run exists.
- Continuations: workflow retry, continue-as-new, child start, reset.
- Internal producers: scheduler, completion triggers, outbox relay,
  debounce, throttle and event-batch scanner fires, transactional starts.
  They cannot act on `Retry-After`. The manual gate still applies to them.
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

## Configuration

```rust
use std::time::Duration;
use autumn_harvest::load_shed::{LoadShedConfig, LoadShedPolicy};

let policy = LoadShedPolicy::new(
    Duration::from_secs(300), // trip_age
    Duration::from_secs(60),  // clear_age
    Duration::from_secs(5),   // retry_after
)?;
let config = LoadShedConfig::new()
    .with_sample_interval(Duration::from_secs(5))
    .queue("default", policy);
let built = HarvestBuilder::new().load_shed(config).build()?;
```

The default `sample_interval` is 5 seconds.

## Observability

| Signal | Kind | When |
|--------|------|------|
| `harvest.load_shed.active{queue}` | gauge | 1 while the queue sheds, 0 otherwise. Set on every sample. |
| `harvest.load_shed.rejected{queue}` | counter | One per shed start. |
| `load_shed.trip` audit row | audit | The queue trips. |
| `load_shed.clear` audit row | audit | The queue clears. |

Audit rows have actor `system`, target type `queue` and the queue name as
target id. `error_summary` holds the sampled age. Each replica runs its own
sampler, so each replica writes its own trip and clear rows.

## Choosing thresholds

- Set `trip_age` above the normal oldest-pending age at peak load.
- Set `clear_age` well below `trip_age`. Half or less is a good start.
- Set `retry_after` near the time the queue needs to drain `clear_age`.
- Keep `sample_interval` well below `clear_age`.
