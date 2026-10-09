# Durable promises

A durable promise is a handle that a workflow creates and any caller settles
once (issue #1985). It is the Harvest form of a Restate awakeable. Use it when
a person, a webhook or another service must hand a result back to a waiting
workflow.

## Create and wait

```rust
use autumn_harvest::prelude::*;

#[workflow]
async fn approve(ctx: &WorkflowContext, request: String) -> HarvestResult<String> {
    let promise = ctx.new_promise()?;
    ctx.execute_activity_raw(
        "send_for_approval",
        serde_json::json!({ "request": request, "token": promise.id().to_string() }),
        "default",
    )
    .await?;
    match promise.wait::<String>().await? {
        Ok(approver) => Ok(approver),
        Err(rejected) => Ok(format!("rejected: {}", rejected.error)),
    }
}
```

| Call | Result |
|---|---|
| `ctx.new_promise()` | A promise with a new `UUIDv7` key. A caller needs the token to settle it. |
| `ctx.promise("approval")` | A promise with a fixed key. A caller that knows the run can build its token. |
| `promise.wait::<T>()` | `HarvestResult<Result<T, PromiseRejected>>`. The outer error is an engine error. The inner error is a rejection. |
| `promise.wait_timeout::<T>(d)` | `Ok(None)` when the durable timer fires first. |

Every wait on one handle returns the same settlement, also when two waits
run at the same time.

The token is `<execution-id>/<key>`. A key holds only `A-Z`, `a-z`, `0-9`,
`.`, `_`, `:` and `-`, and is 128 bytes or fewer.

The token is not a secret. History shows it. Any caller with access to the
signal API or the database can settle any promise.

To race a promise in `ctx.race().signal(promise.id().signal_name())`, decode
the raw payload with `PromiseSettlement::decode::<T>(raw)`.

## Settle

Each path settles the promise once. The first settlement wins. A later one
is a no-op.

**Rust, with a connection to the shard that holds the run:**

```rust,ignore
let id: PromiseId = token.parse()?;
let first = durable_promise::resolve(&mut conn, &id, serde_json::json!("ops")).await?;
// `first` is false if the promise already had a settlement.
durable_promise::reject(&mut conn, &id, "budget exceeded").await?;
```

**From another workflow:** `ctx.resolve_promise(&id, value)` or
`ctx.reject_promise(&id, error)`.

**Over HTTP:** send the settlement as a signal. The signal name and the
`Idempotency-Key` header are both `harvest.promise:<key>`. A missing header
gets that value.

The Rust call targets the exact run in the token. The HTTP route follows the
workflow retry chain to the live attempt. A retry attempt replays from an
empty history, so `new_promise` gets a new key there. Only a named promise
gains from the retry chain.

```bash
curl -s -X POST \
  "http://localhost:3000/api/harvest/workflows/<EXECUTION_ID>/signal/harvest.promise:<KEY>" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: harvest.promise:<KEY>' \
  -d '{"outcome":"resolved","value":"ops"}'
```

A rejection body is `{"outcome":"rejected","error":"budget exceeded"}`.

## Guarantees

- **No new storage.** A promise is a signal named `harvest.promise:<key>`.
  There is no new table, no new event variant and no migration.
- **Settle once, on every path.** `signal::send_signal_idempotent` stores
  every signal. For a `harvest.promise:` name, it sets the idempotency key to
  the name and refuses a payload that is not a settlement. It also refuses a
  `harvest.promise:` key on any other signal. The database drops a second
  settlement.
- **Settle early.** A settlement that arrives before the wait stays buffered.
- **Replay.** The token is recorded in a `SideEffectRecorded` event, and the
  settlement in a `SignalReceived` event. Replay under a new execution id
  returns the same token.
- **Race a timer.** Use `wait_timeout`, or `ctx.race().signal(..)` with
  `promise.id().signal_name()`.

## Limits

- A promise belongs to the run that created it. A settlement for a terminal
  run fails, unless the promise already has a settlement.
- A settlement for a key that the run never created is stored, and `resolve`
  returns `true`. Nothing waits on it.
- Continue-as-new moves an unconsumed settlement to the next run. A named
  promise with the same key in the next run then receives it at once. Put
  the run number in the key when a late settlement must not move forward.
- A reset fork keeps the recorded token of a promise made before the reset
  point. That token names the source run. To settle the promise in the fork,
  use `PromiseId::new(fork_execution_id, id.key())`, or send the HTTP signal
  to the fork.
- Make one handle for each key in a run. A second handle for the same key
  waits for a second settlement, and none comes.
- Do not register a push signal handler for a `harvest.promise:` name.

## Why signals, not external task tokens

External task tokens (`execute_activity_external`) were the first candidate.
A token wait is a solo suspension, so it cannot race a timer. It needs a
deadline. The token row exists only after the park commits. Signals have none
of these limits. They already give buffering, deduplication and replay.

Issue #2006 (durable outbound MCP task calls) can settle a promise when a
remote task completes. Issue #1975 (keyed entities) can use named promises
for per-key replies.
