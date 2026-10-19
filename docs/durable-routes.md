# Durable routes

A durable route is an Autumn route whose work runs as a Harvest workflow. The
HTTP `Idempotency-Key` header keys the run.

- The first request starts the run and waits for its result.
- A retry with the same key attaches to the same run. It gets the stored
  result, and the side effects do not run again.
- A request that outlives its wait budget gets a pending answer. The client
  retries the same request to collect the result.

The run commits before the route waits. A crash, a deploy or a dropped
connection loses nothing. Inngest calls this pattern "durable endpoints". The
design record is [`DESIGN-durable-routes.md`](../DESIGN-durable-routes.md).

## Write a durable route

Register the workflow on the plugin as usual. Then add `DurableRoute` to the
route, before the body extractor:

```rust,ignore
use autumn_harvest_plugin::durable_route::{DurableOutcome, DurableRoute};
use autumn_web::prelude::*;
use autumn_web::reexports::axum::http::StatusCode;

#[post("/orders")]
async fn place_order(
    durable: DurableRoute,
    user: CurrentUser,
    Json(order): Json<OrderRequest>,
) -> DurableOutcome {
    durable
        .scope(user.id)
        .call("place_order", PlaceOrder { user_id: user.id, order })
        .success_status(StatusCode::CREATED)
        .failure_status("OutOfStock", StatusCode::UNPROCESSABLE_ENTITY)
        .send()
        .await
}
```

| Call | Effect |
|---|---|
| `.scope(principal)` | Binds the key to the principal, for example the user id. Two principals with one key get two runs. |
| `.unscoped()` | Uses no principal. Use it only for an anonymous route. Every caller then shares one key space. |
| `.call(workflow, input)` | Names the registered workflow and its input. The input must be a pure function of the request and the principal. |
| `.wait(budget)` | Waits up to `budget` for the result. The default is 5 s and the maximum is 30 s. `Duration::ZERO` never waits. |
| `.success_status(status)` | The status of a completed run: 200 (default), 201 or 204. |
| `.failure_status(error_type, status)` | The status of a run that fails with the typed `WorkflowFailure` class `error_type`. |
| `.expose_execution_id()` | Adds `x-harvest-execution-id` to each answer after the start. |
| `.send()` | Runs the call. It never fails: every problem is a `DurableOutcome`. |

`DurableOutcome` implements `IntoResponse`. To render HTML, read
`outcome.kind()` and `outcome.output_as::<T>()`, and fall back to
`outcome.into_response()` for the other cases.

The scope is not optional. With no scope, user B could send user A's key and
get user A's result. When the app resolves the tenant in the handler, put the
tenant in the scope too. When Autumn's tenancy middleware runs, the tenant
joins the key on its own.

## The client contract

1. Send a **random** `Idempotency-Key` (a UUID) with each new operation.
2. On a network error, a `5xx` or a `Retry-After`, send the same request
   again: the same body and the same `Idempotency-Key`.
3. `Retry-After` means "the run is still going, retry". Use the same
   `Idempotency-Key`.
4. An answer for a run that an earlier request started carries
   `x-idempotent-replayed: true`.

A key binds its body. A retry with another body under the same key gets a
`422` and starts nothing. Use a new key for a new operation.

## Status table

| Case | Status | Problem slug |
|---|---|---|
| The run completed. | `success_status`: 200, 201 or 204 | — (the body is the workflow output) |
| The run is still going, and Autumn's idempotency layer is off. | 202, `Retry-After: 1` | — (the body is `{"state":"pending"}`) |
| The run is still going, and Autumn's idempotency layer is on. | 409, `Retry-After: 1` | `pending` |
| No `Idempotency-Key`, or an empty one. | 400 | `idempotency-key-missing` |
| More than one key, a byte outside printable ASCII, or more than 255 bytes. | 400 | `idempotency-key-invalid` |
| The key belongs to a request with another body. | 422 | `idempotency-key-reused` |
| The input fails the workflow's JSON Schema. | 422 | `input-invalid` |
| The input is larger than the workflow accepts. | 413 | `input-too-large` |
| A workflow quota is full. | 429, `Retry-After` | `quota-exceeded` |
| The run cannot start now, or the plugin has not started. | 503, `Retry-After` | `unavailable` |
| The route setup is wrong, or the workflow cannot start from a route. | 500 | `misconfigured` |
| Another start error. | 500 | `internal` |
| The run failed with a mapped `error_type`. | The mapped status | `failed`, with `error_type` and `details` |
| The run failed with no mapping. | 500 | `failed` |
| The run was cancelled. | 500 | `cancelled` |
| The run was terminated. | 500 | `terminated` |
| The run exceeded its execution timeout. | 500 | `timed-out` |
| PII erasure removed the result. | 410 | `erased` |
| The server cannot decode the result. | 500 | `result-unavailable` |

A problem body is `application/problem+json` (RFC 9457). Its `type` is
`urn:autumn-harvest:durable-route:<slug>` and its `code` is
`harvest.durable_route.<slug>`, with `-` changed to `_`. The `detail` is fixed
text. A problem body never holds the workflow error message, the key, the
scope or the input.

A mapped failure shows the `error_type` and the `details` of the
`WorkflowFailure`, never its message. Put a mapped class in the workflow's
`RetryPolicy::non_retryable_errors`. Otherwise every workflow retry runs
first, and the client sees pending until the last attempt fails.

## Waiting

The route first reads the run with no wait. A completed run answers at once.
A live run waits for up to the budget.

- Each waiting request holds one Postgres `LISTEN` connection. Set the cap
  for each process with `HarvestPlugin::durable_route_max_waiters(n)`. The
  default is 32. A request that finds the cap full answers pending at once.
  `0` means that no request waits.
- The wait ends 2 s before Autumn's request deadline
  (`server.timeouts.request_timeout_ms`). A route that sets its own
  `timeout_ms` must set `.wait(..)` below it, because the route cannot see
  that value.
- A run that continues as new is followed to its last successor.

## With Autumn's idempotency layer

`AppBuilder::idempotent()` caches every 2xx answer. A cached `202` would
replay for 24 h in place of the result, so a durable route answers pending
with `409` when the layer is on. Autumn does not cache a `409`.

With the layer on, Autumn answers some requests before the route runs:

- A duplicate that arrives while the first request is in flight gets
  Autumn's `409` with `Retry-After: 1`.
- A changed body against a cached answer gets Autumn's `422`.
- A repeat of a completed request gets Autumn's cached copy.

Autumn's cache key binds the session cookie, not the durable-route scope.
Two clients that authenticate with a bearer token and send the same key and
body share one cached answer, and that answer skips the handler. Do not turn
on `.idempotent()` for a token-authenticated API with durable routes. The
durable route already replays its own results, bound to the scope. The route
logs one warning when it runs under the layer with an `Authorization` header.

A client that drops its connection while the route waits leaves Autumn's
in-flight lock in place for `idempotency.in_flight_ttl_secs`. Keep that value
short, for example 60 s.

Do not put `#[intercept]` on a durable route. Autumn then refuses every cache
hit with a `409`.

## Known limits

- Durable routes need the plugin's Postgres runtime. The SQLite runtime and
  `HarvestEmbedding` do not support them.
- There is no status monitor and no `Location` header. A client resumes only
  by retrying the same request.
- A server-made value in the input, such as the time or a new id, makes each
  retry a `422`. Make such values inside the workflow.
- A key binds its body for the start-idempotency window (24 h by default,
  `HarvestPlugin::start_idempotency_window`). After that, the same body
  attaches to the original run until retention deletes it, and the attach
  binds the key again. Another body under an expired key starts a new run.
- Workflow-level retries run the activities again. Activities must be
  idempotent.
- Durable runs have no Harvest tenant, record the `transactional` start
  source and do not join the HTTP trace.
- A run that a reset sealed reads as `terminated`. A paused run reads as
  pending.
- An untyped failure (`Err(String)`) cannot be mapped.
- The key and fingerprint hashes are unsalted SHA-256. Use random keys.
- A handler that returns `DurableOutcome` gets no inferred OpenAPI response
  schema.
- A plain HTML form cannot send the header. An htmx form can, with
  `hx-headers`.
