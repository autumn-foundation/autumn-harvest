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
| `.scope(principal)` | Binds the key to the principal, for example the user id. Two principals with one key get two runs. The string must be unique across every route that runs the workflow, so give ids of different kinds a prefix, such as `user:7`. |
| `.unscoped()` | Uses no principal. Use it only for an anonymous route. Every caller then shares one key space. |
| `.call(workflow, input)` | Names the registered workflow and its input. The input must be a pure function of the request and the principal. |
| `.wait(budget)` | Waits up to `budget` for the result. The default is 5 s and the maximum is 30 s. `Duration::ZERO` never waits. See [Waiting](#waiting). |
| `.success_status(status)` | The status of a completed run: 200 (default), 201 or 204. |
| `.failure_status(error_type, status)` | The status of a run that fails with the typed `WorkflowFailure` class `error_type`. |
| `.expose_execution_id()` | Adds `x-harvest-execution-id` to each answer that reports on the run, and an `execution_id` member to a pending body. While the key's claim lasts, the id names the run that the key started, which is the first run of a continue-as-new chain. After the window, it names the run that holds the business id. |
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
2. On a network error, a `502`, `503` or `504`, or any answer with
   `Retry-After`, send the same request again. Use the same body and the
   same `Idempotency-Key`.
3. `Retry-After` means that the run is not finished or cannot start yet.
4. A durable-route problem with no `Retry-After` is final for this key. Do
   not retry it. Its `type` starts with `urn:autumn-harvest:durable-route:`.
5. An answer for a run that an earlier request started carries
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
| The server cannot decode the result, or cannot reach the end of its chain or of its retries. | 500 | `result-unavailable` |

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
  (`server.timeouts.request_timeout_ms`). Every read after the start ends
  1 s before it, and a late read answers pending. Both margins count from
  the time the route reads the request. Middleware time before the handler
  uses up the margin.
- With Autumn's idempotency layer on, the route counts only half of the
  deadline, because that layer reads the whole body before the handler.
- A route that sets its own `timeout_ms` must set `.wait(..)` below it,
  because the route cannot see that value.
- A run that continues as new is followed to its newest run. Each step is
  one successor lookup. A step follows the chain to its newest run of the
  same type, or across one change of workflow type or one workflow-level
  retry. A chain needs at most one step more than its changes of type and
  its retries. The cap is 1024 steps. Set it with
  `HarvestPlugin::durable_route_max_chain_steps(n)`.
- Past the cap, a walk that met a live run answers pending, because a retry
  can need fewer steps once the chain ends. A walk that read only ended runs
  answers `result-unavailable`, because a retry repeats the same walk. A
  chain that loops back answers `result-unavailable` too.
- A read error other than a codec error also answers pending, because the
  run exists. A workflow-level retry chain past the engine's walk depth
  (256 attempts) answers `result-unavailable`, because it never shrinks.

## With Autumn's idempotency layer

`AppBuilder::idempotent()` caches every 2xx answer. A cached `202` would
replay for 24 h in place of the result. When the layer is on, a durable route
answers pending with `409`. Autumn does not cache a `409`.

With the layer on, Autumn answers some requests before the route runs:

- A duplicate that arrives while the first request is in flight gets
  Autumn's `409` with `Retry-After: 1`.
- A changed body against a cached answer gets Autumn's `422`.
- A repeat of a completed request gets Autumn's cached copy, except in the
  fail-closed cases below.

Autumn's cache key binds the session cookie, not the durable-route scope.
Two clients whose principal is not a cookie session, such as a bearer token,
an API key or a client certificate, can send the same key and body. They then
share one cached answer, and that answer skips the handler. Do not turn on
`.idempotent()` for such an API with durable routes. The durable route
already replays its own results, bound to the scope. The route logs one
warning when it runs under the layer with an `Authorization` header.

The cached copy is the workflow output in clear. The Harvest payload codec
does not encrypt it, and PII erasure does not remove it. Until
`idempotency.ttl_secs` ends, a repeat gets that copy, not `410`. For durable
routes whose output holds personal data, keep `.idempotent()` off or set a
short TTL.

A client that drops its connection while the route waits leaves Autumn's
in-flight lock in place for `idempotency.in_flight_ttl_secs`. Keep that value
short, for example 60 s.

Autumn refuses every cache hit with a plain `409` and no `Retry-After` in
these cases:

- The route has `#[intercept]`.
- The route is in a scoped group, or in a merged or nested router.
- The app registers an `AppBuilder::layer` or a `static_gate`, other than
  Autumn's own session layer.

A retry then cannot collect a completed result until `idempotency.ttl_secs`
ends. In such an app, do not turn on `.idempotent()` for durable routes.

## Known limits

- Durable routes need the plugin's Postgres runtime. The SQLite runtime and
  `HarvestEmbedding` do not support them. There, a durable route answers
  `503` `unavailable`. The plugin refuses a pool of more than one shard, so a
  durable route runs on one shard.
- There is no status monitor and no `Location` header. A client resumes only
  by retrying the same request.
- A server-made value in the input, such as the time or a new id, makes each
  retry a `422`. Make such values inside the workflow.
- A key binds its body for the start-idempotency window (24 h by default,
  `HarvestPlugin::start_idempotency_window`). After that, the same body
  attaches to the run that holds the business id until retention deletes
  it, and the attach binds the key again. That is the original run, unless
  an operator rerun or reset gave the business id to a newer run. Another
  body under an expired key starts a new run.
- After the window, a retry of a run that was terminated, or that continued
  as new into another workflow type, starts a new run. Those runs no longer
  hold the business id.
- PII erasure works on one execution. To erase a durable run, erase every
  execution with its `workflow_id`: each retry attempt and each
  continue-as-new run. `x-harvest-execution-id` names only one of them.
- The route checks the input size before any I/O, with the current cap. A
  retry of an accepted input gets `413` when an operator lowers the cap.
- Workflow-level retries run the activities again. Activities must be
  idempotent.
- Durable runs have no Harvest tenant, record the `transactional` start
  source and do not join the HTTP trace.
- Within the claim window, a run that a reset sealed reads as `terminated`.
  When the run failed or timed out before the reset, the MCP task tools
  report that end. They report any other reset-sealed run as `cancelled`. A
  paused run reads as pending.
- Within the claim window, a retry after an operator rerun gets the outcome
  of the original run, not of the rerun. This also holds for a rerun of the
  last run of a chain. `failure_status` still maps the typed failure. After
  the window, the retry attaches to the rerun, which holds the business id.
- The claim window is one setting for the plugin, not one for each route.
- The start does not join the handler's own database transaction.
- There is no `Prefer: wait` header. The handler sets the budget.
- There is no metric family for durable routes. Use the
  `harvest.durable_route` tracing span.
- An untyped failure (`Err(String)`) cannot be mapped.
- The key and fingerprint hashes are unsalted SHA-256. Use random keys.
- A handler that returns `DurableOutcome` gets no inferred OpenAPI response
  schema.
- A plain HTML form cannot send the header. An htmx form can, with
  `hx-headers`.
