# Design — durable routes

A durable route is an Autumn route whose work runs as a Harvest workflow. The
HTTP `Idempotency-Key` header keys the run. A retry with the same key attaches
to the same run and gets its stored result. Side effects do not run again. A
request that outlives its wait budget gets a pending answer. The client then
retries the same request to collect the result.

The October 2026 feature-gap research lists "durable HTTP endpoints, not
background jobs" as a gap. Inngest Durable Endpoints is the reference
(`research_notes/Durable workflow engine feature gaps/code_first_engines.md`).

**No migration. No new `WorkflowEvent` variant. No `harvest_events` write. No
management API route. No core engine change.** The feature is one new module in
`autumn-harvest-plugin`. It uses the keyed transactional start (issue #763) and
the start-idempotency claim (issue #808).

---

## 0. Planning record

### 0.1 Facts found before the plan

| # | Fact | Source |
|---|------|--------|
| F1 | Harvest builds against crates.io `autumn-web 0.8.0`, not the Autumn checkout. | `Cargo.lock` |
| F2 | Autumn ADR 0016 keeps workflow machinery out of Autumn core. Harvest integrates through the `Plugin` seam. | autumn `docs/adr/0016-durable-workflows-live-in-harvest.md` |
| F3 | Autumn's route attribute rejects unknown keys. `#[post("/x", durable)]` needs a core change. | `autumn-macros-0.8.0/src/parse.rs` |
| F4 | Autumn's `.idempotent()` layer caches every 2xx and 3xx answer, a `202` too. It has no per-route opt-out. | `autumn-web-0.8.0/src/idempotency.rs`, `handle_idempotent_request` |
| F5 | That layer puts `IdempotencyContext` into the request extensions before any extractor runs. | `idempotency.rs`, `prepare_idempotency_request` |
| F6 | `IdempotencyContext::scoped_key` binds a session-id digest only. It leaves out bearer tokens, and it changes when the session rotates. | `idempotency.rs`, storage-key derivation |
| F7 | `start_workflow_transactional` with a key probes a committed claim first. On a duplicate it returns the original run and its `workflow_id`. | `autumn-harvest/src/handle.rs` |
| F8 | Same-key starts race on one primary-key upsert. Exactly one run results. | `start_idempotency.rs` |
| F9 | Harvest does not compare the input of a duplicate keyed start. | `api.rs`, `extract_start_idempotency_header_key` |
| F10 | `result_snapshot_with_wait` returns `Ok(None)` for a run that is still live at the deadline. It treats `CONTINUED_AS_NEW` as terminal. | `handle.rs` |
| F11 | Each wait opens one Postgres `LISTEN` connection outside the pool. The connect can take up to 5 s. | `handle.rs`, `notify.rs` |
| F12 | The `prod` profile sets a 30 s request deadline. Autumn answers `503` when it fires and drops the handler future. | `autumn-web-0.8.0/src/config.rs`, `router.rs` |
| F13 | The plugin installs a `WorkflowHandleClient` with every workflow, queue, codec and the claim window. The extension is absent until startup ends. | `plugin.rs`, `start_harvest_runtime` |
| F14 | PII erasure replaces the output with `{"_harvest_erased": true}`. | `erase.rs` |
| F15 | `workflow_id` is clear `TEXT` with no length or charset check. The claim key is clear `TEXT`, at most 512 bytes, and must not start with `conn:`. | `execution.rs`, `start_idempotency.rs` |

### 0.2 Brainstorm — what shape does a durable route take?

| # | Idea | Verdict |
|---|------|---------|
| B1 | An extractor, `DurableRoute`, plus a builder. The handler keeps its own auth and body extractors. | **Adopted.** It fits ADR 0016 and needs no Autumn change. |
| B2 | A plugin descriptor that generates routes, as the webhook receiver does. | Rejected. Plugin routes are `RouteIdempotency::Direct`. Under `.idempotent()` every cache hit fails closed with `409`. The developer also loses the route's own extractors. |
| B3 | A `#[durable]` attribute stacked on `#[post]`. | Rejected. Macro order is fragile (F3), and the macro adds nothing the extractor lacks. |
| B4 | A tower layer that wraps the handler. | Rejected. A layer runs before the body is parsed, so it cannot build the workflow input. |
| B5 | A typestate that forces `.scope(principal)` or `.unscoped()` before `.call(..)`. | **Adopted.** An empty default scope lets one user collect another user's result. |
| B6 | Use `IdempotencyContext::scoped_key` as the default scope. | Rejected (F6). Bearer users share it, and a session rotation starts a second run. |
| B7 | Fold Autumn's `CURRENT_TENANT` into the key. | **Adopted.** Autumn's own layer keys on it for the same reason. |
| B8 | Dedupe with the start-idempotency claim (F7, F8). | **Adopted.** It is the tested exactly-one-run primitive. |
| B9 | Dedupe with a deterministic `workflow_id` only. | Rejected. It has no window and no retention reclaim. |
| B10 | Dedupe with Autumn's response cache. | Rejected. The cache is written after the handler returns, so it cannot attach a retry to a live run. |
| B11 | A fingerprint of the canonical workflow input, carried in the `workflow_id`. | **Adopted.** Both duplicate paths return the stored `workflow_id` (F7), so the check needs no extra read. |
| B12 | A fingerprint in the memo, search attributes or a side table. | Rejected. The memo needs an extra read. Workflow code can change search attributes. A table needs a migration. |
| B13 | Fold the fingerprint into the dedupe key. | Rejected. A changed body would then run the side effects twice. |
| B14 | `202` while pending, and `409` when Autumn's layer is on. | **Adopted** (F4, F5). A cached `202` would replay for 24 h. Autumn does not cache a `4xx`. |
| B15 | `202` with `Location` to a status monitor. | Deferred. Retrying the request is a complete resume path. A monitor adds a second read path. |
| B16 | A process-wide cap on waiting requests. | **Adopted** (F11). With the cap full, the route answers pending at once. |
| B17 | A shared `LISTEN` multiplexer in the engine. | Deferred. It is a core change. The cap bounds the cost for now. |
| B18 | Follow continue-as-new successors inside the wait. | **Adopted** (F10). Otherwise a live chain reads as finished. |
| B19 | A `.fingerprint(..)` override for inputs with server-made values. | Deferred. Such an input fails closed with `422` on the first retry, which is safe and visible. |
| B20 | Run on the SQLite runtime. | Rejected. The keyed start takes a Postgres connection. |

### 0.3 Reverse brainstorm — how can a durable route do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Run the side effects twice for one key. | The claim upsert admits one run (F8). Reuse and conflict policies are pinned to `AllowDuplicate` and `UseExisting`, so a same-body retry after the window still attaches while the run holds the business id. A terminated run, or one that switched type, no longer holds it (§2). |
| R2 | Give one user another user's result. | The scope is mandatory (B5). The tenant joins the key (B7). |
| R3 | Accept a changed body under a used key. | The fingerprint compare answers `422` and starts nothing (B11). |
| R4 | Let a guessable key reveal a stored result. | The scope confines a guess to one principal. The docs tell clients to use random keys. |
| R5 | Cache a pending answer for 24 h under Autumn's layer. | The route answers `409` when the layer is on (B14). |
| R6 | Hold Autumn's in-flight lock for 24 h when the request deadline drops the handler. | The wait ends 2 s before the request deadline, and every read after the start ends 1 s before it. A late read answers pending. Under the layer the route counts half the deadline, because the layer reads the body before the handler (F11, F12). |
| R7 | Lose the start when the client disconnects. | The start runs in a spawned task, so a dropped request still commits. The retry attaches through the claim. |
| R8 | Pin a pool connection for the whole wait. | The spawned task drops the connection before the wait starts. |
| R9 | Exhaust Postgres connections with waiters. | A semaphore caps the waiters (B16). A completed run needs no permit. |
| R10 | Report a continue-as-new row as the final result. | The wait follows successors (B18). A same-type chain takes one lookup, and each change of workflow type takes one step, up to `HarvestPlugin::durable_route_max_chain_steps` (1024 by default). Past the cap the route answers pending. A cycle answers `result-unavailable` (R22). |
| R11 | Leak the workflow error text, a codec envelope or an internal message. | The route never renders `WorkflowResult.error` or a `HarvestError` message. Problem bodies use fixed text. |
| R12 | Leak the execution id, which is the management API handle. | The id header is opt-in. |
| R13 | Replay an erasure tombstone as a success. | A tombstone answers `410` (F14). |
| R14 | Store the raw key in the database. | Only a SHA-256 digest reaches the database. |
| R15 | Read the key differently from a proxy, or inject a header. | More than one key field is a `400`. Only printable ASCII is valid. The route never echoes the key. |
| R16 | Answer a terminal business failure with a retry status. | `failure_status` refuses `408`, `409`, `425`, `429`, `502`, `503` and `504`. Terminal problems carry no `Retry-After`. The client contract says that a durable-route problem with no `Retry-After` is final, so a client does not loop on a final `500`. |
| R17 | Turn a schema violation into a leak of the input shape. | `422` with fixed text. The debug log holds the violation count and at most five field paths, never a violation message, because a message can quote an input value. |
| R18 | Crash on a request that arrives before plugin startup ends. | The extractor never panics. `send()` answers `503` with `Retry-After`. |
| R19 | Fail every request for a debounce, batch or throttle workflow with a confusing error. | `500` misconfigured, with an error log that names the cause. A name that is not a registered workflow answers the same way. |
| R20 | Put the key, scope or input into logs or metrics. | The tracing span records the workflow, route, outcome, creation flag and execution id only. A quota error log names the resource, not the quota key, because that key can come from the input. |
| R21 | Let Autumn's layer replay one token client's cached answer to another client. The layer keys by session cookie, not by scope. | The docs advise against `.idempotent()` for token APIs. The route logs one warning when it sees the layer and an `Authorization` header. |
| R22 | Strand a retry on a long continue-as-new chain, because each retry walks the chain from its start. | A same-type chain takes one business-id lookup to its newest run. Only a change of workflow type costs a step. A visited set stops a cycle. |
| R23 | Copy, validate, encrypt and insert a 32 MiB body before the 2 MiB input cap rejects it. | The route compares the canonical input length with the workflow's effective cap before any I/O, and answers `413`. |
| R24 | Turn a mapped failure into a final `500` because a history read failed once. | A failed typed-failure read follows the read-error policy: pending, or `result-unavailable` for a codec error. |
| R25 | Keep a plaintext copy of the output in Autumn's cache, out of reach of the codec and of erasure. | The guide states it and advises against the layer for routes whose output holds personal data. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | F1 to F15. Unknowns: the cost of many concurrent waiters (not measured), and the behaviour of `MatchedPath` under `nest`. The key leaves the route path out, so the second unknown does not change dedupe. |
| Red | Developers want proof that a side effect ran once: the replay header and the run in Vantage. A pending code that changes with `.idempotent()` feels odd, so the docs state the client rule once: `Retry-After` means "retry the same request". |
| Black | A cross-user leak through an empty scope, a cached `202`, a 24 h lock after a dropped handler, and connection exhaustion. Each has a mitigation in §0.3. An unsalted hash of a weak key is guessable, so the docs demand random keys. |
| Yellow | The run commits before the wait, so a crash or deploy loses nothing. Dedupe holds across replicas through one primary key. No migration, and no Autumn core change. |
| Green | A status monitor, a `LISTEN` multiplexer, a `.fingerprint()` override, RFC 7240 `Prefer: wait`, and a form-field key for plain HTML forms. All are deferred (§2). |
| Blue | Red phase: unit tests for the pure parts, then Postgres tests for each status. Green phase: key and fingerprint, extractor, start, wait, rendering. Refactor phase: docs, gates, then a multi-angle review. |

---

## 1. Design

### 1.1 Handler shape

```rust
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

`DurableRoute` is a `FromRequestParts<AppState>` extractor with an
`Infallible` rejection. It reads the key fields, the matched path, the
presence of `IdempotencyContext`, `CURRENT_TENANT` and the time. `send()`
renders every problem, so a misused route fails loudly for every request.
`DurableOutcome` implements `IntoResponse`. A handler can also inspect it and
render HTML.

### 1.2 Key and fingerprint

`lp(x)` is the length of `x` as a big-endian `u64`, then `x`. A tag is `0x00`
for "absent", or `0x01` and the value.

```text
D           = sha256(lp("autumn-harvest/durable-route/v1") || lp(workflow)
                     || lp(tenant tag) || lp(scope tag) || lp(raw key))
claim key   = "durable-route:v1:" || hex(D)                          81 bytes
F           = sha256(lp("autumn-harvest/durable-route/fp/v1") || lp(raw key)
                     || lp(workflow) || lp(canonical JSON of the input))
workflow_id = "dr1-" || hex(D)[0..32] || "-" || hex(F)[0..32]         69 bytes
```

The route path is not in `D`. A renamed path or a new prefix must not start a
second run for a retry that spans a deploy. A path parameter that changes the
operation must be in the input, so the fingerprint catches it.

Canonical JSON sorts object keys at every depth and keeps array order.

`F` includes the raw key. Without the key, the `workflow_id` does not reveal
the input.

### 1.3 Start

A spawned task takes a connection from `HarvestDbPool`, calls
`start_workflow_transactional` with the claim key, `AllowDuplicate`,
`UseExisting` and the memo `{"durable_route": {"method", "route"}}`. It then
drops the connection and awaits `finish()`. The handler awaits the task. When
`created` is false, the route compares both halves of the returned
`workflow_id` with its own. A mismatch is a `422`.

### 1.4 Wait

The budget is `min(wait, 30 s, deadline - now)`. `wait` defaults to 5 s. The
deadline is the extraction time plus `server.timeouts.request_timeout_ms`
minus 2 s. With Autumn's layer on, the route counts half of
`request_timeout_ms`, because the layer reads the body before the handler.
Every read after the start, the start itself included, ends 1 s before the
same deadline. A late read answers pending. The route first reads a snapshot
with no wait. Only a live run with
a budget above zero takes a waiter permit. With no permit, the route answers
pending at once. A `tokio::time::timeout` bounds the engine wait, so a slow
listener connect cannot overrun the budget. On a `CONTINUED_AS_NEW` result the
route resolves the live attempt and looks up the newest run with the same
business id. A same-type chain of any length takes one lookup. A successor
of another type comes from the continue-as-new event. A visited set stops a
cycle. The walk follows at most `durable_route_max_chain_steps` runs (1024 by
default), and then answers pending. The walk reads runs through a private
trait, so unit tests drive it on a fake chain.

### 1.5 Responses

| Case | Status | Body |
|---|---|---|
| Run completed | `success_status`: 200 (default), 201 or 204 | Workflow output JSON. 204 has no body. |
| Run live at the budget, no Autumn layer | 202, `Retry-After: 1` | `{"state":"pending"}` |
| Run live at the budget, Autumn layer on | 409, `Retry-After: 1` | Problem `pending` |
| Key absent or empty | 400 | Problem `idempotency-key-missing` |
| Several key fields, a byte outside printable ASCII, or over 255 bytes | 400 | Problem `idempotency-key-invalid` |
| Key reused with another input | 422 | Problem `idempotency-key-reused` |
| Input fails the workflow schema | 422 | Problem `input-invalid` |
| Input over the input cap | 413 | Problem `input-too-large` |
| Quota exceeded | 429, `Retry-After` | Problem `quota-exceeded` |
| Start refused for a transient cause, or plugin not started | 503, `Retry-After` | Problem `unavailable` |
| Bad route setup | 500 | Problem `misconfigured` |
| Other start error | 500 | Problem `internal` |
| Run failed with a mapped `error_type` | The mapped status | Problem `failed`, with `error_type` and `details` |
| Run failed, not mapped | 500 | Problem `failed` |
| Run cancelled, terminated or timed out | 500 | Problem `cancelled`, `terminated` or `timed-out` |
| Run erased | 410 | Problem `erased` |
| Result cannot be decoded | 500 | Problem `result-unavailable` |

A problem body is `application/problem+json`. Its `type` is
`urn:autumn-harvest:durable-route:<slug>` and its `code` is
`harvest.durable_route.<slug>`. Every answer that reports on a run that an
earlier request started carries `x-idempotent-replayed: true`.
`expose_execution_id()` adds `x-harvest-execution-id` to the same answers. The
`idempotency-key-reused` answer carries neither header.

### 1.6 Autumn's idempotency layer

With `.idempotent()` on, Autumn answers before the route in three cases:

- an in-flight duplicate gets `409`;
- a changed body against a cached answer gets `422`;
- a cache hit gets the cached answer.

The route answers pending with `409`, so Autumn never caches it. The layer
keys its cache by session cookie, not by scope. Two token clients with one key
and one body share a cached answer, so the docs advise against the layer for
token APIs (R21). The cached answer is plaintext output that erasure does not
reach (R25).

Autumn fails closed with a `409` on every cache hit in four cases: a route
with `#[intercept]`, a route in a scoped group, a route in a merged or nested
router, and an app with its own `AppBuilder::layer` or `static_gate`. The docs
advise against the layer in such an app.

---

## 2. Known limits

- Postgres plugin runtime only. No SQLite runtime and no `HarvestEmbedding`.
- No status monitor and no `Location`. A pending client must keep the body
  and the key, and retry.
- The input must be a pure function of the request and the principal. A
  server-made value in the input makes every retry a `422`.
- A key binds its input for `start_idempotency_window` (24 h by default).
  After that, the same input attaches to the original run until retention
  deletes it, and the attach binds the key again. Another input under an
  expired key starts a new run.
- After the window, a retry of a terminated run, or of a run that continued
  as new into another type, starts a new run. Those runs no longer hold the
  business id.
- Erasure works on one execution. A durable run with retries or a
  continue-as-new chain needs each execution erased.
- The input-size check uses the current cap, before the claim probe. A retry
  of an accepted input gets `413` when an operator lowers the cap.
- The route counts its deadline from the time it reads the request.
  Middleware time before the handler uses up the margin.
- Workflow-level retries run the activities again. Activities must be
  idempotent. Put a mapped `error_type` in `non_retryable_errors`, or the
  mapped answer waits for every retry.
- Durable runs have no Harvest tenant, record the `transactional` start
  source and do not join the HTTP trace.
- Each waiting request holds one `LISTEN` connection. The waiter cap (default
  32 for each process) is the only bound.
- A per-route `timeout_ms` is invisible to the handler. Set `wait` below it.
- A reset seal reads as `terminated`. A paused run reads as pending.
- Untyped failures (`Err(String)`) cannot be mapped.
- The key and fingerprint hashes are unsalted SHA-256. Keys must be random.
- No OpenAPI response schema for a handler that returns `DurableOutcome`.
- A plain HTML form cannot send the header. htmx `hx-headers` can. There is
  no form-field key.
- No in-process single-flight. Concurrent duplicates each reach the
  database, and the claim admits one run.
- No `LISTEN` multiplexer. Each waiting request opens its own listener.
- No `.fingerprint()` override. The fingerprint covers the whole input.
- No RFC 7240 `Prefer: wait`. The handler sets the budget with `.wait(..)`.
- No metric family for durable routes. The `harvest.durable_route` span holds
  the workflow, the route, the outcome, the creation flag and the execution
  id.
- The claim window is one setting for the plugin, not one for each route.
- The start does not join the handler's own database transaction.
- The route keeps its own chain walk. It reads the engine's result snapshot.
  The MCP task tools read the state before a reset seal. For a reset-sealed
  run, the route therefore answers `terminated`, and the MCP task tools
  report the earlier state.
- After an operator rerun, a retry of the original request gets the outcome
  that the original run recorded. `failure_status` still maps its typed
  failure.

---

## 3. Tests and acceptance criteria

Unit tests live in `autumn-harvest-plugin/src/durable_route.rs`. Postgres tests
live in `autumn-harvest-plugin/tests/durable_route_integration.rs`. Docs guards
live in `autumn-harvest-plugin/tests/durable_route_docs.rs`.

| AC | Criterion | Test |
|----|-----------|------|
| AC-01 | The extractor never rejects, and it captures the key fields, the layer flag and the tenant. Only autumn-web can build its layer context, so the layer-on case runs end to end. | `extractor_never_rejects_and_captures_request_facts`, `idempotent_layer_pending_is_409_and_not_cached` |
| AC-02 | With no runtime, `send()` answers `503` with `Retry-After: 1`. | `missing_runtime_returns_503_with_retry_after` |
| AC-03 | An empty scope, an input that does not serialize, or an invalid status answers `500` misconfigured before any I/O. | `misconfigured_call_returns_500_before_any_io` |
| AC-04 | The key policy table holds for absent, empty, repeated, non-ASCII and 256-byte keys. | `key_policy_table` |
| AC-05 | A pinned golden vector gives the 81-byte claim key, and Harvest's key validator accepts it. | `dedupe_key_golden_vector` |
| AC-06 | Shifted component boundaries, tenant presence, scope presence and workflow names never collide. | `dedupe_key_components_never_collide` |
| AC-07 | Object key order, also from a map type that keeps insertion order, does not change the fingerprint. Array order and the raw key do. | `fingerprint_is_order_independent_and_keyed`, `fingerprint_ignores_object_key_order` (property) |
| AC-08 | The `workflow_id` is 69 bytes, parses back, and a foreign id is a mismatch. | `workflow_id_round_trips_and_rejects_foreign_ids` |
| AC-09 | Every `WorkflowResultState` maps to the documented status, with the layer on and off. | `status_map_covers_every_workflow_result_state`, `failure_mapping_uses_the_last_status_and_spots_erasure` |
| AC-10 | Start errors map to the documented statuses. Every pool, shard, load and database error answers `503` with a `Retry-After` of at least 1 s. No error text reaches the body. | `start_errors_map_to_public_statuses`, `retryable_start_errors_answer_503_with_retry_after` |
| AC-11 | Codec read errors give `result-unavailable`. Other read errors give pending. The wait and the typed-failure read share one answer. | `read_errors_map_to_result_unavailable_or_pending` |
| AC-12 | The wait budget defaults to 5 s, clamps to 30 s and to the request deadline less 2 s. | `wait_budget_is_clamped` |
| AC-13 | Every problem has the URN type, the code and fixed text. | `problem_bodies_never_carry_internal_text` |
| AC-14 | The replay header needs a found run. The id header needs `expose_execution_id()`, also on a replay. | `replayed_and_execution_id_headers` |
| AC-15 | `success_status(204)` sends no body. `success_status(201)` sends the output. | `success_status_shapes_the_completed_response` |
| AC-16 | A completed run answers `200` with the output, and no replay header. | `completed_run_returns_200_with_output` |
| AC-17 | A retry starts no new run and no new side effect, and carries the replay header. | `retry_with_same_key_runs_workflow_once` |
| AC-18 | 20 concurrent same-key requests share one run. | `concurrent_duplicates_without_layer_share_one_run` |
| AC-19 | Two principals with one key get two runs and their own outputs. | `principals_with_same_key_get_separate_runs` |
| AC-20 | Two tenants with one key get two runs. | `tenants_with_same_key_get_separate_runs` |
| AC-21 | No table holds the raw key in any row, and the same scan finds the digest claim. | `raw_key_never_reaches_the_database` |
| AC-22 | A changed body answers `422` and starts no run. | `changed_body_with_same_key_returns_422_without_new_run` |
| AC-23 | After the claim window, the same body attaches and binds the key again, and another body starts a run. | `same_body_after_claim_window_attaches_to_original_run` |
| AC-24 | A missing or repeated key answers `400` and starts nothing. | `missing_or_invalid_key_returns_400_and_starts_nothing` |
| AC-25 | A schema violation answers `422` with only the fixed problem members, and the key stays usable. | `schema_violation_returns_422_and_key_stays_usable` |
| AC-26 | An input over the deployment input cap answers `413` and starts nothing. | `oversized_input_returns_413_and_starts_nothing` |
| AC-27 | An unregistered, debounce, batch or throttle workflow answers `500` misconfigured. | `unusable_workflow_returns_500_misconfigured` |
| AC-28 | A parked run answers `202`, and a retry after the signal answers `200`. | `pending_then_retry_returns_result` |
| AC-29 | With Autumn's layer, pending is `409` and is not cached. A later repeat comes from Autumn's cache, with no handler run. | `idempotent_layer_pending_is_409_and_not_cached` |
| AC-30 | With Autumn's layer, a changed body against a cached answer gets Autumn's own `422` text, and the handler does not run. | `idempotent_layer_answers_cached_mismatch` |
| AC-31 | The wait ends before the request deadline. | `wait_is_clamped_below_request_timeout` |
| AC-32 | A full waiter cap answers pending at once, while the permit holder waits out its budget. | `waiter_cap_answers_pending_at_once` |
| AC-33 | A cap of zero never waits, and a completed replay still answers `200`. | `completed_replay_needs_no_waiter_permit` |
| AC-34 | A request dropped inside its start transaction still commits one run, and a retry attaches. | `dropped_request_still_commits_and_retry_attaches` |
| AC-35 | `success_status(201)` applies to the first answer and to a replay. | `success_status_201_is_applied_to_completed_runs` |
| AC-36 | A mapped failure answers the mapped status with `error_type` and details, and no message. | `mapped_failure_returns_mapped_status_with_details` |
| AC-37 | Unmapped and untyped failures answer `500` with no message. | `unmapped_failure_returns_generic_500` |
| AC-38 | A cancelled run answers `500` cancelled. | `cancelled_run_returns_500_cancelled` |
| AC-39 | A terminated run answers `500` terminated. | `terminated_run_returns_500_terminated` |
| AC-40 | A timed-out run answers `500` timed-out. | `timed_out_run_returns_500_timed_out` |
| AC-41 | A continue-as-new chain answers with the final output. | `continued_as_new_chain_returns_final_output` |
| AC-42 | An erased run answers `410`. | `erased_run_returns_410` |
| AC-43 | The run memo names the method and route, and holds no key, scope or input. | `run_memo_names_the_route` |
| AC-44 | The docs status table lists exactly the problem slugs. | `status_table_matches_problem_slugs` |
| AC-45 | The docs page has inbound links from the idempotency chapter and the architecture guide. | `page_is_linked_from_the_idempotency_chapter_and_architecture` |
| AC-46 | The docs page states the client rule, the key guidance, both deadline margins, the per-route timeout rule, the token-API and plaintext-cache warnings, the step cap and the `#[intercept]` warning. | `page_states_layer_timeout_and_key_guidance` |
| AC-47 | Every test this table names exists in the plugin sources or tests. The note keeps its planning sections and its known limits. | `design_note_ac_table_names_existing_tests`, `design_note_keeps_its_planning_sections` |
| AC-48 | One changelog fragment describes the feature and states "No migration" and "No new `WorkflowEvent`". | `changelog_fragment_describes_the_feature` |
| AC-49 | Both new test binaries have a sorted row in `.github/ci/integration-suites.txt`. | Existing `ci_run_coverage.rs` suite |
| AC-50 | Comment hygiene adds no Tier A defect and no Tier B regression. | `docs/audits/comment-hygiene.py --base origin/trunk-dev` |
| AC-51 | `cargo fmt` and `cargo clippy -D warnings` pass for the plugin. | CI lint job |
| AC-52 | The diff adds no migration, no `harvest_events` writer and no version bump. `sha2` is a plain dependency of the plugin. The start calls `finish()` after it commits. | Review |
| AC-53 | A stalled start ends with a pending answer before the request deadline. | `reads_after_the_start_end_before_the_request_deadline` |
| AC-54 | A retry attempt that continues as new into another type gives the final output. | `retry_attempt_that_switches_type_returns_final_output` |
| AC-55 | A request holds no pooled connection while it waits. Eight waiters on a four-slot pool each wait out their budget, and a ninth request completes. | `start_connection_is_released_before_the_wait` |
| AC-56 | After an operator rerun, a retry of the original request gets the outcome its own run recorded. | `rerun_sealed_run_reports_its_original_outcome` |
| AC-57 | Result-wait `LISTEN` connections never exceed the waiter cap. | `listen_connections_never_exceed_waiter_cap` |
| AC-58 | The chain walk follows each step to the last run. A continue-as-new run with no successor is the last run. A finished chain needs no wait. | `chain_walk_follows_each_step_to_the_final_run` |
| AC-59 | Past the step cap, the walk answers pending. The default cap is 1024. | `chain_walk_answers_pending_past_the_step_cap` |
| AC-60 | A cycle in a chain answers `result-unavailable`. | `chain_walk_stops_at_a_cycle` |
| AC-61 | A codec error in any read of the walk answers `result-unavailable`. Another read error answers pending. | `chain_walk_maps_read_errors` |
| AC-62 | Only a wait takes a permit. A full cap or a zero budget never waits, and a wait returns its permit. | `chain_walk_takes_a_permit_only_to_wait` |
| AC-63 | The schema log holds the count and at most five field paths, never a message. The quota log holds no quota key. | `start_error_logs_hold_no_input_values` |
| AC-64 | The span records the workflow, the route, the creation flag, the execution id and the outcome. No route log holds the key, the scope or the input. A setup log names its cause. | `send_logs_hold_no_key_scope_or_input`, `durable_route_logs_hold_no_key_scope_or_input` |
| AC-65 | Under Autumn's layer with an `Authorization` header, the route warns once. | `layer_with_authorization_warns_once` |
| AC-66 | The outcome accessors expose the kind, the status, the execution id, the replay flag and the output. A missing output decodes from `null`. | `outcome_accessors_expose_the_result` |
| AC-67 | The status column of the docs table matches the code for each problem slug. | `status_table_statuses_match_the_code` |
| AC-68 | With Autumn's layer, a duplicate in flight gets Autumn's `409` with `Retry-After: 1`. It skips the route and starts no run. | `idempotent_layer_answers_in_flight_duplicate` |
| AC-69 | A stored output under an unknown codec answers a final `500` `result-unavailable`. | `undecodable_output_returns_result_unavailable` |
| AC-70 | Failure details under an unknown codec answer `result-unavailable` to a mapped call and `failed` to an unmapped call. | `undecodable_typed_failure_returns_result_unavailable` |
| AC-71 | A same-type chain takes one step, so a cap of two reaches the end of an eleven-run chain. | `same_type_chain_takes_one_step` |
| AC-72 | One step cannot cross a change of workflow type, so a cap of one answers pending end to end. | `chain_step_cap_answers_pending` |
| AC-73 | An oversized input answers `413` before any I/O, while every start stalls. | `oversized_input_is_refused_before_any_io` |
| AC-74 | A workflow's raised input cap admits an input over the deployment cap. | `raised_input_cap_admits_a_larger_input` |
| AC-75 | An unscoped route shares one key space. A same-key retry attaches, another body gets `422`, and a scoped key is separate. | `unscoped_route_shares_one_key_space` |
| AC-76 | After an operator rerun, a mapped failure, a cancelled run and a timed-out run keep their outcome. | `rerun_sealed_failures_report_their_original_outcome` |
| AC-77 | The scope is mandatory by type. `call` does not compile before `scope` or `unscoped`. | `DurableRoute` doctest (compile-fail) |
| AC-78 | The waiter cap and the step cap have their defaults. A step cap of `0` counts as `1`. | `durable_route_limits_have_defaults_and_a_floor` |
