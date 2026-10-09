# MCP Tools — expose `#[workflow]`s to AI agents (issue #597)

AI agents are bad at long-running work: the MCP tool model is
request/response, so the moment a tool takes 20 minutes, needs to sleep until
tomorrow, or needs a human to approve a step, the connection dies and the
agent forgets. Harvest already solves the hard half — durable, crash-safe,
resumable workflows on Postgres with signals, updates, and durable timers.
This feature is the front door: a `#[workflow(mcp)]` opt-in that hands an
agent a workflow as a correlated set of MCP tools so it can **start** durable
work, **watch** it, and **steer** it — without the work being tied to the
agent's fragile, short-lived session.

Built on autumn-web's MCP layer (introduced in 0.6: autumn#1117 tool
exposure, autumn#1118 streaming): tools are served at `AppBuilder::mount_mcp("/mcp")` over
Streamable-HTTP JSON-RPC, and `tools/call` replays an in-process HTTP request
through the real, authenticated handler pipeline.

## Opt-in

Three pieces, each explicit:

```rust
// 1. Per-workflow opt-in (and optionally per-update):
#[workflow(mcp, description = "Review a document with a human approval gate")]
async fn document_review(ctx: &WorkflowContext, request: ReviewRequest) -> Result<String, String> { … }

#[update(workflow = "document_review", mcp)]
async fn set_deadline(_ctx: &WorkflowContext, req: DeadlineRequest) -> Result<String, String> { … }

// 2. Plugin-side route generation (cargo feature `mcp` on autumn-harvest-plugin):
HarvestPlugin::new()
    .workflows(vec![__autumn_workflow_info_document_review()
        .with_input_schema_fn(review_input_schema)])   // issue #373 schema => typed tool input
    .updates(updates![set_deadline])
    // api_with_auth (not plain `.api(...)`) is required in production: it
    // protects both the management API and every generated MCP tool
    // route's own HTTP path -- secure_mcp below only gates the /mcp
    // JSON-RPC envelope (see "Safety posture" below).
    .api_with_auth("/api/harvest", RequireApiToken::new(…))
    .mcp_tools()                                        // or .mcp_tools_at("/custom/prefix")

// 3. App-side MCP endpoint (autumn-web):
autumn_web::app()
    .plugin(…)
    // Also secure the /mcp JSON-RPC envelope itself (initialize/tools/list/
    // tools/call dispatch) -- both layers are needed.
    .secure_mcp(RequireApiToken::new(…))
    .mount_mcp("/mcp")
```

For `WorkflowInfo` values built outside the macro, use `.with_mcp()`.

A workflow with a `debounce` or `batch` policy is **excluded** from MCP
exposure (with a `tracing::warn!`), even when `mcp` is set: a deferred start
can return `202 Accepted` with no `execution_id` yet, which every generated
tool's "durable handle immediately" contract depends on.

Generated tool names can collide across two differently-named workflows
(e.g. workflow `invoice_status`'s start tool, `start_invoice_status`, is the
same string as workflow `start_invoice`'s status tool, `start_invoice_status`).
autumn-web's tool derivation keeps only the first registration on a name
collision and logs a `tracing::warn!` (never silent, but it *is* a partial
tool set — the losing registration's other, non-colliding tools would still
ship). Harvest's own collision check runs one step earlier and is stricter:
a colliding workflow (whichever loses the tiebreak, by registration order) is
**excluded** from MCP exposure entirely (its own `tracing::warn!`), rather
than partially shipped with one tool quietly missing from `tools/list`.

This Harvest-side check only compares Harvest-generated operation ids
against each other — it has no visibility into MCP tool names the host app
registers itself (via its own `AppBuilder::routes(...)` calls, before or
after `HarvestPlugin::build()` runs), since `AppBuilder` exposes no accessor
to introspect already- or later-registered routes. If a generated Harvest
tool name collides with an unrelated app-registered tool name, that
collision falls through to autumn-web's own `derive_tools` fallback above
(first-registered-in-the-final-route-list wins, warned, not silent) instead
of Harvest's whole-workflow exclusion. Avoid this by choosing workflow/update
names that don't collide with your app's other MCP tool names, or by
checking your app's full route list for `start_*`/`*_status`/`signal_*`/
`*_watch`/`*_update_*` name clashes before enabling `mcp_tools()`.

## Generated tool set (per workflow `foo`, mcp update `bar`)

| Tool (operation id) | Verb + route | Arguments | Semantics |
|---|---|---|---|
| `start_foo` | POST `{prefix}/workflows/foo/start` | `body` = workflow input | Starts a durable run, returns `{execution_id, workflow_name, workflow_id, state}` **immediately** — never blocks to completion |
| `foo_status` | GET `{prefix}/workflows/foo/{handle}/status` | `handle` | State, `current_details` breadcrumb, output/error, timestamps, `is_terminal` |
| `signal_foo` | POST `{prefix}/workflows/foo/{handle}/signal/{signal_name}` | `handle`, `signal_name`, `body` = payload | Async signal; unblocks `wait_for_signal`/`receive_signal`. `Idempotency-Key` header supported |
| `foo_update_bar` | POST `{prefix}/workflows/foo/{handle}/update/bar` | `handle`, `body` = update input | **Synchronous** request/response: validated, durably admitted, executed, result returned (default 30 s wait) |
| `foo_watch` | GET `{prefix}/workflows/foo/{handle}/watch` | `handle` | Streaming progress over MCP `notifications/progress`; terminates with the final state |

**Known limitation (issue #2035):** the engine admits a declarative update
but does not run its handler yet. `foo_update_bar` validates and admits the
update, and then returns 504 after the 30 s wait.

`{prefix}` defaults to `{api_path}/mcp` (`/api/harvest/mcp` when no management
API is mounted); override with `mcp_tools_at`.

**The handle is the correlation token**: `start_foo` returns `execution_id`,
and every other tool takes it as the `handle` argument, so an agent can drive
a specific run across separate tool calls (and across separate sessions — the
handle is durable). A handle minted by one workflow is rejected by another
workflow's tools with the same 404 an unknown handle gets, so tools are not
an existence oracle across workflows.

## DAGs as MCP tools

A unified DAG (`unified-dag-execution`, on by default) is already a
`WorkflowInfo` under the hood — `#[dag]` lowers it onto a shadow companion
that `HarvestBuilder::dags()` auto-registers alongside ordinary workflows.
Opt one into MCP the same way:

```rust
#[dag(schedule = "0 2 * * *", mcp)]
fn daily_etl(dag: &mut DagBuilder) { … }
```

A DAG's tool set is a **subset** of an ordinary workflow's — exactly three
tools, `start_daily_etl`, `daily_etl_status`, `daily_etl_watch`:

- **No `signal_{dag}`, no update tools.** The generated level-walking DAG
  handler never waits on a signal and DAGs have no update handlers, so both
  are omitted rather than shipped as dead tools.
- **`start_{dag}` preserves the DAG trigger contract.** Unlike an ordinary
  workflow's `start_foo` (which delegates to the generic `start_workflow`
  path), a DAG's start tool routes through the same `trigger_dag_run` handler
  `POST /dags/{name}/trigger` uses — so admission gates and the DAG's
  `max_active_runs`/paused enforcement apply identically to an
  MCP-originated start. This matters: `start_workflow` has no notion of
  either, and would silently let an agent spawn more concurrent DAG runs than
  the operator-configured policy allows.

`status`/`watch` need no DAG-specific handling — they already work by
execution id regardless of whether it names a plain workflow or a DAG run.

## Typed input schema — no second schema

`start_foo`'s `inputSchema` embeds the workflow's published JSON Schema
(issue #373, `with_input_schema_fn` / `with_schemas::<I, O, E>()`) as a
self-contained `$defs` component:

```json
{ "type": "object",
  "properties": { "body": { "$ref": "#/$defs/HarvestMcpInput_foo" } },
  "required": ["body"],
  "$defs": { "HarvestMcpInput_foo": { "type": "object", "properties": { … } } } }
```

Start input is additionally validated against that schema at the tool edge
(400 with structured violations) before any storage access. Workflows without
a published schema get a permissive object schema. Update tools currently
publish a permissive schema carrying the Rust input type hint in the tool
description (schema publishing for updates is a follow-up).

An mcp update declared with `#[update(workflow = "…", validator = …, mcp)]`
has its validator run before admission: an invalid payload is rejected
(`422` with `{"error": "update rejected by validator", "reason"}`) instead
of becoming durable history that then runs or fails deep inside the
workflow.

## Streaming progress (`foo_watch`)

The watch tool returns SSE that autumn-web's MCP layer projects onto the
Streamable-HTTP channel as `notifications/progress` messages (client must send
`Accept: application/json, text/event-stream` and a `params._meta.progressToken`).
Frames are pushed by the shard's LISTEN/NOTIFY `harvest_events` channel — no
busy-polling anywhere:

- progress frames: `{"progress": <n>, "message": <current_details | state>}` —
  publish meaningful breadcrumbs from workflow code with
  `ctx.set_current_details("step 2/5: …")`;
- terminal frame (`event: result`): `{"state", "output", "error"}` — becomes
  the final id-correlated `tools/call` result.

An already-terminal run yields the result frame immediately.

## Durability & determinism

- A workflow started via an MCP tool is an ordinary Harvest execution: it
  survives daemon restarts, the agent does not need to stay connected for
  activities to run or durable timers to fire, and a new process on the same
  database resumes it (integration-tested in
  `autumn-harvest-plugin/tests/mcp_tools_integration.rs`).
- MCP exposure is strictly an HTTP-edge concern. The `mcp` flag is never
  consulted by core execution: no new `WorkflowEvent` variant, no migration,
  no replay surface. Nothing about MCP runs inside the deterministic workflow
  body.
- All four handle-taking tools transparently follow a `ContinuedAsNew`
  successor chain (the same chain-following `GET /workflows/{id}/result`
  uses, issue #527) — a handle for a run that continued itself keeps
  working. `foo_status`/`foo_watch` report the eventual successor's real
  state/output/error, never the sealed predecessor's dead-end
  `CONTINUED_AS_NEW` sentinel; `signal_foo`/`foo_update_bar` resolve to the
  live successor's execution id before delegating, so a signal or update
  sent against the original handle still reaches the running workflow
  instead of failing against a terminal predecessor.

## Safety posture

- **Opt-in only, twice.** Only `mcp`-flagged workflows/updates surface, and
  only when the embedder calls `mcp_tools()`. There is no expose-all firehose
  for workflows: autumn-web's `expose_all_as_mcp` hatch is read-only
  (GET-only) by design and never picks up the mutating workflow tools.
- **Annotations.** Read tools (`_status`) carry `readOnlyHint: true`; the
  mutating tools carry `readOnlyHint: false`. Known inherited gap: autumn-web
  (still as of 0.7) derives annotations from the HTTP verb and only emits
  `destructiveHint: true` for DELETE routes, so the mutating workflow tools
  cannot yet carry a literal `destructiveHint` — flagged for an autumn-web
  follow-up.
- **Auth principal.** `tools/call` forwards the caller's credentials
  (authorization/cookie headers, resolved client identity) into the replayed
  in-process request, so the tools run under the same authenticated principal
  as any HTTP call. The generated tool routes fail closed (runtime not
  started) before startup completes, regardless of auth configuration.
  **Two auth layers, both worth configuring:** `secure_mcp(...)` gates the
  `/mcp` JSON-RPC envelope itself (`initialize`/`tools/list`/`tools/call`
  dispatch); `HarvestPlugin::api_with_auth(path, middleware)` additionally
  applies the *same* middleware directly to every generated tool route's own
  HTTP path (issue #597 code-review hardening — the tool routes are
  registered via `AppBuilder::routes(...)`, not `nest()`, so without this a
  caller could bypass `secure_mcp` entirely by hitting a tool's route path
  directly instead of going through `/mcp`). Configure `api_with_auth`
  wherever the management API needs a credential and MCP tools are also
  enabled. With `.api(path)` (no auth), the mutating tool routes fail closed
  with `401` outside the `dev` profile (issue #1802). That includes a
  `tools/call` that `secure_mcp` admits, unless the caller has an admin
  session. They stay open in `dev` and under
  `allow_unauthenticated_mutations()`. Read tools stay open. A scoped API
  token does not authorize these routes, because the token layer wraps only
  the nested management router. **`secure_mcp`
  alone is not enough**: `HarvestPlugin` cannot detect or intercept it (it's
  configured on the outer `AppBuilder`, after `Plugin::build` returns), so
  enabling `mcp_tools()` without also configuring `api_with_auth` logs a
  `tracing::warn!` at startup naming this exact gap. `secure_mcp` does not
  gate a generated route's own path.

## MCP Tasks (issue #2005)

A tool call can take longer than the client tool timeout, often 60 s. The
`io.modelcontextprotocol/tasks` extension (MCP 2026-07-28) fixes this. The
server answers `tools/call` with a task, and the client polls for the result.
`mcp_tasks()` serves each `#[workflow(mcp)]` workflow as a task:

```rust
HarvestPlugin::new()
    .workflows(vec![__autumn_workflow_info_document_review()])
    .api_with_auth("/api/harvest", RequireApiToken::new(…))
    .mcp_tasks()                       // or .mcp_tasks_at("/custom/tasks")
```

autumn-web's `/mcp` endpoint cannot dispatch the `tasks/*` methods. So
Harvest serves its own JSON-RPC route at `{tools prefix}/tasks`, default
`/api/harvest/mcp/tasks`. Point a Tasks-capable MCP client at that URL. The
route does not need `mcp_tools()` or `mount_mcp`.

| Method | What Harvest does |
|---|---|
| `initialize`, `server/discover` | Advertise `capabilities.extensions["io.modelcontextprotocol/tasks"]`. |
| `tools/list` | One `start_{wf}` tool for each MCP workflow. Its `inputSchema` is the same as on `/mcp`. |
| `tools/call` | Start the run. A client that declares the extension gets a `CreateTaskResult` (`resultType: "task"`). Any other client gets the plain start handle. |
| `tasks/get` | Read the run and return the `DetailedTask`. |
| `tasks/update` | Deliver each `accept` answer as a signal. |
| `tasks/cancel` | Cancel the live run. |

A `tasks/*` call needs the extension in its own
`params._meta["io.modelcontextprotocol/clientCapabilities"]`. Without it, the
call gets error `-32021`.

**The task is the run.** The task id is the execution id. Each read derives
the task from the execution row, so no task state is stored and a restart
loses nothing. A retry or continue-as-new chain is followed to the live run.

| Run state | Task status | Payload |
|---|---|---|
| `COMPLETED` | `completed` | `result`: a `CallToolResult` with the output, `isError: false` |
| `FAILED` with no retry left, `TIMED_OUT` | `completed` | `result`: a `CallToolResult` with the error, `isError: true` |
| `CANCELLED`, `TERMINATED` | `cancelled` | — |
| Live, parked on `wait_for_signal` | `input_required` | `inputRequests`: one `elicitation/create` for each wait |
| Live, any other wait | `working` | `statusMessage`: the `current_details` text |

A workflow error is a tool error, as the spec requires. Harvest never reports
`failed`, so a client does not treat a business error as a protocol fault and
retry it.

**Crash-safe create.** A client that retries `tools/call` sends the same
start key. Put it in the `Idempotency-Key` header, or in
`params._meta["io.autumn-harvest/idempotencyKey"]`. The header wins. The
start then dedups as in issue #808, and the retry gets the same task. A call
with no key starts a new run each time.

**Input.** The awaitables replay (issue #615) finds each parked
`wait_for_signal`. Each wait gets the key `{run id}:signal:{name}:{n}`, where
`n` counts the waits on that signal name. The key does not change while the
run waits. An `accept` answer in `tasks/update` sends its `content` object as
the signal payload. The key is also the signal idempotency key, so a retried
answer is a no-op. Harvest ignores an answer to a key that is not open, and
any action other than `accept`.

**TTL.** `ttlMs` is `null` while the run is live. After the run ends, `ttlMs`
runs from `createdAt` to the time that retention can delete the run. With no
retention, it stays `null`. After the run is deleted, `tasks/get` answers
`-32602` "Task not found". `pollIntervalMs` is 5000.

**Limits.**

- A `#[dag(mcp)]` DAG is not served. Its trigger takes no start key, so a
  retried create could start a second run.
- A debounced or batched workflow is not served, as on `/mcp`.
- Only a signal wait is `input_required`. An update wait and an
  `await_condition` park read as `working` (issue #2035).
- `requestedSchema` is an open object. The client answer is the signal
  payload as is.
- Harvest does not push `notifications/tasks`. Poll `tasks/get`.

**Auth.** The route takes the layers of a mutating tool route:
`api_with_auth`, the custom-role gate, the read-only role gate, the
fail-closed mutation gate (issue #1802) and the tenant refusal (issue #1977).
Every method on the route counts as a mutation, because the route can start
and cancel runs. The route takes `application/json` only. A browser cannot
send that cross-site without a CORS preflight.

## Testing

- No-DB JSON-RPC surface tests: `autumn-harvest-plugin/tests/mcp_tools_http_tests.rs`.
- Full agent flow + restart survival (Docker/testcontainers):
  `autumn-harvest-plugin/tests/mcp_tools_integration.rs`.
- No-DB MCP Tasks route tests: `autumn-harvest-plugin/tests/mcp_tasks_http_tests.rs`.
- MCP Tasks lifecycle (Docker/testcontainers):
  `autumn-harvest-plugin/tests/mcp_tasks_integration.rs`.
- Example: `autumn-harvest-plugin/examples/mcp_tools_quickstart.rs`
  (`cargo run -p autumn-harvest-plugin --example mcp_tools_quickstart --features mcp`).
