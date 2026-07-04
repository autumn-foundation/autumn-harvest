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

Built on autumn-web 0.5's MCP layer (autumn#1117 tool exposure, autumn#1118
streaming): tools are served at `AppBuilder::mount_mcp("/mcp")` over
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
    .api("/api/harvest")
    .mcp_tools()                                        // or .mcp_tools_at("/custom/prefix")

// 3. App-side MCP endpoint (autumn-web):
autumn_web::app()
    .plugin(…)
    .mount_mcp("/mcp")
    .secure_mcp(RequireApiToken::new(…))                // gate agent access in production
```

For `WorkflowInfo` values built outside the macro, use `.with_mcp()`.

A workflow with a `debounce` or `batch` policy is **excluded** from MCP
exposure (with a `tracing::warn!`), even when `mcp` is set: a deferred start
can return `202 Accepted` with no `execution_id` yet, which every generated
tool's "durable handle immediately" contract depends on.

## Generated tool set (per workflow `foo`, mcp update `bar`)

| Tool (operation id) | Verb + route | Arguments | Semantics |
|---|---|---|---|
| `start_foo` | POST `{prefix}/workflows/foo/start` | `body` = workflow input | Starts a durable run, returns `{execution_id, workflow_name, workflow_id, state}` **immediately** — never blocks to completion |
| `foo_status` | GET `{prefix}/workflows/foo/{handle}/status` | `handle` | State, `current_details` breadcrumb, output/error, timestamps, `is_terminal` |
| `signal_foo` | POST `{prefix}/workflows/foo/{handle}/signal/{signal_name}` | `handle`, `signal_name`, `body` = payload | Async signal; unblocks `wait_for_signal`/`receive_signal`. `Idempotency-Key` header supported |
| `foo_update_bar` | POST `{prefix}/workflows/foo/{handle}/update/bar` | `handle`, `body` = update input | **Synchronous** request/response: validated, durably admitted, executed, result returned (default 30 s wait) |
| `foo_watch` | GET `{prefix}/workflows/foo/{handle}/watch` | `handle` | Streaming progress over MCP `notifications/progress`; terminates with the final state |

`{prefix}` defaults to `{api_path}/mcp` (`/api/harvest/mcp` when no management
API is mounted); override with `mcp_tools_at`.

**The handle is the correlation token**: `start_foo` returns `execution_id`,
and every other tool takes it as the `handle` argument, so an agent can drive
a specific run across separate tool calls (and across separate sessions — the
handle is durable). A handle minted by one workflow is rejected by another
workflow's tools with the same 404 an unknown handle gets, so tools are not
an existence oracle across workflows.

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
  0.5 derives annotations from the HTTP verb and only emits
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
  enabled; `.api(path)` (no auth) leaves both surfaces open, matching today's
  unauthenticated-by-default posture for that configuration. **`secure_mcp`
  alone is not enough**: `HarvestPlugin` cannot detect or intercept it (it's
  configured on the outer `AppBuilder`, after `Plugin::build` returns), so
  enabling `mcp_tools()` without also configuring `api_with_auth` logs a
  `tracing::warn!` at startup naming this exact gap — the generated routes
  are reachable unauthenticated at their own path regardless of `secure_mcp`.

## Testing

- No-DB JSON-RPC surface tests: `autumn-harvest-plugin/tests/mcp_tools_http_tests.rs`.
- Full agent flow + restart survival (Docker/testcontainers):
  `autumn-harvest-plugin/tests/mcp_tools_integration.rs`.
- Example: `autumn-harvest-plugin/examples/mcp_tools_quickstart.rs`
  (`cargo run -p autumn-harvest-plugin --example mcp_tools_quickstart --features mcp`).
