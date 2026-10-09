## Feature — MCP Tasks for `#[workflow(mcp)]` workflows (issue #2005)

`HarvestPlugin::mcp_tasks()` serves each `#[workflow(mcp)]` workflow as a task
of the `io.modelcontextprotocol/tasks` extension (MCP 2026-07-28). A client
gets a task at once and polls `tasks/get` for the result. A long run no
longer hits a tool timeout.

autumn-web's `/mcp` cannot dispatch `tasks/*`, so Harvest serves its own
JSON-RPC route at `{tools prefix}/tasks`, default `/api/harvest/mcp/tasks`.
`mcp_tasks_at(path)` moves it. The route answers `initialize`,
`server/discover`, `ping`, `tools/list`, `tools/call`, `tasks/get`,
`tasks/update` and `tasks/cancel`.

- The task id is the execution id. Each read derives the task from the run,
  so no task state is stored.
- `COMPLETED` is `completed`. `FAILED` with no retry left and `TIMED_OUT` are
  `completed` with `isError: true`, so a workflow error is a tool error.
  `CANCELLED` and `TERMINATED` are `cancelled`. Harvest never reports
  `failed`.
- A retried `tools/call` with the same `Idempotency-Key` header, or the same
  `io.autumn-harvest/idempotencyKey` in `_meta`, gets the same task. The
  start dedups as in issue #808.
- A run parked on `wait_for_signal` is `input_required`, found by the
  awaitables replay (issue #615) and cached for each history position. Each
  wait gets a key from the run id and its history position. An `accept`
  answer in `tasks/update` delivers the signal, with the key as its
  idempotency key. A `decline`, an answer without the `payload` string, or a
  payload that the signal refuses, gets `-32602`. A client without the `elicitation` capability sees `working`.
- Only a 2026-07-28 request can use the extension. A 2025 session does not
  see it advertised.
- A DAG, and a debounced or batched workflow, is not served.
- The route takes the auth layers of a mutating tool route and accepts
  `application/json` only.

**No migration. No new `WorkflowEvent` variant.** The change is HTTP-edge
only, behind the `mcp` cargo feature. The plan is in `DESIGN-2005.md`.

Tests: `mcp_tasks` unit tests pin the status mapping and the spec transition
table. `tests/mcp_tasks_http_tests.rs` pins the JSON-RPC contract and the auth
layers with no database. `tests/mcp_tasks_integration.rs` (Docker) proves:

- the spec transitions, with a terminal status that never moves;
- a single run for a retried create;
- the resume of an `input_required` task, and one delivery for a raced answer;
- no early terminal status across a workflow retry;
- cancellation, a TTL from retention, and a task read from a second app.
