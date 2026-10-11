## Feature — durable outbound MCP/A2A task calls (issue #2006)

`autumn_harvest::remote_task::call` lets a workflow call a long-running
task on a remote MCP or A2A server. The workflow suspends until the task
ends. The wait holds no worker slot and no open connection, and a worker
restart loses nothing.

- The activity `harvest_remote_task_start` starts the remote task. It
  sends `ActivityContext::idempotency_key()`, the same on each retry, in the
  `Idempotency-Key` header and in `_meta` (MCP), or as the A2A `messageId`.
- The external activity `harvest_remote_task_await` journals the handle as
  an external task token. The workflow suspends.
- `RemoteTaskPoller` reads each pending handle from its
  `ActivityAwaitingExternal` event through the payload codecs. It settles
  the token when the remote task ends. `remote_task::resolve` is the push
  form, and the shipped `/activities/external/{token}/complete` route works
  too.
- A tool result with `isError: true` is a completed result. The workflow
  gets `Ok` with `is_error: true`, and nothing retries. A failed or
  cancelled remote task fails the token, not retryable.
- The first settlement wins and returns `true`. A later one returns
  `false`, as the durable promise resolvers of issue #1985 do.
- The plugin crate adds `remote_tasks::HttpRemoteTasks` behind the `mcp`
  feature. It is a JSON-RPC client for MCP Tasks (`2026-07-28`) and A2A.

**No migration. No new `WorkflowEvent` variant. No new route.** The plan is
in `DESIGN-2006.md`. The guide is `docs/remote-tasks.md`.

This change also drops the removed `refuse_erased_source` field from two
tests, which did not compile on trunk-dev after #2103.

Tests:

- `tests/integration/remote_task_tests.rs` (no database): the MCP and A2A
  wire format, the `isError` rule, the start activity key and errors, and
  replay of both steps.
- `tests/integration/remote_task_db_tests.rs`: a remote MCP task survives a
  worker restart and starts once, a failed task fails the run, the first
  settlement wins, and the poller pages and wraps. Each worker history
  replays clean.
- `autumn-harvest-plugin/tests/remote_tasks_http_tests.rs`: the JSON-RPC
  client against a stub server.
