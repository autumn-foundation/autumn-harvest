## Feature — durable outbound MCP/A2A task calls (issue #2006)

`WorkflowContext::call_remote_task` lets a workflow call a long-running
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
- The poller reads only tokens of live runs. It frees its database
  connection while it sends 8 remote reads at a time. A failing read backs
  off up to 5 minutes. A result over the cap, 2 MiB by default, fails the
  token. A failure message keeps at most 4 KiB.
- A tool result with `isError: true` is a completed result. The workflow
  gets `Ok` with `is_error: true`, and nothing retries. A failed or
  cancelled remote task fails the token, not retryable.
- The first settlement wins and returns `true`. A later one returns
  `false`, as the durable promise resolvers of issue #1985 do.
- The plugin crate adds `remote_tasks::HttpRemoteTasks` behind the `mcp`
  feature. It is a JSON-RPC client for MCP Tasks (`2026-07-28`) and A2A
  `v0.3`. It reads at most 4 MiB of a response, refuses a response from
  another origin, and keeps URLs out of its error messages.

`harvest-verify` classifies `call_remote_task` as a sink.

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
  settlement wins, the poller pages and wraps, skips an ended run, backs off
  a failing read, and caps the result. Each worker history replays clean.
- `autumn-harvest-plugin/tests/remote_tasks_http_tests.rs`: the JSON-RPC
  client against a stub server, with the size, origin and error rules.
- `autumn-harvest-plugin/tests/remote_tasks_integration.rs`: a workflow
  calls a real Harvest MCP Tasks route (#2005) over TCP and resumes. A
  remote workflow error completes the call with `is_error`. A retried start
  gets the same task.
