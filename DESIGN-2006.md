# Design — Issue #2006: durable outbound MCP/A2A task calls

Issue #2006 asks for a workflow that calls a remote, long-running MCP or A2A
task. The workflow must not poll from an activity. It must not hold a
connection open. It journals the remote task handle as an external task
token. Then it suspends until a poll or a push resolves the token.

**No migration. No new `WorkflowEvent` variant. No new route.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- `execute_activity_external` mints an `ExternalActivityToken` in the
  workflow. The worker appends `ActivityAwaitingExternal` with the `input`
  and records a `PENDING` row in `harvest_external_tasks`. Both writes are in
  one transaction.
- The workflow holds no worker slot while it waits. A restart re-runs the
  workflow from history. The matcher returns the same token.
- `complete_externally` and `fail_externally` lock the row. They return
  `true` on the first settlement and `false` after it. They append the
  terminal event and wake the workflow in one transaction.
- `fail_externally` stores `retryable`, but replay ignores it. The workflow
  gets `ActivityFailed` with attempt 1. Nothing retries.
- `enforce_external_task_timeouts` moves an overdue row to `TIMED_OUT`.
- An external activity is a solo suspension. It cannot share a batch.
- The core crate has no HTTP client. The plugin crate has `reqwest`.
- `ActivityContext::idempotency_key()` is the same on each retry of one
  activity.
- MCP Tasks (extension `io.modelcontextprotocol/tasks`, revision
  `2026-07-28`) is in `DESIGN-2005.md`. A client declares the extension in
  `params._meta`. A `tools/call` then returns `resultType: "task"` with a
  `taskId`. `tasks/get` returns the status. A tool result with
  `isError: true` is `completed`. `failed` is for a JSON-RPC error only.
- A2A `message/send` returns a `Task` with `id` and `status.state`.
  `tasks/get` reads it again. The terminal states are `completed`,
  `failed`, `canceled` and `rejected`.
- PR #2060 (issue #1985) builds the durable promise on signals. Its B4
  rejects external tokens for the promise: a token wait is solo, cannot race
  a timer and needs a deadline. Its §1.2 names #2006 as a consumer.

### 0.2 Brainstorm — how can a workflow call a remote task durably?

| # | Idea | Verdict |
|---|------|---------|
| B1 | An activity calls `tools/call`, then polls `tasks/get` in a loop. | Rejected. That is the problem in the issue. It holds a slot for the whole task. |
| B2 | The workflow polls with a timer and a `tasks/get` activity per tick. | Rejected. Each tick adds events. A long task fills the history. |
| B3 | One activity starts the remote task. A second step journals the handle as an external token. An engine-side poller resolves the token. | **Adopted.** It uses the shipped token path. The start is a normal activity with retries. The wait holds no slot. |
| B4 | Mint the token first, then start the remote task from the worker. | Rejected. The token exists only after the park commits. The worker would need a new hook and a place to store the remote id. |
| B5 | Store the remote handle in a new column of `harvest_external_tasks`. | Rejected. A migration. The handle is already in the `ActivityAwaitingExternal` input, under the payload codec. |
| B6 | Read the handle back from the `ActivityAwaitingExternal` event. Decode it with the payload codecs. | **Adopted.** No migration. A codec deployment keeps the handle encrypted at rest. |
| B7 | Put the HTTP client in the core crate. | Rejected. The core crate has no HTTP dependency. |
| B8 | A `RemoteTaskTransport` trait in core. A JSON-RPC HTTP transport in the plugin crate. | **Adopted.** A test can use an in-memory transport. An app can bring its own client. |
| B9 | Push: a relay that knows the token calls the shipped `/activities/external/{token}/complete` route. | **Adopted.** No new route. `remote_task::resolve` is the library form. `pending_remote_tasks` maps a handle to its token. |
| B10 | Map `isError: true` to `fail_externally`. | Rejected. The issue says it is a completed result. It completes the token with `is_error: true`. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A retried start creates a second remote task. | The start sends `ActivityContext::idempotency_key()`, the same on each retry. It goes in the `Idempotency-Key` header and in `_meta["io.autumn-harvest/idempotencyKey"]`, as #2005 reads it. A test pins the key. |
| R2 | A worker restart loses the remote handle. | The handle is in history. The token row is durable. A DB test stops the worker and the poller, then a new pair completes the run. |
| R3 | A restart starts the remote task again. | The start result is in history. Replay returns it. The DB test counts one start. |
| R4 | `isError: true` triggers a retry. | The token completes. The workflow gets `Ok` with `is_error: true`. Unit and DB tests pin it. |
| R5 | Two pollers settle one token twice. | `complete_externally` locks the row. The second settle returns `false`. A DB test proves it. |
| R6 | A transport error fails the token. | A `get` error leaves the token `PENDING`. The `schedule_to_close` deadline still ends it. |
| R7 | The poller reads every pending row on each tick. | It reads one page by token order, with a cursor. It reads only rows named `harvest_remote_task_await`. |
| R8 | The poller logs a task id or a result. | It logs counts and the token only. The task id can be a bearer handle. |
| R9 | A codec deployment cannot read the handle. | The poller decodes with the worker codecs. It skips a row that it cannot decode, and logs a warning. |
| R10 | The remote result is too large for the event. | The external output follows the same cap and codec path as any `ActivityCompletedExternally`. |
| R11 | A remote `input_required` blocks the run forever. | The token stays `PENDING`. The deadline ends it. The docs say that Harvest does not answer remote input. |
| R12 | A JSON-RPC error on `tools/call` is retried forever. | A protocol error is non-retryable. A network error is retryable. The start activity retry policy bounds both. |
| R13 | A replay under a new codec or binary sees a different handle. | Replay reads the recorded start output and the recorded external outcome. It never calls the transport. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | Tokens, the timeout scan and idempotent settlement are shipped. MCP Tasks and A2A both have a task id and a `tasks/get`. The core crate has no HTTP client. |
| Red | One call, `remote_task::call`, feels right. Two activity names in history feel heavy, but they are honest. |
| Black | The spec is a draft. A remote server can ignore the idempotency key. The poller is a new loop to run. A push needs a relay. A solo suspension cannot race a timer. |
| Yellow | No migration, no new event, no new route. The wait holds no slot. Replay never calls out. The plugin gets a real MCP and A2A client. |
| Green | Later: `tasks/cancel` on a Harvest cancel. A shard-aware poller in the worker loop. Remote `input_required` answered through a signal. A promise resolved from a remote task. |
| Blue | TDD: pure mapping tests, then context replay tests, then a DB test for the restart, then the plugin transport tests. Then docs, the changelog fragment and a review. |

### 0.5 Decisions

1. Module `autumn_harvest::remote_task`. `RemoteTaskCall` names the server,
   the protocol, the tool and the arguments, and sets `timeout`.
2. `remote_task::call(ctx, &call)` runs two durable steps:
   1. Activity `harvest_remote_task_start`. It returns a `Task` handle or a
      `Completed` result.
   2. On a handle, external activity `harvest_remote_task_await`. Its input
      is the `RemoteTaskHandle`.
3. The result is `RemoteTaskOutcome { result, is_error }`.
4. Remote state to token action:

   | Remote state | Token action | Workflow sees |
   |---|---|---|
   | `completed` | `complete_externally` | `Ok`, `is_error` from the result |
   | `failed` (MCP), `failed` or `rejected` (A2A) | `fail_externally`, not retryable | `ActivityFailed` |
   | `cancelled` (MCP), `canceled` (A2A) | `fail_externally`, not retryable | `ActivityFailed` |
   | `working`, `input_required`, other | none | still waits |

5. `RemoteTaskPoller::poll_once(conn)` settles one page.
   `RemoteTaskPoller::run(pool, cancel)` loops on an interval. Run one per
   shard pool.
6. `remote_task::resolve(conn, token, state, codecs)` is the push form. It
   returns `true` on the first settlement, as the promise resolvers in
   #2060 do.
7. The plugin crate ships `HttpRemoteTasks`, a JSON-RPC transport for MCP
   and A2A.

### 0.6 Alignment with the durable promise (#1985, PR #2060)

| | Durable promise (#2060) | Remote task call (#2006) |
|---|---|---|
| Direction | Inbound: any caller settles a handle. | Outbound: a remote task settles a handle. |
| Storage | A signal named `harvest.promise:<key>`. | An external task token. |
| Settle | `resolve` / `reject`. `true` first, then `false`. | `complete_externally` / `fail_externally`, through `remote_task::resolve`. `true` first, then `false`. |
| Replay | Reads the signal from history. | Reads the token outcome from history. |
| Race a timer | Yes. | No. The token has a deadline. |

The two share the settlement rule: one settlement wins, and a later one
returns `false`. The tokens suit #2006: the remote task has a deadline,
the wait is solo, and the poller needs a durable row to scan. A promise
suits a wait that races a timer. `docs/remote-tasks.md` states this choice.

### 0.7 Out of scope

- `tasks/cancel` to the remote server when the run is cancelled or times
  out.
- An answer to remote `input_required`.
- Push notifications from MCP `notifications/tasks/status`. A relay can
  call `remote_task::resolve`.
- A remote task inside `join!`, a race or a mixed batch. The external
  activity is a solo suspension.
- The SQLite backend. It has no external activities.

---

## 1. Tests (red first)

| Test | AC | Kind |
|------|----|------|
| `remote_task` unit tests: MCP and A2A parse, state map, `isError` | 1 | No DB |
| `tests/integration/remote_task_tests.rs`: context replay, a start that completes at once, `isError` replay, a failed replay | 1 | No DB |
| `tests/integration/remote_task_db_tests.rs`: restart survival, one start, `isError` completes, failure, double settle | 1, 2 | DB |
| Plugin `remote_tasks` tests: JSON-RPC client against a stub server | 1 | No DB |
