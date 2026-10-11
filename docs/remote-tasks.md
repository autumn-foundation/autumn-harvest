# Remote MCP and A2A tasks (issue #2006)

A workflow can call a long-running task on a remote MCP or A2A server. The
workflow suspends until the remote task ends. The wait holds no worker slot
and no open connection. A worker restart loses nothing.

## How it works

`ctx.call_remote_task(&call)` runs two durable steps:

1. The activity `harvest_remote_task_start` starts the remote task. It
   records the task handle, or the result if the server answers at once.
2. The external activity `harvest_remote_task_await` journals the handle as
   an external task token. The workflow suspends.

A `RemoteTaskPoller` reads each pending handle from history and asks the
remote server for its state. When the task ends, the poller settles the
token, and the workflow resumes. Replay reads both steps from history. It
never calls the remote server.

| Remote state | Workflow sees |
|---|---|
| `completed` | `Ok(RemoteTaskOutcome)`. `is_error` is `true` for a tool result with `isError: true`. |
| `failed` (MCP), `failed` or `rejected` (A2A) | `Err(HarvestError::ActivityFailed)` |
| `cancelled` (MCP), `canceled` (A2A) | `Err(HarvestError::ActivityFailed)` |
| `working`, `input_required` and other states | The workflow still waits. |
| No end before `timeout` | `Err(HarvestError::Timeout)` |

A tool result with `isError: true` is a completed result. It does not
trigger a retry. The workflow reads `is_error` and decides.

## Use it

```rust,ignore
use std::time::Duration;
use autumn_harvest::remote_task::RemoteTaskCall;

#[workflow]
async fn monthly_report(ctx: &WorkflowContext, month: String) -> Result<String, String> {
    // A Harvest MCP server (issue #2005) takes the workflow input as `body`.
    let call = RemoteTaskCall::mcp(
        "reports",
        "start_export",
        serde_json::json!({ "body": { "month": month } }),
        Duration::from_secs(6 * 3600),
    );
    let outcome = ctx.call_remote_task(&call).await.map_err(|e| e.to_string())?;
    if outcome.is_error {
        return Err(outcome.result.to_string());
    }
    Ok(outcome.result.to_string())
}
```

The client sends `arguments` to the tool as they are. For A2A, one data part
holds `{"skill": <tool>, "arguments": <arguments>}`. That shape is a Harvest
convention, not an A2A rule.

Install a transport as worker state, register the start activity and run
one poller for each shard pool:

```rust,ignore
use autumn_harvest::remote_task::{self, RemoteTaskPoller, RemoteTasks};
use autumn_harvest_plugin::remote_tasks::{HttpRemoteTasks, RemoteServer};

let token = std::env::var("REPORTS_TOKEN")?;
let remote = RemoteTasks::new(
    HttpRemoteTasks::new()
        .server("reports", RemoteServer::mcp("https://reports.example.com/mcp")
            .bearer_token(&token)),
);
let built = HarvestBuilder::new()
    .workflows(workflows![monthly_report])
    .activities(remote_task::activities())
    .state(remote.clone())
    .build();

// Pass the worker codecs and result cap, or the poller cannot read an
// encoded handle.
let poller = RemoteTaskPoller::new(&remote)
    .with_codecs(codecs.clone())
    .with_max_result_bytes(max_activity_result_bytes);
tokio::spawn(async move { poller.run(&pool, cancel).await });
```

`HttpRemoteTasks` is in the plugin crate, behind the `mcp` feature. It
speaks JSON-RPC over HTTP: MCP Tasks `2026-07-28` and A2A `v0.3`. An app can
implement `RemoteTaskTransport` for any other client.

### Defaults

| Setting | Default |
|---|---|
| Start activity | 5 attempts, exponential from 1 s, 60 s each |
| Retryable start errors | network error, HTTP 429, HTTP 5xx |
| Poll interval | 5 s |
| Page size | 100 tokens |
| Remote reads at a time | 8, each with a limit of 30 s |
| Backoff of a failing read | doubles from the interval, up to 5 min |
| Result limit | 2 MiB (`DEFAULT_MAX_ACTIVITY_RESULT_BYTES`) |
| Failure message limit | 4 KiB |
| HTTP response limit | 4 MiB |

`RemoteTaskCall::timeout` starts when the token is recorded, after the
start. The engine rounds it up to whole seconds, with a minimum of 1 s.

## Crash safety

- The start activity sends `ActivityContext::idempotency_key()`. The key is
  the same on each retry. It goes in the `Idempotency-Key` header and in
  `_meta["io.autumn-harvest/idempotencyKey"]` (MCP), or as the A2A
  `messageId`. A server that honours the key starts one task. A Harvest
  server does so (issue #2005).
- The handle is in history, under the payload codec. The token row is in
  `harvest_external_tasks`. A restart re-runs the workflow from history, and
  the poller finds the token again.
- `complete_externally` and `fail_externally` lock the token row. Two
  pollers can read one task. Only the first settlement counts.
- The poller reads only tokens of live runs. It holds no database
  connection while it waits for a remote server.

## Push

The remote server, or a relay, can settle a token without the poller:

- `remote_task::resolve(conn, token, state, codecs)` from Rust.
- `POST /activities/external/{token}/complete` with
  `{"output": {"result": …, "is_error": false}}`. A missing `is_error` reads
  as `false`. `POST /activities/external/{token}/fail` takes
  `{"error": "…", "retryable": false}`.

A token is a capability: whoever holds it can settle the call. A relay must
authenticate each push before it settles a token.
`remote_task::pending_remote_tasks` lists each pending token with its
handle. Match on the server and the task id together, because two servers
can use the same task id.

## Durable promises

A durable promise (issue #1985) and a remote task call settle a durable
handle the same way:

| | Durable promise | Remote task call |
|---|---|---|
| Who settles | Any caller | The remote task, through the poller or a push |
| Storage | A signal named `harvest.promise:<key>` | An external task token |
| Settle | `resolve` / `reject` | `remote_task::resolve` |
| First settlement | `true`, later ones `false` | `true`, later ones `false` |
| Replay | Reads the signal | Reads the token outcome |
| Race a timer | Yes | No. The token has its own deadline. |

The two share a contract, not code. Use a promise for a wait that races a
timer or that any caller can settle. Use a remote task call for a task on
another server.

## Limits

- An external activity is a solo suspension. Do not call a remote task
  inside `join!`, a race or a batch.
- Harvest does not answer a remote `input_required`. The client declares no
  elicitation, so a Harvest server shows such a task as `working`. The wait
  ends at the timeout.
- Harvest does not send `tasks/cancel` when the run is cancelled or times
  out.
- A failed remote read leaves the token pending. The poller backs off, and
  the timeout still ends the wait.
- A Harvest server answers with a plain run handle when it cannot read the
  run just after the start (`DESIGN-2005.md`, R14). The call then completes
  with that handle as its result. A Harvest server also reports a refused
  start, such as a quota refusal, as a tool error. The call then completes
  with `is_error: true`.
- The SQLite backend has no external activities, so it has no remote task
  calls.
