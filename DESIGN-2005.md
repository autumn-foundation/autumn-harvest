# Design — Issue #2005: serve `#[workflow(mcp)]` tools as MCP Tasks

Issue #2005 asks Harvest to serve its MCP start tools as tasks of the
`io.modelcontextprotocol/tasks` extension (2026-07-28 revision). A client then
gets a task handle at once and polls for the result. It does not hit a 60 s
tool timeout.

**No migration. No new `WorkflowEvent` variant.** The change adds one HTTP
route behind the existing `mcp` cargo feature.

---

## 0. Planning record

### 0.1 Facts found before the plan

- The spec is
  `https://tasks.extensions.modelcontextprotocol.io/specification/draft/tasks`.
  It defines three methods: `tasks/get`, `tasks/update` and `tasks/cancel`.
  It has no `tasks/list` and no `tasks/result`.
- A client declares the extension per request, in
  `params._meta["io.modelcontextprotocol/clientCapabilities"].extensions`.
  A server must not return a task to a client that did not declare it.
  A `tasks/*` call from such a client gets error `-32021`.
- A `tools/call` result with `resultType: "task"` is a `CreateTaskResult`.
  The server sends it only after a `tasks/get` for that id resolves.
- A tool result with `isError: true` is `completed`, not `failed`.
  `failed` is for a JSON-RPC error only.
- `input_required` carries `inputRequests`, a map of keys to server-to-client
  requests. A key is unique for the life of the task. The client answers in
  `tasks/update` with `inputResponses` under the same keys.
- autumn-web 0.8 serves `/mcp`. Its dispatcher knows `initialize`, `ping`,
  `tools/list` and `tools/call` only. It has no extension hook. So Harvest
  must serve the task methods on its own JSON-RPC route.
- The execution id is a UUIDv4 with 16 shard bits. About 106 random bits are
  left, so a task id cannot be guessed.
- `start_workflow` dedups on the `Idempotency-Key` header (issue #808). A
  retry with the same key returns the first `execution_id`.
- The engine stores no "waiting on a signal" marker. `build_awaitables_report`
  (issue #615) replays history read-only and returns each parked
  `wait_for_signal` by name.
- A signal delivery takes an idempotency key. A second delivery with the same
  key on the same execution is a no-op.
- `cancel_workflow` routes to the live attempt and ends in `CANCELLED`.
- Retention is `RetentionConfig::effective_max_age(workflow)`, counted from
  completion. `None` means no expiry.

### 0.2 Brainstorm — how can Harvest serve tasks?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Patch autumn-web to dispatch `tasks/*`. | Rejected. autumn-web is an external crate. This repository cannot change it. |
| B2 | A Harvest JSON-RPC route that serves `initialize`, `server/discover`, `tools/list`, `tools/call` and `tasks/*` for the MCP workflows. | **Adopted.** One route, one handler, the same auth layers as the tool routes. |
| B3 | Proxy `/mcp` and add the task methods in front of it. | Rejected. Two dispatchers for one envelope. The `secure_mcp` layer and the CORS rules of `/mcp` would need a copy. |
| B4 | Task id = a new random id in a new table. | Rejected. A migration for no gain. The execution id is durable and unguessable already. |
| B5 | Task id = execution id. Task state derives from the execution row on each read. | **Adopted.** No new state, so nothing can drift. A restart loses nothing. |
| B6 | `input_required` from a new "waiting" column written by the engine. | Rejected. That is a core change and a migration. |
| B7 | `input_required` from the replayed awaitables report. | **Adopted.** It is read-only and already shipped. |
| B8 | Crash-safe create: derive the start key from the JSON-RPC `id`. | Rejected. Clients reuse small ids such as `1` across sessions. Two different calls would merge. |
| B9 | Crash-safe create: the `Idempotency-Key` header, else a `_meta` key. | **Adopted.** The header is the #808 contract. autumn-web forwards it too. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A task id from any workflow reads a run that is not an MCP workflow. | `tasks/*` loads the row and checks that its workflow is an exposed MCP workflow. Any other id gets the same `-32602` as an unknown id. |
| R2 | A retried `tools/call` starts a second run. | The start runs through `start_workflow` with the request key. Test: `a_retried_task_create_starts_one_execution`. |
| R3 | A workflow error shows as `failed`, so a client treats it as a protocol fault and retries. | A `FAILED` or `TIMED_OUT` run maps to `completed` with `isError: true`. A unit test pins the mapping. |
| R4 | A retried `tasks/update` delivers the signal twice. | The input key is the signal idempotency key. The second delivery is a no-op. |
| R5 | A stale key from an earlier wait delivers a signal to a later wait. | Each key holds the run id and the history position at the park. The handler delivers only for a key that is outstanding now. |
| R6 | A later wait reuses the key of an earlier one, for example after a `wait_for_signal_timeout` that timed out. | The position is the id of the last history event. A later wait comes after a new event, such as `SignalReceived` or `TimerFired`, so its key is new. |
| R6a | An event lands during the replay, so a key names a wait that just ended. | The handler reads the position before and after the replay. If it moved, the read reports no wait this time. |
| R6b | A user declines, or the signal refuses the payload, and the client gets an ack. The task then waits forever. | Both get `-32602`, so the client knows that the run did not take the answer. |
| R6c | Another caller with mutate rights sends a signal with the next predictable key, so the answer of the real client is a no-op. | Accepted. That caller can already cancel or terminate the run. |
| R7 | A continue-as-new successor reuses an ordinal. | The key holds the live run id, so a successor has new keys. |
| R8 | A client that did not declare the extension gets a task it cannot read. | No declaration means a plain `CallToolResult` with the handle, as `start_{wf}` returns today. |
| R9 | A cross-site form posts to the route with the session cookie. | The route takes `application/json` only. A browser sends that cross-site only after a CORS preflight. |
| R9a | A hostile page rebinds its DNS name to the server, so `Origin` and `Host` agree. | A same-origin `Origin` passes only on a trusted host, as on autumn-web's `/mcp`. Any other origin must be in the CORS allowlist, or it gets `403`. |
| R10 | The route skips the tool-route auth layers. | It reuses the layer stack of a mutating tool route. A test proves a read-only principal gets `403`. |
| R11 | A task outlives retention and `tasks/get` fails with a 500. | A missing row is `-32602` "Task not found", as the spec allows. `ttlMs` reports retention once the run ends. |
| R12 | `tasks/get` replays history on each poll and loads the database. | The route caches the waits of each run at each history position, so a poll with no new event does not replay. `pollIntervalMs` asks for 5 s. |
| R13 | A cross-type continue-as-new moves the live run to a workflow outside the catalog. | `tasks/update` and `tasks/cancel` refuse such a run. |
| R14 | The read after a start fails, so the client sees an error and retries. | The create retries the read. If it still fails, the client gets the plain run handle, not a task, because the spec sends a task only once `tasks/get` resolves. The run is never hidden. |
| R16 | A gateway authorizes on `Mcp-Method: ping`, and the body runs `tools/call`. | The route compares `MCP-Protocol-Version`, `Mcp-Method` and `Mcp-Name` with the body and refuses a mismatch with `400` and `-32020`. |
| R15 | A cross-type continue-as-new leaves the start row unguarded, so it expires before the TTL says. | The TTL follows the start row: its own completion and retention when no live row with the same name and business id guards it. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | The spec has three task methods and five states. Harvest has durable runs, start keys, a signal key and an awaitables replay. autumn-web has no hook. |
| Red | Agents want one call that survives a long run. A second endpoint feels heavier than one `/mcp`. |
| Black | A second endpoint is a second URL to secure. The spec is still a draft, so names can change. `tasks/get` replays history. Update waits and condition parks are not visible. |
| Yellow | No migration and no core change. The task is the run, so a crash or a restart loses nothing. The same auth layers apply. |
| Green | Later: `notifications/tasks` through the shard LISTEN channel. A per-signal schema for `requestedSchema`. A hook in autumn-web to serve one `/mcp`. |
| Blue | TDD order: pure mapping tests, then no-database route tests, then Docker tests for the three acceptance criteria. Then docs, the changelog fragment and a review. |

### 0.5 Decisions

1. `HarvestPlugin::mcp_tasks()` serves the route at `{tools prefix}/tasks`,
   default `/api/harvest/mcp/tasks`. `mcp_tasks_at(path)` sets the path.
2. The route serves one `start_{wf}` tool for each MCP workflow. The
   arguments are the same as on `/mcp`. A DAG is not served: its trigger
   takes no start key, so a retried create could start a second run.
3. Run state to task status:

   | Run state | Task status | Payload |
   |-----------|-------------|---------|
   | `COMPLETED` | `completed` | `CallToolResult`, `isError: false`, the output |
   | `FAILED` (no retry left), `TIMED_OUT` | `completed` | `CallToolResult`, `isError: true`, the error |
   | `CANCELLED`, `TERMINATED` | `cancelled` | `statusMessage`: the reason |
   | Any other state, parked on a signal | `input_required` | one `elicitation/create` per wait |
   | Any other state | `working` | `statusMessage`: `current_details` |

   `CONTINUED_AS_NEW` and a retried `FAILED` follow the chain first.
   Harvest never reports `failed`.
4. An input key is `{run id}:signal:{name}:{position}`, where `position` is
   the id of the last history event at the park. The elicitation asks for
   one string field, `payload`, which holds the payload as JSON text. An
   `accept` answer delivers the signal, with the key as the idempotency key.
   Another action is an error. A client without the `elicitation`
   capability sees `working` and the signal names.
5. `tasks/cancel` cancels the live attempt. It acknowledges a run that is
   already terminal.
6. `ttlMs` is `null` while the run is live. After it ends, `ttlMs` runs from
   `createdAt` to the retention cut-off of the row that holds the task id,
   or stays `null` with no retention.

### 0.6 Out of scope

- `notifications/tasks` and `subscriptions/listen`. Polling is enough, and
  the spec makes push optional.
- An update wait as `input_required`. The engine does not run an admitted
  update yet (issue #2035), and no history marks a wait for one.
- A `tasks/list`. The spec removed it.
- A reset fork. An operator reset of an ended run seals it as `TERMINATED`,
  so the task reads `cancelled` and does not follow the fork.
- A `#[dag(mcp)]` task. `trigger_dag_run_inner` has no start key, so a
  task-create for a DAG is not crash-safe.
