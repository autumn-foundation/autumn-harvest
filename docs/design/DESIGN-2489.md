# DESIGN-2489: SQLite HarvestPlugin wire-contract parity

Issue #2489. Parent epic #2466. Follows #2467 (PR #2474).

## Problem

Slice 1 ports the core management routes to SQLite. Reviews of PR #2474
found, one at a time, SQLite routes that differ from the Postgres wire
contract. A review should not be the gate. A shared test should fail first.

Slice 1 also answers `GET /workflows/{id}/result` with `501`, and it refuses
the paging keys of `GET /workflows/{id}/history` with `400`.

## Facts

- The Postgres handlers live in `autumn-harvest-plugin/src/api.rs`. The
  SQLite handlers live in `autumn-harvest-plugin/src/sqlite/api.rs`.
- The Postgres tests of these routes need Docker. The SQLite suite
  (`tests/sqlite_plugin.rs`) needs neither Docker nor Postgres. One test
  binary cannot hold both, because the `sqlite` feature flips `autumn-web`
  for the whole build.
- `/result` follows the workflow retry chain (issue #523) and the
  continue-as-new chain (issue #527). Postgres reads the successor from
  history. SQLite stores the same events: `WorkflowContinuedAsNew` and
  `WorkflowRetryScheduled`.
- A Postgres history event has a global `id` (the cursor) and a 0-based
  `event_id`. A SQLite event has a key `(exec_id, seq)`. `seq` is 0-based.
- Before this change, a SQLite event row has no append time. A Postgres
  row has one.
- Postgres routes a signal, cancel, terminate, query or update that targets
  a `FAILED` run with a live retry to that retry (issue #843). Before this
  change, SQLite answers `409`.

Two audit passes compared every ported route. The table lists each real
break and its decision.

| Route | Break | Decision |
|---|---|---|
| `/result` | `501` | Port: `204` with `Retry-After: 1`, the compact form, both chains, `wait` |
| `/by-id/{name}/{id}/result` | `501` | Port, with the execution id header |
| `/history` | no paging; no `id`, no `timestamp`; `last_event_id` is `null` when empty | Port the cursor. `id = seq + 1`. Store an append time |
| `/workflows/{id}` | the whole history; no `parent_id`, `external_handoffs`, `legal_hold` | Cap at 100 events. Add the fields |
| `/by-id/...` | no `X-Harvest-Execution-Id`; newest run, not active first; `501` for an empty id | Port all three |
| `/workflows` | unknown `state` is an empty `200`; blank name filters; page 100/1000; no `status` | Port the check, the trim, 50/200, the envelope |
| `/workflows/registered` | `{name}` only | Port `RegisteredWorkflowRecord` |
| `/health/live`, `/health/ready` | other bodies; ready never checks the file | Port the bodies; ready reads the file |
| signal, by-id signal | `200`; no header; `409` for a terminal run | `202`, the header, `400` |
| signal-with-start | always `200`; plain `409`; `403` for a non-admin | `201` when fresh; the Postgres `409` body; `401` |
| start | `""` id starts a run; plain `409`; `403` | `400`; the `409` body; `401` |
| cancel | a repeat answers `newly_cancelled: true`; a cancelled run is `409`; blank reason echoed | `false` and the stored reason; idempotent `202`; the default reason |
| terminate | `200`; a terminal run is `409` | `202`; idempotent `202` with the state reason |
| query | handler error `422`; timeout `504`; terminal `409` | `400`, `408`, `410` |
| update | no `update_id`; failure `422`; schema `400` without `violations` | add `update_id`; `409`; `violations` |
| all writes on a failed run | no retry routing | route to the live retry; `routed_execution_id` on a signal |

The audit also named the payload caps `max_signal_payload_bytes` and
`max_workflow_input_bytes`. `HarvestPlugin` has no setter for either, so a
plugin always runs the defaults. Both backends then apply the same caps.

These stay a documented subset:

- A cancel is cooperative. It answers `state: "RUNNING"` and
  `deferred: true`.
- A route refuses a query key that it does not read. Postgres ignores it.
  `/history` ignores it, as on Postgres. `/result` refuses it on both.
- An event written before this change has `timestamp: null`.
- A body that is empty is the default. Postgres needs a content type.
- A signal key dedupes across the whole run chain. Postgres dedupes per run.
- A start refuses an idempotency key. Postgres dedupes on it (issue #808).
- A list row carries the fields that this backend stores. `workflow_id` and
  `workflow_id_prefix` are SQLite filters.
- `/health` adds `backend` and `path`.
- A cancel or a terminate body has no `failed_task_count`. This backend
  deletes the pending tasks. It does not mark them failed.

## Brainstorm

1. One shared test binary that boots both backends. Not possible: the
   feature flip is global.
2. A shared module of checks in `tests/support/wire_contract.rs`. Each
   harness seeds its own runs and calls the same check on the answer.
3. A recorded golden answer per route, compared byte for byte. Ids and
   times differ per run, so it needs masks, and it hides the reason.
4. Generate both routers from one route table. Too large for this issue.
5. An OpenAPI conformance pass (`openapi_response_conformance.rs`) on
   SQLite. It checks shapes, not status semantics or chains.
6. Walk the chain in the plugin, or add a reader method. The issue asks for
   a reader method.
7. Long-poll `wait` by a reader poll on a short interval. SQLite has no
   `LISTEN`.
8. Store the event time as a new column, or derive it from the event. Most
   events carry no time.

Decision: 2, 6 (`SqliteReader::successor`), 7 and a new column for 8.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| Answer `200` with `state: "running"` for a running run. A client then treats it as done. | `result_is_pending` asserts `204`, `Retry-After: 1` and no body. |
| Stop at the `CONTINUED_AS_NEW` run. | `result_is_terminal` on the first id of a two-run chain. |
| Stop at a `FAILED` run with a live retry. | A test reads `/result` of the failed run and gets `204`. |
| Loop on a cycle in a corrupt chain. | The walk stops after 128 hops, as on Postgres. |
| Use `id = seq`. Then `after=0` skips the first event. | `id = seq + 1`. A test reads `after=0`. |
| Count filtered events in `total_events`. | `history_filter_holds_only` checks the total. |
| Bind one parameter per `event_type`. A long filter then hits the SQLite limit. | One JSON array parameter through `json_each`. |
| Read `recorded_at` on an old file. The reader cannot add the column. | The query asks the file, and a missing column reads `NULL`. |
| Change a stored event. | Only the `INSERT` writes `recorded_at`. The append-only guard still refuses an `UPDATE`. Replay reads `event_json` only. |
| Route a signal to a sealed `CONTINUED_AS_NEW` run's successor. Postgres does not. | Only a `FAILED` run routes. |
| Report `newly_cancelled: true` on a repeat cancel. | The driver reads the pending request first. |
| Fix one backend and leave the other. | Both suites call `wire_contract.rs`. |

## Six hats

- **White.** The facts and the audit table above.
- **Red.** The cooperative cancel is the largest gap in meaning. It stays a
  documented subset. A hidden difference is worse than a stated one.
- **Black.** A wider fix touches more lines than slice 1 did. Each change
  has its own test, so a review can check one row at a time. A new
  `harvest_events` column touches the append-only table. It is additive,
  written once by the `INSERT`, and replay never reads it.
- **Yellow.** A client written against Postgres now works on SQLite for the
  core routes. The contract module is the gate for later slices.
- **Green.** The retry routing is one plugin helper, `live_attempt`. Each
  write route calls it. The by-id header is one helper too.
- **Blue.** Red tests first: the storage reads, then the HTTP suite, then
  the contract module in the Postgres tests. Then the smallest code. Then
  the docs and the gates.

## Acceptance

- `cargo test -p autumn-harvest-sqlite --test integration history_page`
- `cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin`
- The Postgres suites `workflow_result_integration`,
  `workflow_history_pagination_integration` and `by_id_integration` call the
  same checks.
- `docs/sqlite-plugin.md` lists `/result` and the history keys, and the
  subset above.
