## Plugin — SQLite read API parity, Vantage and declarative queries (issue #2468, epic #2466)

The SQLite `HarvestPlugin` now serves the read routes that slice 1 answered
with `501`. Each route keeps its Postgres path, query keys, status codes
and JSON shape. The plan is `docs/design/DESIGN-2468.md`.

- **Read routes.** `GET /workflows/count`, `/workflows/summaries`,
  `/workflows/registered/{name}/schema` and `/interface`,
  `/workflows/{id}/children` (also by-id), `/tree`, `/run-chain`,
  `/timeline`, `/logs`, `/awaitables`, `/stack` (also by-id) and
  `/history/export`. Each route reads the file through `SqliteReader`, never
  the driver. It then calls the same pure step as its Postgres route:
  `build_count_response`, `LineageWalk`, `assemble_run_chain`,
  `derive_timeline`, `classify_replayed_wait_set` or
  `export_history_decoded`. Summaries, logs and awaitables keep the
  Postgres admin gate.
- **Vantage.** `{api}/ui/workflows` lists the runs. `{api}/ui/workflows/{id}`
  shows one run, its payloads, its children and its history. The pages link
  only to SQLite pages and hold no form that writes.
- **Declarative queries.** `queries![..]` no longer fails the build.
  `SqliteRuntime::register_query` registers a handler per workflow. The
  drive, the query replay and the awaitables replay all pass it.
- **Durable workflow logs.** `HarvestPlugin::workflow_log_persistence` and
  `SqliteRuntime::set_workflow_log_policy` store the `ctx.log_*` lines in a
  new `harvest_workflow_logs` table. The dedup, the cap and the latched
  marker follow Postgres (#790). The lines share the cycle transaction, and
  a purge removes them. The table is new, so an older file gets it on open.
  It is not `harvest_events`, so the append-only guard is unchanged.
- **New reads.** `SqliteReader::count_executions`, `children`,
  `parents_with_children`, `timed_history`, `pending_work`, `workflow_logs`,
  `count_workflow_logs`, `workflow_log_truncated` and `replay_input`.
- **Refusals.** `decode_payloads_on_read` now names the epic. This backend
  stores no codec envelope, so the option has nothing to decode.
- **One contract, two backends.** `tests/support/wire_contract.rs` gains a
  shape check per route. The SQLite `read_parity` suite and the Postgres
  suite of each route call it.
