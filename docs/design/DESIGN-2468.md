# DESIGN-2468: SQLite HarvestPlugin read API parity and Vantage

Issue #2468. Parent epic #2466. Follows #2467 and #2489.

## Problem

Slice 1 answers these read routes with `501` on SQLite: count, summaries,
the registered schema and interface, children, tree, run chain, timeline,
logs, awaitables, stack and the history export. Vantage is not mounted.
The build refuses declarative `queries![..]`, because the SQLite replay
registers no declarative query handler.

## Facts

- The Postgres handlers are in `autumn-harvest-plugin/src/api.rs`. Most of
  them split into a database read and a pure step. The pure steps are
  backend-neutral:
  - `WorkflowCountParams` and `build_count_response` (count);
  - `LineageWalk` (tree);
  - `assemble_run_chain` and `reconstruct_legacy_chain_order` (run chain);
  - `derive_timeline` (timeline);
  - `drive_query_replay_async`, then `project_awaitables` through the new
    shared `classify_replayed_wait_set` (awaitables);
  - `export_history_decoded` (history export).
- SQLite stores the parent of a child run (`parent_exec_id`) and its close
  policy. A `NULL` policy is an awaited child.
- SQLite stores no back-link on a continue-as-new successor. A run names
  its successor in its last event. A successor keeps the workflow id.
- SQLite stores an append time on each event since #2489. An older event
  has none.
- SQLite has tables for tasks, timers and signals. It has no external
  handoff, no rate-limit bucket and no worker id.
- SQLite has no retention summary. A purge removes the whole run (#2031).
- `ctx.log_*` pushes `RecordLog` only when a `WorkflowLogPolicy` is set.
  The SQLite runtime drops it. So a logs route would always be empty.
- The SQLite query replay and the drive pass no declarative query handler.
- Vantage on Postgres is `{api}/ui`. Its pages read Postgres models and
  link to Postgres-only pages, such as DAGs, workers and schedules.

## Route table

| Route | Decision |
|---|---|
| `GET /workflows/count` | Port. A `GROUP BY` on SQLite feeds `build_count_response`. One file is one shard. |
| `GET /workflows/summaries` | Port. Parse the Postgres filters. The page is always empty: this backend never demotes a run. Admin only, as on Postgres. |
| `GET /workflows/registered/{name}/schema` | Port. |
| `GET /workflows/registered/{name}/interface` | Port, with the signal, query and update handlers. |
| `GET /workflows/{id}/children`, by-id | Port. The Postgres filters, cursor and depth walk. |
| `GET /workflows/{id}/tree` | Port. `LineageWalk` on SQLite rows. |
| `GET /workflows/{id}/run-chain` | Port. Gather the runs of the key, link them by successor, assemble. |
| `GET /workflows/{id}/timeline` | Port. `derive_timeline` on timestamped rows. |
| `GET /workflows/{id}/logs` | Port the read and the sink. Admin only. |
| `GET /workflows/{id}/awaitables` | Port. Replay with the declarative handlers. Admin only. |
| `GET /workflows/{id}/stack`, by-id | Port the SQLite tables. External handoffs stay empty. |
| `GET /workflows/{id}/history/export` | Port. `export_history_decoded`. |
| `{api}/ui/workflows`, `{api}/ui/workflows/{id}` | A SQLite Vantage: the run list and the run page. |
| `queries![..]` | Register on the runtime, thread into the drive and the query replay, lift the refusal. |

These stay `501`: the bulk export `/admin/history/exports` and the other
admin routes. They read Postgres-only tables, or later slices port them.

## Brainstorm

1. Port each Postgres handler line by line. It copies the Postgres SQL
   shape into SQLite, and the two drift.
2. Split each route into a SQLite read and the shared pure step. The shape
   then comes from one function for both backends.
3. Wrap SQLite rows in the Postgres models (`WorkflowExecution`,
   `HarvestEvent`) and call the Postgres handlers. The models carry many
   Postgres-only fields, so each default is a silent lie.
4. Answer `logs` with an empty page. It matches Postgres with no policy.
   An app that sets a policy then sees no lines. That is a silent no-op.
5. Port the log sink: one table, the Postgres cap and the marker.
6. Reuse the Postgres Vantage renderers. They need the Postgres models and
   show forms and links that SQLite cannot serve.
7. Write a small SQLite Vantage that reuses the Vantage style and helpers.
8. Share the shape checks in `tests/support/wire_contract.rs`, as #2489
   does.

Decision: 2, 5, 7 and 8.

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| Answer an empty `200` for a route that has data. | Each test seeds data and checks the rows. |
| Count runs of another state, or skip the long tail. | The count test checks each group and `total`. |
| Treat a detached child as awaited in the stack. | The stack lists children that are not terminal, as Postgres does. The children test checks `status`. |
| Loop forever on a corrupt chain or tree. | The run chain uses the Postgres hop bound. `LineageWalk` bounds the tree. |
| Put a predecessor after its successor. | Links come from the last event, not the start time. |
| Show an event with no time as now. | A timeline row with no time takes the time of the event before it, or the run start. |
| Store a log line twice on a re-drive. | `INSERT OR IGNORE` on `(exec_id, seq)`. |
| Count the re-driven batch against the cap. | The count skips the batch seqs, as on Postgres. |
| Reopen the cap after a raise. | The marker latches the cap, as on Postgres. |
| Commit log lines of a cycle that rolls back. | The lines share the cycle transaction. |
| Serve the awaitables of a drifted run. | A divergence degrades to `history_only`, as on Postgres. |
| Answer a declarative query with the drive's handler of another workflow. | Handlers are kept per workflow name. |
| Show logs or awaitables to an anonymous caller. | The admin gate wraps these routes, as on Postgres. |
| Render a Vantage link to a Postgres-only page. | The SQLite layout links only to SQLite pages. A test checks it. |
| Render a payload as HTML. | `maud` escapes text. A test seeds `<script>`. |
| Fix one backend and leave the other. | Both suites call the shared checks. |

## Six hats

- **White.** The facts and the route table above.
- **Red.** A logs route that always reads empty feels broken. Porting the
  sink is more work, but the route then means what it says.
- **Black.** The change is wide. It touches the SQLite schema, the drive and
  many routes. Each route has its own test. The schema change is a new
  table, so an old file gets it on open. The drive change only adds rows to
  that table, so replay is unchanged.
- **Yellow.** A client written against Postgres works on SQLite for the
  read routes. An operator gets a dashboard on an embedded app.
- **Green.** The pure steps already exist, so each route is a read plus one
  call. Make the private Postgres helpers `pub(crate)` instead of copying
  them.
- **Blue.** Red tests first: the SQLite reads, then the HTTP suite, then the
  shared checks in the Postgres suites. Then the smallest code. Then the
  docs and the gates.

## Acceptance

- `cargo test -p autumn-harvest-sqlite --test integration`
- `cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin`
- The Postgres suites of the ported routes call the same checks of
  `tests/support/wire_contract.rs`.
- Vantage renders the run list and one run page on SQLite.
- A declarative query answers on SQLite as on Postgres.
