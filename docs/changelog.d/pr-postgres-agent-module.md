## Agent helpers for Postgres: the `postgres` module of `autumn-harvest-agent`

The `postgres` feature of `autumn-harvest-agent` now adds a `postgres`
module. It mirrors the `sqlite` module function by function, so an app whose
workers run on Postgres no longer builds a 35-field signal-with-start by hand.

- `PostgresAgents` is the target: `single(pool)` for one database, or
  `sharded(&pools, router)` for a sharded fleet, with `with_codecs` and
  `with_queue`.
- `register`, `start`, `start_heartbeat`, `decide`, `send`, `delete`,
  `current_run`, `state`, `LedgerSpend` and `pause_mesh` do what their
  `sqlite` namesakes do.
- A worker drives each run, so `sqlite::drive` has no copy. `wait_for` waits
  on the projected state of a durable agent. `run_state` and `wait_run`
  report a run as `Completed`, `Failed`, `WaitingSignal`, `Ended` or
  `Running`. `pending_approvals` reads the open approval waits from history.
- A steer or an interrupt that races a continue-as-new resolves the successor
  and goes again.
- The new example `postgres_agent` runs the approval and restart story of
  `sqlite_agent` on Postgres, then sends two messages to a durable agent.

Core gains two read-only queries for the mesh controls, each with a Ledger
profile: `execution::list_running_with_search_attrs` (`per_request`) and
`llm_ledger::spend_for_search_attrs` (`per_task`). The GIN index
`idx_harvest_we_search` and `harvest_llm_ledger_recorded_at` serve them.
No migration, no new `WorkflowEvent` variant, and no write to
`harvest_events`.

Test evidence: `autumn-harvest-agent/tests/postgres_agents.rs` runs seven
tests on a real Postgres worker. They cover an approval round trip,
message-id dedupe, delete, a mesh pause that spares another mesh, the mesh
spend sum, a heartbeat tick and a two-shard lookup. Five unit tests cover
the approval-wait scan.
