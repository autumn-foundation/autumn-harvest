## Phase X.Y — Diesel tables fit the 64-column tier again (issue #2492)

`harvest_workflow_executions` had 65 columns in its `table!`, so the workspace
enabled diesel's `128-column-tables` feature. Cargo unifies features, so every
embedder built diesel at that tier too. The workspace now enables
`64-column-tables`.

- Four shard-rebalance columns leave the `table!`: `migrated_from_shards`,
  `migrated_run_terminal_state`, `staging_vacated_state` and
  `staging_vacated_by`. The table keeps 61 columns. The physical columns stay,
  so there is no migration. Raw SQL already wrote all four.
- `schema::harvest_workflow_executions_unmapped` holds a typed SQL fragment for
  each of them. `WorkflowExecution` reads them through
  `#[diesel(embed)] rebalance: RebalanceMarkers` with `#[serde(flatten)]`, so
  the API JSON keeps the same keys. Code reads `exec.rebalance.<field>`.
- `docs/audits/diesel-table-width.py` runs in the `lint` job. It fails on a
  `table!` over 64 columns, on a new `table!` over 32 columns, and on a
  manifest that enables `128-column-tables`. The three tables over 32 columns
  have ceilings that only go down.

No new `WorkflowEvent` variant. No migration. `harvest_events` is untouched.

### Also: durable route chain walk race (issue #2491)

The chain walk answered a final `500 result-unavailable` at its step cap on a
chain that was still advancing, so a client lost a result that existed. The
successor lookup now reports whether it read a live run, and the walk counts
it. The cap then answers pending. This adds no query.
`chain_walk_answers_pending_when_the_chain_moves_during_the_walk` pins the race.
