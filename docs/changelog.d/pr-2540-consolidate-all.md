## One consolidation schedule for many agents (issue #2540)

The engine keys a schedule by its workflow name. The old
`consolidate::schedule(when, &agent)` therefore served one agent per
deployment, and a second schedule replaced the first with no error. Now
one schedule serves every agent. The design is in
`docs/design/DESIGN-2540.md`.

- **Breaking.** `consolidate::schedule(when, &ConsolidateAll)` builds one
  schedule of the new workflow `agent_consolidate_all`. The old per-agent
  form is gone. A deployment keeps a stored per-agent schedule of
  `agent_consolidate`, so delete it on upgrade.
- The activity `agent_consolidate_due` lists the running agents of each
  kind with `episodes` and the trigger `Schedule`, by `agent_kind`. It
  reads them through the new `AgentDirectory` trait. Install
  `postgres::Directory` or `sqlite::Directory` with
  `AgentHarness::directory`. With none, the read fails as
  `AgentDirectoryMissing`.
- An agent with no pending episode gets no pass and no model call. An agent
  whose pass still runs gets no second pass. A kind whose pass cannot run,
  for example under a mandate that forbids the pass model, is not served.
- One firing starts at most `max_passes` passes, 16 by default and 256 at
  most. The rest wait for the next firing. The turn order is round robin:
  the least recent pass first, then the oldest pending episode. So an
  agent whose pass marks nothing cannot starve the others. The dispatcher
  waits for each pass. A failed pass is one row of the `DispatchReport`.
- `AgentDirectory::last_pass` reads the last pass of an agent. On Postgres
  it uses the new engine query `execution::latest_with_search_attrs`, with
  its Ledger profile.
- **Breaking.** `ConsolidateTask` gets `dispatched` and is now
  `#[non_exhaustive]`. Build it with `ConsolidateTask::new`. A dispatched
  pass publishes `agent_consolidation_of`, so `harvest agents erase` erases
  it.
- New helpers: `sqlite::consolidate_all`, `postgres::consolidate` and
  `postgres::consolidate_all`.
- New workflow `agent_consolidate_all` and new activity
  `agent_consolidate_due`. `workflows()`, `activities()` and
  `sqlite::register` list them. A worker with a hand-picked activity list
  must add them. No new `WorkflowEvent` variant and no migration.
- `formal/verus/src/agent_dispatch.rs` proves the bound kernel
  `bounds::dispatch_passes`.

Tests: `autumn-harvest-agent/tests/consolidate_all.rs` covers each
acceptance criterion on SQLite. `postgres_agents.rs` covers the Postgres
directory and the dispatcher on a real worker.
