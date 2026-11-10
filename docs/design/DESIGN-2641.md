# DESIGN-2641: the durable agent under `HarvestPlugin` on SQLite

Issue #2641. Related: #2467 (the SQLite plugin), #2263 (the durable
agent) and #2372 (the recorded overlay read).

## 1. Problem

An autumn-web app on SQLite cannot host the durable agent through
`HarvestPlugin`:

- `autumn_harvest_agent::sqlite::register` takes a `SqliteRuntime`. It
  registers closures, not `#[activity]` bodies.
- The builder form, `autumn_harvest_agent::postgres::register`, needs the
  `postgres` feature of the agent crate.
- `HarvestPlugin` keeps its `HarvestBuilder` private. An app cannot apply
  either form to it.

So an app runs a second `SqliteRuntime` beside the plugin. It then has no
management API, no Harvest UI, and a job loop of its own.

## 2. Facts

- The SQLite runtime gives `HarvestBuilder::state` to each `#[activity]`
  body through `SqliteRuntime::set_shared_state` (issue #2467). The plugin
  driver calls it with the plugin state.
- Each agent activity reads the harness with `ctx.state::<AgentHarness>()`.
  No body reads a Postgres table. Each context call has a SQLite form:
  `check_llm_budget` passes, `run_tenant` reads `None`, and
  `record_llm_call` writes a ledger row with the completion.
- No agent `#[activity]` declares a feature that the SQLite audit refuses.

So the `#[activity]` bodies already run on the plugin driver. The gap is
the registration, not the driver.

## 3. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| Where the neutral call lives | a new feature · the `sqlite` module · **the crate root, with no feature** |
| How the plugin takes it | a `map_builder` closure · `HarvestPlugin::state` plus two lists · **`HarvestPlugin::agent(harness)`** |
| How the driver runs a harness body | a second closure registry · **the `#[activity]` body, with the harness as shared state** |
| How an app sends an op | raw `signal_with_start` · **`sqlite::agent::send`, which mirrors `autumn_harvest_agent::sqlite::send`** |
| The proof | one new test · **the overlay read suite on a third backend, and one test of the issue path** |

Why no closure registry in the driver? The `#[activity]` body already
reads the harness from shared state. A second registry would copy each
binding of `autumn_harvest_agent::sqlite`. The copies would drift.

## 4. Reverse brainstorming

"How could the plugin path run the agent wrong?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| An app needs the `postgres` feature for the call. | `register` is at the crate root with no feature gate. |
| The Postgres call drifts from the neutral one. | `postgres::register` calls `register`. |
| A steer or an interrupt queues behind the op that it must stop. | `send` signals the running run first, as the `sqlite` module does. |
| A client retry delivers an op twice. | `send` passes the idempotency key to each signal. |
| A state read sees no run and panics. | `state` returns `None` when the agent has no running run. |
| A decision reaches a later wait. | `decide` names the wait by its signal, as the `sqlite` module does. |
| An overlay read goes outside history. | The suite replays each run with a newer overlay and asserts `ReplaySucceeded`. |
| A body runs on a current-thread runtime and panics. | The `#[activity]` body is async, so it needs no `block_in_place`. |

## 5. Six thinking hats

- **White (facts).** Section 2 holds them. The issue asks for one neutral
  call and a test through `HarvestSqlite::signal_with_start`.
- **Red (feelings).** An app author wants one line: `.agent(harness)`.
- **Black (risks).** One driver runs every body. A long model turn delays
  the next command of the driver. `docs/sqlite-plugin.md` already states
  this. A fleet tenant shares the one harness.
- **Yellow (benefits).** The app gets the management API, the UI, the
  schedules and the retention of the plugin. It drops its own runtime.
- **Green (ideas).** The `/agents/{kind}/profile` routes stay Postgres-only
  in this change. The plugin still refuses `agent_overlay_store` on SQLite.
- **Blue (process).** Red: the suite and the issue test fail to compile.
  Green: the smallest code that passes. Refactor: the gates and the docs.

## 6. Acceptance criteria

1. An app with `HarvestPlugin` on SQLite registers the agent with no
   `SqliteRuntime` of its own.
2. The ops, the approvals and the overlay reads of the agent work through
   the plugin handle.
3. A test starts an entity agent through `HarvestSqlite::signal_with_start`,
   drives one op with the model turn, and reads the state through a query.
