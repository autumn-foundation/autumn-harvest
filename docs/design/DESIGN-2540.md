# DESIGN-2540 — One consolidation schedule for many agents

Issue #2540. Parent epic #2452. It follows `docs/design/DESIGN-2454.md`.

## 1. Problem

`ConsolidationTrigger::Schedule` needs an engine schedule to start the
workflow `agent_consolidate`. Before this change, `consolidate::schedule`
built one schedule for one agent. The engine keys a schedule by its
workflow name. A second agent therefore replaced the schedule of the first,
with no error. A deployment could schedule passes for one agent only.

`EpisodeSettings::every(n)` runs a pass inside an op. An idle agent with
pending episodes then gets no pass.

## 2. Facts

- The durable agent publishes the search attribute `agent_kind` on each op.
  Its workflow id is its agent id.
- A pass that no agent run started publishes `agent_consolidation_of`.
  `harvest agents erase` erases each such pass.
- `execution::list_running_with_search_attrs` lists running runs by a
  JSON containment test on Postgres. `SqliteReader::list_executions` takes
  the same filter on SQLite.
- `ctx.spawn_child_workflow_fan_out_collect_raw` starts N children and
  returns one result for each child. A failed child does not fail the
  others.
- A Postgres schedule has `max_active_runs = 1` by default. A SQLite
  schedule has the overlap policy `skip`. So one firing runs at a time.
- The engine records a child result in the history of its parent.

## 3. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| How one schedule serves many agents | **a dispatcher workflow** · named schedules in the engine, with a Postgres migration |
| What the dispatcher lists | the episode store scopes · **the running agents of each kind, by `agent_kind`** · a list in the schedule input |
| Which kinds it reads | every kind · **each kind of the harness with `episodes` set and the trigger `Schedule`** |
| Where the list comes from | an engine API in the workflow · **an `AgentDirectory` trait on the harness, with a Postgres and a SQLite form** |
| How it skips an idle agent | start a pass that ends as `nothing_to_do` · **read the episode log first, and start no pass** |
| How it skips a running pass | a child workflow id per agent · **an awaited fan-out, and a directory read of `agent_consolidation_of`** |
| How it starts passes | detached children · **awaited children, one fan-out per firing** |
| The bound of one firing | none · **`max_passes`, 16 by default, at most 256** |
| Which agents go first | agent id order · the oldest pending episode first · **round robin: no pass first, then the least recent pass, then the oldest pending episode** |
| A kind whose pass cannot run | serve it, and report `disabled` · **skip it in the read** |
| A failed read of one agent | fail the read · **count it as `unreadable`, and go on** |
| How a dispatched pass is found by erase | none · **the task flag `dispatched`, so the pass publishes `agent_consolidation_of`** |
| The old per-agent `schedule` | keep it · **replace it: `consolidate::schedule` builds the dispatcher schedule** |

## 4. Reverse brainstorming

"How could the dispatcher break a deployment?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| A second agent loses its schedule again. | One schedule starts `agent_consolidate_all`. It reads every agent. No schedule names an agent. |
| An idle agent pays for a model call. | The read loads the episode log first. An agent with no pending episode gets no pass and no child run. |
| Two passes of one agent read the same episodes and pay twice. | The dispatcher awaits its passes, and the schedule runs one firing at a time. The read also skips an agent whose pass publishes `agent_consolidation_of` and still runs. |
| One firing starts a pass for each of 100 000 agents. | One firing starts at most `max_passes`. The cap is 256. The rest wait for the next firing. |
| A stuck agent holds a slot of the bound on each firing. A pass that marks nothing, for example on a spent budget, keeps its oldest episode. | The turn order is round robin. The agent whose last pass started first goes first, so a stuck agent goes to the back. |
| A kind whose mandate forbids the pass model pays for a child run on each firing. | The read skips a kind whose pass cannot run. |
| One agent whose log does not load stops every firing. | A failed read of one agent counts as `unreadable`. The other agents still get their passes. |
| A large fleet makes the read time out. | The read takes 8 agents at a time, in one activity of at most 15 minutes. |
| A failed pass stops the passes of the other agents. | The fan-out collects each result. A failed pass is one row of the report. |
| `harvest agents erase` misses a dispatched pass. | A dispatched pass sets `dispatched`. It then publishes `agent_consolidation_of`, as a top-level pass does. |
| A replay of the dispatcher lists agents again and diverges. | The list is the output of one activity. Replay reads the recorded output. |
| A kind with `every(n)` gets a second pass from the schedule. | The read takes only kinds with the trigger `Schedule`. |
| A worker with no directory fails with no hint. | The read fails as `AgentDirectoryMissing`, and the message names the fix. |
| The history of the dispatcher grows with the fleet. | It records the picked agents only, at most `max_passes`. It records counts for the rest. |
| The history of the dispatcher keeps episode text. | The read returns agent ids and times only. A pass report holds counts and short refusal reasons. |

## 5. Six thinking hats

- **White (facts).** Section 2. The engine has a fan-out that collects
  each result. Both backends can list running runs by a search attribute.
- **Red (feeling).** A silent replace of a schedule felt like data loss. A
  dispatcher that names what it skipped feels safe.
- **Black (risks).** A full scan of a large fleet on each firing. A pass
  that an app starts can run before it publishes its attribute, so a
  firing can miss it. A long pass delays the next firing. Section 7 lists
  each one as a known limit.
- **Yellow (value).** One schedule serves every agent. No migration and no
  engine change. Erase still finds each pass.
- **Green (alternatives).** Named schedules in the engine give one
  schedule per agent. That needs a Postgres migration and a scheduler
  change. A store index of pending scopes avoids the scan. Both can come
  later.
- **Blue (process).** Spec and proof of the bound, red tests, then code.
  The tests map one to one to the acceptance criteria.

## 6. Decision

```rust
pub struct ConsolidateAll {
    pub max_passes: u32,            // default 16, at most 256
}

pub trait AgentDirectory: Send + Sync + Debug {
    fn agents(&self, kind: &str) -> BoxFuture<Result<Vec<AgentId>, AgentError>>;
    fn last_pass(&self, agent: &AgentId) -> BoxFuture<Result<Option<LastPass>, AgentError>>;
}
```

1. `consolidate::schedule(when, &ConsolidateAll)` builds one schedule for
   the workflow `agent_consolidate_all`.
2. The activity `agent_consolidate_due` reads each kind of the harness
   with `episodes` set and the trigger `Schedule`. It lists the running
   agents of each kind with `AgentDirectory::agents`.
3. For each agent, it loads the episode log. An agent with no pending
   episode is idle. An agent whose pass still runs is skipped. Each other
   agent is due.
4. It sorts the due agents in turn order. An agent with no pass goes
   first. Then the agent whose last pass started first, then the oldest
   pending episode, then the agent id. `bounds::dispatch_passes` gives the
   number to start.
5. The workflow starts one child `agent_consolidate` for each picked agent
   in one fan-out, with `dispatched: true`. It waits for each one.
6. It returns a `DispatchReport`: the counts `listed`, `idle`, `running`,
   `due`, `unreadable`, `started`, `waiting` and `failed`, and one row for
   each pass.
7. `postgres::Directory` and `sqlite::Directory` implement the trait.
   Install one with `AgentHarness::directory`.

## 7. Known limits

- **Scan.** Each firing loads the episode log of each running agent of
  each scheduled kind, 8 at a time. A load needs no model call. The read
  has 15 minutes, so a very large fleet needs a store index of pending
  scopes. That index is not part of this change.
- **Fleets.** `postgres::Directory` lists the agents of a kind over the
  whole database, as `pause_mesh` does. Two fleets of one database need
  their own kind names.
- **Upgrade.** A stored per-agent schedule of `agent_consolidate` stays
  after an upgrade. The operator deletes it.
- **A pass that an app starts.** The attribute `agent_consolidation_of`
  appears at the first task of a pass. A firing between the start and that
  task does not see the pass, and can start a second one. The second pass
  then pays once more. The facts and marks stay correct.
- **A long pass.** The dispatcher waits for each pass. The schedule starts
  no firing while one runs. A long pass therefore delays the next firing
  for every agent.

## 8. Acceptance criteria and their tests

The tests are in `autumn-harvest-agent/tests/consolidate_all.rs` unless
noted.

| Criterion | Test |
|---|---|
| Two agents with `Schedule` both get a pass on each firing | `two_scheduled_agents_both_get_a_pass_on_one_firing`, `the_schedule_fires_one_dispatcher_for_every_agent` |
| An agent with no pending episode costs no model call | `an_idle_agent_costs_no_model_call_and_no_pass` |
| A pass that still runs for an agent gets no second pass | `an_agent_whose_pass_still_runs_gets_no_second_pass` |
| A stuck agent does not starve the others (review fix) | `a_stuck_agent_does_not_starve_the_others`; `round_robin_serves_each_due_agent` (proptest, `consolidate_all.rs`) |
| A kind whose pass cannot run is not served (review fix) | `a_kind_whose_mandate_forbids_the_pass_model_is_not_served` |
| The fan-out of one firing has a bound, and the rest wait | `one_firing_starts_at_most_max_passes_and_the_rest_wait`; `dispatch_passes` in `bounds.rs`, proved in `formal/verus/src/agent_dispatch.rs` |
| `consolidate::schedule` and `docs/agent-adapter.md` describe the new path | `consolidate::schedule` docs; section "Episodes and consolidation" |
| `ledger-profiles.py` and `comment-hygiene.py` pass | CI `lint` job |

The Postgres directory has its own test in
`autumn-harvest-agent/tests/postgres_agents.rs`.
