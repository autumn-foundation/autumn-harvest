# Durable agents: the agent adapter

`autumn-harvest-agent` runs an agent as a durable workflow (issue #1973).
The agent loop is the workflow. Each model call and each tool call is an
activity. A human approval is a durable wait.

A crash costs at most the one step that was in flight. A restart reads every
completed model call, policy decision and tool call from history. It does not
pay for them again.

The crate owns its agent primitives: `AgentModel`, `Tool`, `ToolPolicy`,
`Approval` and the message types. They follow the shape of the
`autumn-plugin-agent` primitives, but the crate does not depend on it. It
depends on the core engine only, with no default features.
[ADR 0006](adr/0006-agent-adapter-framework.md) records why.

Each Rust example compiles as written, and CI checks this (issue #2482). An
example takes each value from your app as a function parameter. The type of
the parameter shows what to pass.

## 1. The mapping

```mermaid
flowchart LR
  T[AgentTask] --> W["workflow agent_loop"]
  W -->|ModelTurnRequest| M["activity agent_model_turn\nAgentModel::chat + ToolPolicy::decide"]
  M -->|ModelTurn: reply + one decision per call| W
  W -->|Allow| X["activity agent_tool_call\nTool::execute"]
  W -->|RequireApproval| S["signal tool_approval:step:position:call_id\nwith a deadline"]
  S -->|Approve or Edit| X
  S -->|Reject or timeout| W
  W -->|Deny| W
  X -->|ToolOutcome| W
  W --> R[AgentReport]
```

| Agent concept | Engine primitive | What history records |
|---|---|---|
| The loop and its budgets | Workflow `agent_loop` | `AgentTask` in, `AgentReport` out |
| One model call | Activity `agent_model_turn` | The reply and the policy decision for each call |
| One tool call | Activity `agent_tool_call` | The tool result, cut to fit the result cap |
| "Ask a person first" | Signal with a deadline | The `Approval`, or the timeout |

## 2. Run it on SQLite

The example needs no API key and no network. It drives a run to its
approval wait, drops the runtime as a crash would, opens the file again,
approves, and finishes:

```sh
cargo run -p autumn-harvest-agent --features sqlite --example sqlite_agent
```

```text
waiting on tool_approval:0:1:write_1 for call Some("write_1")
  write_note ran with {"text":"remember the milk"}
stop:   Completed
answer: The note is saved.
model calls: 1 before the restart, 1 after it
```

The core of the example follows. `model` is the `AgentModel` of your provider:

```rust
use std::sync::Arc;
use autumn_harvest_agent::{
    AgentHarness, AgentModel, AgentTask, Approval, FnTool, Rule, ToolEffect, ToolRules, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use serde_json::{Value, json};

async fn save_a_note(model: Arc<dyn AgentModel>) -> Result<RunState, Box<dyn std::error::Error>> {
    // A harness takes each tool as an `Arc<dyn Tool>`. `shared` returns that type.
    let write_note = FnTool::new(
        "write_note",
        "Replace the note.",
        json!({"type": "object", "properties": {"text": {"type": "string"}}}),
        |input: Value| async move {
            println!("  write_note ran with {input}");
            Ok(json!({"saved": true}))
        },
    )
    .effect(ToolEffect::Write)
    .shared();
    let harness = AgentHarness::new(model)
        .tool(write_note)
        .policy(Arc::new(ToolRules::new().effect(ToolEffect::Write, Rule::Ask)));

    let mut rt = SqliteRuntime::open("agent.db")?;
    sqlite::register(&mut rt, Arc::new(harness));
    let exec = sqlite::start(&mut rt, &AgentTask::new("Save a note."))?;

    if let RunState::WaitingSignal(signal) = rt.run_until_blocked(exec).await? {
        // Show the call to a person, then send the decision.
        sqlite::decide(&mut rt, exec, &signal, &Approval::Approve)?;
    }
    Ok(rt.run_until_blocked(exec).await?)
}
```

The SQLite backend has no shared-state registry, so `sqlite::register`
captures the harness in synchronous closures. Each closure runs its async
body with `block_in_place`, so use a multi-thread Tokio runtime.

## 3. Run it on Postgres

Register the workflows and the activities. Install the harness as worker
state, because the activities read it with `ActivityContext::state`:

```rust
use std::sync::Arc;
use autumn_harvest::builder::{BuiltHarvest, HarvestBuilder};
use autumn_harvest_agent::{
    AgentHarness, AgentModel, Rule, Tool, ToolEffect, ToolRules, activities, workflows,
};

fn worker(client: Arc<dyn AgentModel>, lookup: Arc<dyn Tool>) -> BuiltHarvest {
    let harness = AgentHarness::new(client)
        .tool(lookup)
        .policy(Arc::new(ToolRules::new().effect(ToolEffect::Write, Rule::Ask)));

    HarvestBuilder::new()
        .workflows(workflows())
        .activities(activities())
        .state(harness)
        .build()
}
```

Section 2 shows how `FnTool::shared` makes a tool such as `lookup`.

`workflows()` returns `agent_loop`, `agent_heartbeat` and the durable
`agent` (section 15). `activities()` returns `agent_model_turn`,
`agent_tool_call`, `agent_memory_snapshot`, `agent_deliver`,
`agent_precheck`, `agent_discover_peers`, `agent_profile`,
`agent_profile_overlay` (section 15), `agent_memory_reset`,
`agent_workspace_retain`, `agent_workspace_reset`, `agent_workspace_floor`,
`agent_refine_propose` (section 16) and `agent_score` (section 17).
The test `the_engine_registration_builds` builds this
exact registration.

### The `postgres` module

The `postgres` feature adds the module `postgres`. It mirrors `sqlite`
function by function: `register`, `start`, `start_heartbeat`, `decide`,
`send`, `delete`, `current_run`, `state`, `LedgerSpend` and `pause_mesh`. A
worker drives each run, so a function records a start or a signal and
returns. It does not drive the run. `wait_run` and `wait_for` take the place
of `sqlite::drive`.

The example needs a database that it may migrate, and no API key. It runs a
worker in the process. The first worker stops at the approval wait, as a
crashed process does. A second worker takes the approval and finishes:

```sh
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/harvest
cargo run -p autumn-harvest-agent --features postgres --example postgres_agent
```

```text
waiting on tool_approval:0:1:write_1 for call Some("write_1")
  write_note ran with {"text":"remember the milk"}
stop:   Completed
answer: The note is saved.
model calls: 1 before the restart, 1 after it
agent support/acct-42: 2 ops, 26 tokens, last answer Some("Your order ships tomorrow.")
```

The core of it:

```rust
use std::time::Duration;
use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest::worker::DbPool;
use autumn_harvest_agent::postgres::{self, PostgresAgents, RunState};
use autumn_harvest_agent::{AgentHarness, AgentTask, Approval};

// On each worker:
fn worker(harness: AgentHarness) -> HarvestBuilder {
    postgres::register(HarvestBuilder::new(), harness)
}

// In the app:
async fn save_a_note(pool: DbPool) -> Result<RunState, Box<dyn std::error::Error>> {
    // `wait_run` returns `Running` when `limit` passes first.
    let limit = Duration::from_secs(60);
    let pg = PostgresAgents::single(pool);
    let exec = postgres::start(&pg, &AgentTask::new("Save a note.")).await?;
    if let RunState::WaitingSignal(signal) = postgres::wait_run(&pg, exec, limit).await? {
        // Show the call to a person, then send the decision.
        postgres::decide(&pg, exec, &signal, &Approval::Approve).await?;
    }
    Ok(postgres::wait_run(&pg, exec, limit).await?)
}
```

| Function | What it does |
|---|---|
| `PostgresAgents::single(pool)` | Targets one database. Each run lands on shard 0. |
| `PostgresAgents::sharded(&pools, router)` | Targets a sharded fleet. Pass the pools and the router of the workers. |
| `with_codecs`, `with_queue` | Use the payload codecs and the queue of the workers. The default queue is `default`. |
| `run_state`, `wait_run` | Where one run stands: `Completed`, `Failed`, `WaitingSignal`, `Ended` or `Running`. |
| `pending_approvals` | The approval signals that one run waits for now. |
| `wait_for` | Waits until the state of a durable agent passes a test. |

- `pending_approvals` reads the history of the run. Each approval wait has
  a deadline timer, `__signal_timeout:<seq>:<signal name>`. A wait is open
  until its timer fires or is cancelled, or its signal arrives. A decision
  that is queued but not yet read also closes it. The queued signals are
  read before the history, so a decision that a worker consumes between the
  reads still closes the wait. It reads that run only, not its child runs.
- `state` reads the entity projection, so it usually replays nothing. A
  snapshot in the payload store, or one past the side-effect cap, has no
  readable state in the row. `state` then replays the live run, as the query
  route does. It returns `None` before the first op commits, after a delete,
  and after an erasure.
- A lookup by agent id reads each physical database once. A first send goes
  to the shard that the router picks for the id.
- A shard rebalance moves a run and keeps its execution id. A sharded target
  follows the forwarding chain for each call by execution id. A lookup by
  agent id skips the `MIGRATED` seal and the `MIGRATING` copy, and so does
  `LedgerSpend`, so a moved run counts once.
- A steer or an interrupt that races a continue-as-new resolves the
  successor run and goes again.

To start a run without the module, start the name `agent_loop` with an
`AgentTask` as input. To decide on a gated call, send a signal to the run,
with a serialised `Approval` as the payload. Read the signal name from the wait: the
deadline timer of the wait is `__signal_timeout:<seq>:<signal name>`. On
SQLite, `RunState::WaitingSignal` holds the name. For a run with no
delegation, `approval::approval_signal(step, position, call_id)` also builds
it from the recorded `ModelTurn`. A worker with
no harness fails the call as `AgentHarnessMissing`, which is not retryable.

A worker with a payload cap other than the default must say so. Set
`AgentTask::max_request_bytes` to its activity-input cap, and
`AgentHarness::max_result_bytes` to its activity-result cap. A cap of zero
means no cap, as in the engine.

A worker with a payload store offloads an input or a result over the store
threshold. The engine then skips its cap for that value. The adapter does not
see the store, so it still applies its own caps. Set both caps to zero for such
a worker. Otherwise the adapter ends a run that the store could carry.

## 4. Build the harness

`AgentHarness` holds the parts that do real work:

- `new(model)` — any `AgentModel`. Implement its one `chat` method for your
  provider, or bridge a framework you already use. The crate ships no HTTP
  client.
- `tool(tool)` / `tools(iter)` — any `Tool`. `FnTool` wraps an async
  function.
- `policy(policy)` — any `ToolPolicy`. The default is `AllowAll`.
  `ToolRules` gives per-tool and per-effect rules.
- `temperature(t)` — the sampling temperature of each call.
- `tool_output_limit(chars)` — the cap on one tool result and on one
  delegation answer. The default is 8 000 characters. `Peer::answer_limit`
  overrides it for one peer.
- `max_result_bytes(bytes)` — the activity-result cap of the workers. A
  larger tool result is cut to fit it. A model reply that does not fit ends
  the run as `reply_too_large`. The default is the engine default. Zero is
  no cap.
- `model_timeout(d)` — the time budget of one model call. The default is
  13 minutes. With the policy, cache and ledger budgets, it stays below the
  15-minute `start_to_close`. A call over it is a
  retryable failure.
- `tool_timeout(d)` — the time budget of one tool call. The default is
  9 minutes, below the 10-minute `start_to_close`. A call over it is an
  error result that the model reads.
- `policy_timeout(d)` — the time budget of the policy for all calls of one
  turn. The default is one minute. A call with no decision when the budget
  ends is denied, and the decision is recorded.
- `hook_timeout(d)` — the time budget of one memory read, delivery or
  precheck. The default is 30 seconds.
- `cache_timeout(d)` — the time budget of one response-cache call. The
  default is 5 seconds.
- `budget_timeout(d)` — the time budget of the LLM budget check of one
  turn (issue #1997). The default is 10 seconds. A check over it is a
  retryable failure, before the model call.
- `memory(store)` — the `MemoryStore` for runs with a memory scope.
- `delivery(delivery)` — where reports go. The default is `LogDelivery`.
- `precheck(precheck)` — the cheap check that can skip a heartbeat tick.
- `response_cache(cache)` — serve an identical model call across runs.
  Section 11 explains it.

The names `memory` and `schedule_followup` belong to the built-in tools.
When a built-in is active, it hides an app tool with the same name. With a
memory store installed, `memory` is always reserved, even in a run that gets
no memory tool. In a run with a mandate, `delegate` is reserved too. See
[agent mesh](agent-mesh.md).

A tool error is cut to fit the result cap too, so a huge error message
cannot fail the run.

### The cost ledger

Each model turn writes one row to the
[agent cost ledger](llm-cost-ledger.md) (issue #1996). The row holds the
model id, the tokens, the cost and the call latency. It is outside the
encrypted turn output, so the usage report can sum it per tenant. Two
`AgentModel` methods fill it in:

- `model_id()` — the model id, which the response cache also keys on. The
  default is `None`. The ledger stores the id in clear, so do not put PII
  in it. It takes a token of ASCII letters, digits and `._:/@+-`, and
  records `None` or any other id as `"unknown"`.
- `cost_usd_micros(usage)` — the cost of one call, in millionths of a US
  dollar. The default is `None`, which counts the call as unpriced.

A response cache hit calls no model and spends no tokens, so it writes no
ledger row.

The SQLite backend writes the same rows (issue #2275). `sqlite::register`
registers the model turn with `register_activity_with_ledger`, so the turn
returns its call with its output. The runtime writes the row in the
transaction that appends the turn, as Postgres does. A crash before that
commit writes no row, so a resumed run counts each turn once. Read the rows
with `SqliteRuntime::llm_ledger(exec)`, or `llm_ledger_between` for a time
window.

`AgentTask` sets the run. Its bounds are `max_steps` (default 8, per
segment), `max_total_tokens` (per run), `max_output_tokens`,
`approval_timeout` (default one hour, rounded up to whole seconds) and
`max_request_bytes` (the default is the engine default; zero is no cap). Its
content is `input`,
`system`, `history` and `session`. `tenant` scopes the response cache.
Section 10 explains the always-on settings: `deliver`, `memory`,
`followups`, `loop_guard`, `read_only`, `unattended` and
`unattended_memory_writes`.

The loop drops a system message inside `history`. Only `system` reaches
the model as the prompt.

## 5. Approvals

A call that the policy answers with `RequireApproval` waits on one signal.
The name is `tool_approval:<round>:<position>:<call_id>`. `round` counts the
tool rounds of the whole run tree. With no delegation, it is the step of the
run.
`approval::approval_call_id` reads the call id back from a name.

| Decision | Effect |
|---|---|
| `Approval::Approve` | The call runs as the model asked. |
| `Approval::Edit { arguments }` | The call runs with the reviewer's arguments. |
| `Approval::Reject { reason }` | The call does not run. The model reads the reason. |
| No decision before the deadline | The call does not run. The model reads that no approval arrived. |
| A payload that is not an `Approval` | The call does not run. The model reads that the decision was not readable. The run goes on. |

Each name belongs to one wait. A decision that arrives after its deadline
stays unread in history. It never releases a later call, even when the model
reuses a call id. Send each decision once: a second one changes nothing, but
a strict replay check can report it.

A signal payload is capped at 256 KiB, so keep edited arguments under that
size. The policy does not check edited arguments again. Whoever can send a
signal to the run can therefore run a gated tool with any arguments. Guard
the signal endpoint as you guard the tool.

## 6. What a crash costs

| In flight at the crash | On resume |
|---|---|
| Nothing | Nothing runs again. |
| A model call | That one call is sent again. Providers take no idempotency key. |
| A tool call | That one call runs again. Use `ToolContext::run_id`, `step` and `call_id` as an idempotency key. |
| An approval wait | The wait resumes. Its deadline is durable. |

Two tests prove the model-call and tool-call rows. Each one runs its own
binary as a child, which calls `abort` in the middle of the loop. The parent
opens the same database and finishes the run. A log that both processes write
shows what ran in each process.

- `crash_mid_loop_resumes_without_rerunning_completed_calls` aborts inside the
  second tool call. In the parent, only that tool call and the last model call
  run.
- `crash_inside_a_model_call_resends_only_that_call` aborts inside the second
  model call. In the parent, only that model call and what follows it run.

The policy runs inside the model-turn activity. Its decision is part of the
recorded reply, so replay never asks the policy again.

## 7. How a run ends

A bound is a normal end, not an error. `AgentReport.stop` names it:

| `stop` | Meaning |
|---|---|
| `completed` | The model gave a final answer. |
| `output_capped` | The last turn hit the output cap. Its tool calls do not run. |
| `steps_exhausted` | The model asked for tools after `max_steps` rounds. |
| `tokens_exhausted` | The run spent more than `max_total_tokens`, follow-ups included. |
| `transcript_full` | The next request would exceed the request cap. The request is not sent. |
| `reply_too_large` | The model reply exceeds the result cap, even with its decision reasons cut. The activity drops the reply. |
| `budget_exceeded` | A run or tenant LLM budget refused the next model call. The call was not sent. See [LLM budgets](#llm-budgets-issue-1997). |
| `loop_detected` | The loop guard saw the same call, with the same result, too many times. |
| `failed` | The run failed. `AgentReport.error` holds the error. Only a fan-out child ends so, because a parent keeps its usage and labels (issue #2328). |
| `answer_rejected` | The answer check rejected more answers than its cap allows. `AgentReport.answer_rejections` holds each reason. See [Answer check](#21-answer-check-issue-2442). |

A turn that ends the run before its tool calls run is left out of
`AgentReport.messages`. Its text is not the answer either. A round that ends
the run as `transcript_full` stays in the transcript. Each call that did not
run, or whose result did not fit, gets an error result, so the transcript
shows what ran. The text of that turn is not the answer either.

A `transcript_full` transcript is too large for one request. Do not pass it
whole as the `history` of a new run. Summarise it, or keep its last turns.
A run with `AgentTask::context_reset` can reset before it reaches the cap. See
[Context reset](#context-reset-issue-2453).

Retries follow `ErrorKind::is_retryable`. `RateLimited`, `Transport` and
`Unavailable` retry with backoff, up to four attempts. So does a model call
over its time budget. Every other kind fails the model call at once. Your
`AgentModel` picks the kind, so map a provider timeout, 5xx or overload to
`Unavailable`. A tool call runs once, and a tool error is a result that the model
reads.

The run fails when a model call fails for good: a non-retryable kind, for
example a rejected API key, or four failed attempts.

### Payload caps (issue #2040)

A tool round measures the whole next request before it runs a call. The
request holds the transcript, the assistant turn and an answer for each call.
The round reserves a placeholder answer for each call. It replaces the
placeholder with the real result.

- When the placeholders exceed the request cap, no call runs. No result can
  reach the model, so no call with a side effect runs. The run ends as
  `transcript_full`.
- The round drops a result that exceeds the cap. The call ran. The calls
  after it do not run. The run ends as `transcript_full`.

The measure covers the result text and the flow state that this turn can
add. A delegation adds the labels and usage of its child, which the round
cannot know in advance. The loop measures the next request again before it
sends it. It ends the run as `transcript_full` if that request exceeds the
cap.

A task recorded before this rule measures the results alone, so a run in
flight replays as it ran. Such a task also reads a zero request cap as a limit
of zero bytes, as it did. `AgentTask::new` and `HeartbeatTask::new` turn the
new rule on. A recorded tick keeps the flag in `HeartbeatTask`.

The model-turn activity fits its reply to the result cap:

1. It cuts each decision reason to 256 bytes. It does this only when the
   turn exceeds the cap.
2. When the reply still exceeds the cap, the activity returns a turn with
   `oversized` set and no content. The run ends as `reply_too_large`. The
   activity logs a warning with the sizes.

The report keeps the usage of the paid call. The reply is not in
`AgentReport.messages`. A result cap of 256 bytes or more always holds the
`oversized` turn. A smaller cap can still fail the activity with
`PayloadTooLarge`.

A cap of zero is no cap, as in the engine. This holds for
`AgentTask::max_request_bytes` and `AgentHarness::max_result_bytes`, in every
check above.

### LLM budgets (issue #1997)

`max_total_tokens` bounds one run inside the workflow. A budget on the
Postgres engine also bounds a tenant across runs, and it can bound cost.

Each model turn checks the budget before the call. The budget sums the
[cost ledger](#the-cost-ledger) rows of the run and of its tenant. The cost
caps are in millionths of a US dollar, as `cost_usd_micros` is. An unpriced
call adds no cost, so only the token caps apply to it. Price the cache
counts in `cost_usd_micros` if your provider bills them at another rate.

The ledger writes the calls of a completed attempt only. A call in an
attempt that fails or times out is not counted, even when the provider
charges for it.

The budget counts `input_tokens` plus `output_tokens`. `input_tokens` must
count the whole prompt, cached tokens included. Some providers report the
cached tokens apart, so add them to `input_tokens` in your `AgentModel`.

A response cache hit calls no model. It spends no tokens and records no
row. The budget check still runs before the cache lookup, so a spent cap
refuses a turn that the cache could answer.

A failed spend read retries the turn, and uses one of its four attempts.
The turn calls no model before the check passes, so the retry pays nothing.

Declare the caps on `agent_loop`, and set the tenant on each task:

```rust
use autumn_harvest::builder::{BuiltHarvest, HarvestBuilder};
use autumn_harvest::quota::QuotaPolicy;
use autumn_harvest_agent::workflow::{activities, agent_loop_info};
use autumn_harvest_agent::{AgentHarness, AgentTask};

fn worker(harness: AgentHarness) -> BuiltHarvest {
    let budget = QuotaPolicy::new("tenant")
        .with_max_run_llm_tokens(200_000)
        .with_max_tenant_llm_cost_usd_micros(50_000_000) // 50 US dollars a day
        .with_tenant_llm_window_secs(86_400);
    HarvestBuilder::new()
        .workflows(vec![agent_loop_info().with_quota(budget)])
        .activities(activities())
        .state(harness)
        .build()
}

fn task() -> AgentTask {
    AgentTask::new("Summarise the ticket.").tenant("acme")
}
```

Register `agent_loop` with its policy on every worker that runs
`agent_model_turn`. The check reads the policy in its own process. A worker
without it does not enforce the budget, and logs one warning.

A spent cap refuses the next model turn. The adapter does not send the call,
and the run ends under `budget_exceeded`. Three limits apply:

- The cap is soft. The last turn that passes the check can pass the cap by
  its whole usage. Runs of one tenant at the same time can pass it
  together.
- A task with no `tenant` has no tenant budget. Its run caps still apply.
- The budget reads `AgentTask::tenant` as the task gives it. It does not
  read the verified tenant (#1977), as the response cache does.
- The SQLite backend rejects a workflow that declares `quota`. A budgeted
  `agent_loop` therefore does not build there.

## 8. History types

The adapter owns the payloads it records: `AgentTask`, `ModelTurnRequest`,
`ModelTurn`, `ToolCallRequest`, `ToolOutcome`, `AgentReport`,
`HeartbeatTask`, `HeartbeatReport`, `Report`, `ReportSource`,
`MemoryScope`, `Followups`, `LoopGuard`, `Mandate`, `Peer`,
`DelegationRecord`, `SkillText` and `SkillLookup`. They hold only types this crate
defines, so the crate owns their serde shape. Replay reads these payloads
back, so add a field only with `#[serde(default)]`. A new field of an
activity input also needs `skip_serializing_if` at its default, because a
strict replay compares each recorded input as it was written.

### 8a. Transcript deltas (issue #2264)

A model turn records only the messages that it adds. Its input names the
earlier window with a `PrefixRef`: a chained SHA-256 digest of the messages
and their count. The model-turn activity reads that window from the
`TranscriptStore` of the harness, adds the new messages, and keeps the
result for the next turn.

| Case | What happens |
|---|---|
| First turn of a run | The full form. The activity keeps the window. |
| A later turn | A delta on the window that the last turn sent. |
| A new segment or op | A delta when the window still starts with the last sent window. A trimmed transcript is sent in the full form. |
| The store misses | The turn fails with `TranscriptPrefixMissing` before any model call. The workflow sends it again in the full form. |

- The model reads the same window in both forms. The response cache key
  and provider prompt caching do not change.
- The request cap still measures the whole window, so the full form always
  fits.
- `AgentTask::new` turns the delta on. `transcript_delta(false)` turns it
  off. A task recorded before the change has no flag, so it replays as it
  ran.
- The default store is an `InMemoryTranscriptStore` with a 64 MiB bound. A
  restart empties it, so each run sends the full form once. Set a shared
  store with `AgentHarness::transcript_store` to hit on every worker.
- A read checks the digest of the window. A store that hands back a
  changed window is a miss, and the turn goes in the full form.
- A store holds transcript text outside history and outside the payload
  codecs. An erasure does not reach it, because `TranscriptStore` has only
  `get` and `put`. The app must clear its own store when it erases a run.

A 50-turn run with a 1 KB tool result each turn stores about 86% less
history (`a_50_turn_run_stores_at_least_80_percent_less_history`).

## 9. Limits

- **Each turn records its new messages.** A turn records a prefix
  reference and the messages that it adds (section 8a). History still
  grows with each turn, so a long session should continue as new with the
  report's `messages` as `history`.
- **Live text is a side channel.** An activity result is still one value.
  `AgentTask::stream` sends live text beside it, and nothing stores that
  text. See "Live tokens" in section 15.
- **Tool definitions are read at call time.** A deploy that changes the tool
  list changes only later turns. Recorded turns replay as they were.
- **A follow-up chain shares one workflow history.** Each segment adds to
  it. Keep `max_chain` low, or start a new run from the report.
- **A late reply or approval stays unread.** A durable agent never takes
  a promise or approval signal whose wait ended, and a checkpoint carries
  it to the next run. Such signals add bytes for the life of the agent.

## 10. Always-on agents

An always-on agent wakes on a schedule, books its own follow-ups, keeps
notes, and speaks only when something needs attention. Five primitives do
this. Each one maps onto an engine primitive, so a crash costs no more than
it does in a plain run.

| Primitive | Engine primitive | Turn it on |
|---|---|---|
| Heartbeat | Workflow `agent_heartbeat`: one tick per run | `heartbeat::schedule`, `postgres::start_heartbeat`, `sqlite::start_heartbeat` |
| Follow-up | A durable timer, then a new segment in the same workflow | `AgentTask::followups` |
| Delivery | Activity `agent_deliver` | `AgentTask::deliver`, `AgentHarness::delivery` |
| Memory | Activity `agent_memory_snapshot`, once per segment | `AgentTask::memory`, `AgentHarness::memory` |
| Loop guard | A fingerprint of each recorded call, in the workflow | On by default: `AgentTask::loop_guard` |

```mermaid
flowchart LR
  S["engine schedule"] --> H["workflow agent_heartbeat"]
  H --> P["activity agent_precheck"]
  P -->|false| E[skipped]
  P -->|true| B["agent loop, read-only"]
  B --> D["activity agent_deliver"]
  R["workflow agent_loop"] -->|schedule_followup| T["durable timer"]
  T -->|"new segment, read-only"| R
  R --> D
```

### Unattended runs are read-only

No person watches a heartbeat tick or a follow-up segment. Both are
read-only by default:

- The policy denies a tool with the `Write` or `External` effect, and a tool
  that the harness does not know. The model reads why.
- The tool-call activity checks the effect again before it runs a tool.
- The run cannot write its memory. A note from a run that read untrusted
  data would reach the system prompt of every later run in the scope.

`HeartbeatTask::allow_actions` and `Followups::allow_actions` lift the
first two rules only. The memory rule has its own opt-in:
`HeartbeatTask::allow_memory_writes` or `AgentTask::unattended_memory_writes`.
`AgentTask::read_only` applies all three rules to any run.
`AgentTask::unattended` applies the memory rule.

### Heartbeats

A tick is one short workflow:

1. The `Precheck` runs. A `false` answer ends the tick with no model call.
   Use it to look for new work, for example an unread inbox. A precheck over
   the hook budget skips the tick.
2. The agent loop runs the heartbeat prompt. The default prompt tells the
   model to reply with exactly `HEARTBEAT_OK` when nothing needs attention.
3. The workflow delivers the answer, unless it is a silent acknowledgement:
   `HEARTBEAT_OK` with no other letter or digit.

A tick never books a follow-up. `HeartbeatTask` takes its own budget:
`max_steps`, `max_total_tokens`, `max_output_tokens`, `max_request_bytes`
and `approval_timeout`. Set `max_request_bytes` to the activity-input cap
of the workers, as for `AgentTask`. The approval wait defaults to 15
minutes, because the schedule starts no new tick while one runs.

On Postgres, register the schedule that `heartbeat::schedule` builds:

```rust
use std::sync::Arc;
use std::time::Duration;
use autumn_harvest::builder::{BuiltHarvest, HarvestBuilder};
use autumn_harvest::policy::Schedule;
use autumn_harvest_agent::heartbeat::{self, HeartbeatTask};
use autumn_harvest_agent::{AgentHarness, AgentModel, activities, workflows};

fn worker(model: Arc<dyn AgentModel>) -> Result<BuiltHarvest, serde_json::Error> {
    let tick = HeartbeatTask::new().system("You watch the build queue.");
    let every_30_minutes = Schedule::Interval(Duration::from_secs(1_800));
    Ok(HarvestBuilder::new()
        .workflows(workflows())
        .activities(activities())
        .workflow_schedule(heartbeat::schedule(every_30_minutes, &tick)?)
        .state(AgentHarness::new(model))
        .build())
}
```

On SQLite, pass the same `heartbeat::schedule` to
`SqliteRuntime::upsert_schedule` (issue #2031). An app can also call
`sqlite::start_heartbeat` or `postgres::start_heartbeat` on each tick.

The engine keeps one schedule per workflow name, so a deployment has one
heartbeat schedule. For one heartbeat per agent, use a durable `Agent`
(section 15). Each agent key has its own heartbeat, driven by its own
durable timer, and a tick is an op in its inbox. A deployment can run any
number of them.

### Follow-ups

A task with `AgentTask::followups` gives the model the `schedule_followup`
tool. The tool takes a `prompt` and a `delay_minutes`. When the segment
completes, the workflow waits on a durable timer. Then a new segment runs
in the same conversation.

The model wrote the prompt, maybe under the influence of a tool result. So
the prompt never goes into a user message. It stays in the model's own
`schedule_followup` call, an assistant message. The new segment starts with
a fixed user message that the app controls, which tells the model to act on
that call.

- The workflow refuses a second follow-up in one segment.
- The prompt is at most 2 000 characters. The count includes white space,
  because the transcript keeps the prompt as the model wrote it.
- The delay is at most the `max_delay` of `Followups::new`. The shortest
  delay is one minute. A durable agent counts the delay from the end of the
  segment, so a long segment never books a follow-up that is already due.
- A chain cap (`Followups::max_chain`, 10 by default) stops an agent that
  wakes itself forever.
- A segment that ends under any stop other than `completed` runs no
  follow-up. The report then sets `followup_dropped`.
- `max_total_tokens` and the loop guard cover the whole chain. `max_steps`
  counts each segment from zero.
- Approval names and `ToolContext::step` count steps across segments, so
  they never repeat. The report covers the whole run.

A restart during the wait resumes the timer. It sends no report again and
asks the model nothing again.

### Delivery

`AgentTask::deliver` sends the result of each segment to the `Delivery` on
the harness. The default `LogDelivery` logs the run id and the size of the
text, never the text itself.

- A `completed` answer goes out, unless it is empty or a silent heartbeat
  acknowledgement. Only the text of the last turn counts. A segment that
  ends on an empty turn sends nothing, even when an earlier turn wrote
  text beside a tool call. An empty `output_capped` turn sends only the
  stop notice. A run recorded before this rule replays as it ran. The
  workflow records the `agent_final_turn_rules` patch marker to tell them
  apart. The follow-up length rule uses the same marker.
- A segment that a bound ended early sends a notice, for example
  `The agent stopped early: loop_detected.`. An unattended run therefore
  does not fail in silence.
- A failed delivery never fails the run.

The workflow sends each report once, and replay does not send it again. The
activity itself can retry, for example after a send that timed out. So a
`Delivery` that must not repeat a message dedupes on `Report::key()`: the
run id and the segment. The run id is the execution id, so a later run
under the same workflow id gets new keys. A task recorded before this
change has no `run_id_source` field. It keeps the workflow id, so its run
replays as it ran. A task built from JSON can set
`"run_id_source": "execution_id"`.

### Memory

`AgentTask::memory` gives the run a memory scope, and the model a `memory`
tool to add, replace and remove entries. The store is a `MemoryStore` on
the harness. `InMemoryMemoryStore` suits tests. Use a durable store in
production. A run with a scope and no store fails as `MemoryStoreMissing`.

An activity reads the snapshot once per segment. The workflow adds it to
the system prompt. A write during the segment goes to the store at once,
but the prompt does not change until the next segment. A stable prompt
keeps provider prompt caching working, and replay reads the recorded
snapshot.

The snapshot escapes each entry onto one line, so an entry cannot close its
block or add a prompt section. The snapshot tells the model that the
entries are its own notes, not instructions from the user.

Each edit carries an `EditKey`: the run, the step and the call. A store
applies each key once, so a call that runs again after a crash changes
nothing, even when another run edited the scope in between. A store keeps
the first result of a key, an error included, and returns it on a retry. A
failed `remove` therefore cannot remove a newer entry after a crash. A
durable store must save the key and its result with the edit, in one
transaction. An `add` of an entry that already exists changes nothing
either.

### Loop guard

The loop guard counts identical calls: same tool, same arguments, same
result. With the defaults, the third identical call in the last 30 calls
adds a warning to its result. The fifth stops the run as `loop_detected`,
and no later call in that round runs. One guard covers the whole run,
follow-ups included. `LoopGuard::disabled()` turns it off. A recorded task
with no `loop_guard` field decodes with the guard off, so a run started
before the guard existed replays as it ran.

The guard is not a cost control. A model that changes its arguments, or a
tool whose result changes, does not trip it. Use `max_steps`,
`max_total_tokens` and the chain cap for cost.

### Context reset (issue #2453)

A long run can save its progress and restart from it, after CORAL. The
setting is off by default.

```rust
use autumn_harvest_agent::{AgentTask, ContextReset};

// Up to 2 resets, each before a request that reaches 80 % of the cap.
let task = AgentTask::new("audit the ledger")
    .context_reset(ContextReset::new(2, 80).max_notes_bytes(4_096));
```

- The model gets the `save_progress` tool. Its `notes` argument replaces
  the notes of the run. The workflow handles the call, so no activity runs.
  Replay reads the call from the recorded model turn.
- Notes over `max_notes_bytes` get an error result. The earlier notes stay.
  The default bound is 8 192 bytes.
- Before each request, the loop measures it against the request cap of
  [Payload caps](#payload-caps-issue-2040). When the request reaches
  `reset_at_percent` of the cap and a reset is left, a reset is due. 0 counts
  as 1, and a value over 100 counts as 100. A zero cap is no cap, so it never
  resets. The loop measures the reset window with the notes of that time. A
  reset whose window still reaches the threshold frees no room, so it does
  not run.
- A due reset first asks for a save turn. The loop sends the window with the
  fixed notice `RESET_NOTICE`. The model reads the last round, and only
  `save_progress` runs in that turn. Each other call gets an error result.
- The reset runs before the next request. It also runs when the save turn
  answers in text, or when its round does not fit the cap. The reply to the
  notice is no answer. A failed save keeps the earlier notes.
- The save turn needs room under the cap for the notice and
  `max_notes_bytes`. Without that room, the reset runs at once.
- The new window is the system prompt and `AgentTask::input`. In a segment
  that a follow-up woke, the booking that woke it and the wake message
  follow. A booking of the segment itself follows them. Then the notes and
  the steers of a durable agent op follow.
- The notes are the model's own `save_progress` call, with a fixed result.
  The model wrote them, maybe under the influence of a tool result, so they
  never go into a user message.
- `max_resets` counts per run, follow-ups included. With no reset left, the
  run ends as `transcript_full`, as before. `AgentReport::resets` counts the
  resets. A report with no reset does not write the field.
- A reset changes only the transcript. The flow labels belong to the run, so
  a reset clears no taint.
- A delegated child takes the setting and starts with no notes. Its resets
  are its own. `DelegationRecord::resets` counts them.
- The reset reads only recorded data, so replay takes the same reset. The
  next turn after a reset goes in full, not as a delta.

- A tool round can pass the cap in one step (issue #2461). The round drops
  the result that does not fit, as before. With a reset left, the run does
  not end. A reset is due, with a save turn first when one fits.
- That reset keeps the round. The new window shows the assistant turn and
  the answer of each call: its result, `DROPPED_FULL` or `NOT_RUN_FULL`. So
  the model sees which calls ran, and does not run a call with an effect
  again. The round goes after the bookings and before the notes.
- That reset runs only when its window, with the notes of that time, falls
  under the threshold. If not, or with no reset left, the run ends as
  `transcript_full`, as before.
- A full round that the loop guard or an interrupt also stopped never
  resets, so the stop holds. Such a save round ends the run too.
- The patch marker `agent_reset_on_full_round` gates both rules. A run
  recorded before it replays as it ran.

A task with no `context_reset` field takes no new path, so a recorded run
replays as it ran.
`formal/verus/src/agent_reset.rs` proves that a request over the cap always
reaches the threshold, and that no run passes `max_resets`. It also proves
that the window after a full-round reset, with the notes of that time, fits
the cap.
`docs/design/DESIGN-2453.md` and `docs/design/DESIGN-2461.md` record the
design.

## 11. Response cache

Replay reads an answer from the history of one run. A second run that sends
the same request pays again. The response cache (issue #1998) serves that
request from an earlier answer. It is off by default.

```rust
use std::sync::Arc;
use std::time::Duration;
use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest::payload_store::PayloadStore;
use autumn_harvest_agent::{AgentHarness, AgentModel, InMemoryCacheIndex, ResponseCache};

fn cached(
    builder: &HarvestBuilder,
    store: Arc<dyn PayloadStore>,
    model: Arc<dyn AgentModel>,
) -> AgentHarness {
    // Add the codecs to the builder first. Clones share their keys.
    let codecs = builder.payload_codecs().clone();
    let cache = ResponseCache::new(store, Arc::new(InMemoryCacheIndex::default()), codecs)
        .max_age(Duration::from_secs(24 * 3600));
    AgentHarness::new(model).response_cache(cache)
}
```

**The key.** The key is a SHA-256 hash of the model id, the output cap, the
temperature, the tools, the messages, the tenant and its source, the
namespace and the draw.

**The draw (issue #2440).** `ChatRequest::draw` names one sample of a run.
Two draws of one request get two keys, so each draw has its own entry. A
request with no draw keeps the key that it had before draws, so stored
entries still hit.

`run_draws` runs each draw as a child `agent_loop` run with the run id
`{root}:s{n}`. Each call records a new UUID as its root, so two batches never
share a run id. Tools then get one idempotency key per draw. A draw does not
deliver, and it has no memory, no workspace and no follow-ups. Each draw gets
the whole token cap of the task, so the caller bounds the total.

A delegated child keeps the draw id and gets its own run id. A durable agent
segment also ends in `:s{n}`, but its root is an incarnation id. A model may
pass the draw to its provider as a seed. Then draw 0 of each run has the
same seed, and the cache shares that answer across runs.

**The model id.** A model names itself through `AgentModel::model_id`. A
model with no id is never cached. The crate ships no model with an id. The
id must change whenever the answer can change for the same request. Use a
pinned model version, not an alias such as `latest`. Put each client setting
that `ChatRequest` does not hold, such as `top_p`, in the id.

**The tenant.** The default scope is `CacheScope::Tenant`. Only runs of the
same tenant share an entry. Runs with no tenant share only with each other.
A declared tenant never shares an entry with a verified tenant of the same
name.

| Backend | Tenant of the key |
|---------|-------------------|
| Postgres, run with a verified tenant (#1977) | The verified tenant. `AgentTask::tenant` cannot change it. |
| Postgres, run with no tenant | `AgentTask::tenant`, as a declared tenant |
| Postgres, the tenant read fails | No key. The turn skips the cache. |
| SQLite | `AgentTask::tenant`, as a declared tenant |

`HeartbeatTask::tenant` sets the tenant of each tick. `CacheScope::Shared`
lets every run share every entry. Use it only for prompts that hold no
tenant data.

**History.** A hit is the result of `agent_model_turn`. History records it
like any other answer, with `cache_hit: true` and zero token usage. The
policy still decides each tool call, and each tool still runs. Replay reads
history and never reads the cache.

**Tool idempotency.** A hit repeats the tool-call ids of the stored answer.
Tools take the run id, the step and the call id as an idempotency key. With
`RunIdSource::WorkflowId`, two runs can share a run id. On Postgres, such a
turn skips the cache. A child run id `{root}:d{n}` or `{root}:s{n}` of such
a run skips it too. On SQLite, start each run from `AgentTask::new`, which
uses the execution id.

**Storage.** The answer goes to your `PayloadStore`, inside a codec
envelope. Pass a clone of the engine's `PayloadCodecs`, so an active codec
encrypts each entry. A registry with no active codec stores entries in
clear. The entry holds the answer and no prompt. Use a store for the cache
alone, with an expiry of at least `max_age`. Do not put an expiry on the
store that holds offloaded event payloads.

**The index.** A `CacheIndex` maps each key to its blob. `InMemoryCacheIndex`
keeps the map in process memory, with a capacity. It loses the map on a
restart, and each worker has its own map. Implement `CacheIndex` to share
one map between workers:

- `get` returns the blob key of an entry, if any.
- `put` stores a blob key, and returns each blob key that no entry holds
  after the write. The cache deletes those blobs.
- An index owns its blobs. Do not let a store share one blob between two
  indexes.

**Faults.** A cache fault, a slow cache or an entry that does not decode is a
miss with a warning. An out-of-date entry is a quiet miss. The cache never
fails a turn. Each cache call, the tenant read included, has the
`cache_timeout` budget: 5 seconds by default. One turn makes at most three
cache calls. The cache neither stores nor serves a turn over
`AgentHarness::max_result_bytes`, because a cached turn over the cap would end
every identical run as `reply_too_large`. A hit over the cap is a miss, and
the harness asks the model. A codec error is logged with fixed text,
because a custom codec can quote the answer.

**Limits.**

- A hit repeats one sample. At a high temperature, a second run gets the
  same answer. Give each sample its own draw to keep the samples apart.
- `max_age` limits what the cache serves, not what the store keeps. Change
  `namespace` to make every old entry a miss. The old blobs stay in the store
  until the index drops them.
- PII erasure (#495) does not reach the cache. Bound it with `max_age` and a
  store expiry.
- Key rotation (#948) does not re-encrypt entries. An entry under a retired
  key is a miss. Its blob stays until a new answer replaces it or the store
  expires it.
- Two runs that miss at the same time both call the model.
- A run that sets `tenant` records it in each model-turn input. An older
  binary rebuilds that input without it, so set `tenant` only after the
  whole fleet runs this release.
- A draw records `draw` in its task and in each model-turn input. An older
  binary drops the field. Its draws then share one key, and its run ids
  change. Call `run_draws` only after the whole fleet runs this release.

## 12. Evaluate a candidate overlay

`eval::evaluate` re-drives a recorded `agent_loop` run with a candidate model
and a profile overlay (issues #2001 and #2375). It reports where the
decisions of the two runs diverge. Turn on the `eval` feature.

```rust
use std::sync::Arc;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::eval::{Candidate, evaluate};
use autumn_harvest_agent::{AgentHarness, AgentModel, Tool};

// `history` holds the events of a finished run, for example from
// `SqliteRuntime::load_history`.
async fn try_model(
    history: &[WorkflowEvent],
    new_model: Arc<dyn AgentModel>,
    tools: Vec<Arc<dyn Tool>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let candidate = Candidate::new(AgentHarness::new(new_model).tools(tools))
        .system_prompt("Answer in one sentence.");
    let evaluation = evaluate(history, &candidate).await?;
    if let Some(turn) = evaluation.first_divergence {
        println!("{:?}", evaluation.turns[turn]);
    }
    Ok(())
}
```

The harness runs the real loop in the in-memory test engine. It writes
nothing to a database. The source history is a borrowed slice, so it cannot
change.

| Activity | In an evaluation |
|---|---|
| `agent_model_turn` | The candidate model answers under the overlay profile, then the candidate policy decides. These are the only live calls. The response cache is skipped, so every turn asks the candidate. |
| `agent_tool_call` | The recorded outcome of the same call, or an error stub. No tool runs. |
| `agent_memory_snapshot` | The recorded snapshot, or an empty one. |
| `agent_deliver` | A stub. No report leaves the harness. |
| `agent_discover_peers` | The recorded peers for the same names, or none. The candidate needs no roster. |
| A child run of a fan-out | The recorded report of an unused child run with the same delegation chain and task text. With no match, the candidate run fails. No child runs live. |
| Any other activity, such as `agent_precheck` | No mock. The candidate run fails. |

A tool, snapshot or peer-lookup activity that failed the source fails the
candidate at the same activity.

The policy runs live, so use a policy with no side effects. It sees the
recorded run id and the aligned call ids. A retryable model failure retries
under the retry policy of `agent_model_turn`, as on a worker. A turn over the
harness result cap (`AgentHarness::max_result_bytes`) is cut or dropped, as on
a worker with no payload store. A dropped reply reads as a `stop` divergence.

A recorded outcome answers a call with the same step, tool name and
arguments. A candidate call that equals the recorded call at the same turn
and position, of the same agent, takes the recorded call id. Approval signal names hold the call
id, so the recorded approvals stay valid. Any other call gets a new `eval_`
id, so no recorded approval can release it. That id holds the turn and the
position, not the provider id. The harness sends again only an
approval for a wait that the source opened, when it arrived before the
deadline. An approval sent before its wait opened leaves no deadline timer,
so the harness drops it, and the candidate times out at that wait.

The report holds one `TurnDiff` per model turn. A turn diverges when the
calls, the arguments, the policy decisions or the stop reason differ. Two
final answers with the same stop reason and different text get `Reworded`.
That is not a divergence. `end_diverged` compares how the two runs end, for
example `Completed` and `TokensExhausted`. `diverged()` reads both.
`stubbed_tool_calls` counts the calls with no recorded outcome.

### Run a whole overlay

`Candidate::overlay` runs the source under a `ProfileOverlay` (issue
#2375). The harness reads the source profile from the recorded task. That
profile holds the prompt, the bounds, `read_only`, the model, the mandate,
the tool guidance and the skills. `agent::overlay::apply` merges the overlay
before the engine starts.

```rust
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::eval::{Candidate, evaluate};
use autumn_harvest_agent::{AgentHarness, ProfileOverlay};

async fn try_overlay(
    history: &[WorkflowEvent],
    harness: AgentHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let overlay = ProfileOverlay {
        guardrails: Some("Never refund twice.".into()),
        max_steps: Some(4),
        tool_guidance: [("lookup".into(), "Look up one order at a time.".into())].into(),
        ..ProfileOverlay::default()
    };
    let evaluation = evaluate(history, &Candidate::new(harness).overlay(overlay)).await?;
    println!("{}", evaluation.overlay_digest);
    Ok(())
}
```

| Part | In an evaluation |
|---|---|
| The ceilings | `Ceilings::from_profile` of the source profile, so an overlay can only narrow the source bounds and model. `Candidate::ceilings` sets operator ceilings. |
| A refused overlay | `EvalError::Overlay` with the `OverlayError`. It costs no model call. `compare` checks both overlays first. |
| `system` and `guardrails` | The system prompt of every turn of the root agent. |
| `tool_guidance` | The description of each named app tool in the `ToolDefinition` that the model sees (issue #2373). A recorded outcome still answers a call with the same step, name and arguments. |
| `skills` | The skill list in the system prompt and the `load_skill` tool. A `load_skill` call is a tool call like any other: a recorded outcome answers it, or it gets the stub. |
| `Candidate::system_prompt` | A one-part overlay that sets `system`. |
| `Candidate::model` | A one-part overlay that sets `model`. The ceilings admit the model, because the operator names it. The harness and the source mandate must still hold it, or the evaluation fails before any call. Every turn asks it. |
| `Candidate::agent_model` | A route for one agent. It is not an overlay part. A route to a model that the harness does not hold gives `OverlayError::UnknownModel` before any call (issue #2376). |
| `Candidate::overlay` | Replaces the parts and the route that a shorthand set before it. It keeps the ceilings and the agent routes. |
| `Evaluation::overlay_digest` | `ProfileOverlay::digest`: the SHA-256 of the JSON of the overlay. Equal overlays have equal digests. The digest leaves out the ceilings and the agent routes. |

The candidate report shows the recorded run id, as the policy does. So two
evaluations of one candidate with the same replies are equal.

### Run a live probe

An evaluation stubs each new tool call, so it cannot judge a new tool
plan. `eval::live_probe` runs the candidate on a Postgres fork with real
tools (issue #2378). It runs only the tool effects that the operator
allows. Turn on the `eval` and `postgres` features.

```rust
use std::time::Duration;
use autumn_harvest::ExecutionId;
use autumn_harvest_agent::eval::live_probe;
use autumn_harvest_agent::postgres::{self, PostgresAgents, RunState};
use autumn_harvest_agent::{AgentHarness, ProfileOverlay, RefinePolicy, ToolEffect};

async fn probe(
    pg: &PostgresAgents,
    harness: &AgentHarness,
    source: ExecutionId,
    overlay: &ProfileOverlay,
    policy: &RefinePolicy,
) -> Result<RunState, Box<dyn std::error::Error>> {
    let allowed: Vec<ToolEffect> = policy.probe_effects.iter().copied().collect();
    let probe = live_probe(pg, harness, source, overlay, &allowed).await?;
    Ok(postgres::wait_run(pg, probe.exec_id, Duration::from_secs(300)).await?)
}
```

The fork starts at event 0 with `ForkEffects::LiveQuiet`. Its input is the
source task under the overlay, with the source ceilings. The input holds
`AgentTask::probe`: the allowed effects. The `WorkflowForked` event of the
fork names the source. A worker that holds the same tools runs the fork.

| Part | In a live probe |
|---|---|
| `agent_tool_call` | Runs a tool only when its effect is in the set. Any other tool gets `{"error": "probe_effect_refused", "effect": ...}`, and `Tool::execute` does not run. The check is in the activity, so a permissive policy cannot bypass it. |
| The `memory` tool | Refused. A note would show in each later run of the scope. |
| The policy | Runs as in the source. Nobody answers a probe, so the approval wait is one second. A gated call times out to a deny. |
| `agent_deliver` | Never runs. The probe input also sets `deliver` to `false`. |
| A delegated child | Runs inline, as a probe with the same set. A fan-out child would be a separate run that notifies on its end. |
| A durable peer | The delegation is refused. The peer runs its own effects. |
| The workspace | A probe reads it and never moves its head. |
| Follow-ups and the draw | The probe input has neither. A follow-up would wait on a timer, and a draw root would give the probe the run id of the source. |
| The end of the run | `LiveQuiet` sends no completion callback and fires no completion trigger. |
| The cost ledger | The model turns record rows under the fork execution id. |
| The source | A fork only inserts rows, so the source history stays the same. |

An older worker cannot decode `live_quiet`, so it cannot run a probe. Start
a probe only after every worker runs this release.

Pass `PostgresAgents::with_payload_offloader` when the workers offload
payloads. The probe then reads an offloaded source input, and offloads a
large probe input.

The default of `RefinePolicy::probe_effects` is `[read_only]`. An empty set
refuses each tool. SQLite has no fork, so its host returns
`EvalError::Unsupported`. A refused overlay, an erased source or a source
that is not an `agent_loop` run writes nothing. The design is in
[`DESIGN-2378.md`](design/DESIGN-2378.md).

### Compare two candidates

`eval::compare` runs two candidates on one source (issue #2271). Each can
hold a model, an overlay or both (issue #2375). It returns both evaluations,
each with its overlay digest, and one `TurnDiff` per turn of the two
candidates. In that diff, `recorded` holds the first candidate.

```rust
use std::sync::Arc;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::eval::{Candidate, Comparison, compare};
use autumn_harvest_agent::{AgentHarness, AgentModel};

async fn compare_models(
    history: &[WorkflowEvent],
    fallback: Arc<dyn AgentModel>,
    a: Arc<dyn AgentModel>,
    b: Arc<dyn AgentModel>,
) -> Result<Comparison, Box<dyn std::error::Error>> {
    let harness = Arc::new(AgentHarness::new(fallback).model_named("a", a).model_named("b", b));
    let first = Candidate::shared(Arc::clone(&harness)).agent_model("planner", "a");
    let second = Candidate::shared(harness).agent_model("planner", "b");
    Ok(compare(history, &first, &second).await?)
}
```

`Candidate::model` sets the model of every turn. `Candidate::agent_model`
sets the model of one agent of a delegation tree and wins over it. Two
overlays compare the same way:

```rust
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::eval::{Candidate, compare};
use autumn_harvest_agent::{AgentHarness, ProfileOverlay};

async fn compare_overlays(
    history: &[WorkflowEvent],
    harness_a: AgentHarness,
    terse: ProfileOverlay,
    harness_b: AgentHarness,
    guided: ProfileOverlay,
) -> Result<(), Box<dyn std::error::Error>> {
    let first = Candidate::new(harness_a).overlay(terse);
    let second = Candidate::new(harness_b).overlay(guided);
    let comparison = compare(history, &first, &second).await?;
    assert_ne!(comparison.first.overlay_digest, comparison.second.overlay_digest);
    Ok(())
}
```

### Score a comparison

Divergence is not quality. `Candidate::scorer` adds a `score::Scorer`
(issues #2441 and #2447). The report then holds a verdict for each run end.

```rust
use std::sync::Arc;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::eval::{Candidate, compare};
use autumn_harvest_agent::score::{ExactMatch, Scorer, Verdict};
use autumn_harvest_agent::{AgentHarness, AgentModel};

async fn score_models(
    history: &[WorkflowEvent],
    fallback: Arc<dyn AgentModel>,
    a: Arc<dyn AgentModel>,
    b: Arc<dyn AgentModel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let scorer: Arc<dyn Scorer> = Arc::new(ExactMatch::new("42"));
    let harness = Arc::new(AgentHarness::new(fallback).model_named("a", a).model_named("b", b));
    let first = Candidate::shared(Arc::clone(&harness)).model("a").scorer(Arc::clone(&scorer));
    let second = Candidate::shared(harness).model("b").scorer(scorer);
    let comparison = compare(history, &first, &second).await?;
    // The source answered "41", the first candidate "42", the second "40".
    assert!(comparison.first.recorded_verdict.as_ref().is_some_and(Verdict::is_reject));
    assert_eq!(comparison.first_verdict(), Some(&Verdict::score(1.0)?));
    assert!(comparison.second_verdict().is_some_and(Verdict::is_reject));
    Ok(())
}
```

| Part | In a scored evaluation |
|---|---|
| `recorded_verdict` | The verdict on the source end. The source run had no scorer, so the harness scores the recorded report. It does not read a verdict from history. |
| `candidate_verdict` | The verdict on the candidate end. |
| A failed end | No verdict. The scorer never sees it. |
| `scorer_usage` | The tokens of the judge calls. They are not in the candidate usage. It is zero for a scorer that calls no judge. |
| A judge | A live call to a model of the candidate harness, through `ScoreContext::chat`, as the candidate model is. The evaluation writes no ledger row. |
| The order | The harness scores the source before the first candidate model call. So a scorer that fails on the source costs no candidate call. A scorer that fails only on the candidate end fails after the candidate run. |
| A scorer error | `EvalError::Scorer` with the side: `Recorded` for the source end, `Candidate` for the candidate end. Its `usage` holds the tokens of each judge call before the error. The report of the candidate run is lost. An error is not a reject. `AgentHarness::score_timeout` bounds each score. |
| `compare` | Each evaluation holds both verdicts. `Comparison::first_verdict` and `second_verdict` give the verdict of each candidate. Two candidates with the same scorer and harness share one source verdict. The first evaluation holds its tokens. |
| No scorer | Each field is `None`, and the JSON has no new key. |

Each candidate turn is a live model call. `Candidate::max_turns` caps them.
The default cap is the recorded turn count plus `DEFAULT_EXTRA_TURNS`. The
report sets `turn_cap_reached` when the cap stops the candidate. Each engine
cycle replays the history so far, so the time of an evaluation grows with the
square of the run length.

The harness refuses an erased source, a source that has not ended, a source
that was cancelled or timed out, and a history that is not an agent run. It
also refuses an overlay that `agent::overlay::apply` refuses. A history with payload-store references or
encrypted payloads needs decoding first. The model call blocks in place, so
run the evaluation on a multi-thread Tokio runtime.

The evaluation is an in-memory fork at the first event. The database fork of
issue #2000 is a separate feature.

### Anonymise a pool

`eval::compare_anonymised` runs any number of candidates on one source
(issue #2376). A model that reads the report to pick a candidate then sees
no label and no input order. Harness² does the same before selection.

```rust
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::eval::anon::AnonId;
use autumn_harvest_agent::eval::{Candidate, compare_anonymised};
use autumn_harvest_agent::{AgentHarness, ProfileOverlay};

fn candidate_pool(
    harness_a: AgentHarness,
    v7: ProfileOverlay,
    harness_b: AgentHarness,
    v8: ProfileOverlay,
) -> [Candidate; 2] {
    [
        Candidate::new(harness_a).overlay(v7).label("triage@v7"),
        Candidate::new(harness_b).overlay(v8).label("triage@v8"),
    ]
}

// `judge` reads the text, for example with a model, and names one id.
async fn choose(
    history: &[WorkflowEvent],
    pool: &[Candidate],
    seed: u64,
    judge: impl AsyncFnOnce(&str) -> AnonId,
) -> Result<usize, Box<dyn std::error::Error>> {
    let report = compare_anonymised(history, pool, seed).await?;
    let pick = judge(&report.text).await;
    Ok(report.resolve(&pick).ok_or("not in the pool")?.index)
}
```

| Part | In an anonymised report |
|---|---|
| `AnonId` | `eval::anon::assign` gives the ids, as for an evidence bundle. The key of a candidate is `Candidate::digest`. An id is `c-` and a hex prefix of `SHA-256(seed, source run id, Candidate::digest)`. The prefix has at least 12 digits. It grows until no two ids are equal. `formal/verus/src/agent_anonymise.rs` proves the width rule. |
| The order | The pool lists the ids in order. The input order changes no id and no text. The candidates also run in pool order. |
| `text` | `AnonymisedPool::render`: the source end, then per id the counts, the end, each scorer verdict, each turn verdict, and each turn decision as JSON. It holds no model name, overlay digest, label or input index. |
| `pool` | The same content as JSON. Each `AnonymisedEvaluation` keeps every field of `Evaluation` except `overlay_digest` and `candidate_cost_usd_micros` (issue #2380). A price names a model. The recorded end is in the pool once. |
| `key` | Each id with its `CandidateRef`: the input index, the candidate digest, the overlay digest and the label. Keep it from the reader. `AnonymisedReport` does not implement `Serialize`, and its `Debug` form hides the key. |
| A failure text | Each run end error, report error and scorer reject reason replaces each label, digest, route and model id of the pool with `[redacted]`. A judge route of a scorer counts as a route. The source end gets the same treatment. Each route that a run asks for counts, with the model id behind it in each harness. So a route that a candidate keeps from the source, such as the model of a recorded peer, counts too. Only a whole name is replaced, so a short route such as `a` also replaces that word. |
| `Candidate::label` | An operator name, such as an overlay version id. It enters the digest, never the text. |
| `Candidate::digest` | The SHA-256 of the overlay digest, the label, the routes, the admitted models, the ceilings and the turn cap. It cannot read the harness. |
| Equal digests | `EvalError::DuplicateCandidate` with both input indices, before any model call. Give each candidate a label. The scorer is not in the digest, so two candidates that differ only in the scorer need labels too. |
| Scores | Each entry keeps its verdicts and `scorer_usage`. Each reject reason is redacted. Two candidates with the same scorer and harness share one source verdict, and only the first in pool order holds its tokens. So the scores can show which ids share a scorer or a harness. |

The text renders each model reply as written, so a reply can name its own
model. The pool keeps the token usage, and a tokenizer can show a model
family. A policy reason can show a harness setting, such as a timeout. Keep
the seed from the reader, because the seed and the digests give the ids.

### Write an evidence bundle

`eval::evidence::write_bundle` writes the evidence of a probe into one
workspace snapshot (issue #2377). A proposer or a composer then reads the
whole run from files. Its prompt holds only the short pointer in
`MISSION.md`, so no prompt cap cuts a long run.

```rust
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::eval::Evaluation;
use autumn_harvest_agent::eval::evidence::{Bundle, EvidenceWorkspace, read_bundle, write_bundle};
use autumn_harvest_agent::{Evidence, ProfileOverlay, WorkspaceStore};

async fn evidence(
    store: &WorkspaceStore,
    history: &[WorkflowEvent],
    first: Evaluation,
    second: Evaluation,
    current: &ProfileOverlay,
    random_seed: u64,
    version_id: &str,
) -> Result<(Evidence, Bundle), Box<dyn std::error::Error>> {
    let workspace = EvidenceWorkspace::new(store.clone(), "evidence:run-7", random_seed)
        .hide("fast-model")
        .hide(version_id);
    let written = write_bundle(&workspace, history, &[first, second], current).await?;
    let evidence = Evidence::Bundle(written.manifest.clone());
    let bundle = read_bundle(store, &written.manifest).await?;
    Ok((evidence, bundle))
}
```

| File | Content |
|---|---|
| `task.md` | The user message of the source run. |
| `rollouts/source.md` | The full source run: the task, each turn with its calls and decisions, each tool result and the end. |
| `rollouts/<id>.md` | The turns and the end of one candidate. |
| `diffs/<id>.md` | Each `TurnDiff` of one candidate against the source. |
| `overlay/current.json` | The overlay that the editor changes. |
| `flow_labels.json` | The flow labels of the source run. |
| `MISSION.md` | A short pointer to the other files. |

- A Markdown file over the part cap (`DEFAULT_PART_BYTES`, 256 KiB) splits
  into `<stem>.part-001.md` and on. A part ends at a line outside a fence
  when its window allows it. The parts joined in order equal the single
  text. Nothing is cut. `formal/verus/src/evidence_parts.rs` proves it.
- The writer saves the bundle with one put. A put that stops before its
  rename leaves no key, so no reader sees a partial bundle.
- An erased source gives `EvidenceError::Erased`. Decode payload-store
  references and encrypted payloads first, or the writer refuses them.
- `read_bundle` returns the files and the flow labels. Start a run that
  reads the bundle with those labels, as `AgentTask::flow_labels` does
  for a transcript.
- Each candidate has an anonymous id `c-<hex>` from `eval::anon`. The
  hex derives from the seed, the source run id and the overlay digest.
  Use a random seed for each bundle. The files and the manifest list the
  candidates in id order, so the input order does not show.
- `WrittenBundle::candidates` maps each id to its evaluation. Keep it
  from the editor. The manifest holds the ids only.
- An `Evaluation` names no model, so pass each candidate model name and
  overlay version id to `hide`. The writer masks them, their JSON-escaped
  form and each candidate digest in the Markdown files. It then checks
  each Markdown file, `MISSION.md` and `flow_labels.json`, and refuses a
  leak with `EvidenceError::Leak`. `overlay/current.json` goes in as it
  is, so it can name its own model. A candidate digest in it is a leak.
- Each recorded text sits in a fence that it cannot close. Each recorded
  name is a JSON string in a code span. So recorded data cannot forge a
  heading of the bundle.
- The bundle copies payloads of the source run. When you erase the
  source, erase the bundle scope with `WorkspaceStore::erase` too.
- `Evidence::Bundle(manifest)` passes a bundle to a proposer. Its
  `render` is the text of `MISSION.md`. `ModelProposer` has no workspace,
  so it refuses a bundle as a `Config` error.

The design is in [`DESIGN-2377.md`](design/DESIGN-2377.md).

## 13. The daemon example

`examples/claude-agent-daemon` speaks the Anthropic Messages API and replays
thinking blocks verbatim, which the provider-neutral `ChatMessage` cannot
carry. It therefore keeps its own turn activity. It takes three parts from
this crate: the approval signal names, the durable approval wait
(`approval::await_decision`), and the payload-cap checks.

`examples/autoresearch` runs `agent_loop` as a child workflow of each
experiment. It uses a mandate with a reviewer peer, a flow rule, approvals,
memory, delivery, a heartbeat with a precheck, the response cache and
`eval`. Its runs share a memory scope, so each run starts with the flow
labels of the runs before it. Its `edit_train` tool keys each edit on the
run, the step and the call id. Its `FileCacheIndex` implements `CacheIndex`
on disk, so a model call whose result a crash lost is not paid twice.

## 14. Agent mesh

A run can delegate a task to a peer agent. The peer runs as a sub-agent
inside the same workflow, with its own prompt, tools and budget. With
`parallel_children`, the delegations of one turn run at the same time, each
as a child run. They split the token remainder with `bounds::sibling_cap`.
A mandate bounds the delegation tree. Flow rules gate a dangerous sequence
of tool calls across the tree. Read [`agent-mesh.md`](agent-mesh.md).

## 15. Durable agents

An `Agent` (issue #2263) is a named, long-lived actor. It owns its state,
handles one message at a time, and survives a crash. It is the agent loop
inside a keyed entity (ADR 0006). The design is in
[`docs/design/DESIGN-2262.md`](design/DESIGN-2262.md).

| Agent concept | Harvest part |
|---|---|
| Address | `AgentId`, `<kind>/<name>`. It is the workflow id of the workflow `agent`. |
| Inbox | `AgentOp` values on the signal `harvest.entity.op` |
| Config | One `AgentProfile` for each kind, on the harness |
| One op | One segment of the agent loop, with the whole harness |
| State | `AgentState`: the transcript, the flow labels and the counters |
| Checkpoint | Continue-as-new. The state rides in the input. |

```rust
use std::sync::Arc;
use autumn_harvest_agent::{
    AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentState, Tool, sqlite,
};
use autumn_harvest_sqlite::SqliteRuntime;

async fn support(
    rt: &mut SqliteRuntime,
    model: Arc<dyn AgentModel>,
    lookup: Arc<dyn Tool>,
) -> Result<Option<AgentState>, Box<dyn std::error::Error>> {
    let harness = AgentHarness::new(model)
        .tool(lookup)
        .agent(AgentProfile::new("support").system("You answer support mail."));
    sqlite::register(rt, Arc::new(harness));

    let id = AgentId::new("support", "acct-42")?;
    let op = AgentOp::message_with_id("Where is my order?", "m-1");
    sqlite::send(rt, &id, &op, Some("m-1"))?;
    sqlite::drive(rt, &id).await?;
    Ok(sqlite::state(rt, &id).await?)
}
```

On Postgres, the `postgres` module has the same calls. A worker runs the
op, so wait on the state instead of a drive:

```rust
use std::time::Duration;
use autumn_harvest::worker::DbPool;
use autumn_harvest_agent::postgres::{self, PostgresAgents};
use autumn_harvest_agent::{AgentId, AgentOp, AgentState};

async fn support(
    pool: DbPool,
    id: &AgentId,
) -> Result<Option<AgentState>, Box<dyn std::error::Error>> {
    // `wait_for` returns `None` when `limit` passes first.
    let limit = Duration::from_secs(60);
    let pg = PostgresAgents::single(pool);
    let op = AgentOp::message_with_id("Where is my order?", "m-1");
    postgres::send(&pg, id, &op, Some("m-1")).await?;
    Ok(postgres::wait_for(&pg, id, limit, |state| state.segments >= 1).await?)
}
```

`postgres::send` sends a signal-with-start to the workflow `agent`. It
uses the agent id as the workflow id, `{}` as the start input, and
`Agent::payload(&op)` as the signal payload. The message id is the
idempotency key. An app without the module sends the same signal.

### The inbox

| Op | Effect |
|---|---|
| `user_message` | Runs one segment with the text as the user message. |
| `control: pause` | Holds each later message in the state. |
| `control: resume` | Runs the held messages in order, then takes new ones. |
| `control: reset_memory` | Clears the memory: the scope `agent:<id>`, or the state memory. It also removes each lesson of the kind that names the agent (section 24). |
| `control: clear_transcript` | Clears the transcript. Only the labels of the memory and of the workspace files stay (issues #2272, #2274). |
| `control: reset_workspace` | Deletes every workspace snapshot of the agent (issue #2274). |

A control op applies at once, also while the agent is paused. The kernel
`agent::inbox::admit` makes the choice. Verus proves it, see
`formal/verus/src/agent_inbox.rs`.

### Guarantees

- One op runs at a time. A second message waits for the first reply.
- An acknowledged message is never lost. A paused agent holds it, and runs
  it on resume. A full held queue (256 ops) refuses an op and names it in
  `AgentState::last_error`.
- Replay rebuilds the transcript. A restart asks no recorded model call
  again.
- A failed op keeps the transcript as it was. It keeps the flow labels and
  the tokens that its tools and turns used.
- Each segment gets the run id `<incarnation>:s<n>`. The incarnation is a
  recorded id of this life of the agent, so a delete and a new start never
  reuse a tool idempotency key. The state carries the tool-round counter, so
  approval names never repeat.
- The transcript keeps at most `AgentProfile::window` messages. The cut
  lands on a user message.

### Heartbeats and follow-ups (issue #2266)

A wake is an op in the inbox, so it never runs at the same time as a
message. `Entity::run_with_wakes` races the op signal against the next
deadline of the agent. A due wake runs before a waiting message.

| Wake | Config | Op |
|---|---|---|
| Heartbeat | `AgentProfile::heartbeat`, or `Control::SetHeartbeat` for one agent | `HeartbeatTick` |
| Follow-up | `AgentProfile::followups`, then the model calls `schedule_followup` | `FollowupDue` |

- A tick is unattended. It is read-only unless `AgentHeartbeat::allow_actions`
  is set. It can ask the harness precheck first. A silent `HEARTBEAT_OK`
  tick leaves the transcript as it was and delivers nothing.
- The next tick is due one interval after a tick ends. A late tick never
  piles up, and a tick that runs longer than its interval lets the waiting
  ops run first. `agent::wake::next_heartbeat` books it, and a tick always
  books the next one after the time it ran.
- `Control::SetHeartbeat` can only narrow the profile, because any client
  that can signal the agent can send it. A flag holds only when the
  profile heartbeat sets it. Each bound is at most the profile bound. The
  interval is at least `MIN_CUSTOM_HEARTBEAT_SECS` (60 s), or the profile
  interval when that is shorter.
- A follow-up is a deadline in the state. The chain count is state too, so
  `Followups::max_chain` holds across a checkpoint. A message from a person
  starts a new chain.
- A paused agent skips its ticks, and a booked follow-up waits. On resume,
  one due tick runs.
- `HeartbeatTick` and `FollowupDue` do not decode from a signal.
- A restart during a wait resumes the timer. A tick and its report happen
  once.

### Lifecycle (issue #2267)

**Idle.** An agent that waits for its inbox or a timer is a row in the
database. No worker must hold it in memory. A fresh worker, or a fresh
SQLite runtime, serves its next message and its next wake. With resident
workflows on, a worker can keep a waiting run as a cache. The cache is
bounded, and a worker can drop an entry at any time. See `resident.rs` in
the [module guide](architecture.md).

**Search attributes.** Each op upserts `agent_kind`, `agent_status`
(`running`, `waiting_approval`, `idle`, `paused` or `parked`),
`agent_last_active_at`, `agent_next_wake_at`, `agent_wake_reason`,
`agent_tenant`, `agent_mesh`, `agent_tokens_used` and
`agent_profile_version`. Times are epoch milliseconds. The `filter` grammar of `GET /workflows` lists by them. The
index `idx_harvest_we_agent_wake` serves a range on `agent_next_wake_at`,
so a list of the agents due to wake reads only those agents.

**Operate.** `harvest agents` composes management routes:

| Command | Route |
|---|---|
| `harvest agents list --status idle --wakes-before <ms>` | `GET /workflows?state=RUNNING&filter=workflow_name = 'agent' AND ...` |
| `harvest agents describe <kind>/<name>` | `GET /workflows/by-id/agent/{id}/query/harvest.entity.state` |
| `harvest agents pause <id> --reason <text>` | `POST /workflows/by-id/agent/{id}/signal/harvest.entity.op` |
| `harvest agents resume <id>` | the same route, with `resume` |
| `harvest agents erase <id>` | the signal route, then `POST /workflows/{exec}/erase-payloads` for each run and each scheduled pass |
| `harvest agents profile list <kind>` | `GET /agents/{kind}/profile/versions` |
| `harvest agents profile show <kind> [--version v]` | `GET /agents/{kind}/profile`, or `GET /agents/{kind}/profile/versions/{v}` |
| `harvest agents profile diff <kind> <a> <b>` | `GET /agents/{kind}/profile/diff?a=&b=` |
| `harvest agents profile activate <kind> <v> [--expected v\|none]` | `POST /agents/{kind}/profile/activate`, after the `list` route when `--expected` is absent |
| `harvest agents profile rollback <kind>` | `POST /agents/{kind}/profile/rollback` |
| `harvest agents profile pending <kind>` | `GET /workflows?state=RUNNING&filter=workflow_name = 'agent_refine' AND attrs.refine_kind = ... AND attrs.refine_status = 'waiting_approval'`, Postgres only |
| `harvest agents profile approve <run>` | `POST /workflows/{run}/signal/refine_approval:{run}` |
| `harvest agents profile reject <run> [--reason text]` | the same route, with `reject` |
| `harvest agents profile edit <run> --file overlay.json` | the same route, with `edit` |

**Profile overlays (issue #2374).** The `profile` commands use the routes
of the overlay store. Install the store with
`HarvestPlugin::agent_overlay_store`. Without it, each route is `404`. The
SQLite plugin does not serve these routes yet (epic #2466).

- `list` shows each version with its provenance and created time. The
  active version has `active: true`.
- `show` prints one overlay as JSON. `diff` prints only the parts that
  differ, each with its path and both values.
- `activate` is a compare-and-swap on the active version. Without
  `--expected`, the command reads the active version first. A `409` names
  the version that is active.
- `rollback` activates the version that was active before the current
  one. A second rollback undoes the first one. With no earlier
  activation, it exits non-zero and changes nothing. A move after its
  read, also a move away and back, makes it a `409`.
- `activate` and `rollback` need the `mutate` scope, as the
  `harvest-operator` role has. See
  [Custom roles](security-posture.md#custom-roles). A `read` role, or a
  read-only principal (`docs/operator-role.md`), gets `403`.
- Each activate and rollback request writes an audit row,
  `agent_profile.activate` or `agent_profile.rollback`. The row holds the
  kind, the operator id, and `from vN to vM`. A refused request writes a
  failed row. The audit export ships each row.
- The store logs each move with its actor, also a move from code. See
  [`docs/design/DESIGN-2374.md`](design/DESIGN-2374.md).

**Erase.** `harvest agents erase` erases an agent and prints one report.
It removes the history payloads, the entity state, the memory, the
workspace, the stream chunks and the cost ledger rows. It does not reach
a `TranscriptStore` (section 8a) or the response cache (section 11). The
app must clear those stores itself.

1. The agent gets `pause`, `reset_memory` and `reset_workspace`, then a
   delete. The pause holds new work and stops the ticks, so nothing writes
   the stores after the resets. The resets clear a `MemoryStore` scope, the
   episode scope and every workspace snapshot, with the scratch files of stopped puts. See
   "Scratch files" and "A save after a reset" below for the two windows
   that stay. An agent that ended gets a new run for them,
   through `POST /workflows/agent/signal-with-start` on the queue of its
   last run. The command waits for the run to end.
2. The command compares the failure count of the final state with the
   count before the resets. A new failure sets `stores_reset: false`, and
   `stores_error` names the error.
3. The command lists each run of each life of the agent with
   `filter=workflow_name = 'agent' AND workflow_id = '<id>'`.
4. It erases each run. The erasure tombstones the payloads of history, the
   entity state and its projection, and deletes the stream chunks and the
   cost ledger rows. It cascades to each child run of a fan-out, and to
   each child consolidation pass (issue #2454).
5. It lists each consolidation pass that a schedule started, with
   `filter=workflow_name = 'agent_consolidate' AND
   attrs.agent_consolidation_of = '<id>'`, and erases each one that ended.
   A pass that still runs is a failure in the report.
6. The report sums each count over every run and child:
   `events_scrubbed`, `stream_chunks_deleted`, `ledger_rows_deleted`,
   `entity_states_scrubbed` and the rest. `episodes_erased`,
   `facts_erased` and `lessons_removed` come from the final state. It
   lists each skipped child and each failure.

The spend of an erased run leaves the LLM budget and mesh budget sums.

### Mid-turn messages (issue #2265)

A message carries an `Urgency`. `sqlite::send` and `postgres::send` send the
extra signal first, then the op. Without the module, send the signal of
`Agent::urgency_signal(&op)` to the running run, then the op.

| Urgency | Signal | Effect |
|---|---|---|
| `queue` | none | The message runs after the current op. |
| `steer` | `agent.steer` | A running segment adds the text before its next model turn. The op is then a no-op. With no op running, the op runs the message once. |
| `interrupt` | `agent.interrupt` | The current op stops. The message runs next. |

What an interrupt does to the work in flight:

| In flight | Effect |
|---|---|
| A model turn | The race cancels it. The op ends as `interrupted`. |
| A tool call whose tool opts in (`Tool::interruptible`) | The race cancels it. The call answers that it was cancelled. |
| Any other tool call | It runs to its end. No later call of the round runs. |
| An approval wait | It ends as a rejection. The tool does not run. |

The race records its winner, so replay takes the same branch and asks no
model again. An interrupt that arrives while no op runs is stale, and the
next op drops it. On SQLite a running activity never sees a cancel: the
workflow takes the interrupt, and the activity result is dropped.

### Memory as state (issue #2273)

`AgentProfile::memory(AgentMemory::State)` keeps the memory blocks in
`AgentState::memory`. The workflow runs the `memory` tool itself. An edit is
part of the op, so it commits with the op snapshot. A crash replays the op,
and the edit lands once. No store and no edit key are involved.

- The system prompt of an op holds the memory as it was when the op
  started. An edit shows from the next op, so a provider prompt cache keeps
  its prefix.
- The memory lives in the entity snapshot events and in the
  `harvest_entity_state` projection. Erasure and codec rotation cover both
  (issue #2259).
- `AgentMemory::Store` keeps the `MemoryStore` of the harness for an agent
  that shares memory with other runs.

`autumn-harvest-agent/examples/agent_session_entity.rs` runs a chat session
as an agent.

### Progress notes (issue #2453)

`AgentProfile::context_reset` gives each op the
[context reset](#context-reset-issue-2453). The agent keeps its notes in
`AgentState::notes`, with the flow labels of the op that saved them.

- Each op shows the notes after its input, as a `save_progress` call and its
  result. `trim_window` drops old turns, but the notes stay.
- The transcript in the state holds no copy of the notes. The first turn of
  an op with notes therefore goes in full, not as a delta.
- An op that reads the notes takes their labels, so the taint follows the
  text.
- Only a segment that may write memory gets the reset and the
  `save_progress` tool. A read-only op, a heartbeat tick, a follow-up and a
  peer message get neither, unless the segment has
  `unattended_memory_writes`. Such a segment can read untrusted data, and a
  later op with write tools reads the notes. It still reads the notes.
- A silent tick or a failed op keeps no transcript, so it changes no notes
  either.
- `Control::ClearTranscript` clears the notes too.

### Episodes and consolidation (issue #2454)

`AgentProfile::episodes` keeps an episode log for each agent of a kind. It
is off by default. It needs `AgentMemory::Store` and
`AgentHarness::episodes`. The design is in `docs/design/DESIGN-2454.md`.

```rust
use std::sync::Arc;
use autumn_harvest_agent::{
    AgentHarness, AgentMemory, AgentModel, AgentProfile, EpisodeSettings, EpisodeStore, MemoryStore,
};

fn support(
    model: Arc<dyn AgentModel>,
    cheap_model: Arc<dyn AgentModel>,
    memory_store: Arc<dyn MemoryStore>,
    episode_store: Arc<dyn EpisodeStore>,
) -> AgentHarness {
    let profile = AgentProfile::new("support")
        .memory(AgentMemory::Store)
        .episodes(EpisodeSettings::default().every(10).max_episodes(200).model("cheap"));
    AgentHarness::new(model)
        .memory(memory_store)
        .episodes(episode_store)
        .model_named("cheap", cheap_model)
        .agent(profile)
}
```

- **Episode.** At the end of an op, the activity `agent_episode_append`
  appends one episode to the scope `agent:<id>:episodes`. It holds the run
  id, the answer cut to `max_text_bytes`, the stop, the recorded time and
  the flow labels of the agent. The key is `episode:<run id>`, so a retry
  or a crash appends once.
- **Which ops.** Only a segment that keeps its transcript and may write
  memory appends, as for progress notes. A silent tick, a failed op and a
  read-only op append nothing.
- **Bound.** The log keeps `max_episodes`. The oldest goes first. The next
  pass reports each unconsolidated episode that an eviction dropped.
- **Pass.** The workflow `agent_consolidate` reads at most
  `max_pass_episodes` unconsolidated episodes. A model that the profile
  mandate forbids disables the pass. It checks the LLM budget and the
  budget of the agent mesh (issue #2272). It then calls the model once, and records one cost ledger call under the pass
  run. Each valid fact becomes one entry of `facts_block`:
  `<subject> <relation> <object> [episodes: <id>, ...]`.
- **Labels.** A fact takes the labels of every episode of its pass. A
  memory store that cannot keep labels refuses a labelled fact, and the
  report counts it under `facts_refused`. The sources of a refused fact
  stay unconsolidated.
- **Trigger.** `EpisodeSettings::every(n)` runs a child pass at the `n`-th
  append since the last pass. The op waits for it. A failed pass therefore
  waits for `n` more appends, also when the log is full. A pass that
  does not write all its facts fails the op with a named error. With the
  default `ConsolidationTrigger::Schedule`, one engine schedule serves
  every agent (issue #2540). See "Scheduled passes" below. On SQLite,
  `sqlite::consolidate` or `postgres::consolidate` starts one pass.
- **Report.** A pass returns a `ConsolidationReport`. Its `stop` is
  `completed`, `nothing_to_do`, `budget_exceeded`, `invalid_reply`,
  `disabled` or `reset`. A refused budget and a reply that does not parse
  write no fact and mark no episode. SQLite has no run budget, so a pass
  there always passes the budget check.
- **Erase.** `Control::ResetMemory` clears the episode scope first, then the
  memory scope. `AgentState::episode_reset` counts the episodes and the
  derived facts. `harvest agents erase` reports them as `episodes_erased`
  and `facts_erased`. It also erases each pass that a schedule started, by
  the search attribute `agent_consolidation_of`. A pass that runs across
  the reset writes no fact after it and ends as `reset`. A pass that still
  runs is a failure in the report, so erase again after it ends.

The crate ships `InMemoryEpisodeStore`. A durable store implements
`EpisodeStore` and calls the methods of `EpisodeLog`, so the rules are the
same in every store.

#### Scheduled passes (issue #2540)

Register one schedule for the deployment. Each firing starts the
dispatcher `agent_consolidate_all`. The design is in
`docs/design/DESIGN-2540.md`.

```rust
use std::sync::Arc;
use std::time::Duration;
use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest::policy::Schedule;
use autumn_harvest::worker::DbPool;
use autumn_harvest_agent::consolidate;
use autumn_harvest_agent::consolidate_all::ConsolidateAll;
use autumn_harvest_agent::postgres::{self, Directory, PostgresAgents};
use autumn_harvest_agent::AgentHarness;

fn worker(pool: &DbPool, harness: AgentHarness) -> Result<HarvestBuilder, serde_json::Error> {
    let pg = PostgresAgents::single(pool.clone());
    let harness = harness.directory(Arc::new(Directory::new(pg)));
    let hourly = Schedule::Interval(Duration::from_secs(3_600));
    let passes = ConsolidateAll::new().max_passes(16);
    Ok(postgres::register(HarvestBuilder::new(), harness)
        .workflow_schedule(consolidate::schedule(hourly, &passes)?))
}
```

- **Agents.** The activity `agent_consolidate_due` reads each kind with
  `episodes` set and the trigger `Schedule`. It skips a kind whose pass
  cannot run, for example when the mandate forbids the pass model. It
  lists the running agents of each kind by the search attribute
  `agent_kind`, with the `AgentDirectory` of the harness.
  `postgres::Directory` and `sqlite::Directory` read the engine. With no
  directory, the read fails as `AgentDirectoryMissing`. The read takes 8
  agents at a time. A failed read of one agent counts as `unreadable`.
- **Idle agents.** The read loads the episode log of each agent. An agent
  with no pending episode gets no pass, so it costs no model call.
- **Running passes.** An agent whose pass still runs gets no second pass.
  The read finds the pass by `agent_consolidation_of`. The dispatcher waits
  for its passes, and the schedule runs one firing at a time.
- **Bound.** One firing starts at most `max_passes` passes: 16 by default,
  at most 256. The rest wait for the next firing.
- **Turn order.** An agent with no pass goes first. Then the agent whose
  last pass started first, then the oldest pending episode. So a pass that
  marks nothing, for example on a spent budget, sends its agent to the
  back, and a stuck agent cannot starve the others.
- **Report.** A firing returns a `DispatchReport` with the counts
  `listed`, `idle`, `running`, `due`, `unreadable`, `started`, `waiting`
  and `failed`, and one row for each pass. A failed pass does not stop
  the others.
- **Erase.** A dispatched pass sets `ConsolidateTask::dispatched`, so it
  publishes `agent_consolidation_of`. `harvest agents erase` erases it.
  The run of a firing keeps the agent ids and the end of each pass, but
  no episode text. Retention removes it.
- **Limits.** Each firing reads the episode log of each running agent of a
  scheduled kind, in one activity of at most 15 minutes. A pass that an
  app starts publishes its attribute at its first task, so a firing just
  before that task can start a second pass. A long pass delays the next
  firing for every agent. `postgres::Directory` reads every fleet of the
  database, so give each fleet of one database its own kind names.
- **Upgrade.** A deployment that registered the old per-agent schedule of
  `agent_consolidate` keeps it. Delete it, or that agent gets two passes.
- **Workers.** A worker with a hand-picked activity list must add
  `agent_consolidate_due` and the workflow `agent_consolidate_all`.

On SQLite, pass the same schedule to `SqliteRuntime::upsert_schedule`, and
install `sqlite::Directory::open(path)`. `sqlite::consolidate_all` and
`postgres::consolidate_all` start one firing.

### Recall eval (issue #2455)

The module `recall` measures what an agent forgets across ops. Each item
of a fixture states a fact in an early op and asks for it in its last op.
`recall::sweep` runs each item on its own agent of the config kind, one op
per session, and scores the last answer with `ExactMatch`. The report has
one row for each config and item. A failed item reads `error`, and the
other rows keep their scores.

`recall::profiles` gives four configs: `window`, `notes`, `memory` and
`episodes`.
`ReaderModel` answers only from its request, so CI proves that the eval sees
a loss. The `recall-eval` example runs the eval on Claude. The results are
in [`performance-agent-recall.md`](performance-agent-recall.md).

### Workspaces (issue #2274)

A workspace is a set of files, for example a repository checkout and notes.
It lasts across the ops of a durable agent and across restarts. Turn it on
for a kind, and install a store:

```rust
use std::sync::Arc;
use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest::sandbox::SandboxRuntime;
use autumn_harvest_agent::sandbox::SandboxTool;
use autumn_harvest_agent::{
    AgentHarness, AgentModel, AgentProfile, DirWorkspaceBlobs, WorkspaceStore,
};

fn coder(
    builder: &HarvestBuilder,
    model: Arc<dyn AgentModel>,
    sandbox: Arc<SandboxRuntime>,
) -> AgentHarness {
    let store = WorkspaceStore::new(
        Arc::new(DirWorkspaceBlobs::new("/var/lib/app/workspaces")),
        builder.payload_codecs().clone(),
    );
    AgentHarness::new(model)
        .tool(SandboxTool::new(sandbox).shared())
        .workspace(store)
        .agent(AgentProfile::new("coder").workspace())
}
```

- **Outside history.** A snapshot of the files lives in a `WorkspaceBlobs`
  store, the claim-check pattern. History records only its `WorkspaceRef`:
  a SHA-256 digest, a size, a file count and a generation.
- **Content-addressed.** The digest of the plain snapshot names its
  content. A load checks the digest, so a changed blob fails loudly.
- **Generations.** The key of a snapshot is its generation and its digest.
  The agent gives each segment and each reset a generation above the last
  one, and at least the recorded time. The first generation of a life is
  also above every key and reset marker in the scope. The recorded
  activity `agent_workspace_floor` reads it. So a new life after a delete
  starts above the old keys, whatever the clock of a worker reads.
- **Encrypted.** The payload codecs of the engine encode each snapshot. An
  active AEAD key therefore encrypts it at rest.
- **Per call.** A tool that opts in with `Tool::uses_workspace` gets the
  files in `ToolContext::workspace`. `SandboxTool` opts in: it mounts every
  file and asks for each one back, so a run keeps the files it changes. A
  new file joins when the run names it in `outputs`. A file that the run
  deletes leaves the workspace. So a file that the run moves joins only
  when the run names its new path. The tool-call activity
  saves a changed workspace after a call that succeeds, and returns the new
  reference with the result. A read-only run reads the files, but its
  changes are not saved.
- **Run again on a store failure.** The tool-call activity runs once. A
  workspace store that times out or fails with a retryable error is the one
  exception: the workflow runs the call again from the same head, up to
  three times. Only a tool that opts in touches the store. Its effect must
  stay inside the workspace, or be safe to repeat.
- **Fenced.** A reference becomes the head only when history records the
  result that names it. A stale attempt can write a blob, but no recorded
  result names it, so it never becomes the head.
- **Labelled.** The state keeps the flow labels of the run that last moved
  the head. The files can hold what that run read, so
  `Control::ClearTranscript` keeps those labels.
- A delegated child has no workspace. A durable peer has its own.

**Size.** A snapshot holds at most 64 MiB of file content and 10,000 files
by default. Set the caps with `WorkspaceStore::max_bytes` and `max_files`. A
call that grows the workspace past a cap gets a tool error, and the head
stays as it was. The sandbox policy also bounds the files that a run can
mount. A snapshot is JSON with base64 content, and the store base64-encodes
that JSON again inside the codec envelope. A blob is therefore about 1.8
times the size of its files. A 64 MiB snapshot takes about 114 MiB.

**Retention.** A workspace keeps one snapshot: its head. The end of each op
that moved the head deletes every other snapshot of the head generation or
older, the blobs of stale attempts included. `Control::ResetWorkspace`
deletes every snapshot below its own generation, and every key in another
form. It leaves an empty marker at its generation, so a later life starts
above it. `harvest agents erase` sends it before the delete. The engine can reclaim the lease of either
activity and start a new attempt, and the old attempt can still run after a
newer op. It then deletes only older keys, so it never deletes a newer head,
also when that head has the content of an old one.
`formal/tla/AgentWorkspaceRetention.tla` checks this. Keep the blobs in a
store for workspaces only, because retention deletes the blobs of a scope
that are not newer than its head.

**Scratch files (issue #2315).** `DirWorkspaceBlobs` writes each blob to a
scratch file, `<key>.tmp-<32 hex>`, then renames it. The put syncs the file
before the rename. It syncs the folder and its parent after the rename. Until one put succeeds, it syncs every ancestor folder. A
sync error fails the put. A crash, a save timeout
or a cancel can stop a put before the rename and leave that file. The store
lists scratch names. Retention and a reset delete one by the generation of
its key, with the same rule as for keys. So a late attempt never deletes the
scratch file of a live put of a newer op. No operation deletes a scratch
file by its age.

**A save after a reset.** A reset puts its marker before it lists the
scope. After a put, a save at a generation above 0 lists the scope. When it
finds a marker above its own generation, it deletes its own key and returns
a tool error. So a stale attempt that saves after the reset of an erase
deletes its own snapshot. This holds when each store call is atomic and
visible when it returns, as on a local folder. A save at generation 0, as
in a plain `AgentTask` workspace, has no order and skips the check. Each
save costs one more `list` call.

Two windows stay. Each needs a stale attempt that a crash, a timeout or a
cancel also stops:

- The put opens its scratch file after the reset lists the scope, and
  stops before its rename. The scratch file stays.
- The put renames its key after the reset lists the scope, and the save
  stops before its own check deletes that key. The whole snapshot stays.

What stays is deleted by a later reset or retention of the scope. After an
erase, no later op runs, so it stays on disk.

### Live tokens (issue #2268)

`AgentTask::stream` (or `AgentProfile::stream` for a durable agent) streams
the answer of each model turn in two layers:

| Layer | Sent by | Carries | Stored? | A reader that reconnects gets |
|---|---|---|---|---|
| Live | the model-turn activity, with `ActivityContext::publish_progress` | `LiveEvent::Start` and `LiveEvent::Delta` | no | nothing from before |
| Durable | the workflow, with `publish_durable_progress`, after the turn | one `TurnText` per turn | yes (Postgres only) | each finished turn once |

- A model sends live text from `AgentModel::chat_stream`. The default calls
  `chat` and sends no delta. The capstone example has a Claude adapter that
  streams.
- Small deltas join until 256 bytes wait, or until 50 ms pass and the
  provider sends its next event. A streaming adapter passes an empty piece
  at each event with no text, so text goes out while the model thinks.
- Each model call starts with `LiveEvent::Start`. A retry starts again, so a
  reader drops the text before it. On Postgres the activity envelope also
  carries the attempt. See
  [`streaming-progress.md`](streaming-progress.md#from-an-activity-issue-2268).
- `AgentHarness::live_sink` gets the live events on every backend, SQLite
  included.
- One app route gives both layers: a durable route with `.live().stream()`.
  See [durable routes](durable-routes.md#live-chunks).
- The live text is a preview. The `TurnText` and the run result are the
  answer.

### Profile overlays (issue #2372)

An activated overlay changes the next op only, because each op reads it
through a recorded activity and keeps that read to its end.

```rust
use std::path::Path;
use std::sync::Arc;
use autumn_harvest_agent::{AgentHarness, SqliteOverlayStore};

fn with_overlays(db: &Path, base: AgentHarness) -> Result<AgentHarness, Box<dyn std::error::Error>> {
    let store = Arc::new(SqliteOverlayStore::open(db, Arc::new(base.clone()))?);
    Ok(base.overlay_store(store))
}
```

- The run reads its profile through `agent_profile`. With a store, the
  result sets `overlay_store`, and each op that runs a segment also calls
  `agent_profile_overlay` with the run profile. That result holds the
  run profile with the overlay applied, and its `overlay_version`. Replay
  reads both results from history.
- The version must still validate under the current harness profile.
  Then it applies to the run profile under the run ceilings. The op takes
  only the fields that an overlay can set. The mandate, the peers and the heartbeat stay those of the
  run, so a deploy still changes the next run only.
- With no store, the op schedules no read. With no active version, the
  read records `overlay_version: None`, and the op runs on the run
  profile.
- A version that `overlay::apply` refuses fails the op. `last_error` names
  the version, and no model call runs. The agent never runs bare in its
  place. A deploy that removes a model or lowers a bound refuses an old
  version in this way.
- A heartbeat sets its own bounds and model for a tick. They take
  precedence over the overlay, as they do over the profile.
- `AgentState::profile_version` keeps the version of the last op that read
  one. Each op upserts it as `agent_profile_version`, for example `"v2"`.
  `filter=attrs.agent_profile_version = 'v2'` lists the agents whose last op
  ran on version 2.
- Install the store on every worker, and only after every worker runs
  this version. An older worker cannot replay a history that holds an op
  read. On a worker with no store, the op read fails and retries. See
  `docs/design/DESIGN-2372.md`.

### Tool guidance and skills (issue #2373)

A profile and a task can carry two text parts. A profile overlay can set
both. See `docs/design/DESIGN-2373.md`.

```rust
use autumn_harvest_agent::{AgentProfile, SkillText};

let profile = AgentProfile::new("support")
    .tool_guidance("lookup", "Look up an order by its id. Never guess an id.")
    .skill("refund", SkillText::new("Pay a refund.", "Check the order first. ..."));
```

- **Tool guidance.** The text replaces the description of an app tool in
  the definition that the model sees. The name, the schema, the effect and
  the policy stay. A built-in tool keeps its own text. `overlay::apply`
  refuses a key that names no app tool with `OverlayError::UnknownTool`.
- **Skills.** The system prompt lists each name and one-line summary under
  `## Skills`. It never holds a body. The model gets the `load_skill` tool,
  with `ToolEffect::ReadOnly`. A call is an ordinary `agent_tool_call`, so
  history records the body and replay reads it from there. The body is cut
  like any tool result.
- A run with no skills offers no `load_skill` tool. A call to it is an
  unknown tool. The harness never shows an app tool with that name.
- A skill name follows the agent name rules. A summary is one line of at
  most `MAX_SKILL_SUMMARY_CHARS` characters, with no control character. A
  body has at most `MAX_SKILL_BODY_BYTES`, and a map holds at most
  `MAX_SKILLS`. Each segment checks the skills first, so a bad skill fails
  the run or the op before any model call.
- The mandate tool allowlist does not need to name `load_skill`.
- `eval::evaluate` treats a `load_skill` call as any tool call. A recorded
  call takes its recorded body. A new call gets the stub and counts in
  `stubbed_tool_calls`.
- Upgrade every worker before a profile or a task carries skills. An old
  worker does not know the reserved name.
- A delegated child gets neither part. Its peer has its own prompt.

## 16. Propose an overlay (issue #2379)

The refine loop (epic #2369) needs candidate overlays. A proposer writes
one from the evidence of recorded runs. There are two dimensions:

- **INV** edits how the agent works, independent of the subject.
- **DOM** edits what a practitioner in the domain knows.

An app implements `OverlayProposer` for its provider, as it does
`AgentModel`. `ModelProposer` implements it over any `AgentModel`, with
the default INV and DOM templates. `ModelProposer::template` replaces one.
Install the proposer with `AgentHarness::proposer`.

```rust
use std::sync::Arc;
use autumn_harvest_agent::{AgentHarness, AgentModel, ModelProposer};

fn refining(client: Arc<dyn AgentModel>) -> AgentHarness {
    AgentHarness::new(Arc::clone(&client)).proposer(Arc::new(ModelProposer::new(client)))
}
```

The activity `agent_refine_propose` runs one stage. Its input is a
`ProposeRequest`: the dimension, the evidence, the base profile, the
current overlay, the ceilings of the run and the attempt count. The
current overlay must apply under the ceilings. The activity asks the
proposer once per attempt. The proposer reads the base profile and the
ceilings too. It checks each reply with
`overlay::apply` under the ceilings. It returns one `Stage`:

| Outcome | Meaning |
|---|---|
| `Proposed(overlay)` | A valid overlay that differs from `current`. |
| `KeptBase` | A valid overlay that gives the profile of `current`. It is not a candidate. |
| `FellBack { error }` | No attempt was valid. The stage inherits `current`. `error` is the last rejection. |

- `ProposalAttempts` is from 1 to 8. The default is 2.
  `RefinePolicy::proposal_attempts` (#2394) sets it.
- The next attempt gets the last rejection, so the model can fix it.
- A reply that does not decode, is above the ceilings or is above the
  result cap is invalid. So is an attempt over
  `AgentHarness::proposer_timeout` (at most 13 minutes).
- A retryable proposer error (`RateLimited`, `Transport`, `Unavailable`)
  fails the activity. The engine retries the whole stage. `Config`,
  `Authentication` and `Budget` fail it for good. Any other error spends
  one attempt.
- Each ceiling key is required in JSON. Only an explicit `null` sets no
  limit.
- The proposed text is model output. Treat it as untrusted until the
  adoption gate (#2381) accepts it.
- The retries run inside the activity. So one stage is one recorded
  result. A crash before the result runs the stage again from attempt 1.
  Replay reads the result and asks no proposer. On Postgres, a lost
  worker holds the stage until its 2-hour `start_to_close` ends.

The default templates adapt Harness² text under Apache-2.0. See
`autumn-harvest-agent/NOTICE`. The design is in
[`DESIGN-2379.md`](design/DESIGN-2379.md).

## 17. Score a run (issue #2441)

A `Scorer` checks one run and returns a `Verdict`. A verdict is
`Score(FiniteF64)` or `Reject(reason)`. A higher score is better. A score
comes from a verifier: a pure check, a test run or a judge model.
`ChatResponse` has no log-probabilities, so the crate does not use them.

Install each scorer under its id. A later scorer with the same id replaces
the earlier one.

```rust
use std::sync::Arc;
use autumn_harvest_agent::{AgentHarness, AgentModel, ExactMatch, JsonSchema};
use serde_json::Value;

fn scored(client: Arc<dyn AgentModel>, schema: Value) -> AgentHarness {
    AgentHarness::new(client)
        .scorer(Arc::new(ExactMatch::new("Paris")))
        .scorer(Arc::new(JsonSchema::new(schema)))
}
```

The activity `agent_score` takes a `ScoreRequest`: a scorer id and a
`ScoreInput`. The input holds the task input, the final answer, the stop
reason and the tool results of the run. `ScoreInput::from_run` builds it
from an `AgentTask` and its `AgentReport`. The activity returns the
verdict. History records it, so replay never calls the scorer again.

- `FiniteF64` refuses NaN and both infinities. It stores `-0.0` as `0.0`.
  `Verdict::score` refuses a non-finite value with `ErrorKind::Decode`, and
  that fails the activity for good.
- `Verdict::reject` cuts a reason to `MAX_REJECT_REASON_BYTES`. The
  activity cuts a longer reason and a long error message too.
- An unknown scorer id fails the activity as `ScorerMissing`. A retryable
  scorer error retries the activity with backoff.
- `AgentHarness::score_timeout` bounds one score, judge calls included. The
  default and the highest value is 13 minutes, below the 15-minute
  `start_to_close`. A score past it fails as a retryable `Unavailable`.
- A verdict encodes as `{"score": 0.5}` or `{"reject": "reason"}`.

**Built-in scorers.** `ExactMatch` compares the trimmed answer with the
trimmed expected text. Each built-in scorer checks one fixed target. For
a target per task, write an app scorer. `JsonSchema` reads the answer as JSON, with or
without a Markdown code fence, and checks it with the engine schema check.
That check covers a subset of JSON Schema: see
`autumn_harvest::info::validate_against_schema`. Each gives `Score(1.0)` on
a match. Each rejects a miss, and a run that did not stop as `completed`.
`with_id` renames one, so a harness can keep two.

**A judge scorer.** A judge is an app scorer that asks a model. It calls
`ScoreContext::chat` with a harness model name, not the model itself.
That call:

1. checks the LLM budget of the run (issue #1997), before the model call;
2. calls the model under `AgentHarness::model_timeout`;
3. keeps one cost ledger call (issue #1996).

The activity records each ledger call under the run that scores, so a
strategy run owns the tokens of its judge. A budget refusal fails the
activity with `LlmBudgetExceeded`, also when the scorer hides the error.
SQLite has no run budget, so there the check passes.

A failed attempt writes no ledger row on either backend. So an error after
a paid judge call fails the activity for good, and no retry pays again
unseen. Return a reject for a judge reply that does not parse. The budget
sums committed rows only, so the judge calls of one attempt see the spend
from before that attempt.

A judge reads untrusted text: the answer of the run. Treat its reply as
data. Clamp a judge score to a fixed range, so one reply such as `1e308`
cannot rule `select`. Do not put a reject reason back into a prompt as an
instruction. `ScoreContext::unmetered` builds a context with no budget
check and no ledger record, for a test of a scorer.

**Select a draw.** `select(&[(DrawId, Verdict)])` picks the draw with the
highest score. A tie goes to the lowest draw id. A reject never beats a
score. The result is `None` only when every verdict is a reject, or when
the list is empty. The order of the list does not change the result.

`select` maps each score to an `i64` order key, the key of
`f64::total_cmp`. The kernel `best_draw` then works on integers.
`formal/verus/src/agent_select.rs` proves its three properties. It also
proves that the key keeps the order of the bits. A proptest checks that the
key keeps the order of the scores. The design is in
[`DESIGN-2441.md`](design/DESIGN-2441.md).

## 18. The refine policy (issue #2394)

An operator decides what the refine loop may do. Each agent kind has a
`RefinePolicy`. The overlay store holds it, next to the overlay versions.
A kind with no stored policy uses every default. The defaults never let
an overlay go live without a person. A change takes effect at the next
read, with no restart: the next op, or the next refine run.

Today the store enforces `ceilings`, `auto_adopt.parts` and
`auto_adopt.max_per_day`. The refine workflow of #2380 reads `ceilings`,
`probe_effects` and `proposal_attempts` (section 22). The adoption gate
reads `auto_adopt` and `approval_timeout` (section 23). The lesson library
reads `lessons.max_entries` at each write (section 24).

| Setting | Meaning | Default | Range |
|---|---|---|---|
| `auto_adopt.parts` | The overlay parts that may go live without a person: `system`, `guardrails`, `tool_guidance`, `skills`, `model`, `bounds` (the three `max_*` fields), `read_only`. | `[]`, so every overlay waits | Any subset |
| `auto_adopt.max_per_day` | The cap of auto adoptions in one UTC day. | `1` | `0` to `1000`, or `"unlimited"` |
| `approval_timeout` | How long the adoption gate waits. | `"7d"` | `"1m"` to `"90d"`, as `<n><s\|m\|h\|d>` |
| `probe_effects` | The `ToolEffect`s that a live probe may run. | `["read_only"]` | Any subset of `read_only`, `internal`, `write`, `external` |
| `ceilings.max_steps`, `ceilings.max_total_tokens`, `ceilings.max_output_tokens` | The highest bound that an overlay may set. `null` is the profile value. | `null` each, so an overlay can only narrow | `1` or more |
| `ceilings.models` | The models that an overlay may name. `null` is the profile model. | `null` | 1 to 64 names |
| `proposal_attempts` | The attempts of each proposer stage before it inherits. | `2` | `1` to `8` |
| `lessons.max_entries` | The size of the lesson library. | `64` | `1` to `4096` |

```sh
harvest agents profile policy support show
harvest agents profile policy support set --auto-adopt-parts system --max-per-day 3
harvest agents profile policy support set --max-steps 40 --probe-effects read_only,internal
harvest agents profile policy support history
harvest agents profile policy support reset
```

- **Who sets it.** Only an operator. The store refuses a refine or an
  auto actor. `PUT /agents/{kind}/profile/policy` and
  `POST .../policy/reset` are `Mutating`, so a read role gets `403`.
- **Compare-and-swap.** A set sends `expected_revision`. A stale one is a
  `409`. `set` with flags reads the policy, changes only the named keys
  and sends the revision that it read.
- **Audit.** Each set and reset adds one row to the store log, with the
  old and the new policy. `GET .../policy/changes` lists them. Each one
  also writes an `agent_profile.policy_set` or
  `agent_profile.policy_reset` audit row. `error_summary` names each
  changed key with its old and new value. The settings that widen the gate
  come first. A list shows the names that it adds and removes. A summary
  over 512 bytes ends with a mark that names its revision.
- **No self-widening.** A `ProfileOverlay` and an `AgentOp` refuse each
  policy key when they decode. A `ProposeRequest` refuses each one except
  `ceilings`, which holds the resolved ceilings of the recorded read. An
  unknown key in a policy also fails, so an old worker never drops a new
  setting.
- **One read per run.** A refine run calls `read_refine_policy` once. The
  activity `agent_refine_policy` records the policy. A replay reads the
  record. A change during the run never widens that run. A narrowing
  reaches its adoption at once, because the store also checks its current
  policy.
- **Ceilings.** The store validator and the op read of #2372 apply each
  overlay under `policy.ceilings.resolve(profile)`. The op read takes the
  policy in the same recorded activity. A raised ceiling lets an overlay
  pass the profile bound. A lowered one fails the next op of a version
  that no longer fits, and no model runs.
- **Auto adoption.** `ProfileOverlayStore::auto_adopt` enforces the
  gate. A candidate goes live only when each changed part is in
  `auto_adopt.parts` of the run policy and of the current stored policy,
  and the stricter daily cap allows it. One transaction puts the version
  as `auto:<run>`, counts the auto moves of the UTC day of the store clock
  and moves the pointer. Set the clock with `with_clock`. `put`,
  `activate` and `rollback` refuse an `auto` actor. `refine::adopt` wraps
  the call. Otherwise the candidate is put as `refine:<run>` and waits. A
  wait on the cap writes a `harvest.audit` tracing event that names the
  cap. An unchanged candidate stores nothing.

| `refine_status` | Meaning |
|---|---|
| `auto_adopted` | The candidate went live without a person. |
| `awaiting_approval` | The candidate waits, with the reason. |
| `unchanged` | The candidate changes no part. |
| `timed_out` | The gate of #2381 waited for `approval_timeout`. It will use `RefinePolicy::gate_state`. |

- **Probes.** `eval::live_probe` takes the set of `probe_effects` (issue
  #2378). The tool-call activity of the probe refuses any other effect. A
  policy cannot change that. `RefinePolicy::probe_rules()` returns the same
  set as `ToolRules`, for an app that also wants it in the policy. An
  unknown tool is denied.
- **Rollback.** `rollback` after an auto adoption restores the version
  before it, as after any activation.

`formal/verus/src/refine_policy.rs` proves that the default never adopts
and that an empty change never adopts. `formal/verus/src/overlay_log.rs`
proves that the auto moves of a day never pass the cap. The design is in [`DESIGN-2394.md`](design/DESIGN-2394.md). The rule is in
[ADR 0014](adr/0014-operator-refine-policy.md).

## 19. Sampling policy (issue #2439)

Each strategy of epic #2438 runs more than one draw. A draw can call
tools. An operator sets the limits of each agent kind in one
`SamplingPolicy`. A missing policy means every default, and each default
is the safe value.

| Setting | Meaning | Default |
|---|---|---|
| `strategies` | The strategies that the kind may use: `best_of_n`, `rejection`, `vote`, `cascade`, `beam` | none, so each strategy run is refused and each run is a single draw |
| `max_draws` | The most draws of one strategy run, across all rounds. 1 to 256. | 1 |
| `max_concurrent_draws` | The most draws that run at the same time. 1 to `MAX_PARALLEL_CHILDREN` (16). | 1 |
| `draw_effects` | The `ToolEffect`s that a draw may run | `[read_only]` |
| `scorers` | The scorer ids that the kind may use: `deterministic`, or `{"only": [ids]}` | `deterministic`: each scorer that calls no model |
| `judge_models` | The models that a judge scorer may call | none |
| `max_answer_retries` | The cap of the answer check (section 21). 0 to 16. | 0 |

An unknown key fails the decode. So an older worker never drops a setting
that a newer worker recorded. The run of that older worker fails closed,
with the non-retryable error `SamplingPolicyInvalid`.

### Who sets it

Only an operator. The routes are `PUT` and `DELETE`
`/admin/agents/{kind}/sampling-policy`, and both need the operator role.
Without it, the call returns 403 and nothing changes. `GET` reads the
policy and its newest audit rows.

```sh
harvest agents sampling policy researcher show
harvest agents sampling policy researcher set --strategies best_of_n --max-draws 4 --max-concurrent-draws 2
harvest agents sampling policy researcher reset
```

`set` replaces the whole policy. A setting that you leave out takes its
default. Each set and reset writes a row to `harvest_agent_policy_audit`
with the old and new policy, in the transaction of the change. The HTTP
call also writes a `harvest_audit_log` row.

Install a store with `AgentHarness::sampling_policies`. The stores are
`InMemorySamplingPolicyStore`, `SqliteSamplingPolicyStore` (feature
`sqlite`) and `PostgresSamplingPolicyStore` (feature `postgres`). Each
read sends one query, so a change reaches the next strategy run of every
process, with no restart. A harness with no store reads every default.

### What a strategy run does

`agent_draws` takes a `DrawsInput`: the kind, the input text, the
strategy, the draw count, an optional concurrency and an optional scorer.
It holds no task.

1. It reads the `AgentProfile` of the kind, in the recorded activity
   `agent_profile`. A kind with no profile is refused. The profile builds
   the task of each draw, so a caller cannot borrow the policy of a kind
   for a task of its own.
2. It reads the policy once, in the recorded activity
   `agent_sampling_policy`. Replay reads the record, so a change during
   the run does not reach it.
3. `sampling::admit` checks the strategy, the draw count, the
   concurrency, the scorer and the judge model, in that order. A request
   over the policy fails with `sampling refused (<code>)` before any model
   call. The codes are the variants of `sampling::Refusal`.
4. It runs the draws as child `agent_loop` runs, in waves of at most the
   admitted concurrency. A wave starts when the wave before it ends.

Each draw sets `AgentTask::allowed_effects` to `draw_effects` met with
the effects of its profile. The decision phase denies a tool of any other
effect, and the tool-call activity checks it again. So a draw never gets
wider effects than a single run of its kind.

- A draw is read-only unless its effects hold `write` or `external`.
- An inline child of a draw keeps the effect set. A delegation from a draw
  to a durable peer is refused.
- A draw runs its delegations one at a time, so the open model calls stay
  within the concurrency of the run.
- A draw has no memory, no workspace, no follow-ups and no delivery. It
  has no context reset unless `internal` is allowed, because
  `save_progress` is `internal`.
- Each wave is one `run_draws` batch (section 11). Draw `n` has the draw
  id `n`, so N draws are N samples, not one cached answer.

`agent_draws` returns each draw report. An engine failure of a draw fails
the run. `agent_sample` (section 20) scores the draws and selects one.

### Upgrade every worker first

A worker that predates `allowed_effects` drops it. The read-only fallback
stops a write on such a worker, but not an `internal` tool. Upgrade every
worker before you give a kind a strategy. Do not roll back past this
release while a strategy run is in flight.

### A run never carries a policy

`AgentTask`, `AgentOp` and `DrawsInput` fail to decode when they carry a
key from `sampling::SAMPLING_POLICY_FIELDS`. `AgentOp` also refuses each
refine policy key (section 18). Any other unknown key of `AgentTask` and
`AgentOp` still decodes, as before. So a client cannot widen its own
policy.

The design is in [`DESIGN-2439.md`](design/DESIGN-2439.md).

## 20. Best-of-N and rejection sampling (issue #2443)

`agent_sample` runs a task many times, scores each draw and keeps the
best. Each draw is a child `agent_loop` run with the run id `{root}:s{n}`.
History records the end of each draw and each verdict. So a crash costs
only the draws in flight, and replay never calls a scorer again.

```rust
use autumn_harvest_agent::ExactMatch;
use autumn_harvest_agent::sampling::{SampleInput, SampleStrategy, ScorerRef};

// The policy of `researcher` must list `best_of_n`, with `max_draws >= 3`.

let input = SampleInput::new(
    "researcher",
    "Name a prime.",
    SampleStrategy::best_of_n(3),
    ScorerRef::deterministic(ExactMatch::ID).unwrap(),
)
.max_total_tokens(20_000);
// SQLite: autumn_harvest_agent::sqlite::start_sample(&mut rt, &input)
```

| Strategy | What it does |
|---|---|
| `best_of_n { n }` | Runs `n` draws, scores each, and selects with `select` (section 17). A tie goes to the lowest draw id. |
| `rejection { max_attempts, threshold }` | Runs draws until one scores at or above `threshold`, up to `max_attempts` draws. It runs one draw at a time unless the input asks for more. It stops after the wave that holds the pass. |

The run reads the profile and the sampling policy of the kind once each,
as `agent_draws` does (section 19). The policy admits the strategy, the
draw count (`n` or `max_attempts`), the concurrency and the scorer. Each
draw gets `draw_effects`, so with the default `[read_only]` a draw that
calls a `write` tool gets a refusal, and the tool body never runs.

### The budget

The draws share one token budget: the `max_total_tokens` of the profile,
narrowed by the `max_total_tokens` of the input. The run starts each wave
with the tokens that are left. `sample_step` keeps a wave no wider than
the tokens left, and draw `i` gets the cap `sibling_cap(left, width, i)`.
The spend of each draw and of its judge calls counts.

The run starts no more draws when no token is left, when a draw ends
`budget_exceeded`, or when the LLM budget refuses a judge call. It then
selects among the draws that it has, and sets `stopped` to
`budget_exceeded`. The loop checks a token cap between model calls. So
each draw of the last wave can pass its share by one call.

A failed score does not fail the run. The draw gets a reject that names
the error, the run starts no more draws, and `stopped` is
`score_failed`.

### Scores and judges

The `agent_sample_score` activity scores each draw. It is `agent_score`,
but it also returns the tokens and the cost of the judge calls. A draw
that failed, or that a budget stopped, gets a reject and no score call.
The run cuts each tool result of a score input to 16 KiB.

A judge call may name only the judge model of the input `ScorerRef`,
which the policy admitted. So the scorer must name that model, and `None`
fails. A call to any other model fails with `ErrorKind::Config`, and the
model is not called.

### The report

`SampleReport` holds:

- `outcome`: `selected { draw }`, or `no_accepted_draw` when no draw
  passed;
- `selected`: the `AgentReport` of the selected draw;
- `stopped`: `completed`, `accepted` (a rejection run found a pass),
  `budget_exceeded`, `score_failed` or `rounds_exhausted` (a beam run
  ended its last round with a live beam);
- `attempts`: the draws that ran;
- `draws`: for each draw, its stop, verdict, usage and cost. The usage and
  the cost include the judge calls of the draw;
- `usage` and `cost_usd_micros`: the totals. They equal the cost ledger
  rows of the run tree.

Each `ModelTurn` carries the cost that the ledger recorded for it, and
`AgentReport::cost_usd_micros` sums it, as `usage` sums the tokens.

The design is in [`DESIGN-2443.md`](design/DESIGN-2443.md).

### Beam search

Best-of-N scores whole runs. Beam search scores each model turn, so a
path that goes wrong at step 2 stops there.

```rust
use autumn_harvest_agent::sampling::{SampleInput, SampleStrategy, ScorerRef};

// The policy of `researcher` must list `beam`, with
// `max_draws >= width × expansion × max_rounds`, here 12.

let input = SampleInput::new(
    "researcher",
    "Find the path.",
    SampleStrategy::beam(2, 2, 3),
    ScorerRef::deterministic("path_score").unwrap(),
);
// SQLite: autumn_harvest_agent::sqlite::start_sample(&mut rt, &input)
```

| Field | What it does |
|---|---|
| `width` | The most beams that a round keeps. The run stops when `width` beams are complete. |
| `expansion` | The model turns that each live beam gets in a round. |
| `max_rounds` | The most rounds. |

A beam is a transcript. Round 0 has one beam: the user message. Each
round expands each live beam `expansion` times. Each expansion is a step:
a child `agent_loop` run with `AgentTask::beam_step` set. A step sends
its beam as `history`, asks for one model turn, and runs the tool round
that the turn asks for. A tool round ends the step with `step_ended`. A
final answer ends it with `completed`. So a kept beam pays for its new
turn only. Its prefix is data in the request, not a new model call.

Each step has its own draw id, so two steps of one beam never share a
cached answer. Each step gets `draw_effects`, as a draw does. Each step
also gets the flow labels of its beam, so a flow rule sees the labels of
each earlier round.

The scorer of the input is the step scorer. `agent_sample_score` scores
each step. The score input holds the whole transcript of the beam, and
its `stop` is `step_ended` until the beam is complete. `ExactMatch` and
`JsonSchema` give a `step_ended` step the score 0, so a beam keeps it
below each passed answer. A step that ends with another stop gets a
reject and no score call.

A step continues its beam, so it also continues the step count. A beam of
round `r` holds `r` tool rounds, so its step may run `max_steps - r` more.
With none left, a step that asks for a tool ends `steps_exhausted`. So a
path never runs more tool rounds than one run of the profile. In the same
way, the mandate of a step allows only the delegations that its beam has
not used.

After each round, `beam_rank` keeps the best steps: at most `width` less
the complete beams. It calls `select` once per kept step, so a tie goes
to the lowest draw id, and a reject is never kept. A kept step with a
final answer is complete and leaves the pool. Each other kept step is
live in the next round, in rank order.

The run stops with `completed` when `width` beams are complete, or when
no beam is live. It stops with `rounds_exhausted` after `max_rounds` with
a live beam.
The outcome is `select` over the complete beams, and `selected` holds the
report of the best one. Its transcript is the whole path, but its usage
covers its last step only. The totals are on `SampleReport`. With no
complete beam, the outcome is `no_accepted_draw`.

Each draw record holds `beam`: its `round`, its `parent` step and its
`fate` (`live`, `complete` or `pruned`). So the report holds the verdicts
of each round.

The policy admits `width × expansion × max_rounds` as the draw count,
before the first model call. The product saturates, so an overflow is
refused as `too_many_draws`. The budget and the stops of best-of-N apply
to each round.

Known limits:

- Each step sends its whole beam as input tokens. The loop checks a token
  cap between model calls, and a step makes one call. So a step can pass
  its share of the budget by the size of its beam.
- A step that delegates also runs the model calls of its child. The draw
  count bounds the turns of the steps only.
- The loop guard of a step sees only its own turn. `max_rounds` bounds a
  loop across rounds.
- A step has no context reset, because a reset rewrites the beam.

The design is in [`DESIGN-2446.md`](design/DESIGN-2446.md).

## 21. Answer check (issue #2442)

An `AnswerCheck` scores each final answer of a run in the same run. A bad
answer does not need a second run, so the run does not pay for its
transcript again.

```rust
use autumn_harvest_agent::{AgentTask, AnswerCheck};
use autumn_harvest_agent::sampling::ScorerRef;

let scorer = ScorerRef::deterministic("json_schema").expect("a valid id");
let task = AgentTask::new("Give the order as JSON.")
    .answer_check(AnswerCheck::new("support", scorer).retries(2));
```

The check names three things:

- `kind`: an agent kind with a registered `AgentProfile`. Its
  `SamplingPolicy` (section 19) caps the check.
- `scorer`: a `ScorerRef` (section 17). A judge names its model.
- `retries`: the rejected answers after which the model may answer again.
  0 by default.

### Admission

Before the first model call, the recorded activity `agent_answer_check`
admits the check. The checks go in this order:

1. The kind is a valid name (`invalid_kind`).
2. The kind has a profile (`unknown_kind`).
3. A profile with a tenant belongs to the tenant of the run
   (`other_tenant`). On Postgres, the run tenant that the engine keeps
   applies, not `AgentTask::tenant`. So a run with no tenant cannot use a
   profile with a tenant. SQLite keeps no run tenant, so there
   `AgentTask::tenant` applies.
4. `retries` is at most `max_answer_retries` (`too_many_answer_retries`).
5. The policy allows the scorer and its judge model
   (`scorer_not_allowed`, `judge_model_not_allowed`).
6. The harness has the scorer (`unknown_scorer`).

A refused check fails the run with `sampling refused (<code>)`. No model
call runs. With `report_failure`, the run ends as `failed`. Replay reads
the recorded admission, so a policy change during a run does not reach it.

A task can name each kind of its own tenant. The kind sets the cap, the
scorers and the judge models, not the profile of the run.

### The loop

1. The loop scores each answer that ends a segment as `completed`, in the
   recorded activity `agent_score`. It does not score an answer that ends
   as `tokens_exhausted` or `output_capped`.
2. The score input holds `AgentTask::input`, the answer and the tool
   results of the run. In a follow-up segment, the input is still the task
   input.
3. A score ends the segment as `completed`.
4. A reject adds its reason to `AgentReport.answer_rejections`. While the
   rejections are at most `retries`, the loop adds a user message with the
   reason. Then it asks the model again in the same run.
5. After `retries + 1` rejected answers, the run ends as `answer_rejected`.

The cap counts each rejection of the run, follow-ups included. A context
reset keeps the retry messages of the segment, but not the rejected
answers. A delegated child has no check. Each
draw of `run_draws` with a check admits and checks its own answers.

### Bounds and cost

A retry is one more model call of the same run. A retry is not a tool
round, so `max_steps` does not count it. `max_total_tokens` and the LLM
budgets count it. A retry past a bound ends the run under that bound.

A judge call goes to the cost ledger and the LLM budget (section 17).
`max_total_tokens` and `AgentReport.usage` do not count judge tokens. A
judge call that the LLM budget refuses ends the run as `budget_exceeded`.

A judge call may name only the judge model of the `ScorerRef`, which the
policy admitted, as in section 20. So the scorer must name that model, and
`None` fails. With no judge model, each judge call fails. A failed call
returns `ErrorKind::Config`, and the model is not called.

### What the report and the delivery hold

- Under `answer_rejected`, `AgentReport.text` holds the last rejected
  answer. The delivery sends only the notice "The agent stopped early:
  answer_rejected.".
- A bound that ends the run after a retry does not report the rejected
  answer as the text.
- The transcript keeps each rejected answer and each retry message.
- With `stream`, the durable text of a final answer goes out only after
  the answer passes the check. A reader that reconnects never reads a
  rejected answer. The live token stream still shows each answer while it
  streams.

### Safety and size

The retry message puts the reason between `<reason>` tags. The loop
replaces each `<` and `>` of the reason, so the reason cannot close the
tag. A reason from a judge can still quote the answer. Keep a reason short
and plain.

A score request over the request cap drops its tool results. A request
that is still too large ends the run as `transcript_full`.

### Compatibility

`AgentTask` does not write `answer_check` when it is `None`. A task from
before the field decodes with no check. Its run records no new event, so
it replays unchanged. A worker from before this release ignores the field
and runs with no check. Upgrade every worker before a task sets it. A
decoder from before this release cannot read the stop `answer_rejected`.

`formal/verus/src/agent_sampling.rs` proves the integer kernels and a spec
model of the loop. With every answer rejected, a run with `retries = c`
asks for exactly `c + 1` answers. The proptest
`the_answers_of_a_run_never_pass_the_cap` links the model to the engine
kernels. The design is in [`DESIGN-2442.md`](design/DESIGN-2442.md).

## 22. Refine a kind (issue #2380)

The workflow `agent_refine` runs Propose, Probe and Compose over recorded
runs of one kind. It needs the `eval` feature. It never runs on a live op.
The design is in [`DESIGN-2380.md`](design/DESIGN-2380.md).

```rust
use std::path::Path;
use std::sync::Arc;
use autumn_harvest_agent::refine::{OverlayComposer, RefineInput, RefineMode};
use autumn_harvest_agent::{
    AgentHarness, AgentModel, AgentProfile, MeshBudget, OverlayProposer, ProfileOverlayStore,
    sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, SqliteRuntime};

fn refine(
    model: Arc<dyn AgentModel>,
    profile: AgentProfile,
    store: Arc<dyn ProfileOverlayStore>,
    proposer: Arc<dyn OverlayProposer>,
    composer: Arc<dyn OverlayComposer>,
    db: &Path,
    run: ExecutionId,
    seed: u64,
) -> Result<ExecutionId, Box<dyn std::error::Error>> {
    let harness = AgentHarness::new(model)
        .agent(profile)
        .overlay_store(store)
        .proposer(proposer)
        .composer(composer)
        .refine_sources(Arc::new(sqlite::Sources::open(db)));
    let mut rt = SqliteRuntime::open(db)?;
    sqlite::register(&mut rt, Arc::new(harness));
    let input = RefineInput::new("support", vec![run], 2, RefineMode::Parallel, seed)
        .budget(MeshBudget::daily("refine").max_tokens(200_000));
    Ok(sqlite::start_refine(&mut rt, &input)?)
}
```

On Postgres, install `postgres::Sources::new(pg)` and start the workflow
`agent_refine` with the same input.

| Input | Meaning | Range |
|---|---|---|
| `kind` | The kind to refine. | An agent name |
| `source_runs` | Ended `agent_loop` runs of the kind. | 1 to 16, each once |
| `k` | The width of each round. | 1 to 8 |
| `mode` | `parallel` or `sequential`. | |
| `seed` | The seed of the opaque ids and of each stage. | Any `u64` |
| `budget` | A `MeshBudget` cap on the spend of the run. The run compares its own spend with the caps. The period does not apply. | Optional |

- **Start.** `agent_refine_start` checks each source before any model
  call. A bad source fails the run with a named error:
  `RefineSourceNotFound`, `RefineSourceNotAgentRun`,
  `RefineSourceInFlight`, `RefineSourceOtherKind`,
  `RefineSourceNotReplayable` or `RefineSourceOverCeilings`. The last one
  means that the source ran with a bound above the policy ceilings. Each
  stage of a source checks its replies against the profile of that
  source, as its probe does. A reply must also apply to each other source,
  so the run can carry it to a later source. It must apply to the
  registered profile of the kind too, so the kind can take the chosen
  overlay. Call `AgentTask::kind` when you start an
  `agent_loop` run, so the run names its kind. A run with no kind gets
  `RefineSourceOtherKind`, and so does each run recorded before this
  release. A durable `agent` run has no replay probe yet, so it gets
  `RefineSourceNotReplayable`.
- **Parallel.** For each source, k INV and k DOM stages start from one
  base. Each candidate gets one probe. k compositions read the shared
  probes. Each candidate composition gets one probe. One selection
  follows.
- **Sequential.** For each source, k steps. Each step runs INV, DOM, their
  probes, one composition and its probe. A candidate composition is the
  base of the next step.
- **Probe.** `eval::evaluate` against the recorded source, so the base
  rollout costs nothing. A probe that stubs a tool call asks for a live
  re-probe with `probe_effects`. The fork applies the overlay under the
  policy ceilings, as the stages do. SQLite has no fork, so the probe records
  `live re-probe skipped: SQLite has no fork` in the stage notes.
- **Compose and select.** An `OverlayComposer` reads the probes under
  opaque ids, with the anonymised pool of issue #2376. The pool redacts
  each route that a probe asked for, with its model id. So a route that a
  probe inherits from its source task, or the route of a recorded peer, is
  redacted too. With a workspace store, the composer also reads an
  evidence bundle, which hides the same names. An `OverlaySelector` picks
  one id. The rule picks in its place when no selector is installed, or when
  the selector fails, times out or names an id outside the pool. The rule
  takes a completed end first, then the fewest stubbed tool calls, then
  the lowest id.
- **Budget identity.** For each source, both modes run 3k sessions. Each
  candidate stage adds one probe. A `KeptBase` or `FellBack` stage adds
  none. A run that ran its whole plan checks the counts. A mismatch fails
  it with `RefineBudgetIdentity`.
- **Spend.** A proposer, composer or selector reports each model call
  with `charge`. `ModelProposer` charges each of its calls. Each step
  writes its calls to the cost ledger under the refine run. With a budget,
  the run stops before the next step once its own spend reaches a cap. It
  ends as `budget_exhausted` with no `chosen` overlay. A live re-probe
  records its rows under the fork. The run counts the spend of a
  completed fork from its report.
- **Rollout.** An older worker cannot decode a stage request with a
  `sample` above 1. Start a refine run only after every worker runs this
  release.
- **Crash.** Each step is one recorded activity. A crash re-runs only the
  step in flight.

The run ends with a `RefineReport`: the status, the chosen overlay, one
record per stage, each selection, the `RefineLedger` and the gate record.
It holds no id of the refine run and no time, so one seed and one script
give byte-identical reports. A run that chose an overlay goes on to the
adoption gate of section 23. `RefineInput::dry_run` ends the run after the
loop with the status `chosen`. A dry run stores no version and opens no
gate.

## 23. The adoption gate

A refine run that chose an overlay does not put it live by itself. The
design is in [`DESIGN-2381.md`](design/DESIGN-2381.md). The gate needs an
overlay store on the harness. With none, the adoption step fails the run
as `OverlayStoreMissing`. A dry run needs no store.

1. `agent_refine_adopt` calls `refine::adopt_over` with the version that
   was active at refine start. A moved pointer ends the run as
   `superseded`. A candidate whose changed parts the policy lists goes
   live as `auto:<run>`, under the daily cap.
   The step puts any other candidate as an inactive version
   `refine:<run>`. It also reads the head of the activation log.
2. The run drops each decision that arrived before this point. Then it
   sets `refine_status = waiting_approval`.
3. The run waits on the signal `refine_approval:<run>` until
   `approval_timeout` of its policy passes. The default is 7 days.

| Decision | Payload | Effect | `refine_status` |
|---|---|---|---|
| Approve | `{"verdict": "approve"}` | Activates the version over the version that was active at refine start. | `adopted` |
| Edit | `{"verdict": "edit", "overlay": {...}}` | Checks the overlay under the run ceilings, puts it as `refine:<run>` and activates it. | `adopted` |
| Reject | `{"verdict": "reject", "reason": "..."}` | Activates nothing. The version stays stored. | `rejected` |
| No decision | | As reject, at the deadline. | `timed_out` |

- **Superseded.** Any move of the pointer after refine start wins over a
  decision, by an operator or by another run. So does any move after the
  wait opens. An approve or an edit then activates nothing, and the run
  ends as `superseded`. A retry of a decision never moves the pointer
  again, so it never undoes a rollback.
- **Refused.** The run ceilings or the store can refuse an edit. A lowered
  ceiling can refuse an approve. The run notes the reason and waits until
  the same deadline. A payload that does not decode does the same. A key
  that the verdict does not read fails the decode, so an approve that
  carries an overlay activates nothing.
- **Early decision.** The run drops a decision that arrives before the
  wait opens, as eval does. List the run first. The report counts each
  dropped decision.
- **Attributes.** The run sets `refine_kind` and `refine_status` at each
  end: `adopted`, `rejected`, `timed_out`, `superseded`, `auto_adopted`,
  `unchanged`, `budget_exhausted` or `chosen`. A run that fails in the gate
  gets `failed`.
- **Actor.** The move records the actor `refine:<run>`. History holds the
  signal of the person.
- **Upgrade.** A run that started on an older release opens no gate. It
  ends with `chosen`, as that release did.

```sh
harvest agents profile pending support
harvest agents profile approve <run>
harvest agents profile reject <run> --reason "too long"
harvest agents profile edit <run> --file overlay.json
```

`pending` sends the `filter` of `GET /workflows`, which only a Postgres
server serves. The other commands take the execution id in any UUID
spelling and send its canonical form. On SQLite, `sqlite::decide_refine`
sends a decision, and `ExecutionFilter::search_attribute` lists the runs
that wait. On Postgres, `postgres::decide_refine` sends it.

## 24. The lesson library

Each adoption adds one lesson to the library of its kind. The next refine
of the kind reads the library. The design is in
[`DESIGN-2382.md`](design/DESIGN-2382.md). The library needs a memory store
on the harness that implements `MemoryStore::ensure_block` and
`MemoryStore::apply_labelled`. `InMemoryMemoryStore` does.

- **Scope.** The memory scope `kind:<kind>:lessons` holds one block,
  `lessons`. Each entry is one lesson as one line of JSON. It holds the
  adopted `version`, the `why` and `lesson` text of the select step, the
  `sources` exec ids and the `agents` ids. `agents` holds each agent of
  the kind that a source names: its workflow id, the agent of its memory
  scope `agent:<id>`, and each agent of its mandate chain. It also holds
  each agent of the lessons that the run read. `refine::read_lessons`
  decodes a block.
- **Write.** A run that ends `adopted` or `auto_adopted` runs
  `agent_refine_lesson`. A rejected, timed-out or superseded run writes
  none. The report holds the result in `lesson`: `written`, with the
  version and the evicted count of the attempt that answered, or
  `failed`, with the error. A failed write never undoes the adoption. A
  cancel during the write still cancels the run.
- **Taint.** The lesson keeps the flow labels of the run: each label of
  each source and of the lessons that the run read. The report holds them
  in `flow_labels`. A store that cannot keep labels refuses a labelled
  write.
- **Bound.** The write reads the current `lessons.max_entries`. It removes
  the oldest lessons until the block holds at most that many. A change of
  the bound applies to the next write.
- **Read.** The start step records the library, cut to the newest lessons
  that fit 32 768 rendered characters. A failed read gives no library and
  a warning. Each proposer stage gets it in `ProposeContext::lessons`,
  with its labels. `ModelProposer` puts it in its prompt as data.
- **Why and lesson.** A selector gives the text with
  `OverlaySelector::choose`. The default calls `select` and gives none. A
  rule pick and a sequential run give none.
- **Erase.** `Control::ResetMemory` removes each lesson of the kind whose
  `agents` holds the agent id, also one that other agents share.
  `agent_lesson_match` records the matches first, so a retried removal
  keeps the count. The removal step fails while a match remains.
  `AgentState::lessons_removed` counts them, and `harvest agents erase`
  reports `lessons_removed`. History keeps copies of the text in each
  refine run that read or wrote a lesson. Erase those runs too.
- **Upgrade.** Upgrade each activity worker first: an older worker
  refuses a `ProposeRequest` with `lessons`. A run that started on an
  older release has no source labels, so it writes no lesson. Its report
  notes why.
- **Fleet.** `InMemoryMemoryStore` lives in one process. Each worker of a
  fleet needs the same durable store.
