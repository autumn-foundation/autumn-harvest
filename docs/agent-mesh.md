# Agent mesh: delegation, mandates and flow rules

An agent run can delegate a task to another agent, a **peer**. The peer runs
as a sub-agent inside the same workflow. A **mandate** bounds what each agent
of the tree may do. **Flow rules** stop a dangerous sequence of tool calls
across the whole tree.

These primitives are in `autumn-harvest-agent`, modules `mesh` and `flow`.
They work on Postgres and on SQLite. The research behind them is in
[`rnd/2026-10-13-agent-mesh-primitives.md`](rnd/2026-10-13-agent-mesh-primitives.md).
The plan is in [`DESIGN-agent-mesh.md`](../DESIGN-agent-mesh.md).

## 1. The primitives

| Primitive | Type | What it does |
|---|---|---|
| Roster | `Peer`, `AgentHarness::peer` | Names each agent, its prompt, its tools and its bounds. |
| Discovery | activity `agent_discover_peers` | Reads the peers that a mandate names. History records the answer. |
| Delegation | workflow tool `delegate` | Runs a peer as a sub-agent. Only its final answer returns. |
| Mandate | `Mandate` | Bounds depth, delegations, peers and tools. A child gets the meet. |
| Refusal | `Refusal` | Names why a delegation did not run. |
| Budget roll-up | `AgentReport::delegations` | Cuts the child cap from the parent remainder. Adds the child usage to the parent. |
| Flow rules | `FlowRules`, `Tool::labels` | Gate the call that completes a forbidden label set in the tree, and each later call that touches the set. |
| Label carry | `AgentReport::flow_labels`, `AgentTask::flow_labels` | Keeps the labels of a conversation from one run to the next. |
| Wire form | `MANDATE_META_KEY`, `Mandate::outbound`, `Mandate::accept` | Carries a mandate to another process. The receiver can only narrow it. |

## 2. Delegate to a peer

```rust
use autumn_harvest_agent::{AgentHarness, AgentTask, Mandate, Peer};

let harness = AgentHarness::new(model)
    .tool(web_search)
    .tool(write_file)
    .peer(
        Peer::new("researcher", "Finds facts on the web.")
            .system("You research. Cite each source.")
            .tools(["web_search"])
            .max_steps(6)
            .max_total_tokens(40_000),
    );

let task = AgentTask::new("Plan a trip to Lisbon.")
    .max_total_tokens(100_000)
    .mandate(Mandate::new("planner").peers(["researcher"]));
```

The planner sees one more tool, `delegate`. Its input is `agent`, one of the
peer names, and `task`, the text for the peer. The tool description lists
each peer with its description. A run with a mandate reserves the name
`delegate`, so an app tool with that name is hidden. The name stays reserved
when the lookup finds no peer. The model then sees no `delegate` tool, and a
call to it gets a refusal.

The result of a delegation is JSON:

```json
{"agent": "researcher", "stop": "completed", "answer": "The ferry leaves at 9."}
```

The `stop` field tells the parent model how the child ended. The parent
model reads it and decides what to do.

## 3. What the child gets

The child sees nothing of the parent transcript. Put every fact it needs in
the task.

| Child setting | Value |
|---|---|
| System prompt | `Peer::system` |
| Input | The `task` argument |
| Transcript | Fresh |
| Run id | `{root run id}:d{n}`. `n` counts the delegations that ran in the tree, from 1. A refusal takes no number. |
| Tools | The meet of the parent mandate and `Peer::tools` |
| Steps | `Peer::max_steps` (default `DEFAULT_MAX_STEPS`) |
| Token cap | The smaller of `Peer::max_total_tokens` and the parent remainder |
| Read-only | On when the parent, the peer or the mandate is read-only |
| Memory, follow-ups, delivery | Off |
| Session, tenant, output cap, unattended flag | As the parent |
| Loop guard, approval timeout, request cap | As the parent |
| Answer | Cut to 8 000 characters. `AgentHarness::tool_output_limit` does not apply. |

Tools key idempotency on `(run_id, step, call_id)`. A child has its own run
id, so its keys never collide with the parent keys. A child of a run whose
run id is the workflow id skips the response cache, as its parent does.

## 4. Mandates

A mandate holds the authority of one agent:

| Field | Meaning | Default |
|---|---|---|
| `chain` | The agents from the root to the holder. The holder is last. | `[holder]` |
| `max_depth` | The bound on depth. The root agent has depth 0. | 3 |
| `peers` | The peers that the holder may delegate to. | none |
| `tools` | The harness tools that the holder may call. `None` means every tool. | `None` |
| `max_delegations` | The bound on delegations in the whole tree. | 16 |
| `read_only` | Refuse every write and external call. | off |
| `flow_labels` | The labels that a caller in another process sends. | none |

A mandate bounds its holder too. A root mandate with `tools(["lookup"])`
gives the root agent only `lookup`.

`Mandate::delegate` computes the child mandate. Authority only narrows:

- `chain` gets the peer name. Depth goes up by one.
- `peers` is the meet of the parent peers and `Peer::peers`.
- `tools` is the meet of the parent tools and `Peer::tools`.
- `max_depth` and `max_delegations` stay the same. They never grow.
- `read_only` stays on once it is on.

Property tests prove each rule (`a_delegation_never_widens_authority`).

The model does not see a tool outside the mandate. The decision denies a call
to it, and the tool-call activity refuses it again. The mandate never bounds
a workflow tool, such as `delegate` or `schedule_followup`. The `memory` tool
writes state that later runs read, so an allowlist must name `memory`.

An agent name has 1 to 64 characters from `a-z`, `0-9`, `_` and `-`. A tool
name or a label has 1 to 128 bytes and no control character. Each set holds
at most 256 entries. A run whose task mandate is not valid fails at its
start, before any model call.

## 5. Refusals

A refused delegation does not run. The model reads an error result with the
verdict, and the report lists it:

```json
{"error": "\"researcher\" is already in the delegation chain", "verdict": "cycle"}
```

| Verdict | Cause |
|---|---|
| `invalid_arguments` | The `delegate` call does not have exactly the arguments `agent` and `task`. |
| `not_permitted` | The mandate does not name the peer. |
| `unknown_peer` | The mandate names the peer, but the roster has no valid peer of that name. |
| `cycle` | The peer is already in the chain. |
| `depth_exceeded` | The delegation passes `max_depth`. |
| `fan_out_exhausted` | The tree has used `max_delegations`. |
| `budget_exhausted` | The child would get a token cap of zero. |
| `invalid_mandate` | `Mandate::accept` got a malformed mandate. |

A delegation runs the first seven checks in the order of the table.
`Mandate::accept` returns `invalid_mandate`, `cycle` or `depth_exceeded`.
`Refusal` implements `std::error::Error`.

The report keeps a peer name only when it is a valid agent name. Any other
name shows as `_invalid`, because the model wrote it.

## 6. Budgets

The tree shares one token budget, the `max_total_tokens` of the root task:

1. A child cap is the smaller of the peer cap and the parent remainder. A
   cap of zero is `budget_exhausted`.
2. A child stops with `tokens_exhausted` when it passes its cap, as a root
   run does.
3. The parent adds the child usage. A parent over its cap makes no further
   model call. It stops with `tokens_exhausted`.

The tree runs one step at a time inside one workflow. Two children never
read the same remainder, so they cannot overspend together.

`AgentReport::delegations` lists each delegation, refused ones too, in the
order each ended:

| Field | Meaning |
|---|---|
| `chain` | The agents from the root to the delegate |
| `run_id` | The child run id. `None` for a refusal. |
| `stop` | How the child ended. `None` for a refusal. |
| `refusal` | The verdict of a refused delegation |
| `usage` | Tokens of the child and of every agent under it |
| `steps_used`, `tool_calls` | The rounds and calls of the child and of every agent under it |

`AgentReport::usage`, `steps_used` and `tool_calls` cover the whole tree.
Each agent still has its own step bound, so a child round never counts
against the `max_steps` of its parent.

## 7. Flow rules

Some calls are safe alone, but dangerous together. Read an inbox with
untrusted mail, read private data, then send mail: a prompt injection in the
inbox can exfiltrate the data. Flow rules stop that sequence.

Give each tool its labels:

```rust
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::{AgentHarness, FlowRules, FnTool, ToolEffect};

let inbox = FnTool::new("inbox", "Read mail.", schema, read_mail)
    .effect(ToolEffect::ReadOnly)
    .label(labels::UNTRUSTED_INPUT)
    .label(labels::PRIVATE_DATA)
    .shared();

let harness = AgentHarness::new(model)
    .tool(inbox)
    .tool(send_mail)
    .flow_rules(FlowRules::rule_of_two());
```

Each call also carries the label of its effect: `effect:read_only`,
`effect:internal`, `effect:write` or `effect:external`.

`FlowRules::rule_of_two()` follows Meta's Agents Rule of Two. A run tree
needs a person before it holds all three of untrusted input, private data,
and a write or external effect. The call that completes the three waits for
a person. Each later call with one of the labels waits too. One approval
releases one call, so a second send needs a second approval.

Write your own rule with `FlowRules::new().forbid(labels, rule)`.
`Rule::Ask` waits for a person. `Rule::Deny(reason)` refuses the call.
`Rule::Allow` and an empty set add nothing.

How the check works:

- A rule trips when the call carries a label of the set, and the set is
  complete with the call. The label can already be in the tree. So after the
  set is complete, each call that touches it trips the rule again. A call
  with no label of the set passes.
- The strictest tripped rule wins, and the policy answer still counts.
- A label counts once the decision lets its call run: an allow, or an
  approval. It counts even when the tool then fails. A denied, rejected or
  timed-out call adds nothing.
- In one turn, a call counts for the calls after it before it runs, so the
  check fails closed.
- A delegation can add the labels of its child. A later call in the same turn
  then holds a decision that did not see them. That call does not run, and
  the model reads that it must call again. The next turn decides it with the
  new labels.
- With flow rules, the decision denies a call to an unknown tool, because the
  worker cannot label it.
- The tool-call activity gets the recorded labels of the call. It refuses a
  tool that gained a label after the decision, for example in a rolling
  deploy. The model reads that it must call again.
- One label set spans the tree. A child answer enters the parent context, and
  a parent task text enters the child. So labels flow both ways.

The workflow holds the label set. The model-turn activity records each
decision and the labels of each call. Model output cannot remove a label, and
replay does not check the rules again. A turn records labels only when the
harness has flow rules. Install the same flow rules on every worker.

### Carry the labels to the next run

A run that continues a conversation must continue its labels too:

```rust
let next = AgentTask::new("Now reply to the sender.")
    .history(report.messages.clone())
    .flow_labels(report.flow_labels.clone());
```

`HeartbeatTask::flow_labels` does the same for a heartbeat tick.

## 8. Approvals in a child

A gated call in a child waits on a signal, as in a root run. The name is
`tool_approval:<round>:<position>:<call_id>`. `round` counts the tool rounds
of the whole tree, so a parent and a child never share a name. A run with no
delegation keeps the names it had before.

Read the name from the wait. On SQLite, `RunState::WaitingSignal` holds it.
On Postgres, the deadline timer of the wait is
`__signal_timeout:<seq>:<signal name>`.

## 9. Across processes

A mandate can travel with a call to another process. Put it in the `_meta`
of an MCP call or in the metadata of an A2A message:

```rust
use std::collections::BTreeSet;
use serde_json::{Map, Value};
use autumn_harvest_agent::{AgentTask, Mandate, Peer};

fn send(
    mandate: &Mandate,
    remote: &Peer,
    used: u32,
    labels: &BTreeSet<String>,
) -> Result<Map<String, Value>, Box<dyn std::error::Error>> {
    let mut meta = Map::new();
    mandate.outbound(remote, used, labels)?.insert_into(&mut meta);
    Ok(meta)
}

fn receive(meta: &Map<String, Value>, text: &str)
    -> Result<AgentTask, Box<dyn std::error::Error>>
{
    let local = Mandate::new("researcher").peers(["critic"]).tools(["web_search"]);
    let effective = match Mandate::from_meta(meta)? {
        Some(inbound) => local.accept(&inbound)?,
        None => local,
    };
    Ok(AgentTask::new(text).mandate(effective))
}
```

- `Mandate::outbound` is `delegate` for the wire. It sends the remainder of
  the fan-out, because the receiver starts its own count at zero. It sends
  the labels of the tree, because the receiver must keep them.
- `Mandate::from_meta` is strict. It refuses an unknown key, so a field that
  a newer sender adds to narrow the run is never dropped without a sign.
- `Mandate::accept` returns the meet of both mandates and the union of their
  labels. The result is never wider than either side, so an inbound mandate
  can only narrow a run. The local mandate must be a root mandate.
- The chain keeps the caller agents, so a cycle across processes is refused.
- The mandate carries no signature. Authenticate the caller as you do for any
  request.

## 10. Crash and replay

| In flight at the crash | On resume |
|---|---|
| A peer lookup | That lookup runs again. |
| A child model call | The worker sends that one call again. |
| A child tool call | That one call runs again, with the same idempotency key. |
| A child approval wait | The wait resumes. Its deadline is durable. |

Replay reads every finished child step from history and sends none of them
again. A roster change after the lookup does not change a recorded run.
`a_restart_inside_a_child_resends_nothing_and_looks_up_nothing` proves this.

`eval::evaluate` re-drives a delegation run. The recorded lookups answer, so
the candidate needs no roster. A recorded tool outcome answers only an equal
call of the same agent. A candidate call takes a recorded call id only from a
turn of the same agent, so an approval for a child never releases a root
call. The run id suffix `:d{n}` names that agent.

## 11. Observability

A run with a mandate records the acting agent in each model-turn request, as
`agent`. The model-turn span `agent_model_turn` carries
`gen_ai.agent.name`. On Postgres, the cost ledger counts child turns in the
run of the root agent. A `DelegationRecord` gives the tokens, rounds and
calls per subtree.

## 12. Limits

- A child runs inline, on the worker of the parent. It cannot run on another
  worker, and children never run at the same time.
- Each child turn adds to the history of the parent. Keep `max_delegations`
  and the peer step bounds low.
- All agents use the one model of the harness.
- Delegation to a remote peer is a manual step: use `ctx.call_remote_task`
  and the wire form above.
- Flow rules are only as good as the tool labels. A tool with no label adds
  only its effect label.
- A memory note carries no label. A later run reads it with no labels. Keep
  memory writes off in a run that reads untrusted input.
- A run whose run id is a workflow id that ends in `:d<number>` can share
  tool idempotency keys with a child run id. New tasks use the execution id,
  so they are not affected.
- The mesh has no handoff. A peer cannot take over the rest of the run.
