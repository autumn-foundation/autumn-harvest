# DESIGN-2439: the operator sampling policy

Issue #2439. Parent epic #2438. Same store and CLI shape as #2394.

## 1. Problem

Each strategy of epic #2438 runs many draws. A draw can call tools. The
operator must decide the strategies, the draw caps, the concurrency, the
tool effects of a draw, the scorers and the judge models. Each setting
needs a safe default and a runtime change with no restart. A run must never
widen its own policy.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Where the type lives | the core crate · **the agent crate, next to `ToolEffect`** · a new crate |
| Where the SQL lives | the agent crate · **the core crate (`agent_policy_log`) and `autumn-harvest-sqlite`, as for #2371** |
| Table shape | one table per policy · **one row per `(kind, policy)`, so #2394 adds `refine` rows** |
| What a reset does | delete the row · **store a `NULL` body and keep the revision** |
| Where old and new values go | `harvest_audit_log.error_summary` · **a `harvest_agent_policy_audit` row in the same transaction**, and a `harvest_audit_log` row for the HTTP call |
| How a second process sees a change | a cache with a TTL · **one read per strategy run, with no cache** |
| How a run reads the policy | each draw reads it · **one recorded activity, `agent_sampling_policy`, at the start** |
| What the strategy run is | `agent_sample` of #2443 · **a gate and fan-out workflow, `agent_draws`, that #2443 builds on** |
| How the concurrency bound works | a sliding window · **waves of at most `max_concurrent_draws` child runs** |
| How a draw gets its effects | a policy field on `AgentTask` · **`AgentTask::allowed_effects`, which only narrows** |
| How a decode refuses a policy field | `deny_unknown_fields` on `AgentTask` · **a named list of policy keys, checked on decode** |
| The HTTP route | a `PATCH` · **`GET`, `PUT` and `DELETE` on `/admin/agents/{kind}/sampling-policy`** |

## 3. Reverse brainstorming

"How could a run get more than the operator allows?" Each answer gives a
rule.

| Attack or failure | Rule |
|---|---|
| A client puts `max_draws` in its `AgentTask` or `AgentOp`. | The decode fails on each name in `SAMPLING_POLICY_FIELDS`. |
| A client puts a policy field in the strategy input. | `DrawsInput` denies unknown fields. |
| A change during a run moves its bounds. | The run reads the policy once, in a recorded activity. Replay reads the record. |
| A newer worker records a field that an older worker drops. | `SamplingPolicy` denies unknown fields, so the older worker fails closed. |
| A draw writes N times. | Each draw gets `draw_effects` meet the effects of its task. The decision and the tool call both check it. |
| A draw delegates, and its child writes. | An inline child keeps the effects of its parent. A delegation to a durable peer from a narrowed run is refused. |
| A judge calls an expensive model. | A judge scorer names its model. The kernel refuses a model outside `judge_models`. |
| A policy asks for 1000 parallel draws. | The validation caps `max_concurrent_draws` at `MAX_PARALLEL_CHILDREN`, and the kernel caps it again. |
| A viewer sets the policy. | The set and reset routes are `Mutating`. The middleware returns 403 before the handler. |
| A store row is corrupt. | The read fails, so the run fails. It never falls back to a wider value. |
| No store is installed. | The harness reads every default. |

## 4. Six thinking hats

- **White (facts).** `RefinePolicy` (#2394) is not built yet, so this issue
  sets the shape. The management API is in the plugin crate. The built-in
  `harvest-operator` role passes `Mutating` routes. No existing audit row
  records old and new values. `ToolEffect` and `MAX_PARALLEL_CHILDREN` are
  in the agent crate.
- **Red (feelings).** Operators fear a fan-out of writes most. The default
  `draw_effects = [ReadOnly]` must hold without any setting.
- **Black (risks).** A second audit table can drift from
  `harvest_audit_log`. A new `AgentTask` field touches every
  recorded task. Waves leave a slot idle while a slow draw ends.
- **Yellow (benefits).** One table holds every agent policy. One read per
  run means no cache and no stale window. The kernel is small, so Verus
  proves it.
- **Green (ideas).** `audit` lists the old and new values for `show`. The
  same table and routes carry `RefinePolicy` later.
- **Blue (process).** Red tests first, one per acceptance line. Then the
  kernel, its proof, the stores, the workflow, the route and the CLI. Then
  a review from several angles.

## 5. Decision

### The type

`autumn_harvest_agent::sampling::SamplingPolicy`. Each key is optional on
decode and takes its default. An unknown key fails the decode.

| Setting | Default | Bound |
|---|---|---|
| `strategies` | none | `best_of_n`, `rejection`, `vote`, `cascade`, `beam` |
| `max_draws` | 1 | 1 to `MAX_DRAWS` (256) |
| `max_concurrent_draws` | 1 | 1 to `MAX_PARALLEL_CHILDREN` (16) |
| `draw_effects` | `[read_only]` | any set of `ToolEffect` |
| `scorers` | `deterministic` | `deterministic`, or `{"only": [ids]}` |
| `judge_models` | none | model names |
| `max_answer_retries` | 0 | 0 to `MAX_ANSWER_RETRIES` (16) |

### The kernel

`admit(policy, request)` returns an `Admission` or a named `Refusal`. The
checks go in this order: the strategy, the draw count, the concurrency, the
scorer, the judge model. `draw_plan` and `wave_end` hold the integer part.
`formal/verus/src/agent_sampling.rs` proves them.

### The store

```rust
trait SamplingPolicyStore {
    fn read(kind) -> Result<PolicyRead>;
    fn set(kind, &SamplingPolicy, actor) -> Result<PolicyChange>;
    fn reset(kind, actor) -> Result<PolicyChange>;
    fn audit(kind, limit) -> Result<Vec<PolicyAudit>>;
}
```

In-memory, SQLite and Postgres stores pass one shared suite. A write locks
the row, writes the new body, increments the revision and inserts an audit
row with the old and new bodies, in one transaction.

### The run

`agent_draws(DrawsInput)` reads the policy once, admits the request, and
runs the draws as child `agent_loop` runs in waves. Each draw has
`allowed_effects = draw_effects ∩ task effects` and reports a failure as a
report. #2440 adds draw identity. #2441 and #2443 add the score and the
selection.

### Who sets it

`PUT` and `DELETE /admin/agents/{kind}/sampling-policy` are `Mutating`.
`GET` is `ReadOnly`. The CLI is `harvest agents sampling policy <kind>
set|show|reset`.

## 6. Acceptance criteria

The tests of issue #2439, each in `autumn-harvest-agent/tests/sampling_policy.rs`
unless named otherwise:

1. No policy: every default, and `best_of_n` with `n = 3` is refused with
   no model call.
2. `max_draws = 4`: `n = 4` runs, `n = 5` is refused with no model call.
3. `max_concurrent_draws = 2`: no more than 2 open model calls.
4. Default `draw_effects`: a `Write` tool body never runs.
   `[read_only, write]`: it runs.
5. A judge model outside `judge_models` is refused with no model call.
6. A set from one process changes the next run of a second process.
7. A set during a run does not change that run.
8. A set without the operator role returns 403, and the row stays
   (`autumn-harvest-plugin/tests/sampling_policy_integration.rs`).
9. `AgentTask`, `AgentOp` and `DrawsInput` with a policy field fail to
   decode.
