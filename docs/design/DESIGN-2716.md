# DESIGN-2716: the `sweep` workflow tool

Issue #2716. Needs #2448 (`agent_sweep`). Related: #2453 (`save_progress`
is a workflow tool of the same shape) and #2439 (sampling policy).

## 1. Problem

An app starts `agent_sweep` with `sqlite::start_sweep`. A running agent
loop cannot start one. The workflow tools that run in the loop are a
closed set: `delegate`, `memory`, `load_skill`, `schedule_followup` and
`save_progress`.

An embedder tool cannot start one either. A tool that waits on a sweep
inside its activity passes the 9-minute tool timeout. It also contends
with a single-writer runtime. A retried activity starts a second sweep
after a crash.

The need comes from a research tick. The model reads many long sources
through a fetch tool, and its context grows by each source. A sweep runs
each item with `SampleInput::brief`, so the context grows only by the
rows.

## 2. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| Where the call runs | an activity that waits · an app op · **a workflow tool that spawns an `agent_sweep` child** |
| Who sets the kind, the ceiling and the scorer | the call · **the operator, in `SweepTool`** |
| The ceiling | optional · **required (`NonZeroU32`)** |
| The strategy | from the call · **`single`, fixed** |
| The scorer of a research item | `ExactMatch` · a judge · **a new deterministic `Answered`** |
| The result | the `SweepReport` · **one row per label, the joined answers and the spend** |
| The spend in the parent | not counted · **added to the usage and the cost of the parent** |
| Taint | none · **the labels of the setting join the run** |
| Nesting | allowed, under the ceiling · **a draw and a delegated child drop the setting** |
| Replay of old histories | a patch gate · **none: only a task with the setting offers the tool** |

## 3. Reverse brainstorming

"How could the tool spend more, or hide what it read?" Each answer gives a
rule.

| Failure | Rule |
|---|---|
| A call goes out with no token cap. | The setting has no default ceiling, and the field is not optional. Each call sets `max_total_tokens`. |
| The model widens the ceiling or the kind. | The call arguments deny unknown keys. Only the instruction and the items come from the call. |
| A call asks for 1000 items. | `max_items` bounds a call. A wider call gets an error result, and no child starts. |
| An item text passes the item cap. | The plan checks `MAX_SWEEP_ITEM_BYTES` and refuses the call. |
| The whole input passes the child input cap. | The plan checks the activity input cap and refuses the call. |
| An item run sweeps again. | A draw and a delegated child drop the setting. A call in a probe or a draw gets an error result. |
| A usage view of the agent workflow misses the spend. | The parent adds the usage and the cost of the sweep tree to its report. |
| The result hides that the run read outside text. | The labels of the setting join the flow labels of the run. |
| A policy refusal or an engine failure ends the parent. | Either gives an error result. The run goes on. |
| A failed sweep hides its spend. | The ledger keeps the spend of each child. The guide says so. |
| An app tool named `sweep` shadows it. | `sweep` is a built-in name, so tool guidance never rewrites it. |

## 4. Six thinking hats

- **White.** `agent_sweep` returns a `SweepReport` with rows of at most
  512 bytes of answer. `spawn_child_workflow_fan_out_collect_raw` gives
  the child result or its error apart from an engine error.
- **Red.** An owner fears a tick that spends without a bound. The result
  names the ceiling and the spend in each call.
- **Black.** Each item is an agent run, so the spend of an item grows with
  its steps. The profile of the kind bounds its steps and its tokens. The
  first wave can take more than its fair share of the ceiling, because
  `sibling_cap` splits the tokens that are left over the wave. The profile
  cap of each item bounds that share.
- **Yellow.** A crash costs only the items in flight. The parent context
  grows by the rows only.
- **Green.** A later call can name a stored list, or a strategy that the
  policy allows.
- **Blue.** Tests first, then the module, then the guide.

## 5. Decision

```rust
pub struct SweepTool {
    kind: String,
    max_total_tokens: NonZeroU32,
    max_items: u32,            // default and bound: MAX_SWEEP_ITEMS
    scorer: ScorerRef,
    concurrency: Option<NonZeroU32>,
    flow_labels: BTreeSet<String>,
}
// AgentTask::sweep and AgentProfile::sweep carry it.
pub struct SweepToolResult { text, items: Vec<SweepToolRow>, stopped, spent: SweepSpend }
pub struct SweepToolRow { label, ok, status, tokens, error }
pub struct SweepSpend { ceiling, input_tokens, output_tokens, total_tokens, cost_usd_micros }
pub struct Answered; // id "answered"
```

1. A task with `sweep` set offers the `sweep` tool. The workflow handles
   the call. No activity runs it.
2. `sweep_tool::plan` checks the call and builds the `SweepInput`: the
   kind, `single`, the scorer, the concurrency and the ceiling of the
   setting.
3. The loop spawns one `agent_sweep` child and waits for it. The engine
   records its start and its end.
4. A child error gives an error result. An engine error fails the turn,
   as for every child.
5. The parent adds the usage and the cost of `totals` to its own.
6. The labels of the setting join the run once the child ran.
7. A row is `ok` when the item completed and its scorer accepted it.

## 6. Acceptance criteria and their tests

All tests are in `autumn-harvest-agent/tests/sweep_tool.rs`, except where
noted.

| Criterion | Test |
|---|---|
| One call gives one row per label, the joined text and the spend | `a_sweep_call_returns_one_row_per_label_and_its_spend` |
| The parent counts the spend of the sweep tree | `the_parent_counts_the_spend_of_the_sweep_tree` |
| The result marks the run | `a_sweep_result_marks_the_run_with_the_labels_of_the_setting` |
| The ceiling applies to each call | `the_ceiling_stops_the_items_that_it_does_not_reach` |
| A wide call starts no child | `a_call_over_the_item_bound_is_refused_before_any_model_call` |
| A call cannot widen the setting | `a_call_cannot_widen_the_ceiling`, and `a_call_cannot_carry_a_setting` in `src/sweep_tool.rs` |
| No setting, no tool | `a_run_without_the_setting_does_not_offer_the_tool` |
| An item never sweeps again | `an_item_run_never_offers_the_tool` |
| A policy refusal leaves the run going | `a_policy_refusal_gives_an_error_result_and_the_run_goes_on` |
| The history replays | `the_parent_history_replays_clean` |
