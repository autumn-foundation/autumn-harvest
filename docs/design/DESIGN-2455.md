# DESIGN-2455 — Long-horizon recall eval

Issue #2455. Parent epic #2452. It measures #2453 (progress notes) and
#2454 (episodes).

## 1. Problem

Nothing measures what a durable `Agent` forgets across ops. #2453 and #2454
change memory. Without a number, neither change can show that it helps.

## 2. Facts

- `AgentProfile::window` keeps the last `n` messages between ops. It drops
  the oldest turns first.
- `AgentProfile::context_reset` gives the model `save_progress`. The notes
  stay across the window (#2453).
- `AgentProfile::episodes` logs each op. A pass writes facts into the memory
  block, and the system prompt shows that block (#2454).
- `ExactMatch` (#2441) trims both texts and gives `Score(1.0)` or a reject.
- `AgentState::last_answer` holds the answer of the last op.
- `sqlite::send` takes an idempotency key, so a rerun delivers an op once.
- The dataset sweep (#2448) is open. It needs `agent_sample` (#2443), which
  is open too. Its sweep runs one `AgentTask` per item, not a list of ops.

## 3. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| The unit of one item | one `AgentTask` · **one durable `Agent`, one op per session** |
| The fixture | a copy of LoCoMo · **a small JSON file of original items, in `src/recall/fixture.json`** |
| The item shape | free text with a judge · **ops, then a question, with one expected value** |
| A memory config | an enum · **an `AgentProfile` whose kind names the config** |
| The configs | window, notes, episodes · **window, notes, memory, episodes** |
| The values | words · **numbers, so case cannot change a match** |
| The driver | wait for #2448 · a new workflow · **a plain driver over `SqliteRuntime`, one agent per config and item** |
| The scorer | a new one · **`ExactMatch` (#2441)** |
| The CI model | a fixed script per item · **a model that answers only from its request** |
| The real-model run | a CI job · **an example crate, run by hand, with `ClaudeModel` of `autoresearch`** |
| Where the table goes | the README · **`docs/performance-agent-recall.md`** |

## 4. Reverse brainstorming

"How could this eval lie?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| The CI model knows the answer from its script, so loss never shows. | The model reads the request only. A question with no fact in the request gets `unknown`. |
| The harness always gives 0, so a 0 proves nothing. | A control item states its fact one op before the question. It scores 1 with the window only. |
| The question text holds the answer. | A question is `What is the <key>?`. The search key is `<key> is `, which the question does not hold. |
| A failed op leaves the answer of an older op in `last_answer`. | An item with `failures > 0` is an error row, not a score. |
| An op waits on a retry timer, and the sweep scores the older answer. | Each op must end with the agent idle on its inbox. Any other run state fails the item. |
| `episodes` also gives the model the `memory` tool, so a real model saves the fact itself. The column then credits episodes. | The `memory` config has store memory and no episodes. Compare `episodes` with `memory`. |
| A real model answers `Teal` for `teal`, and the miss hides recall. | Each value is a number. The docs say that `ExactMatch` trims spaces only. |
| A rerun on the same file with another model or window prints old rows under a new heading. | The agent name holds a hash of the item, the profile and the model id. |
| Two models with no `model_id` hash the same, so the second reuses the rows of the first. | A model with no id hashes the start time of the sweep in place of the id. `ReaderModel` has an id. |
| A crash loses the in-memory stores of `memory` and `episodes`. | The docs say to rerun those configs on a new file. |
| One config fails and the report loses the other rows. | The driver catches each error per item. The row of a failed item holds the error. |
| A rerun after a crash calls the model again. | Each op has the key `recall:<agent name>:<n>`. A rerun drives only ops that did not run. |
| Two configs share one agent, so one config reads the memory of the other. | The agent kind is the config. Each memory scope is per agent. |
| The scripted 1.00 reads as a measured result. | The docs say that `ReaderModel` scores 1 on notes and episodes by construction. |
| An updated value reads as the old value. | The model takes the last match in the request. An item checks this. |
| A real-model table has no context. | The table states the model, the date and each config. |
| The fixture copies a third-party dataset. | Each item is original. No LoCoMo or LongMemEval text is in it. |

## 5. Six thinking hats

- **White (facts).** Section 2. Each config exists. The sweep of #2448
  does not.
- **Red (feeling).** A number for memory feels overdue. A 0 with no
  control feels like a broken harness.
- **Black (risks).** The eval drifts from #2448. A real-model run costs
  money. Section 4 answers the false results.
- **Yellow (value).** Each memory change gets a number. The CI run proves
  that the harness sees loss. A new config is one more profile.
- **Green (alternatives).** A judge scorer for free text. A larger fixture
  with distractor values. Both can come later on the same report.
- **Blue (process).** Red tests first, then the code. This is measurement,
  not a rule that limits an agent, so it has no Verus proof. A property
  test checks the report shape.

## 6. Decision

```rust
pub struct RecallItem { pub id: String, pub ops: Vec<String>, pub expected: String }
pub struct RecallFixture { pub items: Vec<RecallItem> }
pub enum RecallOutcome { Scored { answer: String, verdict: Verdict }, Failed { error: String } }
pub struct RecallRow { pub config: String, pub item: String, pub outcome: RecallOutcome }
pub struct RecallReport { pub configs: Vec<String>, pub items: Vec<String>, pub rows: Vec<RecallRow> }

pub fn profiles(window: u32) -> Vec<AgentProfile>; // window, notes, memory, episodes
pub async fn sweep(rt, harness, fixture, configs) -> RecallReport;
```

1. The last op of an item is its question. `ExactMatch::new(expected)`
   scores the answer of that op.
2. `sweep` runs each config, then each item, in order. The agent is
   `<config>/<item>.<hash>`. The hash covers the item, the profile and the
   model id. Each op has the key `recall:<agent name>:<n>`. The sweep sends
   the op, then drives the agent until it waits for its next op.
3. A send error, a drive error, an op that does not end, an op failure or a
   missing answer gives a `Failed` row. The sweep goes on to the next item.
4. The report holds one row for each config and item. `to_markdown` prints
   one line per item and one column per config, then the mean with the
   count of scored items.
5. `ReaderModel` is the CI model. It answers a question from the last
   `<key> is <value>` in its request, else `unknown`. It saves a fact with
   `save_progress` when it has the tool. It answers a pass with the facts
   in the episodes.
6. `examples/recall-eval` runs the sweep with a real model. The output goes
   to `docs/performance-agent-recall.md`.

**Known limit.** The sweep is not the durable workflow of #2448. When #2448
lands, each item can run as one child of `agent_sweep`.

**Known limit.** The stores of `memory` and `episodes` are in memory. A
rerun after a crash is valid for `window` and `notes` only. A failed item
stays failed on its file.

## 7. Acceptance criteria and their tests

The tests are in `autumn-harvest-agent/tests/recall.rs`.

| Criterion | Test |
|---|---|
| AC 1: `window = 2`, no notes: a fact of op 1 scores 0 | `with_the_window_only_a_fact_of_op_1_scores_0` |
| AC 2: the same item scores 1 with notes on | `with_notes_on_the_same_item_scores_1` |
| AC 3: the report lists every item under every config | `the_report_lists_every_item_under_every_config` |
| AC 4: one config's crash does not lose another's scores | `a_failed_config_keeps_the_scores_of_the_others`, `a_config_with_no_profile_fails_alone` |
| Support: the control item scores 1 with the window only | `the_control_item_scores_1_with_the_window_only` |
| Support: the episodes config recalls through its facts | `with_episodes_on_the_same_item_scores_1` |
| Support: a later value wins | `the_last_stated_value_wins` |
| Support: an op on a retry timer fails its item | `an_op_that_waits_for_a_retry_fails_its_item` |
| Support: a rerun calls no model | `a_rerun_after_the_sweep_calls_no_model` |
| Support: a rerun with another window runs again | `a_rerun_with_another_window_runs_again` |
| Support: a model with no id never reuses a finished item | `a_model_with_no_id_never_reuses_a_finished_item` |
| AC 5: the first real-model table is under `docs/` | `docs/performance-agent-recall.md` §3. Not met: no API key in CI. |
