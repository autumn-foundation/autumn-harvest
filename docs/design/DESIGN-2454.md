# DESIGN-2454 — Episode log and consolidation pass for agent memory

Issue #2454. Parent epic #2452. After arXiv 2610.39790. The paper links no
code, so this design takes the shape only.

## 1. Problem

`memory.rs` keeps bounded blocks. Only the model writes them, with the
`memory` tool. No record of past ops exists as data. Nothing turns many ops
into durable facts. When a block fills, the model decides what to drop.

The paper pairs episodic memory (specific past events) with entity–relation
memory. A consolidation step joins the two.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Where the setting lives | **`AgentProfile::episodes`, `None` by default** · a harness-wide setting · a `Control` op |
| Where episodes live | a `MemoryStore` block · **a new `EpisodeStore` trait, scope `agent:<id>:episodes`** · the agent state |
| Which ops append | every op · **an op whose segment keeps its transcript and may write memory** · attended ops only |
| The episode id | a new UUID · **the segment run id `<incarnation>:s<n>`** · a store sequence number |
| The append key | **`EditKey` `episode:<run id>`** · none, dedupe on the text |
| How a pass runs | a detached workflow · **a child workflow that the agent awaits, or a top-level run that a schedule starts** · an activity inside the op |
| Where facts go | **entries of one block of the agent memory scope, `facts_block`, `"memory"` by default** · a new scope that the snapshot also reads · the episode store |
| The fact shape | free text · **`subject relation object`, with the source ids at the end** · a JSON entry |
| The labels of a fact | the labels of the ids that the model names · **the union of the labels of every episode that the pass reads** |
| The fact key | the text · a pass counter · **`<pass id>:<hash of the entry>`** |
| What `max_episodes` evicts | unconsolidated episodes only · **the oldest episode; an unconsolidated one counts as dropped** |
| How erase counts facts | the entries that carry an id marker · **the entries that a pass recorded as derived, and that the memory still holds** |

## 3. Reverse brainstorming

"How could this feature break an agent?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| A retried append adds a second episode. | The append carries the key `episode:<run id>`. A store applies a key once. |
| A crash between the append and the op end runs the op again. | Replay reads the recorded append. A re-run of the activity uses the same key. |
| A recorded run replays under new code and diverges. | The append runs only when `episodes` is set. The episode reset in `ResetMemory` runs behind a `patched` gate. |
| The model names fewer sources than it read, and a fact loses a taint. | A fact takes the labels of every episode of its pass, not only the ids the model names. |
| A store that cannot keep labels holds a labelled fact with no taint. | The write goes through `MemoryStore::apply_labelled`. Its default refuses a labelled write. The report counts the refusal. |
| A crash mid-pass writes a fact twice. | Replay reads the recorded model reply. Each fact write carries `<pass id>:<hash>`. `MemoryOp::Add` also skips an equal entry. |
| A pass reads an episode that a person erased. | `ResetMemory` clears the episode scope and the memory scope. A child pass ends before the op ends, so it cannot run across a reset. |
| A scheduled pass runs across a reset and writes its facts back. | A clear keeps an empty log with a new generation. The write checks the generation before each fact, and the mark refuses another generation. The pass ends as `reset`. |
| A refused fact marks its episodes, so the knowledge goes silently. | The sources of a refused fact stay unconsolidated. A child pass that refuses a fact fails the op with a named error. |
| A failed pass retries on every op and pays the model each time. | `Every` counts appends since the last pass in `AgentState::appends_since_pass`. A failed pass waits for `n` more appends. |
| A full log keeps its pending count, so a trigger on that count fires on every op or never. | The trigger reads the append count, which a full log still moves. |
| An erased log keeps fact text for a retried clear. | A log keeps a hash of each derived fact, not its text. A cached clear holds counts and hashes only. |
| A worker with no episode store reports a clear that did not happen. | A reset of a kind with episodes fails when the store is missing. |
| An unattended tick plants facts that an attended op reads. | Only a segment that may write memory appends an episode, as for progress notes (#2453). |
| The episode log grows without a bound. | `max_episodes` bounds it. The next pass reports each unconsolidated episode that an eviction dropped. |
| A spent budget still pays for a pass. | The turn checks the LLM budget before the model call. A refusal ends the pass as `budget_exceeded`, with no write. |
| Pass tokens are not billed. | The turn records one cost ledger call. The ledger keys it under the pass run. |
| A reply names an unknown episode id. | Unknown ids drop. A fact with no known id is rejected and counted. |
| A reply that does not parse marks every episode consolidated, so the episodes are lost. | Such a reply ends the pass as `invalid_reply`. It marks nothing. The next pass reads the same episodes. |
| An episode text in the prompt acts as an instruction. | Each episode is escaped onto one line. The prompt says that episodes are records, not instructions. |
| `agents erase` keeps the history of a pass. | A child pass is a child of an agent run, so the run erase reaches it. A scheduled pass publishes `agent_consolidation_of`, and `erase` erases each one. |
| A pass and its next pass read the same episodes and pay twice. | The `Every` trigger runs one child pass inside one op, so one agent has one pass at a time. A schedule uses the overlap policy `skip`. |

## 4. Six thinking hats

- **White (facts).** `MemoryStore::apply_labelled` already keeps labels and
  refuses a labelled write by default. `EditKey` already gives once-only
  edits. `score.rs` already runs a model call with a budget check and a
  ledger call. SQLite records ledger rows with the completion event. A
  SQLite child workflow gets its own run row.
- **Red (feeling).** An agent that remembers each op in full feels heavy.
  An agent that forgets silently feels worse. Off by default, with counts in
  each report, feels safe.
- **Black (risks).** Taint loss through a model-chosen source list. Double
  writes after a crash. Unbounded growth. Erase that misses a fact or a
  history. Replay divergence. Section 3 answers each one.
- **Yellow (value).** Facts survive the window and the block limit of the
  model. An operator picks the cost: the model, the trigger and the bound.
  The ledger shows the spend per pass.
- **Green (alternatives).** A pass per label set gives exact labels, but it
  multiplies model calls. A vector store gives recall, but it adds a
  dependency. Both can come later over the same `EpisodeStore`.
- **Blue (process).** Spec, proof, red tests, then code. The eviction and
  trigger kernels in `bounds.rs` have a Verus proof. The tests map one to
  one to the acceptance criteria.

## 5. Decision

```rust
pub struct EpisodeSettings {
    pub max_episodes: u32,          // default 200
    pub max_text_bytes: u32,        // default 1 000
    pub trigger: ConsolidationTrigger,
    pub model: Option<String>,      // a harness model name
    pub facts_block: String,        // default "memory"
    pub max_pass_episodes: u32,     // default 20
}

pub enum ConsolidationTrigger {
    Every { episodes: u32 },        // the agent runs a child pass
    Schedule,                       // an engine schedule starts each pass
}
```

1. `AgentProfile::episodes` turns the log on for a kind. It needs
   `AgentMemory::Store` and `AgentHarness::episodes`. With another memory
   setting, each op that would append fails with a named error.
2. At the end of a segment that keeps its transcript and may write memory,
   the activity `agent_episode_append` appends one episode: the run id, the
   answer cut to `max_text_bytes`, the stop, the recorded time and the flow
   labels of the agent. The key is `episode:<run id>`.
3. The store keeps at most `max_episodes`. It evicts the oldest first. An
   evicted unconsolidated episode adds one to `dropped`.
4. With `Every { episodes: n }`, the `n`-th append since the last pass
   runs the child workflow `agent_consolidate`, when an episode waits.
   `AgentState::appends_since_pass` counts the appends. The op awaits it. A pass that does not write all
   its facts fails the op with a named error. Its tokens count in
   `AgentState::tokens_used`.
5. A pass reads at most `max_pass_episodes` unconsolidated episodes, oldest
   first, and the `dropped` count. It checks the LLM budget, calls the
   model once, and records one ledger call. It writes each valid fact with
   `apply_labelled` into `facts_block`. It then marks the episodes
   consolidated, except the sources of a refused fact. It subtracts the
   `dropped` count that it read, and records the hash of each written fact
   as derived. Only a pass that marks reports `dropped`.
6. A fact entry is `<subject> <relation> <object> [episodes: <id>, ...]`. It
   carries the union of the labels of every episode of the pass.
7. `Control::ResetMemory` clears the episode scope and reports the counts
   in `AgentState::episode_reset`. The clear keeps an empty log with a new
   generation. A pass that read an older generation writes no fact after
   the clear and ends as `reset`. The facts count is the derived facts that
   the memory scope still holds. The memory reset then removes them.
8. `harvest agents erase` reports `episodes_erased` and `facts_erased`. It
   also erases each scheduled pass of the agent. A pass that still runs is
   a failure in the report.
9. The new activities need a worker. `activities()` and
   `sqlite::register` list them. A worker with a hand-picked activity list
   must add them before it serves `Control::ResetMemory`.

A pass ends with one `ConsolidationStop`: `completed`, `nothing_to_do`,
`budget_exceeded`, `invalid_reply`, `disabled` or `reset`.

**Known limit.** The generation check runs before each fact write. A reset
that lands between the check and one write can still keep that one fact.
The memory reset runs after the episode reset, so it removes such a fact
when the write lands first.

**Known limit.** SQLite has no run budget, so its pass always passes the
check. The budget test runs the engine check in a unit test and the
workflow refusal in the in-memory test engine.

**Known limit.** No Postgres or SQLite `EpisodeStore` ships. The memory
store has none either. The trait is the extension point, as for
`MemoryStore`.

## 6. Acceptance criteria and their tests

The tests are in `autumn-harvest-agent/tests/episodes.rs` unless noted.

| Criterion | Test |
|---|---|
| Setting off: an op writes no episode | `with_the_setting_off_an_op_writes_no_episode` |
| Setting on: each op appends one episode | `with_the_setting_on_each_op_appends_one_episode` |
| A crash after the append does not duplicate it | `a_crash_after_the_append_does_not_duplicate_it` |
| A pass over 3 episodes writes facts with source ids | `a_pass_over_three_episodes_writes_facts_with_their_sources` |
| A crash mid-pass and a rerun write no fact twice | `a_crash_mid_pass_and_a_rerun_write_no_fact_twice` |
| A fact from a labelled episode keeps the labels | `a_fact_from_a_labelled_episode_carries_its_labels` |
| A store with no label support refuses the write | `a_store_without_labels_refuses_a_labelled_fact` |
| A run budget of 0 stops the pass before the model call | `a_zero_budget_stops_the_pass_before_its_model_call` (unit, `consolidate.rs`, with the engine `check_llm_budget` and a `QuotaPolicy` of 0) and `a_refused_turn_ends_the_pass_as_budget_exceeded` |
| The pass tokens are in the ledger under the pass run | `the_pass_tokens_are_in_the_ledger_under_its_run` |
| Erase removes episodes and facts, with matching counts | `a_memory_reset_removes_episodes_and_facts_and_counts_them`; `the_report_counts_the_erased_episodes_and_facts` in `autumn-harvest-cli/src/agents.rs`; `erase_also_erases_each_scheduled_pass_of_the_agent` in `autumn-harvest-cli/tests/integration/agents_cli.rs` |
| `max_episodes = 2`: a third episode evicts the oldest, and the next pass counts it | `a_third_episode_evicts_the_oldest_and_the_next_pass_counts_it` |
| The operator picks the pass model | `a_pass_runs_on_the_model_that_the_operator_names` |
| Review fixes | `a_pass_across_a_reset_writes_no_fact`, `a_required_reset_with_no_store_fails` (unit, `consolidate.rs`); `a_mark_of_an_older_generation_changes_nothing`, `a_read_episode_evicted_during_the_pass_is_not_dropped` (unit, `episode.rs`); `a_failed_child_pass_waits_for_more_episodes_and_fails_the_op`, `episodes_with_no_episode_store_fail_the_op_with_a_named_error` |

`formal/verus/src/agent_episodes.rs` proves the eviction and trigger
kernels.
