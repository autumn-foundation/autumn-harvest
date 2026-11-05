# DESIGN-2481 — The harness limit governs delegation answers

Issue #2481. Found in the v0.8.0 test drive.

## 1. Problem

A delegation returns the final answer of a peer. All three delegation paths
cut that answer at the constant `DEFAULT_TOOL_OUTPUT_LIMIT`:

| Path | Site |
|---|---|
| Inline child | `delegate` in `workflow.rs` |
| Fan-out child | `merge_child` in `workflow.rs` |
| Durable peer | `peer_outcome` in `workflow.rs` |

`AgentHarness::tool_output_limit` changes the cut of a tool result. It does
not change the cut of a delegation answer. The cut is also silent. The text
ends with `…[truncated]`, and the peer can write that text too.

The owner position on the issue: a cap on agent context is necessary. A
silent cut at a number that no setting controls is the failure. The library
must not hard-code the cap.

## 2. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| The knob | a new harness setting `delegation_answer_limit` · **`AgentHarness::tool_output_limit`, with `Peer::answer_limit` for one peer** · a task setting |
| How workflow code reads it | read the harness in workflow code · a `patched` gate and a new activity · **the recorded peer lookup `agent_discover_peers` carries the limit on each `Peer`** |
| The cut mark | the text suffix only · **a `truncated` key in the delegation JSON, and the suffix** · a separate tool result |
| The operator signal | **a replay-safe warning on each cut, and `DelegationRecord::answer_truncated` in the report** · a metric |
| The byte bound | none, the request cap drops an oversized answer · **a share of the request cap, as a tool result has under its result cap** · the room left in the round |
| A lookup recorded before the change | cut with the harness limit · **cut at the legacy 8 000 characters, with the old JSON shape** |

## 3. Reverse brainstorming

"How could this fix break a run?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| Workflow code reads the harness, so replay on another worker gives another cut. | The discover activity resolves the limit. History records it on each `Peer`. Replay reads the record. |
| A run recorded before the change replays with a new JSON shape. Strict replay then diverges on the next model-turn input. | A recorded `Peer` with no `answer_limit` keeps the old cut and the old shape. |
| An old activity worker answers a new workflow worker in a rolling deploy. | Its peers have no `answer_limit`. The run keeps the old behaviour. It does not fail. |
| The `truncated` flag compares the shown text with the answer. An answer that already ends with the mark then reads as complete. | The flag comes from the character count, not from a text compare. |
| A large limit lets one answer pass the request cap. The round budget then drops the whole answer, and the run ends as `transcript_full`. | The answer is also cut to `max_tool_result_bytes(request_cap)`, a sixth of the cap. That cut sets the flags too. |
| A large answer fits the byte cap but not the context window of the model. | The request cap bounds bytes, not tokens. The operator sets both limits. The mesh guide says so. |
| An older worker replays a history that a new worker wrote. It ignores `answer_limit` and builds the old shape. | The older worker decodes the history and finishes the run. Its next window differs, so the transcript delta sends the whole window once. A strict replay of a history from both versions can report a divergence. |
| A durable agent keeps no delegation records, so the operator misses its cuts. | Each cut logs a warning through `ctx.log_warn`, which replay does not repeat. |
| An embedder turns on `preserve_order` in `serde_json`. The durable result then changes its key order. | The durable result adds its keys in the old order. |
| A per-peer value hides the harness setting. | `Peer::answer_limit` is `None` by default. The docs say which value wins. |
| The report flag changes old reports. | The flag serializes only when it is `true`. |

## 4. Six thinking hats

- **White (facts).** Each delegation path reads its `Peer` from the recorded
  lookup. `truncate` cuts on a character boundary. A tool result gets a
  character cut and a byte cut. Before this change, a delegation answer got
  the character cut only. The 8 000 cap hid that gap.
- **Red (feeling).** A user who raises a limit and sees no change loses
  trust. A parent model that reads a cut answer as complete gives a wrong
  result with confidence.
- **Black (risks).** Replay divergence. A rolling deploy in each
  direction. A double meaning of `None`. An answer that the round budget
  drops whole. Section 3 answers each one.
- **Yellow (value).** One knob for each cut in a run. The parent model and
  the operator can see a cut. A long research answer can come back whole.
- **Green (alternatives).** A `patched` gate also works, but it adds a
  marker event to each run. The recorded lookup is already the version.
- **Blue (process).** Red tests for each path, then the code, then the
  docs. A property test covers the cut kernel. The kernel works on UTF-8
  characters, which the Verus integer kernels in `formal/verus` do not model.

## 5. Decision

1. `Peer::answer_limit: Option<usize>` holds the cut of one peer, in
   characters. The builder `Peer::answer_limit(chars)` sets it.
2. `AgentHarness::discover_peers` resolves `None` to `tool_output_limit`.
   So each recorded lookup from a new worker holds a number.
3. Each delegation path cuts with the limit of its `Peer`. The JSON gets
   `"truncated": true` or `false`. A cut sets
   `DelegationRecord::answer_truncated`.
4. Each answer is also cut to `max_tool_result_bytes(request_cap)` bytes.
   That cut sets the same flags.
5. Each cut logs a warning with the peer, the length and the limits.
6. A recorded `Peer` with no limit cuts at `LEGACY_ANSWER_LIMIT`, 8 000
   characters. Its JSON has no `truncated` key, and no byte cut applies.
   Only histories from before this change take that path.

## 6. Acceptance

| Criterion | Test |
|---|---|
| With `tool_output_limit(N)`, N over 8 000, an inline child answer over 8 000 characters is not cut. | `mesh.rs::the_harness_limit_governs_an_inline_answer` |
| The same holds for a fan-out child. | `fan_out.rs::the_harness_limit_governs_a_fan_out_answer` |
| The same holds for a durable peer. | `durable_peers.rs::the_harness_limit_governs_a_durable_answer` |
| A cut answer has `"truncated": true`, and its record has `answer_truncated`. | `mesh.rs::a_long_child_answer_is_cut`, `fan_out.rs::a_cut_fan_out_answer_is_marked`, `durable_peers.rs::a_cut_durable_answer_is_marked`, `durable_peers.rs::a_one_shot_run_flags_a_cut_durable_answer` |
| A nested cut flags the record of its own peer. | `mesh.rs::a_nested_cut_flags_its_own_record` |
| `Peer::answer_limit` overrides the harness for one peer. | `mesh.rs::a_peer_limit_overrides_the_harness_limit` |
| The lookup resolves and records the limit. | `mesh.rs::discovery_resolves_the_answer_limit`, `mesh.rs::a_new_history_records_the_limit_and_replays` |
| A large limit cuts to the byte share. It does not drop the answer. | `mesh.rs::a_large_limit_still_cuts_to_the_byte_share`, `workflow.rs::a_resolved_lookup_cuts_to_the_byte_share` |
| A history from before the change replays strictly, with the old cut and shape. | `mesh.rs::a_history_from_before_the_answer_limit_replays` (fixture from `trunk-dev`), `workflow.rs::a_legacy_lookup_keeps_the_old_cut_and_shape` |
| The cut flag is true exactly when the cut drops a character. | `harness.rs::the_cut_flag_matches_the_character_count` (property test), `harness.rs::a_text_that_ends_with_the_mark_still_reads_as_cut` |
| The docs name the knob. | `docs/agent-mesh.md` §2 and §3, `docs/agent-adapter.md` §4 |
