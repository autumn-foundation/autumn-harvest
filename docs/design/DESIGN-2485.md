# DESIGN-2485: agent-mesh guide statements that do not match the code

Issue #2485.

The issue lists six statements in `docs/agent-mesh.md` and
`docs/agent-adapter.md` that do not match the code. This record checks each
one against `trunk-dev`, picks a fix, and lists the tests.

## Facts on `trunk-dev`

| # | Statement | Code today | Fix |
|---|---|---|---|
| 1 | Section 10: "A peer lookup: that lookup runs again." | A finished lookup replays from history. Only a lookup in flight runs again. | Docs |
| 2 | Section 5: "A delegation runs the first eight checks in the order of the table." | `admit` now runs before `durable_peer`, so the claim holds. The guide does not state the durable checks that follow. The integration guard lists only some of the codes. | Docs and tests |
| 3 | Section 3: the child-settings table | `child_task` also drops the workspace, the tool guidance, the skills, the answer check and the kind. It keeps the effect allowlist, the probe and the draw id. Labels differ for inline and fan-out children. | Docs and tests |
| 4 | Adapter section 3: `approval_signal(step, position, call_id)` | The parameter is `round`. In an inline child, a round is not a step. A fan-out child counts its own rounds. | Docs |
| 5 | Section 3: one run-id form | Inline: `{root run id}:d{n}`. Fan-out: the child execution id. Durable peer: the record holds the peer workflow id. | Docs and test |
| 6 | The guide does not state the roster rules | `discover_peers` drops an invalid name with a warning. Of two peers with one name, it keeps the peer added last. A mandate that names an invalid peer is not valid, so no run reaches the peer. | Docs |

## Brainstorming

- Fix the prose only. This is cheap. But the next drift goes unseen.
- Fix the prose and add guide guards. A guard fails when a fixed fact leaves
  the guide.
- Add behaviour tests for each fact that has none: the durable check order,
  the run id of each mode, and the child workspace.
- Make the order guard exhaustive. Put it next to `every_refusal` in
  `mesh.rs`. There, a new `Refusal` variant does not compile until it is in
  the chain.
- Change the code instead: reject an invalid roster name at build time.
  `AgentHarness::peer` cannot fail, so this needs an API change. This is out
  of scope.

## Reverse brainstorming

How could the fix make things worse?

- A guard that copies whole sentences. Each later edit of the prose breaks
  it. Mitigation: guard row names, values and key phrases only.
- A guard that counts verdicts in prose. It changes with each new verdict.
  Mitigation: compare the table to `every_refusal`, not to a number.
- A run-id table that invites a parser. Mitigation: tell the reader to read
  `DelegationRecord::chain`, not the run id.
- Long sentences that break STE. Mitigation: write one idea in 25 words or
  fewer per sentence.
- A test that passes for the wrong reason. Mitigation: each behaviour test
  sets up two outcomes, so only the stated behaviour passes.

## Six thinking hats

- **White (facts).** Items 1, 3, 4, 5 and 6 are wrong or missing prose. Item 2
  is now true in code. Its durable tail and its guard are incomplete.
- **Red (feel).** A new mesh user trusts tables. A wrong table row costs more
  than a missing sentence.
- **Black (risk).** Prose guards are brittle. `guide_snippets` compiles the
  Rust fences, so a new fence must compile. This change adds no fence. A
  guard that runs only on code PRs lets a docs-only PR break it.
- **Yellow (value).** Each fixed fact gets a test. Drift then fails CI.
- **Green (ideas).** One order guard checks the whole section 5 table against
  the variant order. One fan-out test checks two run-id forms and the skip
  in `n`.
- **Blue (process).** RED: add the guards and the behaviour tests, and see
  the guards fail. GREEN: fix the guides. REFACTOR: drop the narrow guard,
  tighten the prose, run the guards on docs-only PRs, then run the
  comment-hygiene and docs gates.

## Tests

All paths are in `autumn-harvest-agent/`.

| Test | File | Proves |
|---|---|---|
| `the_guide_lists_every_verdict_in_variant_order` | `src/mesh.rs` | The section 5 table lists each verdict once, in variant order. |
| `a_durable_peer_runs_the_mandate_checks_before_its_own` | `tests/durable_peers.rs` | `model_forbidden` comes before the own checks. The id check comes before the effect check. |
| `fan_out_and_inline_children_have_their_own_run_id_forms` | `tests/fan_out.rs` | Fan-out children get execution ids. A later inline child gets `{root run id}:d3`. |
| `an_inline_child_gets_no_workspace`, `a_fan_out_child_gets_no_workspace` | `tests/agent_workspace.rs` | A workspace tool in a child sees no workspace. |
| guide guards | `tests/mesh_docs.rs` | Items 1 to 6 stay in the guides. |

Existing tests cover the rest:

- `a_restart_inside_a_child_resends_nothing_and_looks_up_nothing` covers
  item 1.
- `a_gated_call_in_a_child_waits_on_a_tree_unique_name` and
  `a_parent_round_after_a_child_takes_the_next_round` cover item 4.
- `a_durable_peer_timeout_is_a_named_verdict_with_the_message_key` covers
  the durable run id of item 5.
- `discovery_filters_and_orders_the_roster` covers item 6.
- `a_delegated_child_gets_neither_part` covers the tool guidance and the
  skills of item 3.

CI runs `mesh_docs` and the order guard in the `doc-snippets` job, so a
docs-only PR runs them too.
