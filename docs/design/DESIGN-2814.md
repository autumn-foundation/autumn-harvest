# Design — swarm mode for durable agents (issue #2814)

The guide is [`docs/agent-swarm.md`](../agent-swarm.md). The code is the
`swarm` module of `autumn-harvest-agent`.

**One migration. No new `WorkflowEvent` variant. No core engine change.**
The migration `20261128001506_harvest_swarm_board` adds five new tables. No
recorded payload changes, so every old history replays the same.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Hugging Face ran 1,000 agents on one goal for nine days ("Scaling Swarms
  to Thousands of Agents", 2026-11-25). The agents were flat peers on a
  message board, in teams of 2 to 4 per lane. A change merged only when it
  beat main on a test deck. The final eval ran apart from the agents, and
  they never saw it. Six earlier setups compared hierarchies, voting and
  strict teams. The authors credit the merge rule over each of them.
- Their failure mode was a merged change that crashed the bot near the
  2,000th commit. The swarm fixed it itself. Their shared state, git and a
  message board, was where the scale showed.
- The `mesh` module of the agent crate runs a delegation tree. Depth is at
  most 3, and a tree has at most 16 delegations. Authority narrows down the
  tree. It has no flat topology, no board and no claim.
- A tool runs in `agent_tool_call`, with one attempt and a 10-minute limit.
  `ToolContext` gives `(run_id, step, call_id)` as an idempotency key. A call
  that was in flight at a crash runs again with the same key.
- A durable agent has its agent id as its session id, and a workspace scope
  `agent:<id>`. A tool that sets `uses_workspace` gets the files. The
  activity saves them after a call that succeeds.
- `WorkspaceStore` keeps content-addressed snapshots by generation and
  digest. A key is the generation as 16 hex digits, a dash and the digest.
- `harvest_agent_store_docs` allows only the stores `episodes` and `memory`.
  A whole board in one document would also make every write race every
  other write of the swarm.
- The overlay log (issue #2371) has the pattern for a store with an
  append-only log on three backends. The core crate holds the Postgres SQL,
  `autumn-harvest-sqlite` holds the SQLite SQL, and the agent crate wraps
  both.
- littleguy measured a Fly.io Sprite on 2026-11-10. It pauses about 60 s
  after the last inbound request. A timer does not fire while it sleeps,
  and an outbound websocket does not keep it awake. A wake took 0.5 s after
  a short idle, and 17 s once after 30 minutes.

### 0.2 Brainstorm — how can Harvest run a swarm?

| # | Idea | Verdict |
|---|------|---------|
| B1 | The board as one keyed entity workflow, with signals for each write. | Rejected. One entity serializes every write of the swarm. A tool would need a request and reply over signals. The history grows with each post. |
| B2 | The board as documents in `harvest_agent_store_docs`. | Rejected. One document races every write. A document per item gives no list by lane. |
| B3 | Dedicated tables, with a compare-and-swap on a `rev` column per item. | **Adopted.** Each item races only its own writes. |
| B4 | The decision logic in each store. | Rejected. Three copies of the rules drift. |
| B5 | The stores keep rows and compare versions. `Swarm` reads, asks a pure kernel, and swaps. | **Adopted.** One kernel decides for every backend, and Verus proves it. |
| B6 | Fence on the lease end. | Rejected. A clock skew between hosts then decides a commit. |
| B7 | Fence on holder and epoch, as the decision owner fence does. | **Adopted.** A take moves the epoch, so a lapsed holder fails once another member takes the item. |
| B8 | Merge by replacing main with the files of the member. | Rejected. It drops each merge that landed after the claim. |
| B9 | A three-way merge per file against the base of the claim. | **Adopted.** It keeps each change of each side, and names a conflict. |
| B10 | Score main again for each submit. | Rejected. It doubles the eval cost, and a flaky eval moves the bar. |
| B11 | Keep the score of main in the main row. | **Adopted.** The bar compares with the stored score. |
| B12 | A judge tool for the members. | Rejected. A member then fits its work to the eval. |
| B13 | The judge on the `Swarm` only, with the scores hidden by default. | **Adopted.** `reveal_scores` lets an operator show them. |
| B14 | A cursor on a global identity column for the posts. | Rejected. Identity values commit out of order, so a reader can skip a post. |
| B15 | A counter row per swarm, locked until the commit. | **Adopted.** The posts of one swarm commit in `seq` order. |
| B16 | Record only the base version of a claim. | Rejected. Two gates can save two snapshots at one version, so a version names no single snapshot. |
| B17 | Record the snapshot reference of the base, and of each merge. | **Adopted.** |
| B18 | A gate as its own durable workflow. | Deferred. The tool call bounds the eval to 10 minutes for now. |
| B19 | A push wake from the board to a member. | Deferred. A heartbeat or an inbound message wakes a member. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Two members hold one item. | A take needs no live lease of another member. Each swap names the `rev` it read. Verus proves the decision. |
| R2 | A lapsed holder submits after another member took the item. | The take moved the epoch. The fence refuses, and the verdict is `fenced`. Verus proves that a new holder fences out the old one. |
| R3 | A claim that runs again after a crash takes a second epoch. | A claim by the holder keeps its epoch and checks out its base again. |
| R4 | A submit that runs again after a crash merges twice. | Each verdict carries the idempotency key of the call, and a key is unique per swarm. The commit checks the key under the lock of the main row. |
| R5 | Two merges land on one main version. | The commit compares the main version under its lock. The loser merges again onto the newer main. |
| R6 | A later merge drops an earlier one. | The three-way merge keeps each change of each side. A proptest checks it. |
| R7 | A change that is worse than main lands. | The bar compares with the score of main. A merge onto a newer main is scored again. |
| R8 | A member reads the eval. | The judge is no tool. A verdict shows no score unless the operator sets `reveal_scores`. |
| R9 | A post or a verdict is rewritten. | A trigger refuses each `UPDATE` on both backends. On SQLite, a second trigger refuses `INSERT OR REPLACE`. |
| R10 | A reader skips a post. | The counter row lock keeps the commit order. A Postgres test runs 24 posts at once. |
| R11 | A prune deletes a snapshot that a gate in flight commits next. | A prune deletes only below the current main version. A gate saves above the main that it read. |
| R12 | A prune deletes the base of a live claim. | A prune keeps the base of each claimed item. A suite case submits from such a base. |
| R13 | A name injects text into a scope or a key. | Swarm, item, lane and channel names have 1 to 64 characters from `a-z`, `0-9`, `_` and `-`. A member id has no control character. |
| R14 | A long eval blocks a worker. | The tool timeout bounds the call. The guide says to run a long eval in a service. |
| R15 | Two concurrent takes by one member both pass the cap, because each counts before the other writes. | A take counts again after its write. A take that sees the cap broken releases its item and returns `at_cap`. Each racing take does so, so the member keeps no more than the cap. `a_raced_take_over_the_cap_gives_its_item_back` (Codex review on #2815). |
| R16 | A prune deletes the snapshot of a merge, and a replay of its verdict then fails. | A replayed or fresh merge checks out the current main, which holds the merge. `gate_prune_keeps_live_bases` replays a pruned merge (Codex review on #2815). |

### 0.4 Decisions

- **Rows, not logic, in the stores.** A store keeps rows and compares
  versions. It decides nothing. `tests/swarm_store_suite` runs one suite on
  each store.
- **Integer scores.** `Score` is `i64` units, so the bar is exact and the
  same on every backend. `Score::from_f64` maps a ratio to micro units.
- **One member, one session.** The tools take the member from the session of
  the run. A durable agent is one member.
- **One root.** `SwarmPolicy::root` names the folder of main in the
  workspace. Files outside it stay with the member.
- **The cap is 1 by default.** A member holds one item at a time, so one
  workspace never mixes two changes.
- **A rejection keeps the claim.** The member improves its change and
  submits again. A merge closes the item.

### 0.5 Out of scope

- A gate as a durable workflow, for an eval longer than the tool timeout.
- A push wake from the board.
- A line merge inside one file.
- Teams with their own channels and leads.

---

## 1. Acceptance criteria

| # | Criterion | Evidence |
|---|-----------|----------|
| AC1 | A claim never takes an item from a live lease of another member, and never takes a closed item. | `theorem_a_take_respects_a_live_lease`, `theorem_a_live_lease_keeps_its_holder`, `gate_lapsed_lease_fences_the_old_holder` |
| AC2 | A new holder fences out the old one. | `theorem_a_new_holder_fences_out_the_old_one`, `gate_lapsed_lease_fences_the_old_holder` |
| AC3 | A merge keeps each change of each side, and names a conflict. | `theorem_a_merge_keeps_every_change`, `a_clean_merge_keeps_every_change`, `gate_names_a_conflict` |
| AC4 | Only a change that passes the bar merges. | `gate_merges_only_a_better_change`, `gate_rebases_onto_a_newer_main` |
| AC5 | A submit that runs again merges once and runs no eval. | `gate_merges_only_a_better_change`, `the_tools_run_a_claim_to_a_merge_and_hide_the_scores` |
| AC6 | A merge commit is all or nothing. | `rows_commit_is_all_or_nothing` on three stores |
| AC7 | Posts and verdicts are append-only, and a cursor skips none. | `sqlite_refuses_a_rewrite_of_a_post_or_a_verdict`, `postgres_refuses_a_rewrite_of_a_post_or_a_verdict`, `concurrent_posts_take_distinct_seq_values` |
| AC8 | The scores stay hidden unless the operator reveals them. | `the_tools_run_a_claim_to_a_merge_and_hide_the_scores`, `an_operator_can_reveal_the_scores` |
| AC9 | Two durable agents share one board and one gate on SQLite. | `two_durable_agents_share_a_board_and_the_gate` |
| AC10 | A prune keeps the live bases and the snapshots a gate can commit, and a replay after a prune works. | `gate_prune_keeps_live_bases` |
| AC11 | A member never keeps more items than the cap, also under a race. | `a_raced_take_over_the_cap_gives_its_item_back` |

## 2. Tests

- `swarm::kernel::tests` and `swarm::merge::tests`: unit tests and
  proptests of the kernel and the merge.
- `formal/verus/src/agent_swarm.rs`: the claim, the fence, the pick and the
  bar for every input.
- `tests/swarm_store.rs` and `tests/swarm_store_postgres.rs`: the store
  suite on memory, SQLite and Postgres, with the guards of each backend.
- `tests/swarm.rs`: the tools, and two durable agents on one SQLite runtime.
