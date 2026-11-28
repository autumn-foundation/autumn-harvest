## Agents — swarm mode: a shared board, fenced claims and a merge gate (issue #2814)

Many durable agents can now work on one goal as flat peers. The `swarm`
module of `autumn-harvest-agent` follows the Hugging Face swarm study: a
shared board, small claims, and a merge that must beat main on an eval
that the members never see. The guide is `docs/agent-swarm.md`, and the
design is `docs/design/DESIGN-2814.md`.

- **Board.** Work items in lanes, and posts on channels. A member reads the
  posts after a cursor. A post and a verdict take their `seq` under the lock
  of a counter row, so a reader never skips one.
- **Fenced claims.** A take increments the epoch of the item. A renew, a
  release and a submit pass only for the holder at the current epoch. The
  fence does not check the lease end, as the decision owner fence does not.
  A member holds `SwarmPolicy::max_claims` items, 1 by default.
- **Main and the gate.** A claim checks main out under `SwarmPolicy::root`
  and records the base snapshot. A submit merges the files of the member
  onto the current main per file, against the base. The `SwarmJudge` of the
  operator scores the merged files, and the merge lands only when it passes
  the `Bar` over the stored score of main. One transaction moves main,
  closes the item and logs the verdict. A merge that loses to another one
  merges again onto the newer main.
- **Hidden eval.** The judge is no tool. A verdict shows the scores only
  with `SwarmPolicy::reveal_scores`.
- **Tools.** `SwarmTools` gives `swarm_board`, `swarm_post`, `swarm_claim`,
  `swarm_renew`, `swarm_release` and `swarm_submit`. The member is the
  session of the run, so a durable agent is one member. A submit that runs
  again after a crash returns its first verdict.
- **Stores.** `InMemorySwarmStore`, `SqliteSwarmStore` and
  `PostgresSwarmStore`. A store keeps rows and compares versions, and the
  `swarm::kernel` decides. `Swarm::prune` deletes the main snapshots that no
  claim and no gate in flight needs.

**Migration.** `20261128001506_harvest_swarm_board` adds
`harvest_swarm_items`, `harvest_swarm_posts`, `harvest_swarm_mains`,
`harvest_swarm_merges` and `harvest_swarm_counters`. A trigger refuses each
`UPDATE` of a post or a verdict. No existing table changes. No
`WorkflowEvent` variant, no recorded payload change, no replay impact.

Tests: `formal/verus/src/agent_swarm.rs` proves the claim, the fence, the
three-way pick and the bar for every input. `swarm::kernel::tests` and
`swarm::merge::tests` add unit tests and proptests. `tests/swarm_store.rs`
and `tests/swarm_store_postgres.rs` run one suite on the three stores, with
the guards of each backend and a race of 24 concurrent posts on Postgres.
`tests/swarm.rs` drives the tools, and two durable agents on one SQLite
runtime that share the board and the gate.
