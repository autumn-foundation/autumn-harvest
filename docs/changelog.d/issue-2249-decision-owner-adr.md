## Phase — ADR 0009: decision partitions and the fenced owner lease (issue #2249)

[ADR 0009](../adr/0009-decision-owner.md) records the stage 1 owner model of
epic #2247. The ADR changes no engine code. Issues #2250 to #2254 build it.

- The unit is a **decision partition**: 512 fixed hash buckets per physical
  database, hashed from `workflow_id` alone. A cross-type continue-as-new
  keeps its owner.
- The owner row lives in the history's own database. Each acquisition
  increments the epoch. A renewal never changes it.
- A commit takes the owner row `FOR KEY SHARE`. An acquisition takes it
  `FOR UPDATE`, which drains in-flight commits. A renewal does not wait.
- The lock order is generation row, then owner row, then execution row.
- A fenced owner unloads the partition and never retries.
- The lease TTL is 10 s, renewed every TTL / 3. A graceful stop releases
  all partitions in one statement.
- Owner mode is a per-database switch with a database guard on the legacy
  workflow-task claim.

`docs/sharding.md` gains a glossary. `docs/architecture.md` gains a terms
note. `CLAUDE.md` gains the owner fence invariant.

**No new `WorkflowEvent` variant. No migration. No engine code change.**

Tests: `decision_owner_docs` guards the ADR facts.
`decision_owner_lock_modes_db_tests` proves each owner-row lock mode on
Postgres 16, including `a_plain_epoch_update_does_not_wait_for_a_commit`
and `a_commit_does_not_queue_behind_a_waiting_acquisition`.

`DESIGN-2249.md` holds the design record, with brainstorm, reverse
brainstorm and six hats.
