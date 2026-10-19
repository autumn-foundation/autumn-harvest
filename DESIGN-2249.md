# Design — Issue #2249: ADR for decision partitions and the fenced owner lease

Issue #2249 asks for an ADR. The ADR records the choices of the stage 1
owner model that are hard to change later. This file is the planning
record. The decision is in
[ADR 0009](docs/adr/0009-decision-owner.md).

**No engine code change. No new `WorkflowEvent` variant. No migration.**
The change adds an ADR, a glossary, a terms note, one engine invariant, a
docs guard and a Postgres lock-mode test.

---

## 0. Planning record

### 0.1 Facts found before the plan

- "Shard" means a logical shard, usually one Postgres database. Colocated
  shards share one database. Neither `docs/sharding.md` nor
  `docs/architecture.md` has a glossary.
- `ShardRouter` hashes the pair `(workflow_name, workflow_id)` with
  `seahash`. `seahash` is stable across processes.
- Cross-type continue-as-new keeps `workflow_id`, shard and queue. The
  predecessor seal and the successor insert are one transaction.
- Reset keeps `workflow_id`. A workflow retry and a fork get a new one.
  A rebalance copies the whole execution row.
- The scanner lease upsert counts holder changes, not acquisitions. The
  same holder that takes back an expired lease keeps its epoch.
- The scanner lease default TTL is 10 s. A TTL must cover
  `MIN_LEASE_TICKS` (3) ticks. The longest TTL is 300 s.
- An `ON CONFLICT DO UPDATE` or an `UPDATE` that changes no key column
  takes `FOR NO KEY UPDATE`. That mode conflicts with `FOR SHARE`, but not
  with `FOR KEY SHARE`.
- A `FOR KEY SHARE` request does not queue behind a waiting `FOR UPDATE`.
- A plain multi-row `UPDATE` locks rows in scan order, not key order.
- `SET LOCAL idle_in_transaction_session_timeout` ends a frozen
  transaction and frees its row locks.
- The DR bump (`bump_generation`) waits for in-flight fenced passes, then
  locks `harvest_shard_generation`. It has a lock timeout.
- The workflow-task persist locks the execution row first
  (`check_paused_and_park`). Its first append then checks the generation
  row. `append_single_event` checks the generation row first.
- Offloaded payloads upload before the fenced transaction opens.
- `harvest_workers` has `version`, `labels`, `status` and
  `drain_deadline_at`. No binary version floor exists.
- The workflow-task claim selects `task_type = 'workflow'` rows.
- The lock-order table in `docs/architecture.md` lists 18 rows. None
  names an owner row.
- `formal/tla` has `ActivityClaim.tla` and `WorkflowTaskClaim.tla`.

### 0.2 Brainstorm — what can each decision be?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Call the unit a "virtual shard". | Rejected. "Shard" stays one physical database. |
| B2 | Call the unit a "decision partition". | **Adopted.** The report uses it. |
| B3 | A count that grows with the fleet. | Rejected. Each change remaps keys. |
| B4 | A fixed count of 512 per physical database. | **Adopted.** It is in the precedent range. |
| B5 | Hash `(workflow_name, workflow_id)`. | Rejected. A cross-type continue-as-new moves the chain to another owner. |
| B6 | Hash `workflow_id` alone. | **Adopted.** One entity chain keeps one owner. |
| B7 | Store the partition on the execution row. | **Adopted.** Continue-as-new and reset copy it. A retry and a fork compute it. |
| B8 | Keep leases in a central database. | Rejected. The lease must fail over with its data. |
| B9 | Reuse the scanner lease upsert as written. | Rejected. Its epoch counts holder changes, and its lock mode does not drain commits. |
| B10 | Bump the epoch on each acquisition, never on renewal. | **Adopted.** Temporal `RangeID` rule. |
| B11 | Check lease expiry in the commit. | Rejected. Safety needs the epoch only. The expiry check fails healthy commits. |
| B12 | The commit takes the owner row `FOR SHARE`. | Rejected. Each renewal then waits behind in-flight commits. |
| B13 | The commit takes the owner row `FOR KEY SHARE`. An acquisition takes it `FOR UPDATE`. | **Adopted.** A renewal never waits, and an acquisition drains. |
| B14 | One holder heartbeat row, not one lease per partition. | Deferred. Measure renewal cost first (#2248). |
| B15 | Hand partitions to a named successor on shutdown. | Rejected. Golem's serial drain failed (golemcloud/golem#3766). |
| B16 | Release all partitions in one statement on shutdown. | **Adopted.** |
| B17 | A per-worker config flag for owner mode. | Rejected. A mixed fleet becomes possible. |
| B18 | A per-shard mode row and a database guard on the legacy claim. | **Adopted.** |
| B19 | Bump the DR generation to fence old binaries. | Rejected. DR fencing is optional, and the bump stops every worker. |

### 0.3 Reverse brainstorm — how can this ADR cause harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A stale owner commits after a takeover. | The commit checks holder and epoch under `FOR KEY SHARE`. The takeover waits for it. |
| R2 | A renewal waits behind commits and the lease lapses under load. | Renewal changes `lease_until` only. `FOR NO KEY UPDATE` does not wait for `FOR KEY SHARE`. |
| R3 | A takeover adds a deadlock. | One owner row per transaction, taken before any other lock. An acquisition holds no other lock. |
| R4 | The same holder takes back a lapsed lease and keeps the epoch. | Each acquisition increments the epoch. The holder drains its own decisions first. |
| R5 | A fenced owner retries at the stale epoch. | The owner unloads the partition and does not retry. |
| R6 | A crash leaves partitions without an owner for a long time. | Lease TTL plus one acquisition scan bounds it. The default is 11 s. |
| R7 | An old binary claims workflow tasks on an owner-mode shard. | A database guard refuses the claim. The enable step refuses while an old worker is live. |
| R8 | A DR failover keeps old-region owners in the replicated rows. | The DR bump expires every owner lease in its transaction. |
| R9 | A cell pool takes partitions of a shared shard. | A worker takes partitions only on shards in its effective `shard_assignments`. |
| R10 | Owner memory acknowledges a signal before a durable write. | Invariant: an acknowledgement follows a durable write. |
| R11 | Glossaries drift between two pages. | `sharding.md` defines the terms. `architecture.md` names the unit and links to it. A guard checks both. |
| R12 | Line citations go stale. | The ADR cites symbols, not line numbers. A guard refuses `.rs:<line>`. |
| R13 | The report gains an open question and the ADR misses it. | The guard counts the `?` marks in the open-questions paragraph. It also lists each stage 1 risk and requires a row for it. |
| R14 | A stale owner claims a task after a takeover, and the task stays `RUNNING`. | The owned claim checks the epoch. The acquisition re-pends the partition's tasks. A fenced worker re-pends its own claim. |
| R15 | A frozen owner holds an open commit, and takeover waits for minutes. | The decision transaction sets a local idle timeout of at most the TTL. A test proves it frees the row. |
| R16 | Commits keep passing while an acquisition waits, and the acquisition starves. | The holder stops new commits at its local deadline and before a release. A test proves the queueing fact. |
| R17 | Two multi-row owner statements deadlock. | Lock owner rows with `ORDER BY partition` first. |
| R18 | A replacement worker with the same id renews or releases the old lease. | The holder is a per-process token. Renew, release and revoke match holder and epoch. |
| R19 | A task claimed the legacy way stays in flight across the switch. | The enable step re-pends such tasks in the same transaction as the guard and the switch. |
| R20 | A rebalance copies a partition into a database with another count. | The rebalance computes it again when the counts differ. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. Postgres gives the fence for one row
  lock. The scanner lease and the DR fence give the patterns.
- **Red (feelings).** A count fixed forever feels risky. A new physical
  shard can use a new count, so the risk has a way out.
- **Black (risks).** Section 0.3. The main risks are the renewal lock
  mode, a new deadlock and a mixed fleet. Each has a recorded rule.
- **Yellow (benefits).** One owner per entity chain. Cache hits survive
  the sticky expiry. Four of seven claims leave the hot path in stage 1b.
- **Green (ideas).** A holder heartbeat row. Group commit per partition.
  A lock-order row for the owner row when #2251 ships.
- **Blue (process).** Red: a docs guard fails. Green: the ADR, glossaries
  and invariant pass it. Refactor: shorten. Then a review from several
  angles.

---

## 1. Change

1. `docs/adr/0009-decision-owner.md` records each decision, the stage 1
   question table and a mermaid sequence diagram.
2. `docs/sharding.md` gains the glossary. `docs/architecture.md` gains a
   terms note that links to it.
3. `CLAUDE.md` gains the owner fence rule under engine invariants.
4. `decision_owner_docs.rs` guards the facts, not the prose. The `lint`
   job runs it on docs-only changes.
5. `decision_owner_lock_modes_db_tests.rs` proves each lock mode that the
   ADR relies on, on Postgres 16.

A review from four angles (protocol safety, code facts, tests and CI,
acceptance criteria and style) found R14 to R20. The ADR closes each.

---

## 2. Acceptance criteria

| AC | Evidence |
|----|----------|
| ADR merged with a mermaid sequence diagram for acquire, commit, takeover and fenced commit. | `adr_records_an_accepted_decision`, `adr_has_the_four_flow_sequence_diagram` |
| Every stage 1 open question has an answer or a deferral. | `every_stage_1_question_has_an_answer_or_a_deferral` |
| `CLAUDE.md` gains the owner fence rule. | `claude_md_states_the_owner_fence_invariant` |
| The glossaries name the unit. | `glossaries_define_the_decision_partition` |
| Each decision the issue lists is recorded. | `adr_records_every_decision_the_issue_lists` |
| Cited facts are real. | `adr_cites_only_real_code`, `adr_cites_no_line_numbers` |
| The lock modes hold on Postgres. | `decision_owner_lock_modes_db_tests` (9 tests) |
