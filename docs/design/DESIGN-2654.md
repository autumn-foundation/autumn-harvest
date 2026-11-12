# Design — hot path round trips that the statement cache audit left

The statement cache audit ([`performance-hot-path-statement-cache.md`](../performance-hot-path-statement-cache.md))
measured six costs and changed none of them. This record plans each one.
Each item is fixed with a paired run of the `throughput` cell, or kept with
its reason. [`performance-hot-path-round-trips.md`](../performance-hot-path-round-trips.md)
holds the measurement.

**No migration. No new `WorkflowEvent` variant. No statement of the claim
changes its text.**

---

## 0. Planning record

### 0.1 Facts

| Fact | Effect on this design |
|---|---|
| The `throughput` cell is CPU-bound on the 4-core box. A third claim loop gives no more throughput. | The gain must come from fewer statements and fewer round trips. |
| `diesel-async` pipelines statements that run on a shared `&AsyncPgConnection` when their futures poll together. | A pipeline needs no new driver. |
| The driver sends a statement on the first poll of its future. A statement that is not prepared yet holds the statement cache lock while it prepares. That lock is fair. | A pipeline keeps the order of its futures, prepared or not. A test must still prove it. |
| Postgres runs pipelined statements in arrival order. In `READ COMMITTED` each statement takes its own snapshot. | The queue lock still runs before the snapshot of the pause recheck. |
| A release statement of the claim matches only a `RUNNING` row of this worker. | A recheck after an earlier release changes nothing. |
| `handle_suspended_workflow` recovers from a full quota or an unavailable shard and then commits. | The savepoint of the suspension body is load-bearing only when a step can raise one of these. |
| Every other error of the suspension body rolls back the persist transaction. | Without a savepoint, those errors change nothing that commits. |
| A lock row comes only from a grant that appends `MutexGranted`. A waiter row has no event. | A history gate can skip the lock read, never the waiter read. |
| A cached statement must name its result columns. A generic plan cannot prove a partial-index predicate from a bind. | Only a scanner whose partial-index predicate is a literal can be cached. |

### 0.2 Brainstorm — how can the hot path send fewer statements?

| # | Idea | Verdict |
|---|---|---|
| B1 | Pipeline the queue lock and the claim rechecks. | **Adopted.** Three round trips become one. |
| B2 | Move the queue lock into the claim statement. | Rejected. The lock must come before the snapshot of the pause recheck (issue #619). One statement has one snapshot. |
| B3 | Merge the two pause rechecks into one `UPDATE`. | Rejected. Each recheck has its own residual-window argument. The pipeline gives the same round trip saving and keeps each statement. |
| B4 | Pipeline the due-timer and pending-signal reads of the wake ingest. | **Adopted.** One round trip less per decision. |
| B5 | Pipeline the execution load and the history load. | Rejected for now. Owner mode needs the execution before it chooses the history read. An offloaded history runs more statements. The error of each read has its own path. |
| B6 | Skip the second history load when the ingest appends nothing. | Rejected. That load also reads events that a concurrent writer appended during the ingest. Without it, a decision persists at an older event id more often and collides on the key. |
| B7 | Drop the unlocked pause read before the persist. | Rejected. It parks a paused run before the non-determinism block, the deadlock requeue, the panic retry, the cascade reads and the history-cap fail. |
| B8 | Open the suspension savepoint only for a detached child spawn or a fork. | **Adopted.** Two statements less per decision of a plain fan-out. |
| B9 | One statement reads the commit state and sends the wakes when every held note is ready. | **Adopted.** The read alone runs when a note is aborted, open or on another server. |
| B10 | Move the coalescing of the notify sender into SQL. | Rejected. B9 sends the same set with the same Rust coalescing. |
| B11 | Cache the poll-tick scanners whose partial-index predicates are literals. | **Adopted** for nine statements. |
| B12 | Cache every poll-tick scanner. | Rejected. Four scanners bind a value that a partial index needs as a literal. |
| B13 | One probe and one key read for the terminal mutex sweep. | **Adopted.** Five statements become two. |
| B14 | Skip the lock read of the sweep when the history has no `MutexGranted`. | Rejected. B13 saves the same read on every terminal path, with no history. |

### 0.3 Reverse brainstorm — how can these changes do harm?

| # | How to make it harmful | Mitigation |
|---|---|---|
| R1 | The pause recheck runs before the queue lock, and a pause slips past the claim. | `claim_recheck_pipeline_db_tests` checks that the recheck sees the lock on a cold, a warm and a mixed connection. With the order swapped, three of its tests fail. |
| R2 | A pipelined recheck runs after the lock failed and keeps a claim. | A recheck can only release. A Verus proof shows that the pipeline keeps a row exactly when the sequential rechecks keep it. |
| R3 | A failed statement in the pipeline hides the cause. | `join4` waits for every reply. The lock error comes first. |
| R4 | The suspension body fails with no savepoint, and a partial write commits. | Only a quota or shard error commits after an error, and those keep the savepoint. Each other error rolls back the persist. |
| R5 | The failure write then fails on the aborted transaction and replaces the cause. | `fail_execution_on_error` reports the first error when the second is the aborted-transaction reply. |
| R6 | The combined notify statement sends a wake for an aborted or open note. | The send runs only when no status is aborted or open and the server matches every note. `a_rolled_back_write_sends_no_wake` covers it. |
| R7 | A failed `pg_notify` now fails the read, and the sender stops reporting health. | The tick reads again with no send. The second send counts its wakes as lost, as before. `a_failed_send_is_counted_and_the_append_commits` covers it. |
| R8 | A cached scanner gets a bad generic plan. | Only scanners with literal partial-index predicates are cached. The others stay unnamed, each with its reason. |
| R9 | The merged sweep frees a key twice, or wakes a waiter too often. | The waiter part skips each key that the lock part freed. `mutex_terminal_sweep_db_tests` checks the rows. |
| R10 | A cached outbox scanner fails after a migration adds a column. | The scanner names its columns. A cached statement with named columns survives an added column (`cached_stmt_db_tests`). |

### 0.4 Six thinking hats

- **White (facts).** One workflow sends about 256 statements. The claim
  sends six round trips. The cell is CPU-bound.
- **Red (feeling).** The claim barrier of issue #619 is the riskiest part.
  Its order must be easy to see and proven.
- **Black (risks).** A reordered pipeline. A lost error. A partial write.
  A wake for an aborted write. A bad generic plan. See R1 to R10.
- **Yellow (benefits).** Fewer round trips per claim and per decision, less
  planning per poll tick, and no change to any statement text of the claim.
- **Green (alternatives).** B2, B3, B5, B6, B7, B10, B12 and B14 stay on
  record with the reason that each is not adopted.
- **Blue (process).** Spec and proof first (the recheck verdict), then a
  failing test, then the change, then the census and the paired runs.

## 1. Items

| # | Item | Decision |
|---|---|---|
| 1 | Claim: six round trips | Fixed. The lock and the rechecks go in one pipeline. |
| 2 | Decision: four reads, a second history load, an unlocked pause read | The timer and signal reads are pipelined. The other three stay, with reasons B5, B6 and B7. |
| 3 | Savepoint of the suspension persist | Fixed. It opens only for a detached child spawn or a fork. |
| 4 | Notify sender: two statements per tick | Fixed. One statement in the common tick. |
| 5 | Unnamed poll-tick scanners | Nine statements fixed. Four stay unnamed, with reasons. |
| 6 | Terminal mutex sweep | Fixed. One probe and one key read. |

## 2. Acceptance

- Each fixed item has a paired before-and-after run of the `throughput`
  cell and a census.
- Each kept item states why it stays.
- The claim order has a test that fails when the order is wrong.
- The recheck verdict has a Verus proof.
