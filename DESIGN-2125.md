# Design — Issue #2125: stage a NOTIFY note without a probe round trip

Each staged NOTIFY note costs one extra round trip. The stage call reads
`txid_current()`, `current_database()` and `pg_postmaster_start_time()` in
its own statement. On the e2e throughput scenario that is 15 statements per
workflow and 5.1% of all calls. This change lets the write that stages the
note return those three values itself.

**No new `WorkflowEvent` variant. No schema change. No replay impact.** The
post-commit gate and the transaction boundaries do not change.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Finding 1 of the issue (the pool recycle ping) shipped before this change.
  `pool::engine_manager` and `pool::VERIFY_IDLE_AFTER` cover it. This change
  covers finding 2 only.
- Findings 3 to 5 are under the 5% floor or inherent. This change does not
  touch them.
- `notify::stage_now` sends one probe per call. In a diesel transaction it
  reads `txid_current()`. Outside one it reads `txid_current_if_assigned()`.
- The sender routes a note by the database fingerprint. It gates the note on
  `txid_status` of the transaction id.
- `txid_current()` returns the id of the top-level transaction, also inside a
  savepoint. Any statement of the transaction can read it.
- `notify::coalesced` holds the notes of a scope and stages them once. Only
  the workflow-task transactions use it.
- A pooled connection moves in memory between checkouts. A cache keyed by
  its address is not safe across transactions.
- Baseline, 240 workflows with 3 activities each: 3,600 probes, 15 per
  workflow. A statement log groups them by transaction:

| Transaction | Per workflow | Probes each | Event insert |
|-------------|--------------|-------------|--------------|
| Workflow start | 1 | 2 | yes |
| Schedule an activity (coalesced) | 3 | 1 | yes |
| Activity start | 3 | 1 | yes |
| Activity completion and wake | 3 | 2 | yes |
| Workflow completion (coalesced) | 1 | 1 | yes |

Each transaction that stages a note also inserts events.

### 0.2 Brainstorm — how can a stage skip the probe?

| # | Idea | Verdict |
|---|------|---------|
| B1 | The event `INSERT` returns the three values. The stage uses them. | **Adopted.** The write already runs. The id is the id of the same transaction. |
| B2 | The task `INSERT` of `queue::enqueue` and `queue::enqueue_batch` returns them too. | **Adopted.** The start transaction stages a task note after its own write. |
| B3 | A `coalesced` scope keeps the values of its first stamped write. | **Adopted.** Every note of the scope belongs to one transaction. |
| B4 | Wrap the activity-completion transaction in `coalesced`. | **Adopted.** Its wake note then uses the values of the event insert. |
| B5 | Wrap the start transaction in `coalesced`. | Rejected. The body has 750 lines. B2 removes its probe with a small change. |
| B6 | Cache the values per connection. | Rejected. A connection moves between checkouts. A stale id would wake a reader before commit. |
| B7 | Send the probe in the same pipeline as the write. | Rejected. The statement count stays the same. |
| B8 | The caller pushes the note after `COMMIT` returns, with no gate. | Rejected. It changes the gate. A cancelled caller loses the wake after commit. |
| B9 | Skip the fingerprint when only one sender exists. | Rejected. A write to another server would read the wrong commit state. |
| B10 | Add the values to the `RETURNING` of `wake_workflow_task`. | Rejected. Its notes come from two statements, one of them a read. B4 covers it. |
| B11 | A trigger writes the id to a side table. | Rejected. It needs a migration. `harvest_events` is append-only. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A stamp from one transaction gates a note of another. The reader wakes before commit. | The stamp is a local value of the write function. A `coalesced` scope keeps it only when the scope opens inside a diesel transaction. Then all its writes share one transaction. |
| R2 | A role that cannot run `txid_current()` now fails a write that used to succeed. | A stamp is read only inside a diesel transaction, and only when a sender is ready. There the probe also fails the transaction today. Autocommit and raw `BEGIN` writes keep the probe. |
| R3 | A stamp makes the fallback think a raw `BEGIN` block is open. It sends a `SAVEPOINT` outside a block. | The fallback reads the raw block state only from its own probe, never from a stamp. |
| R4 | The insert of a write whose own transaction commits before the stage returns a stale id. | That id is committed. The gate sends the note at once, as the probe reading of `NULL` did. |
| R5 | The note is sent before commit when its write is in a savepoint that rolls back. | `txid_current()` returns the top-level id. The gate waits for the top-level commit, as before. |
| R6 | `RETURNING` adds bytes per row to a large event batch. | Three small values per row. The event payload is much larger. |
| R7 | The row count changes, because `execute` becomes `get_results`. | These inserts have no `ON CONFLICT`. The count is the number of rows returned. Tests read the count. |
| R8 | The `coalesced` wrap changes when the completion transaction stages. | The scope stages before the body returns, so before commit. The fallback merges the two notes into one statement. |
| R9 | A query matcher, such as a ledger script, depends on the old insert text. | The ledger script of this change matches the probe text only. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. 15 probes per workflow, all in
  transactions that insert events.
- **Red (feelings).** Data in a `RETURNING` clause that the write does not
  need feels odd. It is the cheapest place for it: the write already runs.
- **Black (risks).** Section 0.3. The main risk is a stamp of the wrong
  transaction. The rule "inside a diesel transaction only" closes it.
- **Yellow (benefits).** 15 fewer statements per workflow. A shorter
  transaction holds its row locks for less time. The gate does not change.
- **Green (ideas).** Stamp more writers, such as signals and timers. Expose
  a probe counter as a metric. Stamp the decision-boundary insert.
- **Blue (process).** Red tests, then green code, then refactor. Then a
  review from several angles, then a ledger capture.

---

## 1. Change

1. `notify::Stamp` holds the transaction id and the database fingerprint.
   `notify::stamp_returning()` gives the three `RETURNING` expressions.
2. `notify::stamp_wanted(conn)` is true inside a diesel transaction when a
   sender is ready. Only then does a write ask for a stamp.
3. `store::insert_event_rows`, both offloaded appends, `queue::enqueue` and
   `queue::enqueue_batch` read a stamp from their insert when it is wanted.
   They pass it to the stage call.
4. `stage_now` uses a stamp in place of its probe. The fallback still probes
   for itself.
5. A `coalesced` scope that opens inside a diesel transaction keeps the first
   stamp. Its single stage at the end uses it.
6. `finalize_activity_completion_write` runs its body in `coalesced`.

---

## 2. Known limits

- A write outside a diesel transaction still sends the probe. A raw `BEGIN`
  block is one example.
- A stage with no stamp still sends the probe. Signals, timers, triage and
  durable streams are examples. None is on the throughput path.
- The values come back once per inserted row. The stage uses the first row.

---

## 3. Tests and acceptance criteria

| AC | Test |
|----|------|
| An append in a transaction stages its note with no probe. | `an_append_in_a_transaction_sends_no_stage_probe` |
| An enqueue in a transaction stages its note with no probe. | `an_enqueue_in_a_transaction_sends_no_stage_probe` |
| A `coalesced` scope stages with the stamp of its append. | `a_coalesced_scope_stages_with_the_stamp_of_its_append` |
| A stamped note still waits for the commit. | `a_registered_pool_wakes_after_commit_and_not_before` |
| A 3-activity workflow sends no stage probe. | `a_three_activity_workflow_sends_no_stage_probe` |

The ledger script `autumn-harvest/scripts/notify_stage_probe_perf_repro.sh`
reruns the e2e throughput capture. Section 4 has the result.
