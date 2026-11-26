## Lean claim tier (follows issue #2680)

A worker claim loop now sends a lean claim statement in front of the window
statement of issue #2680. The lean statement drops the plan nodes of
concurrency keys, rate limits, build pins and capability requirements. It
is exact for a seek window whose rows use none of those. In every other
case it takes no row and no lock, and it sets a flag. The claim then sends
the unchanged window statement in the same transaction. So a claim takes
the same row with and without the lean statement.

- **Text.** `queue::lean_claim_query_for` derives each of the twelve lean
  texts from the final window text of its variant. Each edit replaces an
  exact text and asserts its count, so a changed gate panics at first use.
  No existing claim text changed. The batch claim, the by-id claim and the
  by-execution claim have no lean form.
- **Dispatch.** `queue::claim_task_with_lean` takes the `LeanSkip` of the
  calling claim loop. A lean flag makes the next claims of that loop send
  the window statement first: 1 claim, then 2, then 4, up to 64. A lean
  statement that takes a row ends the skip. The skip counts claims and
  reads no clock. Each poll loop and each follower holds one skip per
  shard. A caller with no state, such as `queue::claim_task`, keeps the
  window statement first.
- **Switch.** `WorkerConfig::with_claim_lean(false)` turns the lean
  statement off. It is on by default. `GET /admin/config` reports
  `claim_lean`. `WorkerConfig` and `WorkerRuntimeConfig` gain the public
  field `claim_lean`. Add `claim_lean: true` to a struct literal. The
  operator guide is `docs/operations/claim-lean.md`.
- **No migration, no new event.** The change adds no table, no column and
  no `WorkflowEvent` variant.

Evidence: `formal/verus/src/claim_window.rs` proves the model of the lean
step and the skip bound. `lean_claim_db_tests.rs` compares the lean-first
claim with the window-first claim over random queue states for the twelve
variants, in owner mode and with an open and a closed fence. It commits a
peer claim between the snapshot and the row lock of a lean statement. It
counts the statements of a claim loop on a plain queue and on a queue with
a keyed row at its head.
