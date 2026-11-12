## Hot path round trips (issue #2654)

The statement cache audit left six measured costs in place. This change
fixes four of them and two in part. It keeps the rest with a reason.
No migration. No new `WorkflowEvent` variant. No claim statement changes
its text.

- **Claim.** The queue lock and the post-claim rechecks go to the server in
  one pipeline. A claim that lands on a row takes four round trips, not
  six. The server runs the statements in order, so the lock still comes
  before the snapshot of the pause recheck (issue #619).
  `claim_recheck_pipeline_db_tests` proves the order on a cold, a warm and
  a mixed connection. `formal/verus/src/claim_recheck.rs` proves that the
  pipeline keeps a row exactly when the sequential rechecks keep it.
- **Decision.** The due-timer and pending-signal reads of the wake ingest go
  in one pipeline. The suspension persist opens its savepoint only for a
  detached child spawn, a race with a losing child, or a fork. Only those
  steps raise an error that its caller recovers from and commits.
  `fail_execution_on_error` reports the first error when the failure write
  fails on the aborted transaction.
- **Notify sender.** One statement reads the commit state and sends the
  wakes when every held note is ready. Otherwise the tick sends as before.
- **Poll tick.** The external outbox claims, the broken-session scan and
  the start-idempotency purge stay prepared. So do the batch and debounce
  due scans, the scanner lease upsert, the active codec key read, the
  outbox retry scan and four table probes. The outbox and session scans
  name their columns.
- **Terminal mutex sweep.** One table probe and one key read replace three
  probes and two key reads.
- **Kept, with reasons:** the execution and history reads of a decision,
  and the second history load after the wake ingest. The unlocked pause
  read before the persist stays, and so do four poll-tick scanners whose
  partial indexes need a literal.
- Measured at 1 shard on one 4-core box: `throughput` 71.8 to 78.5
  workflows/s, the median of six alternating pairs. Statements per run fall
  from 328,863 to 307,677, and planner runs from 13,564 to 9,387. See
  `docs/performance-hot-path-round-trips.md`.
