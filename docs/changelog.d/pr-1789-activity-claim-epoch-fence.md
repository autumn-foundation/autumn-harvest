## Phase — Claim-epoch fence on activity ownership writes (issue #1789)

A worker whose liveness row went stale could still write the result of an
activity. The orphan reclaimer requeued the row, and a second worker claimed
it. The first worker's completion, failure, retry requeue or heartbeat still
matched `state = 'RUNNING'`. Its result could replace the live attempt's
result, and its heartbeats could overwrite the live checkpoint.

**What shipped.** `queue::TaskClaim` holds the claim epoch
`(task_id, worker_id, attempt)`. One predicate, `claim_held` in `queue.rs`, fences
every activity owner write in the same statement:

- New fenced writes: `complete_claimed_task`, `fail_claimed_task`,
  `requeue_claimed_task_for_retry` and `defer_claimed_rate_limited_task`.
  Each returns `ClaimWrite::{Applied, LeaseLost}`.
- `lock_claim_for_update` replaces `worker::task_state_for_update` and
  `queue::task_state_for_update`. The start fence, both finalize paths, the
  in-worker schedule-to-close and session-acquire timeouts, and
  `run_transactional` use it. `timeout::task_state_for_update` stays. The
  broken-session reclaim in `sessions.rs` uses it, and that is not an owner
  write.
- `record_heartbeat` now takes a `&TaskClaim` and returns `ClaimWrite`.
  `spawn_heartbeat_flusher` now takes a `TaskClaim`. This is a breaking API
  change.
- A lost lease appends no event and returns `Ok`, with a debug log. The
  heartbeat flusher and the cancellation observer cancel the activity's
  token. `ctx.heartbeat()` returns `ActivityCancelled` when a later claim
  holds the row.

The unfenced `complete_task`, `fail_task`, `requeue_for_retry` and
`defer_rate_limited_task` stay for timeouts, cancellation and operator
actions. The protocol and its invariant are in `docs/architecture.md`,
"Activity claim epoch".

No new `WorkflowEvent` variant, no migration, no schema change.

**Tests, red then green.** `tests/integration/activity_claim_epoch_tests.rs`
has 12 DB tests. Each one runs the real orphan-requeue statement between two
claims. Before the fix, 8 tests failed. A stale completion, failure, retry,
deferral, start and heartbeat all changed the later attempt, and lease loss
did not cancel the token. The "B completes, then A completes late" test
passed before the fix too. The history check already blocked that order.
