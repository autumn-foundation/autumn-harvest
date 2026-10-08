## Phase — Give the codec-rotation revalidation deadline its own column (issue #1258)

Fixes a liveness bug in the codec-rotation re-encryption sweep (issue
#948): on a busy, already-converged shard, the periodic re-census that
finds a row committed below the cursor could be starved indefinitely.

**The bug.** `harvest_codec_rotation_cursor.updated_at` did two jobs: last
write time, and the deadline `claim_completed_cursor_revalidation` throttled
on. `write_cursor` reset it on every batch that advanced the cursor,
including a batch that converted nothing but still moved `last_event_id`
forward — the steady state on a converged shard that keeps receiving
traffic. A shard that gets at least one row within every revalidation
interval (five minutes) therefore never saw the deadline come due, and a
row that committed below the cursor after the pass converged was never
found again. Safety was unaffected (`retire_codec_key` censuses
`harvest_events` directly and never reads the cursor), but rotation could
never converge on exactly the shards that matter most: busy ones.

**The fix.** A new column, `next_revalidation_at`, is the deadline now.
`write_cursor` arms it only when a pass transitions to complete, clears it
only when a pass transitions away from complete, and leaves it untouched
on an ordinary advancing write that keeps an already-complete pass
complete. `claim_completed_cursor_revalidation` claims and rearms this
column instead of `updated_at`. `updated_at` keeps its original,
operator-facing meaning: the last time the row was written.

**Migration.** `20260914165542_harvest_codec_rotation_revalidation_deadline`
adds the nullable column and backfills existing completed cursors to `NOW()`
(due immediately — the safe direction). Column comments on both
`updated_at` and `next_revalidation_at` state which is which.

**A second hazard, found in review.** The arm/clear `CASE` first checked only
whether `completed_at` transitioned away from `NULL`. A key rotation
followed by a rollback can complete a fresh pass while the *stored*
`completed_at` is already non-`NULL` — belonging to a different key's prior
pass — so that check alone inherited the other pass's stale deadline
instead of arming a new one. The `CASE` also arms fresh when
`active_key_id` changes, even if both the old and new `completed_at` are
set.

**Tests.** Three new integration tests in `codec_rotation_db_tests.rs`:
`an_ordinary_advancing_write_does_not_reset_the_revalidation_deadline`
pins the exact mechanism (an advancing write over an already-complete pass
must move `updated_at` but not `next_revalidation_at`);
`a_busy_shard_still_revalidates_once_the_deadline_is_due` reproduces the
reported scenario end to end (a row committed below the cursor survives
several busy ticks before the deadline, and is converted once the deadline
comes due despite continuous traffic on either side of it); and
`a_fresh_pass_under_a_different_key_arms_its_own_deadline` covers the
rollback hazard above.

`docs/operations/codec-key-rotation.md` and the `GET /admin/codec/rotation`
API contract document the two columns' separate meanings.

Coordinated with issue #1257 (already merged as autumn-foundation/autumn-harvest#1492):
this change touches the same `write_cursor` statement but only adds a
column to its `SET` list — the CAS `WHERE` guard from #1257 is unchanged.
