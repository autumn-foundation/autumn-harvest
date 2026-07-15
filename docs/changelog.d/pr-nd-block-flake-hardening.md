## Trunk hardening — de-flake `nd_blocked_cycle_does_not_emit_signal_unhandled` (Refs #1074)

**Test-code-only.** The issue #603/#684 engine test
`nd_block_tests::nd_blocked_cycle_does_not_emit_signal_unhandled` timed out
intermittently on loaded CI runners. Its `wait_for_nd_block` poll helper
(~20s budget) exhausted because the test relied on worker2's own
timer-scan → append `TimerFired` → re-pend the parked task → claim →
divergent-replay chain to complete inside the budget: the parked workflow
task's `scheduled_at` had been set to the timer's original `fires_at`
(`now + 300s`) at suspension, so backdating `harvest_timers.fires_at` alone
(`fire_timer_now`) does not make the task re-claimable within the poll
window.

Fix (mirrors the already-passing sibling
`divergent_replay_blocks_instead_of_failing`): call
`make_task_claimable_now(&mut conn, exec_b)` immediately after
`fire_timer_now(...)`, so the parked task becomes immediately claimable and
worker2 replays the recorded history and ND-blocks promptly (the test now
finishes in ~2s instead of racing the timer-scan chain against the 20s
budget). Belt-and-braces: widen the local `wait_for_nd_block` loop bound
from 400 to 800 iterations (~20s → ~40s) — this only extends how long a
*failing* wait tolerates before panicking; a satisfied wait still returns
early, so no passing test's timing semantics change.

No production code touched. Verified 5×-green against a local Postgres 16
via `HARVEST_TEST_DATABASE_URL` (no Docker in this sandbox). Refs #1074.
