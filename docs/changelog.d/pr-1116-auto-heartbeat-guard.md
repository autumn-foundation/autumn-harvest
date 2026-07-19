## Phase 3.53 — Auto-heartbeat guard for long-running activities (issue #682)

**Before:** an activity that runs longer than its `heartbeat_timeout` but makes
steady progress (a long transcode, a big batch job) had to either sprinkle
`ctx.heartbeat(..)` calls through its body or hand-roll a `tokio::select!` +
ticker in every such activity, or risk being spuriously reclaimed by the
heartbeat-timeout scanner and dead-lettered. **After:** one line —
`let _guard = ctx.start_auto_heartbeat_default()?;` — installs a background
ticker that keeps the activity's `last_heartbeat_at` fresh for as long as the
guard lives.

New `ActivityContext` surface: `start_auto_heartbeat(interval)` and
`start_auto_heartbeat_default()` (derives `interval = heartbeat_timeout / 3`),
both returning a `#[must_use]` RAII `AutoHeartbeatGuard` (re-exported from
`lib.rs` and the prelude alongside the sibling RAII handles `TimerHandle` /
`Session` / `RaceWinner`); plus a `with_heartbeat_timeout(Duration)` builder and
a `heartbeat_timeout` field the worker populates from the task row via the
internal `with_heartbeat_timeout_opt` (the single worker.rs edit, on the
`new_with_cancellation_check(..)` dispatch builder chain).

**#151 read-path preservation.** A liveness-only auto-heartbeat that pings
before any manual `ctx.heartbeat(..)` persists a JSON `null` into
`harvest_task_queue.heartbeat_details`; a `start_to_close`/crash reclaim that
preserves that column (the #151 contract) would then feed `null` back to the
retry. `heartbeat_details::<T>()` now treats a stored JSON `null` as *no
checkpoint* (`Ok(None)`) rather than a hard `Serialization` error — so a
liveness ping never flips the documented `Ok(None)` resume API into a failure
(and potential DLQ) or masks a real checkpoint.

**Design.** The guard spawns a ticker on `self.cancel.child_token()`, so
cancelling the activity's own token auto-stops the ticker while the handler
still observes cancellation through `is_cancelled()` / `check_cancellation()`
(the guard never swallows it). Dropping the guard — typically when the handler
returns — cancels the child token and aborts the ticker. Each tick re-sends the
activity's most recent heartbeat payload from a shared last-payload cell that
`heartbeat()` now writes on every call (last-write-wins); the cell is seeded
from the previous attempt's resume snapshot (`heartbeat_details`) so the first
liveness ping preserves — rather than clobbers — the durable checkpoint used by
`heartbeat_details()` on a later retry (issue #151 contract). With no manual
heartbeat and no resume snapshot the liveness ping is a JSON `null`. A closed
heartbeat channel makes the ticker `break` (never `.unwrap()`/panic); a
zero-period interval is clamped to 1ms (a zero `tokio::time::interval` panics).

**Guardrails.** Both methods reject with `HarvestError::Config` on a local
activity or a no-flusher context (heartbeats are unsupported there), and reject
when no `heartbeat_timeout` is configured — an auto-heartbeat whose liveness
pings are never checked by the scanner would be a silent no-op, so the caller
is told to configure `#[activity(heartbeat_timeout = "..")]`.

**Liveness tradeoff (documented on both methods):** auto-heartbeat keeps a
*progressing-but-not-manually-pinging* activity alive, but necessarily weakens
heartbeat-based wedge detection for a live-but-deadlocked future. The
independent `start_to_close` / `schedule_to_close` timeouts remain the hard
wedge ceiling and are unaffected by heartbeats — pair auto-heartbeat with a
`start_to_close` for a guaranteed runtime upper bound.

**Invariants:** no new `WorkflowEvent` variant, no migration, no management-API
surface, no scanner/flusher/schema change. The timeout scanner (`timeout.rs`)
and the batched heartbeat flusher (`heartbeat.rs`) are untouched — the guard
simply feeds the existing flusher channel.

**Tests.** Pure unit tests in `context.rs` under paused-clock time
(`start_paused = true`): ticking sends payloads, guard-drop stops the ticker,
cancellation stops it while `is_cancelled()` stays true, closed-channel exits
cleanly (`AutoHeartbeatGuard::into_join_handle().await` returns `Ok(())` — a
stronger no-panic proof than `is_finished()`, which is also true for a panicked
task), last-write-wins re-send, liveness-only `null` ping, the
require-`heartbeat_timeout` rejection (message-pinned) and the local/no-flusher
rejection, the AC#2 `heartbeat_timeout / 3` interval derivation (a 9s timeout
pings at exactly 3s — distinguishing `/3` from `/2`, `/1`, `*3`), the AC#4 #151
seed-from-resume-snapshot (the first liveness ping re-sends the resumed
checkpoint, not `null`), and the stored-`null`→`Ok(None)` read-path.

DB integration tests in `tests/integration/auto_heartbeat_tests.rs`:
`auto_heartbeat_prevents_spurious_heartbeat_reclaim` drives a real worker +
background timeout scanner and proves a long-running activity that only
auto-heartbeats is never reclaimed via `TimeoutReason::Heartbeat` and completes
(AC#8, success metric; widened freshness margin — 6s window, 2s interval — for
CI headroom); `fresh_heartbeat_does_not_defeat_start_to_close` is a pure,
deterministic scanner proof that a fresh `last_heartbeat_at` (what
auto-heartbeat produces) shields the heartbeat timeout but NOT the independent
`start_to_close` ceiling — and that a stale heartbeat IS reclaimed as
`Heartbeat`, so the protection is real;
`auto_heartbeat_activity_still_reclaimed_by_start_to_close` is the end-to-end
complement — a real auto-heartbeating activity that wedges past a tight
`start_to_close` is still reclaimed as `StartToClose` (never `Heartbeat`) and
the workflow fails, the airtight full-composition proof. Example:
`autumn-harvest/examples/auto_heartbeat.rs`.
