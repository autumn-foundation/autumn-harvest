## Phase — A tokenless dev-session record no longer reaps a live pid (issue #1295)

**What was wrong.** `postmaster_is_the_recorded_one` fell back to plain
liveness when a session record carried no postmaster start token: any live
process at the recorded pid counted as a match. `decide_reap` then answered
`StopThenRemove`, and `stop_orphan` ran `pg_ctl stop`. The doc comment
justifying that fallback claimed `pg_ctl` was safe because it derives the
pid from the data directory rather than trusting the record. That claim was
wrong: `pg_ctl stop` reads `postmaster.pid`, which a `SIGKILL` leaves stale,
and its own liveness check passes for a reused pid exactly as readily as
the reaper's did. A tokenless record with a genuinely stale `postmaster.pid`
could make `cargo dev` signal a process that had nothing to do with
Harvest — always on Windows, since `process_start_token` returns `None`
there, and on Unix during the narrow window between `pg_ctl start` and the
record rewrite.

**Fix.** The reap decision is now a three-way `PostmasterIdentity`
(`Confirmed`, `NotRunning`, `Unknown`), not a bool. `decide_reap` reaps
through `StopThenRemove` only on `Confirmed`; `Unknown` now skips, leaving
the session directory in place for a run that can prove identity, the same
conservative choice already made for a cluster that could not be confirmed
stopped. `NotRunning` (a dead pid, or a live pid whose start token does not
match) still removes the directory with no signal, unchanged from before.
`stop_orphan`'s doc comment is corrected in the same change: the identity
proof now happens in `decide_reap`, before this function is ever reached,
not inside `pg_ctl`.

**Cost, accepted.** A tokenless session is now unreclaimable until a run
that can prove identity comes along — most visibly on Windows, which has no
start token at all yet. Closing that gap needs a Windows
`process_start_token` (issue #1287); until then, an orphaned Windows
session leaks rather than risks signalling the wrong process.

No new `WorkflowEvent` variant, no migration, no schema change — this is a
dev-only (`dev-runtime` feature) filesystem and process check.

**Tests, red → green.** `dev_runtime_tests.rs`:
`a_tokenless_but_live_postmaster_is_skipped_not_stopped` pins the exact
regression the issue reports. The existing `decide_reap` table
(`reap_stops_an_orphaned_postmaster_then_removes_the_directory`,
`reap_removes_the_directory_when_the_postmaster_is_already_gone`, the round-4
owner-identity tests) is updated to the new `PostmasterIdentity` parameter and
still passes, so the steady-state and mismatched-token paths do not regress.
`reaper.rs`'s own inline unit tests cover `postmaster_identity` directly: a
tokenless record against a genuinely live pid reads `Unknown`; a dead pid
reads `NotRunning` regardless of its recorded token; a live pid with a
mismatched token reads `NotRunning`, not `Unknown`; a live pid with a
matching token reads `Confirmed`.
