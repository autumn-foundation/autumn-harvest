## Phase — shared release-claim-via helper for queue/activity/workflow pause (🪞 Echo clone-class merge)

`queue_pause::release_claim_if_queue_paused`, `queue_pause::release_claim`,
`activity_pause::release_claim_if_activity_paused`, and
`execution::release_claim_if_workflow_paused` each bound
`task_id`/`worker_id` into their own pre-built statement, executed it, and
mapped the row count to a bool, in a byte-identical body. `queue.rs`'s
`apply_post_claim_rechecks` already calls the three `*_if_*_paused` variants
back to back as siblings (queue-pause issue #619, activity-pause issue #807,
workflow-pause issue #1640), and each function's own doc comment
cross-references the others by name. Issue #1640's own changelog fragment
states the third copy was made by deliberately mirroring the second: "This
fix mirrors `activity_pause::release_claim_if_activity_paused` instead."

**What shipped.** Added `queue::release_claim_via(conn, sql: &'static str,
task_id, worker_id) -> HarvestResult<bool>`. All four call sites now
delegate to it, passing their own `_query()` accessor. `sql` is data, not a
mode flag — the function never branches on its content, so there is no
caller-identity or `is_`/`mode`/`kind` encoding. Each caller keeps its own
statement text and its own barrier strategy (queue-pause's advisory lock vs.
activity/workflow-pause's fresh-statement recheck); only the mechanical
bind-execute-map_err-return step is shared.

**Left alone, checked not missed:** the four `*_query()` const fns
(genuinely different SQL per pause domain), and `queue.rs`'s own
`reset_capability_misses_after_inline_progress`, which has the identical
bind-execute-map_err shape by coincidence (same diesel idiom, unrelated
decision) rather than by copying — confirmed by `jscpd`, which pairs it with
the new `release_claim_via` only after this change.

**Evidence.** `jscpd` (`--min-tokens 40 --min-lines 5 --format rust`, scoped
to `queue.rs`/`queue_pause.rs`/`activity_pause.rs`/`execution.rs`): 9
pairwise fragment matches / 465 duplicated tokens across the four wrapper
bodies before this change, 0 after. Whole-scope numbers for the same 4
files: 196 clones / 13,989 duplicated tokens → 190 clones / 13,680
duplicated tokens. Rule of three clears on instance count alone (4, not 2),
so no missed-fix defect is required as evidence.

No public API or behavior change: all four functions keep their exact
signatures, and each still runs the exact same SQL with the exact same
binds. `cargo check -p autumn-harvest --features db --lib`, `cargo clippy -p
autumn-harvest --features db --lib --all-targets -- -D warnings`, and
`cargo fmt --check` are clean. The existing unit tests that already pin this
behavior — `execution::workflow_pause_recheck_tests::release_claim_restores_the_attempt_and_is_scoped_to_workflow_rows`,
`activity_pause::tests::release_claim_restores_the_attempt_and_is_guarded_to_this_worker`,
and `queue_pause::tests::both_claim_releases_share_the_guard_and_restore_the_attempt`
— pass unchanged. No live Postgres was available in this sandbox to re-run
the full integration suites, but this is a pure mechanical delegation with
the same SQL text and same binds, so no new behavior exists for them to
catch.
