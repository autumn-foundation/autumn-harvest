## Phase — one release-claim-prefix macro for all five task-queue releases (🪞 Echo clone-class merge)

`queue::release_claim_if_over_cap_query`,
`queue_pause::release_claim_query`,
`queue_pause::release_claim_if_queue_paused_query`,
`activity_pause::release_claim_if_activity_paused_query`, and
`execution::release_claim_if_workflow_paused_query` each return a statement
that starts with the identical literal text: `UPDATE harvest_task_queue SET
state = 'PENDING', worker_id = NULL, started_at = NULL, attempt =
GREATEST(attempt - 1, 0) WHERE id = $1 AND state = 'RUNNING' AND worker_id =
$2`. `queue_pause.rs` already named this a shared rule when it introduced a
private `release_claim_prefix!()` macro for its own two releases (issue
#619, round-18 P1): "Both releases must restore `attempt` and be guarded on
`state = 'RUNNING' AND worker_id = $2` identically; keeping one copy makes
that structural rather than a convention two sites must remember." Three
later additions — `activity_pause`'s release (issue #807), `execution`'s
workflow-pause release (issue #1640), and `queue`'s over-cap release (issue
#1971, the most recent) — each hand-rewrote the same text instead of
reaching for that macro, because it was private to `queue_pause.rs`. The
`release_claim_via` doc comment `🪞` Echo wrote for PR #1740 ("Each of the
four callers…") also went stale by one the moment the over-cap release
became its fifth caller, without anyone noticing — a small, concrete
instance of the drift the original macro's own comment predicted.

**What shipped.** `release_claim_prefix!()` moved from `queue_pause.rs` to
`queue.rs` (the module `release_claim_via` already lives in, and that every
one of the five already depends on), as `pub(crate)`. The three
hand-copied `_query()` functions now build their SQL with
`concat!(crate::queue::release_claim_prefix!(), " AND ...")`, same as
`queue_pause.rs`'s own two already did. `release_claim_via`'s doc comment
is corrected to "five callers" and now names the over-cap release too.

**Characterization tests, committed first.** Each of the five `_query()`
functions already had its own shape test pinning its full text
(`queue.rs::the_cap_recheck_counts_what_the_claim_counts`,
`queue_pause.rs::release_claim_query_is_guarded_and_restores_the_attempt`,
`queue_pause.rs::both_claim_releases_share_the_guard_and_restore_the_attempt`,
`activity_pause.rs::release_claim_restores_the_attempt_and_is_guarded_to_this_worker`,
`execution.rs::release_claim_restores_the_attempt_and_is_scoped_to_workflow_rows`)
— these pass unchanged, since the produced strings are byte-identical
before and after. New: `queue.rs::every_release_if_held_statement_shares_the_same_prefix`,
which spells the shared prefix out as a literal (not via the macro) so it
pins real behavior independent of the refactor, and asserts all five
`_query()` outputs start with it. It was committed in a separate commit
before the macro move, and passed against the unrefactored code.

**Evidence.** `jscpd` (rust format, any `--min-tokens` down to 20) never
flagged this class: its Rust tokenizer treats a whole multi-line string
literal as one token, so a shared *prefix* inside literals with different
tails is invisible to token-sequence clone detection. Found instead by
reading `release_claim_via`'s own caller list and grepping the literal SQL
text across `queue.rs`, `queue_pause.rs`, `activity_pause.rs`, and
`execution.rs`. `git log -S` on each `_query()` function's defining commit
shows four independent PRs across two months (#1135, #1187, #1647, #2052)
each adding a caller; only the first (#1135) introduced the macro, and the
other three never adopted it. Named-domain-rule citation: the macro's own
pre-existing doc comment, quoted above. Rule of three clears by a wide
margin (5 instances, not merely 3).

No public API or behavior change: all five functions keep their exact
signatures and return byte-identical strings. `cargo check -p
autumn-harvest --features db --lib`, `cargo clippy -p autumn-harvest
--features db --lib --all-targets -- -D warnings`, and `cargo fmt --check`
are clean. `cargo test -p autumn-harvest --features db --lib
queue::tests:: queue_pause::tests:: activity_pause::tests::
execution::workflow_pause_recheck_tests::` passes, including the five
pre-existing shape tests and the new cross-site one. No live Postgres was
available in this sandbox to re-run the DB integration suites, but this is
a pure mechanical text-sharing change with the same SQL and the same
binds, so no new behavior exists for them to catch.
