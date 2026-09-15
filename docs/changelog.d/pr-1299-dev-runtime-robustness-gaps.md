## Phase — Three narrow dev-runtime robustness gaps closed (issue #1299)

**What was wrong.** Follow-up to #525 / #1281, filed after that PR's review
budget was spent. Three small, independent hardening gaps in the same
module.

1. `escape_conf_string` (`postgres.rs`) doubled a literal single quote but
   left a literal backslash alone. Postgres's config lexer decodes backslash
   escapes inside a single-quoted value, so a session-root path containing a
   backslash — legal on Unix — decoded to a different string than the one
   the runtime created, and the postmaster looked for its socket somewhere
   the runtime never made. Verified empirically against a real `postgres -C`.
2. `read_postmaster_pid_file` (`reaper.rs`) read `NotFound` as confirmed
   absence unconditionally. If the owner was killed after `pg_ctl` launched
   Postgres but before the postmaster wrote `postmaster.pid`, the next run
   saw `NotFound`, concluded no server existed, and deleted the session
   directory — storage under a postmaster that was still starting, along
   with the only record that could later stop it.
3. `abandon_cluster` (`mod.rs`) discarded a teardown failure with `.ok()`,
   unlike the readiness-failure path in the same file, which already logged
   and printed it. A cluster left behind after a failed abandon was silent.

**Fix.**

1. `escape_conf_string` now escapes `\` as `\\`, in addition to doubling `'`.
2. `decide_reap` takes the current time and, when no postmaster pid is known
   at all, treats that absence as uncertain rather than proof within a
   15-second startup grace period measured from the record's `created_at`.
   Past the window, absence is Remove as before. A new `SkipReason::
   PossiblyStillStarting` names the skip. A record whose absence is proven
   another way (a known pid confirmed not running) is unaffected — the grace
   period only ever applies when no postmaster pid was ever recorded or read.
3. `abandon_cluster` and the readiness-failure path in `DevRuntime::start`
   now share one `report_leaked_teardown` helper, so a lost cluster is always
   logged and printed and the two paths cannot drift apart again. The start
   error stays the one returned to the caller.

No new `WorkflowEvent` variant, no migration, no schema change — this is a
dev-only (`dev-runtime` feature) filesystem and process check.

**Tests, red → green.** `dev_runtime_tests.rs`:
`a_backslash_in_the_session_path_cannot_break_the_generated_config` pins the
backslash-doubling fix while the existing quote-doubling test stays green.
`reap_skips_a_freshly_created_session_with_no_pid_file_yet` and
`a_freshly_created_session_with_no_pid_file_survives_the_full_reap_pipeline`
pin the startup grace period at the `decide_reap` and end-to-end
`reap_stale_sessions` levels. Two existing tests —
`reap_removes_a_session_that_died_before_recording_a_postmaster` and
`an_unreadable_postmaster_pid_file_leaves_the_session_alone` — are updated
to age their records past the grace window, so they keep testing confirmed
absence rather than the new skip. The lib-side
`leaked_teardown_tests::the_message_names_the_teardown_failure` pins the
shared teardown-reporting helper's wording.
