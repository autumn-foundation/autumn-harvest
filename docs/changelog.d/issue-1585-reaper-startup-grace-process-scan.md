## Fix — reaper checks the process table before its startup grace period (issue #1585)

The dev reaper skipped a session with no known postmaster pid for 15 seconds
after `created_at`. That deadline is wall-clock time. A system suspend freezes
a starting postmaster but not the clock. After resume, the reaper could delete
the data directory of a postmaster that was about to write `postmaster.pid`.

Now the reaper looks for a live process that names the session data directory,
before it trusts the deadline. It matches `-D <dir>` (the postmaster) and
`--pgdata <dir>` or `--pgdata=<dir>` (`pg_ctl`). A match skips the session at
any age, with `SkipReason::PostmasterProcessFound`. No match keeps the 15-second
grace period.

Linux reads `/proc/<pid>/cmdline`. Other Unix systems use `ps`. Windows has no
scan, so it keeps the grace period only. A path with spaces can match a longer
command line. That only causes a skip, never a deletion.

No migration, no `harvest_events` change. Dev-only (`dev-runtime` feature).

Tests: `command_names_data_dir` unit tests in `reaper.rs`, a `decide_reap` test,
and an end-to-end reap test in `dev_runtime_tests.rs`.
