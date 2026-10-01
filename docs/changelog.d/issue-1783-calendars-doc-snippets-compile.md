## Docs — calendars.md snippets compile under CI (issue #1783)

The "Programmatic Helpers" example in `docs/calendars.md` called `apply_skip_policy` with three arguments. The function takes four. Issue #1772 fixed the call. Nothing guarded it, so the example could drift again.

A `cfg(doctest)` item in `calendar.rs` now compiles every Rust fence in `docs/calendars.md` under `cargo test --doc`. An ungated step in the CI `lint` job runs it, so docs-only PRs also run it.

The example now derives `exclude_weekends` with `calendar_excludes_weekends`. It no longer imports the unused `is_excluded_date`. The unlabeled metric fence is now a `text` fence.

No migration. No `WorkflowEvent` variant. Nothing writes to `harvest_events`.

Tests: the doctest failed on the three-argument call. It passes on the fixed example. A guard test fails if the harness or the CI step is removed.
