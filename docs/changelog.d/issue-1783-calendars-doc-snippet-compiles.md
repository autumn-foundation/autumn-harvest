## Docs — calendars.md helper snippet compiles (issue #1783)

The "Programmatic Helpers" example in `docs/calendars.md` called `apply_skip_policy` with three arguments. The function takes four. The example now passes `exclude_weekends`, derived with `calendar_excludes_weekends`.

The "Notes" bullet on `weekends-off` now says the flag controls the weekend check. Before, it implied weekends were always excluded.

A `cfg(doctest)` item in `calendar.rs` now compiles every Rust fence in `docs/calendars.md` under `cargo test --doc`. The unlabeled metric fence became a `text` fence.

No migration. No `WorkflowEvent` variant. Nothing writes to `harvest_events`.

Tests: red, the doctest failed on the old snippet. Green, it passes on the fixed one.
