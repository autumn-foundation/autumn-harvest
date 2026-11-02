## Phase X.Y — `*_info()` doc example names the real function

`#[workflow]` and `#[activity]` generate a `*_info()` function with a doc
example. The example showed the literal text `#public_info_name()`, because
`quote!` does not interpolate inside a `///` comment. The macros now build
the doc line with `format!`, so the example names the real function, for
example `send_email_info()`.

The example fence is now `text`, not `rust,ignore`. The fragment uses `ctx`
and `input`, which a doctest does not define. `cargo test -- --ignored` and
`--include-ignored` compile `ignore` fences. They then failed once for each
`*_info()` function, in every crate that uses the macros. A downstream crate
that runs its database tests with `--ignored` hit this (force-sync).

No generated code changes. Only the doc attribute text changes.

Test evidence: `info_doc_tests` in `autumn-harvest-macros/src/activity.rs`
and `src/workflow.rs` (6 tests, red before the fix).
`cargo test -p autumn-harvest-agent --doc -- --include-ignored` failed on
each `*_info()` function before the fix and passes after it.
