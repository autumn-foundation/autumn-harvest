//! Compile-fail tests for `#[query]` and `#[update]` error cases (issue #346).
//!
//! Run with:
//!   `cargo test -p autumn-harvest --test macros_compile_fail --features testing`

#[test]
fn compile_fail_cases() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/query_missing_ctx.rs");
    t.compile_fail("tests/compile_fail/query_async.rs");
    t.compile_fail("tests/compile_fail/query_wrong_return.rs");
    t.compile_fail("tests/compile_fail/update_missing_ctx.rs");
    t.compile_fail("tests/compile_fail/update_sync.rs");
    t.compile_fail("tests/compile_fail/hvg001_wallclock.rs");
    t.compile_fail("tests/compile_fail/hvg002_randomness.rs");
    t.compile_fail("tests/compile_fail/hvg003_process_env.rs");
    t.compile_fail("tests/compile_fail/hvg004_sleep_timer.rs");
    t.compile_fail("tests/compile_fail/hvg005_background_task.rs");
    t.compile_fail("tests/compile_fail/hvg006_direct_io.rs");
    t.compile_fail("tests/compile_fail/hvg007_process_global.rs");
    t.compile_fail("tests/compile_fail/hvg008_nondeterministic_predicate.rs");
    t.pass("tests/compile_fail/suppressed_guardrails.rs");
}
