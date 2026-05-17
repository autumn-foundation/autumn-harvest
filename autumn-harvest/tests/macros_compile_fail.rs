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
}
