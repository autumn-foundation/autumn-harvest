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
    t.compile_fail("tests/compile_fail/hvg002_side_effect_escape.rs");
    t.compile_fail("tests/compile_fail/hvg003_process_env.rs");
    t.compile_fail("tests/compile_fail/hvg004_sleep_timer.rs");
    t.compile_fail("tests/compile_fail/hvg005_background_task.rs");
    t.compile_fail("tests/compile_fail/hvg006_direct_io.rs");
    t.compile_fail("tests/compile_fail/hvg007_process_global.rs");
    t.compile_fail("tests/compile_fail/hvg008_nondeterministic_predicate.rs");
    t.compile_fail("tests/compile_fail/hvg010_select_macro.rs");
    t.compile_fail("tests/compile_fail/hvg010_select_combinator.rs");
    t.compile_fail("tests/compile_fail/hvg011_hashmap_iteration.rs");
    t.compile_fail("tests/compile_fail/webhook_missing_path.rs");
    t.compile_fail("tests/compile_fail/webhook_neither_target.rs");
    t.compile_fail("tests/compile_fail/webhook_both_targets.rs");
    t.compile_fail("tests/compile_fail/webhook_signals_missing_signal_name.rs");
    t.compile_fail("tests/compile_fail/webhook_verifier_attr_unsupported.rs");
    t.compile_fail("tests/compile_fail/webhook_async_fn.rs");
    t.compile_fail("tests/compile_fail/throttle_subunit_rate_no_burst.rs");
    t.compile_fail("tests/compile_fail/throttle_infinite_burst.rs");
    t.compile_fail("tests/compile_fail/rate_limit_local.rs");
    t.compile_fail("tests/compile_fail/rate_limit_missing_rps.rs");
    t.compile_fail("tests/compile_fail/rate_limit_flat_and_nested.rs");
    t.compile_fail("tests/compile_fail/rate_limit_key_reserved_prefix.rs");
    t.compile_fail("tests/compile_fail/dag_invalid_execution_timeout.rs");
    t.compile_fail("tests/compile_fail/dag_invalid_sla.rs");
    t.compile_fail("tests/compile_fail/dag_unsupported_attribute.rs");
    t.compile_fail("tests/compile_fail/workflow_blank_dependency_name.rs");
    t.compile_fail("tests/compile_fail/workflow_invalid_dependency_entry.rs");
    t.compile_fail("tests/compile_fail/workflow_dependency_string_container.rs");
    t.compile_fail("tests/compile_fail/workflow_dependency_paren_container.rs");
    t.compile_fail("tests/compile_fail/workflow_duplicate_dependency_attr.rs");
    t.compile_fail("tests/compile_fail/workflow_unsupported_attribute.rs");
    t.pass("tests/compile_fail/suppressed_guardrails.rs");
}
