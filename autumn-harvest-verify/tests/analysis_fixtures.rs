//! End-to-end analysis expectations over real MIR (D4–D7, D9).
//!
//! Two halves:
//!  * the **false-positive baseline** — a real, well-behaved workspace example
//!    that uses only sanctioned primitives and must not be flagged;
//!  * the **laundering matrix** — `tests/fixtures/format_and_outparams.rs`, one
//!    `#[workflow]`-shaped fn per mechanism, each with the
//!    `__autumn_workflow_info_*` companion `entry::discover` keys on.
//!
//! RED phase: `entry::discover`, `analysis::analyze` and `Program::build` are all
//! `todo!()`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use autumn_harvest_verify::analysis;
use autumn_harvest_verify::entry::{self, Entry};
use autumn_harvest_verify::mir::{self, MirDoc};
use autumn_harvest_verify::model::Model;
use autumn_harvest_verify::resolve::{Program, SourceRoots};
use autumn_harvest_verify::verdict::{
    Boundary, BoundaryKind, Finding, FindingKind, TaintKind, Verdict, WorkflowVerdict,
};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-verify has a parent")
        .to_path_buf()
}

fn parse_fixture(name: &str) -> MirDoc {
    let path = fixtures_dir().join(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    mir::parse("fixture", name, &text)
}

fn model() -> Model {
    Model::builtin().expect("the embedded model must parse")
}

fn run(fixture: &str, roots: &SourceRoots) -> (Vec<Entry>, Vec<WorkflowVerdict>) {
    let docs = vec![parse_fixture(fixture)];
    let entries = entry::discover(&docs);
    let program = Program::build(docs, roots).expect("build");
    let verdicts = analysis::analyze(&program, &model(), &entries);
    (entries, verdicts)
}

fn matrix() -> Vec<WorkflowVerdict> {
    run(
        "format_and_outparams.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1
}

fn pick<'a>(verdicts: &'a [WorkflowVerdict], workflow: &str) -> &'a WorkflowVerdict {
    verdicts
        .iter()
        .find(|v| v.workflow == workflow || v.workflow.ends_with(&format!("::{workflow}")))
        .unwrap_or_else(|| {
            let have: Vec<&str> = verdicts.iter().map(|v| v.workflow.as_str()).collect();
            panic!("no verdict for {workflow:?}; analyzed = {have:#?}")
        })
}

fn findings(v: &WorkflowVerdict) -> &[Finding] {
    match &v.verdict {
        Verdict::NondeterminismFound { findings } => findings,
        _ => &[],
    }
}

/// Boundaries from both places they can appear (D9 keeps them even on a finding).
fn boundaries(v: &WorkflowVerdict) -> Vec<&Boundary> {
    let inner = match &v.verdict {
        Verdict::Unknown { boundaries } => boundaries.as_slice(),
        _ => &[],
    };
    inner.iter().chain(v.boundaries.iter()).collect()
}

fn boundary_kinds(v: &WorkflowVerdict) -> BTreeSet<BoundaryKind> {
    boundaries(v).iter().map(|b| b.kind).collect()
}

fn trace_text(f: &Finding) -> String {
    f.trace
        .iter()
        .map(|h| format!("{} :: {}", h.function, h.step))
        .collect::<Vec<_>>()
        .join(" -> ")
}

fn assert_clean(v: &WorkflowVerdict, why: &str) {
    assert!(
        findings(v).is_empty(),
        "{} ({why}) must not be flagged; findings:\n{}",
        v.workflow,
        findings(v)
            .iter()
            .map(trace_text)
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn assert_found(v: &WorkflowVerdict, kind: FindingKind, taint: TaintKind) -> &Finding {
    let f = findings(v)
        .iter()
        .find(|f| f.kind == kind && f.taint == taint);
    f.unwrap_or_else(|| {
        panic!(
            "{} must report a {kind:?}/{taint:?} finding; verdict = {:?}, boundaries = {:?}",
            v.workflow,
            v.verdict.name(),
            boundary_kinds(v)
        )
    })
}

// ── FP baseline: a real, well-behaved workspace example ────────────────────

#[test]
fn deterministic_primitives_example_is_discovered() {
    let docs = vec![parse_fixture("example_deterministic_primitives.mir")];
    let entries = entry::discover(&docs);
    assert_eq!(
        entries.len(),
        1,
        "one `#[workflow]` fn in this example; got {entries:#?}"
    );
    let e = &entries[0];
    assert!(
        e.workflow.ends_with("notify_decision"),
        "got {}",
        e.workflow
    );
    assert_eq!(e.body, "notify_decision::{closure#0}");
    assert!(
        e.body.ends_with("::{closure#0}"),
        "an async `#[workflow]` fn's analyzable body is its coroutine body"
    );
}

#[test]
fn deterministic_primitives_example_is_not_flagged() {
    // The FP baseline. This example deliberately uses only `system_now`,
    // `new_uuid`, `random_range` and `side_effect` — the sanctioned primitives.
    let (_, verdicts) = run(
        "example_deterministic_primitives.mir",
        &SourceRoots {
            roots: vec![workspace_root()],
        },
    );
    let v = pick(&verdicts, "notify_decision");
    assert_ne!(
        v.verdict.name(),
        "nondeterminism-found",
        "false positive on the sanctioned-primitives example; findings:\n{}",
        findings(v)
            .iter()
            .map(trace_text)
            .collect::<Vec<_>>()
            .join("\n")
    );
    if v.verdict.name() == "unknown" {
        // Not a failure here — this test only forbids a false positive — but the
        // boundaries must be named so the gap is visible.
        let named: Vec<String> = boundaries(v)
            .iter()
            .map(|b| format!("{}: {}", b.kind.name(), b.detail))
            .collect();
        println!("notify_decision is `unknown` under boundaries: {named:#?}");
        assert!(
            !named.is_empty(),
            "an `unknown` verdict must name at least one boundary (D9)"
        );
    }
}

#[test]
fn deterministic_primitives_example_is_proven() {
    // The stronger claim, kept separate so a legitimate boundary shows up as a
    // distinct failure from a false positive.
    let (_, verdicts) = run(
        "example_deterministic_primitives.mir",
        &SourceRoots {
            roots: vec![workspace_root()],
        },
    );
    let v = pick(&verdicts, "notify_decision");
    assert_eq!(
        v.verdict,
        Verdict::ProvenDeterministic,
        "boundaries: {:#?}",
        boundaries(v)
            .iter()
            .map(|b| format!("{}: {}", b.kind.name(), b.detail))
            .collect::<Vec<_>>()
    );
}

// ── the laundering matrix: every workflow-like fn is discovered ────────────

#[test]
fn every_companion_fn_in_the_matrix_is_discovered() {
    let docs = vec![parse_fixture("format_and_outparams.mir")];
    let entries = entry::discover(&docs);
    let names: BTreeSet<&str> = entries.iter().map(|e| e.workflow.as_str()).collect();
    for expected in [
        "wf_format_into_activity_name",
        "wf_out_param_launder",
        "wf_side_effect_is_clean",
        "wf_hashmap_iteration",
        "wf_sorted_keys",
        "wf_branch_on_wallclock",
        "wf_branch_on_version",
        "wf_try_on_clean_result",
        "wf_await_is_clean",
        "wf_lazylock_deref",
        "wf_oncelock_get_or_init",
        "wf_plain_static_is_clean",
        "wf_static_mut_raw_read",
        "wf_ffi_clock",
        "wf_fn_pointer",
        "wf_dyn_two_impls",
        "wf_dyn_single_impl",
        "wf_option_map_closure",
        "wf_sort_by_ambient_comparator",
        "wf_thread_local_counter",
        "wf_observability_is_clean",
    ] {
        assert!(
            names.contains(expected),
            "not discovered: {expected}; got {names:#?}"
        );
    }
    for e in &entries {
        assert!(
            e.body.ends_with("::{closure#0}"),
            "{} -> {}",
            e.workflow,
            e.body
        );
    }
}

// ── Value taint ────────────────────────────────────────────────────────────

#[test]
fn format_of_wallclock_into_an_activity_name_is_found() {
    let v = matrix();
    let v = pick(&v, "wf_format_into_activity_name");
    let f = assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Value);
    let trace = trace_text(f);
    assert!(
        trace.contains("stamped_name"),
        "the trace must name the helper: {trace}"
    );
    assert!(
        trace.contains("wall_clock_secs"),
        "and the fn that reads the clock: {trace}"
    );
    assert!(
        f.sink.what.contains("execute_activity_raw"),
        "sink site: {:?}",
        f.sink
    );
}

#[test]
fn out_param_laundering_is_found() {
    let v = matrix();
    let v = pick(&v, "wf_out_param_launder");
    let f = assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Value);
    assert!(
        trace_text(f).contains("fill_seq"),
        "trace: {}",
        trace_text(f)
    );
}

#[test]
fn side_effect_captured_wallclock_is_clean() {
    let v = matrix();
    let v = pick(&v, "wf_side_effect_is_clean");
    assert_clean(v, "`side_effect` records the value once and replays it");
    assert_eq!(
        v.verdict,
        Verdict::ProvenDeterministic,
        "boundaries: {:?}",
        boundary_kinds(v)
    );
}

#[test]
fn lazylock_deref_is_found() {
    let v = matrix();
    let v = pick(&v, "wf_lazylock_deref");
    assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Value);
}

#[test]
fn oncelock_initializer_is_found() {
    let v = matrix();
    let v = pick(&v, "wf_oncelock_get_or_init");
    assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Value);
}

#[test]
fn thread_local_counter_is_found() {
    let v = matrix();
    let v = pick(&v, "wf_thread_local_counter");
    assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Value);
}

#[test]
fn option_map_closure_carries_taint_out() {
    let v = matrix();
    let v = pick(&v, "wf_option_map_closure");
    assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Value);
}

#[test]
fn plain_immutable_static_read_is_clean() {
    // Textually identical to the `COUNTER` read at the use site (`const {allocN: &T}`);
    // only the static's declared TYPE separates them (A13/M7).
    let v = matrix();
    let v = pick(&v, "wf_plain_static_is_clean");
    assert_clean(v, "an immutable `static PLAIN_LIMIT: u64` is deterministic");
    assert_eq!(
        v.verdict,
        Verdict::ProvenDeterministic,
        "boundaries: {:?}",
        boundary_kinds(v)
    );
}

// ── Order taint ────────────────────────────────────────────────────────────

#[test]
fn hashmap_iteration_driving_commands_is_found_as_order() {
    let v = matrix();
    let v = pick(&v, "wf_hashmap_iteration");
    assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Order);
}

#[test]
fn sorting_the_keys_first_is_clean() {
    let v = matrix();
    let v = pick(&v, "wf_sorted_keys");
    assert_clean(v, "`sort` is a sanitizer for Order taint (D4)");
    assert_eq!(
        v.verdict,
        Verdict::ProvenDeterministic,
        "boundaries: {:?}",
        boundary_kinds(v)
    );
}

#[test]
fn an_ambient_comparator_taints_the_order_not_the_values() {
    let v = matrix();
    let v = pick(&v, "wf_sort_by_ambient_comparator");
    assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Order);
}

// ── Control taint ──────────────────────────────────────────────────────────

#[test]
fn a_command_behind_a_wallclock_branch_is_control_dependent() {
    let v = matrix();
    let v = pick(&v, "wf_branch_on_wallclock");
    let f = assert_found(v, FindingKind::ControlDependentSink, TaintKind::Control);
    assert!(
        f.source.what.contains("wall_clock_secs") || f.source.what.contains("SystemTime"),
        "source site: {:?}",
        f.source
    );
}

#[test]
fn branching_on_history_metadata_is_clean() {
    let v = matrix();
    let v = pick(&v, "wf_branch_on_version");
    assert_clean(
        v,
        "`ctx.version()` is the sanctioned versioning idiom, not a source",
    );
}

#[test]
fn try_on_a_clean_result_is_not_control_taint() {
    // `?` lowers to `Try::branch` + `switchInt(discriminant(..))`; it is the
    // single biggest FP driver (A18/M8).
    let v = matrix();
    let v = pick(&v, "wf_try_on_clean_result");
    assert_clean(v, "`?` on a clean Result");
    assert_eq!(
        v.verdict,
        Verdict::ProvenDeterministic,
        "boundaries: {:?}",
        boundary_kinds(v)
    );
}

#[test]
fn the_coroutine_state_switch_is_not_control_taint() {
    // Every async body opens with `switchInt(discriminant((*_N)))`. If that were
    // control taint, every single workflow would be flagged.
    let v = matrix();
    let v = pick(&v, "wf_await_is_clean");
    assert_clean(v, "the coroutine's own resume-state switch");
    assert_eq!(
        v.verdict,
        Verdict::ProvenDeterministic,
        "boundaries: {:?}",
        boundary_kinds(v)
    );
}

#[test]
fn sanctioned_and_non_sink_ctx_methods_are_clean() {
    let v = matrix();
    let v = pick(&v, "wf_observability_is_clean");
    assert_clean(
        v,
        "`metrics` is a non-sink, `system_now` is sanctioned, `timer`'s arg is a constant",
    );
    assert_eq!(
        v.verdict,
        Verdict::ProvenDeterministic,
        "boundaries: {:?}",
        boundary_kinds(v)
    );
}

// ── boundaries ─────────────────────────────────────────────────────────────

#[test]
fn static_mut_raw_read_is_an_unsafe_raw_pointer_boundary() {
    let v = matrix();
    let v = pick(&v, "wf_static_mut_raw_read");
    assert!(
        boundary_kinds(v).contains(&BoundaryKind::UnsafeRawPointer),
        "got {:?} / {:?}",
        v.verdict.name(),
        boundary_kinds(v)
    );
}

#[test]
fn extern_c_call_is_an_ffi_boundary() {
    let v = matrix();
    let v = pick(&v, "wf_ffi_clock");
    assert!(
        boundary_kinds(v).contains(&BoundaryKind::Ffi),
        "got {:?} / {:?}",
        v.verdict.name(),
        boundary_kinds(v)
    );
}

#[test]
fn fn_pointer_call_is_an_indirect_call_boundary() {
    let v = matrix();
    let v = pick(&v, "wf_fn_pointer");
    assert!(
        boundary_kinds(v).contains(&BoundaryKind::IndirectCall),
        "got {:?} / {:?}",
        v.verdict.name(),
        boundary_kinds(v)
    );
}

#[test]
fn two_impls_behind_a_dyn_trait_is_a_dyn_dispatch_boundary() {
    let v = matrix();
    let v = pick(&v, "wf_dyn_two_impls");
    assert!(
        boundary_kinds(v).contains(&BoundaryKind::DynDispatch),
        "got {:?} / {:?}",
        v.verdict.name(),
        boundary_kinds(v)
    );
}

#[test]
fn a_single_impl_behind_a_dyn_trait_is_devirtualized_and_caught() {
    // AC3's seeded "rand behind a trait object" case only becomes
    // `nondeterminism-found` if RTA-lite devirtualizes it (A10).
    let v = matrix();
    let v = pick(&v, "wf_dyn_single_impl");
    let f = assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Value);
    let trace = trace_text(f);
    assert!(
        trace.contains("now_secs") || trace.contains("SystemClock"),
        "the trace must name the devirtualized impl: {trace}"
    );
    assert!(
        !boundary_kinds(v).contains(&BoundaryKind::DynDispatch),
        "exactly one type is unsized to `dyn Clock` here, so this is not a boundary"
    );
}

// ── The adversarial-review gaps: implicit flow, fn items, closures, drop glue ──
//
// `tests/fixtures/implicit_and_higher_order.{rs,mir}` — one workflow per
// false-negative class the soundness review found, plus the false-positive
// traps that must survive each fix.

fn gaps() -> Vec<WorkflowVerdict> {
    run(
        "implicit_and_higher_order.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1
}

/// Every finding's trace, so a test can assert the laundering source is named.
fn all_trace_text(v: &WorkflowVerdict) -> String {
    findings(v)
        .iter()
        .map(trace_text)
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_flagged(v: &WorkflowVerdict, must_name: &str) {
    assert!(
        !findings(v).is_empty(),
        "{} must be flagged; verdict = {}, boundaries = {:?}",
        v.workflow,
        v.verdict.name(),
        boundary_kinds(v)
    );
    let text = all_trace_text(v);
    assert!(
        text.contains(must_name),
        "{}'s trace must name {must_name}; got:\n{text}",
        v.workflow
    );
}

#[test]
fn implicit_flow_through_a_tainted_branch_is_a_finding() {
    let v = gaps();
    // A value produced *by* a branch on ambient state carries that state.
    assert_flagged(pick(&v, "wf_implicit_flow_inline"), "COUNTER");
    assert_flagged(pick(&v, "wf_implicit_flow_helper"), "SystemTime::now");
    assert_flagged(pick(&v, "wf_implicit_flow_name"), "SystemTime::now");
}

#[test]
fn the_implicit_flow_rule_does_not_flag_clean_branches() {
    let v = gaps();
    assert_clean(
        pick(&v, "wf_fp_version_branch"),
        "`ctx.version()` is history-clean, so a branch on it decides nothing ambient",
    );
    assert_clean(
        pick(&v, "wf_fp_try_chain"),
        "`?` on a clean Result is clean by construction",
    );
    assert_clean(
        pick(&v, "wf_fp_clean_branch"),
        "a branch on clean data produces a clean value",
    );
}

#[test]
fn a_bare_fn_item_passed_to_a_higher_order_fn_is_followed() {
    let v = gaps();
    assert_flagged(pick(&v, "wf_fn_item_map"), "SystemTime::now");
    assert_flagged(pick(&v, "wf_fn_item_or_insert_with"), "SystemTime::now");
}

#[test]
fn a_closures_writes_to_its_captured_environment_reach_the_caller() {
    let v = gaps();
    assert_flagged(pick(&v, "wf_closure_env_direct"), "SystemTime::now");
    assert_flagged(pick(&v, "wf_closure_env_retain"), "retain");
}

#[test]
fn a_user_drop_impl_containing_a_sink_is_followed() {
    let v = gaps();
    let d = pick(&v, "wf_drop_glue_sink");
    assert!(
        !findings(d).is_empty(),
        "a `Drop` impl that emits a command from an ambient read is a finding; \
         verdict = {}, boundaries = {:?}",
        d.verdict.name(),
        boundary_kinds(d)
    );
}

#[test]
fn hashset_set_operations_are_order_sources() {
    let v = gaps();
    assert_found(
        pick(&v, "wf_hashset_difference"),
        FindingKind::TaintedSinkArgument,
        TaintKind::Order,
    );
    assert_found(
        pick(&v, "wf_hashset_union"),
        FindingKind::TaintedSinkArgument,
        TaintKind::Order,
    );
}

#[test]
fn thread_sleep_and_spawn_are_forbidden_even_when_rustc_trims_the_path() {
    let v = gaps();
    assert_found(
        pick(&v, "wf_thread_sleep"),
        FindingKind::ForbiddenEffect,
        TaintKind::Value,
    );
    assert_found(
        pick(&v, "wf_thread_spawn"),
        FindingKind::ForbiddenEffect,
        TaintKind::Value,
    );
}

#[test]
fn a_single_segment_forbidden_row_never_fires_on_a_user_fn_with_a_body() {
    let v = gaps();
    assert_clean(
        pick(&v, "wf_user_named_sleep"),
        "a user `fn sleep` with a body is analyzed, not matched against `std::thread::sleep`",
    );
}

// ── Format drift inside a block must degrade to `unknown`, never to `proven` ──

#[test]
fn a_garbled_terminator_is_a_mir_parse_boundary_not_a_silent_proven() {
    let clean = run(
        "parse_drift.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1;
    assert_eq!(
        pick(&clean, "wf_helper_parse_target").verdict.name(),
        "nondeterminism-found",
        "the control dump reports the real flow"
    );

    let garbled = run(
        "parse_drift_garbled.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1;
    let v = pick(&garbled, "wf_helper_parse_target");
    assert_ne!(
        v.verdict.name(),
        "proven-deterministic",
        "one mutated call terminator must never flip a finding to `proven`"
    );
    assert!(
        boundary_kinds(v).contains(&BoundaryKind::MirParse),
        "the shape the parser did not classify must be named as a `mir-parse` \
         boundary; got {:?}",
        boundary_kinds(v)
    );
}

// ── Synthetic dumps: shapes rustc cannot be asked to produce on demand ───────
//
// A 1 000-deep call chain and a call into a dependency that was never asked for
// MIR are both easier to write as MIR text than to build as a crate, and the
// parser is tolerant by design, so a hand-written dump exercises exactly the
// same code path a real one would.

fn analyze_text(text: &str) -> Vec<WorkflowVerdict> {
    let docs = vec![mir::parse("synthetic", "synthetic.mir", text)];
    let entries = entry::discover(&docs);
    let program = Program::build(docs, &SourceRoots::default()).expect("build");
    analysis::analyze(&program, &model(), &entries)
}

/// `fn PATH(_1: u64) -> u64` that tail-calls `next`, or returns its argument.
fn chain_body(path: &str, next: Option<&str>) -> String {
    next.map_or_else(
        || {
            format!(
                "fn {path}(_1: u64) -> u64 {{\n\
             \x20   let mut _0: u64;\n\
             \n\
             \x20   bb0: {{\n\
             \x20       _0 = copy _1;\n\
             \x20       return;\n\
             \x20   }}\n\
             }}\n"
            )
        },
        |next| {
            format!(
                "fn {path}(_1: u64) -> u64 {{\n\
             \x20   let mut _0: u64;\n\
             \x20   let mut _2: u64;\n\
             \n\
             \x20   bb0: {{\n\
             \x20       _2 = {next}(copy _1) -> [return: bb1, unwind continue];\n\
             \x20   }}\n\
             \n\
             \x20   bb1: {{\n\
             \x20       _0 = move _2;\n\
             \x20       return;\n\
             \x20   }}\n\
             }}\n"
            )
        },
    )
}

#[test]
fn a_thousand_body_call_chain_is_a_recursion_boundary_not_a_stack_overflow() {
    const DEPTH: usize = 1000;
    let mut text = String::new();
    text.push_str(
        "fn __autumn_workflow_info_wf_deep() -> u8 {\n\
         \x20   let mut _0: u8;\n\
         \n\
         \x20   bb0: {\n\
         \x20       _0 = const 1_u8;\n\
         \x20       return;\n\
         \x20   }\n\
         }\n",
    );
    text.push_str(&chain_body("wf_deep", Some("chain0")));
    for level in 0..DEPTH {
        let next = format!("chain{}", level.saturating_add(1));
        let last = level.saturating_add(1) == DEPTH;
        text.push_str(&chain_body(
            &format!("chain{level}"),
            (!last).then_some(next.as_str()),
        ));
    }

    // The point of the test is that this *returns* rather than aborting the
    // process with a stack overflow (exit 134, outside the documented contract).
    let verdicts = analyze_text(&text);
    let v = pick(&verdicts, "wf_deep");
    assert_eq!(
        v.verdict.name(),
        "unknown",
        "a chain too deep to follow is `unknown`, never `proven`"
    );
    assert!(
        boundary_kinds(v).contains(&BoundaryKind::Recursion),
        "the depth cap must be named as a `recursion` boundary; got {:?}",
        boundary_kinds(v)
    );
}

#[test]
fn a_body_less_non_std_callee_is_an_external_crate_body_boundary() {
    let text = "fn __autumn_workflow_info_wf_dep() -> u8 {\n\
                \x20   let mut _0: u8;\n\
                \n\
                \x20   bb0: {\n\
                \x20       _0 = const 1_u8;\n\
                \x20       return;\n\
                \x20   }\n\
                }\n\
                fn wf_dep() -> u64 {\n\
                \x20   let mut _0: u64;\n\
                \x20   let mut _2: u64;\n\
                \n\
                \x20   bb0: {\n\
                \x20       _2 = now_ish() -> [return: bb1, unwind continue];\n\
                \x20   }\n\
                \n\
                \x20   bb1: {\n\
                \x20       _0 = move _2;\n\
                \x20       return;\n\
                \x20   }\n\
                }\n";
    let verdicts = analyze_text(text);
    let v = pick(&verdicts, "wf_dep");
    assert_eq!(
        v.verdict.name(),
        "unknown",
        "a dependency compiled without `--emit=mir` is a body the analysis never \
         saw, not a clean propagator"
    );
    assert!(
        boundary_kinds(v).contains(&BoundaryKind::ExternalCrateBody),
        "got {:?}",
        boundary_kinds(v)
    );
}

#[test]
fn a_body_less_std_callee_is_not_a_boundary() {
    // The same shape with a std-rooted declared type at the call site: rustc
    // trimmed the path, but `std::string::String` says what it is.
    let text = "fn __autumn_workflow_info_wf_std() -> u8 {\n\
                \x20   let mut _0: u8;\n\
                \n\
                \x20   bb0: {\n\
                \x20       _0 = const 1_u8;\n\
                \x20       return;\n\
                \x20   }\n\
                }\n\
                fn wf_std() -> u64 {\n\
                \x20   let mut _0: u64;\n\
                \x20   let mut _2: std::string::String;\n\
                \x20   let mut _3: &str;\n\
                \n\
                \x20   bb0: {\n\
                \x20       _3 = const \"x\";\n\
                \x20       _2 = <str as ToString>::to_string(move _3) -> [return: bb1, unwind continue];\n\
                \x20   }\n\
                \n\
                \x20   bb1: {\n\
                \x20       _0 = const 0_u64;\n\
                \x20       return;\n\
                \x20   }\n\
                }\n";
    let verdicts = analyze_text(text);
    let v = pick(&verdicts, "wf_std");
    assert_eq!(
        v.verdict.name(),
        "proven-deterministic",
        "boundaries = {:?}",
        boundary_kinds(v)
    );
}

// ── Review round 2: three false-`proven` shadowing / trust holes ────────────

/// Finding 1: `a::COUNTER: u64` must not decide the verdict for `b::COUNTER:
/// AtomicU64`. Both print with their FULL path in the `static` header and in
/// the `allocN (static: ..)` footer on rustc 1.98, so nothing is ambiguous
/// here — the index just has to keep them apart.
#[test]
fn a_shadowed_atomic_static_is_still_an_ambient_read() {
    let verdicts = run(
        "shadowed_statics.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1;
    let v = pick(&verdicts, "wf_reads_shadowed_atomic");
    assert_eq!(
        v.verdict.name(),
        "nondeterminism-found",
        "reading `b::COUNTER: AtomicU64` is ambient however `a::COUNTER: u64` is \
         spelled; boundaries = {:?}",
        boundary_kinds(v)
    );
    let finding = assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Value);
    let trace = trace_text(finding);
    assert!(
        trace.contains("b::COUNTER"),
        "the trace must name the static that was actually read; got:\n{trace}"
    );
}

/// The other half of finding 1: the immutable static keeps its clean verdict.
#[test]
fn the_shadowing_immutable_static_stays_clean() {
    let verdicts = run(
        "shadowed_statics.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1;
    let v = pick(&verdicts, "wf_reads_shadowing_plain");
    assert_eq!(
        v.verdict.name(),
        "proven-deterministic",
        "`a::COUNTER: u64` is plain immutable data; boundaries = {:?}",
        boundary_kinds(v)
    );
}

/// Finding 3: `b::Worker::run` reads the wall clock; `a::Worker::run` is a
/// constant. Keying the impl index on the bare `Worker` collapsed them.
#[test]
fn a_shadowed_impl_method_is_resolved_by_its_module() {
    let verdicts = run(
        "shadowed_impls.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1;
    let v = pick(&verdicts, "wf_calls_ambient_worker");
    assert_eq!(
        v.verdict.name(),
        "nondeterminism-found",
        "`b::Worker::run` reads `SystemTime::now`; boundaries = {:?}",
        boundary_kinds(v)
    );
    assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Value);
}

/// The other half of finding 3: disambiguation by module path is precise
/// enough to keep the clean `a::Worker::run` caller proven.
#[test]
fn the_shadowing_clean_impl_method_stays_proven() {
    let verdicts = run(
        "shadowed_impls.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1;
    let v = pick(&verdicts, "wf_calls_clean_worker");
    assert_eq!(
        v.verdict.name(),
        "proven-deterministic",
        "`a::Worker::run` returns a constant; boundaries = {:?}",
        boundary_kinds(v)
    );
}

/// Finding 2: a body-less callee from a dependency compiled without
/// `--emit=mir`. Its `std::string::String` destination is not evidence about
/// the CALLEE, so the call is an honest `external-crate-body` boundary.
#[test]
fn a_body_less_dependency_fn_is_not_trusted_by_its_result_type() {
    let verdicts = run(
        "bodyless_dependency.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1;
    let v = pick(&verdicts, "wf_calls_bodyless_dependency");
    assert_eq!(
        v.verdict.name(),
        "unknown",
        "`now_ish` has no body here; boundaries = {:?}",
        boundary_kinds(v)
    );
    assert!(
        boundary_kinds(v).contains(&BoundaryKind::ExternalCrateBody),
        "got {:?}",
        boundary_kinds(v)
    );
}

/// The other half of finding 2: `format`, `must_use`, `String::len`,
/// `Vec::new`, `Vec::push` and `Vec::len` are all body-less here and must stay
/// trusted, or every workflow in the workspace goes `unknown`.
#[test]
fn body_less_std_receivers_and_free_fns_stay_trusted() {
    let verdicts = run(
        "bodyless_dependency.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1;
    let v = pick(&verdicts, "wf_std_receivers_stay_trusted");
    assert_eq!(
        v.verdict.name(),
        "proven-deterministic",
        "boundaries = {:?}",
        boundaries(v)
            .iter()
            .map(|b| (b.kind, b.detail.as_str()))
            .collect::<Vec<_>>()
    );
}

/// The conservative half of finding 1: when an `allocN` footer names only a
/// bare last segment that two modules both define, and the pointee type does
/// not pick one — both are `u64` here, and only their MUTABILITY differs — the
/// read is ambient because ONE of the candidates is.
#[test]
fn an_ambiguous_static_read_is_ambient_if_any_candidate_is() {
    let text = "static a::COUNTER: u64 = {\n\
                \x20   let mut _0: u64;\n\
                \n\
                \x20   bb0: {\n\
                \x20       _0 = const 7_u64;\n\
                \x20       return;\n\
                \x20   }\n\
                }\n\
                static mut b::COUNTER: u64 = {\n\
                \x20   let mut _0: u64;\n\
                \n\
                \x20   bb0: {\n\
                \x20       _0 = const 0_u64;\n\
                \x20       return;\n\
                \x20   }\n\
                }\n\
                fn __autumn_workflow_info_wf_bare_static() -> u8 {\n\
                \x20   let mut _0: u8;\n\
                \n\
                \x20   bb0: {\n\
                \x20       _0 = const 0_u8;\n\
                \x20       return;\n\
                \x20   }\n\
                }\n\
                fn wf_bare_static(_1: &WorkflowContext) -> u64 {\n\
                \x20   let mut _0: u64;\n\
                \x20   let mut _2: &u64;\n\
                \x20   let mut _3: std::string::String;\n\
                \n\
                \x20   bb0: {\n\
                \x20       _2 = const {alloc9: &u64};\n\
                \x20       _3 = const \"charge\";\n\
                \x20       _0 = autumn_harvest::WorkflowContext::execute_activity_raw(copy _1, move _3, copy _2) -> [return: bb1, unwind continue];\n\
                \x20   }\n\
                \n\
                \x20   bb1: {\n\
                \x20       return;\n\
                \x20   }\n\
                }\n\
                alloc9 (static: COUNTER, size: 8, align: 8) {\n\
                \x20   00 00 00 00 00 00 00 00\n\
                }\n";
    let verdicts = analyze_text(text);
    let v = pick(&verdicts, "wf_bare_static");
    assert_eq!(
        v.verdict.name(),
        "nondeterminism-found",
        "`COUNTER` could be `static mut b::COUNTER`, and the conservative answer \
         over the candidates is the ambient one; boundaries = {:?}",
        boundary_kinds(v)
    );
    let finding = assert_found(v, FindingKind::TaintedSinkArgument, TaintKind::Value);
    let trace = trace_text(finding);
    assert!(
        trace.contains("b::COUNTER"),
        "the trace must name the candidate that made it ambient; got:\n{trace}"
    );
}

// ── one printed `{closure@..}` span, two bodies ─────────────────────────────

fn shadowed_closures() -> Vec<WorkflowVerdict> {
    run(
        "shadowed_closures.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1
}

#[test]
fn a_macro_expanded_closure_is_resolved_per_expansion_not_per_span() {
    // Both expansions of `mk_add!` print the same `{closure@..}` span, and only
    // one of them reads the wall clock. Keeping the first body indexed under
    // that span reports the ambient workflow as `proven-deterministic`.
    let v = shadowed_closures();
    assert_flagged(pick(&v, "wf_macro_closure_ambient"), "SystemTime::now");
}

#[test]
fn the_clean_expansion_of_a_shared_closure_span_stays_proven() {
    let v = shadowed_closures();
    let clean = pick(&v, "wf_macro_closure_clean");
    assert_clean(
        clean,
        "the call site sits inside its own expansion's body, which is what tells \
         the two closures that share a span apart",
    );
    assert_eq!(
        clean.verdict.name(),
        "proven-deterministic",
        "boundaries = {:?}",
        boundary_kinds(clean)
    );
}

// ── one `Drop` glue key, two types ──────────────────────────────────────────

#[test]
fn shadowed_drop_glue_is_followed_into_the_dropped_types_own_impl() {
    // `clean::Guard` and `ambient::Guard` share a last segment. A glue lookup
    // that insists on a single answer finds none and treats the drop as inert,
    // which loses the wall-clock read inside `ambient::Guard::drop` entirely.
    let v = run(
        "shadowed_drops.mir",
        &SourceRoots {
            roots: vec![fixtures_dir()],
        },
    )
    .1;
    assert_flagged(pick(&v, "wf_drop_ambient_guard"), "SystemTime::now");
    assert_clean(
        pick(&v, "wf_drop_clean_guard"),
        "the other module's glue emits a command built from clean data",
    );
}

#[test]
fn drop_glue_with_no_readable_impl_header_is_unknown_never_proven() {
    // How a pre-emitted `.mir` file is analyzed: no source root, so no
    // `<impl at FILE:L:C>` header can be read back and nothing says these
    // `::drop` bodies are `Drop` impls at all.
    let v = run("shadowed_drops.mir", &SourceRoots::default()).1;
    for workflow in ["wf_drop_ambient_guard", "wf_drop_clean_guard"] {
        let picked = pick(&v, workflow);
        assert_ne!(
            picked.verdict.name(),
            "proven-deterministic",
            "{workflow}: unreadable drop glue must never be assumed inert"
        );
        assert!(
            boundary_kinds(picked).contains(&BoundaryKind::DropGlue),
            "{workflow}: the glue the analyzer could not resolve must be named; \
             got {:?}",
            boundary_kinds(picked)
        );
    }
}
