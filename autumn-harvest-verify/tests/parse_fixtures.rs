//! Golden parser tests against checked-in, **real** `rustc --emit=mir` dumps.
//!
//! Every expectation in this file was read off the fixture by hand; see
//! `tests/fixtures/RUSTC_VERSION.txt` for the exact toolchain and the commands
//! that produced each `.mir`. RED phase: `mir::parse` is `todo!()`.

use std::path::{Path, PathBuf};

use autumn_harvest_verify::mir::{
    self, Body, MirDoc, Operand, Place, Projection, Rvalue, Statement, Terminator,
};

// ── helpers ─────────────────────────────────────────────────────────────────

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn read_fixture(name: &str) -> String {
    let path = fixtures_dir().join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

fn parse_fixture(name: &str) -> MirDoc {
    let text = read_fixture(name);
    mir::parse("fixture", name, &text)
}

fn body<'a>(doc: &'a MirDoc, path: &str) -> &'a Body {
    doc.bodies
        .iter()
        .find(|b| b.path == path)
        .unwrap_or_else(|| {
            let have: Vec<&str> = doc.bodies.iter().map(|b| b.path.as_str()).collect();
            panic!("no body {path:?} in {}; bodies = {have:#?}", doc.path)
        })
}

fn block<'a>(b: &'a Body, label: &str) -> &'a mir::BasicBlock {
    b.blocks
        .iter()
        .find(|bb| bb.label == label)
        .unwrap_or_else(|| panic!("no block {label:?} in {}", b.path))
}

/// Every callee path a body calls, in block order.
fn callees(b: &Body) -> Vec<String> {
    b.blocks
        .iter()
        .filter_map(|bb| match &bb.terminator {
            Terminator::Call { callee, .. } => callee.clone(),
            _ => None,
        })
        .collect()
}

const fn local(n: u32) -> Place {
    Place {
        local: mir::Local(n),
        projections: Vec::new(),
    }
}

fn assign_rvalues(b: &Body, label: &str) -> Vec<(Place, Rvalue)> {
    block(b, label)
        .statements
        .iter()
        .filter_map(|s| match s {
            Statement::Assign { dest, rvalue } => Some((dest.clone(), rvalue.clone())),
            Statement::Other(_) => None,
        })
        .collect()
}

// ── spike.mir: the golden fixture ───────────────────────────────────────────

/// Item order is file order. 13 `fn` items + 3 constant bodies.
///
/// `TL::{constant#0}` is the anonymous-constant form (`PATH: TY = { .. }`, with
/// no leading keyword); the parser must treat it as a constant body, not skip it.
const SPIKE_BODIES: &[&str] = &[
    "<impl at spike.rs:5:1: 5:15>::next",
    "TL",
    "__rust_std_internal_init_fn",
    "TL::{constant#0}",
    "TL::{constant#0}::{closure#0}",
    "TL::{constant#0}::{closure#1}",
    "<impl at spike.rs:9:1: 9:9>::emit",
    "helper",
    "deep",
    "sub",
    "sub::{closure#0}",
    "wf",
    "wf::{closure#0}",
    "wf::{closure#0}::promoted[0]",
    "wf::{closure#0}::{closure#0}",
    "wf::{closure#0}::{closure#1}",
];

#[test]
fn spike_body_count_and_paths_are_exact() {
    let doc = parse_fixture("spike.mir");
    assert!(
        doc.parse_failures.is_empty(),
        "unexpected parse failures: {:#?}",
        doc.parse_failures
    );
    let got: Vec<&str> = doc.bodies.iter().map(|b| b.path.as_str()).collect();
    assert_eq!(got, SPIKE_BODIES);
    assert_eq!(doc.bodies.len(), 16);
    assert_eq!(doc.crate_name, "fixture");
    assert_eq!(doc.path, "spike.mir");
}

#[test]
fn spike_constant_bodies_are_flagged_is_const() {
    let doc = parse_fixture("spike.mir");
    for path in ["TL", "TL::{constant#0}", "wf::{closure#0}::promoted[0]"] {
        assert!(body(&doc, path).is_const, "{path} should be is_const");
    }
    for path in ["helper", "deep", "wf::{closure#0}"] {
        assert!(!body(&doc, path).is_const, "{path} should not be is_const");
    }
}

#[test]
fn spike_helper_params_return_and_callees_are_exact() {
    let doc = parse_fixture("spike.mir");
    let helper = body(&doc, "helper");
    assert_eq!(helper.params, vec![(mir::Local(1), "T".to_string())]);
    assert_eq!(helper.return_ty, "Vec<(String, u32)>");
    assert_eq!(
        callees(helper),
        vec![
            "<T as IntoIterator>::into_iter".to_string(),
            "<<T as IntoIterator>::IntoIter as Iterator>::collect::<Vec<(String, u32)>>"
                .to_string(),
        ]
    );
    // `let mut _2: <T as std::iter::IntoIterator>::IntoIter;` — decl types are
    // fully qualified even though the callee path is trimmed (§0.1).
    assert_eq!(
        helper.locals.get(&mir::Local(2)).map(String::as_str),
        Some("<T as std::iter::IntoIterator>::IntoIter")
    );
    assert_eq!(helper.debug_names, vec![("t".to_string(), local(1))]);
}

#[test]
fn spike_wf_closure_locals_and_block_count() {
    let doc = parse_fixture("spike.mir");
    let wf = body(&doc, "wf::{closure#0}");
    assert_eq!(
        wf.locals.get(&mir::Local(15)).map(String::as_str),
        Some("std::cell::Ref<'_, u32>")
    );
    assert_eq!(
        wf.locals.get(&mir::Local(11)).map(String::as_str),
        Some("{closure@spike.rs:16:21: 16:24}")
    );
    assert_eq!(wf.blocks.len(), 49, "bb0..=bb48");
    assert_eq!(
        wf.blocks.iter().filter(|b| b.cleanup).count(),
        8,
        "cleanup blocks must be parsed, not skipped (a sink on an unwind path is still a sink)"
    );
    // Scoped `let`s inside `scope N { .. }` are locals too.
    assert!(
        wf.locals.contains_key(&mir::Local(4)),
        "scope-nested `let _4` must be a local"
    );
}

#[test]
fn spike_async_body_header_first_param_is_pinned_coroutine() {
    let doc = parse_fixture("spike.mir");
    let wf = body(&doc, "wf::{closure#0}");
    assert_eq!(
        wf.params.first().map(|(l, t)| (*l, t.as_str())),
        Some((mir::Local(1), "Pin<&mut {async fn body of wf()}>"))
    );
    assert_eq!(
        wf.params.get(1).map(|(_, t)| t.as_str()),
        Some("&mut Context<'_>")
    );
    assert_eq!(wf.return_ty, "Poll<u32>");
}

#[test]
fn spike_statics_and_alloc_footer() {
    let doc = parse_fixture("spike.mir");
    assert_eq!(
        doc.alloc_statics.get("alloc1").map(String::as_str),
        Some("COUNTER")
    );
    let counter = doc
        .statics
        .iter()
        .find(|s| s.path == "COUNTER")
        .expect("COUNTER static item");
    assert_eq!(counter.ty, "AtomicU64");
    assert!(!counter.is_mut);
    assert_eq!(
        doc.statics.len(),
        3,
        "COUNTER + the two thread-local LazyStorage statics"
    );
}

#[test]
fn spike_call_terminator_shape_is_exact() {
    let doc = parse_fixture("spike.mir");
    let wf = body(&doc, "wf::{closure#0}");
    // bb1: `_5 = helper::<HashMap<String, u32>>(copy _4) -> [return: bb2, unwind: bb43];`
    let Terminator::Call {
        dest,
        callee,
        indirect,
        args,
        target,
        unwind,
    } = &block(wf, "bb1").terminator
    else {
        panic!(
            "bb1 must end in a Call, got {:?}",
            block(wf, "bb1").terminator
        );
    };
    assert_eq!(*dest, local(5));
    assert_eq!(callee.as_deref(), Some("helper::<HashMap<String, u32>>"));
    assert_eq!(*indirect, None);
    assert_eq!(*args, vec![Operand::Copy(local(4))]);
    assert_eq!(target.as_deref(), Some("bb2"));
    assert_eq!(unwind.as_deref(), Some("bb43"));
}

#[test]
fn spike_switchint_targets_are_all_recorded() {
    let doc = parse_fixture("spike.mir");
    let wf = body(&doc, "wf::{closure#0}");
    // bb0: `switchInt(move _60) -> [0: bb1, 1: bb48, 2: bb47, 3: bb46, otherwise: bb11];`
    let Terminator::SwitchInt { operand, targets } = &block(wf, "bb0").terminator else {
        panic!("bb0 must end in a SwitchInt");
    };
    assert_eq!(*operand, Operand::Move(local(60)));
    assert_eq!(targets, &["bb1", "bb48", "bb47", "bb46", "bb11"]);
    assert_eq!(
        block(wf, "bb0").terminator.successors(),
        vec!["bb1", "bb48", "bb47", "bb46", "bb11"]
    );
}

#[test]
fn spike_static_read_rvalue_carries_the_alloc_name() {
    let doc = parse_fixture("spike.mir");
    let wf = body(&doc, "wf::{closure#0}");
    // bb2: `_7 = const {alloc1: &AtomicU64};`
    let (dest, rvalue) = assign_rvalues(wf, "bb2")
        .into_iter()
        .find(|(d, _)| *d == local(7))
        .expect("_7 assignment in bb2");
    assert_eq!(dest, local(7));
    assert_eq!(rvalue.static_alloc.as_deref(), Some("alloc1"));
    assert!(
        matches!(rvalue.reads.first(), Some(Operand::Const { alloc: Some(a), .. }) if a == "alloc1"),
        "the const operand must carry its alloc id too: {:?}",
        rvalue.reads
    );
}

#[test]
fn spike_discriminant_and_ref_rvalues() {
    let doc = parse_fixture("spike.mir");
    let wf = body(&doc, "wf::{closure#0}");

    // bb10: `_23 = discriminant(_21);` — reads `_21` exactly, never its extensions.
    let (_, disc) = assign_rvalues(wf, "bb10")
        .into_iter()
        .find(|(d, _)| *d == local(23))
        .expect("_23 = discriminant(_21)");
    assert_eq!(disc.discriminant_of, Some(local(21)));

    // bb5: `_14 = &_15;`
    let (_, shared) = assign_rvalues(wf, "bb5")
        .into_iter()
        .find(|(d, _)| *d == local(14))
        .expect("_14 = &_15");
    assert_eq!(shared.ref_of, Some((local(15), false)));

    // bb9: `_22 = &mut _20;`
    let (_, exclusive) = assign_rvalues(wf, "bb9")
        .into_iter()
        .find(|(d, _)| *d == local(22))
        .expect("_22 = &mut _20");
    assert_eq!(exclusive.ref_of, Some((local(20), true)));
}

#[test]
fn spike_debug_names_include_source_level_bindings() {
    let doc = parse_fixture("spike.mir");
    // `fn wf` shim: `debug m => _2;`
    let wf_shim = body(&doc, "wf");
    assert!(
        wf_shim.debug_names.contains(&("m".to_string(), local(2))),
        "expected (\"m\", _2), got {:?}",
        wf_shim.debug_names
    );
    assert!(wf_shim.debug_names.contains(&("ctx".to_string(), local(1))));
    assert!(wf_shim.debug_names.contains(&("s".to_string(), local(3))));

    // The coroutine body names them through the self place instead.
    let wf = body(&doc, "wf::{closure#0}");
    let m = wf
        .debug_names
        .iter()
        .find(|(n, _)| n == "m")
        .map(|(_, p)| p.clone())
        .expect("debug m in wf::{closure#0}");
    assert_eq!(m.local, mir::Local(61));
    assert_eq!(m.projections, vec![Projection::Deref, Projection::Field(1)]);
}

#[test]
fn spike_coroutine_variant_projection_is_kept_in_the_place_key() {
    let doc = parse_fixture("spike.mir");
    let wf = body(&doc, "wf::{closure#0}");
    // bb27: `(((*_61) as variant#3).2: u32) = move (_47.0: u32);`
    let (dest, _) = assign_rvalues(wf, "bb27")
        .into_iter()
        .next()
        .expect("an assignment in bb27");
    assert_eq!(dest.local, mir::Local(61));
    assert_eq!(
        dest.projections,
        vec![
            Projection::Deref,
            Projection::Downcast("variant#3".to_string()),
            Projection::Field(2)
        ],
        "`as variant#K` must survive as a Downcast projection — different variants are different slots"
    );
}

#[test]
fn spike_drop_and_assert_terminators() {
    let doc = parse_fixture("spike.mir");
    let wf = body(&doc, "wf::{closure#0}");
    // bb6: `drop(_15) -> [return: bb7, unwind: bb42];`
    match &block(wf, "bb6").terminator {
        Terminator::Drop {
            place,
            target,
            unwind,
        } => {
            assert_eq!(*place, local(15));
            assert_eq!(target, "bb7");
            assert_eq!(unwind.as_deref(), Some("bb42"));
        }
        other => panic!("bb6 must be a Drop, got {other:?}"),
    }
    // bb14 ends in an overflow assert.
    assert!(matches!(
        block(wf, "bb14").terminator,
        Terminator::Assert { .. }
    ));
    // bb11: `unreachable;`
    assert_eq!(block(wf, "bb11").terminator, Terminator::Unreachable);
}

// ── format_and_outparams.mir ────────────────────────────────────────────────

#[test]
fn format_launders_taint_through_tuple_and_array_aggregates() {
    let doc = parse_fixture("format_and_outparams.mir");
    assert!(doc.parse_failures.is_empty(), "{:#?}", doc.parse_failures);
    let stamped = body(&doc, "stamped_name");

    // bb1: `_5 = (move _6, move _7);` — a tuple aggregate reading two places.
    let (_, tuple) = assign_rvalues(stamped, "bb1")
        .into_iter()
        .find(|(d, _)| *d == local(5))
        .expect("_5 = (move _6, move _7)");
    assert_eq!(
        tuple.reads,
        vec![Operand::Move(local(6)), Operand::Move(local(7))]
    );

    // bb3: `_8 = [move _9, move _10];` — an array aggregate.
    let (_, array) = assign_rvalues(stamped, "bb3")
        .into_iter()
        .find(|(d, _)| *d == local(8))
        .expect("_8 = [move _9, move _10]");
    assert_eq!(
        array.reads,
        vec![Operand::Move(local(9)), Operand::Move(local(10))]
    );

    // The whole laundering chain is visible as plain callees.
    let names = callees(stamped);
    for expected in [
        "wall_clock_secs",
        "core::fmt::rt::Argument::<'_>::new_display::<u64>",
        "Arguments::<'_>::new::<5, 2>",
        "format",
        "must_use::<String>",
    ] {
        assert!(
            names.iter().any(|c| c == expected),
            "missing callee {expected}; got {names:#?}"
        );
    }
}

#[test]
fn out_param_write_is_an_assignment_through_a_deref() {
    let doc = parse_fixture("format_and_outparams.mir");
    let fill = body(&doc, "fill_seq");
    assert_eq!(fill.return_ty, "()");
    // bb1: `(*_1) = move _2;`
    let (dest, _) = assign_rvalues(fill, "bb1")
        .into_iter()
        .next()
        .expect("(*_1) = move _2");
    assert_eq!(dest.local, mir::Local(1));
    assert_eq!(dest.projections, vec![Projection::Deref]);
}

#[test]
fn zero_sized_closure_operand_carries_its_span() {
    let doc = parse_fixture("format_and_outparams.mir");
    let parse_amount = body(&doc, "parse_amount");
    let call = parse_amount
        .blocks
        .iter()
        .find_map(|bb| match &bb.terminator {
            Terminator::Call {
                callee: Some(c),
                args,
                ..
            } if c.contains("map_err") => Some(args.clone()),
            _ => None,
        })
        .expect("Result::map_err call");
    let zero_sized = call
        .iter()
        .find_map(|a| match a {
            Operand::Const {
                closure: Some(span),
                ..
            } => Some(span.clone()),
            _ => None,
        })
        .expect("`const ZeroSized: {closure@...}` operand must set `closure`");
    assert_eq!(zero_sized, "format_and_outparams.rs:104:32: 104:35");
}

#[test]
fn indirect_call_has_no_callee_path() {
    let doc = parse_fixture("format_and_outparams.mir");
    let wf = body(&doc, "wf_fn_pointer::{closure#0}");
    let (callee, indirect) = wf
        .blocks
        .iter()
        .find_map(|bb| match &bb.terminator {
            Terminator::Call {
                callee,
                indirect: Some(op),
                ..
            } => Some((callee.clone(), op.clone())),
            _ => None,
        })
        .expect("`_8 = copy _5()` must parse as an indirect Call");
    assert_eq!(callee, None, "an indirect call has no callee path at all");
    assert!(matches!(indirect, Operand::Copy(_)), "got {indirect:?}");
}

#[test]
fn unsize_coercion_records_its_target_type() {
    let doc = parse_fixture("format_and_outparams.mir");
    let wf = body(&doc, "wf_dyn_single_impl::{closure#0}");
    let unsized_to: Vec<String> = wf
        .blocks
        .iter()
        .flat_map(|bb| bb.statements.iter())
        .filter_map(|s| match s {
            Statement::Assign { rvalue, .. } => rvalue.unsize_to.clone(),
            Statement::Other(_) => None,
        })
        .collect();
    assert_eq!(
        unsized_to,
        vec!["std::boxed::Box<dyn Clock>".to_string()],
        "RTA-lite needs every `... as T (PointerCoercion(Unsize, ..))` target"
    );
}

#[test]
fn static_mut_item_is_flagged_mutable() {
    let doc = parse_fixture("format_and_outparams.mir");
    let raw = doc
        .statics
        .iter()
        .find(|s| s.path == "RAW_COUNTER")
        .expect("RAW_COUNTER");
    assert!(raw.is_mut, "`static mut RAW_COUNTER: u64` must set is_mut");
    assert_eq!(raw.ty, "u64");
    let plain = doc
        .statics
        .iter()
        .find(|s| s.path == "PLAIN_LIMIT")
        .expect("PLAIN_LIMIT");
    assert!(!plain.is_mut);
    assert_eq!(
        plain.ty, "u64",
        "textually identical to COUNTER at the use site — only the TYPE differs"
    );
    let counter = doc
        .statics
        .iter()
        .find(|s| s.path == "COUNTER")
        .expect("COUNTER");
    assert_eq!(counter.ty, "AtomicU64");
}

#[test]
fn duplicate_alloc_footers_do_not_conflict() {
    // `alloc9 (static: COUNTER, ...)` is printed four times in this dump.
    let doc = parse_fixture("format_and_outparams.mir");
    assert_eq!(
        doc.alloc_statics.get("alloc9").map(String::as_str),
        Some("COUNTER")
    );
    assert_eq!(
        doc.alloc_statics.get("alloc7").map(String::as_str),
        Some("PLAIN_LIMIT")
    );
    assert_eq!(
        doc.alloc_statics.get("alloc5").map(String::as_str),
        Some("LAZY_START")
    );
    assert_eq!(
        doc.alloc_statics.get("alloc8").map(String::as_str),
        Some("RAW_COUNTER")
    );
}

#[test]
fn duplicate_identical_fn_headers_are_both_kept() {
    // rustc prints the tuple-struct constructor shims twice, byte-identically.
    let doc = parse_fixture("generic_layers.mir");
    assert!(doc.parse_failures.is_empty(), "{:#?}", doc.parse_failures);
    assert_eq!(doc.bodies.iter().filter(|b| b.path == "Leaf").count(), 2);
    assert_eq!(doc.bodies.iter().filter(|b| b.path == "Wrapper").count(), 2);
}

#[test]
fn generic_layers_records_turbofish_substitutions() {
    let doc = parse_fixture("generic_layers.mir");
    assert_eq!(
        callees(body(&doc, "entry")),
        vec!["outer::<Wrapper<Leaf>>".to_string()]
    );
    assert_eq!(
        callees(body(&doc, "outer")),
        vec!["inner::<T>".to_string(), "inner::<T>".to_string()]
    );
    assert_eq!(
        callees(body(&doc, "inner")),
        vec!["<T as Score>::score".to_string()]
    );
    assert_eq!(
        callees(body(&doc, "<impl at generic_layers.rs:21:1: 21:36>::score")),
        vec!["<T as Score>::score".to_string()]
    );
}

// ── async_multi_await.mir ───────────────────────────────────────────────────

#[test]
fn multi_await_body_uses_several_coroutine_variants() {
    let doc = parse_fixture("async_multi_await.mir");
    assert!(doc.parse_failures.is_empty(), "{:#?}", doc.parse_failures);
    let pipeline = body(&doc, "pipeline::{closure#0}");

    let downcasts: std::collections::BTreeSet<String> = pipeline
        .blocks
        .iter()
        .flat_map(|bb| bb.statements.iter())
        .filter_map(|s| match s {
            Statement::Assign { dest, .. } => Some(dest.projections.clone()),
            Statement::Other(_) => None,
        })
        .flatten()
        .filter_map(|p| match p {
            Projection::Downcast(v) => Some(v),
            _ => None,
        })
        .collect();
    for variant in ["variant#3", "variant#4", "variant#5"] {
        assert!(
            downcasts.contains(variant),
            "expected a Downcast({variant}) place; got {downcasts:?}"
        );
    }
    // The self place is *not* `_1`; it is read out of `(_1.0: &mut {async fn body of ...})`.
    assert!(
        pipeline
            .debug_names
            .iter()
            .any(|(n, p)| n == "order" && p.local != mir::Local(1)),
        "the coroutine self place must be derived, not assumed to be _1: {:?}",
        pipeline.debug_names
    );
}

// ── the large, real example dump ────────────────────────────────────────────

#[test]
fn real_example_dump_parses_without_failures() {
    let doc = parse_fixture("example_deterministic_primitives.mir");
    assert!(
        doc.parse_failures.is_empty(),
        "a real 186 KB dump must parse cleanly: {:#?}",
        doc.parse_failures
    );
    let paths: Vec<&str> = doc.bodies.iter().map(|b| b.path.as_str()).collect();
    assert!(
        paths.contains(&"notify_decision::{closure#0}"),
        "the analyzable body of the `#[workflow]` fn; got {paths:#?}"
    );
    assert!(
        paths.contains(&"__autumn_workflow_info_notify_decision"),
        "the discovery anchor the `#[workflow]` macro emits"
    );
    assert!(
        paths.contains(&"notify_decision"),
        "the sync shim that builds the coroutine"
    );
    // Real dumps carry `<impl at WORKSPACE-RELATIVE path>` headers.
    assert!(
        paths
            .iter()
            .any(|p| p.starts_with("<impl at autumn-harvest/examples/deterministic_primitives.rs:")),
        "derive impls must keep their workspace-relative span headers"
    );
}

// ── tolerance: never panic, always record ───────────────────────────────────

fn parse_no_panic(label: &str, text: &str) -> MirDoc {
    let owned = text.to_string();
    std::panic::catch_unwind(move || mir::parse("fixture", "truncated.mir", &owned))
        .unwrap_or_else(|_| panic!("mir::parse panicked on {label}; it must never panic (D1)"))
}

#[test]
fn truncated_input_never_panics() {
    let text = read_fixture("spike.mir");
    let len = text.len();
    // 20 "random-ish" byte offsets, deterministically chosen so a failure reproduces.
    for i in 0..20_usize {
        let raw = (i * 7919 + 1237) % len;
        // Truncate on a char boundary (the fixture is ASCII, but be explicit).
        let cut = (0..=raw)
            .rev()
            .find(|c| text.is_char_boundary(*c))
            .unwrap_or(0);
        let doc = parse_no_panic(&format!("truncation at byte {cut}"), &text[..cut]);
        // Whatever survived must still be self-consistent.
        for b in &doc.bodies {
            assert!(!b.path.is_empty(), "a body with an empty path at cut {cut}");
        }
    }
}

#[test]
fn truncation_inside_a_body_is_recorded_as_a_parse_failure() {
    let text = read_fixture("spike.mir");
    // Cut in the middle of `wf::{closure#0}` (its header is at line 275).
    let cut = text
        .find("fn wf::{closure#0}")
        .map(|i| i + 4_000)
        .expect("wf::{closure#0} header");
    let doc = parse_no_panic("mid-body truncation", &text[..cut]);
    assert!(
        !doc.parse_failures.is_empty(),
        "an unterminated body must surface as a `mir-parse` boundary, not vanish silently"
    );
    let failure = &doc.parse_failures[0];
    assert!(
        failure.line > 0,
        "a parse failure must name its line: {failure:?}"
    );
    assert!(!failure.reason.is_empty());
}

#[test]
fn injected_junk_lines_never_panic_and_are_recorded() {
    let text = read_fixture("spike.mir");
    let junk = [
        "fn ???(_1: !!!) -> {",
        "    bb0: { _0 = ",
        "}}}}}}",
        "alloc9999 (static: , size: , align: ) {",
        "\u{0}\u{1}\u{2} not mir at all",
        "const : = {",
    ];
    let mut out = String::new();
    for (i, line) in text.lines().enumerate() {
        out.push_str(line);
        out.push('\n');
        if i % 97 == 0 {
            out.push_str(junk[(i / 97) % junk.len()]);
            out.push('\n');
        }
    }
    let doc = parse_no_panic("junk-injected", &out);
    assert!(
        !doc.parse_failures.is_empty(),
        "injected junk must be reported"
    );
    // The good bodies around the junk still parse.
    assert!(
        doc.bodies.iter().any(|b| b.path == "helper"),
        "a junk line must not swallow the neighbouring bodies"
    );
}

#[test]
fn empty_and_whitespace_input_is_an_empty_doc() {
    for text in ["", "\n\n\n", "   ", "// only a comment\n"] {
        let doc = parse_no_panic("degenerate", text);
        assert!(doc.bodies.is_empty());
        assert!(doc.statics.is_empty());
        assert!(
            doc.parse_failures.is_empty(),
            "nothing to fail on: {:#?}",
            doc.parse_failures
        );
    }
}

// ── pure-logic sanity (already implemented in the scaffold: expected GREEN) ──

#[test]
fn terminator_successors_excludes_unwind_edges() {
    let call = Terminator::Call {
        dest: local(0),
        callee: Some("f".to_string()),
        indirect: None,
        args: Vec::new(),
        target: Some("bb2".to_string()),
        unwind: Some("bb9".to_string()),
    };
    assert_eq!(
        call.successors(),
        vec!["bb2"],
        "unwind edges are not CFG successors (D5)"
    );

    let diverging = Terminator::Call {
        dest: local(0),
        callee: Some("panic".to_string()),
        indirect: None,
        args: Vec::new(),
        target: None,
        unwind: None,
    };
    assert!(diverging.successors().is_empty());

    assert_eq!(
        Terminator::Goto {
            target: "bb1".into()
        }
        .successors(),
        vec!["bb1"]
    );
    assert!(Terminator::Return.successors().is_empty());
    assert!(Terminator::Unreachable.successors().is_empty());
    assert_eq!(
        Terminator::SwitchInt {
            operand: Operand::Move(local(3)),
            targets: vec!["bb1".into(), "bb2".into()],
        }
        .successors(),
        vec!["bb1", "bb2"]
    );
    assert_eq!(
        Terminator::Drop {
            place: local(4),
            target: "bb5".into(),
            unwind: Some("bb6".into())
        }
        .successors(),
        vec!["bb5"]
    );
    assert_eq!(
        Terminator::Assert {
            operand: Operand::Copy(local(7)),
            target: "bb8".into(),
            unwind: Some("bb9".into()),
        }
        .successors(),
        vec!["bb8"]
    );
}
