//! Call-target resolution over the checked-in MIR fixtures (D7).
//!
//! The resolution *query* API proposed during the RED phase is implemented and
//! its tests are enabled below; the signatures they were written against are
//! reproduced verbatim in the doc block above them.

use std::path::{Path, PathBuf};

use autumn_harvest_verify::BoundaryKind;
use autumn_harvest_verify::mir::{self, MirDoc};
use autumn_harvest_verify::resolve::{Program, Resolution, SourceRoots};

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

/// Source roots for the hand-written fixtures: their `<impl at FILE:l:c>`
/// headers are relative to `tests/fixtures/`.
fn fixture_roots() -> SourceRoots {
    SourceRoots {
        roots: vec![fixtures_dir()],
    }
}

/// Source roots for the real example dump: its headers are workspace-relative.
fn workspace_roots() -> SourceRoots {
    SourceRoots {
        roots: vec![workspace_root()],
    }
}

// ── what today's API can already assert ─────────────────────────────────────

#[test]
fn build_keeps_every_doc_it_was_given() {
    let docs = vec![
        parse_fixture("spike.mir"),
        parse_fixture("format_and_outparams.mir"),
    ];
    let program = Program::build(docs, &fixture_roots()).expect("build over well-formed docs");
    assert_eq!(program.docs.len(), 2);
    assert_eq!(program.docs[0].path, "spike.mir");
    assert_eq!(program.docs[1].path, "format_and_outparams.mir");
}

#[test]
fn build_tolerates_an_unresolvable_impl_header() {
    // No source roots at all: `<impl at spike.rs:9:1: 9:9>` cannot be read back
    // to `(Ctx, None, emit)`. That is a `missing-body` boundary at analysis
    // time, never a build error (D7).
    let docs = vec![parse_fixture("spike.mir")];
    let program = Program::build(docs, &SourceRoots::default())
        .expect("missing sources must not fail the build");
    assert_eq!(program.docs.len(), 1);
}

#[test]
fn build_over_the_real_example_dump_succeeds() {
    let docs = vec![parse_fixture("example_deterministic_primitives.mir")];
    let program = Program::build(docs, &workspace_roots()).expect("build over the real example");
    assert_eq!(program.docs.len(), 1);
    assert!(program.docs[0].parse_failures.is_empty());
}

#[test]
fn inherent_method_resolves_to_its_impl_body() {
    let program = Program::build(vec![parse_fixture("spike.mir")], &fixture_roots()).unwrap();
    assert_eq!(
        program.resolve_call("wf::{closure#0}", "Ctx::emit"),
        Resolution::Body("<impl at spike.rs:9:1: 9:9>::emit".to_string())
    );
}

#[test]
fn trait_method_on_a_concrete_type_resolves_to_its_impl_body() {
    let program = Program::build(vec![parse_fixture("spike.mir")], &fixture_roots()).unwrap();
    assert_eq!(
        program.resolve_call("wf::{closure#0}", "<A as Src>::next"),
        Resolution::Body("<impl at spike.rs:5:1: 5:15>::next".to_string())
    );
}

#[test]
fn closure_span_resolves_to_the_numbered_closure_body() {
    let program = Program::build(vec![parse_fixture("spike.mir")], &fixture_roots()).unwrap();
    // The `Fn::call` call site names the closure only by its span.
    assert_eq!(
        program.resolve_call(
            "wf::{closure#0}",
            "<{closure@spike.rs:18:13: 18:21} as Fn<(u32,)>>::call",
        ),
        Resolution::Body("wf::{closure#0}::{closure#1}".to_string())
    );
    // The `LocalKey::with` turbofish names the other one.
    assert_eq!(
        program.resolve_call(
            "wf::{closure#0}",
            "LocalKey::<RefCell<u64>>::with::<{closure@spike.rs:16:21: 16:24}, u64>",
        ),
        Resolution::Body("wf::{closure#0}::{closure#0}".to_string())
    );
}

#[test]
fn async_fn_resolves_to_its_coroutine_body() {
    let program = Program::build(vec![parse_fixture("spike.mir")], &fixture_roots()).unwrap();
    assert_eq!(
        program.resolve_call("wf::{closure#0}", "sub"),
        Resolution::Body("sub::{closure#0}".to_string()),
        "`sub` is the shim that builds the coroutine; the analyzable body is `sub::{{closure#0}}`"
    );
    assert_eq!(
        program.resolve_call(
            "wf::{closure#0}",
            "<{async fn body of sub()} as Future>::poll"
        ),
        Resolution::Body("sub::{closure#0}".to_string())
    );
}

#[test]
fn dyn_dispatch_with_no_unsized_impl_in_the_doc_set_is_a_boundary() {
    // `wf` receives an already-unsized `Box<dyn Src>`; the coercion happened in
    // a caller that is not in this dump, so RTA-lite sees ZERO candidate types.
    let program = Program::build(vec![parse_fixture("spike.mir")], &fixture_roots()).unwrap();
    assert_eq!(
        program.resolve_call("wf::{closure#0}", "<dyn Src as Src>::next"),
        Resolution::Boundary(
            BoundaryKind::DynDispatch,
            "<dyn Src as Src>::next".to_string()
        )
    );
}

#[test]
fn dyn_dispatch_with_two_unsized_impls_is_a_boundary() {
    let program = Program::build(
        vec![parse_fixture("format_and_outparams.mir")],
        &fixture_roots(),
    )
    .unwrap();
    assert_eq!(
        program.resolve_call(
            "wf_dyn_two_impls::{closure#0}",
            "<dyn Namer as Namer>::name"
        ),
        Resolution::Boundary(
            BoundaryKind::DynDispatch,
            "<dyn Namer as Namer>::name".to_string()
        ),
        "StaticNamer and CountingNamer are both unsized to `dyn Namer` in this dump"
    );
}

#[test]
fn dyn_dispatch_with_exactly_one_unsized_impl_is_devirtualized() {
    let program = Program::build(
        vec![parse_fixture("format_and_outparams.mir")],
        &fixture_roots(),
    )
    .unwrap();
    assert_eq!(
        program.resolve_call(
            "wf_dyn_single_impl::{closure#0}",
            "<dyn Clock as Clock>::now_secs"
        ),
        Resolution::Body("<impl at format_and_outparams.rs:123:1: 123:27>::now_secs".to_string()),
        "closed-world assumption: exactly one type is unsized to `dyn Clock` here"
    );
}

#[test]
fn indirect_call_is_an_indirect_call_boundary() {
    let program = Program::build(
        vec![parse_fixture("format_and_outparams.mir")],
        &fixture_roots(),
    )
    .unwrap();
    // `_8 = copy _5() -> [return: bb4, unwind: bb6];`
    let resolution = program
        .resolve_terminator("wf_fn_pointer::{closure#0}", "bb3")
        .expect("bb3 ends in an indirect Call");
    assert!(
        matches!(
            resolution,
            Resolution::Boundary(BoundaryKind::IndirectCall, _)
        ),
        "got {resolution:?}"
    );
}

#[test]
fn extern_c_declaration_is_an_ffi_boundary() {
    let program = Program::build(
        vec![parse_fixture("format_and_outparams.mir")],
        &fixture_roots(),
    )
    .unwrap();
    assert_eq!(
        program.resolve_call("wf_ffi_clock::{closure#0}", "clock_gettime"),
        Resolution::Boundary(BoundaryKind::Ffi, "clock_gettime".to_string()),
        "an `unsafe extern \"C\"` fn has no MIR body and must never be treated as clean"
    );
}

#[test]
fn a_std_callee_is_external_not_a_boundary() {
    let program = Program::build(vec![parse_fixture("spike.mir")], &fixture_roots()).unwrap();
    assert_eq!(
        program.resolve_call("deep", "SystemTime::now"),
        Resolution::External("SystemTime::now".to_string()),
        "std/core/alloc are trusted by table; External never shadows a source rule"
    );
}

#[test]
fn generic_substitution_is_threaded_through_the_turbofish() {
    let program = Program::build(vec![parse_fixture("spike.mir")], &fixture_roots()).unwrap();
    // `_5 = helper::<HashMap<String, u32>>(copy _4)` binds `T`.
    let subst = program.call_substitution("wf::{closure#0}", "helper::<HashMap<String, u32>>");
    assert_eq!(subst.get("T"), Some("HashMap<String, u32>"));
    assert_eq!(
        program.substituted_callees("helper", &subst),
        vec![
            "<HashMap<String, u32> as IntoIterator>::into_iter".to_string(),
            "<<HashMap<String, u32> as IntoIterator>::IntoIter as Iterator>::collect::<Vec<(String, u32)>>"
                .to_string(),
        ],
        "without this, the HashMap iteration order-source inside `helper` is invisible"
    );
}

#[test]
fn generic_substitution_survives_two_layers() {
    let program =
        Program::build(vec![parse_fixture("generic_layers.mir")], &fixture_roots()).unwrap();
    let outer = program.call_substitution("entry", "outer::<Wrapper<Leaf>>");
    assert_eq!(outer.get("T"), Some("Wrapper<Leaf>"));
    assert_eq!(
        program.substituted_callees("outer", &outer),
        vec![
            "inner::<Wrapper<Leaf>>".to_string(),
            "inner::<Wrapper<Leaf>>".to_string()
        ]
    );
    let inner = program.call_substitution("outer", "inner::<Wrapper<Leaf>>");
    assert_eq!(
        program.substituted_callees("inner", &inner),
        vec!["<Wrapper<Leaf> as Score>::score".to_string()]
    );
    assert_eq!(
        program.resolve_call("inner", "<Wrapper<Leaf> as Score>::score"),
        Resolution::Body("<impl at generic_layers.rs:21:1: 21:36>::score".to_string())
    );
}

#[test]
fn an_unbound_type_parameter_is_an_unresolved_generic_boundary() {
    let program =
        Program::build(vec![parse_fixture("generic_layers.mir")], &fixture_roots()).unwrap();
    assert_eq!(
        program.resolve_call("inner", "<T as Score>::score"),
        Resolution::Boundary(BoundaryKind::UnresolvedGeneric, "T".to_string()),
        "with no substitution in hand the honest answer is `unknown`, not `clean` (A21)"
    );
}

#[test]
fn a_body_that_is_simply_absent_is_a_missing_body_boundary() {
    let program = Program::build(vec![parse_fixture("spike.mir")], &fixture_roots()).unwrap();
    assert_eq!(
        program.resolve_call("wf::{closure#0}", "some_crate::never_emitted"),
        Resolution::Boundary(
            BoundaryKind::ExternalCrateBody,
            "some_crate::never_emitted".to_string()
        )
    );
}

#[test]
fn the_real_example_workflow_body_is_reachable_by_path() {
    let program = Program::build(
        vec![parse_fixture("example_deterministic_primitives.mir")],
        &workspace_roots(),
    )
    .unwrap();
    assert!(program.body("notify_decision::{closure#0}").is_some());
    assert!(
        program
            .body_paths()
            .contains(&"__autumn_workflow_info_notify_decision")
    );
}

// ── Review round 2: shadowed statics and shadowed impl methods ──────────────

#[test]
fn statics_are_indexed_by_their_full_printed_path() {
    let program = Program::build(
        vec![parse_fixture("shadowed_statics.mir")],
        &fixture_roots(),
    )
    .unwrap();
    let a = program.static_named("a::COUNTER").expect("a::COUNTER");
    assert_eq!(a.path, "a::COUNTER");
    assert_eq!(a.ty, "u64");
    let b = program.static_named("b::COUNTER").expect("b::COUNTER");
    assert_eq!(b.path, "b::COUNTER");
    assert!(
        b.ty.contains("Atomic"),
        "the atomic must not be shadowed by the plain one; got {}",
        b.ty
    );
}

#[test]
fn an_alloc_footer_resolves_to_the_static_it_names() {
    let doc = parse_fixture("shadowed_statics.mir");
    let alloc = doc
        .alloc_statics
        .iter()
        .find(|(_, name)| name.as_str() == "b::COUNTER")
        .map(|(alloc, _)| alloc.clone())
        .expect("an alloc for b::COUNTER");
    let program = Program::build(vec![doc], &fixture_roots()).unwrap();
    let doc = program.docs.first();
    let item = program
        .static_of_alloc(doc, &alloc)
        .expect("the alloc resolves");
    assert_eq!(item.path, "b::COUNTER");
}

#[test]
fn shadowed_impl_methods_resolve_by_their_module_path() {
    let program =
        Program::build(vec![parse_fixture("shadowed_impls.mir")], &fixture_roots()).unwrap();
    let ambient = program.resolve_call("wf_calls_ambient_worker::{closure#0}", "b::Worker::run");
    assert!(
        matches!(&ambient, Resolution::Body(path) if path.starts_with("b::<impl at")),
        "`b::Worker::run` must reach b's body; got {ambient:?}"
    );
    let clean = program.resolve_call("wf_calls_clean_worker::{closure#0}", "a::Worker::run");
    assert!(
        matches!(&clean, Resolution::Body(path) if path.starts_with("a::<impl at")),
        "`a::Worker::run` must reach a's body; got {clean:?}"
    );
}

/// With no module qualifier to go on, the answer must cover BOTH candidates
/// rather than silently pick the first.
#[test]
fn an_unqualified_shadowed_impl_method_unions_its_candidates() {
    let program =
        Program::build(vec![parse_fixture("shadowed_impls.mir")], &fixture_roots()).unwrap();
    let bare = program.resolve_call("wf_calls_clean_worker::{closure#0}", "Worker::run");
    let Resolution::Bodies(paths, _) = &bare else {
        panic!("a bare `Worker::run` is ambiguous; got {bare:?}");
    };
    assert_eq!(paths.len(), 2, "got {paths:?}");
}

/// A name that is only a last segment stays ambiguous, and the resolver hands
/// back BOTH candidates rather than the first one it indexed.
#[test]
fn a_bare_static_last_segment_resolves_to_every_candidate() {
    let program = Program::build(
        vec![parse_fixture("shadowed_statics.mir")],
        &fixture_roots(),
    )
    .unwrap();
    let mut paths: Vec<&str> = program
        .statics_named_all("COUNTER")
        .into_iter()
        .map(|item| item.path.as_str())
        .collect();
    paths.sort_unstable();
    assert_eq!(paths, vec!["a::COUNTER", "b::COUNTER"]);
    assert!(
        program.static_named("COUNTER").is_none(),
        "an ambiguous bare name has no single answer"
    );
}

// ── closure spans two bodies share ──────────────────────────────────────────

/// A closure written in a `macro_rules!` body carries the macro *definition*'s
/// span, so every expansion prints the same `{closure@..}`. Indexing them with
/// "first one wins" resolves the second expansion's call site into the first
/// expansion's body — which is how a wall-clock read comes back `proven`.
#[test]
fn a_closure_span_two_bodies_share_resolves_to_the_enclosing_ones() {
    let program = Program::build(
        vec![parse_fixture("shadowed_closures.mir")],
        &fixture_roots(),
    )
    .unwrap();
    let span = "{closure@shadowed_closures.rs:49:9: 49:17}";
    let mut both = program.closure_bodies(span).to_vec();
    both.sort();
    assert_eq!(
        both,
        vec![
            "wf_macro_closure_ambient::{closure#0}::{closure#0}".to_string(),
            "wf_macro_closure_clean::{closure#0}::{closure#0}".to_string(),
        ],
        "both expansions must be indexed under the span they share"
    );

    let callee = format!("Option::<u64>::map::<u64, {span}>");
    let ambient = program.resolve_call("wf_macro_closure_ambient::{closure#0}", &callee);
    assert_eq!(
        ambient,
        Resolution::Body("wf_macro_closure_ambient::{closure#0}::{closure#0}".to_string()),
        "the call site's own body disambiguates the span"
    );
    let clean = program.resolve_call("wf_macro_closure_clean::{closure#0}", &callee);
    assert_eq!(
        clean,
        Resolution::Body("wf_macro_closure_clean::{closure#0}::{closure#0}".to_string()),
    );
}

/// With nothing to disambiguate on, every body the span names is analyzed.
#[test]
fn an_undisambiguated_closure_span_unions_its_candidates() {
    let program = Program::build(
        vec![parse_fixture("shadowed_closures.mir")],
        &fixture_roots(),
    )
    .unwrap();
    let callee = "Option::<u64>::map::<u64, {closure@shadowed_closures.rs:49:9: 49:17}>";
    let from_elsewhere = program.resolve_call("wall_clock_secs", callee);
    let Resolution::Bodies(paths, _) = &from_elsewhere else {
        panic!("an unrelated caller cannot pick one; got {from_elsewhere:?}");
    };
    assert_eq!(paths.len(), 2, "got {paths:?}");
}

// ── drop glue two types share ───────────────────────────────────────────────

/// `clean::Guard` and `ambient::Guard` answer to the same last segment, so the
/// glue lookup must narrow on the dropped type's module — and never come back
/// empty, which reads as "this type has no user `Drop` impl" and drops the
/// whole glue body on the floor.
#[test]
fn shadowed_drop_glue_resolves_by_the_dropped_types_module() {
    let program =
        Program::build(vec![parse_fixture("shadowed_drops.mir")], &fixture_roots()).unwrap();
    let ambient = program.drop_targets("ambient::Guard<'_>");
    assert_eq!(
        ambient.bodies.len(),
        1,
        "the module qualifier picks exactly one; got {:?}",
        ambient.bodies
    );
    assert!(
        ambient.bodies[0].starts_with("ambient::<impl at"),
        "got {:?}",
        ambient.bodies
    );
    assert!(ambient.unresolved.is_none(), "{:?}", ambient.unresolved);

    let clean = program.drop_targets("clean::Guard<'_>");
    assert!(
        clean.bodies.len() == 1 && clean.bodies[0].starts_with("clean::<impl at"),
        "got {:?}",
        clean.bodies
    );

    let bare = program.drop_targets("Guard");
    assert_eq!(
        bare.bodies.len(),
        2,
        "with no module to go on both glue bodies are followed; got {:?}",
        bare.bodies
    );

    assert!(
        program.drop_targets("std::string::String").is_inert(),
        "a type with no user `Drop` impl in the analyzed set runs no user code"
    );
}

/// Analyzing a pre-emitted dump with no source root leaves every `<impl at ..>`
/// header unreadable, so nothing says these `::drop` bodies are `Drop` impls.
/// That is a boundary, not an inert drop.
#[test]
fn unresolvable_drop_glue_is_a_boundary_not_an_inert_drop() {
    let program = Program::build(
        vec![parse_fixture("shadowed_drops.mir")],
        &SourceRoots::default(),
    )
    .unwrap();
    let targets = program.drop_targets("ambient::Guard<'_>");
    assert!(
        targets.unresolved.is_some(),
        "an unreadable impl header must be named, not assumed inert"
    );
    assert!(
        program.drop_targets("std::string::String").is_inert(),
        "a std type is still inert: nothing in the analyzed set drops it"
    );
}
