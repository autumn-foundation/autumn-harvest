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

// ── API GAP ─────────────────────────────────────────────────────────────────
//
// Everything below is written against an API the GREEN agent must add. The
// exact proposed signatures come first; the tests follow, verbatim, ready to be
// uncommented.
//
// ```rust
// // autumn-harvest-verify/src/resolve/mod.rs
//
// /// Where a call goes.
// #[derive(Debug, Clone, PartialEq, Eq)]
// pub enum Resolution {
//     /// A body present in the analyzed doc set, by its MIR path.
//     Body(String),
//     /// An honest analysis boundary (D7/D9); the `String` is the `Boundary::detail`.
//     Boundary(crate::BoundaryKind, String),
//     /// A body outside the analyzed MIR (std/core/alloc or a `[trusted]` crate),
//     /// by its callee path. Taint propagates through it; it never shadows a source.
//     External(String),
// }
//
// /// A generic substitution: type-parameter name -> concrete type text.
// #[derive(Debug, Clone, Default, PartialEq, Eq)]
// pub struct Substitution(pub std::collections::BTreeMap<String, String>);
//
// impl Substitution {
//     #[must_use] pub fn new() -> Self;
//     pub fn bind(&mut self, param: &str, ty: &str);
//     #[must_use] pub fn get(&self, param: &str) -> Option<&str>;
//     /// Rewrite a callee path's type parameters under this substitution.
//     #[must_use] pub fn apply(&self, path: &str) -> String;
// }
//
// impl Program {
//     /// One body by MIR path (the first, when rustc printed duplicates).
//     #[must_use] pub fn body(&self, path: &str) -> Option<&crate::mir::Body>;
//     /// Every body path in the doc set, in doc then file order.
//     #[must_use] pub fn body_paths(&self) -> Vec<&str>;
//     /// Resolve a printed callee path as seen from `caller_body`.
//     #[must_use] pub fn resolve_call(&self, caller_body: &str, callee: &str) -> Resolution;
//     /// Resolve the Call terminator of `block` in `caller_body`, including the
//     /// indirect form (`_8 = copy _5()`), which has no callee path to pass to
//     /// `resolve_call`. `None` when the block does not end in a Call.
//     #[must_use] pub fn resolve_terminator(&self, caller_body: &str, block: &str) -> Option<Resolution>;
//     /// The substitution a call from `caller_body` to `callee` induces on the
//     /// callee's body (turbofish by elimination + header/arg-type unification, D6).
//     #[must_use] pub fn call_substitution(&self, caller_body: &str, callee: &str) -> Substitution;
//     /// `body`'s callee paths with `subst` applied to each.
//     #[must_use] pub fn substituted_callees(&self, body: &str, subst: &Substitution) -> Vec<String>;
// }
// ```
//
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
