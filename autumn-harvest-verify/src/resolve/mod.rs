//! Call-target resolution: free fns, `<impl at file:l:c>` bodies (via `syn`), closures,
//! async bodies, generic substitution and RTA-lite devirtualization (D7).
//!
//! Everything the taint analysis knows about *where a call goes* is decided
//! here, and every answer is one of three shapes ([`Resolution`]): a body in the
//! analyzed set, a body outside it that is modelled as a pure propagator, or a
//! **named** analysis boundary. There is deliberately no fourth shape meaning
//! "assume it is clean".
//!
//! # How a printed callee becomes a body
//!
//! | printed at the call site | resolved to |
//! |---|---|
//! | `stamp`, `pairs::<HashMap<..>>` | the body of that path, turbofish stripped |
//! | `Ctx::emit`, `<HashSet<String> as Plan>::steps` | the `<impl at FILE:L:C>` body whose header names that `(self type, trait, method)` |
//! | `sub` returning `{async fn body of sub()}` | `sub::{closure#0}` — the shim only builds the coroutine |
//! | `<{async fn body of sub()} as Future>::poll` | `sub::{closure#0}` |
//! | `<{closure@f.rs:1:1: 1:2} as Fn<..>>::call` | the body whose first parameter has that closure type |
//! | `<dyn Tr as Tr>::m` with exactly one unsized impl type | that type's impl body (RTA-lite) |
//! | `<dyn Tr as Tr>::m` with zero or several | [`BoundaryKind::DynDispatch`] |
//! | `copy _5(..)` (no path at all) | [`BoundaryKind::IndirectCall`] |
//! | an `unsafe extern "C"` fn | [`BoundaryKind::Ffi`] |
//! | `<T as Tr>::m` with `T` still a type parameter | [`BoundaryKind::UnresolvedGeneric`] |
//! | `some_crate::gone` (rooted at a crate that is neither analyzed nor trusted) | [`BoundaryKind::ExternalCrateBody`] |
//! | `analyzed_crate::gone` | [`BoundaryKind::MissingBody`] |
//! | anything else without a body (`SystemTime::now`, `format`) | [`Resolution::External`] |
//!
//! The last row is the load-bearing default, and it is a *deliberate* asymmetry:
//! rustc prints **trimmed** def-paths, so the overwhelming majority of std calls
//! arrive as `String::clone` or `format` with no crate root at all. Treating
//! those as boundaries would make every workflow `unknown` and the tool useless;
//! treating them as opaque propagators keeps taint flowing through them while
//! the `[[source]]` table stays the only thing that *starts* taint.

mod impls;
mod subst;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use crate::mir::ast::{Body, Local, MirDoc, Operand, Statement, StaticItem, Terminator};
use crate::model::callee::{CalleePath, TypeName};
use crate::util::{
    crate_root, is_segment_suffix, last_segment, looks_like_type_param, peel_containers, peel_refs,
    segments, split_last, split_top_trim, strip_generics_everywhere,
};
use crate::verdict::BoundaryKind;

pub use impls::ImplHeader;
pub use subst::Substitution;

/// Index of source files needed to resolve `<impl at file:line:col>` headers.
#[derive(Debug, Clone, Default)]
pub struct SourceRoots {
    /// Directories that `<impl at PATH>` paths are relative to (workspace root first).
    pub roots: Vec<PathBuf>,
}

/// Where a call goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// A body present in the analyzed doc set, by its MIR path.
    Body(String),
    /// Several bodies any of which this call site could reach, by their MIR
    /// paths, sorted and deduplicated (always two or more), plus what made the
    /// site ambiguous.
    ///
    /// Two analyzed modules can define the same `(self type, method)` pair
    /// (`a::Worker::run` and `b::Worker::run`), and two expansions of one
    /// `macro_rules!` closure carry the same printed `{closure@..}` span. Where
    /// the printed text does not say which body is meant, picking one is a coin
    /// flip that can hide a finding. The analysis descends into **all** of them
    /// and unions the result, which over-approximates: any candidate's finding
    /// is reported.
    Bodies(Vec<String>, Ambiguity),
    /// An honest analysis boundary (D7/D9); the `String` is the `Boundary::detail`.
    Boundary(BoundaryKind, String),
    /// A body outside the analyzed MIR (std/core/alloc or a `[trusted]` crate),
    /// by its callee path. Taint propagates through it; it never shadows a source.
    External(String),
}

/// What the printed text failed to pin down at an ambiguous call site.
///
/// It decides two things a reader needs: how the union may be narrowed (a
/// receiver's declared type narrows impls and says nothing about closures) and
/// which noun the hop and the report warning use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ambiguity {
    /// Several `(self type, trait, method)` impl bodies answer to the callee.
    Impl,
    /// Several closure bodies are printed with the same `{closure@..}` span.
    Closure,
}

impl Ambiguity {
    /// The noun the hop note and the warning use (`ambiguous impl`, ...).
    #[must_use]
    pub const fn noun(self) -> &'static str {
        match self {
            Self::Impl => "impl",
            Self::Closure => "closure",
        }
    }
}

/// Key of an impl method: `(self type name, trait name, method)`.
type ImplKey = (String, Option<String>, String);

/// One impl method a key can denote, with the module path that tells it apart
/// from a same-named impl elsewhere in the analyzed set.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ImplCandidate {
    /// Module path the impl block sits in, exactly as rustc trimmed it in the
    /// body path (`a` for `a::<impl at f.rs:30:5: 30:16>::run`), or `""` at the
    /// crate root.
    module: String,
    /// The body id.
    body: String,
}

/// One `::drop` body, with what the analyzed set knows about its impl block.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DropCandidate {
    /// Module the impl block sits in, as [`module_of_impl`] reads it.
    module: String,
    /// The glue body id.
    body: String,
    /// Why the impl header could not be read, when it could not — in which case
    /// nothing says this body is a `Drop` impl at all, only that it is a
    /// `::drop` whose receiver type matches.
    unresolved: Option<String>,
}

/// What a `drop(place)` terminator runs, as far as the analyzed set knows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DropTargets {
    /// `Drop` impl bodies to follow: narrowed by the dropped type's module
    /// where that selects some, unioned where it does not.
    pub bodies: Vec<String>,
    /// Why the glue could not be pinned down, when it could not — the
    /// `drop-glue` boundary's detail.
    ///
    /// Independent of [`Self::bodies`]: a type can have one glue body the
    /// analyzer read and another it could not, and both are reported.
    pub unresolved: Option<String>,
}

impl DropTargets {
    /// Nothing in the analyzed set runs user code when this type is dropped —
    /// every std type, and any plain struct with no `Drop` impl.
    #[must_use]
    pub const fn is_inert(&self) -> bool {
        self.bodies.is_empty() && self.unresolved.is_none()
    }
}

/// The resolved program: all bodies across all docs plus the lookup tables.
#[derive(Debug, Default)]
pub struct Program {
    pub docs: Vec<MirDoc>,
    /// Body path → (doc index, body index).
    bodies: BTreeMap<String, (usize, usize)>,
    /// Body paths in doc-then-file order.
    order: Vec<String>,
    /// Plain path → every body id that prints under it (more than one only
    /// when two analyzed crates export the same trimmed path).
    ambiguous: BTreeMap<String, Vec<String>>,
    /// `crate_name::path` → the body id.
    ///
    /// rustc prints *trimmed* def-paths, and how much it trims depends on what
    /// else is in scope: `harvest_verify_corpus_helpers::origin_tag` at a call
    /// site is the very same body the helpers crate's own dump printed as
    /// `origin_tag`, lengthened only because `helpers_deep` exports the name too.
    qualified: BTreeMap<String, String>,
    /// `(self type, trait, method)` → every impl body that answers to it.
    impl_methods: BTreeMap<ImplKey, Vec<ImplCandidate>>,
    /// Impl body path → its header.
    impl_headers: BTreeMap<String, ImplHeader>,
    /// `{closure@FILE:l:c: l:c}` / `{async block@..}` → every body printed with
    /// that span.
    ///
    /// A span is **not** a key: a closure written in a `macro_rules!` body
    /// carries the macro *definition*'s span, so every expansion of it prints
    /// the same `{closure@..}` for a different body — and so do the several
    /// bodies one `#[workflow]` attribute expands to. Keeping the first one
    /// resolved the other expansions' call sites into a body they never call,
    /// which is a `proven-deterministic` over code that was never analyzed.
    closures: BTreeMap<String, Vec<String>>,
    /// `f` (and the path printed inside `{async fn body of f()}`) → `f::{closure#0}`.
    async_bodies: BTreeMap<String, String>,
    /// Trait name → `(concrete type, the body that built the trait object)`.
    unsized_to: BTreeMap<String, BTreeSet<(String, String)>>,
    /// Static / thread-local FULL printed path → item.
    ///
    /// rustc 1.94-1.98 print both the `static PATH: Ty` item header and the
    /// `allocN (static: PATH, ..)` footer with the same trimmed-but-qualified
    /// path, so `a::COUNTER: u64` and `b::COUNTER: AtomicU64` are two distinct
    /// keys here. Keying on the last segment collapsed them, and whichever the
    /// index happened to keep decided whether reading the *other* was ambient.
    statics: BTreeMap<String, StaticItem>,
    /// Last path segment → every full path indexed under it, in insertion order.
    statics_by_last: BTreeMap<String, Vec<String>>,
    /// `allocN` → every static name any doc gave it.
    ///
    /// Alloc ids are numbered per dump, so two docs routinely disagree about
    /// what `alloc1` is; the set keeps both rather than letting the first win.
    alloc_statics: BTreeMap<String, BTreeSet<String>>,
    /// Body id → the crate whose dump defined it.
    crate_of: BTreeMap<String, String>,
    /// Crate names present in the analyzed set.
    crates: BTreeSet<String>,
    sources: impls::SourceIndex,
    /// Item path → why the parser could not read that item's body.
    ///
    /// A call into one of these is a call into a body the analysis never saw,
    /// which is a [`BoundaryKind::MirParse`], not a silent propagator.
    parse_failed: BTreeMap<String, String>,
    /// Method name → the `<impl at FILE>` header that could not be resolved.
    ///
    /// An impl block whose source file is missing or unreadable indexes no
    /// methods at all, so `Ty::m` falls through to the body-less default. The
    /// method name is the only thing left to key the boundary on.
    unresolved_impl_methods: BTreeMap<String, String>,
    /// Dropped type name → every `::drop` body the analyzed set has for it.
    drop_glue: BTreeMap<String, Vec<DropCandidate>>,
    /// `::drop` bodies whose self type could not be determined at all.
    ///
    /// Nothing then rules out their being the glue of *any* dropped type, so
    /// while this is non-empty every drop is a boundary. In practice it stays
    /// empty: MIR declares a glue body's receiver (`_1: &mut clean::Guard<'_>`)
    /// even where no source root can be read, which is exactly why the index
    /// falls back to it.
    drop_glue_untyped: Vec<String>,
}

impl Program {
    /// Build the resolution tables. Unresolvable impl headers are kept and surface as
    /// `missing-body` boundaries when called.
    ///
    /// # Errors
    /// Only on i/o failure reading a source root that exists but is unreadable.
    pub fn build(docs: Vec<MirDoc>, sources: &SourceRoots) -> crate::Result<Self> {
        let mut program = Self {
            docs,
            ..Self::default()
        };
        program.index_bodies();
        program.index_parse_failures();
        let files = program.referenced_source_files();
        program.sources = impls::SourceIndex::build(&sources.roots, &files);
        program.index_impls();
        program.index_drop_glue();
        program.index_rta();
        Ok(program)
    }

    // ── indexing ────────────────────────────────────────────────────────────

    fn index_bodies(&mut self) {
        // A trimmed path is only unique *within* one dump: two analyzed crates
        // can both export `origin_tag`. Where that happens the body id carries
        // the crate name, so a call never resolves to the wrong crate's body
        // (which would look like recursion and lose the flow).
        let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
        for doc in &self.docs {
            for body in &doc.bodies {
                *seen.entry(body.path.as_str()).or_insert(0) += 1;
            }
        }
        let collides: BTreeSet<String> = seen
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(path, _)| path.to_string())
            .collect();

        for (doc_at, doc) in self.docs.iter().enumerate() {
            self.crates.insert(doc.crate_name.clone());
            for (name, value) in &doc.alloc_statics {
                self.alloc_statics
                    .entry(name.clone())
                    .or_default()
                    .insert(value.clone());
            }
            for item in &doc.statics {
                index_static(&mut self.statics, &mut self.statics_by_last, item.clone());
            }
            for (body_at, body) in doc.bodies.iter().enumerate() {
                let id = if collides.contains(&body.path) {
                    format!("{}::{}", doc.crate_name, body.path)
                } else {
                    body.path.clone()
                };
                self.order.push(id.clone());
                self.ambiguous
                    .entry(body.path.clone())
                    .or_default()
                    .push(id.clone());
                self.qualified
                    .entry(format!("{}::{}", doc.crate_name, body.path))
                    .or_insert_with(|| id.clone());
                self.crate_of.insert(id.clone(), doc.crate_name.clone());
                self.bodies.entry(id).or_insert((doc_at, body_at));
                if body.is_const {
                    // A `const NAME: Ty = {..}` body is how a `thread_local!` key
                    // and a promoted constant reach the analysis.
                    index_static(
                        &mut self.statics,
                        &mut self.statics_by_last,
                        StaticItem {
                            path: body.path.clone(),
                            ty: body.return_ty.clone(),
                            is_mut: false,
                        },
                    );
                }
                if let Some(span) = closure_param_span(body) {
                    let id = self
                        .qualified
                        .get(&format!("{}::{}", doc.crate_name, body.path))
                        .cloned()
                        .unwrap_or_else(|| body.path.clone());
                    let under = self.closures.entry(span).or_default();
                    if !under.contains(&id) {
                        under.push(id);
                    }
                }
            }
        }
        // Async shims: `fn f(..) -> {async fn body of m::f()}` means the body to
        // analyze for a call to `f` is `f::{closure#0}`.
        let paths: BTreeSet<&str> = self.order.iter().map(String::as_str).collect();
        let mut async_bodies: BTreeMap<String, String> = BTreeMap::new();
        for doc in &self.docs {
            for body in &doc.bodies {
                let Some(inner) = async_body_of(&body.return_ty) else {
                    continue;
                };
                let id = self
                    .qualified
                    .get(&format!("{}::{}", doc.crate_name, body.path))
                    .cloned()
                    .unwrap_or_else(|| body.path.clone());
                let coroutine = format!("{id}::{{closure#0}}");
                let printed = format!("{}::{{closure#0}}", body.path);
                let coroutine = if paths.contains(coroutine.as_str()) {
                    coroutine
                } else if paths.contains(printed.as_str()) {
                    printed
                } else {
                    continue;
                };
                async_bodies.insert(format!("{}::{}", doc.crate_name, inner), coroutine.clone());
                async_bodies.insert(id, coroutine.clone());
                async_bodies.insert(inner, coroutine);
            }
        }
        self.async_bodies = async_bodies;
    }

    /// Source files named by `<impl at FILE:..>` headers and `{closure@FILE:..}` spans.
    fn referenced_source_files(&self) -> BTreeSet<String> {
        let mut files: BTreeSet<String> = BTreeSet::new();
        for doc in &self.docs {
            for body in &doc.bodies {
                if let Some((file, _, _)) = impl_span(&body.path) {
                    files.insert(file);
                }
                for (_, ty) in &body.params {
                    if let Some((file, _, _)) = brace_span(ty) {
                        files.insert(file);
                    }
                }
                if let Some((file, _, _)) = brace_span(&body.return_ty) {
                    files.insert(file);
                }
            }
        }
        files.retain(|file| !file.starts_with("/rustc/"));
        files
    }

    /// Index every doc's parse failures by the item path they name.
    ///
    /// The recorded `item` is the raw header text (`fn helper((((_1: u64) -> …`),
    /// so the path is recovered the same way the header parser recovers it: the
    /// text between `fn ` and the first top-level `(`.
    fn index_parse_failures(&mut self) {
        let mut failed: BTreeMap<String, String> = BTreeMap::new();
        for doc in &self.docs {
            for failure in &doc.parse_failures {
                let Some(path) = failed_item_path(&failure.item) else {
                    continue;
                };
                failed.entry(path).or_insert_with(|| failure.reason.clone());
            }
        }
        self.parse_failed = failed;
    }

    fn index_impls(&mut self) {
        let mut methods: BTreeMap<ImplKey, Vec<ImplCandidate>> = BTreeMap::new();
        let mut headers: BTreeMap<String, ImplHeader> = BTreeMap::new();
        let mut unresolved: BTreeMap<String, String> = BTreeMap::new();
        for path in &self.order {
            let Some((prefix, method)) = split_last(path) else {
                continue;
            };
            let Some((file, line, column)) = impl_span(prefix) else {
                continue;
            };
            let Some(header) = self.sources.impl_header_at(&file, line, column) else {
                // Two very different reasons land here. When the file *was*
                // read, the span usually points at a `#[derive(..)]` attribute
                // rather than at an `impl` keyword: derived impls are structural
                // and following them buys nothing, so they stay silent. When the
                // file could not be read at all (a remapped path, a path
                // dependency outside the source roots, a deleted file), nothing
                // about this impl is known and a call to one of its methods must
                // not fall through to "body-less, assumed clean".
                if let Some(reason) = self.sources.unreadable.get(&file) {
                    let detail = format!("{prefix}: {reason}");
                    unresolved.entry(method.to_string()).or_insert(detail);
                }
                continue;
            };
            let self_name = TypeName::parse(&header.self_ty).name;
            if self_name.is_empty() {
                continue;
            }
            let trait_name = header
                .trait_
                .as_deref()
                .map(|t| TypeName::parse(t).name)
                .filter(|t| !t.is_empty());
            // rustc prints the impl body path with the module it sits in
            // (`a::<impl at f.rs:30:5: 30:16>::run`), trimmed exactly the way
            // the call site's `a::Worker::run` is trimmed — so the two agree
            // and the module is all that tells two same-named impls apart.
            let candidate = ImplCandidate {
                module: module_of_impl(prefix, self.crate_of.get(path).map(String::as_str)),
                body: path.clone(),
            };
            push_candidate(
                methods
                    .entry((self_name.clone(), trait_name, method.to_string()))
                    .or_default(),
                candidate.clone(),
            );
            // The inherent spelling `Ty::m` must resolve too, even for a trait impl.
            push_candidate(
                methods
                    .entry((self_name, None, method.to_string()))
                    .or_default(),
                candidate,
            );
            headers.insert(path.clone(), header);
        }
        self.impl_methods = methods;
        self.impl_headers = headers;
        // A method that some *readable* impl does define is resolvable after
        // all; only the ones nothing in the analyzed set defines stay boundaries.
        unresolved.retain(|method, _| !self.impl_methods.keys().any(|(_, _, name)| name == method));
        self.unresolved_impl_methods = unresolved;
    }

    /// Index every `::drop` body by the type it drops.
    ///
    /// Drop glue is the one call the analysis cannot see in the MIR text: a
    /// `drop(_4)` terminator names a place, never a body. The body is found by
    /// the dropped place's type, so every `<impl at ..>::drop` in the analyzed
    /// set is indexed under the type it takes as `&mut self`.
    ///
    /// Two sources for that type, and the difference is the whole point:
    ///
    ///  * the impl **header**, when the source root could be read — and then the
    ///    body counts as glue only if the header actually names `Drop`, so a
    ///    user's inherent `fn drop(&mut self)` is not mistaken for glue;
    ///  * the body's **receiver parameter** otherwise. Analyzing a pre-emitted
    ///    dump has no source root, so no header can be read and nothing says
    ///    this is a `Drop` impl — but `_1: &mut clean::Guard<'_>` still says
    ///    which type's drop would run it. Such a body is recorded as
    ///    *unresolved*: a drop of that type becomes a named boundary rather
    ///    than the silent "this type has no glue" that a missing header used to
    ///    mean.
    fn index_drop_glue(&mut self) {
        let mut glue: BTreeMap<String, Vec<DropCandidate>> = BTreeMap::new();
        let mut untyped: Vec<String> = Vec::new();
        for path in &self.order {
            let Some((prefix, method)) = split_last(path) else {
                continue;
            };
            // Glue is always an impl method; a free `fn drop` is not glue.
            if method != "drop" || impl_span(prefix).is_none() {
                continue;
            }
            let header = self.impl_headers.get(path);
            if let Some(header) = header
                && header
                    .trait_
                    .as_deref()
                    .map(|t| TypeName::parse(t).name)
                    .as_deref()
                    != Some("Drop")
            {
                // A readable header that is not a `Drop` impl: an inherent
                // `fn drop`, or some other trait's method of that name.
                continue;
            }
            let unresolved = header.is_none().then(|| {
                impl_span(prefix)
                    .and_then(|(file, _, _)| self.sources.unreadable.get(&file).cloned())
                    .unwrap_or_else(|| {
                        "the `impl` header it is declared in could not be read".to_string()
                    })
            });
            let named = header
                .map(|h| TypeName::parse(&h.self_ty).name)
                .filter(|name| !name.is_empty())
                .or_else(|| self.drop_receiver_type(path));
            let candidate = DropCandidate {
                module: module_of_impl(prefix, self.crate_of.get(path).map(String::as_str)),
                body: path.clone(),
                unresolved,
            };
            match named {
                Some(name) => glue.entry(name).or_default().push(candidate),
                None => untyped.push(candidate.body),
            }
        }
        self.drop_glue = glue;
        self.drop_glue_untyped = untyped;
    }

    /// The type name a glue body's `&mut self` parameter declares.
    fn drop_receiver_type(&self, path: &str) -> Option<String> {
        let (_, ty) = self.body(path)?.params.first()?;
        let name = TypeName::parse(peel_containers(peel_refs(ty))).name;
        (!name.is_empty()).then_some(name)
    }

    /// RTA-lite: every concrete type unsized into a `dyn Trait` anywhere in the set.
    fn index_rta(&mut self) {
        let mut map: BTreeMap<String, BTreeSet<(String, String)>> = BTreeMap::new();
        for doc in &self.docs {
            for body in &doc.bodies {
                let id = self
                    .qualified
                    .get(&format!("{}::{}", doc.crate_name, body.path))
                    .cloned()
                    .unwrap_or_else(|| body.path.clone());
                for block in &body.blocks {
                    for statement in &block.statements {
                        let Statement::Assign { rvalue, .. } = statement else {
                            continue;
                        };
                        let Some(target) = &rvalue.unsize_to else {
                            continue;
                        };
                        let Some(trait_name) = dyn_trait_of(target) else {
                            continue;
                        };
                        let Some(operand) = rvalue.reads.first() else {
                            continue;
                        };
                        let Some(place) = operand_place(operand) else {
                            continue;
                        };
                        let Some(ty) = body.locals.get(&place.local) else {
                            continue;
                        };
                        let concrete = TypeName::parse(peel_containers(ty)).name;
                        if !concrete.is_empty() && concrete != trait_name {
                            map.entry(trait_name)
                                .or_default()
                                .insert((concrete, id.clone()));
                        }
                    }
                }
            }
        }
        self.unsized_to = map;
    }

    // ── queries ─────────────────────────────────────────────────────────────

    /// One body by MIR path (the first, when rustc printed duplicates).
    #[must_use]
    pub fn body(&self, path: &str) -> Option<&Body> {
        let real = self.real_path(path)?;
        let &(doc_at, body_at) = self.bodies.get(&real)?;
        self.docs.get(doc_at)?.bodies.get(body_at)
    }

    /// The doc a body came from (for its `allocN (static: NAME)` footer).
    #[must_use]
    pub fn doc_of(&self, path: &str) -> Option<&MirDoc> {
        let real = self.real_path(path)?;
        let &(doc_at, _) = self.bodies.get(&real)?;
        self.docs.get(doc_at)
    }

    /// Every body path in the doc set, in doc then file order.
    #[must_use]
    pub fn body_paths(&self) -> Vec<&str> {
        self.order.iter().map(String::as_str).collect()
    }

    /// The declared type of a local in `body`.
    #[must_use]
    pub fn local_ty<'a>(&self, body: &'a Body, local: Local) -> Option<&'a str> {
        body.locals.get(&local).map(String::as_str)
    }

    /// Every `static` (or `thread_local!` key) an `allocN` footer entry could name.
    ///
    /// The alloc footer sits in the same dump as the body that reads it, so the
    /// doc's own map is authoritative; the merged map is a fallback for a body
    /// whose doc is not in hand, and it keeps every doc's answer because alloc
    /// ids are numbered per dump and collide freely across them.
    #[must_use]
    pub fn statics_of_alloc(&self, doc: Option<&MirDoc>, alloc: &str) -> Vec<&StaticItem> {
        if let Some(name) = doc.and_then(|d| d.alloc_statics.get(alloc)) {
            return self.statics_named_all(name);
        }
        let Some(names) = self.alloc_statics.get(alloc) else {
            return Vec::new();
        };
        let mut out: Vec<&StaticItem> = Vec::new();
        for name in names {
            for item in self.statics_named_all(name) {
                if !out.iter().any(|have| have.path == item.path) {
                    out.push(item);
                }
            }
        }
        out
    }

    /// The single static an `allocN` footer names, when exactly one answers.
    #[must_use]
    pub fn static_of_alloc(&self, doc: Option<&MirDoc>, alloc: &str) -> Option<&StaticItem> {
        single(self.statics_of_alloc(doc, alloc))
    }

    /// Every `static`/`const` item a printed name could denote.
    ///
    /// The printed name is normally a full path (`b::COUNTER`) and matches one
    /// item exactly. It can also be *more* qualified than the item's own dump
    /// printed it (`mycrate::b::COUNTER`) or — for a name recovered from a
    /// `const NAME` rvalue in a dump that trimmed harder — *less*. Both are
    /// resolved by segment-suffix in the appropriate direction, and only a bare
    /// last segment that two modules both define comes back with two answers.
    #[must_use]
    pub fn statics_named_all(&self, name: &str) -> Vec<&StaticItem> {
        let name = name.trim();
        if let Some(item) = self.statics.get(name) {
            return vec![item];
        }
        let Some(paths) = self.statics_by_last.get(last_segment(name)) else {
            return Vec::new();
        };
        let want = segments(name);
        let mut matched: Vec<&StaticItem> = Vec::new();
        for path in paths {
            let have = segments(path);
            if (is_segment_suffix(name, &have) || is_segment_suffix(path, &want))
                && let Some(item) = self.statics.get(path)
            {
                matched.push(item);
            }
        }
        matched
    }

    /// A `static`/`const` item by its printed name, when exactly one answers.
    #[must_use]
    pub fn static_named(&self, name: &str) -> Option<&StaticItem> {
        single(self.statics_named_all(name))
    }

    /// True when the analyzed set defines `(self type, trait, method)`.
    ///
    /// A user trait implemented on a std type (`impl MyTrait for Vec<u32>`) is
    /// user code, so a body-less `<Vec<u32> as MyTrait>::m` must not inherit
    /// `Vec`'s std-ness. In practice [`Self::resolve_call`] finds that impl's
    /// body first and the question never arises; this is the belt on top.
    #[must_use]
    pub fn has_impl_method(&self, self_ty: &str, trait_: Option<&str>, method: &str) -> bool {
        self.impl_methods.contains_key(&(
            self_ty.to_string(),
            trait_.map(str::to_string),
            method.to_string(),
        ))
    }

    /// Every body printed with the `{closure@..}` / `{async block@..}` span,
    /// in the order the dumps defined them.
    #[must_use]
    pub fn closure_bodies(&self, span: &str) -> &[String] {
        self.closures.get(span).map_or(&[], Vec::as_slice)
    }

    /// [`Self::closure_bodies`], narrowed to the ones a call site inside
    /// `caller_body` can mean.
    ///
    /// Several bodies share a span whenever the closure was written inside a
    /// macro, and the printed span is then the *only* thing the call site says
    /// about which one it invokes — so the disambiguation has to come from
    /// where the call site sits, in this order:
    ///
    ///  1. **Inside the caller.** `outer::{closure#0}` is a closure of `outer`,
    ///     so a call in `outer` (or in one of `outer`'s own closures) that names
    ///     the span means one of those, never a sibling expansion elsewhere.
    ///  2. **The same enclosing function.** Two closures of one function are
    ///     each other's siblings: `f::{closure#0}::{closure#1}` called from
    ///     `f::{closure#0}::{closure#0}` is still `f`'s.
    ///  3. **The same crate**, exactly as [`Self::real_path_near`] prefers a
    ///     free function from the crate the call site is in.
    ///
    /// The closure's declared **type text** is deliberately not a fourth step:
    /// MIR prints it as the span and nothing else, so every candidate carries
    /// the identical text by construction — it is the map key. A future rustc
    /// that printed more would disambiguate them at step 0, because they would
    /// no longer share a key at all.
    ///
    /// What survives all three is returned whole: the union is the sound
    /// answer, and the caller analyzes every candidate rather than guessing.
    #[must_use]
    pub fn closure_bodies_near(&self, caller_body: &str, span: &str) -> Vec<String> {
        let candidates = self.closure_bodies(span);
        if candidates.len() < 2 {
            return candidates.to_vec();
        }
        let inside: Vec<String> = candidates
            .iter()
            .filter(|body| body.starts_with(&format!("{caller_body}::")))
            .cloned()
            .collect();
        if !inside.is_empty() {
            return inside;
        }
        let owner = closure_owner(caller_body);
        let siblings: Vec<String> = candidates
            .iter()
            .filter(|body| closure_owner(body) == owner)
            .cloned()
            .collect();
        if !siblings.is_empty() {
            return siblings;
        }
        if let Some(krate) = self.crate_of.get(caller_body) {
            let same: Vec<String> = candidates
                .iter()
                .filter(|body| self.crate_of.get(*body) == Some(krate))
                .cloned()
                .collect();
            if !same.is_empty() {
                return same;
            }
        }
        candidates.to_vec()
    }

    /// Generic parameter names in scope inside `body`, in declaration order when known.
    #[must_use]
    pub fn generic_params(&self, body_path: &str) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        if let Some(header) = self.impl_headers.get(body_path) {
            names.extend(header.generics.iter().cloned());
        }
        let name = split_last(body_path).map_or(body_path, |(_, last)| last);
        if let Some(extra) = self.sources.fn_generics.get(name) {
            for param in extra {
                if !names.contains(param) {
                    names.push(param.clone());
                }
            }
        }
        names
    }

    /// True when `name`, used as a type inside `body_path`, is a type parameter.
    #[must_use]
    pub fn is_generic_param(&self, body_path: &str, name: &str) -> bool {
        self.generic_params(body_path).iter().any(|p| p == name) || looks_like_type_param(name)
    }

    /// Resolve a printed callee path as seen from `caller_body`.
    #[must_use]
    pub fn resolve_call(&self, caller_body: &str, callee: &str) -> Resolution {
        let path = CalleePath::parse(callee);
        let near = self.crate_of.get(caller_body).map(String::as_str);
        // 1. An async coroutine reached through its shim, `poll` or `into_future`.
        if let Some(inner) = async_body_of(callee)
            && let Some(body) = near
                .and_then(|krate| self.async_bodies.get(&format!("{krate}::{inner}")))
                .or_else(|| self.async_bodies.get(&inner))
        {
            return Resolution::Body(body.clone());
        }
        // 2. A closure named by its span, either as the receiver or in the turbofish.
        if let Some(span) = path.closure_span.as_deref()
            && let Some(resolution) = bodies_resolution(
                self.closure_bodies_near(caller_body, span),
                Ambiguity::Closure,
            )
        {
            return resolution;
        }
        // 3. A plain path (turbofish stripped) naming a body directly.
        let bare = strip_generics_everywhere(callee);
        if let Some(resolved) = self.body_or_coroutine(near, &bare) {
            return Resolution::Body(resolved);
        }
        // 4. An impl method, possibly behind `dyn`.
        if let Some(receiver) = path.receiver.as_deref() {
            let method = path.last_segment();
            if path.is_dyn {
                let trait_name = path.trait_.as_deref().unwrap_or(receiver);
                let candidates = self.unsized_to.get(trait_name);
                let unique = candidates.filter(|set| set.len() == 1).and_then(|set| {
                    set.iter().next().map(|(concrete, _)| {
                        self.impl_method_candidates(
                            concrete,
                            path.trait_.as_deref(),
                            method,
                            None,
                            near,
                        )
                    })
                });
                return unique
                    .and_then(|bodies| bodies_resolution(bodies, Ambiguity::Impl))
                    .unwrap_or_else(|| {
                        Resolution::Boundary(BoundaryKind::DynDispatch, callee.trim().into())
                    });
            }
            if self.is_generic_param(caller_body, receiver) && !self.is_known_type(receiver) {
                return Resolution::Boundary(BoundaryKind::UnresolvedGeneric, receiver.to_string());
            }
            let bodies = self.impl_method_candidates(
                receiver,
                path.trait_.as_deref(),
                method,
                path.receiver_path.as_deref(),
                near,
            );
            if let Some(resolution) = bodies_resolution(bodies, Ambiguity::Impl) {
                return resolution;
            }
            if let Some(detail) = self.unresolved_impl_methods.get(method) {
                return Resolution::Boundary(BoundaryKind::MissingBody, detail.clone());
            }
        }
        // 5. A closure named only in the turbofish of a body-less callee
        // (`LocalKey::<T>::with::<{closure@..}, R>`): the call goes into std,
        // but the only analyzable code it runs is that closure.
        for argument in subst::turbofish(callee) {
            if let Some(resolution) = bodies_resolution(
                self.closure_bodies_near(caller_body, argument.trim()),
                Ambiguity::Closure,
            ) {
                return resolution;
            }
        }
        // 6. An item whose body the parser could not read.
        if let Some(reason) = self.parse_failed.get(&bare) {
            return Resolution::Boundary(BoundaryKind::MirParse, format!("{bare}: {reason}"));
        }
        // 7. A foreign function: declared, never given a body.
        let last = path.last_segment();
        if !last.is_empty() && self.sources.foreign_fns.contains(last) {
            return Resolution::Boundary(BoundaryKind::Ffi, callee.trim().to_string());
        }
        // 8. An explicitly rooted path we have no body for.
        if let Some(root) = crate_root(&bare).filter(|root| !is_std_module(root)) {
            if self.crates.contains(root) {
                return Resolution::Boundary(BoundaryKind::MissingBody, callee.trim().to_string());
            }
            if !is_trusted_crate(root) {
                return Resolution::Boundary(
                    BoundaryKind::ExternalCrateBody,
                    callee.trim().to_string(),
                );
            }
        }
        Resolution::External(callee.trim().to_string())
    }

    /// Resolve the `Call` terminator of `block` in `caller_body`, including the
    /// indirect form (`_8 = copy _5()`), which has no callee path.
    #[must_use]
    pub fn resolve_terminator(&self, caller_body: &str, block: &str) -> Option<Resolution> {
        let body = self.body(caller_body)?;
        let target = body.blocks.iter().find(|b| b.label == block)?;
        let Terminator::Call {
            callee, indirect, ..
        } = &target.terminator
        else {
            return None;
        };
        Some(callee.as_ref().map_or_else(
            || {
                let detail = indirect
                    .as_ref()
                    .map_or_else(|| format!("{caller_body}:{block}"), operand_text);
                Resolution::Boundary(BoundaryKind::IndirectCall, detail)
            },
            |path| self.resolve_call(caller_body, path),
        ))
    }

    /// The substitution a call from `caller_body` to `callee` induces on the
    /// callee's body (header/argument-type unification + turbofish, D6).
    #[must_use]
    pub fn call_substitution(&self, caller_body: &str, callee: &str) -> Substitution {
        self.call_substitution_in(caller_body, callee, &Substitution::new())
    }

    /// [`Self::call_substitution`] with the caller's own substitution already applied
    /// to the argument types (two-layer generics, D6).
    #[must_use]
    pub fn call_substitution_in(
        &self,
        caller_body: &str,
        callee: &str,
        caller_subst: &Substitution,
    ) -> Substitution {
        let mut out = Substitution::new();
        let Resolution::Body(target) = self.resolve_call(caller_body, callee) else {
            return out;
        };
        let Some(target_body) = self.body(&target) else {
            return out;
        };
        let params = self.generic_params(&target);
        let is_param = |name: &str| params.iter().any(|p| p == name) || looks_like_type_param(name);
        // (1) The turbofish, when it lines up one-for-one with the callee's
        // declared parameters, is the most faithful spelling of the binding:
        // it is what rustc printed at the call site, so the substituted callee
        // path reads exactly as the monomorphised one would.
        let arguments = subst::turbofish(callee);
        let mut order = params.clone();
        if order.is_empty() {
            order = Self::inferred_params(target_body);
        }
        if !arguments.is_empty() && order.len() == arguments.len() {
            for (param, argument) in order.iter().zip(&arguments) {
                out.bind(param, &caller_subst.apply(argument));
            }
        }
        // (2) Unify the callee's declared parameter and return types against the
        // actual types at the call site.
        if let Some(caller) = self.body(caller_body)
            && let Some(site) = Self::call_site(caller, callee)
        {
            for (index, (_, declared)) in target_body.params.iter().enumerate() {
                let Some(operand) = site.args.get(index) else {
                    continue;
                };
                let Some(actual) = operand_ty(caller, operand) else {
                    continue;
                };
                subst::unify(declared, &caller_subst.apply(&actual), &is_param, &mut out);
            }
            if let Some(ty) = caller.locals.get(&site.dest.local)
                && site.dest.projections.is_empty()
            {
                subst::unify(
                    &target_body.return_ty,
                    &caller_subst.apply(ty),
                    &is_param,
                    &mut out,
                );
            }
        }
        // (3) Whatever is still unbound comes from the turbofish, by elimination.
        if !arguments.is_empty() {
            let unbound: Vec<&String> = order.iter().filter(|p| out.get(p).is_none()).collect();
            if unbound.len() == arguments.len() {
                for (param, argument) in unbound.into_iter().zip(&arguments) {
                    out.bind(param, &caller_subst.apply(argument));
                }
            }
        }
        out
    }

    /// `body`'s callee paths with `subst` applied to each.
    #[must_use]
    pub fn substituted_callees(&self, body: &str, subst: &Substitution) -> Vec<String> {
        let Some(body) = self.body(body) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for block in &body.blocks {
            if let Terminator::Call {
                callee: Some(path), ..
            } = &block.terminator
            {
                out.push(subst.apply(path));
            }
        }
        out
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /// The call terminator in `caller` whose callee is `callee`, if there is one.
    fn call_site<'a>(caller: &'a Body, wanted: &str) -> Option<CallSite<'a>> {
        for block in &caller.blocks {
            if let Terminator::Call {
                dest,
                callee: Some(path),
                args,
                ..
            } = &block.terminator
                && (path == wanted
                    || strip_generics_everywhere(path) == strip_generics_everywhere(wanted))
            {
                return Some(CallSite { dest, args });
            }
        }
        None
    }

    /// Type-parameter names guessed from a body when no source file was read.
    fn inferred_params(body: &Body) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let types = body
            .params
            .iter()
            .map(|(_, ty)| ty.as_str())
            .chain(std::iter::once(body.return_ty.as_str()));
        for ty in types {
            for token in ty.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
                if looks_like_type_param(token) && !out.iter().any(|p| p == token) {
                    out.push(token.to_string());
                }
            }
        }
        out
    }

    /// Does the callee **path itself** name a body in the analyzed set?
    ///
    /// Narrower than [`Self::resolve_call`] on purpose: `spawn::<{closure@..},
    /// u64>` *resolves* to the closure in its turbofish, but nothing in the
    /// analyzed set is called `spawn`. A model row keyed on a bare name needs
    /// the second question, not the first.
    #[must_use]
    pub fn names_analyzed_body(&self, caller_body: &str, callee: &str) -> bool {
        let near = self.crate_of.get(caller_body).map(String::as_str);
        let bare = strip_generics_everywhere(callee);
        self.body_or_coroutine(near, &bare).is_some()
    }

    /// What dropping a place of type `ty` runs.
    ///
    /// `ty` is a declared type as MIR prints it; references and the transparent
    /// containers are peeled and generic arguments stripped before the lookup,
    /// so `&mut Bomb<'_>` and `Box<Bomb>` both find `Bomb`'s glue.
    ///
    /// Three answers, and only the first two are ever silent:
    ///
    ///  * **inert** — nothing in the analyzed set implements `Drop` for this
    ///    type name, so the drop runs no user code. Every std type is here.
    ///  * **bodies** — one `Drop` impl, or several that the dropped type's own
    ///    module could not tell apart. MIR prints a local's type fully
    ///    qualified (`_4: ambient::Guard<'_>`) and prints the glue body path
    ///    with the same trimming (`ambient::<impl at ..>::drop`), so the module
    ///    normally picks exactly one; where it does not, all of them are
    ///    returned and the caller unions them. Returning *none* on that tie —
    ///    as asking for a single body did — reads as "no glue" and silently
    ///    discards whatever the real one does.
    ///  * **unresolved** — a `::drop` body whose receiver type matches but
    ///    whose impl header could not be read. Nothing says whether it is glue
    ///    for this type, so it is a `drop-glue` boundary.
    #[must_use]
    pub fn drop_targets(&self, ty: &str) -> DropTargets {
        let peeled = peel_containers(peel_refs(ty));
        let name = TypeName::parse(peeled).name;
        let mut out = DropTargets::default();
        if name.is_empty() {
            return out;
        }
        if let Some(body) = self.drop_glue_untyped.first() {
            out.unresolved = Some(format!(
                "{ty}: `{body}` may be its `Drop` glue and its self type could not be read"
            ));
        }
        let candidates = self
            .drop_glue
            .get(&name)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if candidates.is_empty() {
            return out;
        }
        let mut chosen: Vec<&DropCandidate> = candidates.iter().collect();
        let stripped = strip_generics_everywhere(peeled);
        if let Some((module, _)) = split_last(stripped.trim())
            && !module.is_empty()
        {
            let narrowed: Vec<&DropCandidate> = chosen
                .iter()
                .copied()
                .filter(|c| modules_agree(&c.module, module))
                .collect();
            if !narrowed.is_empty() {
                chosen = narrowed;
            }
        }
        for candidate in chosen {
            match &candidate.unresolved {
                Some(reason) => {
                    out.unresolved
                        .get_or_insert_with(|| format!("{ty}: {reason}"));
                }
                None => out.bodies.push(candidate.body.clone()),
            }
        }
        out
    }

    /// Every impl body `(self type, trait, method)` names, module-disambiguated
    /// by `receiver_path` (the receiver as the call site spelled it) when that
    /// selects exactly one.
    fn impl_method_candidates(
        &self,
        self_ty: &str,
        trait_: Option<&str>,
        method: &str,
        receiver_path: Option<&str>,
        near: Option<&str>,
    ) -> Vec<String> {
        let key = (
            self_ty.to_string(),
            trait_.map(str::to_string),
            method.to_string(),
        );
        let found = self
            .impl_methods
            .get(&key)
            .or_else(|| {
                self.impl_methods
                    .get(&(self_ty.to_string(), None, method.to_string()))
            })
            .map(Vec::as_slice)
            .unwrap_or_default();
        if found.len() < 2 {
            return found.iter().map(|c| c.body.clone()).collect();
        }
        // Analyzing several targets at once (`--all-examples`) puts one
        // `impl Serialize for Receipt` per example in the same program. A call
        // inside one example means *its own* crate's impl, exactly as
        // `real_path_near` decides for a free function.
        let mut found: Vec<&ImplCandidate> = found.iter().collect();
        if let Some(krate) = near {
            let same: Vec<&ImplCandidate> = found
                .iter()
                .copied()
                .filter(|c| self.crate_of.get(&c.body).map(String::as_str) == Some(krate))
                .collect();
            if !same.is_empty() {
                found = same;
            }
        }
        if let [only] = found.as_slice() {
            return vec![only.body.clone()];
        }
        // `b::Worker::run` at the call site and `b::<impl at ..>::run` in the
        // body path carry the same trimmed module, so the qualifier the caller
        // wrote is enough — and it is the only evidence in the printed text.
        let wanted = receiver_path.and_then(split_last).map(|(module, _)| module);
        if let Some(module) = wanted.filter(|m| !m.is_empty()) {
            let narrowed: Vec<&ImplCandidate> = found
                .iter()
                .copied()
                .filter(|c| modules_agree(&c.module, module))
                .collect();
            if let [only] = narrowed.as_slice() {
                return vec![only.body.clone()];
            }
        }
        found.iter().map(|c| c.body.clone()).collect()
    }

    /// Narrow an ambiguous [`Resolution::Bodies`] with a receiver's declared
    /// type, which MIR prints fully qualified (`_1: &a::Worker`).
    ///
    /// Returns the bodies unchanged when the type picks none or several: the
    /// union is the sound answer, never a guess.
    #[must_use]
    pub fn narrow_by_receiver(&self, bodies: &[String], declared: &str) -> Vec<String> {
        let stripped = strip_generics_everywhere(peel_containers(declared));
        let Some((module, _)) = split_last(stripped.trim()) else {
            return bodies.to_vec();
        };
        if module.is_empty() {
            return bodies.to_vec();
        }
        let narrowed: Vec<String> = bodies
            .iter()
            .filter(|body| {
                split_last(body)
                    .map(|(prefix, _)| {
                        module_of_impl(prefix, self.crate_of.get(*body).map(String::as_str))
                    })
                    .is_some_and(|have| modules_agree(&have, module))
            })
            .cloned()
            .collect();
        if narrowed.len() == 1 {
            narrowed
        } else {
            bodies.to_vec()
        }
    }

    /// A body by exact path, redirected to its coroutine body when it is an async shim.
    fn body_or_coroutine(&self, near: Option<&str>, path: &str) -> Option<String> {
        let real = self.real_path_near(near, path)?;
        Some(self.async_bodies.get(&real).cloned().unwrap_or(real))
    }

    /// The id a body is indexed under, from any of the spellings a call site
    /// can use: the id itself, the crate-qualified path, or the trimmed path.
    fn real_path(&self, path: &str) -> Option<String> {
        self.real_path_near(None, path)
    }

    /// [`Self::real_path`], preferring a body from `near` when the trimmed path
    /// is ambiguous.
    ///
    /// Analyzing several targets at once (`--all-examples`) routinely puts two
    /// unrelated `charge_card` bodies in the same program. A call inside one
    /// example must resolve to *its own* crate's body, or the analysis reports
    /// a finding from a completely different file.
    fn real_path_near(&self, near: Option<&str>, path: &str) -> Option<String> {
        if self.bodies.contains_key(path) {
            return Some(path.to_string());
        }
        if let Some(krate) = near
            && let Some(id) = self.qualified.get(&format!("{krate}::{path}"))
        {
            return Some(id.clone());
        }
        if let Some(id) = self.qualified.get(path) {
            return Some(id.clone());
        }
        self.ambiguous
            .get(path)
            .and_then(|ids| ids.first())
            .cloned()
    }

    /// The body id for `path` as seen from `crate_name` — how an entry point and
    /// the pipeline name a body that several analyzed targets also define.
    #[must_use]
    pub fn body_id_in(&self, crate_name: &str, path: &str) -> String {
        self.real_path_near(Some(crate_name), path)
            .unwrap_or_else(|| path.to_string())
    }

    /// `crate::path` for a body id — the spelling every report uses.
    #[must_use]
    pub fn qualified_name(&self, id: &str) -> String {
        match self.crate_of.get(id) {
            Some(krate) if !id.starts_with(&format!("{krate}::")) => format!("{krate}::{id}"),
            _ => id.to_string(),
        }
    }

    /// Concrete types unsized into `dyn trait_name`, with the body that did it.
    #[must_use]
    pub fn dyn_candidates(&self, trait_name: &str) -> Vec<(String, String)> {
        self.unsized_to
            .get(trait_name)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// True when `name` is a concrete type of the analyzed set (so it is not a
    /// type parameter even if it is spelled like one).
    fn is_known_type(&self, name: &str) -> bool {
        self.impl_methods
            .keys()
            .any(|(self_ty, _, _)| self_ty == name)
    }
}

/// Index one `static`/`const` item under its full path and its last segment.
fn index_static(
    statics: &mut BTreeMap<String, StaticItem>,
    by_last: &mut BTreeMap<String, Vec<String>>,
    item: StaticItem,
) {
    let by = by_last
        .entry(last_segment(&item.path).to_string())
        .or_default();
    if !by.iter().any(|have| have == &item.path) {
        by.push(item.path.clone());
    }
    statics.entry(item.path.clone()).or_insert(item);
}

/// Append a candidate unless the same body is already recorded.
fn push_candidate(into: &mut Vec<ImplCandidate>, candidate: ImplCandidate) {
    if !into.iter().any(|have| have.body == candidate.body) {
        into.push(candidate);
    }
}

/// The module an impl body path sits in: everything before `<impl at`, with the
/// body id's crate prefix (added only when two dumps print the same path)
/// removed, because a call site never spells it.
fn module_of_impl(prefix: &str, crate_name: Option<&str>) -> String {
    let head = prefix
        .find("<impl at")
        .map_or(prefix, |at| prefix.get(..at).unwrap_or(prefix));
    let head = head.trim().trim_end_matches("::").trim();
    let head = crate_name
        .and_then(|krate| head.strip_prefix(&format!("{krate}::")))
        .unwrap_or(head);
    if head == crate_name.unwrap_or_default() {
        return String::new();
    }
    head.to_string()
}

/// Do two printed module paths denote the same module?
///
/// One side can be trimmed harder than the other (`deep::inner` vs `inner`),
/// so a `::`-segment suffix in either direction counts — but never the empty
/// path, which would match everything.
fn modules_agree(have: &str, want: &str) -> bool {
    if have.is_empty() || want.is_empty() {
        return have == want;
    }
    have == want
        || is_segment_suffix(want, &segments(have))
        || is_segment_suffix(have, &segments(want))
}

/// The only element of a candidate list, or `None` when there is not exactly one.
fn single<T>(mut items: Vec<T>) -> Option<T> {
    if items.len() == 1 { items.pop() } else { None }
}

/// One body resolves to [`Resolution::Body`], several to
/// [`Resolution::Bodies`], none to `None`.
fn bodies_resolution(mut bodies: Vec<String>, kind: Ambiguity) -> Option<Resolution> {
    bodies.sort();
    bodies.dedup();
    match bodies.len() {
        0 => None,
        1 => bodies.pop().map(Resolution::Body),
        _ => Some(Resolution::Bodies(bodies, kind)),
    }
}

/// The function a closure body belongs to: its path with every trailing
/// `::{closure#N}` (and the `::{closure#N}::{closure#M}` chains nesting builds)
/// removed. `f::{closure#0}::{closure#1}` → `f`; a non-closure path is its own
/// owner.
fn closure_owner(path: &str) -> &str {
    let mut head = path;
    while let Some((prefix, last)) = split_last(head) {
        if !last.starts_with("{closure#") && !last.starts_with("{coroutine#") {
            break;
        }
        head = prefix;
    }
    head
}

struct CallSite<'a> {
    dest: &'a crate::mir::ast::Place,
    args: &'a [Operand],
}

/// A crate whose body the analyzer never has, but whose behaviour is modelled as
/// pure taint propagation. Kept in step with the `[[trusted]]` table's intent;
/// `Model::classify` is the authority at analysis time, this list only decides
/// whether a *rooted* path with no body is a boundary.
fn is_trusted_crate(root: &str) -> bool {
    crate::Model::builtin_ref()
        .map(|model| model.trusted.iter().any(|c| c.name == root))
        .unwrap_or(matches!(root, "std" | "core" | "alloc"))
}

/// `{async fn body of m::f()}` → `m::f`.
fn async_body_of(ty: &str) -> Option<String> {
    let at = ty.find("{async fn body of ")?;
    let rest = ty.get(at.saturating_add("{async fn body of ".len())..)?;
    let end = rest.find("()}")?;
    Some(rest.get(..end)?.trim().to_string())
}

/// `{closure@FILE:l:c: l:c}` → (FILE, l, c); also matches `{async block@..}`.
fn brace_span(text: &str) -> Option<(String, usize, usize)> {
    let at = text.find('@')?;
    let rest = text.get(at.saturating_add(1)..)?;
    let end = rest.find([',', '}']).unwrap_or(rest.len());
    parse_file_line_col(rest.get(..end)?)
}

/// `<impl at FILE:l:c: l:c>` → (FILE, l, c).
/// `fn helper((((_1: u64) -> u64 {` → `helper`.
fn failed_item_path(item: &str) -> Option<String> {
    let head = item.trim().strip_prefix("fn ")?.trim_start();
    let end = head.find('(').unwrap_or(head.len());
    let path = head.get(..end).unwrap_or(head).trim();
    let path = strip_generics_everywhere(path);
    (!path.is_empty()).then_some(path)
}

fn impl_span(path: &str) -> Option<(String, usize, usize)> {
    let at = path.find("<impl at ")?;
    let rest = path.get(at.saturating_add("<impl at ".len())..)?;
    let end = rest.find('>')?;
    parse_file_line_col(rest.get(..end)?)
}

/// `dir/file.rs:12:1: 12:9` → (`dir/file.rs`, 12, 1).
fn parse_file_line_col(text: &str) -> Option<(String, usize, usize)> {
    // The form is FILE:l1:c1: l2:c2 — take the first two numbers after the path.
    let mut parts = text.rsplitn(5, ':');
    let _c2 = parts.next()?;
    let _l2 = parts.next()?;
    let c1 = parts.next()?.trim().parse::<usize>().ok()?;
    let l1 = parts.next()?.trim().parse::<usize>().ok()?;
    let file = parts.next()?.trim().to_string();
    Some((file, l1, c1))
}

/// The closure/coroutine span a body's first parameter carries, if any.
fn closure_param_span(body: &Body) -> Option<String> {
    let (_, ty) = body.params.first()?;
    let ty = peel_refs(ty).trim();
    let ty = ty.trim_start_matches("mut ").trim();
    (ty.starts_with('{') && ty.contains('@') && ty.ends_with('}')).then(|| ty.to_string())
}

/// `Box<dyn Jitter>` / `&dyn Jitter + Send` → `Jitter`.
fn dyn_trait_of(ty: &str) -> Option<String> {
    let inner = peel_containers(ty);
    let rest = peel_refs(inner).strip_prefix("dyn ")?;
    let name = TypeName::parse(split_top_trim(rest, "+").first().copied().unwrap_or(rest)).name;
    (!name.is_empty()).then_some(name)
}

/// The declared type of an operand, as the caller's `let` declarations print it.
fn operand_ty(body: &Body, operand: &Operand) -> Option<String> {
    match operand {
        // A projected argument's own type is not printed; the root local's is
        // the best available approximation, and unification simply fails when
        // it does not match.
        Operand::Copy(place) | Operand::Move(place) => body.locals.get(&place.local).cloned(),
        Operand::Const { text, closure, .. } => closure
            .clone()
            .or_else(|| Some(text.trim_start_matches("const ").trim().to_string())),
    }
}

const fn operand_place(operand: &Operand) -> Option<&crate::mir::ast::Place> {
    match operand {
        Operand::Copy(place) | Operand::Move(place) => Some(place),
        Operand::Const { .. } => None,
    }
}

fn operand_text(operand: &Operand) -> String {
    match operand {
        Operand::Copy(place) => format!("copy _{}", place.local.0),
        Operand::Move(place) => format!("move _{}", place.local.0),
        Operand::Const { text, .. } => text.clone(),
    }
}

/// Top-level `std`/`core`/`alloc` modules, which rustc's trimmed paths print
/// *as if* they were crate roots (`slice::<impl [T]>::into_vec`,
/// `str::parse`). They are std, and std is trusted: treating them as unknown
/// third-party crates would turn ordinary `Vec`/slice code into a boundary.
fn is_std_module(root: &str) -> bool {
    const MODULES: [&str; 43] = [
        "alloc",
        "any",
        "array",
        "ascii",
        "borrow",
        "boxed",
        "cell",
        "char",
        "clone",
        "cmp",
        "collections",
        "convert",
        "default",
        "env",
        "error",
        "ffi",
        "fmt",
        "fs",
        "future",
        "hash",
        "hint",
        "io",
        "iter",
        "marker",
        "mem",
        "net",
        "num",
        "ops",
        "option",
        "panic",
        "path",
        "pin",
        "primitive",
        "process",
        "ptr",
        "rc",
        "result",
        "slice",
        "str",
        "string",
        "sync",
        "task",
        "thread",
    ];
    MODULES.contains(&root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans_are_split_off_their_file_paths() {
        assert_eq!(
            impl_span("<impl at corpus/helpers/src/lib.rs:240:1: 240:30>::steps"),
            Some(("corpus/helpers/src/lib.rs".to_string(), 240, 1))
        );
        assert_eq!(
            brace_span("{closure@src/lib.rs:16:21: 16:24}"),
            Some(("src/lib.rs".to_string(), 16, 21))
        );
    }

    #[test]
    fn async_body_types_name_their_function() {
        assert_eq!(
            async_body_of("{async fn body of m::f()}"),
            Some("m::f".to_string())
        );
        assert_eq!(async_body_of("u64"), None);
    }

    #[test]
    fn dyn_targets_are_peeled_out_of_their_containers() {
        assert_eq!(
            dyn_trait_of("std::boxed::Box<dyn Jitter>").as_deref(),
            Some("Jitter")
        );
        assert_eq!(dyn_trait_of("&dyn Fetcher").as_deref(), Some("Fetcher"));
        assert_eq!(dyn_trait_of("Vec<u8>"), None);
    }
}
