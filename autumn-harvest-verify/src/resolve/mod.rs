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
use crate::verdict::BoundaryKind;

pub use impls::ImplHeader;
pub use subst::{Substitution, split_top};

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
    /// An honest analysis boundary (D7/D9); the `String` is the `Boundary::detail`.
    Boundary(BoundaryKind, String),
    /// A body outside the analyzed MIR (std/core/alloc or a `[trusted]` crate),
    /// by its callee path. Taint propagates through it; it never shadows a source.
    External(String),
}

/// Key of an impl method: `(self type name, trait name, method)`.
type ImplKey = (String, Option<String>, String);

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
    /// `(self type, trait, method)` → body path.
    impl_methods: BTreeMap<ImplKey, String>,
    /// Impl body path → its header.
    impl_headers: BTreeMap<String, ImplHeader>,
    /// `{closure@FILE:l:c: l:c}` / `{async block@..}` → body path.
    closures: BTreeMap<String, String>,
    /// `f` (and the path printed inside `{async fn body of f()}`) → `f::{closure#0}`.
    async_bodies: BTreeMap<String, String>,
    /// Trait name → `(concrete type, the body that built the trait object)`.
    unsized_to: BTreeMap<String, BTreeSet<(String, String)>>,
    /// Static / thread-local name (last segment) → item.
    statics: BTreeMap<String, StaticItem>,
    /// `allocN` → static name, merged across docs.
    alloc_statics: BTreeMap<String, String>,
    /// Body id → the crate whose dump defined it.
    crate_of: BTreeMap<String, String>,
    /// Crate names present in the analyzed set.
    crates: BTreeSet<String>,
    sources: impls::SourceIndex,
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
        let files = program.referenced_source_files();
        program.sources = impls::SourceIndex::build(&sources.roots, &files);
        program.index_impls();
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
                    .or_insert_with(|| value.clone());
            }
            for item in &doc.statics {
                let name = last_segment(&item.path).to_string();
                self.statics.entry(name).or_insert_with(|| item.clone());
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
                    self.statics
                        .entry(last_segment(&body.path).to_string())
                        .or_insert_with(|| StaticItem {
                            path: body.path.clone(),
                            ty: body.return_ty.clone(),
                            is_mut: false,
                        });
                }
                if let Some(span) = closure_param_span(body) {
                    let id = self
                        .qualified
                        .get(&format!("{}::{}", doc.crate_name, body.path))
                        .cloned()
                        .unwrap_or_else(|| body.path.clone());
                    self.closures.entry(span).or_insert(id);
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

    fn index_impls(&mut self) {
        let mut methods: BTreeMap<ImplKey, String> = BTreeMap::new();
        let mut headers: BTreeMap<String, ImplHeader> = BTreeMap::new();
        for path in &self.order {
            let Some((prefix, method)) = split_last(path) else {
                continue;
            };
            let Some((file, line, column)) = impl_span(prefix) else {
                continue;
            };
            let Some(header) = self.sources.impl_header_at(&file, line, column) else {
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
            methods
                .entry((self_name.clone(), trait_name, method.to_string()))
                .or_insert_with(|| path.clone());
            // The inherent spelling `Ty::m` must resolve too, even for a trait impl.
            methods
                .entry((self_name, None, method.to_string()))
                .or_insert_with(|| path.clone());
            headers.insert(path.clone(), header);
        }
        self.impl_methods = methods;
        self.impl_headers = headers;
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

    /// The static (or `thread_local!` key) an `allocN` footer entry names.
    #[must_use]
    pub fn static_of_alloc(&self, doc: Option<&MirDoc>, alloc: &str) -> Option<&StaticItem> {
        let name = doc
            .and_then(|d| d.alloc_statics.get(alloc))
            .or_else(|| self.alloc_statics.get(alloc))?;
        self.statics.get(last_segment(name))
    }

    /// A `static`/`const` item by its printed name.
    #[must_use]
    pub fn static_named(&self, name: &str) -> Option<&StaticItem> {
        self.statics.get(last_segment(name))
    }

    /// The body a `{closure@..}` / `{async block@..}` span belongs to.
    #[must_use]
    pub fn closure_body(&self, span: &str) -> Option<&str> {
        self.closures.get(span).map(String::as_str)
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
        if let Some(inner) = async_receiver(callee)
            && let Some(body) = near
                .and_then(|krate| self.async_bodies.get(&format!("{krate}::{inner}")))
                .or_else(|| self.async_bodies.get(&inner))
        {
            return Resolution::Body(body.clone());
        }
        // 2. A closure named by its span, either as the receiver or in the turbofish.
        if let Some(span) = path.closure_span.as_deref()
            && let Some(body) = self.closures.get(span)
        {
            return Resolution::Body(body.clone());
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
                    set.iter().next().and_then(|(concrete, _)| {
                        self.impl_method(concrete, path.trait_.as_deref(), method)
                    })
                });
                return unique.map_or_else(
                    || Resolution::Boundary(BoundaryKind::DynDispatch, callee.trim().into()),
                    Resolution::Body,
                );
            }
            if self.is_generic_param(caller_body, receiver) && !self.is_known_type(receiver) {
                return Resolution::Boundary(BoundaryKind::UnresolvedGeneric, receiver.to_string());
            }
            if let Some(body) = self.impl_method(receiver, path.trait_.as_deref(), method) {
                return Resolution::Body(body);
            }
        }
        // 5. A closure named only in the turbofish of a body-less callee
        // (`LocalKey::<T>::with::<{closure@..}, R>`): the call goes into std,
        // but the only analyzable code it runs is that closure.
        for argument in subst::turbofish(callee) {
            if let Some(body) = self.closures.get(argument.trim()) {
                return Resolution::Body(body.clone());
            }
        }
        // 6. A foreign function: declared, never given a body.
        let last = path.last_segment();
        if !last.is_empty() && self.sources.foreign_fns.contains(last) {
            return Resolution::Boundary(BoundaryKind::Ffi, callee.trim().to_string());
        }
        // 7. An explicitly rooted path we have no body for.
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

    fn impl_method(&self, self_ty: &str, trait_: Option<&str>, method: &str) -> Option<String> {
        let key = (
            self_ty.to_string(),
            trait_.map(str::to_string),
            method.to_string(),
        );
        if let Some(path) = self.impl_methods.get(&key) {
            return Some(path.clone());
        }
        self.impl_methods
            .get(&(self_ty.to_string(), None, method.to_string()))
            .cloned()
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

/// The `{async fn body of X()}` a callee path mentions (its receiver or its own type).
fn async_receiver(callee: &str) -> Option<String> {
    async_body_of(callee)
}

/// `{closure@FILE:l:c: l:c}` → (FILE, l, c); also matches `{async block@..}`.
fn brace_span(text: &str) -> Option<(String, usize, usize)> {
    let at = text.find('@')?;
    let rest = text.get(at.saturating_add(1)..)?;
    let end = rest.find([',', '}']).unwrap_or(rest.len());
    parse_file_line_col(rest.get(..end)?)
}

/// `<impl at FILE:l:c: l:c>` → (FILE, l, c).
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
    let ty = crate::model::callee::peel_refs(ty).trim();
    let ty = ty.trim_start_matches("mut ").trim();
    (ty.starts_with('{') && ty.contains('@') && ty.ends_with('}')).then(|| ty.to_string())
}

/// `Box<dyn Jitter>` / `&dyn Jitter + Send` → `Jitter`.
fn dyn_trait_of(ty: &str) -> Option<String> {
    let inner = peel_containers(ty);
    let rest = crate::model::callee::peel_refs(inner).strip_prefix("dyn ")?;
    let name = TypeName::parse(split_top(rest, '+').first().copied().unwrap_or(rest)).name;
    (!name.is_empty()).then_some(name)
}

/// Peel `Box`/`Arc`/`Rc`/`Pin` wrappers and references off a type.
fn peel_containers(ty: &str) -> &str {
    const TRANSPARENT: [&str; 4] = ["Box", "Arc", "Rc", "Pin"];
    let mut current = crate::model::callee::peel_refs(ty).trim();
    for _ in 0..8 {
        let Some(open) = current.find('<') else {
            return current;
        };
        let base = current.get(..open).unwrap_or("").trim();
        let base_name = last_segment(base);
        if !TRANSPARENT.contains(&base_name) {
            return current;
        }
        let Some(inner) = current
            .get(open.saturating_add(1)..current.len().saturating_sub(1))
            .map(str::trim)
        else {
            return current;
        };
        current = crate::model::callee::peel_refs(
            split_top(inner, ',').first().copied().unwrap_or(inner),
        )
        .trim();
    }
    current
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

/// Remove every `::<..>` turbofish group from a path, keeping the segments.
#[must_use]
pub fn strip_generics_everywhere(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut depth = 0i32;
    let mut previous = ' ';
    for c in path.chars() {
        match c {
            '<' => depth = depth.saturating_add(1),
            '>' if previous != '-' => {
                depth = depth.saturating_sub(1);
                previous = c;
                continue;
            }
            _ => {}
        }
        if depth == 0 && c != '<' {
            out.push(c);
        }
        previous = c;
    }
    out.trim()
        .replace("::::", "::")
        .trim_end_matches("::")
        .to_string()
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

/// The leading crate identifier of an explicitly rooted path.
fn crate_root(text: &str) -> Option<&str> {
    let text = text.trim();
    let end = text
        .find(|c: char| !(c.is_alphanumeric() || c == '_'))
        .unwrap_or(text.len());
    let ident = text.get(..end)?;
    if ident.is_empty() || !ident.starts_with(|c: char| c.is_ascii_lowercase()) {
        return None;
    }
    text.get(end..)?.starts_with("::").then_some(ident)
}

/// `T`, `U`, `K`, `F`, `T1`, `__S` — the spellings a type parameter takes.
#[must_use]
pub fn looks_like_type_param(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    if let Some(rest) = name.strip_prefix("__") {
        return rest.starts_with(|c: char| c.is_ascii_uppercase());
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap_or(' ');
    if !first.is_ascii_uppercase() {
        return false;
    }
    let rest: String = chars.collect();
    rest.is_empty() || rest.chars().all(|c| c.is_ascii_digit())
}

fn last_segment(path: &str) -> &str {
    path.rsplit("::").next().unwrap_or(path)
}

fn split_last(path: &str) -> Option<(&str, &str)> {
    let at = path.rfind("::")?;
    Some((path.get(..at)?, path.get(at.saturating_add(2)..)?))
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

    #[test]
    fn turbofish_groups_are_stripped_from_a_path() {
        assert_eq!(
            strip_generics_everywhere("pairs::<HashMap<String, u32>>"),
            "pairs"
        );
        assert_eq!(
            strip_generics_everywhere("with_page_cursor::<R, F>::promoted[0]"),
            "with_page_cursor::promoted[0]"
        );
    }

    #[test]
    fn type_parameter_spellings() {
        assert!(looks_like_type_param("T"));
        assert!(looks_like_type_param("T1"));
        assert!(looks_like_type_param("__S"));
        assert!(!looks_like_type_param("HashMap"));
        assert!(!looks_like_type_param("Live"));
    }
}
