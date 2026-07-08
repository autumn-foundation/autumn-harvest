use proc_macro2::Span;
use std::collections::HashMap;
use syn::{Expr, spanned::Spanned as _, visit::Visit};

#[derive(Debug, Clone)]
pub struct RuleInfo {
    pub id: String,
    pub severity: String, // "HardBlocker" or "Warning"
    pub explanation: String,
    pub alternative: String,
}

pub struct LinterFinding {
    pub rule_id: String,
    pub severity: String,
    pub message: String,
    pub alternative: String,
    pub span: Span,
}

pub struct DeterminismVisitor {
    pub findings: Vec<LinterFinding>,
    catalog: HashMap<String, RuleInfo>,
    in_await_condition_closure: bool,
    pub context_param_name: Option<String>,
    /// HVG011 (issue #785): lexical scope stack of local bindings, ident →
    /// is-hash. Lookup walks innermost-out; a `false` entry MASKS an outer
    /// tracked binding (shadowing) rather than deleting it, so scope exit
    /// (block/arm/closure/for-pattern) restores the outer state — the flat
    /// function-wide set this replaces caused `HardBlocker` false positives on
    /// pattern-shadowed idents and inner-block bindings (PR #970 review, P1-A).
    /// Last-binding-wins within a scope; unknown shapes default to not-tracked.
    hash_scopes: Vec<HashMap<String, bool>>,
}

/// Collects every ident bound by a pattern (match arm, destructuring `let`,
/// closure input, for-loop pattern) so it can be masked in the current HVG011
/// scope. Over-collection is safe: masking means "not hash", so at worst a
/// legitimate finding is missed, never a false positive introduced.
struct PatIdentCollector {
    idents: Vec<String>,
}

impl<'ast> Visit<'ast> for PatIdentCollector {
    fn visit_pat_ident(&mut self, i: &'ast syn::PatIdent) {
        self.idents.push(i.ident.to_string());
        syn::visit::visit_pat_ident(self, i);
    }
}

fn pattern_idents(pat: &syn::Pat) -> Vec<String> {
    let mut collector = PatIdentCollector { idents: Vec::new() };
    collector.visit_pat(pat);
    collector.idents
}

impl DeterminismVisitor {
    #[allow(clippy::missing_const_for_fn)]
    pub fn new(catalog: HashMap<String, RuleInfo>) -> Self {
        Self {
            findings: Vec::new(),
            catalog,
            in_await_condition_closure: false,
            context_param_name: None,
            hash_scopes: vec![HashMap::new()],
        }
    }

    fn push_hash_scope(&mut self) {
        self.hash_scopes.push(HashMap::new());
    }

    fn pop_hash_scope(&mut self) {
        if self.hash_scopes.len() > 1 {
            self.hash_scopes.pop();
        }
    }

    /// Innermost-out lookup: the nearest binding of `ident` decides. Missing
    /// everywhere means not tracked.
    fn ident_is_tracked_hash(&self, ident: &str) -> bool {
        for scope in self.hash_scopes.iter().rev() {
            if let Some(&is_hash) = scope.get(ident) {
                return is_hash;
            }
        }
        false
    }

    fn bind_in_current_scope(&mut self, ident: String, is_hash: bool) {
        if let Some(scope) = self.hash_scopes.last_mut() {
            scope.insert(ident, is_hash);
        }
    }

    /// Masks (insert `false` for) every ident bound by `pat` in the current
    /// scope — shadowing, not removal, so outer state is restored on scope exit.
    fn mask_pattern_idents(&mut self, pat: &syn::Pat) {
        for ident in pattern_idents(pat) {
            self.bind_in_current_scope(ident, false);
        }
    }

    fn add_finding(&mut self, rule_id: &str, span: Span) {
        self.push_finding(rule_id, span, None);
    }

    /// Like [`Self::add_finding`] but overrides the catalog severity.
    ///
    /// Used by HVG011's command-aware downgrade: the catalog severity is the
    /// class's worst case (`HardBlocker` — a command-emitting loop); a
    /// command-free loop is surfaced as a Warning instead. Kept as a separate
    /// path so the HVG008 rewrite logic in [`Self::push_finding`] is untouched.
    ///
    /// CONTRACT NOTES: the override string must exactly match `"HardBlocker"`
    /// or `"Warning"` — workflow.rs's severity dispatch is an exact string
    /// match, so any other spelling silently falls through to the warning
    /// path. And this method must never be used for HVG001/HVG002 findings:
    /// those two IDs are remapped to HVG008 inside `await_condition` closures
    /// by [`Self::push_finding`], and an override would pin the pre-remap
    /// rule's severity onto the remapped HVG008 finding.
    fn add_finding_with_severity(&mut self, rule_id: &str, span: Span, severity: &str) {
        self.push_finding(rule_id, span, Some(severity));
    }

    fn push_finding(&mut self, rule_id: &str, span: Span, severity_override: Option<&str>) {
        let actual_rule_id =
            if self.in_await_condition_closure && (rule_id == "HVG001" || rule_id == "HVG002") {
                "HVG008"
            } else {
                rule_id
            };
        if let Some(rule) = self.catalog.get(actual_rule_id) {
            self.findings.push(LinterFinding {
                rule_id: rule.id.clone(),
                severity: severity_override
                    .map_or_else(|| rule.severity.clone(), ToString::to_string),
                message: rule.explanation.clone(),
                alternative: rule.alternative.clone(),
                span,
            });
        }
    }

    /// HVG011: returns the tracked ident when `expr` (a `for` loop's iterated
    /// expression) iterates a locally-bound `HashMap`/`HashSet`.
    ///
    /// Deliberately narrow (issue #785's explicit syntactic boundary): only a
    /// bare tracked ident, `&ident` / `&mut ident`, or a SINGLE
    /// argument-free iteration method call on a tracked ident matches. Longer
    /// chains (`map.keys().sorted()`, `.collect::<Vec<_>>()`) are never
    /// flagged — that is how "already-sorted iterators are never flagged"
    /// holds — and function parameters/fields are never tracked.
    fn iterated_hash_local(&self, expr: &syn::Expr) -> Option<String> {
        let path_ident = |e: &syn::Expr| -> Option<String> {
            if let Expr::Path(p) = e {
                p.path.get_ident().map(ToString::to_string)
            } else {
                None
            }
        };
        let ident = match expr {
            Expr::Path(_) => path_ident(expr)?,
            Expr::Reference(r) => path_ident(&r.expr)?,
            Expr::MethodCall(mc)
                if mc.args.is_empty()
                    && mc.turbofish.is_none()
                    && HASH_ITER_METHODS.contains(&mc.method.to_string().as_str()) =>
            {
                path_ident(&mc.receiver)?
            }
            _ => return None,
        };
        self.ident_is_tracked_hash(&ident).then_some(ident)
    }
}

/// Iteration methods on `HashMap`/`HashSet` whose order is hash-randomized.
const HASH_ITER_METHODS: &[&str] = &[
    "iter",
    "iter_mut",
    "keys",
    "values",
    "values_mut",
    "drain",
    "into_iter",
    "into_keys",
    "into_values",
];

fn is_hash_collection_name(ident: &syn::Ident) -> bool {
    ident == "HashMap" || ident == "HashSet"
}

/// `true` when a type annotation's path ends in `HashMap`/`HashSet`
/// (any prefix, e.g. `std::collections::HashMap<K, V>`).
fn type_is_hash_collection(ty: &syn::Type) -> bool {
    if let syn::Type::Path(tp) = ty {
        tp.path
            .segments
            .last()
            .is_some_and(|seg| is_hash_collection_name(&seg.ident))
    } else {
        false
    }
}

/// `true` when a `.collect` turbofish argument produces a `HashMap`/`HashSet`,
/// including the fallible/optional forms `Result<HashMap<..>, E>` and
/// `Option<HashMap<..>>` (PR #970 review, P2-C): the wrapper's FIRST generic
/// type argument is checked recursively. Pinned contract: the wrapper form is
/// accepted both behind a `?` (the realistic `.collect::<Result<..>>()?`) and
/// unwrapped.
fn collect_target_is_hash(ty: &syn::Type) -> bool {
    if type_is_hash_collection(ty) {
        return true;
    }
    if let syn::Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && (seg.ident == "Result" || seg.ident == "Option")
        && let syn::PathArguments::AngleBracketed(ab) = &seg.arguments
        && let Some(syn::GenericArgument::Type(first)) = ab
            .args
            .iter()
            .find(|arg| matches!(arg, syn::GenericArgument::Type(_)))
    {
        return collect_target_is_hash(first);
    }
    false
}

/// `true` when a `let` initializer expression produces a `HashMap`/`HashSet`:
/// a constructor call (`HashMap::new()`, `HashSet::from(..)`, `default`,
/// `with_capacity` variants — any path prefix) or a chain ending in
/// `.collect::<HashMap<..>>()` / `.collect::<HashSet<..>>()` (also the
/// `Result`/`Option`-wrapped turbofish forms — see [`collect_target_is_hash`]).
fn init_is_hash_collection(expr: &syn::Expr) -> bool {
    match expr {
        Expr::Call(call) => {
            if let Expr::Path(p) = &*call.func {
                let segments: Vec<_> = p.path.segments.iter().collect();
                if segments.len() >= 2 {
                    let ctor = segments[segments.len() - 1].ident.to_string();
                    let collection = &segments[segments.len() - 2].ident;
                    return is_hash_collection_name(collection)
                        && matches!(
                            ctor.as_str(),
                            "new"
                                | "from"
                                | "from_iter"
                                | "with_capacity"
                                | "with_hasher"
                                | "with_capacity_and_hasher"
                                | "default"
                        );
                }
            }
            false
        }
        Expr::MethodCall(mc) if mc.method == "collect" => mc.turbofish.as_ref().is_some_and(|tf| {
            tf.args.iter().any(
                |arg| matches!(arg, syn::GenericArgument::Type(ty) if collect_target_is_hash(ty)),
            )
        }),
        // `..collect::<Result<HashMap<..>, E>>()?` — unwrap the try.
        Expr::Try(t) => init_is_hash_collection(&t.expr),
        _ => false,
    }
}

/// Extracts the bound ident (and whether its explicit type annotation is a
/// hash collection) from a `let` pattern. Tuple/struct destructuring returns
/// `None` — unknown binding shapes default to NOT tracking (false positives
/// are the top risk for a `HardBlocker` lint).
fn local_binding_target(pat: &syn::Pat) -> Option<(String, bool)> {
    match pat {
        syn::Pat::Ident(pi) => Some((pi.ident.to_string(), false)),
        syn::Pat::Type(pt) => {
            if let syn::Pat::Ident(pi) = &*pt.pat {
                Some((pi.ident.to_string(), type_is_hash_collection(&pt.ty)))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Command-call predicate for HVG011's severity decision: method names on the
/// workflow context that schedule durable commands (history-ordered work).
fn is_ctx_command_method(name: &str) -> bool {
    name.starts_with("execute_activity")
        || name.starts_with("spawn_child_workflow")
        || name.starts_with("execute_local_activity")
        || name == "timer"
        || name == "side_effect"
        // ctx.race() schedules durable commands per branch (issue #600).
        || name == "race"
}

/// Nested scan of a `for` loop body for command calls on the context param.
struct CtxCommandFinder<'a> {
    ctx_name: &'a str,
    found: bool,
}

impl<'ast> Visit<'ast> for CtxCommandFinder<'_> {
    fn visit_expr_method_call(&mut self, i: &'ast syn::ExprMethodCall) {
        if self.found {
            return;
        }
        if let Expr::Path(p) = &*i.receiver
            && p.path.is_ident(self.ctx_name)
            && is_ctx_command_method(&i.method.to_string())
        {
            self.found = true;
            return;
        }
        syn::visit::visit_expr_method_call(self, i);
    }
}

fn block_contains_ctx_command(block: &syn::Block, ctx_name: &str) -> bool {
    let mut finder = CtxCommandFinder {
        ctx_name,
        found: false,
    };
    finder.visit_block(block);
    finder.found
}

fn path_to_string(path: &syn::Path) -> String {
    let mut s = String::new();
    for (i, segment) in path.segments.iter().enumerate() {
        if i > 0 {
            s.push_str("::");
        }
        s.push_str(&segment.ident.to_string());
    }
    s
}

fn is_rng_receiver(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::Path(expr_path) => {
            let path_str = path_to_string(&expr_path.path);
            path_str.to_lowercase().contains("rng")
        }
        syn::Expr::Call(expr_call) => {
            if let syn::Expr::Path(func_path) = &*expr_call.func {
                let path_str = path_to_string(&func_path.path);
                path_str.to_lowercase().contains("rng")
            } else {
                false
            }
        }
        syn::Expr::MethodCall(expr_method) => {
            expr_method
                .method
                .to_string()
                .to_lowercase()
                .contains("rng")
                || is_rng_receiver(&expr_method.receiver)
        }
        syn::Expr::Reference(expr_ref) => is_rng_receiver(&expr_ref.expr),
        syn::Expr::Unary(expr_unary) => is_rng_receiver(&expr_unary.expr),
        syn::Expr::Field(expr_field) => {
            let member_is_rng = if let syn::Member::Named(ident) = &expr_field.member {
                ident.to_string().to_lowercase().contains("rng")
            } else {
                false
            };
            member_is_rng || is_rng_receiver(&expr_field.base)
        }
        _ => false,
    }
}

#[allow(clippy::collapsible_if)]
fn is_io_path(path_str: &str) -> bool {
    // Filesystem IO
    if path_str.starts_with("std::fs::")
        || path_str.starts_with("fs::")
        || path_str.starts_with("tokio::fs::")
    {
        return true;
    }

    // Databases & SQL
    if path_str.starts_with("diesel::")
        || path_str.starts_with("sqlx::")
        || path_str.starts_with("tokio_postgres::")
        || path_str.starts_with("redis::")
    {
        return true;
    }

    // HTTP clients (reqwest, hyper)
    if path_str.starts_with("reqwest::Client")
        || path_str.starts_with("reqwest::blocking::Client")
        || path_str.starts_with("reqwest::get")
        || path_str.starts_with("hyper::client::")
        || path_str.starts_with("hyper::server::")
        || path_str.starts_with("hyper::service::")
        || path_str.starts_with("hyper::rt::")
    {
        return true;
    }

    // gRPC client/transport (tonic)
    if path_str.starts_with("tonic::transport::") || path_str.starts_with("tonic::client::") {
        return true;
    }

    // Process spawning
    if path_str == "std::process::Command::new" {
        return true;
    }

    // Networking (only sockets and connections, not IP address helpers)
    let is_network_connect_or_bind = path_str.ends_with("::connect")
        || path_str.ends_with("::connect_lazy")
        || path_str.ends_with("::from_shared")
        || path_str.ends_with("::bind");

    if is_network_connect_or_bind {
        if path_str.starts_with("std::net::")
            || path_str.starts_with("tokio::net::")
            || path_str.starts_with("TcpStream::")
            || path_str.starts_with("TcpListener::")
            || path_str.starts_with("UdpSocket::")
            || path_str.starts_with("UnixStream::")
            || path_str.starts_with("UnixListener::")
            || path_str.starts_with("UnixDatagram::")
            || path_str.starts_with("Endpoint::")
            || path_str.starts_with("Channel::")
            || path_str == "TcpStream::connect"
            || path_str == "TcpListener::bind"
            || path_str == "UdpSocket::bind"
            || path_str == "UnixStream::connect"
            || path_str == "UnixListener::bind"
            || path_str == "UnixDatagram::bind"
            || path_str == "Endpoint::connect"
            || path_str == "Channel::connect"
            || path_str == "Channel::from_shared"
            || path_str == "Endpoint::from_shared"
        {
            return true;
        }
    }

    false
}

impl<'ast> Visit<'ast> for DeterminismVisitor {
    fn visit_local(&mut self, i: &'ast syn::Local) {
        // HVG011 binding tracking: mark `let` bindings that are hash-typed via
        // an explicit type annotation, a `HashMap::new()`-style constructor
        // call, or a `.collect::<HashMap<..>>()` turbofish. Last-binding-wins
        // within a scope: re-binding the same ident to a non-hash type masks
        // it. Unknown binding shapes (destructuring, let-else patterns, enum
        // variants) mask EVERY ident they bind — shadowing, never tracking —
        // so an outer tracked binding cannot bleed through a pattern re-bind
        // (PR #970 review, P1-A).
        if let Some((name, annotated_hash)) = local_binding_target(&i.pat) {
            let init_hash = i
                .init
                .as_ref()
                .is_some_and(|init| init_is_hash_collection(&init.expr));
            self.bind_in_current_scope(name, annotated_hash || init_hash);
        } else {
            self.mask_pattern_idents(&i.pat);
        }

        // Delegate to nested traversal
        syn::visit::visit_local(self, i);
    }

    fn visit_block(&mut self, i: &'ast syn::Block) {
        // HVG011: every block is a lexical scope — bindings made inside it
        // must not survive block exit (inner-block leak, PR #970 P1-A(b)).
        self.push_hash_scope();
        syn::visit::visit_block(self, i);
        self.pop_hash_scope();
    }

    fn visit_arm(&mut self, i: &'ast syn::Arm) {
        // HVG011: a match arm's pattern bindings shadow outer idents for the
        // guard + body extent.
        self.push_hash_scope();
        self.mask_pattern_idents(&i.pat);
        syn::visit::visit_arm(self, i);
        self.pop_hash_scope();
    }

    fn visit_expr_closure(&mut self, i: &'ast syn::ExprClosure) {
        // HVG011: closure parameters shadow outer idents for the closure body.
        // NOTE: the ctx.side_effect(..) closure-skip in visit_expr_method_call
        // never reaches this method (it visits the closure's input pats
        // directly and skips the body), so that escape hatch is unaffected.
        self.push_hash_scope();
        for input in &i.inputs {
            self.mask_pattern_idents(input);
        }
        syn::visit::visit_expr_closure(self, i);
        self.pop_hash_scope();
    }

    fn visit_expr_for_loop(&mut self, i: &'ast syn::ExprForLoop) {
        // HVG011: NonDeterministicIteration (issue #785; the issue proposed
        // HVG010, permanently taken by `SelectMacro`/#600, hence HVG011).
        // Command-aware severity: a loop body that schedules commands on the
        // context param is a HardBlocker (the recorded command order would
        // follow the randomized hash order and diverge on replay); a
        // command-free loop is downgraded to a Warning.
        //
        // The check runs against the OUTER scopes — the iterated expression is
        // evaluated before the loop pattern binds.
        if self.iterated_hash_local(&i.expr).is_some() {
            let ctx_name = self.context_param_name.clone();
            let has_command =
                block_contains_ctx_command(&i.body, ctx_name.as_deref().unwrap_or("ctx"));
            let span = i.expr.span();
            if has_command {
                self.add_finding("HVG011", span);
            } else {
                self.add_finding_with_severity("HVG011", span, "Warning");
            }
        }

        // Manual traversal (mirrors syn::visit::visit_expr_for_loop's order)
        // so the loop pattern's bindings shadow outer idents for the body
        // extent only (PR #970 P1-A(a): `for m in &lists { for x in m … }`).
        for attr in &i.attrs {
            self.visit_attribute(attr);
        }
        if let Some(label) = &i.label {
            self.visit_label(label);
        }
        self.visit_expr(&i.expr);
        self.push_hash_scope();
        self.mask_pattern_idents(&i.pat);
        self.visit_pat(&i.pat);
        self.visit_block(&i.body);
        self.pop_hash_scope();
    }

    #[allow(clippy::too_many_lines)]
    fn visit_expr_call(&mut self, i: &'ast syn::ExprCall) {
        if let Expr::Path(expr_path) = &*i.func {
            let path_str = path_to_string(&expr_path.path);

            // HVG001: WallClock
            if path_str == "chrono::Utc::now"
                || path_str == "Utc::now"
                || path_str == "chrono::Local::now"
                || path_str == "Local::now"
                || path_str == "std::time::Instant::now"
                || path_str == "Instant::now"
                || path_str == "std::time::SystemTime::now"
                || path_str == "SystemTime::now"
            {
                self.add_finding(
                    "HVG001",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }

            // HVG002: Randomness
            if path_str == "rand::random"
                || path_str == "random"
                || path_str == "rand::random_range"
                || path_str == "random_range"
                || path_str == "rand::random_bool"
                || path_str == "random_bool"
                || path_str == "rand::thread_rng"
                || path_str == "thread_rng"
                || path_str == "Uuid::new_v4"
                || path_str == "uuid::Uuid::new_v4"
                || path_str == "Uuid::now_v7"
                || path_str == "uuid::Uuid::now_v7"
                || path_str.starts_with("rand::Rng::")
                || path_str.starts_with("Rng::")
                || (path_str.starts_with("rand::")
                    && (path_str.ends_with("::gen")
                        || path_str.ends_with("::r#gen")
                        || path_str.ends_with("::gen_range")
                        || path_str.ends_with("::r#gen_range")
                        || path_str.ends_with("::gen_bool")
                        || path_str.ends_with("::gen_ratio")
                        || path_str.ends_with("::sample")
                        || path_str.ends_with("::fill")
                        || path_str.ends_with("::try_fill")))
            {
                self.add_finding(
                    "HVG002",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }

            // HVG003: ProcessEnv
            if path_str == "std::env::var"
                || path_str == "env::var"
                || path_str == "std::env::var_os"
                || path_str == "env::var_os"
                || path_str == "std::env::args"
                || path_str == "env::args"
                || path_str == "std::env::args_os"
                || path_str == "env::args_os"
            {
                self.add_finding(
                    "HVG003",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }

            // HVG004: SleepTimer
            if path_str == "std::thread::sleep"
                || path_str == "thread::sleep"
                || path_str == "tokio::time::sleep"
                || path_str == "time::sleep"
                || path_str == "async_std::task::sleep"
            {
                self.add_finding(
                    "HVG004",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }

            // HVG005: BackgroundTask
            if path_str == "tokio::spawn"
                || path_str == "std::thread::spawn"
                || path_str == "thread::spawn"
                || path_str == "async_std::task::spawn"
                || path_str == "rayon::spawn"
            {
                self.add_finding(
                    "HVG005",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }

            // HVG006: DirectIo
            if is_io_path(&path_str) {
                self.add_finding(
                    "HVG006",
                    expr_path.path.segments.last().unwrap().ident.span(),
                );
            }
        }

        // Delegate to nested traversal
        syn::visit::visit_expr_call(self, i);
    }

    #[allow(clippy::collapsible_if)]
    fn visit_expr_method_call(&mut self, i: &'ast syn::ExprMethodCall) {
        // HVG007: ProcessGlobal (lock call on uppercase static/const path receiver)
        if i.method == "lock" {
            if let Some(last_seg) = match &*i.receiver {
                Expr::Path(expr_path) => expr_path.path.segments.last(),
                _ => None,
            } {
                let ident_str = last_seg.ident.to_string();
                if ident_str.chars().all(|c| !c.is_lowercase()) {
                    self.add_finding("HVG007", i.method.span());
                }
            }
        }

        let is_await_cond = i.method == "await_condition" || i.method == "await_condition_timeout";
        let old_await_cond = self.in_await_condition_closure;
        if is_await_cond {
            self.in_await_condition_closure = true;
        }

        // HVG002: Randomness method calls (gen, gen_range, etc. on Rng trait)
        let method_str = i.method.to_string();
        if (method_str == "gen"
            || method_str == "r#gen"
            || method_str == "gen_range"
            || method_str == "r#gen_range"
            || method_str == "gen_bool"
            || method_str == "gen_ratio")
            && is_rng_receiver(&i.receiver)
        {
            self.add_finding("HVG002", i.method.span());
        }

        let is_ctx_side_effect = if i.method == "side_effect" {
            if let syn::Expr::Path(expr_path) = &*i.receiver {
                self.context_param_name.as_ref().map_or_else(
                    || expr_path.path.is_ident("ctx"),
                    |ctx_name| expr_path.path.is_ident(ctx_name),
                )
            } else {
                false
            }
        } else {
            false
        };

        // Delegate to nested traversal
        if is_ctx_side_effect {
            self.visit_expr(&i.receiver);
            if let Some(first_arg) = i.args.first() {
                self.visit_expr(first_arg);
            }
            if let Some(second_arg) = i.args.get(1) {
                if let syn::Expr::Closure(closure) = second_arg {
                    for attr in &closure.attrs {
                        self.visit_attribute(attr);
                    }
                    for input in &closure.inputs {
                        self.visit_pat(input);
                    }
                } else {
                    self.visit_expr(second_arg);
                }
            }
            for arg in i.args.iter().skip(2) {
                self.visit_expr(arg);
            }
        } else {
            syn::visit::visit_expr_method_call(self, i);
        }

        self.in_await_condition_closure = old_await_cond;
    }

    fn visit_expr_macro(&mut self, i: &'ast syn::ExprMacro) {
        // HVG009: UnsafeLogging
        let path_str = path_to_string(&i.mac.path);
        if path_str == "info"
            || path_str == "warn"
            || path_str == "error"
            || path_str == "debug"
            || path_str == "trace"
            || path_str == "tracing::info"
            || path_str == "tracing::warn"
            || path_str == "tracing::error"
            || path_str == "tracing::debug"
            || path_str == "tracing::trace"
            || path_str == "log::info"
            || path_str == "log::warn"
            || path_str == "log::error"
            || path_str == "log::debug"
            || path_str == "log::trace"
        {
            self.add_finding("HVG009", i.mac.path.segments.last().unwrap().ident.span());
        }

        // Delegate to nested traversal
        syn::visit::visit_expr_macro(self, i);
    }

    fn visit_macro(&mut self, i: &'ast syn::Macro) {
        // HVG010: `SelectMacro` (issue #600) -- tokio::select!/futures::select!/
        // futures::select_biased! racing ctx-managed awaitables is a double
        // footgun (non-deterministic winner on replay, no durable loser
        // cancellation). Checked at the generic `visit_macro` level (rather
        // than `visit_expr_macro`) because `select! { .. }` used bare at
        // statement position -- the idiomatic form, with no trailing `;` --
        // parses as `syn::StmtMacro`, not `syn::ExprMacro`; `visit_macro` is
        // called from both. Detected by macro path only, mirroring HVG009 --
        // the macro body's tokens are not parsed as exprs by syn, so this
        // cannot distinguish a `select!` over ctx awaitables from one over
        // plain futures, but a workflow body has no legitimate reason to
        // `select!` over anything else (ctx.race() and futures::join! cover
        // every wait-first/wait-all shape blessed for workflow code).
        let path_str = path_to_string(&i.path);
        if path_str == "select"
            || path_str == "select_biased"
            || path_str == "tokio::select"
            || path_str == "futures::select"
            || path_str == "futures::select_biased"
        {
            self.add_finding("HVG010", i.path.segments.last().unwrap().ident.span());
        }

        // Delegate to nested traversal
        syn::visit::visit_macro(self, i);
    }

    fn visit_path(&mut self, i: &'ast syn::Path) {
        let path_str = path_to_string(i);
        if path_str == "rand::rngs::OsRng" || path_str == "OsRng" {
            self.add_finding("HVG002", i.segments.last().unwrap().ident.span());
        }
        if path_str == "std::process::Command" {
            self.add_finding("HVG006", i.segments.last().unwrap().ident.span());
        }
        syn::visit::visit_path(self, i);
    }
}

pub fn load_catalog_metadata() -> HashMap<String, RuleInfo> {
    let mut rules = HashMap::new();
    let entries = vec![
        RuleInfo {
            id: "HVG001".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Reading wall-clock time inside a workflow (std::time::Instant::now(), SystemTime::now(), chrono::Utc::now(), etc.) produces a different value on every replay. The workflow engine re-executes the function body deterministically against recorded history; a fresh timestamp breaks that contract and causes a non-determinism error.".to_string(),
            alternative: "Use ctx.now() (WorkflowContext) to read the workflow-logical clock, which returns the WorkflowStarted timestamp and replays identically on every subsequent run. For a real wall-clock instant captured at the call site (e.g. \"skip the notification if the event is older than 24h *now*\"), use ctx.system_now() -> DateTime<Utc> (or ctx.system_time_now() -> SystemTime), which captures the current time once and replays it deterministically via a recorded SideEffectRecorded event.".to_string(),
        },
        RuleInfo {
            id: "HVG002".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Calling rand::random(), rand::thread_rng(), Uuid::new_v4(), or any other source of randomness directly in a workflow body produces a different value on each replay pass. Since the workflow function is re-run from the top on every resume, the random sequence diverges from what was recorded in harvest_events.".to_string(),
            alternative: "Use the deterministic primitives on WorkflowContext, which capture the value once and replay it verbatim: ctx.new_uuid() for a UUIDv7 (idempotency keys), ctx.random_u64() / ctx.random_f64() / ctx.random_range(range) for sampling draws, or ctx.side_effect(name, f) to capture any one-shot non-deterministic value. For cryptographically secure randomness, generate it inside an activity (ActivityContext) and return it as the durably-recorded activity result instead.".to_string(),
        },
        RuleInfo {
            id: "HVG003".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Reading std::env::var(), std::env::args(), or process environment at workflow execution time couples replay correctness to the environment of whichever worker process happens to run the replay. Environment variables may differ between the original execution host and a replay host, causing divergence.".to_string(),
            alternative: "Read configuration at worker startup (WorkerConfig) and pass it as typed state via ctx.state::<T>(). For values that must vary per workflow run, pass them as workflow input parameters. Signal-based reconfiguration can use ctx.wait_for_signal() to update workflow-local state durably.".to_string(),
        },
        RuleInfo {
            id: "HVG004".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Calling std::thread::sleep(), tokio::time::sleep(), async_std::task::sleep(), or any OS/runtime sleep primitive in a workflow body does not record a durable timer. The workflow worker's task is blocked but no timer event is appended to harvest_events. After a worker restart the sleep is replayed as a no-op, changing observable timing.".to_string(),
            alternative: "Use ctx.timer(timer_id, duration_secs) (WorkflowContext) which emits a TimerStarted event into the durable history. The timer is enforced by the harvest timeout scanner and survives worker restarts. For periodic scheduling, use DagBuilder with an Interval or Cron schedule.".to_string(),
        },
        RuleInfo {
            id: "HVG005".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Spawning a background task with tokio::spawn(), std::thread::spawn(), async_std::task::spawn(), or rayon::spawn() from inside a workflow function creates untracked side-effects. The spawned task runs outside Harvest's supervision: its completion is not recorded in harvest_events, it is not retried on failure, and it is silently abandoned on worker restart.".to_string(),
            alternative: "Model concurrent work as parallel activity branches using ctx.execute_activity_raw(name, input, queue) calls combined with futures::join! or futures::try_join!. Harvest records each branch's result durably and re-joins them correctly on replay. For fire-and-forget side-effects, use a local activity (#[activity(local = true)]) so the result is at least logged to history.".to_string(),
        },
        RuleInfo {
            id: "HVG006".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Performing network requests (reqwest, hyper, tonic), database queries (sqlx, diesel, tokio-postgres), or filesystem I/O (std::fs, tokio::fs) directly inside a workflow body creates non-idempotent side-effects that are invisible to the event store. On replay the I/O is re-executed, potentially sending duplicate requests, corrupting database state, or failing because external state has changed.".to_string(),
            alternative: "Wrap all I/O in activities (#[activity]). Activities are the unit of durable, retryable side-effect in Harvest. Their inputs and outputs are recorded in harvest_events so the workflow can replay without re-executing I/O. Use ctx.execute_activity_raw(name, input, queue) from the workflow body to schedule the activity.".to_string(),
        },
        RuleInfo {
            id: "HVG007".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Mutating process-global state — static mut variables, std::sync::Mutex guards wrapping shared counters, lazy_static or once_cell singletons, global metrics registries, or similar — from inside a workflow body creates a side-effect that is not recorded in harvest_events. On replay the mutation is re-applied, producing double-counting or inconsistent global state across worker processes.".to_string(),
            alternative: "Keep workflow execution stateless with respect to process globals. Accumulate workflow-local state in local variables across ctx.timer() and ctx.execute_activity_raw() boundaries. If you need to emit metrics or update a registry, do so inside an activity where the side-effect is bounded to a single retryable execution unit and is not re-applied on replay.".to_string(),
        },
        RuleInfo {
            id: "HVG008".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Evaluating non-deterministic predicates inside await_condition or await_condition_timeout (such as checking Instant::now(), SystemTime::now(), or calling random generators inside the closure) leads to non-deterministic execution paths during replay. Predicates must be pure projections of deterministic workflow local state variables rehydrated by replaying events.".to_string(),
            alternative: "Use durable timers (ctx.timer()) for time-based pauses, and ensure the predicate closure relies purely on local variables mutated by deterministic signals or activities.".to_string(),
        },
        RuleInfo {
            id: "HVG009".to_string(),
            severity: "Warning".to_string(),
            explanation: "Calling tracing::info!(), tracing::warn!(), tracing::error!(), or any other bare tracing macro directly inside a #[workflow] body emits one log event per replay cycle. Because the workflow executor re-runs the function from the top on every suspension/resume, a single log statement fires N times for a workflow that suspends N times. This amplifies log volume in proportion to replay depth and fills Loki/Elastic with duplicate lines that lack correlation keys, making incident triage harder.".to_string(),
            alternative: "Use ctx.logger().info(message), ctx.logger().warn(message), ctx.logger().error(message), or the convenience wrappers ctx.log_info(message), ctx.log_warn(message), ctx.log_error(message). These are suppressed automatically during replay (is_replaying() == true) and auto-tag every event with workflow_id, execution_id, workflow_type, and replay = false for log correlation.".to_string(),
        },
        RuleInfo {
            id: "HVG010".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Using tokio::select!, futures::select!, or futures::select_biased! to race concurrent ctx-managed operations (activities, timers, signals, child workflows) inside a workflow body is a double footgun: (1) the winning branch depends on non-deterministic poll/arrival order, so a replay can pick a different branch than the original run and diverge; (2) the dropped loser branches do not durably cancel the underlying work -- a scheduled activity keeps running on a worker and a durable timer row stays live, leaking state that is never cleaned up.".to_string(),
            alternative: "Use ctx.race() (WorkflowContext), the deterministic race/select primitive (issue #600). It records the winning branch durably via a MarkerRecorded event so replay always resolves the same winner, and durably cancels every losing branch (activity task rows, child-workflow executions, or a losing durable timer) so no leaked in-flight work remains. For a single signal bounded by a deadline, ctx.receive_signal_timeout()/wait_for_signal_timeout() is the direct primitive ctx.race()'s timer-plus-signal shape wraps.".to_string(),
        },
        // HVG011 (issue #785; the issue proposed HVG010, permanently taken by
        // `SelectMacro`/#600 — IDs are never reused). Text must stay
        // byte-identical to the guardrail.rs CATALOG entry.
        RuleInfo {
            id: "HVG011".to_string(),
            severity: "HardBlocker".to_string(),
            explanation: "Iterating a std::collections::HashMap or HashSet directly inside a workflow body observes the hash-iteration order, which is randomized per process (RandomState seeds differ across workers and restarts), so a replay can visit the entries in a different order than the original run. When the loop body schedules commands (ctx.execute_activity*, ctx.spawn_child_workflow*, ctx.execute_local_activity*, ctx.timer, ctx.side_effect), the command sequence is recorded in history in iteration order -- a reordered replay produces a different command sequence and diverges (non-determinism error / nd-block). Even a command-free loop can leak the non-deterministic order into workflow-local state and flip a later branch.".to_string(),
            alternative: "Use a BTreeMap/BTreeSet (deterministic iteration order) for any collection a workflow iterates, or collect the keys into a Vec and sort() it before iterating: let mut keys: Vec<_> = map.keys().cloned().collect(); keys.sort(); for k in keys { ... }. Collections that are only ever point-looked-up (never iterated) are fine to keep as HashMap/HashSet.".to_string(),
        },
    ];

    for entry in entries {
        rules.insert(entry.id.clone(), entry);
    }
    rules
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod hvg011_tests {
    //! Visitor-level tests for HVG011 (issue #785): HashMap/HashSet
    //! iteration-order determinism.
    //!
    //! NOTE on the rule ID: issue #785's text proposed HVG010, but HVG010 was
    //! already permanently assigned to `SelectMacro` (issue #600) and rule IDs
    //! are never reused, so the iteration-order rule ships as HVG011.

    use super::{DeterminismVisitor, LinterFinding, load_catalog_metadata};
    use syn::visit::Visit as _;

    fn lint_with_ctx(src: &str, ctx_name: &str) -> Vec<LinterFinding> {
        let item_fn: syn::ItemFn = syn::parse_str(src).expect("test source must parse");
        let mut visitor = DeterminismVisitor::new(load_catalog_metadata());
        visitor.context_param_name = Some(ctx_name.to_string());
        visitor.visit_item_fn(&item_fn);
        visitor.findings
    }

    fn lint(src: &str) -> Vec<LinterFinding> {
        lint_with_ctx(src, "ctx")
    }

    fn hvg011_severities(findings: &[LinterFinding]) -> Vec<String> {
        findings
            .iter()
            .filter(|f| f.rule_id == "HVG011")
            .map(|f| f.severity.clone())
            .collect()
    }

    #[test]
    fn command_emitting_hashmap_loop_is_hard_blocker() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let mut m: HashMap<String, u64> = HashMap::new();
                for (k, v) in &m {
                    ctx.execute_activity_raw("debit", input, "default").await?;
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()],
            "command-emitting HashMap loop must be a HardBlocker"
        );
    }

    #[test]
    fn command_free_hashmap_loop_is_warning() {
        let findings = lint(
            r"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let mut m: HashMap<String, u64> = HashMap::new();
                let mut total = 0u64;
                for (_k, v) in &m {
                    total += v;
                }
                Ok(())
            }
            ",
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["Warning".to_string()],
            "command-free HashMap loop must be downgraded to Warning"
        );
    }

    #[test]
    fn fully_qualified_hashset_new_binding_is_tracked() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let mut s = std::collections::HashSet::new();
                for item in s.iter() {
                    ctx.spawn_child_workflow_raw("child", item).await?;
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()]
        );
    }

    #[test]
    fn iteration_method_forms_are_flagged() {
        for method in [
            "iter()",
            "iter_mut()",
            "keys()",
            "values()",
            "values_mut()",
            "drain()",
            "into_iter()",
            "into_keys()",
            "into_values()",
        ] {
            let src = format!(
                r#"
                async fn wf(ctx: &WorkflowContext) -> Result<(), String> {{
                    let mut m: HashMap<String, u64> = HashMap::new();
                    for x in m.{method} {{
                        ctx.execute_activity_raw("a", x, "q").await?;
                    }}
                    Ok(())
                }}
                "#
            );
            let findings = lint(&src);
            assert_eq!(
                hvg011_severities(&findings).len(),
                1,
                "`for x in m.{method}` must be flagged exactly once"
            );
        }
    }

    #[test]
    fn mut_borrow_iteration_is_flagged() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let mut m: HashMap<String, u64> = HashMap::new();
                for (_k, v) in &mut m {
                    ctx.timer("t", 1).await?;
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()]
        );
    }

    #[test]
    fn collect_turbofish_binding_is_tracked() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m = items.into_iter().collect::<std::collections::HashMap<String, u64>>();
                for (k, _v) in &m {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()]
        );
    }

    #[test]
    fn hashmap_from_binding_is_tracked() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m = HashMap::from([("a", 1u64)]);
                for (k, _v) in &m {
                    ctx.execute_local_activity_raw("a", k).await?;
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()]
        );
    }

    #[test]
    fn shadowing_with_vec_untracks_the_ident() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                let m: Vec<u64> = m.values().copied().collect();
                for v in &m {
                    ctx.execute_activity_raw("a", v, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "a later non-hash binding of the same ident must untrack it"
        );
    }

    #[test]
    fn ordered_collections_and_params_are_never_flagged() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext, param_map: HashMap<String, u64>) -> Result<(), String> {
                let b: BTreeMap<String, u64> = BTreeMap::new();
                for (k, _v) in &b {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                let v: Vec<u64> = Vec::new();
                for x in &v {
                    ctx.execute_activity_raw("a", x, "q").await?;
                }
                for (k, _v) in &param_map {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "BTreeMap/Vec/function-parameter iteration must never be flagged"
        );
    }

    #[test]
    fn sorted_keys_vec_and_longer_chains_are_never_flagged() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                let mut keys: Vec<String> = m.keys().cloned().collect();
                keys.sort();
                for k in keys {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                for k in m.keys().sorted() {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "sorted-keys Vec and multi-call iterator chains must never be flagged"
        );
    }

    #[test]
    fn iteration_inside_side_effect_closure_is_not_flagged() {
        // The side_effect closure subtree is deliberately not visited (the
        // existing escape hatch) — iteration inside it must not be flagged.
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                let _ = ctx.side_effect("sum", || {
                    let mut total = 0u64;
                    for (_k, v) in &m {
                        total += v;
                    }
                    total
                });
                Ok(())
            }
            "#,
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "iteration inside a ctx.side_effect closure must not be flagged"
        );
    }

    // ── P1-A regression pins (PR #970 review): scope-aware binding tracking ──
    // Four confirmed false-positive shapes under the old flat function-wide
    // `hash_locals` set, plus the symmetric legit case. Each FP shape must
    // produce ZERO HVG011 findings.

    #[test]
    fn match_arm_pattern_binding_shadows_tracked_hash_ident() {
        // `Some(m)` re-binds `m` to a non-hash value inside the arm; iterating
        // it there must not be flagged even though an outer `m` is hash-typed.
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                match resolve() {
                    Some(m) => {
                        for item in &m {
                            ctx.execute_activity_raw("a", item, "q").await?;
                        }
                    }
                    None => {}
                }
                Ok(())
            }
            "#,
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "a match-arm pattern binding must shadow the tracked hash ident"
        );
    }

    #[test]
    fn destructuring_let_masks_tracked_hash_ident() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                let (m, n) = split();
                for v in &m {
                    ctx.execute_activity_raw("a", v, "q").await?;
                }
                let _ = n;
                Ok(())
            }
            "#,
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "a destructuring `let` must mask the tracked hash ident"
        );
    }

    #[test]
    fn closure_param_shadows_tracked_hash_ident() {
        let findings = lint(
            r"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                let mut total = 0u64;
                let f = |m: &Vec<u64>| {
                    for v in m {
                        total += v;
                    }
                };
                f(&Vec::new());
                Ok(())
            }
            ",
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "a closure parameter must shadow the tracked hash ident"
        );
    }

    #[test]
    fn for_loop_pattern_shadows_tracked_hash_ident() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                let lists: Vec<Vec<u64>> = Vec::new();
                for m in &lists {
                    for x in m {
                        ctx.execute_activity_raw("a", x, "q").await?;
                    }
                }
                Ok(())
            }
            "#,
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "a for-loop pattern binding must shadow the tracked hash ident"
        );
    }

    #[test]
    fn inner_block_hash_binding_does_not_leak_past_block_exit() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: Vec<u64> = Vec::new();
                {
                    let m: HashMap<String, u64> = HashMap::new();
                    let _ = m.len();
                }
                for v in &m {
                    ctx.execute_activity_raw("a", v, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "an inner-block hash binding must not leak past the block exit"
        );
    }

    #[test]
    fn same_scope_hash_binding_still_fires_after_an_inner_block() {
        // Symmetric legit case: the hash `let` and the loop share a scope; an
        // intervening inner block must not swallow the tracked binding.
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                {
                    let other: Vec<u64> = Vec::new();
                    let _ = other;
                }
                for (k, _v) in &m {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()],
            "hash let in the same scope with a later loop must still fire"
        );
    }

    // ── P2-C: fallible collect (`.collect::<Result<HashMap<..>, E>>()?`) ─────

    #[test]
    fn collect_result_turbofish_with_try_is_tracked() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m = items
                    .into_iter()
                    .collect::<Result<HashMap<String, u64>, String>>()?;
                for (k, _v) in &m {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()],
            ".collect::<Result<HashMap<..>, E>>()? must be tracked"
        );
    }

    #[test]
    fn collect_option_turbofish_without_try_is_tracked() {
        // The Result/Option unwrapping is accepted with or without the `?` —
        // pinned contract (see PR #970 review, P2-C).
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m = items.into_iter().collect::<Option<HashMap<String, u64>>>();
                let m = m.unwrap_or_default();
                for (k, _v) in &m {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        // NOTE: the second `let m = m.unwrap_or_default();` re-binding is NOT
        // hash-tracked (unknown shape) so this asserts the tracking happens on
        // the collect line and is then masked — zero findings is the honest
        // outcome for this exact shape; assert instead on a direct iteration.
        let findings_direct = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m = items.into_iter().collect::<Option<HashMap<String, u64>>>();
                for (k, _v) in &m {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "re-binding via unwrap_or_default() untracks the ident"
        );
        assert_eq!(
            hvg011_severities(&findings_direct),
            vec!["HardBlocker".to_string()],
            ".collect::<Option<HashMap<..>>>() (unwrapped) must be tracked"
        );
    }

    // ── P2-F: Gemini bot coverage (default ctor, ctx.race) ──────────────────

    #[test]
    fn hashmap_default_binding_is_tracked() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m = HashMap::default();
                for (k, _v) in &m {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                let s = std::collections::HashSet::default();
                for x in &s {
                    ctx.execute_activity_raw("a", x, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string(), "HardBlocker".to_string()],
            "HashMap::default()/HashSet::default() bindings must be tracked"
        );
    }

    #[test]
    fn ctx_race_in_loop_body_is_a_command() {
        // ctx.race() schedules durable commands (issue #600) — a race inside a
        // hash-ordered loop must be a HardBlocker, not a Warning.
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                for (k, _v) in &m {
                    let _ = ctx.race().activity_raw("a", k, "q");
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()],
            "ctx.race() in a loop body must make the finding a HardBlocker"
        );
    }

    // ── P3 polish pins: test-matrix holes + negative controls ───────────────

    #[test]
    fn bare_by_move_iteration_is_flagged() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashSet<String> = HashSet::new();
                for x in m {
                    ctx.execute_activity_raw("a", x, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()],
            "bare by-move `for x in m` must be flagged"
        );
    }

    #[test]
    fn collect_hashset_turbofish_is_tracked() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let s = items.into_iter().collect::<HashSet<String>>();
                for x in &s {
                    ctx.execute_activity_raw("a", x, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()],
            ".collect::<HashSet<..>>() must be tracked"
        );
    }

    #[test]
    fn fan_out_helper_in_loop_body_is_a_command() {
        let findings = lint(
            r"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                for (_k, v) in &m {
                    let _ = ctx.execute_activity_fan_out(&info(), vec![v]).await;
                }
                Ok(())
            }
            ",
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()],
            "execute_activity_fan_out in a loop body must be a HardBlocker"
        );
    }

    #[test]
    fn ctx_side_effect_in_loop_body_is_a_command() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                for (_k, v) in &m {
                    let _ = ctx.side_effect("draw", || *v);
                }
                Ok(())
            }
            "#,
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()],
            "ctx.side_effect in a loop body must be a HardBlocker"
        );
    }

    #[test]
    fn indexmap_btreeset_and_arrays_are_never_flagged() {
        let findings = lint(
            r#"
            async fn wf(ctx: &WorkflowContext) -> Result<(), String> {
                let im: IndexMap<String, u64> = IndexMap::new();
                for (k, _v) in &im {
                    ctx.execute_activity_raw("a", k, "q").await?;
                }
                let bs: BTreeSet<String> = BTreeSet::new();
                for x in &bs {
                    ctx.execute_activity_raw("a", x, "q").await?;
                }
                let arr = [1u64, 2, 3];
                for x in arr {
                    ctx.execute_activity_raw("a", x, "q").await?;
                }
                Ok(())
            }
            "#,
        );
        assert!(
            hvg011_severities(&findings).is_empty(),
            "IndexMap/BTreeSet/array iteration must never be flagged"
        );
    }

    #[test]
    fn renamed_context_param_is_used_for_command_detection() {
        let findings = lint_with_ctx(
            r#"
            async fn wf(wf_ctx: &WorkflowContext) -> Result<(), String> {
                let m: HashMap<String, u64> = HashMap::new();
                for (k, _v) in &m {
                    wf_ctx.execute_activity_raw("a", k, "q").await?;
                }
                Ok(())
            }
            "#,
            "wf_ctx",
        );
        assert_eq!(
            hvg011_severities(&findings),
            vec!["HardBlocker".to_string()],
            "command detection must respect the renamed context param"
        );
    }
}
