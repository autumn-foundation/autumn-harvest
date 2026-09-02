//! Classification of a call site against the model tables.
//!
//! [`Model::classify`] answers the only question the taint analysis asks about a
//! callee: *which model rows apply here?* It returns **every** class that
//! matches, in a fixed order, because a single method routinely belongs to more
//! than one table — `WorkflowContext::side_effect` is a sink (its closure must
//! not be descended) *and* sanctioned (its return value is clean), and
//! `autumn_harvest::chrono::Utc::now` is a `[[source]]` inside a `[[trusted]]`
//! crate.
//!
//! # The matching semantics, exactly
//!
//! * **`path`** — a `::`-separated **suffix** of the callee's generic-stripped
//!   segments ([`CalleePath::ends_with_path`]). MIR prints rustc's *trimmed*
//!   def-paths, so a rule keyed on a full path would match nothing.
//! * **`receiver`** — when a row carries one it must equal the callee's
//!   receiver: the self type's last `::` segment with generics stripped and
//!   references peeled (`&HashMap<K, V>` → `HashMap`, `dyn Src` → `Src`,
//!   `{closure@..}` → `{closure}`). A row *without* a receiver matches
//!   regardless of the callee's.
//! * **`dest_type`** — when a row carries one, the destination local's declared
//!   type must match it: references are peeled off the declared type, then the
//!   rule's base path must be a `::`-segment **suffix** of the declared type's
//!   base path, and the rule's generic arguments (when it writes any) must be
//!   textually equal to the declared type's. So
//!   `std::collections::BTreeMap` matches
//!   `std::collections::BTreeMap<std::string::String, u32>` (a rule that names
//!   no arguments does not constrain them), `BTreeMap` matches it too, and
//!   `std::result::Result<std::string::String, std::env::VarError>` matches only
//!   that exact instantiation. A row with a `dest_type` never matches a call
//!   whose destination type is unknown.
//! * **`[[trusted]]`** — matched on the callee's **crate root**: the leading
//!   identifier of the path text, after peeling `<`, `&`, `*const`/`*mut` and
//!   `dyn `, and only when it is followed by `::` and starts with a lowercase
//!   ASCII letter. Trimmed paths are the reason for that guard: `SystemTime::now`
//!   and `String::len` have *no* crate root (`SystemTime` is a type, not a
//!   crate), so `std`/`core`/`alloc` are treated as trusted only when the path
//!   is *clearly* std — explicitly rooted at `std::`, `core::` or `alloc::`, as
//!   `std::string::String::clone` is. A trimmed std path therefore falls through
//!   to [`CallClass::Unclassified`] rather than being silently trusted.
//!
//! # Order of the returned classes
//!
//! `Source`, `Forbidden`, `Sink`, `Sanctioned`, `NonSink`, `HandlerRegistration`,
//! `Sanitizer`, `Reduction`, `Trusted` — then `UnmodeledCtxMethod` when the
//! receiver is `WorkflowContext` and nothing matched at all, else
//! `Unclassified`. Within one table the rows are ordered most-specific first
//! (`receiver` + `dest_type` > one of them > `path` alone), matching the model
//! TOML header. **A `[[source]]` match is always reported, and always first,
//! even when a `[[trusted]]` row also matches** — that precedence is what keeps
//! `chrono` a trusted taint-propagator without making `Utc::now` clean.

use super::callee::{self, CalleePath};
use super::{
    CtxMethodRule, ForbiddenRule, Model, SanitizerRule, SinkRule, SourceRule, TrustedCrate,
};
use crate::util::{
    crate_root, is_segment_suffix, normalize_ws, peel_containers, peel_path_head, peel_refs,
    segments, split_generic,
};

/// One way a call site is classified by the model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallClass<'m> {
    /// Starts taint of the row's [`crate::TaintKind`].
    Source(&'m SourceRule),
    /// Command-emitting `WorkflowContext` method.
    Sink(&'m SinkRule),
    /// Ctx primitive whose return value is recorded in history, hence clean.
    Sanctioned(&'m CtxMethodRule),
    /// Ctx method that is neither sink nor source (observability, metadata).
    NonSink(&'m CtxMethodRule),
    /// Ctx method that registers a handler closure (analyzed as entry-adjacent).
    HandlerRegistration(&'m CtxMethodRule),
    /// Effect that is a finding on reachability alone.
    Forbidden(&'m ForbiddenRule),
    /// Clears the row's taint kind on its receiver/result.
    Sanitizer(&'m SanitizerRule),
    /// Order-killing reduction (`len`, `count`, `sum`, ...).
    Reduction(&'m SanitizerRule),
    /// Body unavailable, but the crate is modelled as a pure taint-propagator.
    Trusted(&'m TrustedCrate),
    /// `WorkflowContext::<name>` with no row at all — an honest boundary
    /// ([`crate::BoundaryKind::UnmodeledCtxMethod`]), never "assumed clean".
    UnmodeledCtxMethod(String),
    /// Nothing in the model says anything about this call.
    Unclassified,
}

/// The three keys every row can be matched on.
#[derive(Debug, Clone, Copy)]
struct RuleKey<'m> {
    path: &'m str,
    receiver: Option<&'m str>,
    dest_type: Option<&'m str>,
}

impl Model {
    /// Every class that applies to `callee`, in the documented order.
    ///
    /// `dest_type` is the declared type of the call's destination local
    /// (`let _7: T;`), or `None` when it is unknown — in which case no row that
    /// carries a `dest_type` can match.
    #[must_use]
    pub fn classify(&self, callee: &CalleePath, dest_type: Option<&str>) -> Vec<CallClass<'_>> {
        let mut out: Vec<CallClass<'_>> = Vec::new();

        push_matches(
            &self.source,
            callee,
            dest_type,
            |r| RuleKey {
                path: &r.path,
                receiver: r.receiver.as_deref(),
                dest_type: r.dest_type.as_deref(),
            },
            CallClass::Source,
            &mut out,
        );
        push_matches(
            &self.forbidden,
            callee,
            dest_type,
            |r| RuleKey {
                path: &r.path,
                receiver: r.receiver.as_deref(),
                dest_type: r.dest_type.as_deref(),
            },
            CallClass::Forbidden,
            &mut out,
        );
        push_matches(
            &self.sink,
            callee,
            dest_type,
            |r| RuleKey {
                path: &r.path,
                receiver: Some(&r.receiver),
                dest_type: None,
            },
            CallClass::Sink,
            &mut out,
        );
        push_matches(
            &self.sanctioned,
            callee,
            dest_type,
            ctx_key,
            CallClass::Sanctioned,
            &mut out,
        );
        push_matches(
            &self.non_sink,
            callee,
            dest_type,
            ctx_key,
            CallClass::NonSink,
            &mut out,
        );
        push_matches(
            &self.handler_registration,
            callee,
            dest_type,
            ctx_key,
            CallClass::HandlerRegistration,
            &mut out,
        );
        push_matches(
            &self.sanitizer,
            callee,
            dest_type,
            sanitizer_key,
            CallClass::Sanitizer,
            &mut out,
        );
        push_matches(
            &self.reduction,
            callee,
            dest_type,
            sanitizer_key,
            CallClass::Reduction,
            &mut out,
        );

        if let Some(trusted) = self.trusted_crate(callee) {
            out.push(CallClass::Trusted(trusted));
        }

        if out.is_empty() {
            if callee.receiver.as_deref() == Some("WorkflowContext") {
                out.push(CallClass::UnmodeledCtxMethod(
                    callee.last_segment().to_string(),
                ));
            } else {
                out.push(CallClass::Unclassified);
            }
        }
        out
    }

    /// The `[[trusted]]` row for `callee`'s crate root, if the path has one.
    ///
    /// See the module docs: only an *explicitly rooted* path (`tokio::…`,
    /// `std::…`) has a crate root; rustc's trimmed `Ty::method` prints do not.
    #[must_use]
    pub fn trusted_crate(&self, callee: &CalleePath) -> Option<&TrustedCrate> {
        let root = crate_root(peel_path_head(&callee.text))?;
        self.trusted.iter().find(|c| c.name == root)
    }

    /// True when `ty` is (or wraps) a type whose `static` instances are ambient
    /// taint roots — `AtomicU64`, `Mutex`, `LazyLock`, `LocalKey`, ... .
    ///
    /// References (`&`, `&mut`, `*const`, `*mut`) and the four transparent
    /// containers `Box<..>`, `Arc<..>`, `Rc<..>` and `Pin<..>` are peeled first,
    /// repeatedly, so `&Arc<Mutex<Vec<u64>>>` is ambient via `Mutex`. Anything
    /// else is matched on the type's last `::` segment with generics stripped.
    #[must_use]
    pub fn is_ambient_type(&self, ty: &str) -> bool {
        let name = callee::TypeName::parse(ty).name;
        if self.ambient_type.iter().any(|t| t.name == name) {
            return true;
        }
        let inner = peel_containers(ty);
        if inner == ty.trim() {
            return false;
        }
        let name = callee::TypeName::parse(inner).name;
        self.ambient_type.iter().any(|t| t.name == name)
    }
}

/// Match key of a `[[sanctioned]]` / `[[non_sink]]` / `[[handler_registration]]` row.
fn ctx_key(rule: &CtxMethodRule) -> RuleKey<'_> {
    RuleKey {
        path: &rule.path,
        receiver: Some(&rule.receiver),
        dest_type: None,
    }
}

/// Match key of a `[[sanitizer]]` / `[[reduction]]` row.
fn sanitizer_key(rule: &SanitizerRule) -> RuleKey<'_> {
    RuleKey {
        path: &rule.path,
        receiver: rule.receiver.as_deref(),
        dest_type: rule.dest_type.as_deref(),
    }
}

/// Push every matching row of one table, most-specific first.
fn push_matches<'m, R, C>(
    rules: &'m [R],
    callee: &CalleePath,
    dest_type: Option<&str>,
    key: impl Fn(&'m R) -> RuleKey<'m>,
    wrap: impl Fn(&'m R) -> C,
    out: &mut Vec<C>,
) {
    let mut hits: Vec<(u8, &'m R)> = Vec::new();
    for rule in rules {
        if let Some(score) = rule_matches(callee, dest_type, key(rule)) {
            hits.push((score, rule));
        }
    }
    hits.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
    out.extend(hits.into_iter().map(|(_, rule)| wrap(rule)));
}

/// `Some(specificity)` when the row applies; higher is more specific.
fn rule_matches(callee: &CalleePath, dest_type: Option<&str>, key: RuleKey<'_>) -> Option<u8> {
    if !callee.ends_with_path(key.path) {
        return None;
    }
    let mut score = 0u8;
    if let Some(want) = key.receiver {
        if callee.receiver.as_deref() != Some(want) {
            return None;
        }
        score = score.saturating_add(1);
    }
    if let Some(want) = key.dest_type {
        if !dest_type.is_some_and(|have| dest_type_matches(want, have)) {
            return None;
        }
        score = score.saturating_add(1);
    }
    Some(score)
}

/// See the module docs for the exact `dest_type` rule.
#[must_use]
pub fn dest_type_matches(rule: &str, declared: &str) -> bool {
    let rule = normalize_ws(rule);
    let declared = normalize_ws(peel_refs(declared));
    if rule == declared {
        return true;
    }
    let (rule_base, rule_args) = split_generic(&rule);
    let (decl_base, decl_args) = split_generic(&declared);
    if !is_segment_suffix(rule_base, &segments(decl_base)) {
        return false;
    }
    // A rule that writes no generic arguments does not constrain them; one that
    // writes any must match the declared type's argument list exactly.
    rule_args.is_empty() || rule_args == decl_args
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaintKind;

    fn model() -> Model {
        Model::builtin().expect("the embedded model must parse")
    }

    fn classify<'m>(m: &'m Model, path: &str, dest: Option<&str>) -> Vec<CallClass<'m>> {
        m.classify(&CalleePath::parse(path), dest)
    }

    fn source_of<'m>(classes: &'m [CallClass<'m>]) -> Option<&'m SourceRule> {
        classes.iter().find_map(|c| match c {
            CallClass::Source(r) => Some(*r),
            _ => None,
        })
    }

    #[test]
    fn a_trimmed_path_matches_by_segment_suffix() {
        let m = model();
        let classes = classify(&m, "SystemTime::now", None);
        let rule = source_of(&classes).expect("SystemTime::now is a source");
        assert_eq!(rule.kind, TaintKind::Value);
        assert!(source_of(&classify(&m, "std::time::SystemTime::now", None)).is_some());
        assert!(
            source_of(&classify(&m, "my_crate::MySystemTime::now", None)).is_none(),
            "the suffix is matched segment-wise, not textually: `MySystemTime` \
             is not `SystemTime`"
        );
    }

    #[test]
    fn receiver_narrows_a_bare_suffix() {
        let m = model();
        // `iter` with a HashMap receiver is an Order source ...
        let hash = classify(
            &m,
            "<HashMap<String, u64> as IntoIterator>::into_iter",
            None,
        );
        let rule = source_of(&hash).expect("HashMap iteration is an Order source");
        assert_eq!(rule.kind, TaintKind::Order);
        // ... the same method on a Vec is not.
        let vec = classify(&m, "<Vec<u64> as IntoIterator>::into_iter", None);
        assert!(source_of(&vec).is_none(), "{vec:?}");
    }

    #[test]
    fn a_dest_type_row_must_spell_out_the_type_rustc_prints_not_its_alias() {
        let m = model();
        // Measured with `rustc --emit=mir` (1.98.0): `std::env::current_dir()`
        // declares its destination as the RESOLVED type, never as the
        // `std::io::Result<T>` alias the function signature is written with.
        let printed = "std::result::Result<std::path::PathBuf, std::io::Error>";
        assert!(
            source_of(&classify(&m, "current_dir", Some(printed))).is_some(),
            "the `current_dir` row must fire on the type MIR actually prints"
        );
        assert!(
            source_of(&classify(&m, "current_exe", Some(printed))).is_some(),
            "so must `current_exe`, which shares the destination type"
        );
        assert!(
            !dest_type_matches("std::io::error::Result<std::path::PathBuf>", printed),
            "the alias spelling is exactly what could never match — a row \
             written that way is dead, not merely imprecise"
        );
        assert!(
            source_of(&classify(&m, "current_dir", None)).is_none(),
            "a row carrying a `dest_type` never matches an unknown destination"
        );
    }

    #[test]
    fn dest_type_pins_a_bare_suffix_to_the_real_std_item() {
        let m = model();
        let decl = "std::result::Result<std::string::String, std::env::VarError>";
        assert!(
            source_of(&classify(&m, "var::<&str>", Some(decl))).is_some(),
            "std::env::var is pinned by its destination type"
        );
        assert!(
            source_of(&classify(&m, "var::<&str>", None)).is_none(),
            "a user fn named `var` must not be flagged"
        );
        assert!(source_of(&classify(&m, "var::<&str>", Some("u64"))).is_none());
    }

    #[test]
    fn a_dest_type_rule_without_generics_ignores_them() {
        assert!(dest_type_matches(
            "std::collections::BTreeMap",
            "std::collections::BTreeMap<std::string::String, u32>"
        ));
        assert!(dest_type_matches(
            "BTreeMap",
            "std::collections::BTreeMap<std::string::String, u32>"
        ));
        assert!(!dest_type_matches(
            "std::collections::BTreeMap",
            "std::collections::HashMap<std::string::String, u32>"
        ));
        assert!(dest_type_matches(
            "tokio::time::Sleep",
            "tokio::time::Sleep"
        ));
        assert!(dest_type_matches(
            "tokio::time::Sleep",
            "&tokio::time::Sleep"
        ));
        assert!(!dest_type_matches(
            "std::result::Result<std::string::String, std::env::VarError>",
            "std::result::Result<std::string::String, u32>"
        ));
    }

    #[test]
    fn collect_into_a_btreemap_is_a_sanitizer_but_into_a_vec_is_not() {
        let m = model();
        let sorted = classify(
            &m,
            "<std::vec::IntoIter<(String, u32)> as Iterator>::collect::<BTreeMap<String, u32>>",
            Some("std::collections::BTreeMap<std::string::String, u32>"),
        );
        assert!(
            sorted
                .iter()
                .any(|c| matches!(c, CallClass::Sanitizer(r) if r.clears == TaintKind::Order)),
            "{sorted:?}"
        );
        let unsorted = classify(
            &m,
            "<std::vec::IntoIter<(String, u32)> as Iterator>::collect::<Vec<(String, u32)>>",
            Some("std::vec::Vec<(std::string::String, u32)>"),
        );
        assert!(
            !unsorted
                .iter()
                .any(|c| matches!(c, CallClass::Sanitizer(_))),
            "collect into a Vec preserves iteration order: {unsorted:?}"
        );
    }

    #[test]
    fn a_source_is_never_shadowed_by_a_trusted_crate() {
        let m = model();
        let classes = classify(&m, "autumn_harvest::chrono::Utc::now", None);
        assert!(
            matches!(classes.first(), Some(CallClass::Source(_))),
            "the source row must be reported first: {classes:?}"
        );
        assert!(
            classes
                .iter()
                .any(|c| matches!(c, CallClass::Trusted(t) if t.name == "autumn_harvest")),
            "and the trusted row is still reported: {classes:?}"
        );
    }

    #[test]
    fn trusted_needs_an_explicit_crate_root() {
        let m = model();
        assert_eq!(
            m.trusted_crate(&CalleePath::parse("std::string::String::len"))
                .map(|t| t.name.as_str()),
            Some("std")
        );
        assert_eq!(
            m.trusted_crate(&CalleePath::parse("<std::string::String as Clone>::clone"))
                .map(|t| t.name.as_str()),
            Some("std"),
            "a qualified path is rooted at its self type"
        );
        assert_eq!(
            m.trusted_crate(&CalleePath::parse("String::len")),
            None,
            "a trimmed path has no crate root and must not be assumed std"
        );
        assert_eq!(m.trusted_crate(&CalleePath::parse("wall_clock_secs")), None);
        assert_eq!(
            m.trusted_crate(&CalleePath::parse("_::_serde::de::Error::custom")),
            None,
            "`_` is rustc's elision marker, not a crate"
        );
    }

    #[test]
    fn a_ctx_method_can_be_both_a_sink_and_sanctioned() {
        let m = model();
        let classes = classify(
            &m,
            "WorkflowContext::side_effect::<u64, {closure@f.rs:1:1: 1:2}>",
            None,
        );
        assert!(
            classes.iter().any(|c| matches!(c, CallClass::Sink(_))),
            "{classes:?}"
        );
        assert!(
            classes
                .iter()
                .any(|c| matches!(c, CallClass::Sanctioned(_))),
            "{classes:?}"
        );
        let sink_at = classes
            .iter()
            .position(|c| matches!(c, CallClass::Sink(_)))
            .unwrap_or(usize::MAX);
        let sanctioned_at = classes
            .iter()
            .position(|c| matches!(c, CallClass::Sanctioned(_)))
            .unwrap_or(usize::MAX);
        assert!(
            sink_at < sanctioned_at,
            "sinks are reported before sanctioned rows"
        );
    }

    #[test]
    fn an_unmodeled_ctx_method_is_a_boundary_not_a_clean_call() {
        let m = model();
        let classes = classify(&m, "WorkflowContext::mystery_method", None);
        assert_eq!(
            classes,
            vec![CallClass::UnmodeledCtxMethod("mystery_method".to_string())]
        );
    }

    #[test]
    fn an_unknown_call_is_unclassified() {
        let m = model();
        assert_eq!(
            classify(&m, "my_crate::helpers::compute_total", None),
            vec![CallClass::Unclassified]
        );
    }

    #[test]
    fn ambient_types_see_through_wrappers() {
        let m = model();
        assert!(m.is_ambient_type("AtomicU64"));
        assert!(m.is_ambient_type("std::sync::atomic::AtomicU64"));
        assert!(m.is_ambient_type("&std::sync::Mutex<std::vec::Vec<u64>>"));
        assert!(m.is_ambient_type("std::sync::Arc<std::sync::Mutex<u64>>"));
        assert!(m.is_ambient_type("Box<RefCell<u64>>"));
        assert!(m.is_ambient_type("std::pin::Pin<Box<Cell<u8>>>"));
        assert!(!m.is_ambient_type("u64"));
        assert!(!m.is_ambient_type("std::vec::Vec<u64>"));
        assert!(
            !m.is_ambient_type("std::sync::Arc<std::vec::Vec<u64>>"),
            "a transparent wrapper around a plain type stays clean"
        );
    }

    #[test]
    fn classification_never_panics_on_hostile_paths() {
        let m = model();
        for text in ["", "<", "::", "<<T as>>::", "{closure@", "a::"] {
            let _ = classify(&m, text, Some("<"));
        }
    }
}
