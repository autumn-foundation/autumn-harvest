//! Decomposition of the callee path text that MIR prints at a call site.
//!
//! Stable `rustc --emit=mir` prints a call target as one line of text, and that
//! text is the only handle the analyzer has on *what* is being called:
//!
//! ```text
//! _5 = SystemTime::now() -> [return: bb3, unwind continue];
//! _7 = <HashMap<String, u64> as IntoIterator>::into_iter(move _8) -> ...;
//! _9 = LocalKey::<RefCell<u64>>::with::<{closure@s.rs:16:21: 16:24}, u64>(...);
//! _2 = <impl at src/lib.rs:5:1: 5:15>::next(move _3) -> ...;
//! ```
//!
//! [`CalleePath::parse`] turns that text into the fields the model matches on:
//! the generic-stripped path segments, the receiver (self) type, the trait of a
//! qualified `<T as Tr>::m` call, and the generic arguments — all recovered with
//! a balanced-delimiter scan, never with a regex or a naive `split("::")`, both
//! of which fall apart on `<<T as IntoIterator>::IntoIter as Iterator>::collect`.
//!
//! Parsing is total: any text at all yields a `CalleePath` (in the worst case
//! one whose `segments` is the whole text), because a callee the analyzer
//! cannot decompose must become a boundary, never a panic.

use crate::util::{
    generic_args_of, is_segment_suffix, is_type_shaped, matching_angle, peel_refs, split_top,
    split_top_trim, strip_generics, strip_generics_everywhere,
};

/// A callee path as printed at a MIR call site, decomposed for rule matching.
///
/// See the module docs for the shapes this handles.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CalleePath {
    /// The original text, trimmed.
    pub text: String,
    /// Path segments with generic arguments stripped.
    ///
    /// For a qualified call these are only the segments *after* the `<..>`
    /// header: `<HashMap<K, V> as IntoIterator>::into_iter` yields
    /// `["into_iter"]`, with the self type in [`Self::receiver`] and the trait
    /// in [`Self::trait_`].
    pub segments: Vec<String>,
    /// The self / receiver type name: its last `::` segment, generics stripped.
    ///
    /// `dyn Src` yields `Src` (plus [`Self::is_dyn`]), `Box<dyn Fn()>` yields
    /// `Box`, `{closure@f.rs:1:1: 1:2}` yields `{closure}`. For an unqualified
    /// `Ty::method` path it is the penultimate segment, and only when that
    /// segment is type-shaped (see [`Self::parse`]).
    pub receiver: Option<String>,
    /// The receiver type as the call site spelled it, generics stripped and
    /// references peeled: `b::Worker` for `b::Worker::run`, `HashMap` for
    /// `<HashMap<K, V> as IntoIterator>::into_iter`.
    ///
    /// [`Self::receiver`] is only its last segment, which is what the model
    /// matches on; the module qualifier is what tells two same-named impls in
    /// the analyzed set apart, so it is kept here rather than thrown away.
    pub receiver_path: Option<String>,
    /// Trait of a qualified `<T as Trait>::m` call (last segment, generics stripped).
    pub trait_: Option<String>,
    /// Turbofish arguments of the called item, in order, verbatim.
    pub generic_args: Vec<String>,
    /// Generic arguments of the receiver type, in order, verbatim.
    pub receiver_generic_args: Vec<String>,
    /// The receiver is a trait object (`dyn Tr`).
    pub is_dyn: bool,
    /// The span-carrying brace form of the receiver, verbatim and complete —
    /// `{closure@f.rs:16:21: 16:24}`, `{async block@..}`, `{coroutine@..}` —
    /// which is the key the resolver matches closure bodies on (D7).
    pub closure_span: Option<String>,
}

impl CalleePath {
    /// Decompose a MIR callee path. Never fails and never panics.
    ///
    /// Two shapes exist. A **qualified** path starts with `<`: the header is
    /// `<SelfTy as Trait>`, `<SelfTy>`, `<impl Trait for Ty>` or
    /// `<impl at file:l:c: l:c>`, and everything after the matching `>` is the
    /// segment list. An **unqualified** path is a `::`-separated segment list in
    /// which a token that starts with `<` is either the preceding segment's
    /// generic arguments (`LocalKey::<RefCell<u64>>::with`) or an inherent-impl
    /// self type (`slice::<impl [String]>::sort_by`).
    ///
    /// For an unqualified path the receiver is the penultimate *name* segment,
    /// but only when it is type-shaped — it starts with an uppercase ASCII
    /// letter or with `{`. Without that guard `std::env::var` would report a
    /// receiver of `env` and a rule keyed on `receiver = "HashMap"` could be
    /// satisfied by a module named `HashMap`-alike; module segments are
    /// lowercase by convention and are skipped.
    #[must_use]
    pub fn parse(text: &str) -> Self {
        let text = text.trim();
        let mut out = Self {
            text: text.to_string(),
            ..Self::default()
        };
        if text.starts_with('<') {
            parse_qualified(text, &mut out);
        } else {
            parse_unqualified(text, &mut out);
        }
        out
    }

    /// The last path segment (the called item's own name), or `""`.
    #[must_use]
    pub fn last_segment(&self) -> &str {
        self.segments.last().map_or("", String::as_str)
    }

    /// True when [`Self::segments`] ends with `rule`'s `::`-separated segments.
    ///
    /// This is the `path` matching rule of the model: a rule path is a **suffix**
    /// of the callee path, because MIR prints rustc's *trimmed* def-paths and a
    /// rule keyed on a full path would match nothing (see the model TOML header).
    #[must_use]
    pub fn ends_with_path(&self, rule: &str) -> bool {
        is_segment_suffix(rule, &self.segments)
    }
}

fn parse_qualified(text: &str, out: &mut CalleePath) {
    let Some(close) = matching_angle(text) else {
        // Unbalanced header (a truncated dump): keep the text, decompose nothing.
        return;
    };
    let inner = text.get(1..close).unwrap_or("").trim();
    let rest = text
        .get(close.saturating_add(1)..)
        .unwrap_or("")
        .trim_start()
        .trim_start_matches("::");

    if !inner.starts_with("impl at ") {
        let halves = split_top(inner, " as ");
        let self_ty = halves.first().copied().unwrap_or(inner).trim();
        let self_ty = strip_impl_header(self_ty);
        let ty = TypeName::parse(self_ty);
        out.receiver_path = Some(
            strip_generics_everywhere(peel_refs(self_ty))
                .trim()
                .to_string(),
        );
        out.receiver = Some(ty.name);
        out.receiver_generic_args = ty.generic_args;
        out.is_dyn = ty.is_dyn;
        out.closure_span = ty.brace_span;
        if let Some(trait_text) = halves.last().filter(|_| halves.len() > 1) {
            let trait_name = TypeName::parse(trait_text).name;
            if !trait_name.is_empty() {
                out.trait_ = Some(trait_name);
            }
        }
    }

    let mut tail = CalleePath::default();
    parse_unqualified(rest, &mut tail);
    out.segments = tail.segments;
    out.generic_args = tail.generic_args;
}

fn parse_unqualified(text: &str, out: &mut CalleePath) {
    let text = text.trim();
    if text.is_empty() {
        return;
    }
    let mut names: Vec<String> = Vec::new();
    let mut generics: Vec<Vec<String>> = Vec::new();
    let mut impl_receiver: Option<TypeName> = None;

    for token in split_top(text, "::") {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        if token.starts_with('<') && token.ends_with('>') && !names.is_empty() {
            let inner = token
                .get(1..token.len().saturating_sub(1))
                .unwrap_or("")
                .trim();
            if inner.starts_with("impl") {
                // `slice::<impl [String]>::sort_by`: an inherent-impl self type,
                // not generic arguments.
                if !inner.starts_with("impl at ") {
                    impl_receiver = Some(TypeName::parse(strip_impl_header(inner)));
                }
            } else if let Some(slot) = generics.last_mut() {
                *slot = split_top_trim(inner, ",")
                    .into_iter()
                    .map(str::to_string)
                    .collect();
            }
            continue;
        }
        names.push(strip_generics(token).to_string());
        generics.push(Vec::new());
    }

    out.generic_args = generics.last().cloned().unwrap_or_default();
    if let Some(ty) = impl_receiver {
        out.receiver_path = Some(ty.name.clone());
        out.receiver = Some(ty.name);
        out.receiver_generic_args = ty.generic_args;
        out.is_dyn = ty.is_dyn;
        out.closure_span = ty.brace_span;
    } else if names.len() >= 2 {
        let idx = names.len().saturating_sub(2);
        if let Some(candidate) = names.get(idx).filter(|name| is_type_shaped(name)) {
            {
                let ty = TypeName::parse(candidate);
                out.receiver_path = Some(names.get(..=idx).unwrap_or_default().join("::"));
                out.receiver = Some(if ty.name.is_empty() {
                    candidate.clone()
                } else {
                    ty.name
                });
                out.is_dyn = ty.is_dyn;
                out.closure_span = ty.brace_span;
                let own = generics.get(idx).cloned().unwrap_or_default();
                out.receiver_generic_args = if own.is_empty() { ty.generic_args } else { own };
            }
        }
    }
    out.segments = names;
}

/// `impl Trait for Ty` / `impl Ty` → the self type; anything else unchanged.
fn strip_impl_header(text: &str) -> &str {
    let text = text.trim();
    let Some(after) = text.strip_prefix("impl ") else {
        return text;
    };
    split_top(after, " for ").last().map_or(after, |s| s.trim())
}

/// A type's name plus the pieces the model narrows on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypeName {
    /// Last `::` segment, generic arguments stripped, references peeled.
    pub name: String,
    /// Generic arguments of that segment, in order, verbatim.
    pub generic_args: Vec<String>,
    /// The type is `dyn Tr` (possibly behind references).
    pub is_dyn: bool,
    /// The complete brace form when the type is one (`{closure@..}`), if it
    /// carries an `@` span.
    pub brace_span: Option<String>,
}

impl TypeName {
    /// Decompose a type as MIR prints it. Never fails and never panics.
    ///
    /// References, raw pointers and lifetimes are peeled (`&'a mut HashMap<K, V>`
    /// → `HashMap`), `dyn Tr + Send` yields `Tr`, a qualified `<T as Tr>::Assoc`
    /// yields `Assoc`, and a brace form is normalised to its kind
    /// (`{closure@f.rs:1:1: 1:2}` → `{closure}`, `{async fn body of sub()}` →
    /// `{async fn body}`).
    #[must_use]
    pub fn parse(ty: &str) -> Self {
        let mut out = Self::default();
        let mut t = peel_refs(ty);
        if let Some(rest) = t.strip_prefix("dyn ") {
            out.is_dyn = true;
            t = split_top(rest, "+").first().map_or(rest, |s| s.trim());
        }
        let t = peel_refs(t);
        if t.is_empty() {
            return out;
        }
        if t.starts_with('{') {
            if t.contains('@') {
                out.brace_span = Some(t.to_string());
            }
            out.name = normalize_brace(t);
            return out;
        }
        if t.starts_with('<') {
            // `<T as IntoIterator>::IntoIter` — the associated item is the name.
            let Some(close) = matching_angle(t) else {
                out.name = t.to_string();
                return out;
            };
            let rest = t
                .get(close.saturating_add(1)..)
                .unwrap_or("")
                .trim_start()
                .trim_start_matches("::");
            if rest.is_empty() {
                let inner = t.get(1..close).unwrap_or("").trim();
                let head = split_top(inner, " as ").first().map_or(inner, |s| s.trim());
                if head == t {
                    out.name = t.to_string();
                    return out;
                }
                return Self::parse(head);
            }
            return Self::parse(rest);
        }
        if t.starts_with('(') || t.starts_with('[') {
            // Tuple or slice: there is no nameable last segment.
            out.name = t.to_string();
            return out;
        }
        let tokens = split_top(t, "::");
        let mut name = String::new();
        let mut args: Vec<String> = Vec::new();
        for token in tokens {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            if token.starts_with('<') && token.ends_with('>') && !name.is_empty() {
                let inner = token
                    .get(1..token.len().saturating_sub(1))
                    .unwrap_or("")
                    .trim();
                args = split_top_trim(inner, ",")
                    .into_iter()
                    .map(str::to_string)
                    .collect();
                continue;
            }
            name = strip_generics(token).to_string();
            args = generic_args_of(token)
                .into_iter()
                .map(str::to_string)
                .collect();
        }
        out.name = name;
        out.generic_args = args;
        out
    }
}

/// `{closure@f.rs:1:1: 1:2}` → `{closure}`; `{async fn body of sub()}` →
/// `{async fn body}`; anything else is returned unchanged.
fn normalize_brace(t: &str) -> String {
    let inner = t
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(t);
    let kind = inner
        .split_once('@')
        .map_or(inner, |(head, _)| head)
        .split(" of ")
        .next()
        .unwrap_or(inner)
        .trim();
    format!("{{{kind}}}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(text: &str) -> CalleePath {
        CalleePath::parse(text)
    }

    fn segs(text: &str) -> Vec<String> {
        p(text).segments
    }

    #[test]
    fn an_arrow_does_not_close_an_angle_bracket() {
        assert_eq!(matching_angle("<dyn Fn() -> String as Foo>"), Some(26));
        let path = p("<Box<dyn Fn() -> String> as Deref>::deref");
        assert_eq!(path.receiver.as_deref(), Some("Box"));
        assert_eq!(path.trait_.as_deref(), Some("Deref"));
        assert_eq!(path.segments, vec!["deref".to_string()]);
    }

    #[test]
    fn plain_paths_keep_all_segments() {
        assert_eq!(segs("SystemTime::now"), vec!["SystemTime", "now"]);
        assert_eq!(
            segs("std::time::SystemTime::now"),
            vec!["std", "time", "SystemTime", "now"]
        );
        assert_eq!(segs("wall_clock_secs"), vec!["wall_clock_secs"]);
    }

    #[test]
    fn the_receiver_of_a_plain_path_is_the_penultimate_type_segment() {
        assert_eq!(p("SystemTime::now").receiver.as_deref(), Some("SystemTime"));
        assert_eq!(p("AtomicU64::load").receiver.as_deref(), Some("AtomicU64"));
        assert_eq!(
            p("autumn_harvest::chrono::Utc::now").receiver.as_deref(),
            Some("Utc")
        );
        assert_eq!(
            p("autumn_harvest::WorkflowContext::execute_activity_raw")
                .receiver
                .as_deref(),
            Some("WorkflowContext")
        );
        // A module segment is not a receiver.
        assert_eq!(p("std::env::var").receiver, None);
        assert_eq!(p("var").receiver, None);
    }

    #[test]
    fn turbofish_arguments_are_kept_verbatim_and_stripped_from_segments() {
        let path = p("Option::<u64>::map::<String, {closure@f.rs:1:1: 1:2}>");
        assert_eq!(path.segments, vec!["Option", "map"]);
        assert_eq!(path.receiver.as_deref(), Some("Option"));
        assert_eq!(path.receiver_generic_args, vec!["u64".to_string()]);
        assert_eq!(
            path.generic_args,
            vec!["String".to_string(), "{closure@f.rs:1:1: 1:2}".to_string()]
        );
    }

    #[test]
    fn local_key_with_closure_turbofish() {
        let path = p("LocalKey::<RefCell<u64>>::with::<{closure@s.rs:16:21: 16:24}, u64>");
        assert_eq!(path.segments, vec!["LocalKey", "with"]);
        assert_eq!(path.receiver.as_deref(), Some("LocalKey"));
        assert_eq!(path.receiver_generic_args, vec!["RefCell<u64>".to_string()]);
        assert_eq!(
            path.generic_args,
            vec!["{closure@s.rs:16:21: 16:24}".to_string(), "u64".to_string()]
        );
        assert!(path.ends_with_path("LocalKey::with"));
        assert!(path.ends_with_path("with"));
        assert!(!path.ends_with_path("Key::with"));
    }

    #[test]
    fn qualified_calls_split_self_type_and_trait() {
        let path = p("<HashMap<String, u64> as IntoIterator>::into_iter");
        assert_eq!(path.segments, vec!["into_iter"]);
        assert_eq!(path.receiver.as_deref(), Some("HashMap"));
        assert_eq!(path.trait_.as_deref(), Some("IntoIterator"));
        assert_eq!(
            path.receiver_generic_args,
            vec!["String".to_string(), "u64".to_string()]
        );
        assert!(!path.is_dyn);
    }

    #[test]
    fn references_are_peeled_off_the_receiver() {
        let path = p("<&HashMap<String, u64> as IntoIterator>::into_iter");
        assert_eq!(path.receiver.as_deref(), Some("HashMap"));
        let path = p("<&mut Vec<u64> as DerefMut>::deref_mut");
        assert_eq!(path.receiver.as_deref(), Some("Vec"));
    }

    #[test]
    fn nested_qualified_self_types_resolve_to_the_associated_item() {
        let path = p("<<T as IntoIterator>::IntoIter as Iterator>::collect::<Vec<(String, u32)>>");
        assert_eq!(path.segments, vec!["collect"]);
        assert_eq!(path.receiver.as_deref(), Some("IntoIter"));
        assert_eq!(path.trait_.as_deref(), Some("Iterator"));
        assert_eq!(
            path.generic_args,
            vec!["Vec<(String, u32)>".to_string()],
            "the comma inside the tuple must not split the argument list"
        );
    }

    #[test]
    fn dyn_receivers_are_flagged_and_named_by_their_trait() {
        let path = p("<dyn Src as Src>::next");
        assert_eq!(path.segments, vec!["next"]);
        assert_eq!(path.receiver.as_deref(), Some("Src"));
        assert_eq!(path.trait_.as_deref(), Some("Src"));
        assert!(path.is_dyn);
    }

    #[test]
    fn a_box_of_dyn_is_not_itself_dyn() {
        let path = p("<Box<dyn Fn()> as FnOnce<()>>::call_once");
        assert_eq!(path.receiver.as_deref(), Some("Box"));
        assert!(
            !path.is_dyn,
            "the receiver type is Box, not the trait object"
        );
    }

    #[test]
    fn closure_receivers_normalise_and_keep_their_span() {
        let path = p("<{closure@f.rs:16:21: 16:24} as FnOnce<(u64,)>>::call_once");
        assert_eq!(path.receiver.as_deref(), Some("{closure}"));
        assert_eq!(
            path.closure_span.as_deref(),
            Some("{closure@f.rs:16:21: 16:24}")
        );
        assert_eq!(path.segments, vec!["call_once"]);
    }

    #[test]
    fn async_fn_bodies_normalise_to_their_kind() {
        let path = p("<{async fn body of sub()} as Future>::poll");
        assert_eq!(path.receiver.as_deref(), Some("{async fn body}"));
        assert_eq!(path.trait_.as_deref(), Some("Future"));
        assert_eq!(path.segments, vec!["poll"]);
        assert_eq!(path.closure_span, None, "no @ span in this form");
    }

    #[test]
    fn span_impl_headers_leave_the_receiver_unresolved() {
        let path = p("<impl at src/lib.rs:5:1: 5:15>::next");
        assert_eq!(path.segments, vec!["next"]);
        assert_eq!(
            path.receiver, None,
            "the self type is only recoverable from the source file (D7)"
        );
        assert_eq!(path.text, "<impl at src/lib.rs:5:1: 5:15>::next");
    }

    #[test]
    fn inherent_impl_markers_are_not_generic_arguments() {
        let path = p("slice::<impl [String]>::sort_by::<{closure@f.rs:33:15: 33:21}>");
        assert_eq!(path.segments, vec!["slice", "sort_by"]);
        assert_eq!(path.receiver.as_deref(), Some("[String]"));
        assert_eq!(
            path.generic_args,
            vec!["{closure@f.rs:33:15: 33:21}".to_string()]
        );
        assert!(path.ends_with_path("sort_by"));
    }

    #[test]
    fn impl_for_headers_name_the_self_type() {
        let path = p("_::<impl _::_serde::Deserialize<'de> for NotificationDecision>::deserialize");
        assert_eq!(path.receiver.as_deref(), Some("NotificationDecision"));
        assert!(path.ends_with_path("deserialize"));
    }

    #[test]
    fn lifetime_only_generics_are_kept_verbatim() {
        let path = p("core::fmt::rt::Argument::<'_>::new_display::<u64>");
        assert_eq!(
            path.segments,
            vec!["core", "fmt", "rt", "Argument", "new_display"]
        );
        assert_eq!(path.receiver.as_deref(), Some("Argument"));
        assert_eq!(path.receiver_generic_args, vec!["'_".to_string()]);
        assert_eq!(path.generic_args, vec!["u64".to_string()]);
    }

    #[test]
    fn ctx_calls_keep_their_receiver_and_turbofish() {
        let path = p("WorkflowContext::side_effect::<u64, {closure@f.rs:180:41: 180:43}>");
        assert_eq!(path.receiver.as_deref(), Some("WorkflowContext"));
        assert_eq!(path.last_segment(), "side_effect");
        assert_eq!(path.generic_args.len(), 2);
    }

    #[test]
    fn suffix_matching_is_segment_wise_not_textual() {
        let path = p("std::time::SystemTime::now");
        assert!(path.ends_with_path("SystemTime::now"));
        assert!(path.ends_with_path("now"));
        assert!(path.ends_with_path("std::time::SystemTime::now"));
        assert!(!path.ends_with_path("Time::now"));
        assert!(
            path.ends_with_path("::now::"),
            "empty segments in a rule path are ignored, not matched"
        );
        assert!(!path.ends_with_path(""));
        assert!(
            !path.ends_with_path("other::std::time::SystemTime::now"),
            "a rule longer than the callee cannot match"
        );
    }

    #[test]
    fn parsing_never_panics_on_hostile_input() {
        for text in [
            "",
            "   ",
            "<",
            "<<<",
            ">>>",
            "::",
            "a::",
            "::a",
            "<T as>",
            "<T as Tr>",
            "Foo<",
            "{closure@",
            "<impl at ",
            "Ünïcödé::méthode",
            "<dyn Fn() -> ",
        ] {
            let path = CalleePath::parse(text);
            assert_eq!(path.text, text.trim());
        }
    }

    #[test]
    fn type_names_peel_wrappers_and_lifetimes() {
        assert_eq!(TypeName::parse("&'a mut HashMap<K, V>").name, "HashMap");
        assert_eq!(TypeName::parse("*const u64").name, "u64");
        assert_eq!(
            TypeName::parse("std::collections::HashSet<u8>").name,
            "HashSet"
        );
        assert_eq!(TypeName::parse("dyn Src + Send").name, "Src");
        assert!(TypeName::parse("dyn Src + Send").is_dyn);
        assert_eq!(TypeName::parse("()").name, "()");
        assert_eq!(TypeName::parse("[String]").name, "[String]");
        assert_eq!(
            TypeName::parse("std::collections::BTreeMap<String, u32>").generic_args,
            vec!["String".to_string(), "u32".to_string()]
        );
    }
}
