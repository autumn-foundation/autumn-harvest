//! Generic substitution: what `[T := HashMap<String, u32>]` means to a callee path.
//!
//! MIR prints a generic body **once**, with its type parameters still spelled
//! `T`, and prints the concrete type only at the call site
//! (`pairs::<HashMap<String, u32>>`, or in the declared type of the argument
//! local). Without threading that binding into the callee, the Order source
//! inside `pairs` — `<T as IntoIterator>::into_iter` — is invisible (D6/D7).
//!
//! Two mechanisms, in this order:
//!
//! 1. **Unification** of each callee parameter's declared type against the
//!    call site's actual argument type, plus the callee's return type against
//!    the destination local's type. This is order-free and needs no knowledge
//!    of the parameter list, so it works even when the source file that
//!    declares the callee was never read.
//! 2. **The turbofish, by elimination**: whatever unification left unbound is
//!    bound positionally from `f::<A, B>`, in the callee's declared parameter
//!    order (recovered with `syn`) or, failing that, in order of first
//!    appearance in the body.
//!
//! [`Substitution::apply`] rewrites a path at the **token** level, never as a
//! substring: `T` must not match inside `Tuple`, and nothing inside a
//! `{closure@file.rs:1:1: 1:2}` brace form may be rewritten at all.

use std::collections::BTreeMap;

use crate::util::{is_bare_ident, split_generic, split_top_trim};

/// A generic substitution: type-parameter name → concrete type text.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Substitution(pub BTreeMap<String, String>);

impl Substitution {
    /// An empty substitution.
    #[must_use]
    pub const fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// True when nothing is bound.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Bind `param` to `ty`, unless it is already bound or the binding is a no-op.
    pub fn bind(&mut self, param: &str, ty: &str) {
        let ty = ty.trim();
        if param.is_empty() || ty.is_empty() || param == ty {
            return;
        }
        self.0
            .entry(param.to_string())
            .or_insert_with(|| ty.to_string());
    }

    /// The concrete type bound to `param`, if any.
    #[must_use]
    pub fn get(&self, param: &str) -> Option<&str> {
        self.0.get(param).map(String::as_str)
    }

    /// A stable key for memoisation.
    #[must_use]
    pub fn key(&self) -> String {
        let mut out = String::new();
        for (k, v) in &self.0 {
            out.push_str(k);
            out.push('=');
            out.push_str(v);
            out.push(';');
        }
        out
    }

    /// Rewrite every type parameter in `path` under this substitution.
    ///
    /// Token-level: an identifier is replaced only when it is the *whole*
    /// identifier, and never inside a `{...}` brace form (a closure/coroutine
    /// span, whose file path is not a type).
    #[must_use]
    pub fn apply(&self, path: &str) -> String {
        if self.0.is_empty() {
            return path.to_string();
        }
        let mut out = String::with_capacity(path.len());
        let mut brace_depth = 0u32;
        let bytes = path.as_bytes();
        let mut at = 0usize;
        while at < path.len() {
            let Some(&byte) = bytes.get(at) else { break };
            if byte == b'{' {
                brace_depth = brace_depth.saturating_add(1);
            } else if byte == b'}' {
                brace_depth = brace_depth.saturating_sub(1);
            }
            if brace_depth == 0 && is_ident_start(byte) {
                let end = ident_end(path, at);
                let word = path.get(at..end).unwrap_or_default();
                match self.0.get(word) {
                    Some(replacement) => out.push_str(replacement),
                    None => out.push_str(word),
                }
                at = end;
                continue;
            }
            out.push(char::from(byte));
            at = at.saturating_add(1);
        }
        out
    }
}

const fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn ident_end(text: &str, from: usize) -> usize {
    let bytes = text.as_bytes();
    let mut at = from;
    while let Some(&byte) = bytes.get(at) {
        if byte.is_ascii_alphanumeric() || byte == b'_' {
            at = at.saturating_add(1);
        } else {
            break;
        }
    }
    at
}

/// Unify a callee-side declared type against the call site's actual type,
/// binding any type parameter it meets.
///
/// `is_param` decides whether a bare identifier on the callee side is a type
/// parameter (and may therefore bind) or a concrete type (which must match).
pub fn unify(pattern: &str, actual: &str, is_param: &dyn Fn(&str) -> bool, out: &mut Substitution) {
    unify_at(pattern, actual, is_param, out, 0);
}

fn unify_at(
    pattern: &str,
    actual: &str,
    is_param: &dyn Fn(&str) -> bool,
    out: &mut Substitution,
    depth: u32,
) {
    if depth > 8 {
        return;
    }
    let pattern = strip_lifetimes(pattern.trim());
    let actual = strip_lifetimes(actual.trim());
    if pattern.is_empty() || actual.is_empty() || pattern == actual {
        return;
    }
    // Peel matching reference/pointer prefixes, so `&mut T` unifies with `&mut u64`.
    for prefix in ["&mut ", "&", "*const ", "*mut "] {
        if let (Some(p), Some(a)) = (pattern.strip_prefix(prefix), actual.strip_prefix(prefix)) {
            unify_at(p, a, is_param, out, depth.saturating_add(1));
            return;
        }
    }
    if is_bare_ident(pattern) {
        if is_param(pattern) && !is_placeholder(actual) {
            out.bind(pattern, actual);
        }
        return;
    }
    let (pattern_base, pattern_args) = split_generic(pattern);
    let (actual_base, actual_args) = split_generic(actual);
    if pattern_base != actual_base || pattern_args.len() != actual_args.len() {
        return;
    }
    for (p, a) in pattern_args.iter().zip(&actual_args) {
        unify_at(p, a, is_param, out, depth.saturating_add(1));
    }
}

/// A type the analyzer must not bind to: an unresolved parameter on the *actual*
/// side would make the substitution a lie.
fn is_placeholder(ty: &str) -> bool {
    is_bare_ident(ty) && ty.len() <= 2 && ty.starts_with(|c: char| c.is_ascii_uppercase())
}

/// A leading lifetime, stripped without touching the `&` it qualifies.
///
/// [`crate::util::peel_refs`] would take the reference off too, which
/// [`unify`] must not do before it has matched the two sides' reference
/// prefixes against each other.
fn strip_lifetimes(ty: &str) -> &str {
    let ty = ty.trim();
    ty.strip_prefix('\'')
        .and_then(|rest| rest.split_once(' '))
        .map_or(ty, |(_, rest)| rest.trim())
}

/// The turbofish arguments written on the last segment of `path`, verbatim.
#[must_use]
pub fn turbofish(path: &str) -> Vec<String> {
    let path = path.trim();
    // The item's own generic arguments are the last top-level `::<...>` group.
    let mut best: Option<(usize, usize)> = None;
    let mut depth = 0i32;
    let bytes = path.as_bytes();
    let mut previous = b' ';
    for (at, c) in path.char_indices() {
        let byte = *bytes.get(at).unwrap_or(&b' ');
        match c {
            '<' => {
                if depth == 0 && at >= 2 && path.get(at.saturating_sub(2)..at) == Some("::") {
                    best = Some((at, 0));
                }
                depth = depth.saturating_add(1);
            }
            '>' if previous != b'-' => {
                depth = depth.saturating_sub(1);
                if depth == 0
                    && let Some((open, _)) = best
                    && open < at
                {
                    best = Some((open, at));
                }
            }
            '(' | '[' | '{' => depth = depth.saturating_add(1),
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
        previous = byte;
    }
    let Some((open, close)) = best else {
        return Vec::new();
    };
    if close <= open {
        return Vec::new();
    }
    let inner = path.get(open.saturating_add(1)..close).unwrap_or("");
    split_top_trim(inner, ",")
        .into_iter()
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subst(pairs: &[(&str, &str)]) -> Substitution {
        let mut s = Substitution::new();
        for (k, v) in pairs {
            s.bind(k, v);
        }
        s
    }

    #[test]
    fn apply_is_token_level_not_substring() {
        let s = subst(&[("T", "HashMap<String, u32>")]);
        assert_eq!(
            s.apply("<T as IntoIterator>::into_iter"),
            "<HashMap<String, u32> as IntoIterator>::into_iter"
        );
        assert_eq!(
            s.apply("Tuple::<Twin>::t"),
            "Tuple::<Twin>::t",
            "`T` must not match inside `Tuple`, `Twin` or a field name"
        );
        assert_eq!(
            s.apply("<<T as IntoIterator>::IntoIter as Iterator>::collect::<Vec<(String, u32)>>"),
            "<<HashMap<String, u32> as IntoIterator>::IntoIter as Iterator>::collect::<Vec<(String, u32)>>"
        );
    }

    #[test]
    fn apply_never_rewrites_inside_a_closure_span() {
        let s = subst(&[("T", "u8")]);
        assert_eq!(
            s.apply("LocalKey::<T>::with::<{closure@src/T.rs:1:1: 1:2}, T>"),
            "LocalKey::<u8>::with::<{closure@src/T.rs:1:1: 1:2}, u8>"
        );
    }

    #[test]
    fn unification_peels_references_and_recurses_into_arguments() {
        let is_param = |name: &str| name == "T" || name == "K";
        let mut out = Substitution::new();
        unify("&T", "&Wrapper<Leaf>", &is_param, &mut out);
        assert_eq!(out.get("T"), Some("Wrapper<Leaf>"));

        let mut out = Substitution::new();
        unify("&mut Vec<T>", "&mut Vec<u64>", &is_param, &mut out);
        assert_eq!(out.get("T"), Some("u64"));

        let mut out = Substitution::new();
        unify(
            "HashMap<K, u32>",
            "HashMap<String, u32>",
            &is_param,
            &mut out,
        );
        assert_eq!(out.get("K"), Some("String"));
    }

    #[test]
    fn unification_refuses_to_bind_a_parameter_to_a_parameter() {
        let is_param = |name: &str| name == "T";
        let mut out = Substitution::new();
        unify("&T", "&T", &is_param, &mut out);
        assert!(out.is_empty(), "an identity binding is not information");
    }

    #[test]
    fn the_turbofish_is_the_items_own_argument_group() {
        assert_eq!(
            turbofish("pairs::<HashMap<String, u32>>"),
            vec!["HashMap<String, u32>".to_string()]
        );
        assert_eq!(
            turbofish("LocalKey::<RefCell<u64>>::with::<{closure@s.rs:1:1: 1:2}, u64>"),
            vec!["{closure@s.rs:1:1: 1:2}".to_string(), "u64".to_string()]
        );
        assert!(turbofish("SystemTime::now").is_empty());
    }

    #[test]
    fn a_lifetime_is_stripped_but_its_reference_is_kept() {
        assert_eq!(strip_lifetimes("&'a mut T"), "&'a mut T");
        assert_eq!(strip_lifetimes("'a mut T"), "mut T");
        assert_eq!(strip_lifetimes("T"), "T");
    }
}
