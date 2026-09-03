//! Balanced-delimiter text helpers for the *paths and types* MIR prints.
//!
//! Four of the analyzer's modules — [`crate::model::callee`],
//! [`crate::model::matcher`], [`crate::resolve`] and its `subst`/`impls`
//! submodules — all have to answer the same handful of questions about a
//! printed path or type: where does this segment end, what are its generic
//! arguments, what is its last `::` segment, is this rule a suffix of that
//! path. Each question has exactly one implementation, and it lives here.
//!
//! # The one rule everything obeys
//!
//! Nothing splits on a bare byte. `rustc --emit=mir` prints
//! `<<T as IntoIterator>::IntoIter as Iterator>::collect::<Vec<(String, u32)>>`
//! and `Box<dyn Fn() -> String>`, which contain ` as `, `,` and `>` at nesting
//! depths a naive `split` cannot see. Every helper here tracks `<>`, `()`,
//! `[]` and `{}` depth, and treats the `>` of an `->` arrow as part of the
//! arrow rather than as a closing bracket.
//!
//! # Tolerance
//!
//! Every function is total: unbalanced, truncated or empty input yields a
//! best-effort answer or `None`, never a panic. A path the analyzer cannot
//! decompose has to become a named boundary, and it cannot become one from
//! inside a panicking helper.
//!
//! # Not to be confused with [`crate::mir::lexer`]
//!
//! That module scans *statement text* and therefore also has to skip
//! double-quoted string literals (MIR assert messages contain `{}`, `,` and
//! quotes). This module scans type and path text, where no string literals
//! occur, and is `&str`-separator based rather than byte based.

/// Split `text` on every occurrence of `sep` that is not nested inside
/// `<>`, `()`, `[]` or `{}`. Pieces are returned verbatim, empties included.
pub fn split_top<'a>(text: &'a str, sep: &str) -> Vec<&'a str> {
    if sep.is_empty() {
        return vec![text];
    }
    let bytes = text.as_bytes();
    let mut parts: Vec<&'a str> = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut chars = text.char_indices();
    while let Some((idx, c)) = chars.next() {
        match c {
            '<' | '(' | '[' | '{' => depth = depth.saturating_add(1),
            '>' if !is_arrow_tail(bytes, idx) => depth = depth.saturating_sub(1),
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth != 0 {
            continue;
        }
        let Some(rest) = text.get(idx..) else {
            continue;
        };
        if rest.starts_with(sep) {
            parts.push(text.get(start..idx).unwrap_or(""));
            start = idx.saturating_add(sep.len());
            // Skip the separator's remaining characters.
            let mut consumed = c.len_utf8();
            while consumed < sep.len() {
                match chars.next() {
                    Some((_, skipped)) => consumed = consumed.saturating_add(skipped.len_utf8()),
                    None => break,
                }
            }
        }
    }
    parts.push(text.get(start..).unwrap_or(""));
    parts
}

/// [`split_top`] with every piece trimmed and every empty piece dropped.
pub fn split_top_trim<'a>(text: &'a str, sep: &str) -> Vec<&'a str> {
    let mut out = split_top(text, sep);
    for piece in &mut out {
        *piece = piece.trim();
    }
    out.retain(|piece| !piece.is_empty());
    out
}

/// True when the `>` at `idx` is the tail of an `->` arrow, and so does not
/// close a generic-argument list.
fn is_arrow_tail(bytes: &[u8], idx: usize) -> bool {
    idx > 0 && bytes.get(idx.saturating_sub(1)) == Some(&b'-')
}

/// Byte index of the `>` that closes the `<` at the start of `text`.
///
/// `->` is skipped, so `<{closure@f.rs:1:1: 1:2} as FnOnce<()>>::call_once`,
/// `Box<dyn Fn() -> String>` and `impl<F: Fn() -> u32> Tr for X` all scan
/// correctly. Returns `None` when `text` does not start with `<` or is
/// unbalanced.
pub fn matching_angle(text: &str) -> Option<usize> {
    if !text.starts_with('<') {
        return None;
    }
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    for (idx, c) in text.char_indices() {
        match c {
            '<' => depth = depth.saturating_add(1),
            '>' if !is_arrow_tail(bytes, idx) => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
    }
    None
}

/// Byte index of the first depth-0 `<` in `token`, if any.
pub fn top_angle(token: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (idx, c) in token.char_indices() {
        match c {
            '<' => {
                if depth == 0 {
                    return Some(idx);
                }
                depth = depth.saturating_add(1);
            }
            '(' | '[' | '{' => depth = depth.saturating_add(1),
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    None
}

/// Peel `&`, `&mut`, `*const`, `*mut`, `mut ` and lifetimes off a type.
pub fn peel_refs(ty: &str) -> &str {
    let mut t = ty.trim();
    for _ in 0..16 {
        let next = if let Some(r) = t.strip_prefix("&mut ") {
            r
        } else if let Some(r) = t.strip_prefix("*const ") {
            r
        } else if let Some(r) = t.strip_prefix("*mut ") {
            r
        } else if let Some(r) = t.strip_prefix("mut ") {
            r
        } else if let Some(r) = t.strip_prefix('&') {
            r
        } else if t.starts_with('\'') {
            match t.split_once(' ') {
                Some((_, rest)) => rest,
                None => return t,
            }
        } else {
            return t;
        };
        t = next.trim_start();
    }
    t
}

/// Peel the four transparent smart-pointer wrappers — `Box`, `Arc`, `Rc`,
/// `Pin` — and any references, repeatedly.
///
/// `Box<dyn Jitter>` → `dyn Jitter`; `&Arc<Mutex<Vec<u64>>>` → `Mutex<Vec<u64>>`.
/// A wrapper is transparent for both questions the analyzer asks of a type
/// (which trait object is inside, and whether a static is ambient), so both
/// callers peel with this one function.
pub fn peel_containers(ty: &str) -> &str {
    const TRANSPARENT: [&str; 4] = ["Box", "Arc", "Rc", "Pin"];
    let mut current = peel_refs(ty).trim();
    for _ in 0..8 {
        let Some(open) = current.find('<') else {
            return current;
        };
        let base = current.get(..open).unwrap_or("").trim();
        if !TRANSPARENT.contains(&last_segment(base)) {
            return current;
        }
        let Some(inner) = current
            .get(open.saturating_add(1)..current.len().saturating_sub(1))
            .map(str::trim)
        else {
            return current;
        };
        current = peel_refs(split_top_trim(inner, ",").first().copied().unwrap_or(inner)).trim();
    }
    current
}

/// The segment name with any trailing generic-argument group removed
/// (`Foo<A, B>` → `Foo`).
pub fn strip_generics(token: &str) -> &str {
    let token = token.trim();
    let end = top_angle(token).unwrap_or(token.len());
    token
        .get(..end)
        .unwrap_or(token)
        .trim()
        .trim_end_matches("::")
}

/// The generic arguments written directly on `token` (`Foo<A, B>` → `["A", "B"]`).
///
/// Only a group that closes at the very end of `token` counts, so a
/// half-written `Foo<A` yields nothing rather than a truncated argument.
pub fn generic_args_of(token: &str) -> Vec<&str> {
    let token = token.trim();
    let Some(start) = top_angle(token) else {
        return Vec::new();
    };
    let Some(inner) = token.get(start.saturating_add(1)..token.len().saturating_sub(1)) else {
        return Vec::new();
    };
    split_top_trim(inner, ",")
}

/// `Wrapper<A, B>` → (`Wrapper`, `["A", "B"]`); a non-generic type → (itself, `[]`).
pub fn split_generic(ty: &str) -> (&str, Vec<&str>) {
    let ty = ty.trim();
    if !ty.ends_with('>') {
        return (ty, Vec::new());
    }
    top_angle(ty).map_or((ty, Vec::new()), |open| {
        (ty.get(..open).unwrap_or(ty).trim(), generic_args_of(ty))
    })
}

/// Remove every `<..>` group from a path, keeping the segments
/// (`pairs::<HashMap<String, u32>>` → `pairs`).
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

/// The last `::` segment of a path (the path itself when it has only one).
pub fn last_segment(path: &str) -> &str {
    path.rsplit("::").next().unwrap_or(path)
}

/// Split `a::b::c` into `("a::b", "c")`, or `None` when there is no `::`.
pub fn split_last(path: &str) -> Option<(&str, &str)> {
    let at = path.rfind("::")?;
    Some((path.get(..at)?, path.get(at.saturating_add(2)..)?))
}

/// A path's `::` segments, trimmed, with empties dropped.
pub fn segments(path: &str) -> Vec<&str> {
    split_top_trim(path, "::")
}

/// True when `needle`'s `::` segments are a suffix of `have`.
///
/// This is *the* path-matching rule of the model and of
/// [`crate::model::callee::CalleePath::ends_with_path`]: MIR prints rustc's
/// **trimmed** def-paths, so a rule keyed on a full path would match nothing,
/// and a rule is therefore always a suffix of what was printed.
pub fn is_segment_suffix<S: AsRef<str>>(needle: &str, have: &[S]) -> bool {
    let want = segments(needle);
    if want.is_empty() || want.len() > have.len() {
        return false;
    }
    let start = have.len().saturating_sub(want.len());
    have.get(start..).is_some_and(|tail| {
        tail.iter()
            .zip(&want)
            .all(|(have, want)| have.as_ref() == *want)
    })
}

/// Peel the `&`, `*mut`, `dyn ` and `<` a printed *path* can begin with, so the
/// crate root of `<&dyn tokio::Sleeper as Tr>::poll` is still `tokio`.
pub fn peel_path_head(text: &str) -> &str {
    let mut t = text.trim();
    for _ in 0..8 {
        t = peel_refs(t.trim_start());
        if let Some(rest) = t.strip_prefix('<') {
            t = rest;
            continue;
        }
        if let Some(rest) = t.strip_prefix("dyn ") {
            t = rest;
            continue;
        }
        break;
    }
    t
}

/// The leading crate identifier of an **explicitly rooted** path.
///
/// `Some` only when the leading identifier starts with a lowercase ASCII letter
/// and is followed by `::`. rustc's trimmed prints (`SystemTime::now`,
/// `String::len`) therefore have no crate root at all, which is exactly what
/// keeps a `[[trusted]]` row from matching a type named like a crate.
pub fn crate_root(text: &str) -> Option<&str> {
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

/// Collapse whitespace runs to a single space and trim.
pub fn normalize_ws(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// True when `text` is a single unqualified identifier.
pub fn is_bare_ident(text: &str) -> bool {
    !text.is_empty()
        && text.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && text.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
}

/// A name is "type-shaped" when it starts with an uppercase ASCII letter
/// (`HashMap`, `WorkflowContext`) or is a brace form (`{closure}`).
///
/// Module segments (`env`, `time`, `collections`) are lowercase by convention
/// and are therefore never mistaken for a receiver type.
pub fn is_type_shaped(name: &str) -> bool {
    name.starts_with('{') || name.chars().next().is_some_and(char::is_uppercase)
}

/// `T`, `U`, `K`, `F`, `T1`, `__S` — the spellings a type parameter takes.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitting_honours_every_nesting_kind() {
        assert_eq!(
            split_top(
                "<<T as IntoIterator>::IntoIter as Iterator>::collect",
                " as "
            ),
            vec!["<<T as IntoIterator>::IntoIter as Iterator>::collect"],
            "the ` as ` at depth 1 must not split the qualified header"
        );
        assert_eq!(
            split_top_trim("Vec<(String, u32)>, T", ","),
            vec!["Vec<(String, u32)>", "T"]
        );
        assert_eq!(
            split_top_trim("fn(&u32) -> u32, T", ","),
            vec!["fn(&u32) -> u32", "T"],
            "the `>` of `->` must not unbalance the scan"
        );
        assert_eq!(split_top_trim("", ","), Vec::<&str>::new());
    }

    #[test]
    fn matching_angle_skips_arrows_and_tolerates_garbage() {
        let text = "<{closure@f.rs:1:1: 1:2} as FnOnce<()>>::call_once";
        assert_eq!(
            text.get(..=matching_angle(text).unwrap()).unwrap().len(),
            39
        );
        // The `->` inside a generic bound is the case a naive scan gets wrong.
        let bound = "<F: Fn() -> u32> Namer for X";
        assert_eq!(
            bound.get(..=matching_angle(bound).unwrap()),
            Some("<F: Fn() -> u32>")
        );
        assert_eq!(matching_angle("no angle"), None);
        assert_eq!(matching_angle("<unbalanced"), None);
    }

    #[test]
    fn types_are_peeled_down_to_what_the_model_matches() {
        assert_eq!(peel_refs("&'a mut HashMap<K, V>"), "HashMap<K, V>");
        assert_eq!(peel_refs("*const u8"), "u8");
        assert_eq!(peel_containers("std::boxed::Box<dyn Jitter>"), "dyn Jitter");
        assert_eq!(peel_containers("&Arc<Mutex<Vec<u64>>>"), "Mutex<Vec<u64>>");
        assert_eq!(
            peel_containers("Vec<u8>"),
            "Vec<u8>",
            "Vec is not transparent"
        );
    }

    #[test]
    fn generics_are_split_off_a_segment() {
        assert_eq!(strip_generics("Foo<A, B>"), "Foo");
        assert_eq!(generic_args_of("Foo<A, Bar<B>>"), vec!["A", "Bar<B>"]);
        assert_eq!(generic_args_of("Foo"), Vec::<&str>::new());
        assert_eq!(split_generic("Wrapper<A, B>"), ("Wrapper", vec!["A", "B"]));
        assert_eq!(split_generic("u64"), ("u64", Vec::new()));
        assert_eq!(
            strip_generics_everywhere("with_page_cursor::<R, F>::promoted[0]"),
            "with_page_cursor::promoted[0]"
        );
    }

    #[test]
    fn a_rule_path_is_a_suffix_of_the_printed_path() {
        let printed = ["std", "collections", "HashMap", "iter"];
        assert!(is_segment_suffix("HashMap::iter", &printed));
        assert!(is_segment_suffix("iter", &printed));
        assert!(!is_segment_suffix("BTreeMap::iter", &printed));
        assert!(
            !is_segment_suffix("collections::iter", &printed),
            "a suffix is contiguous, not a subsequence"
        );
        assert!(!is_segment_suffix("", &printed));
    }

    #[test]
    fn only_an_explicitly_rooted_path_has_a_crate_root() {
        assert_eq!(crate_root("tokio::time::sleep"), Some("tokio"));
        assert_eq!(crate_root("SystemTime::now"), None, "a type is not a crate");
        assert_eq!(crate_root("format"), None);
        assert_eq!(
            crate_root(peel_path_head("<&dyn tokio::S as T>::p")),
            Some("tokio")
        );
    }

    #[test]
    fn segment_and_ident_shapes() {
        assert_eq!(last_segment("a::b::c"), "c");
        assert_eq!(last_segment("c"), "c");
        assert_eq!(split_last("a::b::c"), Some(("a::b", "c")));
        assert_eq!(split_last("c"), None);
        assert!(is_bare_ident("T1"));
        assert!(!is_bare_ident("Vec<u8>"));
        assert!(is_type_shaped("HashMap"));
        assert!(is_type_shaped("{closure}"));
        assert!(!is_type_shaped("env"));
        assert!(looks_like_type_param("T"));
        assert!(looks_like_type_param("__S"));
        assert!(!looks_like_type_param("HashMap"));
        assert_eq!(normalize_ws("  a   b \n c "), "a b c");
    }
}
