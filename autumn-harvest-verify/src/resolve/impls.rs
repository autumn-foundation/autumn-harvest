//! Reading back what MIR left in the source: `<impl at FILE:L:C: L:C>` headers,
//! `unsafe extern` declarations and generic parameter lists.
//!
//! Stable MIR prints an inherent or trait impl block as a *span*, not as a
//! path: `<impl at src/lib.rs:240:1: 240:30>::steps`. The only way back to
//! "`Plan::steps` for `HashSet<String>`" is to open the file and read the
//! header the span points at — which is exactly what [`SourceIndex`] does. The
//! span is anchored on the `impl` keyword, so the header text is recovered by
//! scanning from (line, column) to the block's opening brace; that is more
//! robust than a token-span comparison and needs no extra dependency.
//!
//! `syn` is used for the two questions a span cannot answer: which functions
//! are **foreign** (an `unsafe extern "C"` fn has no MIR body and must become an
//! `ffi` boundary rather than a silent "clean"), and what each generic
//! function's **type parameters** are called, in declaration order (the
//! turbofish is positional).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use super::subst::split_top;

/// One `impl` block header, as written in the source.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImplHeader {
    /// Self type, verbatim (`HashSet<String>`, `Wrapper<T>`).
    pub self_ty: String,
    /// Trait being implemented, verbatim (`Plan`, `Jitter`), or `None` for an inherent impl.
    pub trait_: Option<String>,
    /// The impl block's own generic parameter names, in declaration order.
    pub generics: Vec<String>,
}

/// Source files indexed for impl-header, foreign-fn and generics lookups.
#[derive(Debug, Default)]
pub struct SourceIndex {
    files: BTreeMap<String, Vec<String>>,
    /// Names of functions declared in an `extern` block (no MIR body exists).
    pub foreign_fns: BTreeSet<String>,
    /// Function name → generic parameter names, in declaration order.
    pub fn_generics: BTreeMap<String, Vec<String>>,
}

impl SourceIndex {
    /// Read every `file` (each relative to one of `roots`, first match wins) and
    /// index it. Unreadable or unparsable files are skipped: a source file the
    /// analyzer cannot read is a resolution boundary later, never an error here.
    #[must_use]
    pub fn build(roots: &[PathBuf], files: &BTreeSet<String>) -> Self {
        let mut index = Self::default();
        let mut expanded: BTreeSet<String> = files.clone();
        // A file with an `extern` block but no impl and no closure is invisible
        // to MIR spans; its siblings are cheap to read and usually contain it.
        for file in files {
            if let Some(parent) = Path::new(file).parent() {
                for sibling in siblings(roots, parent) {
                    expanded.insert(sibling);
                }
            }
        }
        for file in &expanded {
            let Some(text) = read_relative(roots, file) else {
                continue;
            };
            index.absorb_syn(&text);
            index
                .files
                .insert(file.clone(), text.lines().map(str::to_string).collect());
        }
        index
    }

    /// The `impl` header whose `impl` keyword sits at `line`:`column` (both 1-based).
    #[must_use]
    pub fn impl_header_at(&self, file: &str, line: usize, column: usize) -> Option<ImplHeader> {
        let lines = self.files.get(file)?;
        let first = lines.get(line.checked_sub(1)?)?;
        let start: String = first.chars().skip(column.checked_sub(1)?).collect();
        let mut header = String::new();
        header.push_str(&start);
        let mut at = line;
        // Scan forward to the block's opening brace (or `;` for an impl-less item).
        while brace_start(&header).is_none() && at < lines.len() && header.len() < 4096 {
            let Some(next) = lines.get(at) else { break };
            header.push(' ');
            header.push_str(next.trim());
            at = at.saturating_add(1);
        }
        let end = brace_start(&header).unwrap_or(header.len());
        parse_impl_header(header.get(..end).unwrap_or(&header))
    }

    fn absorb_syn(&mut self, text: &str) {
        let Ok(file) = syn::parse_file(text) else {
            return;
        };
        self.absorb_items(&file.items, 0);
    }

    fn absorb_items(&mut self, items: &[syn::Item], depth: u32) {
        if depth > 8 {
            return;
        }
        for item in items {
            match item {
                syn::Item::Fn(item) => {
                    let generics = generic_names(&item.sig.generics);
                    if !generics.is_empty() {
                        self.fn_generics
                            .insert(item.sig.ident.to_string(), generics);
                    }
                }
                syn::Item::Mod(item) => {
                    if let Some((_, items)) = &item.content {
                        self.absorb_items(items, depth.saturating_add(1));
                    }
                }
                syn::Item::ForeignMod(item) => {
                    for foreign in &item.items {
                        if let syn::ForeignItem::Fn(f) = foreign {
                            self.foreign_fns.insert(f.sig.ident.to_string());
                        }
                    }
                }
                syn::Item::Impl(item) => {
                    for member in &item.items {
                        if let syn::ImplItem::Fn(f) = member {
                            let mut generics = generic_names(&item.generics);
                            generics.extend(generic_names(&f.sig.generics));
                            if !generics.is_empty() {
                                self.fn_generics
                                    .entry(f.sig.ident.to_string())
                                    .or_insert(generics);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

fn generic_names(generics: &syn::Generics) -> Vec<String> {
    generics
        .params
        .iter()
        .filter_map(|param| match param {
            syn::GenericParam::Type(ty) => Some(ty.ident.to_string()),
            syn::GenericParam::Const(c) => Some(c.ident.to_string()),
            syn::GenericParam::Lifetime(_) => None,
        })
        .collect()
}

/// Every `.rs` file directly under `dir` (relative to whichever root holds it).
fn siblings(roots: &[PathBuf], dir: &Path) -> Vec<String> {
    for root in roots {
        let full = root.join(dir);
        let Ok(entries) = std::fs::read_dir(&full) else {
            continue;
        };
        let mut out: Vec<String> = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "rs")
                && let Some(name) = path.file_name().and_then(|n| n.to_str())
            {
                out.push(dir.join(name).to_string_lossy().into_owned());
            }
        }
        if !out.is_empty() {
            return out;
        }
    }
    Vec::new()
}

fn read_relative(roots: &[PathBuf], file: &str) -> Option<String> {
    let candidate = Path::new(file);
    if candidate.is_absolute() {
        return std::fs::read_to_string(candidate).ok();
    }
    for root in roots {
        if let Ok(text) = std::fs::read_to_string(root.join(candidate)) {
            return Some(text);
        }
    }
    // A file named relative to a *parent* of the root (or vice versa) still
    // resolves when the tail matches.
    for root in roots {
        if let Some(name) = candidate.file_name() {
            let direct = root.join(name);
            if let Ok(text) = std::fs::read_to_string(direct) {
                return Some(text);
            }
        }
    }
    None
}

/// Byte index of the `{` that opens the impl block, ignoring generic brackets.
fn brace_start(header: &str) -> Option<usize> {
    let mut angle = 0i32;
    for (at, c) in header.char_indices() {
        match c {
            '<' => angle = angle.saturating_add(1),
            '>' => angle = angle.saturating_sub(1),
            '{' if angle == 0 => return Some(at),
            _ => {}
        }
    }
    None
}

/// `impl<T: Score> Score for Wrapper<T> where ...` → the three fields.
fn parse_impl_header(header: &str) -> Option<ImplHeader> {
    let header = header.trim();
    let rest = header.strip_prefix("impl")?;
    let rest = rest.trim_start();
    let (generics, rest) = if rest.starts_with('<') {
        let close = matching_angle(rest)?;
        let inner = rest.get(1..close)?;
        (
            split_top(inner, ',')
                .into_iter()
                .filter_map(generic_param_name)
                .collect(),
            rest.get(close.saturating_add(1)..)?,
        )
    } else {
        (Vec::new(), rest)
    };
    let body = rest.split(" where ").next().unwrap_or(rest).trim();
    let halves = split_top_str(body, " for ");
    let (trait_, self_ty) = if halves.len() >= 2 {
        (
            halves.first().map(|s| (*s).trim().to_string()),
            halves.last().map_or(body, |s| s.trim()).to_string(),
        )
    } else {
        (None, body.to_string())
    };
    Some(ImplHeader {
        self_ty: self_ty.trim().trim_end_matches('{').trim().to_string(),
        trait_: trait_.filter(|t| !t.is_empty()),
        generics,
    })
}

/// `T: Score` → `T`; `'a` → `None`; `const N: usize` → `N`.
fn generic_param_name(param: &str) -> Option<String> {
    let param = param.trim();
    if param.starts_with('\'') {
        return None;
    }
    let param = param.strip_prefix("const ").unwrap_or(param).trim_start();
    let name = param
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .next()
        .unwrap_or_default();
    (!name.is_empty()).then(|| name.to_string())
}

fn matching_angle(text: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (at, c) in text.char_indices() {
        match c {
            '<' => depth = depth.saturating_add(1),
            '>' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(at);
                }
            }
            _ => {}
        }
    }
    None
}

/// Split on a top-level multi-character separator (`" for "`).
fn split_top_str<'a>(text: &'a str, separator: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut at = 0usize;
    let mut previous = ' ';
    while at < text.len() {
        let Some(rest) = text.get(at..) else { break };
        let Some(c) = rest.chars().next() else { break };
        match c {
            '<' | '(' | '[' => depth = depth.saturating_add(1),
            '>' if previous == '-' => {}
            '>' | ')' | ']' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 && rest.starts_with(separator) {
            if let Some(piece) = text.get(start..at) {
                out.push(piece);
            }
            at = at.saturating_add(separator.len());
            start = at;
            previous = c;
            continue;
        }
        previous = c;
        at = at.saturating_add(c.len_utf8());
    }
    if let Some(piece) = text.get(start..) {
        out.push(piece);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trait_impl_header_is_split_into_trait_and_self_type() {
        let header = parse_impl_header("impl Plan for HashSet<String>").expect("header");
        assert_eq!(header.trait_.as_deref(), Some("Plan"));
        assert_eq!(header.self_ty, "HashSet<String>");
        assert!(header.generics.is_empty());
    }

    #[test]
    fn a_generic_impl_records_its_parameters() {
        let header = parse_impl_header("impl<T: Score> Score for Wrapper<T>").expect("header");
        assert_eq!(header.generics, vec!["T".to_string()]);
        assert_eq!(header.self_ty, "Wrapper<T>");
        assert_eq!(header.trait_.as_deref(), Some("Score"));
    }

    #[test]
    fn an_inherent_impl_has_no_trait() {
        let header = parse_impl_header("impl Ctx").expect("header");
        assert_eq!(header.trait_, None);
        assert_eq!(header.self_ty, "Ctx");
    }

    #[test]
    fn a_where_clause_is_not_part_of_the_self_type() {
        let header =
            parse_impl_header("impl<T> Score for Wrapper<T> where T: Score").expect("header");
        assert_eq!(header.self_ty, "Wrapper<T>");
    }

    #[test]
    fn for_inside_generic_arguments_is_not_the_separator() {
        let header = parse_impl_header("impl Namer for Box<dyn Fn() -> u32>").expect("header");
        assert_eq!(header.trait_.as_deref(), Some("Namer"));
        assert_eq!(header.self_ty, "Box<dyn Fn() -> u32>");
    }
}
