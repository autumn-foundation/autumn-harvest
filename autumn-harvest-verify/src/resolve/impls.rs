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

use crate::util::{matching_angle, split_top, split_top_trim};

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

/// What the field types of one declared type hold (issue #2010).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldFacts {
    /// A field type holds `&mut`.
    pub mut_ref: bool,
    /// A field type takes a lifetime argument, as `Refs<'a>`. A shared
    /// `&'a T` does not count.
    pub lifetime: bool,
    /// Each path segment a field type names, as `Arc` and `Mutex`.
    pub names: BTreeSet<String>,
}

/// Collects the items declared in a function body, at any block depth. It
/// does not enter an item, as `absorb_items` reads each one.
#[derive(Default)]
struct BodyItems(Vec<syn::Item>);

impl<'ast> syn::visit::Visit<'ast> for BodyItems {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        self.0.push(item.clone());
    }
}

/// Collects [`FieldFacts`] from field types.
#[derive(Default)]
struct FieldScan(FieldFacts);

impl<'ast> syn::visit::Visit<'ast> for FieldScan {
    fn visit_type_reference(&mut self, reference: &'ast syn::TypeReference) {
        if reference.mutability.is_some() {
            self.0.mut_ref = true;
        }
        // The lifetime of a shared reference cannot write. Skip it.
        self.visit_type(&reference.elem);
    }

    fn visit_type_ptr(&mut self, pointer: &'ast syn::TypePtr) {
        // A `*mut` pointer writes as a `&mut` reference does.
        if pointer.mutability.is_some() {
            self.0.mut_ref = true;
        }
        self.visit_type(&pointer.elem);
    }

    fn visit_lifetime(&mut self, lifetime: &'ast syn::Lifetime) {
        if lifetime.ident != "static" {
            self.0.lifetime = true;
        }
    }

    fn visit_path_segment(&mut self, segment: &'ast syn::PathSegment) {
        self.0.names.insert(segment.ident.to_string());
        syn::visit::visit_path_segment(self, segment);
    }
}

/// Source files indexed for impl-header, foreign-fn and generics lookups.
#[derive(Debug, Default)]
pub struct SourceIndex {
    files: BTreeMap<String, Vec<String>>,
    /// Names of functions declared in an `extern` block (no MIR body exists).
    pub foreign_fns: BTreeSet<String>,
    /// Function name → generic parameter names, in declaration order.
    pub fn_generics: BTreeMap<String, Vec<String>>,
    /// The structs, enums, unions and type aliases the sources declare, each
    /// by its module path and name, such as `helpers::Clock`.
    ///
    /// The module path comes from the file path and any inline `mod` around
    /// the item. It is the full path from the target root, as MIR prints a
    /// local type that it does not trim to its bare name.
    pub type_paths: BTreeSet<String>,
    /// The self type name of each `impl Future for T` in the sources, as
    /// `T` with no generics.
    pub future_impls: BTreeSet<String>,
    /// Struct, enum and union name → what its field types hold. Two types of
    /// one name share an entry, which can only add to it.
    pub type_fields: BTreeMap<String, FieldFacts>,
    /// Source file → why it could not be indexed.
    ///
    /// A file MIR named that the source roots could not produce, or that `syn`
    /// rejected: either way the impl headers, foreign-fn declarations and
    /// generic parameter lists it holds are invisible, and every resolution that
    /// needed them must become a named boundary rather than a silent "clean".
    pub unreadable: BTreeMap<String, String>,
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
        // A `mod x;` declaration names a file that MIR may never name, such as
        // a file with types only. Each one read adds its own to the queue.
        // Each entry carries its module path. A `#[path]` file has a path that
        // does not give its module, so the declaring module gives it. MIR can
        // name that file too, so a file is read once per module path.
        let mut queue: Vec<(String, Vec<String>)> = expanded
            .iter()
            .map(|f| (f.clone(), file_module(f)))
            .collect();
        let mut seen: BTreeSet<(String, Vec<String>)> = queue.iter().cloned().collect();
        while let Some((file, module)) = queue.pop() {
            let file = &file;
            let Some(text) = read_relative(roots, file) else {
                // Only the files MIR *named* are worth reporting; the siblings
                // are a best-effort widening and their absence means nothing.
                if files.contains(file) {
                    index.unreadable.insert(
                        file.clone(),
                        "the source file was not found under any source root".to_string(),
                    );
                }
                continue;
            };
            match index.absorb_syn(&text, &module) {
                Some(children) => {
                    for (child, path) in module_files(roots, file, &children) {
                        let mut child_module = module.clone();
                        child_module.extend(path);
                        // A `#[path]` cycle grows the module path. The cap ends it.
                        if child_module.len() <= MODULE_DEPTH
                            && seen.insert((child.clone(), child_module.clone()))
                        {
                            queue.push((child, child_module));
                        }
                    }
                }
                None if files.contains(file) => {
                    index.unreadable.insert(
                        file.clone(),
                        "the source file could not be parsed as Rust".to_string(),
                    );
                }
                None => {}
            }
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
            .or_else(|| self.derive_header_at(file, line, column))
    }

    /// The impl a `#[derive(..)]` entry stands for.
    ///
    /// A derived impl has no `impl` keyword to point at, so rustc anchors its
    /// span on the **derive macro's own path** inside the attribute:
    /// `#[derive(Debug, Serialize, Deserialize, Clone)]` at `25:41: 25:46` is
    /// `impl Clone for <the item the attribute is on>`. Reading it back matters
    /// because the derived body *is* in the dump — skipping the header is what
    /// made `<Order as Clone>::clone` an `external-crate-body` boundary on a
    /// type defined three lines away.
    fn derive_header_at(&self, file: &str, line: usize, column: usize) -> Option<ImplHeader> {
        let lines = self.files.get(file)?;
        let at = lines.get(line.checked_sub(1)?)?;
        let tail: String = at.chars().skip(column.checked_sub(1)?).collect();
        let trait_: String = tail
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if trait_.is_empty() || !trait_.starts_with(|c: char| c.is_ascii_uppercase()) {
            return None;
        }
        // The item the attribute is attached to: the next `struct`/`enum`/`union`
        // declaration at or below the attribute.
        let self_ty = lines
            .iter()
            .skip(line.checked_sub(1)?)
            .take(32)
            .find_map(|text| item_name(text))?;
        Some(ImplHeader {
            self_ty,
            trait_: Some(trait_),
            generics: Vec::new(),
        })
    }

    /// `None` when `syn` rejected the file (its `extern` blocks and generic
    /// parameter lists are then invisible). Otherwise each `mod x;` it
    /// declares, as the path of inline modules around it and its name.
    fn absorb_syn(&mut self, text: &str, module: &[String]) -> Option<Vec<Declared>> {
        let file = syn::parse_file(text).ok()?;
        self.absorb_items(&file.items, 0, module);
        let mut children = Vec::new();
        declared_module_files(&file.items, &mut Vec::new(), &mut children);
        Some(children)
    }

    /// Record what the field types of the type `name` hold.
    fn scan_fields<'f>(&mut self, name: &syn::Ident, fields: impl Iterator<Item = &'f syn::Field>) {
        self.scan_types(name, fields.map(|field| &field.ty));
    }

    /// Record what the types `types` of the type `name` hold.
    fn scan_types<'t>(&mut self, name: &syn::Ident, types: impl Iterator<Item = &'t syn::Type>) {
        let mut scan = FieldScan::default();
        for ty in types {
            syn::visit::Visit::visit_type(&mut scan, ty);
        }
        let facts = self.type_fields.entry(name.to_string()).or_default();
        facts.mut_ref |= scan.0.mut_ref;
        facts.lifetime |= scan.0.lifetime;
        facts.names.extend(scan.0.names);
    }

    /// Record the type `name`, declared in `module`.
    fn declare(&mut self, module: &[String], name: &syn::Ident) {
        let mut path = module.to_vec();
        path.push(name.to_string());
        self.type_paths.insert(path.join("::"));
    }

    /// Absorb the items declared in the body of the function `name`. A
    /// workflow can declare a wrapper type there.
    fn absorb_body(
        &mut self,
        block: &syn::Block,
        name: &syn::Ident,
        depth: u32,
        module: &[String],
    ) {
        let mut found = BodyItems::default();
        syn::visit::Visit::visit_block(&mut found, block);
        if !found.0.is_empty() {
            let mut inner = module.to_vec();
            inner.push(name.to_string());
            self.absorb_items(&found.0, depth.saturating_add(1), &inner);
        }
    }

    fn absorb_items(&mut self, items: &[syn::Item], depth: u32, module: &[String]) {
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
                    self.absorb_body(&item.block, &item.sig.ident, depth, module);
                }
                syn::Item::Struct(item) => {
                    self.declare(module, &item.ident);
                    self.scan_fields(&item.ident, item.fields.iter());
                }
                syn::Item::Enum(item) => {
                    self.declare(module, &item.ident);
                    let fields = item.variants.iter().flat_map(|v| v.fields.iter());
                    self.scan_fields(&item.ident, fields);
                }
                syn::Item::Union(item) => {
                    self.declare(module, &item.ident);
                    self.scan_fields(&item.ident, item.fields.named.iter());
                }
                syn::Item::Type(item) => {
                    // A wrapper can reach interior-mutable state through an
                    // alias, so the alias target counts as a field.
                    self.declare(module, &item.ident);
                    self.scan_types(&item.ident, std::iter::once(&*item.ty));
                }
                syn::Item::Mod(item) => {
                    if let Some((_, items)) = &item.content {
                        let mut inner = module.to_vec();
                        inner.push(item.ident.to_string());
                        self.absorb_items(items, depth.saturating_add(1), &inner);
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
                    if let Some(name) = future_self_type(item) {
                        self.future_impls.insert(name);
                    }
                    for member in &item.items {
                        if let syn::ImplItem::Fn(f) = member {
                            let mut generics = generic_names(&item.generics);
                            generics.extend(generic_names(&f.sig.generics));
                            if !generics.is_empty() {
                                self.fn_generics
                                    .entry(f.sig.ident.to_string())
                                    .or_insert(generics);
                            }
                            self.absorb_body(&f.block, &f.sig.ident, depth, module);
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

/// The self type name of an `impl Future for T` block, with no generics.
/// An impl for a shared reference, `&T`, gives `&T`.
fn future_self_type(item: &syn::ItemImpl) -> Option<String> {
    let (_, trait_path, _) = item.trait_.as_ref()?;
    if trait_path.segments.last()?.ident != "Future" {
        return None;
    }
    let (prefix, self_ty) = match item.self_ty.as_ref() {
        syn::Type::Reference(r) if r.mutability.is_none() => ("&", r.elem.as_ref()),
        other => ("", other),
    };
    let syn::Type::Path(self_ty) = self_ty else {
        return None;
    };
    Some(format!("{prefix}{}", self_ty.path.segments.last()?.ident))
}

/// The deepest module path the source index follows.
const MODULE_DEPTH: usize = 16;

/// Each `mod x;` in `items`, as the path of inline modules around it and its
/// name. A `mod x { .. }` with a body adds its name to the path.
fn declared_module_files(items: &[syn::Item], path: &mut Vec<String>, out: &mut Vec<Declared>) {
    for item in items {
        let syn::Item::Mod(item) = item else {
            continue;
        };
        path.push(item.ident.to_string());
        match &item.content {
            Some((_, inner)) => declared_module_files(inner, path, out),
            None => out.push(Declared {
                path: path.clone(),
                file: path_attribute(&item.attrs),
            }),
        }
        path.pop();
    }
}

/// A body-less `mod x;` declaration.
struct Declared {
    /// The module path below the declaring file, inline modules included.
    path: Vec<String>,
    /// The `#[path = "..."]` value, when the declaration has one.
    file: Option<String>,
}

/// The value of a `#[path = "..."]` attribute.
fn path_attribute(attrs: &[syn::Attribute]) -> Option<String> {
    attrs.iter().find_map(|attr| {
        let syn::Meta::NameValue(meta) = &attr.meta else {
            return None;
        };
        let syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(value),
            ..
        }) = &meta.value
        else {
            return None;
        };
        meta.path.is_ident("path").then(|| value.value())
    })
}

/// The files of the modules `children` that `file` declares and that exist.
///
/// A target root and a `mod.rs` hold their modules in their own directory.
/// Any other file holds them in a directory named after it, as rustc reads
/// `src/a.rs` then `src/a/b.rs`. Each module is `x.rs` or `x/mod.rs`. In an
/// unknown layout the file can be either kind, so both directories are read.
/// An extra file only adds to the index.
///
/// A `#[path]` value replaces the `x.rs` name. At the top of a file it is
/// relative to the directory of that file, as rustc reads it.
fn module_files(
    roots: &[PathBuf],
    file: &str,
    children: &[Declared],
) -> Vec<(String, Vec<String>)> {
    let path = Path::new(file);
    let Some(parent) = path.parent() else {
        return Vec::new();
    };
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    let bases = if matches!(stem, "mod" | "lib" | "main") {
        vec![parent.to_path_buf()]
    } else if layout_module(&file_parts(file)).is_empty() {
        vec![parent.join(stem), parent.to_path_buf()]
    } else {
        vec![parent.join(stem)]
    };
    let mut out = Vec::new();
    for child in children {
        let Some((name, inline)) = child.path.split_last() else {
            continue;
        };
        let top = [parent.to_path_buf()];
        let bases = match &child.file {
            Some(_) if inline.is_empty() => &top[..],
            _ => &bases[..],
        };
        for base in bases {
            let dir = inline.iter().fold(base.clone(), |dir, part| dir.join(part));
            let candidates = child.file.as_ref().map_or_else(
                || {
                    vec![
                        dir.join(format!("{name}.rs")),
                        dir.join(name).join("mod.rs"),
                    ]
                },
                |named| vec![dir.join(named)],
            );
            for candidate in candidates {
                let candidate = candidate.to_string_lossy().into_owned();
                if read_relative(roots, &candidate).is_some() {
                    out.push((candidate, child.path.clone()));
                }
            }
        }
    }
    out
}

/// The module path a source file stands for, from its path.
///
/// `src/a/b.rs` and `src/a/b/mod.rs` give `a::b`. A target root, such as
/// `src/lib.rs`, `src/bin/x.rs` or `examples/x.rs`, gives no segment. A file
/// in another layout reads as a target root. A type of a submodule there
/// then reads as foreign, which can only add a boundary.
fn file_module(file: &str) -> Vec<String> {
    let parts = file_parts(file);
    let mut module: Vec<String> = layout_module(&parts)
        .iter()
        .map(|p| (*p).to_string())
        .collect();
    if module
        .last()
        .is_some_and(|p| matches!(p.as_str(), "mod" | "lib" | "main"))
    {
        module.pop();
    }
    module
}

/// The `/` parts of a source path, with no `.rs`.
fn file_parts(file: &str) -> Vec<&str> {
    file.trim_end_matches(".rs")
        .split('/')
        .filter(|p| !p.is_empty())
        .collect()
}

/// The parts of a source path below its target root.
fn layout_module<'p>(parts: &'p [&'p str]) -> &'p [&'p str] {
    // `x.rs` and `x/main.rs` are the root of target `x`. `x/y.rs` is `y`.
    let below_target = |rest: &'p [&'p str]| rest.get(1..).unwrap_or_default();
    if let Some(at) = parts.iter().rposition(|p| *p == "src") {
        let rest = parts.get(at.saturating_add(1)..).unwrap_or_default();
        if rest.first() == Some(&"bin") {
            return below_target(rest.get(1..).unwrap_or_default());
        }
        return rest;
    }
    if let Some(at) = parts
        .iter()
        .rposition(|p| matches!(*p, "examples" | "tests" | "benches"))
    {
        return below_target(parts.get(at.saturating_add(1)..).unwrap_or_default());
    }
    &[]
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
/// `pub struct NotificationEvent {` → `NotificationEvent`.
fn item_name(text: &str) -> Option<String> {
    let mut rest = text.trim_start();
    for prefix in ["pub(crate) ", "pub(super) ", "pub ", "#[", "///", "//"] {
        if let Some(stripped) = rest.strip_prefix(prefix) {
            if prefix.starts_with('#') || prefix.starts_with('/') {
                return None;
            }
            rest = stripped.trim_start();
        }
    }
    let rest = ["struct ", "enum ", "union "]
        .iter()
        .find_map(|kind| rest.strip_prefix(kind))?
        .trim_start();
    let name: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then_some(name)
}

fn parse_impl_header(header: &str) -> Option<ImplHeader> {
    let header = header.trim();
    let rest = header.strip_prefix("impl")?;
    let rest = rest.trim_start();
    let (generics, rest) = if rest.starts_with('<') {
        let close = matching_angle(rest)?;
        let inner = rest.get(1..close)?;
        (
            split_top_trim(inner, ",")
                .into_iter()
                .filter_map(generic_param_name)
                .collect(),
            rest.get(close.saturating_add(1)..)?,
        )
    } else {
        (Vec::new(), rest)
    };
    let body = rest.split(" where ").next().unwrap_or(rest).trim();
    let halves = split_top(body, " for ");
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

#[cfg(test)]
mod tests {

    #[test]
    fn a_file_path_gives_its_module_path() {
        let none = Vec::<String>::new();
        assert_eq!(super::file_module("src/lib.rs"), none);
        assert_eq!(super::file_module("src/a/b.rs"), ["a", "b"]);
        assert_eq!(super::file_module("crates/x/src/a/mod.rs"), ["a"]);
        assert_eq!(super::file_module("src/bin/tool.rs"), none);
        assert_eq!(super::file_module("src/bin/tool/main.rs"), none);
        assert_eq!(super::file_module("src/bin/tool/cli.rs"), ["cli"]);
        assert_eq!(
            super::file_module("autumn-harvest/examples/ctx_info.rs"),
            none
        );
        assert_eq!(super::file_module("examples/demo/flows.rs"), ["flows"]);
        assert_eq!(super::file_module("flow.rs"), none);
    }

    use super::*;

    #[test]
    fn a_module_file_named_by_a_path_attribute_is_indexed() {
        // `#[path]` moves the file away from `state.rs`. Its types must be
        // indexed, or a capture of them reads as read-only.
        let dir = tempfile::tempdir().expect("temp dir");
        let write = |file: &str, text: &str| {
            let path = dir.path().join(file);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
            std::fs::write(path, text).expect("write");
        };
        write(
            "src/lib.rs",
            "#[path = \"types/state.rs\"]\nmod state;\nmod outer { #[path = \"inner.rs\"] mod inner; }\n",
        );
        write(
            "src/types/state.rs",
            "pub struct Shared(std::sync::Arc<std::sync::Mutex<u64>>);\n",
        );
        write(
            "src/outer/inner.rs",
            "pub struct Deep(std::cell::Cell<u64>);\n",
        );
        let files = BTreeSet::from(["src/lib.rs".to_string()]);
        let index = SourceIndex::build(&[dir.path().to_path_buf()], &files);
        assert!(index.type_fields.contains_key("Shared"), "{index:?}");
        assert!(index.type_paths.contains("state::Shared"), "{index:?}");
        assert!(index.type_paths.contains("outer::inner::Deep"), "{index:?}");
    }

    #[test]
    fn a_path_attribute_file_that_mir_names_gets_its_declared_module() {
        // MIR names the moved file too, so it is seen first by its own path.
        // Its declared module path must still be indexed.
        let dir = tempfile::tempdir().expect("temp dir");
        let write = |file: &str, text: &str| {
            let path = dir.path().join(file);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
            std::fs::write(path, text).expect("write");
        };
        write("src/lib.rs", "#[path = \"types/state.rs\"]\nmod state;\n");
        write("src/types/state.rs", "pub struct Shared(u64);\n");
        // A file that declares itself must not loop.
        write("src/again.rs", "#[path = \"again.rs\"]\nmod again;\n");
        let files = BTreeSet::from([
            "src/lib.rs".to_string(),
            "src/types/state.rs".to_string(),
            "src/again.rs".to_string(),
        ]);
        let index = SourceIndex::build(&[dir.path().to_path_buf()], &files);
        assert!(index.type_paths.contains("state::Shared"), "{index:?}");
    }

    #[test]
    fn a_type_declared_inside_a_function_is_indexed() {
        // A workflow can declare its wrapper in its own body.
        let mut index = SourceIndex::default();
        index.absorb_syn(
            "fn wf() { struct Shared(std::sync::Arc<std::sync::Mutex<u64>>); }\n\
             impl Ctx { fn run(&self) { struct Inner(std::cell::Cell<u8>); } }\n",
            &[],
        );
        let names = |ty: &str| index.type_fields.get(ty).map(|f| f.names.clone());
        assert!(
            names("Shared").is_some_and(|n| n.contains("Mutex")),
            "{index:?}"
        );
        assert!(
            names("Inner").is_some_and(|n| n.contains("Cell")),
            "{index:?}"
        );
        assert!(index.type_paths.contains("wf::Shared"), "{index:?}");
    }

    #[test]
    fn a_type_alias_records_what_its_target_holds() {
        // `Wrapper` reaches the mutex through the alias `Shared`. The
        // writable fixpoint needs the alias target to see that.
        let mut index = SourceIndex::default();
        index.absorb_syn(
            "type Shared = std::sync::Arc<std::sync::Mutex<u64>>;\nstruct Wrapper(Shared);\n",
            &[],
        );
        let names = |ty: &str| index.type_fields.get(ty).map(|f| f.names.clone());
        assert!(
            names("Wrapper").is_some_and(|n| n.contains("Shared")),
            "{index:?}"
        );
        assert!(
            names("Shared").is_some_and(|n| n.contains("Mutex")),
            "{index:?}"
        );
    }

    #[test]
    fn a_raw_mut_pointer_field_can_write() {
        // `*mut` writes as `&mut` does. A `*const` field cannot write.
        let mut index = SourceIndex::default();
        index.absorb_syn(
            "struct Ptr(*mut u64);\nstruct Step(Ptr);\nstruct Read(*const u64);\n",
            &[],
        );
        let facts = |ty: &str| index.type_fields.get(ty).cloned().unwrap_or_default();
        assert!(facts("Ptr").mut_ref, "{index:?}");
        assert!(facts("Step").names.contains("Ptr"), "{index:?}");
        assert!(!facts("Read").mut_ref, "{index:?}");
    }

    #[test]
    fn a_trait_impl_header_is_split_into_trait_and_self_type() {
        let header = parse_impl_header("impl Plan for HashSet<String>").expect("header");
        assert_eq!(header.trait_.as_deref(), Some("Plan"));
        assert_eq!(header.self_ty, "HashSet<String>");
        assert_eq!(header.generics, [] as [std::string::String; 0]);
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
