//! Symbol resolution: modules, imports, globs, and the one resolver.
//!
//! A struct, a fn and a type alias each resolve through `resolve_symbol`.
//! It reads the site of a reference: the file text that holds it and the
//! position in that text. The module of a site decides which import and
//! which definition a name reaches.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use super::aliases::{alias_scope, canonical_type, use_leaves};
use super::mask::{code_only, masked_source};
use super::py::{self, balanced, brace, paren};
use super::query::raw_reader;
use super::state::{self, AliasIndex, B, Site, StructBlock, StructIndex, Text, TypeAlias};
use super::syntax::split_expression;
use super::tables::{
    BYTE_PARAMETER, BYTE_SLICE, EXTRACTOR_NAMES, PUBLIC_ITEM, SCANNED_CRATES, STD_ROOTS,
    TRUSTED_GLOBS, std_type, wire_types,
};

/// An `impl`, `trait` or `mod` block: its start, its end and its name.
pub type Owner = (usize, usize, String);

/// One fn definition: its start, owner, inline modules and visibility.
#[derive(Clone, Debug)]
pub struct FnDef {
    pub start: usize,
    pub owner: Option<String>,
    pub modules: String,
    pub public: bool,
}

/// One plain `use` leaf: its scope, its path and where the `use` starts.
pub type UseBinding = ((usize, usize), String, usize);

/// One `impl` or `trait` block, with its keyword and type parameters.
pub type OwnerBlock = (usize, usize, String, BTreeSet<String>);

orc_cache!(BLOCK_OWNERS: (usize, &'static str) => Rc<Vec<Owner>>);
orc_cache!(FUNCTION_INDEX: usize => Rc<HashMap<String, Vec<FnDef>>>);
orc_cache!(USE_BINDINGS: usize => Rc<HashMap<String, Vec<UseBinding>>>);
orc_cache!(GLOB_IMPORTS: usize => Rc<Vec<UseBinding>>);
orc_cache!(OWNER_BLOCKS: usize => Rc<Vec<OwnerBlock>>);
orc_cache!(BLOCK_START: (usize, String) => isize);
orc_cache!(CANONICAL_PARAMS: (String, usize) => Rc<Params>);
orc_cache!(PARTS: (usize, usize) => Option<Rc<Parts>>);

thread_local! {
    static DEFINED_STRUCTS: std::cell::OnceCell<Rc<StructIndex>> =
        const { std::cell::OnceCell::new() };
    static CRATE_ALIASES: std::cell::OnceCell<Rc<AliasIndex>> =
        const { std::cell::OnceCell::new() };
}

/// A definition's place: its crate, then its module path.
pub fn location(parts: &[&str]) -> String {
    parts
        .iter()
        .filter(|part| !part.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join("::")
}

/// The place of the audited source's own module.
pub fn local_place() -> String {
    let (crate_name, module) = state::local();
    location(&[&crate_name, &module])
}

/// The names of a path, with the generic arguments of every segment cut.
///
/// `Decoder::<Config>::` is `["Decoder"]`, and `a::B<T>::c` is `["a", "B",
/// "c"]`. Brackets are matched by `split_expression`, so a `::` inside
/// generic arguments does not split. Every struct, function and alias
/// resolver reads a qualifier through this one step.
pub fn path_segments(path: &str) -> Vec<String> {
    let mut names = Vec::new();
    for part in split_expression(path.trim(), &["::"], true, None) {
        let part = part.trim();
        if !part.is_empty() && !part.starts_with('<') {
            // The cut needs a `<`, so a part without one stays as it is.
            let name = if part.contains('<') {
                py::sub(r"(?s)\s*<.*$", "", part)
            } else {
                part.to_string()
            };
            names.push(name.trim().to_string());
        }
    }
    names
}

/// Whether a path `qualifier`, written in module `origin`, reaches `place`.
///
/// `place` and `origin` are a crate, then a module path, as `location`
/// builds them. A qualifier that starts with `crate` names a module path
/// from the root of the crate of `origin`. One that starts with a scanned
/// crate's name names a path from that crate's root. `self` names `origin`,
/// and each `super` one module up from it. Any other first name is
/// relative, as in the 2018 edition, so `a::b` reaches `origin::a::b` only.
/// Every form must match `place` whole. A first name that is no module
/// inside `origin` names an external crate, as `models::Filter` does with no
/// import. So it matches nothing, and its item is unresolved. An imported
/// first name has already been expanded by `expanded_path`.
/// `resolve_symbol` reads every qualifier through this step.
pub fn qualified_match(qualifier: &[String], place: &str, origin: &str) -> bool {
    let whole: Vec<&str> = place.split("::").collect();
    let mut base: Vec<&str> = origin.split("::").collect();
    let mut wanted: Vec<&str> = qualifier
        .iter()
        .map(String::as_str)
        .filter(|part| !part.is_empty())
        .collect();
    if wanted.first() == Some(&"crate") {
        let mut expected = vec![base[0]];
        expected.extend(&wanted[1..]);
        return whole == expected;
    }
    if wanted
        .first()
        .is_some_and(|head| SCANNED_CRATES.contains(head))
    {
        return whole == wanted;
    }
    if matches!(wanted.first(), Some(&"self" | &"super")) {
        if wanted[0] == "self" {
            wanted.remove(0);
        }
        while wanted.first() == Some(&"super") {
            if base.len() < 2 {
                return false;
            }
            base.pop();
            wanted.remove(0);
        }
        base.extend(wanted);
        return whole == base;
    }
    // Any other first name is relative, as in the 2018 edition. It must be a
    // module inside `origin`. Otherwise it names an external crate, which
    // matches nothing.
    base.extend(wanted);
    whole == base
}

/// `(start, end, name)` for each `impl`, `trait` or `mod` block in `source`.
///
/// For `impl`, the name is the type the block is for, as in `impl Trait for
/// Type` or `impl<T> Type<T>`. For `trait` and `mod`, it is the declared
/// name.
pub fn block_owners(source: &Text, keyword: &'static str) -> Rc<Vec<Owner>> {
    // The blocks are kept sorted, as each reader of the original sorts them
    // or reads them in any order.
    state::cached(&BLOCK_OWNERS, (state::key(source), keyword), || {
        let mut found = block_owners_uncached(source, keyword);
        found.sort();
        Rc::new(found)
    })
}

fn block_owners_uncached(source: &str, keyword: &str) -> Vec<Owner> {
    let mut found = Vec::new();
    for head in py::finditer(&format!(r"\b{keyword}\b"), source) {
        let brace_at = py::find(source, "{", head.end());
        let stop = py::find(source, ";", head.end());
        if brace_at < 0 || (0 <= stop && stop < brace_at) {
            continue;
        }
        let brace_at = py::to_usize(brace_at);
        let mut header = source[head.end()..brace_at].to_string();
        if keyword == "impl" {
            header = py::sub(r"^\s*<[^{]*?>", "", &header);
            header = header
                .split_once(" for ")
                .map_or_else(|| header.clone(), |(_, after)| after.to_string());
            header = py::sub(r"(?s)\bwhere\b.*", "", &header);
            if py::matches(r"\s*&?\s*\[\s*u8\s*\]", &header) {
                let end = brace_at + brace(&source[brace_at..]).len();
                found.push((brace_at, end, BYTE_SLICE.to_string()));
                continue;
            }
        }
        let name = py::rmatch(
            r"\s*&?\s*(?:dyn\s+)?(?:[A-Za-z_]\w*\s*::\s*)*([A-Za-z_]\w*)",
            &header,
        );
        if let Some(name) = name {
            let end = brace_at + brace(&source[brace_at..]).len();
            found.push((brace_at, end, name.g(1).to_string()));
        }
    }
    found
}

/// The innermost owner block that holds `position`, or `None`.
pub fn owner_at(owners: &[Owner], position: usize) -> Option<String> {
    owners
        .iter()
        .filter(|(start, end, _)| *start < position && position < *end)
        .map(|(start, _, name)| (*start, name.clone()))
        .max()
        .map(|(_, name)| name)
}

/// The owners of every `impl` and `trait` block, `impl` first.
fn impl_and_trait_owners(source: &Text) -> Vec<Owner> {
    let mut owners = (*block_owners(source, "impl")).clone();
    owners.extend(block_owners(source, "trait").iter().cloned());
    owners
}

/// The inline module names that hold `position`, outermost first.
///
/// `modules` is sorted, as `block_owners` keeps it.
fn inline_modules(modules: &[Owner], position: usize) -> Vec<String> {
    modules
        .iter()
        .filter(|(start, end, _)| *start < position && position < *end)
        .map(|(_, _, name)| name.clone())
        .collect()
}

/// Every definition of each function in `source`.
///
/// The owner is the type of the `impl` block or the name of the `trait`
/// that holds the definition, or `None` for a free function. `modules` is
/// the path of the inline `mod` blocks that hold it. `public` tells whether
/// the definition is public in any form. `resolve_symbol` reads this index
/// for every fn lookup in the audit.
pub fn function_index(source: &Text) -> Rc<HashMap<String, Vec<FnDef>>> {
    state::cached(&FUNCTION_INDEX, state::key(source), || {
        let owners = impl_and_trait_owners(source);
        let modules = block_owners(source, "mod");
        let mut index: HashMap<String, Vec<FnDef>> = HashMap::new();
        for found in py::finditer(r"\b(?:async )?fn ([A-Za-z_][A-Za-z_0-9]*)\s*[(<]", source) {
            // A declaration with no body, as in a trait, is no definition.
            let opener = py::find(source, "(", found.start());
            if opener < 0 {
                continue;
            }
            let opener = py::to_usize(opener);
            let after = opener + paren(&source[opener..]).len();
            let head = source[after..]
                .find([';', '{'])
                .unwrap_or(source.len() - after);
            if py::byte(source, after + head) != b'{' {
                continue;
            }
            let inline = inline_modules(&modules, found.start());
            let window = py::sl(source, found.start().saturating_sub(80), found.start());
            let public = py::found(PUBLIC_ITEM, window);
            index
                .entry(found.g(1).to_string())
                .or_default()
                .push(FnDef {
                    start: found.start(),
                    owner: owner_at(&owners, found.start()),
                    modules: inline.join("::"),
                    public,
                });
        }
        Rc::new(index)
    })
}

/// The module a site is in: its file's place, then its inline modules.
pub fn site_origin(site: &Site) -> String {
    let base = state::file_place(&site.text).unwrap_or_else(local_place);
    let Some(at) = site.at else {
        return base;
    };
    let inline = inline_modules(&block_owners(&site.text, "mod"), at);
    let mut parts: Vec<&str> = vec![&base];
    parts.extend(inline.iter().map(String::as_str));
    location(&parts)
}

/// Where `block` first starts in `text`, or -1.
pub fn block_start(text: &Text, block: &str) -> isize {
    state::cached(&BLOCK_START, (state::key(text), block.to_string()), || {
        py::find(text, block, 0)
    })
}

/// The site of `position` in `block`, a slice of the audited source.
///
/// A block that knows its start counts from it. Any other slice is found by
/// its text.
pub fn site_in(block: B<'_>, position: usize) -> Site {
    let source = state::source();
    let start = block
        .start
        .map_or_else(|| block_start(&source, block.s), py::to_isize);
    let at = (start >= 0).then(|| py::to_usize(start) + position);
    Site { text: source, at }
}

/// Every module place that holds a struct, a type alias or a function.
pub fn known_modules() -> Rc<HashSet<String>> {
    if let Some(found) = state::with_ctx(|ctx| ctx.known_modules.clone()) {
        return found;
    }
    let source = state::source();
    let (crate_name, module) = state::local();
    let mut places: HashSet<String> = HashSet::new();
    for found in struct_candidates().values() {
        places.extend(found.iter().map(|(place, _)| place.clone()));
    }
    for found in state::type_aliases().values() {
        places.extend(found.iter().map(|alias| alias.place.clone()));
    }
    for found in function_index(&source).values() {
        places.extend(
            found
                .iter()
                .map(|def| location(&[&crate_name, &module, &def.modules])),
        );
    }
    for (_, _, name) in block_owners(&source, "mod").iter() {
        places.insert(location(&[&crate_name, &module, name]));
    }
    let mut prefixes = HashSet::new();
    for place in places {
        let parts: Vec<&str> = place.split("::").collect();
        for count in 0..parts.len() {
            prefixes.insert(parts[..=count].join("::"));
        }
    }
    let made = Rc::new(prefixes);
    state::set_ctx(|ctx| ctx.known_modules = Some(Rc::clone(&made)));
    made
}

/// The struct index that `resolve_symbol` reads.
pub fn struct_candidates() -> Rc<StructIndex> {
    state::with_ctx(|ctx| ctx.structs.clone()).unwrap_or_else(defined_structs)
}

/// The names of `reference`, with an imported first name replaced by its
/// path.
///
/// The first name goes through the innermost `use` that binds it in scope
/// at `site`. So both `Filter` and `schema::Filter` resolve through an
/// import. The result is an absolute path, from `crate` or a scanned crate.
/// `None` means the import names another crate. A name with no import is
/// returned as written.
pub fn expanded_path(reference: &str, site: &Site) -> Option<Vec<String>> {
    let (segments, used_at) = written_path(reference, site);
    let Some(used_at) = used_at else {
        return Some(segments);
    };
    absolute_path(
        &segments,
        &site_origin(&Site::new(&site.text, Some(used_at))),
    )
}

/// The names of `reference` with an imported first name replaced, and the
/// import's place.
///
/// The innermost `use` that binds the first name in scope at `site`
/// decides. So `use crate::signed as axum;` makes `axum::Json` read as
/// `crate::signed::Json`. A path from `::`, `crate`, `self`, `super` or
/// `Self` has no import. The position is `None` when no import applies.
/// Symbol lookups and the extractor path trust both expand through this.
pub fn written_path(reference: &str, site: &Site) -> (Vec<String>, Option<usize>) {
    let segments = path_segments(reference);
    let Some(at) = site.at else {
        return (segments, None);
    };
    if segments.is_empty() || reference.trim().starts_with("::") {
        return (segments, None);
    }
    if matches!(segments[0].as_str(), "crate" | "self" | "super" | "Self") {
        return (segments, None);
    }
    let Some((path, used_at)) = innermost_use(&site.text, &segments[0], at) else {
        return (segments, None);
    };
    let mut expanded = path_segments(&path);
    expanded.extend(segments.into_iter().skip(1));
    (expanded, Some(used_at))
}

/// An import path from `crate` or a scanned crate, or `None` for another
/// crate.
///
/// `self` and `super` count from `origin`, the module of the `use`. A first
/// name that is a child module of `origin` is relative to it, as in a 2018
/// edition `use`. Any other first name is another crate.
pub fn absolute_path(segments: &[String], origin: &str) -> Option<Vec<String>> {
    let head = segments.first().map_or("", String::as_str);
    if head == "crate" || SCANNED_CRATES.contains(&head) {
        return Some(segments.to_vec());
    }
    let mut base: Vec<String> = origin.split("::").map(str::to_string).collect();
    if head == "self" || head == "super" {
        let mut rest: Vec<String> = if head == "self" {
            segments[1..].to_vec()
        } else {
            segments.to_vec()
        };
        while rest.first().map(String::as_str) == Some("super") {
            if base.len() < 2 {
                return None;
            }
            base.pop();
            rest.remove(0);
        }
        let mut out = vec!["crate".to_string()];
        out.extend(base.into_iter().skip(1));
        out.extend(rest);
        return Some(out);
    }
    if known_modules().contains(&format!("{origin}::{head}")) {
        let mut out = vec!["crate".to_string()];
        out.extend(base.into_iter().skip(1));
        out.extend(segments.iter().cloned());
        return Some(out);
    }
    None
}

/// The path and position of the innermost plain `use` that binds `name` at
/// `at`.
pub fn innermost_use(text: &Text, name: &str, at: usize) -> Option<(String, usize)> {
    let bindings = use_bindings(text);
    let mut found: Option<(usize, String, usize)> = None;
    for ((start, end), path, used_at) in bindings.get(name).into_iter().flatten() {
        if *start <= at && at < *end && found.as_ref().is_none_or(|best| *start >= best.0) {
            found = Some((*start, path.clone(), *used_at));
        }
    }
    found.map(|(_, path, used_at)| (path, used_at))
}

/// `(scope, path, position)` for each plain, non-glob `use` leaf, by bound
/// name.
pub fn use_bindings(code: &Text) -> Rc<HashMap<String, Vec<UseBinding>>> {
    state::cached(&USE_BINDINGS, state::key(code), || {
        let mut found: HashMap<String, Vec<UseBinding>> = HashMap::new();
        for found_use in py::finditer(r"\buse\b[^;]*;", code) {
            let scope = alias_scope(code, found_use.start());
            for (path, alias) in use_leaves(found_use.whole()) {
                if !path.ends_with('*') {
                    let binding = alias.unwrap_or_else(|| last_segment(&path).to_string());
                    found
                        .entry(binding)
                        .or_default()
                        .push((scope, path, found_use.start()));
                }
            }
        }
        Rc::new(found)
    })
}

/// The text after the last `::` of a path, as `rsplit("::", 1)[-1]`.
pub fn last_segment(path: &str) -> &str {
    path.rsplit_once("::").map_or(path, |(_, last)| last)
}

/// `(scope, module path, position)` for each glob `use` leaf in `code`.
///
/// Both the extractor scan in `glob_unknowns` and `resolve_symbol` read
/// globs through this one step.
pub fn glob_imports(code: &Text) -> Rc<Vec<UseBinding>> {
    state::cached(&GLOB_IMPORTS, state::key(code), || {
        let mut found = Vec::new();
        for found_use in py::finditer(r"\buse\b[^;]*;", code) {
            let scope = alias_scope(code, found_use.start());
            for (path, _) in use_leaves(found_use.whole()) {
                if let Some(module) = path.strip_suffix("::*") {
                    found.push((scope, module.to_string(), found_use.start()));
                }
            }
        }
        Rc::new(found)
    })
}

/// The module a glob covers: `TRUSTED`, a module place, or `None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GlobModule {
    Trusted,
    Place(String),
    Unknown,
}

/// One candidate definition of a symbol.
#[derive(Clone, Debug)]
pub struct Cand {
    pub place: String,
    pub item: Item,
    pub owner: Option<String>,
    pub public: bool,
}

/// What a candidate is: a struct block, a type alias or a fn start.
#[derive(Clone, Debug)]
pub enum Item {
    Struct(Rc<StructBlock>),
    Alias(Rc<TypeAlias>),
    Fn(usize),
}

/// The candidates for a bare name that the glob imports at `site` leave.
///
/// A definition in the site's own module wins over any glob, as in Rust. A
/// trusted glob, one of `TRUSTED_GLOBS`, brings no request type. A glob of
/// a scanned module, such as `super::*`, brings that module's items. Any
/// other glob may bind the name. So the result is `None`, and the caller
/// fails closed. An empty list means no glob decides, so the name follows
/// its bare rule. A name that no scanned item has is left to that rule too,
/// since a glob can then bring only an item the audit cannot read.
pub fn glob_candidates(found: &[Cand], site: &Site, origin: &str) -> Option<Vec<Cand>> {
    let Some(at) = site.at else {
        return Some(Vec::new());
    };
    if found.is_empty() {
        return Some(Vec::new());
    }
    let covering: Vec<GlobModule> = glob_imports(&site.text)
        .iter()
        .filter(|((start, end), _, _)| *start <= at && at < *end)
        .map(|(_, path, used_at)| glob_module(&site.text, path, *used_at))
        .filter(|module| *module != GlobModule::Trusted)
        .collect();
    if covering.is_empty() {
        return Some(Vec::new());
    }
    let own: Vec<Cand> = found
        .iter()
        .filter(|item| item.place == origin)
        .cloned()
        .collect();
    if !own.is_empty() {
        return Some(own);
    }
    if covering.contains(&GlobModule::Unknown) {
        return None;
    }
    Some(
        found
            .iter()
            .filter(|item| covering.contains(&GlobModule::Place(item.place.clone())))
            .cloned()
            .collect(),
    )
}

/// What a glob `use path::*` at `used_at` brings.
///
/// A glob of `TRUSTED_GLOBS` is `TRUSTED`. A glob of a module the audit
/// scans, such as `super::*` or `crate::x::*`, is that module's place. So
/// its items resolve through it. Any other glob is `None`. It may bring any
/// name, so a bare name it covers fails closed. This is the one glob rule,
/// for extractor names and for every kind of symbol.
pub fn glob_module(text: &Text, path: &str, used_at: usize) -> GlobModule {
    if TRUSTED_GLOBS.contains(&path) {
        return GlobModule::Trusted;
    }
    let origin = site_origin(&Site::new(text, Some(used_at)));
    let Some(module) = absolute_path(&path_segments(path), &origin) else {
        return GlobModule::Unknown;
    };
    let head = if module[0] == "crate" {
        origin.split("::").next().unwrap_or("").to_string()
    } else {
        module[0].clone()
    };
    let mut parts = vec![head];
    parts.extend(module.into_iter().skip(1));
    let place = parts.join("::");
    if known_modules().contains(&place) {
        GlobModule::Place(place)
    } else {
        GlobModule::Unknown
    }
}

/// Whether the module at `place` defines or imports `name`, as a glob of it
/// brings.
///
/// A struct or a type alias there counts. So does a local item or a `use`
/// of that name inside an inline `mod` block of `code`.
pub fn module_binds(code: &Text, place: &str, name: &str) -> bool {
    if struct_candidates()
        .get(name)
        .is_some_and(|found| found.iter().any(|(found, _)| found == place))
    {
        return true;
    }
    if state::type_aliases()
        .get(name)
        .is_some_and(|found| found.iter().any(|alias| alias.place == place))
    {
        return true;
    }
    for (start, end, _) in block_owners(code, "mod").iter() {
        if site_origin(&Site::new(code, Some(start + 1))) != place {
            continue;
        }
        let block = &code[*start..*end];
        let item = format!(
            r"\b(?:struct|enum|trait|union|type|fn)\s+{}\b",
            py::esc(name)
        );
        if py::found(&item, block) {
            return true;
        }
        if use_bindings(code)
            .get(name)
            .is_some_and(|found| found.iter().any(|(scope, _, _)| scope.0 == *start))
        {
            return true;
        }
    }
    false
}

/// Whether `reference` names a standard library item, through any import.
pub fn std_path(reference: &str, site: &Site) -> bool {
    let segments = path_segments(reference);
    if segments.is_empty() {
        return false;
    }
    let imported = site
        .at
        .and_then(|at| innermost_use(&site.text, &segments[0], at));
    let head = imported.map_or_else(
        || segments[0].clone(),
        |(path, _)| path_segments(&path).into_iter().next().unwrap_or_default(),
    );
    STD_ROOTS.contains(&head.as_str())
}

/// The kind of symbol that `resolve_symbol` looks for.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Struct,
    Fn,
    Alias,
}

/// Every definition of `kind` that `reference`, written at `site`, reaches.
///
/// `kind` is a struct, a fn or a type alias. This is the one resolver for
/// all three. A struct is its block, a type alias its `TypeAlias`, and a fn
/// its start in the audited source. A fn reference `.name` is a method
/// call, which reaches any method of that name.
///
/// The first name goes through `expanded_path`, so an import decides first.
/// An import from another crate reaches nothing. A qualified path keeps the
/// definitions that `qualified_match` reaches from the site's module. For a
/// fn, a type before the name, such as `Decoder::`, keeps that type's
/// methods, and `Self::` keeps any method. A plain module path keeps free
/// functions.
///
/// `Self::` keeps the methods of the `impl` or `trait` block that holds the
/// site. Outside such a block it keeps any method.
///
/// A bare name with no import then goes through the glob imports in scope,
/// as `glob_candidates` reads them. A glob that may bind the name makes it
/// `None`, unless the site's own module defines it.
///
/// A bare name with no import follows the visibility rule of its kind. A fn
/// must be in the site's module or an ancestor, or be public. A struct may
/// be anywhere. A type alias must be the only alias of that name, and be in
/// the site's module or the crate root.
///
/// `None` means the reference is ambiguous or not visible, so the caller
/// fails closed. More than one struct or alias is `None` too. A caller of a
/// fn treats more than one result as ambiguous where it needs one.
pub fn resolve_symbol(kind: Kind, reference: &str, site: Option<&Site>) -> Option<Vec<Item>> {
    let source = state::source();
    let site = site.cloned().unwrap_or_else(|| Site::new(&source, None));
    let origin = site_origin(&site);
    let trimmed = reference.trim();
    if kind == Kind::Fn
        && let Some(method) = trimmed.strip_prefix('.')
    {
        let name = method.trim();
        return Some(
            function_index(&source)
                .get(name)
                .into_iter()
                .flatten()
                .filter(|def| def.owner.is_some())
                .map(|def| Item::Fn(def.start))
                .collect(),
        );
    }
    let Some(segments) = expanded_path(reference, &site) else {
        return Some(Vec::new());
    };
    if segments.is_empty() {
        return Some(Vec::new());
    }
    let bare = segments.len() == 1;
    let name = segments[segments.len() - 1].clone();
    let mut qualifier: Vec<String> = segments[..segments.len() - 1].to_vec();
    let mut found: Vec<Cand> = match kind {
        Kind::Struct => struct_candidates()
            .get(&name)
            .into_iter()
            .flatten()
            .map(|(place, block)| Cand {
                place: place.clone(),
                item: Item::Struct(Rc::clone(block)),
                owner: None,
                public: true,
            })
            .collect(),
        Kind::Alias => state::type_aliases()
            .get(&name)
            .into_iter()
            .flatten()
            .map(|alias| Cand {
                place: alias.place.clone(),
                item: Item::Alias(Rc::clone(alias)),
                owner: None,
                public: true,
            })
            .collect(),
        Kind::Fn => {
            let (crate_name, module) = state::local();
            let last = qualifier.last().cloned().unwrap_or_default();
            let upper = last.chars().next().is_some_and(char::is_uppercase);
            let mut typed = (upper || last == BYTE_SLICE).then(|| last.clone());
            if typed.is_some() {
                qualifier.pop();
            }
            if typed.as_deref() == Some("Self") {
                typed = Some(enclosing_owner(&site).unwrap_or_else(|| "Self".to_string()));
            }
            function_index(&source)
                .get(&name)
                .into_iter()
                .flatten()
                .map(|def| Cand {
                    place: location(&[&crate_name, &module, &def.modules]),
                    item: Item::Fn(def.start),
                    owner: def.owner.clone(),
                    public: def.public,
                })
                .filter(|item| item.owner.is_none() == typed.is_none())
                .filter(|item| match &typed {
                    None => true,
                    Some(typed) => typed == "Self" || item.owner.as_ref() == Some(typed),
                })
                .collect()
        }
    };
    if bare {
        let globbed = glob_candidates(&found, &site, &origin)?;
        if !globbed.is_empty() {
            found = globbed;
        } else if kind == Kind::Fn {
            found.retain(|item| {
                item.public
                    || item.place == origin
                    || origin.starts_with(&format!("{}::", item.place))
            });
        } else if kind == Kind::Alias && found.len() == 1 {
            let root = origin.split("::").next().unwrap_or("");
            if found[0].place != origin && found[0].place != root {
                return None;
            }
        }
    } else {
        found.retain(|item| qualified_match(&qualifier, &item.place, &origin));
    }
    if kind != Kind::Fn && found.len() > 1 {
        return None;
    }
    Some(found.into_iter().map(|cand| cand.item).collect())
}

/// The fn starts that `resolve_symbol` finds, or `None`.
pub fn resolve_fn(reference: &str, site: Option<&Site>) -> Option<Vec<usize>> {
    resolve_symbol(Kind::Fn, reference, site).map(|items| {
        items
            .into_iter()
            .filter_map(|item| match item {
                Item::Fn(start) => Some(start),
                _ => None,
            })
            .collect()
    })
}

/// The type of the `impl`, or the `trait`, that holds a site in the audited
/// source.
pub fn enclosing_owner(site: &Site) -> Option<String> {
    let at = site.at?;
    if !state::same(&site.text, &state::source()) {
        return None;
    }
    owner_at(&impl_and_trait_owners(&site.text), at)
}

/// The block of the one struct `reference` names at `site`, or `None`.
pub fn struct_body(reference: &str, site: Option<&Site>) -> Option<Rc<StructBlock>> {
    let found = resolve_symbol(Kind::Struct, reference, site)?;
    match found.into_iter().next() {
        Some(Item::Struct(block)) => Some(block),
        _ => None,
    }
}

/// Every struct in `source`, with its place, by bare name.
///
/// The place is the crate, then the module path, as `location` builds it.
/// `module` is the path of the file itself. An inline `mod name { .. }` adds
/// its name to the path of each struct inside it. Each block keeps its
/// site, so a field type resolves from the struct's own module and imports.
pub fn struct_index(source: &str, module: &str, crate_name: &str) -> StructIndex {
    let code = state::intern(&code_only(source, true));
    state::set_file_place(&code, location(&[crate_name, module]));
    let modules = block_owners(&code, "mod");
    let mut index: StructIndex = HashMap::new();
    for found in py::finditer(r"\bstruct ([A-Za-z_][A-Za-z_0-9]*)\s*\{", &code) {
        let inline = inline_modules(&modules, found.start());
        let mut parts = vec![crate_name, module];
        parts.extend(inline.iter().map(String::as_str));
        let place = location(&parts);
        let block = StructBlock {
            text: struct_text(source, found.start(), found.end()),
            site: Site::new(&code, Some(found.start())),
        };
        index
            .entry(found.g(1).to_string())
            .or_default()
            .push((place, Rc::new(block)));
    }
    index
}

/// The `.rs` files under a crate's `src`, sorted by path.
fn crate_files(crate_dir: &str) -> Vec<(PathBuf, PathBuf)> {
    let root = crate::util::repo_path(crate_dir).join("src");
    let mut files: Vec<PathBuf> = walkdir::WalkDir::new(&root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_file() && entry.path().extension().is_some_and(|ext| ext == "rs")
        })
        .map(walkdir::DirEntry::into_path)
        .collect();
    files.sort();
    files.into_iter().map(|path| (root.clone(), path)).collect()
}

/// Reads a crate file, recording an error the audit reports.
fn read_text(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| {
        py::fail(format!("reading {}: {error}", path.display()));
        String::new()
    })
}

/// Every struct in either crate, as `struct_index` reads it, by bare name.
///
/// A file's module path comes from its place under `src`. So `src/a/b.rs`
/// and `src/a/b/mod.rs` are `a::b`, and `lib.rs` and `main.rs` are the root.
pub fn defined_structs() -> Rc<StructIndex> {
    DEFINED_STRUCTS.with(|cell| {
        Rc::clone(cell.get_or_init(|| {
            let mut index: StructIndex = HashMap::new();
            for crate_dir in super::tables::CRATES {
                let crate_name = crate_dir.replace('-', "_");
                for (root, path) in crate_files(crate_dir) {
                    let text = read_text(&path);
                    let place = module_path(&root, &path);
                    for (name, found) in struct_index(&text, &place, &crate_name) {
                        index.entry(name).or_default().extend(found);
                    }
                }
            }
            Rc::new(index)
        }))
    })
}

/// The attribute lines above a struct match, then its block.
///
/// An attribute that rustfmt splits over several lines is read up to its
/// opening `#[`, since its closing `)]` line alone does not start with `#[`.
pub fn struct_text(source: &str, start: usize, end: usize) -> String {
    let mut lines: Vec<&str> = py::upto(source, start).split('\n').collect();
    lines.pop();
    let mut attributes: Vec<String> = Vec::new();
    let mut depth = 0i64;
    while let Some(last) = lines.last() {
        let text = last.trim();
        if depth == 0 && !(text.starts_with("#[") || text.starts_with("//") || text.ends_with(']'))
        {
            break;
        }
        depth += py::count_byte(text, b']') - py::count_byte(text, b'[');
        attributes.insert(0, text.to_string());
        lines.pop();
    }
    let block = balanced(py::from(source, end.saturating_sub(1)), b'{', b'}');
    attributes.push(block.to_string());
    attributes.join("\n")
}

/// Each `type Name<P, ..> = Target;` in `source`, by name, with its module.
///
/// `module` is the path of the file itself. An inline `mod name { .. }` adds
/// its name, as `struct_index` does for structs. Only a module-level alias
/// is read: one whose every enclosing block is a `mod`. An alias inside a fn
/// or another block is local to it, and the extractor alias layer reads it
/// in that scope. A target is kept as written. `canonical_type` cuts its
/// standard paths after the alias pass, as for every other type text.
pub fn type_aliases_in(source: &str, module: &str, crate_name: &str) -> AliasIndex {
    let code = state::intern(&code_only(source, true));
    let modules = block_owners(&code, "mod");
    let mut found: AliasIndex = HashMap::new();
    for alias in py::finditer(
        r"\btype\s+([A-Z]\w*)\s*(?:<([^=;]*)>)?\s*=\s*([^;]+);",
        &code,
    ) {
        let inline = inline_modules(&modules, alias.start());
        let depth = super::aliases::brace_balance(&code, alias.start());
        if depth != i64::try_from(inline.len()).unwrap_or(i64::MAX) {
            continue;
        }
        let mut parts = vec![crate_name, module];
        parts.extend(inline.iter().map(String::as_str));
        let parameters = split_expression(alias.get(2).unwrap_or(""), &[","], true, None)
            .into_iter()
            .map(|part| part.trim().to_string())
            .collect();
        found
            .entry(alias.g(1).to_string())
            .or_default()
            .push(Rc::new(TypeAlias {
                place: location(&parts),
                parameters,
                target: alias.g(3).trim().to_string(),
            }));
    }
    found
}

/// The type aliases of both crates, with each file's module path.
pub fn crate_type_aliases() -> Rc<AliasIndex> {
    CRATE_ALIASES.with(|cell| {
        Rc::clone(cell.get_or_init(|| {
            let mut found: AliasIndex = HashMap::new();
            for crate_dir in super::tables::CRATES {
                let crate_name = crate_dir.replace('-', "_");
                for (root, path) in crate_files(crate_dir) {
                    let text = read_text(&path);
                    let place = module_path(&root, &path);
                    for (name, aliases) in type_aliases_in(&text, &place, &crate_name) {
                        found.entry(name).or_default().extend(aliases);
                    }
                }
            }
            Rc::new(found)
        }))
    })
}

/// The module path of a file under `src`: `a/b.rs` and `a/b/mod.rs` are
/// `a::b`.
pub fn module_path(root: &Path, path: &Path) -> String {
    let relative = path.strip_prefix(root).unwrap_or(path).with_extension("");
    let mut parts: Vec<String> = relative
        .components()
        .map(|part| part.as_os_str().to_string_lossy().into_owned())
        .collect();
    if parts
        .last()
        .is_some_and(|last| matches!(last.as_str(), "mod" | "lib" | "main"))
    {
        parts.pop();
    }
    parts.join("::")
}

/// A fn's parameter list, return clause and block, as `parts_at` reads them.
#[derive(Debug)]
pub struct Parts {
    pub params: Rc<Params>,
    pub returns: String,
    pub block: String,
    pub start: usize,
}

impl Parts {
    /// The block, with where it starts.
    pub fn block(&self) -> B<'_> {
        B {
            s: &self.block,
            start: Some(self.start),
        }
    }
}

/// The parameter list, return clause and block of the function a call
/// reaches.
///
/// `resolve_symbol` reads the call `reference` from its `site`. `None`
/// means the audit cannot find the function, or cannot tell which of
/// several it is. So the caller fails closed.
pub fn function_parts(source: &Text, reference: &str, site: Option<&Site>) -> Option<Rc<Parts>> {
    let starts = resolve_fn(reference, site).unwrap_or_default();
    if starts.len() == 1 {
        parts_at(source, starts[0])
    } else {
        None
    }
}

/// The parameter list, return clause and block of the fn at `start`.
///
/// The block starts at the first `{` after the balanced parameter list. So a
/// brace in a parameter comment does not end the search early. The returned
/// parameter list has its comments removed. So a comment that names an
/// extractor is not read as one.
pub fn parts_at(source: &Text, start: usize) -> Option<Rc<Parts>> {
    state::cached(&PARTS, (state::key(source), start), || {
        // Brackets are matched on a copy with literals blanked. So a `{` in a
        // string or a char literal does not end a block early or late.
        let masked = masked_source(source);
        let opener = py::find(&masked, "(", start);
        if opener < 0 {
            return None;
        }
        let opener = py::to_usize(opener);
        let length = paren(&masked[opener..]).len();
        let brace_at = py::find(&masked, "{", opener + length);
        if brace_at < 0 {
            return None;
        }
        let brace_at = py::to_usize(brace_at);
        let returns = py::sl(source, opener + length, brace_at).to_string();
        // The masked copy blanks nested comments too.
        let params = canonical_params(&masked[opener..opener + length], opener);
        let end = brace_at + brace(&masked[brace_at..]).len();
        let block = py::sl(source, brace_at, end).to_string();
        Some(Rc::new(Parts {
            params,
            returns,
            block,
            start: brace_at,
        }))
    })
}

/// A canonical parameter list, with the names that may carry a raw body.
///
/// `possible` holds each plainly named parameter whose type the audit can
/// neither read as a byte carrier nor rule out as one. `written` is the list
/// as the source spells it, for a finding that names an alias. `raw_reader`
/// tells whether a top-level parameter is a trusted raw query reader, as
/// `raw_reader` reads it.
#[derive(Debug, Default)]
pub struct Params {
    pub text: String,
    pub possible: BTreeSet<String>,
    pub written: String,
    pub raw_reader: bool,
}

/// A parameter list with each parameter type in its `canonical_type` form.
///
/// Each type is read at its own site in the audited source. A plainly named
/// parameter whose type is no byte carrier is `possible` when its type has
/// an alias the audit cannot read, or a head the audit does not know, as
/// `known_non_bytes` reads it.
pub fn canonical_params(params: &str, opener: usize) -> Rc<Params> {
    state::cached(&CANONICAL_PARAMS, (params.to_string(), opener), || {
        let source = state::source();
        let mut out = params.to_string();
        let mut possible = BTreeSet::new();
        let mut readers = false;
        for (start, end) in parameter_spans(params).into_iter().rev() {
            let item = &params[start..end];
            let Some(colon) = py::search(r"(?<!:):(?!:)", item) else {
                continue;
            };
            let written = &item[colon.end()..];
            let site = Site::new(&source, Some(opener + start + colon.end()));
            readers = readers || raw_reader(written, &site);
            let (canonical, unread) = canonical_type(written, Some(&site));
            let pattern = item[..colon.start()].trim();
            let named = py::fullmatch(r"(?:mut\s+)?([a-z_][a-z_0-9]*)", pattern);
            let typed = format!("{pattern}: {canonical}");
            if let Some(named) = named
                && !py::found(BYTE_PARAMETER, &typed)
                && (unread.is_some() || !known_non_bytes(&canonical, opener + start))
            {
                possible.insert(named.g(1).to_string());
            }
            out = format!("{} {typed}{}", &out[..start], &out[end..]);
        }
        Rc::new(Params {
            text: out,
            possible,
            written: params.to_string(),
            raw_reader: readers,
        })
    })
}

/// The span of each top-level parameter in a parenthesized parameter list.
pub fn parameter_spans(params: &str) -> Vec<(usize, usize)> {
    let bytes = params.as_bytes();
    let mut spans = Vec::new();
    let mut depth = 0i64;
    let mut start = 1;
    for index in 1..bytes.len().saturating_sub(1) {
        let char = bytes[index];
        if b"([{<".contains(&char) {
            depth += 1;
        } else if b")]}".contains(&char) || (char == b'>' && bytes[index - 1] != b'-') {
            depth -= 1;
        } else if char == b',' && depth == 0 {
            spans.push((start, index));
            start = index + 1;
        }
    }
    let last = bytes.len().saturating_sub(1);
    if !py::sl(params, start, last).trim().is_empty() {
        spans.push((start, last));
    }
    spans
}

/// Whether a canonical parameter type is one the audit knows holds no raw
/// body.
///
/// A standard type, a wire scalar, a bare extractor, a tuple and a struct
/// the audit scans are known. Any other head, such as an unknown path or a
/// fixed-size array, may hold bytes.
pub fn known_non_bytes(canonical: &str, at: usize) -> bool {
    let text = py::sub(
        r"^(?:&\s*(?:'[a-z_]+\s+)?(?:mut\s+)?|impl\s+|dyn\s+)*",
        "",
        canonical.trim(),
    );
    if text.starts_with('(') {
        return true;
    }
    let Some(head) = py::rmatch(r"((?:[A-Za-z_]\w*\s*::\s*)*[A-Za-z_]\w*)", &text) else {
        return false;
    };
    let name = py::no_space(head.g(1));
    if std_type(&name) || wire_types(&name).is_some() || EXTRACTOR_NAMES.contains(&name.as_str()) {
        return true;
    }
    let site = Site::new(&state::source(), Some(at));
    resolve_symbol(Kind::Struct, &name, Some(&site)).is_some_and(|found| !found.is_empty())
}

/// `(start, end, keyword, type parameters)` for each `impl` or `trait` block.
pub fn owner_blocks(source: &Text) -> Rc<Vec<OwnerBlock>> {
    state::cached(&OWNER_BLOCKS, state::key(source), || {
        let mut found = Vec::new();
        for head in py::finditer(r"\b(impl|trait)\b", source) {
            let brace_at = py::find(source, "{", head.end());
            let stop = py::find(source, ";", head.end());
            if brace_at < 0 || (0 <= stop && stop < brace_at) {
                continue;
            }
            let brace_at = py::to_usize(brace_at);
            let mut header = source[head.end()..brace_at].trim_start().to_string();
            if head.g(1) == "trait" {
                header = py::sub(r"^[A-Za-z_]\w*\s*", "", &header);
            }
            let mut names = BTreeSet::new();
            if header.starts_with('<') {
                let group = balanced(&header, b'<', b'>');
                let inner = py::sl(group, 1, group.len().saturating_sub(1));
                for item in split_expression(inner, &[","], false, None) {
                    if let Some(name) = py::rmatch(r"\s*(?:const\s+)?([A-Za-z_]\w*)", &item) {
                        names.insert(name.g(1).to_string());
                    }
                }
            }
            let end = brace_at + brace(&source[brace_at..]).len();
            found.push((brace_at, end, head.g(1).to_string(), names));
        }
        Rc::new(found)
    })
}

/// The keyword and type parameters of the innermost `impl` or `trait` at
/// `position`.
pub fn owner_block(source: &Text, position: usize) -> (Option<String>, BTreeSet<String>) {
    owner_blocks(source)
        .iter()
        .filter(|block| block.0 < position && position < block.1)
        .max()
        .map_or((None, BTreeSet::new()), |(_, _, keyword, generics)| {
            (Some(keyword.clone()), generics.clone())
        })
}

/// The parts of `async fn <name>(..)`, or `None` when it is elsewhere.
///
/// The lookup starts at `async fn`. So an earlier fn of the same name, such
/// as a method, does not hide the handler.
pub fn handler_parts(source: &Text, name: &str) -> Option<Rc<Parts>> {
    let at = block_start(source, &format!("async fn {name}("));
    if at < 0 {
        None
    } else {
        parts_at(source, py::to_usize(at))
    }
}
