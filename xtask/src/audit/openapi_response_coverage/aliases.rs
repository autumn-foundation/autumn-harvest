//! The alias layer: `use .. as ..`, `type` aliases, shadowing and the
//! canonical type pipeline.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use super::mask::code_only;
use super::py::{self, paren};
use super::rejects::live_binding;
use super::state::{self, Site, Text};
use super::symbols::{
    GlobModule, Item, Kind, glob_imports, glob_module, last_segment, module_binds, resolve_symbol,
    written_path,
};
use super::syntax::{closure_body_end, enclosing_block, split_expression};
use super::tables::{
    ALIAS_PASSES, BARE_TRUSTED_NAMES, EXTRACTOR_NAMES, PRELUDE_NAMES, PRELUDE_PATH,
    RAW_READER_NAMES, SERDE_PARSERS, STD_ROOTS, TRUSTED_TYPE_PATHS, VARIANT_PATH, alias_target,
    prelude_path, serde_paths, std_type, supported_path,
};

/// One alias: its scope, its declaration span, its pattern and replacement.
pub type AliasDecl = ((usize, usize), (usize, usize), String, Option<String>);

orc_cache!(ALIAS_SCOPE: (usize, usize) => (usize, usize));
orc_cache!(BRACE_BALANCE: usize => Rc<Vec<i32>>);
orc_cache!(ALIAS_DECLARATIONS: usize => Rc<Vec<AliasDecl>>);
orc_cache!(GLOB_UNKNOWNS: usize => Rc<Vec<(usize, usize, String)>>);
orc_cache!(CANONICAL_TYPE: (String, usize, Option<usize>) => (String, Option<String>));

/// `text` with each standard path to a type in `STD_TYPES`, or a variant,
/// cut.
///
/// `std::option::Option<T>` is `Option<T>`, `alloc::vec::Vec<u8>` is
/// `Vec<u8>`, `core::primitive::u32` is `u32`, and
/// `std::result::Result::Err(e)` is `Err(e)`. Every check then reads the
/// short name only, so no check needs its own rule for a path. A standard
/// path always names the real type. Only a bare name can be shadowed, and
/// `alias_declarations` binds those before this cut runs.
pub fn canonical_paths(text: &str) -> String {
    let cut = py::sub_with(PRELUDE_PATH, text, first_group);
    py::sub_with(VARIANT_PATH, &cut, first_group)
}

/// The first group of a match that took part, or the empty text.
///
/// The cut patterns capture the name after the path, where the original
/// pattern only looked ahead at it. The cut then writes the name back.
fn first_group(hit: &py::M<'_>) -> String {
    (1..=6)
        .find_map(|index| hit.get(index))
        .unwrap_or("")
        .to_string()
}

/// The supported kind a path names, or `None`.
///
/// A bare `Query`, `Json` or `Bytes` names the one in scope, which the `use`
/// aliases have already made canonical. A qualified path counts only when
/// `SUPPORTED_PATHS` trusts it, after the `autumn_web::reexports::` prefix
/// is cut. Both the `use` aliases and the `type` aliases ask this.
pub fn extractor_kind(path: &str) -> Option<String> {
    let path = py::no_space(path);
    let path = path.trim_start_matches(':');
    let path = py::sub(r"^autumn_web::reexports::", "", path);
    if !path.contains("::") {
        return EXTRACTOR_NAMES.contains(&path.as_str()).then_some(path);
    }
    supported_path(&path).map(str::to_string)
}

/// `source` with each trusted qualified extractor path cut to its bare name.
///
/// The first name of each path goes through the scoped `use` bindings
/// first, as `written_path` reads them. The expanded path is then trusted or
/// not, as `extractor_kind` reads it. This is the one path-trust decision.
/// `axum::Json<T>` becomes `Json<T>`, and so does
/// `autumn_web::reexports::axum::Json`. A trusted path to one of
/// `SERDE_PARSERS` becomes its `serde_json::` path. A path it does not
/// trust, such as `crate::signed::Query<T>`, is left whole. If an import
/// rebinds its first name, the path is spelled out in full. So `axum::Json`
/// under `use crate::signed as axum;` reads as `crate::signed::Json`. Every
/// extractor reader matches only a bare name, so that type is no extractor.
/// A `use` statement keeps its paths, since the alias layer reads them.
/// Comments and literals are left alone, and newlines stay.
pub fn canonical_extractor_paths(source: &str) -> String {
    let code = state::intern(&code_only(source, true));
    let uses: Vec<(usize, usize)> = py::finditer(r"\buse\b[^;]*;", &code)
        .iter()
        .map(|found| (found.start(), found.end()))
        .collect();
    let qualified =
        r"(?<![\w:])(?:::\s*)?(?:[A-Za-z_]\w*\s*::\s*)+(Query|Json|Bytes|from_slice|from_str)\b";
    let mut source = source.to_string();
    for found in py::finditer(qualified, &code).iter().rev() {
        if uses
            .iter()
            .any(|(start, end)| *start <= found.start() && found.start() < *end)
        {
            continue;
        }
        let (segments, used_at) =
            written_path(found.whole(), &Site::new(&code, Some(found.start())));
        let trusted = match extractor_kind(&segments.join("::")) {
            Some(kind) => alias_target(&kind).unwrap_or(kind),
            // A rebound first name, as in `use crate::signed as axum;`, names
            // that module. The path is spelled out, so no reader trusts it.
            None if used_at.is_some() => segments.join("::"),
            None => continue,
        };
        source.replace_range(found.start()..found.end(), &trusted);
    }
    source
}

/// `(path, alias)` for each leaf of a `use` tree.
///
/// A group such as `serde_json::{self as json, Value}` or
/// `axum::{extract::{Query as Q}}` is read at any depth. A `self` leaf names
/// its group's path. The prefix `autumn_web::reexports::` is cut, since that
/// crate re-exports axum unchanged.
pub fn use_leaves(statement: &str) -> Vec<(String, Option<String>)> {
    let tree = py::sub(r"^\s*(?:pub(?:\([^)]*\))?\s+)?use\s+", "", statement);
    let tree = tree.trim_end_matches(';').trim().to_string();
    expand_leaves("", &tree)
}

fn expand_leaves(prefix: &str, text: &str) -> Vec<(String, Option<String>)> {
    let text = text.trim();
    if let Some(group) = py::fullmatch(r"(?s)((?:::)?(?:[\w]+\s*::\s*)*)\{(.*)\}", text) {
        let base = format!("{prefix}{}", py::no_space(group.g(1)));
        return split_expression(group.g(2), &[","], false, None)
            .iter()
            .flat_map(|item| expand_leaves(&base, item))
            .collect();
    }
    let named = py::fullmatch(r"(?s)(.*?)\s+as\s+([A-Za-z_]\w*)", text);
    let (path, alias) = named.as_ref().map_or((text, None), |named| {
        (named.g(1), Some(named.g(2).to_string()))
    });
    let path = py::no_space(&format!("{prefix}{path}"));
    let path = py::sub(r"::self$", "", &path);
    let path = path.trim_start_matches(':');
    vec![(py::sub(r"^autumn_web::reexports::", "", path), alias)]
}

/// How many `{` minus how many `}` come before `position` in `code`.
///
/// It reads `code[:position].count("{") - code[:position].count("}")`. A
/// table per text answers it at once, since a scan of a long prefix per
/// call is slow.
pub fn brace_balance(code: &Text, position: usize) -> i64 {
    let table = state::cached(&BRACE_BALANCE, state::key(code), || {
        let mut table = Vec::with_capacity(code.len() + 1);
        let mut balance = 0i32;
        table.push(balance);
        for char in code.bytes() {
            balance += i32::from(char == b'{') - i32::from(char == b'}');
            table.push(balance);
        }
        Rc::new(table)
    });
    i64::from(table[position.min(code.len())])
}

/// Where a declaration at `position` holds: the file at depth 0, else its
/// block.
pub fn alias_scope(code: &Text, position: usize) -> (usize, usize) {
    state::cached(&ALIAS_SCOPE, (state::key(code), position), || {
        if brace_balance(code, position) == 0 {
            return (0, code.len());
        }
        enclosing_block(code, position)
    })
}

/// A type spelling in its canonical form, and the alias it cannot read, if
/// any.
///
/// Every type the audit reads goes through this one pipeline: handler and
/// helper parameters, receiver and `let` types, and struct fields. At the
/// type's `site` it applies, in order:
///
/// 1. prelude and extractor shadowing, as `shadow_bound` reads it;
/// 2. scoped `use` expansion of standard items, as `std_imports` reads it;
/// 3. type aliases, to a fixed point, as `resolve_field_type` reads them;
/// 4. the `STD_TYPES` cut of `canonical_paths`.
///
/// Every reader then matches only the canonical text.
pub fn canonical_type(declared_type: &str, site: Option<&Site>) -> (String, Option<String>) {
    let site = site
        .cloned()
        .unwrap_or_else(|| Site::new(&state::source(), None));
    canonical_type_at(declared_type.trim(), &site)
}

/// `canonical_type` for a site, found once.
fn canonical_type_at(declared_type: &str, site: &Site) -> (String, Option<String>) {
    let cache_key = (declared_type.to_string(), state::key(&site.text), site.at);
    state::cached(&CANONICAL_TYPE, cache_key, || {
        let declared_type = std_imports(&shadow_bound(declared_type, site), site);
        // A reference or a lifetime before the type stays. The alias pass
        // reads the type after it.
        let lead = py::rmatch(r"(?:&\s*(?:'[a-z_]+\s+)?(?:mut\s+)?)?", &declared_type)
            .map_or(0, |lead| lead.end());
        let (resolved, unread) = resolve_field_type(&declared_type[lead..], Some(site));
        (
            canonical_paths(&format!("{}{resolved}", &declared_type[..lead])),
            unread,
        )
    })
}

/// A type with each name that a `use` binds to a standard or trusted item
/// spelled out.
///
/// `use std::option::Option as Maybe;` makes `Maybe<T>` read as
/// `std::option::Option<T>`, which the `STD_TYPES` cut then shortens.
/// `use uuid::Uuid;` makes `Uuid` read as `uuid::Uuid`, one of
/// `TRUSTED_TYPE_PATHS`. A written trusted path whose first name an import
/// rebinds, as under `use crate::fake as uuid;`, is spelled out too. So it
/// no longer reads as trusted. `written_path` reads each import. A name
/// bound to any other item is left as written, since `resolve_symbol` reads
/// its import itself. A bare `Uuid` with no import stays bare and is no wire
/// type, so the audit fails closed on it.
pub fn std_imports(declared_type: &str, site: &Site) -> String {
    if site.at.is_none() {
        return declared_type.to_string();
    }
    let names = r"(?<![\w:])(?:::\s*)?[A-Za-z_]\w*(?:\s*::\s*[A-Za-z_]\w*)*";
    let mut declared_type = declared_type.to_string();
    let hits: Vec<(usize, usize, String)> = py::finditer(names, &declared_type)
        .iter()
        .map(|found| (found.start(), found.end(), found.whole().to_string()))
        .collect();
    for (start, end, written) in hits.into_iter().rev() {
        let (segments, used_at) = written_path(&written, site);
        // The audited source has its standard paths cut already, `use`
        // statements too. So a bare standard name there is one as well.
        let cut = segments.len() == 1 && std_type(&segments[0]);
        let joined = segments.join("::");
        let trusted = TRUSTED_TYPE_PATHS.contains(&joined.as_str());
        let rebound = TRUSTED_TYPE_PATHS.contains(&py::no_space(&written).as_str());
        let standard = segments
            .first()
            .is_some_and(|head| STD_ROOTS.contains(&head.as_str()));
        if used_at.is_some() && (standard || cut || trusted || rebound) {
            declared_type.replace_range(start..end, &joined);
        }
    }
    declared_type
}

/// A type with each prelude or extractor name that its site shadows
/// replaced.
///
/// `alias_declarations` binds the names. A name shadowed by a local item or
/// an import reads as that item's path. So a shadowed `Option<T>` is no
/// `Option`, and the field stays mandatory. The audited source needs no pass
/// here, since `resolve_aliases` has already rewritten it whole.
pub fn shadow_bound(declared_type: &str, site: &Site) -> String {
    let Some(at) = site.at else {
        return declared_type.to_string();
    };
    if state::same(&site.text, &state::source()) {
        return declared_type.to_string();
    }
    let mut declared_type = declared_type.to_string();
    for name in PRELUDE_NAMES.iter().chain(EXTRACTOR_NAMES) {
        let pattern = format!(r"(?<![\w:.]){name}\b");
        if !py::found(&pattern, &declared_type) {
            continue;
        }
        let bound = alias_declarations(&site.text)
            .iter()
            .filter(|((start, end), _, declared, replacement)| {
                *declared == pattern
                    && *start <= at
                    && at < *end
                    && replacement.as_ref().is_some_and(|text| !text.is_empty())
            })
            .map(|((start, _), _, _, replacement)| {
                (*start, replacement.clone().unwrap_or_default())
            })
            .max();
        if let Some((_, replacement)) = bound {
            declared_type = py::sub_with(&pattern, &declared_type, |_| replacement.clone());
        }
    }
    declared_type
}

/// `(scope, declaration span, pattern, replacement)` for each alias in
/// `code`.
///
/// It reads `use .. as ..` for `Query`, `Json`, `Bytes` and each of
/// `serde_json`'s `SERDE_PARSERS`. It also reads a module alias of
/// `serde_json`, a plain or glob import of those parsers, and `type X<..> =
/// Y<..>;`. An import or a
/// local item that binds an extractor's name to something else shadows it
/// in its scope. A type alias to one of those types with the same
/// parameters is a rename. One with no parameters is replaced by its
/// target. Any other type alias that names an extractor has no replacement
/// (`None`), so the audit reports it.
pub fn alias_declarations(code: &Text) -> Rc<Vec<AliasDecl>> {
    state::cached(&ALIAS_DECLARATIONS, state::key(code), || {
        Rc::new(alias_declarations_uncached(code))
    })
}

fn alias_declarations_uncached(code: &Text) -> Vec<AliasDecl> {
    let call = r"(?=\s*(?:::<|\())";
    let mut found: Vec<AliasDecl> = Vec::new();
    for found_use in py::finditer(r"\buse\b[^;]*;", code) {
        let scope = alias_scope(code, found_use.start());
        let span = (found_use.start(), found_use.end());
        for (path, alias) in use_leaves(found_use.whole()) {
            let kind = if path.contains("::") {
                extractor_kind(&path)
            } else {
                None
            };
            let binding = alias
                .clone()
                .unwrap_or_else(|| last_segment(&path).to_string());
            let kind_is_extractor = kind
                .as_deref()
                .is_some_and(|kind| EXTRACTOR_NAMES.contains(&kind));
            if EXTRACTOR_NAMES.contains(&binding.as_str())
                && kind.as_deref() != Some(binding.as_str())
                && !kind_is_extractor
            {
                // A plain import of another item with an extractor's name
                // binds that name in scope. It is then that item, not the
                // extractor.
                found.push((scope, span, format!(r"(?<![\w:.]){binding}\b"), Some(path)));
            } else if PRELUDE_NAMES.contains(&binding.as_str()) && !prelude_path(&path) {
                // An import of another item with a prelude name hides the
                // prelude type in scope, as an extractor name does.
                found.push((scope, span, format!(r"(?<![\w:.]){binding}\b"), Some(path)));
            } else if let (Some(kind), Some(alias)) =
                (kind.as_deref().filter(|k| !k.is_empty()), alias.as_deref())
            {
                let suffix = if SERDE_PARSERS.contains(&kind) {
                    call
                } else {
                    ""
                };
                let pattern = format!(r"(?<![\w:.]){alias}\b{suffix}");
                found.push((scope, span, pattern, alias_target(kind)));
            } else if path == "serde_json" && alias.is_some() {
                let alias = alias.unwrap_or_default();
                let pattern = format!(r"(?<![\w:.]){alias}(?=\s*::)");
                found.push((scope, span, pattern, Some("serde_json".to_string())));
            } else if py::full(r"(?:std|core|alloc)(?:::[a-z_]\w*)+", &path) {
                // A standard-library module keeps its root, so `handoff_path`
                // can see that `s::from_utf8` is `std::str::from_utf8`.
                let name = alias.unwrap_or_else(|| last_segment(&path).to_string());
                let pattern = format!(r"(?<![\w:.]){name}(?=\s*::)");
                found.push((scope, span, pattern, Some(path)));
            } else if alias.is_none() && (path == "serde_json::*" || serde_paths(&path)) {
                // A plain or glob import binds each modeled parser by name.
                let parsers: Vec<&str> = if path.ends_with('*') {
                    SERDE_PARSERS.to_vec()
                } else {
                    vec![last_segment(&path)]
                };
                for parser in parsers {
                    let pattern = format!(r"(?<![\w:.]){parser}{call}");
                    found.push((scope, span, pattern, alias_target(parser)));
                }
            }
        }
    }
    // A local item named like an extractor or a prelude type shadows it in
    // its scope, as an untrusted import does. A `type` alias that renames
    // the type to its own name is no shadow.
    let mut shadowable: Vec<&str> = EXTRACTOR_NAMES.to_vec();
    shadowable.extend(PRELUDE_NAMES);
    shadowable.extend(RAW_READER_NAMES);
    let item = format!(
        r"\b(struct|enum|trait|union|type)\s+({})\b",
        shadowable.join("|")
    );
    for local in py::finditer(&item, code) {
        if local.g(1) == "type" {
            let target = py::rmatch(r"\s*(<[^=;]*?>)?\s*=\s*([^;]+);", &code[local.end()..]);
            let renamed = target
                .as_ref()
                .map_or_else(String::new, |target| canonical_paths(target.g(2).trim()));
            if target.is_some() && py::full(&format!(r"{}\s*(<.*>)?", local.g(2)), &renamed) {
                continue;
            }
        }
        let scope = alias_scope(code, local.start());
        let span = (local.start(), local.end());
        found.push((
            scope,
            span,
            format!(r"(?<![\w:.]){}\b", local.g(2)),
            Some(format!("self::{}", local.g(2))),
        ));
    }
    for declared in py::finditer(r"\btype\s+([A-Z]\w*)\s*(<[^=;]*?>)?\s*=\s*([^;]+);", code) {
        let name = declared.g(1);
        let parameters = declared.g(2);
        let target = declared.g(3).trim();
        let scope = alias_scope(code, declared.start());
        let span = (declared.start(), declared.end());
        let plain = py::fullmatch(
            r"((?:[A-Za-z_]\w*\s*::\s*)*(?:Query|Json|Bytes))\s*(<.*>)?",
            target,
        );
        let kind = plain.as_ref().and_then(|plain| extractor_kind(plain.g(1)));
        // Only a supported extractor counts. A same-named type from another
        // path, such as `crate::signed::Query`, is left alone.
        let named = py::finditer(r"((?:[A-Za-z_]\w*\s*::\s*)*)\b(Query|Json|Bytes)\b", target)
            .iter()
            .any(|hit| extractor_kind(hit.whole()).is_some());
        let generics = plain.as_ref().map_or("", |plain| plain.g(2));
        match kind.filter(|kind| !kind.is_empty()) {
            Some(kind) if py::no_space(parameters) == py::no_space(generics) => {
                found.push((scope, span, format!(r"(?<![\w:]){name}\b"), Some(kind)));
            }
            Some(kind) if parameters.is_empty() => {
                let pattern = format!(r"(?<![\w:]){name}\b(?!\s*<)");
                found.push((scope, span, pattern, Some(format!("{kind}{generics}"))));
            }
            _ if named => found.push((scope, span, name.to_string(), None)),
            _ => {}
        }
    }
    found
}

/// `(scope start, scope end, name)` for each bare-trusted name a glob may
/// hide.
///
/// The names are `BARE_TRUSTED_NAMES`. `glob_module` reads each glob. An
/// untrusted one, such as `use crate::models::*;` for a module the audit
/// does not scan, may bring its own `Query`, `Request` or `Option`. A glob
/// of a scanned module brings such a name only when that module binds it,
/// as `module_binds` reads it. In the glob's scope, a bare use of such a
/// name cannot be told apart. The exception is a plain import in the same
/// scope that binds it, since a plain import wins over a glob.
pub fn glob_unknowns(code: &Text) -> Rc<Vec<(usize, usize, String)>> {
    state::cached(&GLOB_UNKNOWNS, state::key(code), || {
        let globs: Vec<((usize, usize), GlobModule)> = glob_imports(code)
            .iter()
            .map(|(scope, base, used_at)| (*scope, glob_module(code, base, *used_at)))
            .collect();
        let mut bound: HashMap<(usize, usize), HashSet<String>> = HashMap::new();
        for found_use in py::finditer(r"\buse\b[^;]*;", code) {
            let scope = alias_scope(code, found_use.start());
            for (path, alias) in use_leaves(found_use.whole()) {
                if !path.ends_with("::*") {
                    let name = alias.unwrap_or_else(|| last_segment(&path).to_string());
                    bound.entry(scope).or_default().insert(name);
                }
            }
        }
        let mut found = Vec::new();
        for (scope, module) in globs {
            if module == GlobModule::Trusted {
                continue;
            }
            for name in BARE_TRUSTED_NAMES {
                if bound.get(&scope).is_some_and(|names| names.contains(*name)) {
                    continue;
                }
                let binds = match &module {
                    GlobModule::Place(place) => module_binds(code, place, name),
                    _ => true,
                };
                if binds {
                    found.push((scope.0, scope.1, (*name).to_string()));
                }
            }
        }
        Rc::new(found)
    })
}

/// `source` with every alias resolved, and the names that did not settle.
///
/// Each pass replaces the aliases that `alias_declarations` reads. A chain
/// such as `use Query as Q; type ApiQuery<T> = Q<T>;` needs one pass per
/// link, so the passes repeat until the text stops changing. A cycle, or a
/// chain longer than `ALIAS_PASSES`, never settles. Its names are returned,
/// so the audit reports each one and fails closed.
pub fn resolve_aliases(source: String) -> (String, Vec<String>) {
    let mut source = source;
    for _ in 0..ALIAS_PASSES {
        let resolved = resolve_aliases_once(&source);
        if resolved == source {
            return (source, Vec::new());
        }
        source = resolved;
    }
    // Every alias name left in the text may be a link that never settled.
    let code = code_only(&source, true);
    let mut names: Vec<String> =
        py::finditer(r"\btype\s+([A-Za-z_]\w*)|\bas\s+([A-Za-z_]\w*)", &code)
            .iter()
            .flat_map(|hit| [hit.g(1).to_string(), hit.g(2).to_string()])
            .filter(|name| !name.is_empty())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
    names.sort();
    (source, names)
}

/// Whether a local value binding hides `name` at `position`.
///
/// A `let` or a pattern that binds it, as `live_binding` reads them, hides
/// it. So does a closure parameter or a fn parameter of that name, in the
/// closure body or the fn body.
pub fn value_shadowed(code: &str, name: &str, position: usize, bound_at: isize) -> bool {
    if !live_binding(code, name, position, bound_at) {
        return true;
    }
    let variable = py::esc(name);
    let head = py::upto(code, position);
    for closure in py::finditer(r"(?<![|\w])\|([^|]*)\|", head) {
        if py::found(&format!(r"(?<![\w.]){variable}\b"), closure.g(1)) {
            let (_, stop) = closure_body_end(code, closure.end());
            if closure.start() < position && position < stop {
                return true;
            }
        }
    }
    for found in py::finditer(r"\bfn\s+\w+\s*(?:<[^>]*>)?\s*\(", head) {
        let params = paren(&code[found.end() - 1..]);
        if py::found(&format!(r"(?<![\w.]){variable}\s*:"), params) {
            let brace_at = py::find(code, "{", found.end() - 1 + params.len());
            if brace_at >= 0 {
                let brace_at = py::to_usize(brace_at);
                let end = brace_at + py::brace(&code[brace_at..]).len();
                if brace_at < position && position < end {
                    return true;
                }
            }
        }
    }
    false
}

/// `source` with each alias in `alias_declarations` replaced in its scope.
///
/// A module-level alias holds in the whole file. One in a block holds only
/// in that block, before or after the statement, as in Rust. Names in
/// comments and literals are not replaced. Newlines stay, so line numbers
/// still point at the source. Every scan reads the result, so no scan keeps
/// its own alias rule.
pub fn resolve_aliases_once(source: &str) -> String {
    let code = state::intern(&code_only(source, true));
    let mut edits: HashMap<usize, (usize, String)> = HashMap::new();
    for ((start, end), (skip_start, skip_end), pattern, replacement) in
        alias_declarations(&code).iter()
    {
        let Some(replacement) = replacement else {
            continue;
        };
        // A fn alias names a value. So a local binding of the same name, a
        // `let`, a pattern or a closure or fn parameter, stops it in its
        // scope.
        let value = serde_paths(replacement);
        let scoped = py::upto(&code, *end);
        let mut pos = *start;
        let mut must_advance = false;
        while pos <= scoped.len() {
            let Some(hit) = py::search_from(pattern, scoped, pos) else {
                break;
            };
            if must_advance && hit.start() == hit.end() && hit.start() == pos {
                pos += 1;
                must_advance = false;
                continue;
            }
            must_advance = hit.start() == hit.end();
            pos = hit.end();
            if *skip_start <= hit.start() && hit.start() < *skip_end {
                continue;
            }
            if value
                && value_shadowed(
                    &code,
                    hit.whole().trim(),
                    hit.start(),
                    py::to_isize(*skip_end),
                )
            {
                continue;
            }
            edits
                .entry(hit.start())
                .or_insert_with(|| (hit.end(), replacement.clone()));
        }
    }
    let mut positions: Vec<usize> = edits.keys().copied().collect();
    positions.sort_unstable_by(|left, right| right.cmp(left));
    let mut source = source.to_string();
    for position in positions {
        let (stop, replacement) = &edits[&position];
        let stop = (*stop).max(position).min(source.len());
        source.replace_range(position..stop, replacement);
    }
    source
}

/// `(scope start, scope end, name)` for each type alias the audit cannot
/// read.
///
/// An alias whose target names an unreadable alias is unreadable too, as in
/// `type Inner<T> = Query<Vec<T>>; type Outer<T> = Inner<T>;`. The set grows
/// until nothing changes.
pub fn unreadable_aliases(code: &Text) -> Vec<(usize, usize, String)> {
    let mut found: Vec<(usize, usize, String)> = alias_declarations(code)
        .iter()
        .filter(|(_, _, _, replacement)| replacement.is_none())
        .map(|(scope, _, pattern, _)| (scope.0, scope.1, pattern.clone()))
        .collect();
    let declared: Vec<((usize, usize), String, String)> =
        py::finditer(r"\btype\s+([A-Za-z_]\w*)[^=;]*=\s*([^;]+);", code)
            .iter()
            .map(|hit| {
                (
                    alias_scope(code, hit.start()),
                    hit.g(1).to_string(),
                    hit.g(2).to_string(),
                )
            })
            .collect();
    loop {
        let names: HashSet<String> = found.iter().map(|(_, _, name)| name.clone()).collect();
        let grown: Vec<(usize, usize, String)> = declared
            .iter()
            .filter(|(_, name, target)| {
                !names.contains(name)
                    && names
                        .iter()
                        .any(|known| py::found(&format!(r"\b{}\b", py::esc(known)), target))
            })
            .map(|(scope, name, _)| (scope.0, scope.1, name.clone()))
            .collect();
        if grown.is_empty() {
            return found;
        }
        found.extend(grown);
    }
}

/// A field type with its type aliases resolved, and an alias it cannot
/// read.
///
/// A no-parameter alias is replaced by its target. An alias whose target is
/// the same type applied to the same parameters, such as `type Maybe<T> =
/// Option<T>`, is a rename. `resolve_symbol` finds the alias from the
/// struct's `site`, so an import there decides first. The passes repeat up
/// to `ALIAS_PASSES`. Any other alias, an ambiguous one, or a chain that does
/// not settle, is returned by name. So the field is unreadable. A field
/// spelled through an alias is never read as a required non-`Option` by
/// mistake.
pub fn resolve_field_type(declared_type: &str, site: Option<&Site>) -> (String, Option<String>) {
    let mut declared_type = declared_type.to_string();
    for _ in 0..ALIAS_PASSES {
        let Some(head) = py::fullmatch(
            r"\s*((?:[A-Za-z_]\w*\s*::\s*)*)([A-Z]\w*)\s*(?:<(.*)>)?\s*",
            &declared_type,
        ) else {
            return (declared_type, None);
        };
        let reference = format!("{}{}", head.g(1), head.g(2));
        let Some(found) = resolve_symbol(Kind::Alias, &reference, site) else {
            return (declared_type.clone(), Some(head.g(2).to_string()));
        };
        let Some(Item::Alias(alias)) = found.into_iter().next() else {
            return (declared_type, None);
        };
        let arguments: Vec<String> = split_expression(head.g(3), &[","], true, None)
            .into_iter()
            .map(|part| part.trim().to_string())
            .collect();
        if alias.parameters.is_empty() && arguments.is_empty() {
            declared_type.clone_from(&alias.target);
            continue;
        }
        let renamed = py::fullmatch(r"\s*([A-Za-z_][\w:]*)\s*<(.*)>\s*", &alias.target);
        if let Some(renamed) = &renamed {
            let inner: Vec<String> = split_expression(renamed.g(2), &[","], true, None)
                .into_iter()
                .map(|part| part.trim().to_string())
                .collect();
            if inner == alias.parameters && arguments.len() == alias.parameters.len() {
                declared_type = format!("{}<{}>", renamed.g(1), arguments.join(", "));
                continue;
            }
        }
        return (declared_type.clone(), Some(head.g(2).to_string()));
    }
    let head = py::rmatch(
        r"\s*(?:[A-Za-z_]\w*\s*::\s*)*([A-Za-z_]\w*)",
        &declared_type,
    )
    .map_or_else(|| declared_type.clone(), |head| head.g(1).to_string());
    (declared_type, Some(head))
}
