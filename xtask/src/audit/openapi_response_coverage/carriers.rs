//! Raw-body carriers: which names hold the request body, where each one is
//! handed on, and which `serde_json` parse reads it.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;

use super::aliases::canonical_type;
use super::py::{self, M, balanced, paren};
use super::rejects::{
    discards_error, empty_allowed, error_rejects, live_binding, parse_type, wrapped_successes,
};
use super::scan::{
    call_receiver, local_methods, macro_invocations, method_targets, qualified_calls,
    receiver_expression, receiver_start,
};
use super::state::{self, B, Site, Text};
use super::symbols::{
    Parts, function_parts, glob_imports, handler_parts, parts_at, path_segments, resolve_fn,
    site_in, std_path, written_path,
};
use super::syntax::{
    closure_body_end, closure_value, split_expression, split_top_level, statement_end,
};
use super::tables::{
    BYTE_PARAMETER, BYTE_PRESERVING_STEP, BYTE_RETURN, BYTE_TYPE, FREE_CALL_NAME, FROM_SLICE_CALL,
    GENERIC_HELPERS, HELPER_DEPTH, NOT_CALLS, OPTION, RESULT, SCALAR_READS, SUCCESS_STEP,
    TURBOFISH, WHOLE_VIEW, conversion, receiver_read, serde_paths, std_type,
};

/// One raw-body parse: its type, whether it is optional, whether it is
/// tolerant, and its site.
pub type Parse = (Option<String>, bool, bool, Site);

/// One `let` of one plain name: name, start, value start and value.
pub type Let = (String, usize, usize, String);

/// One destructuring `let`: bound names with where, start, value start and
/// value.
pub type Destructure = (Vec<(String, usize)>, usize, usize, String);

/// How an argument hands a variable on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Way {
    Direct,
    Wrapped,
}

orc_cache!(PLAIN_LETS: String => Rc<Vec<Let>>);
orc_cache!(ASSIGNMENTS: String => Rc<Vec<Let>>);
orc_cache!(DESTRUCTURING: String => Rc<Vec<Destructure>>);
orc_cache!(CLOSURES: String => Rc<Vec<(usize, usize, usize)>>);
orc_cache!(ASYNC_BLOCKS: String => Rc<Vec<(usize, usize, usize)>>);
orc_cache!(FREE_CALLS: String => Rc<Vec<Rc<[Option<(usize, usize)>]>>>);

thread_local! {
    static RETURN_STACK: std::cell::RefCell<HashSet<usize>> = std::cell::RefCell::new(HashSet::new());
}

/// Every `FREE_CALL_NAME` match in `block`, found once.
pub fn free_calls(block: &str) -> Vec<M<'_>> {
    let spans = state::cached(&FREE_CALLS, block.to_string(), || {
        Rc::new(
            py::finditer(FREE_CALL_NAME, block)
                .iter()
                .map(M::spans)
                .collect(),
        )
    });
    spans
        .iter()
        .map(|spans| M::from_spans(block, Rc::clone(spans)))
        .collect()
}

/// Carrier names with where each was bound, and the names that are
/// partial.
///
/// A partial carrier holds part of the body, or the body inside another
/// value. A parse of it, or a handoff of it, cannot be typed, so it fails
/// closed. A plain map of names has no partial, possible, rebound or
/// derived name.
#[derive(Clone, Debug, Default)]
pub struct Carriers {
    /// Each carrier name with where it is bound, in insertion order.
    pub names: Vec<(String, isize)>,
    pub partial: BTreeSet<String>,
    pub possible: BTreeSet<String>,
    /// Each name a destructuring pattern binds anew to a carrier, with where.
    pub rebound: HashMap<String, usize>,
    /// Each name bound to a standard conversion of a carrier, with where.
    pub derived: Vec<(String, usize)>,
}

impl Carriers {
    /// A plain map of names.
    pub fn plain(names: Vec<(String, isize)>) -> Self {
        Self {
            names,
            ..Self::default()
        }
    }

    pub fn contains(&self, name: &str) -> bool {
        self.names.iter().any(|(known, _)| known == name)
    }

    pub fn get(&self, name: &str) -> Option<isize> {
        self.names
            .iter()
            .find(|(known, _)| known == name)
            .map(|(_, at)| *at)
    }

    pub fn set(&mut self, name: &str, at: isize) {
        if let Some(entry) = self.names.iter_mut().find(|(known, _)| known == name) {
            entry.1 = at;
        } else {
            self.names.push((name.to_string(), at));
        }
    }

    fn derived_get(&self, name: &str) -> Option<usize> {
        self.derived
            .iter()
            .find(|(known, _)| known == name)
            .map(|(_, at)| *at)
    }
}

/// Names of the parameters that carry the raw request body.
///
/// `params` is canonical, as `parts_at` returns it. So an alias such as
/// `type RawBody = Vec<u8>;` is already its target here.
pub fn byte_parameters(params: &str) -> BTreeSet<String> {
    py::findall1(BYTE_PARAMETER, params)
        .into_iter()
        .map(str::to_string)
        .collect()
}

/// `(type, optional, tolerant, site)` for each raw-body parse in a handler
/// and its helpers.
///
/// `optional` means an empty body can skip the parse, so the body is
/// optional. `tolerant` means the parse turns its error into a value. So a
/// present body need not carry the mandatory fields either. A guard or a
/// tolerant call at the helper call site carries into the helper.
///
/// A helper counts only when the handler passes it a body variable. Only
/// the helper parameters that receive the body are read as bodies. A helper
/// that passes the body on is followed too, as `carrier_parses` reads it.
/// The type comes from a turbofish, then from a `let` binding in the same
/// statement, then from a `Result<T, _>` return type. It is `None` when none
/// of those names it, or when the call reads the body in a form the audit
/// cannot read. The site is where the parse is, as `block_parses` reads it.
pub fn raw_body_parses(source: &Text, handler: &str) -> Vec<Parse> {
    let Some(handler_found) = handler_parts(source, handler) else {
        return Vec::new();
    };
    // `source` is masked, so a call in a comment or a string is no parse.
    let handler_block = handler_found.block();
    // A byte carrier inside a `Result` or an `Option` is seeded where its
    // success binds it, as `wrapped_successes` reads it.
    let mut seeds = Carriers::default();
    for name in byte_parameters(&handler_found.params.text) {
        seeds.set(&name, -1);
    }
    for (name, kind) in wrapped_carriers(&handler_found.params.text) {
        seeds.set(&name, -1);
        for (bound, at) in wrapped_successes(&handler_block, &name, kind) {
            seeds.set(&bound, py::to_isize(at));
        }
    }
    let mut carriers = moved_names(handler_block, seeds);
    carriers.possible.clone_from(&handler_found.params.possible);
    if carriers.names.is_empty() {
        // No byte carrier, so only a parse of a possible one can be found.
        return block_parses(handler_block, &carriers, &handler_found.returns);
    }
    carrier_parses(
        source,
        handler_block,
        &carriers,
        &handler_found.returns,
        false,
        false,
        &BTreeSet::new(),
        &handler_found.params.text,
    )
}

/// `(type, optional, tolerant, site)` for each parse of a carrier in `block`
/// and below.
///
/// Each helper that `block` passes a carrier to is read the same way, with
/// the receiving parameter as its carrier. A guard or a tolerant call at a
/// call site carries into every level below it. `path` holds each
/// `(helper, parameter)` already on this chain of calls, so a recursive
/// helper is read once. A chain deeper than `HELPER_DEPTH` is a `None`
/// type, so the audit reports it and fails closed.
#[allow(clippy::too_many_arguments)]
pub fn carrier_parses(
    source: &Text,
    block: B<'_>,
    carriers: &Carriers,
    returns: &str,
    optional: bool,
    tolerant: bool,
    path: &BTreeSet<(String, String)>,
    params: &str,
) -> Vec<Parse> {
    let mut parses: Vec<Parse> = block_parses(block, carriers, returns)
        .into_iter()
        .map(|(kind, own_optional, own_tolerant, site)| {
            (
                kind,
                own_optional || optional,
                own_tolerant || tolerant,
                site,
            )
        })
        .collect();
    let handed = handoffs(source, block, carriers, true, params);
    let unknown = site_in(block, 0);
    for handoff in handed {
        let guarded = optional || handoff.optional;
        let discarded = tolerant || handoff.tolerant;
        // A helper the audit cannot find or read gets the body all the same.
        // So it is an unresolved parse, as for a `Result` extractor handoff.
        let (Some(parts), Some(name)) = (&handoff.parts, &handoff.parameter) else {
            parses.push((None, guarded, discarded, unknown.clone()));
            continue;
        };
        if path.contains(&(handoff.helper.clone(), name.clone())) {
            continue;
        }
        // A method that gets the body as its receiver reads it as `self`. A
        // body handed to a parameter of another type cannot be followed. So
        // it is an unresolved parse.
        if name != "self" && !byte_parameters(&parts.params.text).contains(name) {
            parses.push((None, guarded, discarded, unknown.clone()));
            continue;
        }
        if path.len() >= HELPER_DEPTH {
            parses.push((None, guarded, discarded, unknown.clone()));
            continue;
        }
        let mut deeper = path.clone();
        deeper.insert((handoff.helper.clone(), name.clone()));
        let helper_block = parts.block();
        let moved = moved_names(helper_block, Carriers::plain(vec![(name.clone(), -1)]));
        parses.extend(carrier_parses(
            source,
            helper_block,
            &moved,
            &parts.returns,
            guarded,
            discarded,
            &deeper,
            &parts.params.text,
        ));
    }
    parses
}

/// The carrier that a standard conversion in `expression` reads whole, or
/// `None`.
///
/// A standard conversion is a call of a `std`, `core` or `alloc` path, or of
/// a standard type such as `String::from_utf8`. It has one argument that
/// reads a live, whole carrier. A trailing success projection or `.map_err(..)`
/// keeps it, so `std::str::from_utf8(&body)?` reads `body`. The argument may
/// copy the carrier through `BYTE_PRESERVING`. So
/// `String::from_utf8(body.to_vec())` reads `body` too. `handoffs` fails
/// closed on a standard conversion of any other part of a carrier.
pub fn std_conversion_source(
    block: B<'_>,
    carriers: &Carriers,
    expression: &str,
    position: usize,
) -> Option<String> {
    let mut text = expression.trim().trim_end_matches(',').trim().to_string();
    let mapped = r"\.\s*map_err\s*\((?:[^()]|\([^()]*\))*\)";
    let step_pattern = format!(r"\s*(?:{SUCCESS_STEP}|{mapped})$");
    loop {
        if let Some(lead) = py::rmatch(r"(?:&\s*(?:mut\b)?|\*)\s*", &text)
            && lead.end() > 0
        {
            text = text[lead.end()..].trim().to_string();
            continue;
        }
        if text.starts_with('(') && paren(&text).len() == text.len() {
            text = py::sl(&text, 1, text.len() - 1).trim().to_string();
            continue;
        }
        if let Some(step) = py::search(&step_pattern, &text)
            && step.start() > 0
        {
            text = text[..step.start()].trim().to_string();
            continue;
        }
        break;
    }
    let call = py::rmatch(r"((?:[A-Za-z_]\w*\s*::\s*)+[a-z_]\w*)\s*\(", &text)?;
    if paren(&text[call.end() - 1..]).len() != text.len() - call.end() + 1 {
        return None;
    }
    let arguments = split_top_level(py::sl(&text, call.end(), text.len().saturating_sub(1)));
    if !std_conversion(call.g(1), &site_in(block, position)) || arguments.len() != 1 {
        return None;
    }
    let (root, whole) = argument_root(&arguments[0], true, true);
    let root = root?;
    if !whole || !carriers.contains(&root) || !carrier_live(&block, carriers, &root, position) {
        return None;
    }
    if carrier_partial(carriers, &root, position) {
        None
    } else {
        Some(root)
    }
}

/// Whether the call path `path` names a standard conversion.
///
/// That is a path through `std`, `core` or `alloc`, as `std_path` reads it,
/// or a path that starts at one of `STD_TYPES`, such as `String::from_utf8`.
pub fn std_conversion(path: &str, site: &Site) -> bool {
    std_path(path, site)
        || path_segments(path)
            .first()
            .is_some_and(|head| std_type(head))
}

/// Whether the call path `reference` names a `serde_json` item.
///
/// An import decides first, as `written_path` reads it. So
/// `use serde_json::from_value as decode;` makes `decode` a `serde_json`
/// path. A
/// bare name under a glob of `serde_json` counts when no local fn has it.
pub fn serde_path(reference: &str, site: &Site) -> bool {
    let (segments, _) = written_path(reference, site);
    if segments.is_empty() {
        return false;
    }
    if segments[0] == "serde_json" {
        return true;
    }
    let Some(at) = site.at else {
        return false;
    };
    if segments.len() != 1 {
        return false;
    }
    let covered = glob_imports(&site.text)
        .iter()
        .filter(|(_, path, _)| path == "serde_json")
        .any(|((start, end), _, _)| *start <= at && at < *end);
    covered && resolve_fn(&segments[0], Some(site)).is_none_or(|found| found.is_empty())
}

/// Whether the carrier `name` holds the body at `position`.
///
/// The first binding decides, as `live_binding` reads it. A destructuring
/// pattern that binds the same name anew to a carrier, as `moved_names`
/// records it, holds the body from its own binding on.
pub fn carrier_live(block: &str, carriers: &Carriers, name: &str, position: usize) -> bool {
    if live_binding(block, name, position, carriers.get(name).unwrap_or(-1)) {
        return true;
    }
    carriers.rebound.get(name).is_some_and(|rebind| {
        position >= *rebind && live_binding(block, name, position, py::to_isize(*rebind))
    })
}

/// Whether the carrier `name` holds only part of the body at `position`.
pub fn carrier_partial(carriers: &Carriers, name: &str, position: usize) -> bool {
    if carriers.partial.contains(name) {
        return true;
    }
    carriers
        .rebound
        .get(name)
        .is_some_and(|rebind| position >= *rebind)
}

/// `(name, start, value start, value)` for each `let` of one plain name in
/// `block`.
pub fn plain_lets(block: &str) -> Rc<Vec<Let>> {
    state::cached(&PLAIN_LETS, block.to_string(), || {
        Rc::new(
            py::finditer(
                r"\blet\s+(?:mut\s+)?([a-z_][a-z_0-9]*)\s*(?::[^=;]*)?=(?!=)",
                block,
            )
            .iter()
            .map(|found| {
                let value = py::sl(block, found.end(), statement_end(block, found.end()));
                (
                    found.g(1).to_string(),
                    found.start(),
                    found.end(),
                    value.to_string(),
                )
            })
            .collect(),
        )
    })
}

/// `names` plus each name that a `let` binds to one of them, with where.
///
/// Each `let` value goes through `argument_passes`. A value that is a whole
/// carrier, such as `body`, `&body` or `body.as_ref()`, makes a carrier
/// alias, bound at that `let`. A value that holds a carrier in any other
/// way, such as `&body[4..]`, `body.to_vec()` or a struct literal, makes a
/// partial carrier. An alias of a partial carrier is partial too. Each name
/// of `names` comes with where it is bound, or -1 for a parameter. A `let`
/// counts only while its source still holds the body, as `live_binding`
/// reads it. A read through a call, such as `from_utf8(&body)`, is the
/// call's own handoff, so it binds no carrier.
pub fn moved_names(block: B<'_>, names: Carriers) -> Carriers {
    let mut found = Carriers::plain(names.names);
    let mut partial: BTreeSet<String> = BTreeSet::new();
    let lets = plain_lets(&block);
    let patterns = destructuring_lets(&block);
    let assigned: Vec<Let> = assignments(&block)
        .iter()
        .filter(|item| py::full(r"[a-z_][a-z_0-9]*", &item.0))
        .cloned()
        .collect();
    loop {
        // The partial names so far, so `std_conversion_source` sees them.
        found.partial.clone_from(&partial);
        let mut moved: Vec<(String, (isize, bool))> = Vec::new();
        let moved_has = |moved: &Vec<(String, (isize, bool))>, name: &str| {
            moved.iter().any(|(known, _)| known == name)
        };
        for (bound, start, opener, value) in lets.iter() {
            if found.contains(bound) || moved_has(&moved, bound) {
                continue;
            }
            let mut matched = false;
            for (name, bound_at) in found.names.clone() {
                let way = live_passes(&block, value, *opener, &name, bound_at);
                if way.is_some() && live_binding(&block, &name, *start, bound_at) {
                    let is_partial = way == Some(Way::Wrapped) || partial.contains(&name);
                    moved.push((bound.clone(), (py::to_isize(*start), is_partial)));
                    matched = true;
                    break;
                }
            }
            if !matched {
                // A helper that returns a byte type hands its result back.
                if let Some(result) = helper_result(block, &found, value, *opener) {
                    moved.push((bound.clone(), (py::to_isize(*start), result == "partial")));
                }
            }
        }
        // A carrier that flows into a destructuring pattern makes each name
        // the pattern binds a partial carrier. So a parse of it fails closed.
        for (binds, start, opener, value) in patterns.iter() {
            // A pattern whose names are all seeded here, as the success
            // binding of a wrapped carrier is, or already rebound here, is
            // settled.
            if binds
                .iter()
                .all(|(bound, _)| found.contains(bound) || moved_has(&moved, bound))
            {
                let settled =
                    binds
                        .iter()
                        .filter(|(bound, _)| found.contains(bound))
                        .all(|(bound, at)| {
                            found.rebound.get(bound) == Some(at)
                                || found.get(bound) == Some(py::to_isize(*at))
                        });
                if settled {
                    continue;
                }
            }
            let flows = found.names.clone().into_iter().any(|(name, bound_at)| {
                live_passes(&block, value, *opener, &name, bound_at).is_some()
                    && carrier_live(&block, &found, &name, *start)
            });
            if !flows {
                continue;
            }
            for (bound, at) in binds {
                if let Some(current) = found.get(bound) {
                    if found.rebound.get(bound) != Some(at) {
                        found.rebound.insert(bound.clone(), *at);
                        if !moved_has(&moved, bound) {
                            moved.push((bound.clone(), (current, partial.contains(bound))));
                        }
                    }
                } else if !moved_has(&moved, bound) {
                    moved.push((bound.clone(), (py::to_isize(*at), true)));
                }
            }
        }
        // A `let` or an assignment whose value is a standard conversion of a
        // carrier, or another derived name, binds a derived name. Only a
        // serde_json parse reads it, as `block_parses` does.
        let derived_before = found.derived.len();
        for (bound, start, opener, value) in lets.iter().chain(assigned.iter()) {
            if found.contains(bound) || found.derived_get(bound).is_some() {
                continue;
            }
            let converted = std_conversion_source(block, &found, value, *start);
            let aliased = found.derived.clone().into_iter().any(|(name, at)| {
                live_passes(&block, value, *opener, &name, py::to_isize(at)) == Some(Way::Direct)
                    && live_binding(&block, &name, *start, py::to_isize(at))
            });
            if converted.is_some() || aliased {
                found.derived.push((bound.clone(), *start));
            }
        }
        // A plain assignment `name = value;` moves a carrier as a `let`
        // does. One that gives a carrier name another value leaves it
        // partial from there. That fails closed, since a branch may skip
        // it. That is decided only once no name moves. A later pass can
        // find a carrier in the value, as in a cycle of plain moves.
        let mut rebinds: Vec<(String, usize)> = Vec::new();
        for (target, start, opener, value) in &assigned {
            let flows: Vec<Option<Way>> = found
                .names
                .clone()
                .into_iter()
                .filter(|(name, _)| carrier_live(&block, &found, name, *start))
                .map(|(name, bound_at)| live_passes(&block, value, *opener, &name, bound_at))
                .collect();
            let mut way = if flows.contains(&Some(Way::Wrapped)) {
                Some(Way::Wrapped)
            } else if flows.contains(&Some(Way::Direct)) {
                Some(Way::Direct)
            } else {
                None
            };
            if way.is_none() {
                way = match helper_result(block, &found, value, *opener).as_deref() {
                    Some("whole") => Some(Way::Direct),
                    Some("partial") => Some(Way::Wrapped),
                    _ => None,
                };
            }
            if found.contains(target) {
                let rebound = found.rebound.get(target).copied().unwrap_or(block.len());
                if way != Some(Way::Direct) && rebound > *start {
                    rebinds.push((target.clone(), *start));
                }
                continue;
            }
            if let Some(way) = way
                && !moved_has(&moved, target)
            {
                moved.push((target.clone(), (py::to_isize(*start), way == Way::Wrapped)));
            }
        }
        if moved.is_empty() && found.derived.len() == derived_before {
            if !rebinds.is_empty() {
                for (target, start) in rebinds {
                    let current = found.rebound.get(&target).copied().unwrap_or(start);
                    found.rebound.insert(target, start.min(current));
                }
                continue;
            }
            found.partial.clone_from(&partial);
            return found;
        }
        for (bound, (at, is_partial)) in moved {
            found.set(&bound, at);
            if is_partial {
                partial.insert(bound);
            }
        }
    }
}

/// How a `let` value at `opener` holds a carrier that a helper returns.
///
/// The value can be one call that `byte_call` reads, with any trailing
/// success projection. Then the result is `"whole"` when `returns_carrier`
/// proves that the helper returns the carrier, and `"partial"` otherwise.
/// Such a call can also be anywhere else in the value, as in a branch, a
/// block, a match arm or a group. Then the result is `"partial"`. The audit does not
/// model which branch runs, so a later parse fails closed. A value that
/// holds a carrier outside every call is read by `argument_passes`, the
/// same partial rule. Any other value is `None`. It holds no carrier the
/// audit models. A helper the audit cannot resolve is already an unresolved
/// handoff.
pub fn helper_result(
    block: B<'_>,
    carriers: &Carriers,
    value: &str,
    opener: usize,
) -> Option<String> {
    let live = Carriers::plain(
        carriers
            .names
            .iter()
            .filter(|(name, _)| carrier_live(&block, carriers, name, opener))
            .cloned()
            .collect(),
    );
    if !live
        .names
        .iter()
        .any(|(name, _)| py::found(&format!(r"(?<![.\w]){}\b", py::esc(name)), value))
    {
        return None;
    }
    let mut text = value.trim().trim_end_matches(';').trim().to_string();
    let step_pattern = format!(r"\s*{SUCCESS_STEP}$");
    loop {
        match py::search(&step_pattern, &text) {
            Some(step) if step.start() > 0 => {
                text = text[..step.start()].trim_end().to_string();
            }
            _ => break,
        }
    }
    let at = opener + value.len() - value.trim_start().len();
    if let Some((parts, names)) = byte_call(block, &live, at, at + text.len()) {
        if names.contains(&None) {
            return Some("partial".to_string());
        }
        let names: BTreeSet<String> = names.into_iter().flatten().collect();
        return Some(if returns_carrier(&parts, &names) {
            "whole".to_string()
        } else {
            "partial".to_string()
        });
    }
    // A byte-returning call that gets a carrier inside a larger expression.
    let stop = opener + value.len();
    // A method call is read from its `.`, as `byte_call` finds it by range.
    let mut spans: Vec<(usize, usize)> = free_calls(&block)
        .iter()
        .map(|found| (found.start(), found.end()))
        .collect();
    spans.extend(
        qualified_calls(&block)
            .iter()
            .map(|found| (found.start(), found.end())),
    );
    for (start, head_end) in spans {
        if !(opener <= start && start < stop) {
            continue;
        }
        let end = head_end - 1 + paren(&block[head_end - 1..]).len();
        if end <= stop && byte_call(block, &live, start, end).is_some() {
            return Some("partial".to_string());
        }
    }
    None
}

/// `(parts, names)` when `block[start:end]` is one call of a byte-returning
/// helper.
///
/// A free or path-qualified call resolves at the call site, as `call_sites`
/// resolves it. A method call reaches what `method_targets` finds. The
/// helper must get a carrier of `live` as an argument, and `names` holds
/// each parameter that gets one, as `receiving_parameters` maps it. A
/// receiver that holds a carrier is a move, as `moved_names` reads it. The
/// return type must be a byte type or wrap one, as `BYTE_RETURN` reads it.
/// Any other text is `None`.
pub fn byte_call(
    block: B<'_>,
    live: &Carriers,
    start: usize,
    end: usize,
) -> Option<(Rc<Parts>, BTreeSet<Option<String>>)> {
    let source = state::source();
    let text = py::sl(&block, start, end);
    let head =
        format!(r"((?:[A-Za-z_]\w*(?:\s*::\s*<[^()]*?>)?\s*::\s*)*)([a-z_]\w*){TURBOFISH}\s*\(");
    let call = py::rmatch(&head, text)
        .filter(|call| paren(&text[call.end() - 1..]).len() == text.len() - call.end() + 1);
    let (helper, parts, qualified, at) = if let Some(call) = call {
        let path = py::no_space(call.g(1));
        let helper = call.g(2).to_string();
        let site = site_in(block, start);
        let joined = format!("{path}{helper}");
        if NOT_CALLS.contains(&helper.as_str())
            || GENERIC_HELPERS.contains(&helper.as_str())
            || std_conversion(&joined, &site)
        {
            return None;
        }
        if serde_path(&joined, &site) {
            return None;
        }
        let reference = if path.is_empty() {
            helper.clone()
        } else {
            format!("{}::{helper}", path.trim_end_matches(':'))
        };
        let parts = function_parts(&source, &reference, Some(&site));
        let qualified = (!path.is_empty()).then_some(path);
        (helper, parts, qualified, start)
    } else {
        // A method call whose argument list ends the text.
        let methods: Vec<M<'_>> = qualified_calls(&block)
            .into_iter()
            .filter(|found| {
                found.g(1).trim() == "."
                    && start <= found.start()
                    && found.start() < end
                    && found.end() - 1 + paren(&block[found.end() - 1..]).len() == end
            })
            .collect();
        if methods.len() != 1 {
            return None;
        }
        let helper = methods[0].g(2).to_string();
        let at = methods[0].start();
        if GENERIC_HELPERS.contains(&helper.as_str()) {
            return None;
        }
        let receiver = call_receiver(&block, at);
        let targets = method_targets(block, "", receiver.as_deref(), &helper, at);
        let parts = match targets.as_deref() {
            Some([only]) => parts_at(&source, *only),
            _ => None,
        };
        (helper, parts, Some(".".to_string()), at)
    };
    let parts = parts?;
    if !py::found(BYTE_RETURN, &parts.returns) {
        return None;
    }
    let names: BTreeSet<Option<String>> = receiving_parameters(
        block,
        &helper,
        &parts.params.text,
        live,
        qualified.as_deref(),
        Some(at),
    )
    .into_iter()
    .map(|(name, _)| name)
    .collect();
    if names.is_empty() {
        None
    } else {
        Some((parts, names))
    }
}

/// Whether every value the fn of `parts` returns is a whole carrier from
/// `names`.
///
/// The exits are the tail of the block and each `return`. An `Ok(..)` or a
/// `Some(..)` exit is read inside, and an `Err(..)` or a `None` exit returns
/// no body. Each value must be a live, whole carrier of the block, as
/// `moved_names` reads it from `names`. A helper already being read returns
/// nothing the audit can prove.
pub fn returns_carrier(parts: &Parts, names: &BTreeSet<String>) -> bool {
    let key = parts.start;
    if RETURN_STACK.with(|stack| stack.borrow().contains(&key)) {
        return false;
    }
    RETURN_STACK.with(|stack| stack.borrow_mut().insert(key));
    let outcome = returns_carrier_inner(parts, names);
    RETURN_STACK.with(|stack| stack.borrow_mut().remove(&key));
    outcome
}

fn returns_carrier_inner(parts: &Parts, names: &BTreeSet<String>) -> bool {
    let helper_block = parts.block();
    let seeds = Carriers::plain(names.iter().map(|name| (name.clone(), -1)).collect());
    let carriers = moved_names(helper_block, seeds);
    let tail = closure_value(&helper_block);
    let mut exits: Vec<(String, usize)> = Vec::new();
    if !tail.is_empty() {
        exits.push((
            tail.to_string(),
            py::to_usize(py::rfind(&helper_block, tail)),
        ));
    }
    for found in py::finditer(r"\breturn\b([^;]*);", &helper_block) {
        exits.push((found.g(1).to_string(), found.start()));
    }
    if exits.is_empty() {
        return false;
    }
    for (expression, position) in exits {
        let mut expression = expression.trim().to_string();
        let wrapped = py::fullmatch(r"(?s)(?:Ok|Some)\s*\((.*)\)", &expression)
            .map(|wrapped| wrapped.g(1).to_string());
        let open = expression.find('(').unwrap_or(0);
        if let Some(inner) = wrapped
            && paren(&expression[open..]).len() == expression.len() - open
        {
            expression = inner;
        } else if py::full(r"(?s)Err\s*\(.*\)|None", &expression) {
            continue;
        }
        let (root, whole) = argument_root(&expression, false, true);
        let Some(root) = root.filter(|root| whole && carriers.contains(root)) else {
            return false;
        };
        if carrier_partial(&carriers, &root, position) {
            return false;
        }
        if !carrier_live(&helper_block, &carriers, &root, position) {
            return false;
        }
    }
    true
}

/// How a `let` value at `opener` in `block` hands on the carrier `name`.
///
/// A use of `name` that a pattern in the value binds anew, as in the arm
/// `Ok(body) => body.into_request()`, is another value. So it is blanked
/// first. `argument_passes` reads what is left.
pub fn live_passes(
    block: &str,
    value: &str,
    opener: usize,
    name: &str,
    bound_at: isize,
) -> Option<Way> {
    let pattern = format!(r"(?<![.\w]){}\b", py::esc(name));
    let uses = py::finditer(&pattern, value);
    if uses.is_empty() {
        return None;
    }
    let mut value = value.to_string();
    for found in uses.iter().rev() {
        if !live_binding(block, name, opener + found.start(), bound_at) {
            value.replace_range(found.start()..found.end(), &" ".repeat(name.len()));
        }
    }
    argument_passes(&value, name)
}

/// `(target, start, value start, value)` for each assignment statement in
/// `block`.
///
/// An assignment is a top-level `=` in a statement that no keyword starts.
/// So a `let`, a comparison, a `=>` and a named macro argument are none. The
/// target is the text before the `=`, such as `bytes`, `self.x` or `*p`.
pub fn assignments(block: &str) -> Rc<Vec<Let>> {
    state::cached(&ASSIGNMENTS, block.to_string(), || {
        let keyword = r"(?:let|if|while|for|match|return|else|const|static|type|use|pub)\b|[#|]";
        let mut found = Vec::new();
        for equals in py::finditer(r"(?<![=!<>+\-*/%&|^:])=(?![=>])", block) {
            let before = equals.start();
            let start = py::rfind_before(block, ";", before)
                .max(py::rfind_before(block, "{", before))
                .max(py::rfind_before(block, "}", before))
                + 1;
            let start = py::to_usize(start);
            let target = &block[start..before];
            let depth = py::count_byte(target, b'(') + py::count_byte(target, b'[')
                - py::count_byte(target, b')')
                - py::count_byte(target, b']');
            let stripped = target.trim();
            if depth != 0 || stripped.is_empty() || py::matches(keyword, stripped) {
                continue;
            }
            let opener = equals.end();
            let at = start + target.len() - target.trim_start().len();
            let value = py::sl(block, opener, statement_end(block, opener));
            found.push((stripped.to_string(), at, opener, value.to_string()));
        }
        Rc::new(found)
    })
}

/// `(bound names, start, value start, value)` for each `let` whose pattern
/// is not one name.
///
/// It reads a plain `let`, an `if let` and a `while let` with a tuple,
/// struct, slice, reference or variant pattern. Each bound name comes with
/// where the pattern binds it. A name followed by `(`, `{`, `::` or a
/// single `:` is a path or a field, not a binding.
pub fn destructuring_lets(block: &str) -> Rc<Vec<Destructure>> {
    state::cached(&DESTRUCTURING, block.to_string(), || {
        Rc::new(destructuring_lets_uncached(block))
    })
}

fn destructuring_lets_uncached(block: &str) -> Vec<Destructure> {
    let bytes = block.as_bytes();
    let mut found = Vec::new();
    for found_let in py::finditer(r"\blet\s+", block) {
        let rest = &block[found_let.end()..];
        if py::matches(r"(?:mut\s+)?[a-z_][a-z_0-9]*\s*(?::(?!:)|=(?![=>]))", rest) {
            continue;
        }
        let rest_bytes = rest.as_bytes();
        let mut depth = 0i64;
        let mut equals = None;
        let mut colon = None;
        for (index, &char) in rest_bytes.iter().enumerate() {
            let signed = py::to_isize(index);
            if b"([{".contains(&char) {
                depth += 1;
            } else if b")]}".contains(&char) {
                depth -= 1;
                if depth < 0 {
                    break;
                }
            } else if depth == 0 && char == b';' {
                break;
            } else if depth == 0
                && char == b':'
                && py::count(py::ps(rest, signed - 1, signed + 2), ":") == 1
            {
                colon = colon.or(Some(index));
            } else if depth == 0
                && char == b'='
                && !matches!(py::ps(rest, signed + 1, signed + 2), "=" | ">")
            {
                if !matches!(py::ps(rest, signed - 1, signed), "=" | "!" | "<" | ">") {
                    equals = Some(index);
                    break;
                }
            }
        }
        let Some(equals) = equals else {
            continue;
        };
        let pattern = &rest[..colon.unwrap_or(equals)];
        let opener = found_let.end() + equals + 1;
        let head = py::sl(
            block,
            found_let.start().saturating_sub(12),
            found_let.start(),
        );
        let value_end = if py::found(r"\b(?:if|while)\s*$", head) {
            let mut value_end = opener;
            let mut depth = 0i64;
            while value_end < bytes.len() && !(depth == 0 && bytes[value_end] == b'{') {
                depth += i64::from(b"([".contains(&bytes[value_end]));
                depth -= i64::from(b")]".contains(&bytes[value_end]));
                value_end += 1;
            }
            value_end
        } else {
            statement_end(block, opener)
        };
        let binds: Vec<(String, usize)> = py::finditer(r"(?<![\w:.])[a-z_][a-z_0-9]*\b", pattern)
            .iter()
            .filter(|name| {
                !matches!(name.whole(), "ref" | "mut" | "box" | "_")
                    && !py::matches(r"\s*(?:\(|\{|::|:(?!:))", &pattern[name.end()..])
            })
            .map(|name| (name.whole().to_string(), found_let.end() + name.start()))
            .collect();
        if !binds.is_empty() {
            let value = py::sl(block, opener, value_end).to_string();
            found.push((binds, found_let.start(), opener, value));
        }
    }
    found
}

/// The variable an argument reads, and whether it reads all of it.
///
/// `&`, `&mut`, `*` and grouping parentheses are stripped. So are whole
/// views in `WHOLE_VIEW` and success projections in `SUCCESS_STEP`. So
/// `body.as_ref().unwrap()` reads a wrapped carrier `body` whole. With
/// `copies`, each step of `BYTE_PRESERVING_STEP` is stripped too, so
/// `body.to_vec()` reads `body` whole. With `projections` false, only
/// borrows, dereferences and grouping are stripped. What is left must be a
/// plain name, which is then the root, read whole. A name followed by
/// anything else, such as `body[4..]` or `body.to_vec()`, is the root, read
/// in a form the audit cannot type. Any other argument has no root. Every
/// carrier and argument reader in the audit strips borrows through this one
/// step.
pub fn argument_root(argument: &str, copies: bool, projections: bool) -> (Option<String>, bool) {
    let mut text = argument.trim().trim_end_matches(',').trim().to_string();
    let views = if copies {
        BYTE_PRESERVING_STEP
    } else {
        WHOLE_VIEW
    };
    let view_pattern = format!(r"\s*(?:{views}|{SUCCESS_STEP})$");
    loop {
        if let Some(lead) = py::rmatch(r"(?:&\s*(?:mut\b)?|\*)\s*", &text)
            && lead.end() > 0
        {
            text = text[lead.end()..].trim().to_string();
            continue;
        }
        if text.starts_with('(') && paren(&text).len() == text.len() {
            text = py::sl(&text, 1, text.len() - 1).trim().to_string();
            continue;
        }
        if projections
            && let Some(view) = py::search(&view_pattern, &text)
            && view.start() > 0
        {
            text = text[..view.start()].trim().to_string();
            continue;
        }
        break;
    }
    if text == "self" || (text != "mut" && py::full(r"[a-z_][a-z_0-9]*", &text)) {
        return (Some(text), true);
    }
    match py::rmatch(r"([a-z_][a-z_0-9]*)\b", &text) {
        Some(head) if head.g(1) != "mut" => (Some(head.g(1).to_string()), false),
        _ => (None, false),
    }
}

/// How a call argument hands on `variable`: direct, wrapped or not at all.
///
/// It is direct when the whole argument, borrows stripped, is the variable,
/// as `argument_root` reads it. It is wrapped when the variable appears
/// anywhere else in it, as in a struct literal, a tuple, an array, a
/// closure or a macro. A use inside the argument list of an inner call is
/// that call's own handoff, and so is a receiver of one of `SCALAR_READS`.
/// A wrapped handoff cannot be followed, so the caller fails closed.
pub fn argument_passes(argument: &str, variable: &str) -> Option<Way> {
    let (root, whole) = argument_root(argument, false, true);
    if whole && root.as_deref() == Some(variable) {
        return Some(Way::Direct);
    }
    let visible = outside_calls(argument, false);
    let pattern = format!(r"(?<![.\w]){}\b", py::esc(variable));
    for found in py::finditer(&pattern, &visible) {
        let read = py::rmatch(r"\s*\.\s*([a-z_]\w*)\s*\(", &visible[found.end()..]);
        match read {
            Some(read) if SCALAR_READS.contains(&read.g(1)) => {}
            _ => return Some(Way::Wrapped),
        }
    }
    None
}

/// `argument` with the argument list of each call in it blanked.
///
/// A name inside `normalize(body)` is passed to `normalize`, not to the call
/// that receives its result. So only a name outside every inner call is
/// passed on. A receiver such as `body` in `body.as_ref()` stays. With
/// `macros` false, a macro's arguments stay, since a macro is no call.
pub fn outside_calls(argument: &str, macros: bool) -> String {
    let pattern = if macros {
        r"[\w!>]\s*\("
    } else {
        r"[\w>]\s*\("
    };
    let mut out = argument.to_string();
    for call in py::finditer(pattern, argument) {
        let opener = call.end() - 1;
        let inner = paren(&argument[opener..]);
        let last = inner.chars().next_back().map_or(1, char::len_utf8);
        let keep_from = opener + inner.len() - last;
        let blank = keep_from.saturating_sub(opener + 1);
        let head = py::upto(&out, opener + 1).to_string();
        let tail = py::from(&out, keep_from).to_string();
        out = format!("{head}{}{tail}", " ".repeat(blank));
    }
    out
}

/// The `from` or `try_from` fn that a conversion `method` runs, and its
/// parameter.
///
/// `start` is where the receiver of the conversion call starts in `block`.
///
/// The target type must be written in the same statement, as in `let x: T =
/// body.into();`. `resolve_symbol` then finds `T::from`, or `T::try_from`
/// for `try_into`, from the call site. A `Result<T, _>` annotation names
/// `T`. A target the audit cannot see, or a constructor it cannot find, is
/// `(None, None)`. So the handoff fails closed.
pub fn conversion_parts(
    source: &Text,
    block: B<'_>,
    method: &str,
    start: usize,
) -> (Option<Rc<Parts>>, Option<String>) {
    let annotated = r"\blet\s+(?:mut\s+)?[a-z_]\w*\s*:\s*([^=;]+?)\s*=\s*$";
    let Some(typed) = py::search(annotated, py::upto(&block, start)) else {
        return (None, None);
    };
    let typed_at = py::to_usize(typed.start_of(1));
    let (target, _) = canonical_type(typed.g(1), Some(&site_in(block, typed_at)));
    let wrapped = py::fullmatch(r"(?s)Result<\s*(.+?)\s*,.*>", &target);
    let inner = wrapped
        .as_ref()
        .map_or(target.as_str(), |wrapped| wrapped.g(1));
    let Some(head) = py::rmatch(r"((?:[A-Za-z_]\w*\s*::\s*)*[A-Z]\w*)", inner) else {
        return (None, None);
    };
    let constructor = conversion(method).unwrap_or("from");
    let reference = format!("{}::{constructor}", head.g(1));
    let starts = resolve_fn(&reference, Some(&site_in(block, start))).unwrap_or_default();
    let parts = if starts.len() == 1 {
        parts_at(source, starts[0])
    } else {
        None
    };
    let first = parts.as_ref().and_then(|parts| {
        py::rmatch(r"\(\s*(?:mut\s+)?([a-z_]\w*)\s*:", &parts.params.text)
            .map(|first| first.g(1).to_string())
    });
    match first {
        Some(first) => (parts, Some(first)),
        None => (None, None),
    }
}

/// One carrier handoff: the helper, its parts, the receiving parameter, and
/// whether it is optional and tolerant.
#[derive(Clone, Debug)]
pub struct Handoff {
    pub helper: String,
    pub parts: Option<Rc<Parts>>,
    pub parameter: Option<String>,
    pub optional: bool,
    pub tolerant: bool,
}

/// What a merged handoff is keyed by: a resolved block, or a call site.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Identity {
    Block(usize),
    Call(String, usize),
}

/// `(helper, parts, parameter, optional, tolerant)` for each carrier handoff.
///
/// A handoff is a call in `block` that gets a live carrier as an argument,
/// outside any inner call. `call_sites` resolves each call at its own site.
/// `parts` is `None` when the audit cannot find the helper. `parameter` is
/// `None` when the receiving parameter has no plain name. Both the raw-body
/// scan and the `Result` extractor scan read their helpers through this one
/// step.
///
/// With `receivers`, a raw body that is the receiver of a method call, as in
/// `body.decode::<T>()`, is handed to that method's `self`. The exception is
/// a method of `BYTE_ACCESSORS`. The method is found through the symbol
/// index. An unknown or ambiguous one has no `parts`, so the caller fails
/// closed.
///
/// A method call such as `x.decode(body)` reaches only the method of the
/// type that `receiver_type` finds for `x` in `block` or `params`. The audit
/// infers no other type, so a receiver of unknown type has no `parts`.
pub fn handoffs(
    source: &Text,
    block: B<'_>,
    carriers: &Carriers,
    receivers: bool,
    params: &str,
) -> Vec<Handoff> {
    // Each call resolves at its own site, since block-scoped imports can bind
    // one name to different helpers. Calls that reach the same definition
    // and parameter merge: optional and tolerant only when every call is.
    let mut merged: Vec<((Identity, Option<String>), Handoff)> = Vec::new();
    for (helper, parts, at, qualified) in call_sites(source, block, params) {
        let helper_params = parts
            .as_ref()
            .map_or("", |parts| parts.params.text.as_str());
        let states = receiving_parameters(
            block,
            &helper,
            helper_params,
            carriers,
            qualified.as_deref(),
            Some(at),
        );
        let identity = parts.as_ref().map_or_else(
            || Identity::Call(helper.clone(), at),
            |parts| Identity::Block(parts.start),
        );
        for (name, (mut optional, mut tolerant)) in states {
            let merge_key = (identity.clone(), name.clone());
            let made = |optional, tolerant| Handoff {
                helper: helper.clone(),
                parts: parts.clone(),
                parameter: name.clone(),
                optional,
                tolerant,
            };
            if let Some(entry) = merged.iter_mut().find(|(known, _)| *known == merge_key) {
                optional = optional && entry.1.optional;
                tolerant = tolerant && entry.1.tolerant;
                entry.1 = made(optional, tolerant);
            } else {
                merged.push((merge_key, made(optional, tolerant)));
            }
        }
    }
    let mut found: Vec<Handoff> = merged.into_iter().map(|(_, handoff)| handoff).collect();
    let unresolved =
        |helper: String, optional: bool, tolerant: bool, parameter: Option<&str>| Handoff {
            helper,
            parts: None,
            parameter: parameter.map(str::to_string),
            optional,
            tolerant,
        };
    if !receivers {
        return found;
    }
    // An assignment to a target the audit cannot read, such as `self.x` or
    // `*p`, moves a live carrier out of its view. So it fails closed.
    for (target, start, opener, value) in assignments(&block).iter() {
        if py::full(r"[a-z_][a-z_0-9]*", target) || in_dead_closure(&block, *start) {
            continue;
        }
        for (variable, _) in &carriers.names {
            if argument_passes(value, variable).is_none() {
                continue;
            }
            if !carrier_live(&block, carriers, variable, *start) {
                continue;
            }
            let end = opener + value.len();
            let tolerant = discards_error(py::upto(&block, *start), py::from(&block, end));
            let allowed = empty_allowed(block, *start, variable, tolerant);
            found.push(unresolved("=".to_string(), allowed, tolerant, None));
        }
    }
    // A macro whose token tree holds a live carrier hands it on in a form the
    // audit cannot follow. So the handoff is unresolved and fails closed.
    for (name, start, tree, end) in macro_invocations(&block).iter() {
        if in_dead_closure(&block, *start) {
            continue;
        }
        for (variable, _) in &carriers.names {
            if argument_passes(tree, variable).is_none() {
                continue;
            }
            if !carrier_live(&block, carriers, variable, *start) {
                continue;
            }
            let tolerant = discards_error(py::upto(&block, *start), py::from(&block, *end));
            let allowed = empty_allowed(block, *start, variable, tolerant);
            found.push(unresolved(format!("{name}!"), allowed, tolerant, None));
        }
    }
    for (helper, start, arguments, end) in std_and_serde_calls(&block) {
        if in_dead_closure(&block, start) {
            continue;
        }
        let tolerant = discards_error(py::upto(&block, start), py::from(&block, end));
        let site = site_in(block, start);
        // A standard conversion reads a carrier whole only as its one
        // argument, as `std_conversion_source` does. Any other read fails
        // closed. A partial carrier or a part of one, such as `&body[1..]`,
        // has no type.
        if std_conversion(&helper, &site) {
            for argument in &arguments {
                for (variable, _) in &carriers.names {
                    if argument_passes(argument, variable).is_none() {
                        continue;
                    }
                    if !carrier_live(&block, carriers, variable, start) {
                        continue;
                    }
                    let (root, whole) = argument_root(argument, true, true);
                    let whole = whole && root.as_deref() == Some(variable.as_str());
                    if whole && arguments.len() == 1 && !carrier_partial(carriers, variable, start)
                    {
                        continue;
                    }
                    let allowed = empty_allowed(block, start, variable, tolerant);
                    found.push(unresolved(helper.clone(), allowed, tolerant, None));
                }
            }
        }
        // A derived name reaches no serde_json call but one of
        // `SERDE_PARSERS`, which `block_parses` reads. Any other, such as
        // `from_value` or `Deserializer::from_str`, is a handoff the audit
        // cannot follow.
        if !serde_path(&helper, &site) || serde_paths(&py::no_space(&helper)) {
            continue;
        }
        for (variable, bound_at) in &carriers.derived {
            if !arguments
                .iter()
                .any(|argument| argument_passes(argument, variable).is_some())
            {
                continue;
            }
            if !live_binding(&block, variable, start, py::to_isize(*bound_at)) {
                continue;
            }
            let allowed = empty_allowed(block, start, variable, tolerant);
            found.push(unresolved(helper.clone(), allowed, tolerant, None));
        }
    }
    // A method call whose receiver holds a carrier hands it to `self`. The
    // receiver goes through `argument_passes`, as an argument does. So a
    // grouped or borrowed receiver such as `(&body)` is read like `body`,
    // and one that holds the carrier in any other way fails closed.
    for call in qualified_calls(&block) {
        if call.g(1).trim() != "." {
            continue;
        }
        let method = call.g(2);
        let expression = receiver_expression(&block, call.start());
        for (variable, _) in &carriers.names {
            let Some(way) = argument_passes(expression, variable) else {
                continue;
            };
            if !carrier_live(&block, carriers, variable, call.start()) {
                continue;
            }
            if in_dead_closure(&block, call.start()) {
                continue;
            }
            // A method of `RECEIVER_READS` is no handoff, unless a local
            // definition of that name may run on the carrier's type.
            let mut local = None;
            if receiver_read(method) {
                let methods = local_methods(block, params, variable, method, call.start());
                if methods.is_empty() {
                    continue;
                }
                local = Some(methods);
            }
            let after_at = call.end() - 1 + paren(&block[call.end() - 1..]).len();
            let tolerant =
                discards_error(py::upto(&block, call.start()), py::from(&block, after_at));
            let allowed = empty_allowed(block, call.start(), variable, tolerant);
            if way == Way::Wrapped {
                found.push(unresolved(
                    method.to_string(),
                    allowed,
                    tolerant,
                    Some("self"),
                ));
                continue;
            }
            // A partial carrier reaches no method the audit can follow. A
            // whole one reaches only a method of an `impl` for its own type.
            let partial = carrier_partial(carriers, variable, call.start());
            let starts = if partial {
                None
            } else if local.is_some() {
                local
            } else {
                method_targets(block, params, Some(variable), method, call.start())
            };
            let parts = match starts.as_deref() {
                Some([only]) => parts_at(source, *only),
                _ => None,
            };
            if conversion(method).is_some() {
                // A conversion hands the body to the constructor it runs.
                let start = receiver_start(&block, call.start());
                let (converted, parameter) = if partial {
                    (None, None)
                } else {
                    conversion_parts(source, block, method, start)
                };
                found.push(Handoff {
                    helper: method.to_string(),
                    parts: converted,
                    parameter,
                    optional: allowed,
                    tolerant,
                });
                continue;
            }
            let by_self = r"\(\s*&?\s*(?:'[a-z_]+\s+)?(?:mut\s+)?self\b";
            let self_param = parts
                .as_ref()
                .is_some_and(|parts| py::matches(by_self, &parts.params.text));
            found.push(Handoff {
                helper: method.to_string(),
                parts: if self_param { parts } else { None },
                parameter: Some("self".to_string()),
                optional: allowed,
                tolerant,
            });
        }
    }
    found
}

/// `(helper, parts, start, path)` for each call in `block` that can take a
/// body.
///
/// A bare call, a path-qualified call and a method call each resolve at
/// their own site. A call in a closure that never runs is skipped. `path`
/// is `None` for a bare call, the written path for a qualified one, and `.`
/// for a method call, as `receiving_parameters` reads it. A method call
/// reaches what `method_targets` finds. One with no known receiver type has
/// no `parts`, so a handoff to it fails closed.
pub fn call_sites(
    source: &Text,
    block: B<'_>,
    params: &str,
) -> Vec<(String, Option<Rc<Parts>>, usize, Option<String>)> {
    let mut found = Vec::new();
    for call in free_calls(&block) {
        let helper = call.g(1);
        let site = site_in(block, call.start());
        if NOT_CALLS.contains(&helper)
            || GENERIC_HELPERS.contains(&helper)
            || std_path(helper, &site)
        {
            continue;
        }
        if in_dead_closure(&block, call.start()) {
            continue;
        }
        let parts = function_parts(source, helper, Some(&site));
        found.push((helper.to_string(), parts, call.start(), None));
    }
    for call in qualified_calls(&block) {
        let helper = call.g(2);
        let path = py::no_space(call.g(1));
        if GENERIC_HELPERS.contains(&helper) || !handoff_path(&call, block) {
            continue;
        }
        if in_dead_closure(&block, call.start()) {
            continue;
        }
        let parts = if path == "." {
            let receiver = call_receiver(&block, call.start());
            let starts = method_targets(block, params, receiver.as_deref(), helper, call.start())
                .unwrap_or_default();
            if starts.len() == 1 {
                parts_at(source, starts[0])
            } else {
                None
            }
        } else {
            let site = site_in(block, call.start());
            let reference = format!("{}::{helper}", path.trim_end_matches(':'));
            function_parts(source, &reference, Some(&site))
        };
        found.push((helper.to_string(), parts, call.start(), Some(path)));
    }
    found
}

/// Whether a `QUALIFIED_CALL` match in `block` can hand a body to a JSON
/// parse.
///
/// A call of one of `SERDE_PARSERS`, such as `serde_json::from_slice`, is
/// the parse itself, which `block_parses` reads. Any other `serde_json` call,
/// such as `from_reader` or `Deserializer::from_slice`, is a handoff the
/// audit cannot follow, so it fails closed. A standard conversion, as
/// `std_conversion` reads it, such as `std::str::from_utf8` or
/// `String::from_utf8`, cannot run `serde_json`, so it is no handoff.
/// `std_path` reads the root through an import, as `use std::str as text;`
/// binds it. Any other path or method can hand the body on.
pub fn handoff_path(call: &M<'_>, block: B<'_>) -> bool {
    let path = call.g(1).replace(' ', "");
    let path = path.trim_start_matches(':');
    if path == "." {
        return true;
    }
    let joined = format!("{path}{}", call.g(2));
    if serde_paths(&joined) {
        return false;
    }
    !std_conversion(&joined, &site_in(block, call.start()))
}

/// `(path, start, arguments, end)` for each bare or path call in `block`.
///
/// `path` is the call as written, spaces removed, as in
/// `std::str::from_utf8` or `from_value`. A method call is not here.
/// `handoffs` reads the standard conversions and the `serde_json` calls from
/// this one list.
pub fn std_and_serde_calls(block: &str) -> Vec<(String, usize, Vec<String>, usize)> {
    let mut heads: Vec<(M<'_>, String)> = free_calls(block)
        .into_iter()
        .map(|call| (call, String::new()))
        .collect();
    heads.extend(qualified_calls(block).into_iter().map(|call| {
        let path = call.g(1).to_string();
        (call, path)
    }));
    let mut found = Vec::new();
    for (call, path) in heads {
        let path = py::no_space(&path);
        let path = path.trim_start_matches(':');
        if path == "." {
            continue;
        }
        let name = if path.is_empty() {
            call.g(1)
        } else {
            call.g(2)
        };
        if path.is_empty() && NOT_CALLS.contains(&name) {
            continue;
        }
        let raw = paren(&block[call.end() - 1..]);
        let arguments = split_top_level(py::sl(raw, 1, raw.len().saturating_sub(1)));
        found.push((
            format!("{path}{name}"),
            call.start(),
            arguments,
            call.end() - 1 + raw.len(),
        ));
    }
    found
}

/// The written type of a method call's receiver, or `None`.
///
/// `self` is `Self`. Otherwise the last `let` of that name before `position`
/// decides: its type annotation, or a value such as `Type::new(..)` or
/// `Type { .. }`. With no `let`, a parameter with a written type decides.
/// Any other receiver has no type the audit can know.
pub fn receiver_type(
    block: B<'_>,
    params: &str,
    receiver: &str,
    position: usize,
) -> Option<String> {
    if receiver == "self" {
        return Some("Self".to_string());
    }
    let name = py::esc(receiver);
    let binding = format!(r"\blet\s+(?:mut\s+)?{name}\s*(?::\s*([^=;]+?))?\s*=\s*([^;]+)");
    let bindings = py::finditer(&binding, py::upto(&block, position));
    let head = r"&?\s*(?:'[a-z_]+\s+)?(?:mut\s+)?((?:[A-Za-z_]\w*\s*::\s*)*[A-Z]\w*|\[\s*u8\s*\])";
    if let Some(last) = bindings.last() {
        if let Some(annotated) = last.get(1).filter(|annotated| !annotated.is_empty()) {
            let at = py::to_usize(last.start_of(1));
            let (canonical, _) = canonical_type(annotated, Some(&site_in(block, at)));
            return py::rmatch(head, &canonical).map(|typed| py::no_space(typed.g(1)));
        }
        let generic = r"(?:\s*::\s*<[^()]*?>)?";
        let built = format!(r"{head}{generic}\s*(?:::\s*[a-z_]\w*\s*{generic}\s*\(|\{{)");
        return py::rmatch(&built, last.g(2).trim()).map(|built| built.g(1).to_string());
    }
    let declared = format!(r"(?<![\w.]){name}\s*:\s*{head}");
    py::search(&declared, params).map(|declared| py::no_space(declared.g(1)))
}

/// `(optional, tolerant)` for each `helper` parameter that gets a variable.
///
/// Each argument maps to the parameter at its position. A `self` receiver is
/// skipped, since a call does not pass it in the argument list. A parameter
/// is optional when every call that fills it is guarded by `.is_empty()` or
/// turns the error into a value. It is tolerant when every such call turns
/// the error into a value. `argument_passes` decides how an argument hands
/// a variable on. A wrapped one, and an argument with no named parameter,
/// map to `None`, so the caller fails closed. With `qualified`, the calls
/// read are the calls of `helper` through that path, or `.` for a method
/// call. With `at`, only the call that starts there is read.
pub fn receiving_parameters(
    block: B<'_>,
    helper: &str,
    params: &str,
    variables: &Carriers,
    qualified: Option<&str>,
    at: Option<usize>,
) -> Vec<(Option<String>, (bool, bool))> {
    // A pattern such as `Extension(state): ..` keeps its slot with no name.
    // So the parameters after it keep their positions.
    let inner = if params.is_empty() {
        ""
    } else {
        py::sl(params, 1, params.len().saturating_sub(1))
    };
    let mut names: Vec<Option<String>> = Vec::new();
    for item in split_expression(inner, &[","], true, None) {
        if py::full(r"&?\s*(?:'[a-z_]+\s+)?(?:mut\s+)?self", &item) {
            continue;
        }
        names.push(
            py::rmatch(r"(?:mut\s+)?([a-z_][a-z_0-9]*)\s*:", &item)
                .map(|plain| plain.g(1).to_string()),
        );
    }
    let calls: Vec<M<'_>> = match qualified.filter(|path| !path.is_empty()) {
        Some(path) => qualified_calls(&block)
            .into_iter()
            .filter(|call| {
                call.g(2) == helper && py::no_space(call.g(1)) == path && handoff_path(call, block)
            })
            .collect(),
        None => py::finditer(
            &format!(r"(?<![\w.:]){}{TURBOFISH}\s*\(", py::esc(helper)),
            &block,
        ),
    };
    let mut states: Vec<(Option<String>, (bool, bool))> = Vec::new();
    for call in calls {
        if at.is_some_and(|at| call.start() != at) {
            continue;
        }
        let raw = paren(&block[call.end() - 1..]);
        let inner = py::sl(raw, 1, raw.len().saturating_sub(1)).replace("->", "");
        let arguments = split_top_level(&inner);
        let tolerant = discards_error(
            py::upto(&block, call.start()),
            py::from(&block, call.end() - 1 + raw.len()),
        );
        for (index, argument) in arguments.iter().enumerate() {
            let live: Vec<&String> = variables
                .names
                .iter()
                .map(|(name, _)| name)
                .filter(|name| carrier_live(&block, variables, name, call.start()))
                .collect();
            let ways: Vec<(&String, Option<Way>)> = live
                .iter()
                .map(|name| {
                    let way = argument_passes(argument, name);
                    let way = if way.is_some() && carrier_partial(variables, name, call.start()) {
                        Some(Way::Wrapped)
                    } else {
                        way
                    };
                    (*name, way)
                })
                .collect();
            let passed: Vec<&String> = ways
                .iter()
                .filter(|(_, way)| way.is_some())
                .map(|(name, _)| *name)
                .collect();
            let mut name = names.get(index).cloned().flatten();
            if passed.is_empty() {
                continue;
            }
            // A wrapped carrier reaches no parameter the audit can follow.
            if ways.iter().any(|(_, way)| *way == Some(Way::Wrapped)) {
                name = None;
            }
            let allowed = passed
                .iter()
                .any(|variable| empty_allowed(block, call.start(), variable, tolerant));
            if let Some(entry) = states.iter_mut().find(|(known, _)| *known == name) {
                entry.1 = (entry.1.0 && allowed, entry.1.1 && tolerant);
            } else {
                states.push((name, (allowed, tolerant)));
            }
        }
    }
    states
}

/// `(start, body start, body end)` for each closure expression in `block`.
///
/// The body is a block, or an expression up to the first top-level `,` or
/// `;`, as `closure_body_end` reads it. An operator such as `a || b` can
/// read as one too. `in_dead_closure` acts only on a closure that a `let`
/// binds directly, which an operator never is.
pub fn closures(block: &str) -> Rc<Vec<(usize, usize, usize)>> {
    state::cached(&CLOSURES, block.to_string(), || {
        Rc::new(
            py::finditer(r"(?<![|\w])(?:move\s+)?\|([^|]*)\|", block)
                .iter()
                .map(|head| {
                    let (opener, stop) = closure_body_end(block, head.end());
                    (head.start(), opener, stop)
                })
                .collect(),
        )
    })
}

/// `(start, body start, body end)` for each `async` or `async move` block in
/// `block`.
pub fn async_blocks(block: &str) -> Rc<Vec<(usize, usize, usize)>> {
    state::cached(&ASYNC_BLOCKS, block.to_string(), || {
        Rc::new(
            py::finditer(r"\basync\s+(?:move\s+)?(?=\{)", block)
                .iter()
                .map(|head| {
                    let body = balanced(&block[head.end()..], b'{', b'}');
                    (head.start(), head.end(), head.end() + body.len())
                })
                .collect(),
        )
    })
}

/// Whether `position` is inside a deferred body in `block` that never runs.
///
/// A deferred body is a closure, or an `async` or `async move` block, as
/// `async_blocks` finds it. Each enclosing one is its own site. One that is
/// called or awaited in place, passed to a helper, a combinator or a spawn,
/// returned or stored runs, or may run. So its body counts as today, which
/// fails closed. Only a body bound by `let` whose name is never read after
/// it, as `live_binding` reads the name, never runs. So does one bound to
/// `_`.
pub fn in_dead_closure(block: &str, position: usize) -> bool {
    let mut bodies = (*closures(block)).clone();
    bodies.extend(async_blocks(block).iter().copied());
    for (start, opener, stop) in bodies {
        if !(opener <= position && position < stop) {
            continue;
        }
        let bound = py::search(
            r"\blet\s+(?:mut\s+)?([a-z_][a-z_0-9]*)\s*(?::[^=;]*)?=\s*$",
            py::upto(block, start),
        );
        let Some(bound) = bound else {
            continue;
        };
        let name = bound.g(1);
        if name == "_" {
            return true;
        }
        let pattern = format!(r"(?<![.\w]){}\b", py::esc(name));
        let rest = py::from(block, stop);
        let used = py::finditer(&pattern, rest).iter().any(|found| {
            live_binding(
                block,
                name,
                stop + found.start(),
                py::to_isize(bound.start()),
            )
        });
        if !used {
            return true;
        }
    }
    false
}

/// Where the deferred body that holds `position` in `block` runs, or
/// nothing.
///
/// A deferred body is a closure, or an `async` or `async move` block, as
/// `async_blocks` finds it. The innermost one that holds `position` counts
/// only when a `let` binds it by name. `moved_names` then follows that name
/// through each plain move, such as `let run = parse;` or `run = parse;`,
/// to every alias. So there is no second alias walker. Each later use of
/// the name or an alias, as `live_binding` reads it, must run the body or
/// move it to an alias. That is a direct call such as `run()` for a
/// closure, or `fut.await` for an async block. The body then runs at those
/// uses, so a guard or an early exit there applies. Any other use may run
/// where the audit cannot see. Examples are a handoff to a helper, a
/// combinator or a spawn, or an alias that is wrapped or given another
/// value. This
/// is then empty, and the caller reads `position` itself.
pub fn closure_calls(block: B<'_>, position: usize) -> Vec<usize> {
    // Each body, with the pattern that a use of its name must start with.
    let closure_runs = r"\s*\(";
    let async_runs = r"\s*\.\s*await\b";
    let mut bodies: Vec<(usize, usize, &str)> = Vec::new();
    for (start, opener, stop) in closures(&block).iter() {
        if *opener <= position && position < *stop {
            bodies.push((*start, *stop, closure_runs));
        }
    }
    for (start, opener, stop) in async_blocks(&block).iter() {
        if *opener <= position && position < *stop {
            bodies.push((*start, *stop, async_runs));
        }
    }
    let Some((start, stop, runs)) = bodies.into_iter().max() else {
        return Vec::new();
    };
    let bound = py::search(
        r"\blet\s+(?:mut\s+)?([a-z_][a-z_0-9]*)\s*(?::[^=;]*)?=\s*$",
        py::upto(&block, start),
    );
    let Some(bound) = bound.filter(|bound| bound.g(1) != "_") else {
        return Vec::new();
    };
    let first = bound.g(1).to_string();
    let first_at = py::to_isize(bound.start());
    let moved = moved_names(block, Carriers::plain(vec![(first.clone(), first_at)]));
    if !moved.rebound.is_empty() {
        return Vec::new();
    }
    // An alias is a name that a plain move, such as `let run = parse;`,
    // `let run = (parse);` or `run = &parse;`, binds from the name or another
    // alias. `argument_root` reads the move with borrows and grouping only.
    // A name that holds the body in another value, such as `(parse, 1)`, is
    // none, and neither is the result of a run, such as `fut.await`. The
    // fixed point ends, since each pass adds a name or stops.
    let mut aliases: Vec<(String, isize)> = vec![(first, first_at)];
    let mut moves: HashSet<usize> = HashSet::new();
    let mut statements: Vec<Let> = (*plain_lets(&block)).clone();
    statements.extend(assignments(&block).iter().cloned());
    loop {
        let mut added = false;
        for (target, _, opener, value) in &statements {
            let (root, whole) = argument_root(value, false, false);
            let Some(root) = root else {
                continue;
            };
            if !whole || !aliases.iter().any(|(name, _)| *name == root) {
                continue;
            }
            let Some(target_at) = moved.get(target) else {
                continue;
            };
            let pattern = format!(r"(?<![.\w]){}\b", py::esc(&root));
            let Some(found) = py::search(&pattern, value) else {
                continue;
            };
            let used = opener + found.start();
            if moved.partial.contains(target) || moves.contains(&used) {
                continue;
            }
            moves.insert(used);
            if !aliases.iter().any(|(name, _)| name == target) {
                aliases.push((target.clone(), target_at));
            }
            added = true;
        }
        if !added {
            break;
        }
    }
    let mut calls = Vec::new();
    for (alias, bound_at) in &aliases {
        let pattern = format!(r"(?<![.\w]){}\b", py::esc(alias));
        for found in py::finditer(&pattern, &block) {
            let at = found.start();
            if at < stop || !live_binding(&block, alias, at, *bound_at) {
                continue;
            }
            if moves.contains(&at) || py::found(r"\blet\s+(?:mut\s+)?$", py::upto(&block, at)) {
                continue;
            }
            if py::matches(r"\s*=(?![=>])", py::from(&block, found.end())) {
                continue;
            }
            // A grouped or borrowed use, such as `(parse)()` or
            // `(&fut).await`, runs when the whole group is the receiver of the
            // call or await. `receiver_start` bounds it, and `argument_root`
            // reads it.
            let mut end = found.end();
            while let Some(closing) = py::rmatch(r"\s*\)", py::from(&block, end)) {
                end += closing.end();
            }
            if !py::matches(runs, py::from(&block, end)) {
                return Vec::new();
            }
            let rest = py::from(&block, end);
            let run = end + rest.len() - rest.trim_start().len();
            let receiver = receiver_start(&block, run);
            let (read, whole) = argument_root(py::sl(&block, receiver, run), false, false);
            if receiver > at || !whole || read.as_deref() != Some(alias.as_str()) {
                return Vec::new();
            }
            calls.push(at);
        }
    }
    calls.sort_unstable();
    calls
}

/// `(type, optional, tolerant, site)` for each `from_slice` call that reads
/// a carrier.
///
/// `carriers` maps each body name to where it was bound. A name that a later
/// `let` gave a new value is no carrier after that `let`. A partial carrier,
/// as `moved_names` marks it, has no type the audit can read. The site is
/// where the call is, so its type resolves from that module and its
/// imports. A call in a closure that never runs, as `in_dead_closure` reads
/// it, is no parse.
pub fn block_parses(block: B<'_>, carriers: &Carriers, returns: &str) -> Vec<Parse> {
    let mut parses = Vec::new();
    for hit in py::finditer(FROM_SLICE_CALL, &block) {
        if in_dead_closure(&block, hit.start()) {
            continue;
        }
        let mut turbofish = None;
        let mut opener = hit.end() - 1;
        if py::byte(&block, opener) == b'<' {
            let group = balanced(&block[opener..], b'<', b'>');
            let inner = py::sl(group, 1, group.len().saturating_sub(1))
                .trim()
                .to_string();
            let found = py::find(&block, "(", opener + inner.len() + 2);
            opener = if found < 0 {
                block.len().saturating_sub(1)
            } else {
                py::to_usize(found)
            };
            turbofish = Some(inner);
        }
        let call = paren(py::from(&block, opener));
        let argument = py::sl(call, 1, call.len().saturating_sub(1))
            .trim()
            .trim_end_matches(',')
            .trim()
            .to_string();
        let (mut root, mut whole) = argument_root(&argument, false, true);
        // A standard conversion of a carrier, inline or bound to a derived
        // name, is read like the carrier itself.
        let converted = std_conversion_source(block, carriers, &argument, hit.start());
        if let Some(converted) = converted {
            root = Some(converted);
            whole = true;
        } else if let Some(name) = root.as_deref()
            && let Some(bound_at) = carriers.derived_get(name)
            && !carriers.contains(name)
            && !live_binding(&block, name, hit.start(), py::to_isize(bound_at))
        {
            continue;
        }
        let Some(root) = root else {
            continue;
        };
        let in_carriers = carriers.contains(&root);
        let possible = carriers.possible.contains(&root) && !in_carriers;
        if !in_carriers && !possible && carriers.derived_get(&root).is_none() {
            continue;
        }
        if in_carriers && !carrier_live(&block, carriers, &root, hit.start()) {
            continue;
        }
        if carriers.possible.contains(&root) && !live_binding(&block, &root, hit.start(), -1) {
            continue;
        }
        let before = py::upto(&block, hit.start());
        let after = py::from(&block, opener + call.len());
        let mut tolerant = discards_error(before, after);
        // A result stored by `let parsed = ..;` is read where it is used. It
        // stays strict when it is never used or is passed on, since a callee
        // may reject.
        let stored = py::search(
            r"\blet\s+(?:mut\s+)?([a-z_][a-z_0-9]*)\s*(?::[^=;]*)?=\s*$",
            before,
        );
        if let Some(stored) = stored
            && !tolerant
            && after.trim_start().starts_with(';')
        {
            let name = py::esc(stored.g(1));
            let used = py::found(&format!(r"(?<![.\w]){name}\b"), after);
            let passed = py::found(&format!(r"[(,]\s*&?\s*(?:mut\s+)?{name}\b"), after);
            tolerant = used && !passed && !error_rejects(stored.g(1), B::plain(after), "result");
        }
        let optional = empty_allowed(block, hit.start(), &root, tolerant);
        // A body read through an index, a method or a generic type is not
        // something the audit can type, so it is reported.
        let partial = in_carriers && carrier_partial(carriers, &root, hit.start());
        let kind = if !whole || possible || partial {
            None
        } else if turbofish
            .as_deref()
            .is_some_and(|turbofish| !py::full(r"[A-Za-z0-9_:]+", turbofish))
        {
            None
        } else {
            parse_type(turbofish.as_deref(), before, after, returns)
        };
        parses.push((kind, optional, tolerant, site_in(block, hit.start())));
    }
    parses
}

/// Each `Result<Bytes, _>` or `Option<Bytes>` parameter, with its kind.
///
/// The kind is `"result"` or `"option"`, as `FAILURE_VARIANTS` reads it.
/// `Vec<u8>` and `[u8]` count as `Bytes` does.
pub fn wrapped_carriers(params: &str) -> Vec<(String, &'static str)> {
    let mut found: Vec<(String, &'static str)> = Vec::new();
    for (kind, wrapper) in [("result", RESULT), ("option", OPTION)] {
        let typed = format!(r"\b([a-z_][a-z_0-9]*)\s*:\s*{wrapper}<\s*{BYTE_TYPE}");
        for hit in py::finditer(&typed, params) {
            let name = hit.g(1).to_string();
            if let Some(entry) = found.iter_mut().find(|(known, _)| *known == name) {
                entry.1 = kind;
            } else {
                found.push((name, kind));
            }
        }
    }
    found
}
