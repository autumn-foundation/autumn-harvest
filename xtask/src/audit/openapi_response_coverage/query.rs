//! Query checks 4 and 6, and the contract readers they share.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::Value;

use super::aliases::glob_unknowns;
use super::py::{self, repr};
use super::serde::{documented_as, struct_fields, wire_type};
use super::state::{Site, StructBlock};
use super::symbols::written_path;
use super::tables::{RAW_READER_NAMES, RAW_READER_PATHS};

/// One `Query<T>` the audit reads: its name, its struct and whether an
/// absent query yields `None`.
pub type QueryStruct<'a> = (String, &'a StructBlock, bool);

/// The text of a JSON value as Python's `%s` prints it.
pub fn show(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "None".to_string(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Bool(true)) => "True".to_string(),
        Some(Value::Bool(false)) => "False".to_string(),
        Some(other) => other.to_string(),
    }
}

/// A JSON value as Python's `repr` prints it inside a tuple.
fn repr_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => repr(text),
        other => show(other),
    }
}

/// The entries of a list field of a route, or none.
pub fn entries<'a>(route: &'a Value, key: &str) -> Vec<&'a Value> {
    match route.get(key) {
        Some(Value::Array(items)) => items.iter().collect(),
        _ => Vec::new(),
    }
}

/// Check 6: each parameter's `(name, in)` pair is documented once.
///
/// `OpenAPI` requires the pair to be unique. A second entry would otherwise
/// collapse into the first before any check reads it, so it is reported.
pub fn duplicate_params(method: &str, path: &str, route: &Value) -> Vec<String> {
    let mut counts: Vec<((Option<&Value>, Option<&Value>), usize)> = Vec::new();
    for entry in entries(route, "params") {
        let entry_key = (entry.get("name"), entry.get("in"));
        if let Some(found) = counts.iter_mut().find(|(known, _)| *known == entry_key) {
            found.1 += 1;
        } else {
            counts.push((entry_key, 1));
        }
    }
    let mut keyed: Vec<(String, (Option<&Value>, Option<&Value>), usize)> = counts
        .into_iter()
        .map(|((name, place), count)| {
            let text = format!("(({}, {}), {count})", repr_value(name), repr_value(place));
            (text, (name, place), count)
        })
        .collect();
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    keyed
        .into_iter()
        .filter(|(_, _, count)| *count > 1)
        .map(|(_, (name, place), count)| {
            format!(
                "  {method} {path}: `{}` is documented {count} times in {}",
                show(name),
                show(place)
            )
        })
        .collect()
}

/// Whether a parameter type is a trusted raw query reader at its top level.
///
/// The head of the type goes through `written_path`, so an import decides.
/// It counts when it expands to one of `RAW_READER_PATHS`, after the
/// `autumn_web::reexports::` prefix is cut. A bare name with no import
/// counts, as a bare extractor name does, unless a local item shadows it or
/// an untrusted glob may bring it, as `glob_unknowns` reads it. A reader
/// nested in a generic argument, as in `State<Request>`, or one from an
/// untrusted path, does not count.
pub fn raw_reader(written: &str, site: &Site) -> bool {
    let text = py::sub(r"^(?:&\s*(?:'[a-z_]+\s+)?(?:mut\s+)?)*", "", written.trim());
    let Some(head) = py::rmatch(r"(?:::\s*)?[A-Za-z_]\w*(?:\s*::\s*[A-Za-z_]\w*)*", &text) else {
        return false;
    };
    let (segments, _) = written_path(head.whole(), site);
    let path = py::sub(r"^autumn_web::reexports::", "", &segments.join("::"));
    if segments.len() == 1 {
        // An untrusted glob that may bring this name makes it unknown.
        let at = site.at.unwrap_or(0);
        let hidden = glob_unknowns(&site.text)
            .iter()
            .any(|(start, end, name)| *start <= at && at < *end && *name == path);
        return RAW_READER_NAMES.contains(&path.as_str()) && !hidden;
    }
    RAW_READER_PATHS.contains(&path.as_str())
}

/// The route's documented `in: query` parameters, by name.
///
/// Checks 4 and 6 read query keys only through this. So a path or header
/// parameter of the same name never documents a query key.
pub fn query_params(route: &Value) -> BTreeMap<String, &Value> {
    let mut found = BTreeMap::new();
    for entry in entries(route, "params") {
        if entry.get("in").and_then(Value::as_str) != Some("query") {
            continue;
        }
        match entry.get("name") {
            Some(Value::String(name)) if !name.is_empty() => {
                found.insert(name.clone(), entry);
            }
            _ => {}
        }
    }
    found
}

/// Check 6: the `Query<T>` structs and the route's query parameters agree.
///
/// This is the forward check. It reads each field of each extractor.
/// `documented_query_findings` is the inverse check, gated on its own. Each
/// extractor reads the whole query string. So a key that several structs
/// accept is judged once for the route. It is mandatory when any extractor
/// that is not wrapped requires it. Its `OpenAPI` type must be the same in
/// every struct, or the key is a finding.
pub fn query_struct_findings(
    method: &str,
    path: &str,
    route: &Value,
    queries: &[QueryStruct<'_>],
) -> Vec<String> {
    let documented = query_params(route);
    let place = |field: &str| format!("  {method} {path}: `{field}`");
    let mut found = Vec::new();
    // Each documented key, with its readers, in the order they are met.
    let mut readers: Vec<(String, Vec<(String, String, String, bool)>)> = Vec::new();
    for (name, block, wrapped) in queries {
        for (field, declared_type, mandatory, spellings) in struct_fields(block) {
            let key = documented_as(&spellings, &|name| documented.contains_key(name));
            let Some(key) = key else {
                found.push(format!(
                    "{} is accepted by {name} but the contract does not document it",
                    place(&field)
                ));
                continue;
            };
            let reader = (
                name.clone(),
                field.clone(),
                declared_type,
                mandatory && !wrapped,
            );
            if let Some(entry) = readers.iter_mut().find(|(known, _)| known == key) {
                entry.1.push(reader);
            } else {
                readers.push((key.to_string(), vec![reader]));
            }
        }
    }
    for (key, fields) in &readers {
        let entry = documented.get(key).copied();
        let mut kinds: BTreeSet<Option<&str>> = BTreeSet::new();
        for (_, _, declared_type, _) in fields {
            kinds.insert(wire_type(declared_type));
        }
        for (_, field, declared_type, _) in fields {
            if wire_type(declared_type).is_none() {
                found.push(format!(
                    "{} has type {declared_type}, which maps to no OpenAPI type",
                    place(field)
                ));
            }
        }
        kinds.remove(&None);
        let contract_type = entry.and_then(|entry| entry.get("type"));
        if kinds.len() > 1 {
            let listed: Vec<String> = fields
                .iter()
                .map(|(name, _, declared_type, _)| {
                    format!("{} in {name}", wire_type(declared_type).unwrap_or("None"))
                })
                .collect();
            found.push(format!(
                "{} has conflicting types: {}",
                place(key),
                listed.join(", ")
            ));
        } else if !kinds.is_empty()
            && !kinds
                .iter()
                .any(|kind| contract_type.and_then(Value::as_str) == *kind)
        {
            let (name, field, declared_type, _) = &fields[0];
            found.push(format!(
                "{} is {} in {name} but the contract says {}",
                place(field),
                wire_type(declared_type).unwrap_or("None"),
                show(contract_type)
            ));
        }
        let required = entry.and_then(|entry| entry.get("required")) == Some(&Value::Bool(true));
        let strict: Vec<&(String, String, String, bool)> = fields
            .iter()
            .filter(|(_, _, _, mandatory)| *mandatory)
            .collect();
        if let Some((name, field, _, _)) = strict.first()
            && !required
        {
            found.push(format!(
                "{} is mandatory in {name} but the contract marks it optional",
                place(field)
            ));
        }
        if strict.is_empty() && required {
            let field = &fields[0].1;
            let owners: BTreeSet<&str> = fields
                .iter()
                .map(|(reader, _, _, _)| reader.as_str())
                .collect();
            let owners: Vec<&str> = owners.into_iter().collect();
            found.push(format!(
                "{} is optional in {} but the contract marks it required",
                place(field),
                owners.join(" or ")
            ));
        }
    }
    found
}

/// Check 6, inverse: each documented query key is one an extractor accepts.
///
/// It runs for every served route. So a route with no `Query<T>` accepts no
/// documented query key. A route whose query the audit cannot read whole
/// skips it, as `audit` gates it.
pub fn documented_query_findings(
    method: &str,
    path: &str,
    route: &Value,
    queries: &[QueryStruct<'_>],
) -> Vec<String> {
    let documented = query_params(route);
    let mut accepted: HashSet<String> = HashSet::new();
    for (_, block, _) in queries {
        for (_, _, _, spellings) in struct_fields(block) {
            for spelling in spellings {
                accepted.insert(spelling);
            }
        }
    }
    let owners: Vec<&str> = queries.iter().map(|(name, _, _)| name.as_str()).collect();
    let readers = if owners.is_empty() {
        "no extractor of the route accepts it".to_string()
    } else {
        format!("{} does not accept it", owners.join(" or "))
    };
    documented
        .keys()
        .filter(|key| !accepted.contains(*key))
        .map(|key| format!("  {method} {path}: `{key}` is documented but {readers}"))
        .collect()
}

/// Every status the contract declares for a route.
pub fn declared_statuses(route: &Value) -> BTreeSet<i64> {
    let mut statuses = BTreeSet::new();
    if let Some(status) = route
        .get("success_response")
        .and_then(|response| response.get("status"))
        .and_then(Value::as_i64)
    {
        statuses.insert(status);
    }
    for key in ["additional_responses", "error_responses"] {
        for entry in entries(route, key) {
            if let Some(status) = entry.get("status").and_then(Value::as_i64) {
                statuses.insert(status);
            }
        }
    }
    statuses
}
