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

/// Whether Python can use a JSON value as a dict key or a set member.
const fn hashable(value: &Value) -> bool {
    !matches!(value, Value::Array(_) | Value::Object(_))
}

/// Whether a JSON value is falsy, as Python's `or` reads it.
fn falsy(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Bool(flag) => !flag,
        Value::Number(number) => number.as_f64() == Some(0.0),
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(map) => map.is_empty(),
    }
}

/// Fails when Python could not iterate `value` as a list of entries.
///
/// An empty string or an empty object iterates to nothing. Any other
/// non-list value raises in Python, or yields an item that is no dict.
fn check_iterable(value: &Value, what: &str) -> Result<(), String> {
    match value {
        Value::Array(_) => Ok(()),
        Value::String(text) if text.is_empty() => Ok(()),
        Value::Object(map) if map.is_empty() => Ok(()),
        _ => Err(format!("{what} is not a list")),
    }
}

/// Fails when each entry of a list is not an object.
fn check_objects(value: &Value, what: &str) -> Result<(), String> {
    check_iterable(value, what)?;
    if let Value::Array(items) = value
        && items.iter().any(|item| !item.is_object())
    {
        return Err(format!("an entry of {what} is not an object"));
    }
    Ok(())
}

/// The contract routes, or the error that the Python original raised.
///
/// The original read the contract with plain indexing. So a contract with
/// no `routes`, or a route with no `method` or `path`, stopped the audit.
/// A gate that skips such a contract would pass on input it cannot read.
/// The `params` of every route are read the same way, as
/// `duplicate_params` reads them.
pub fn contract_routes(contract: &Value) -> Result<Vec<&Value>, String> {
    let routes = contract
        .get("routes")
        .ok_or_else(|| "KeyError: the contract has no `routes`".to_string())?;
    check_objects(routes, "`routes`")?;
    let routes = entries(contract, "routes");
    for route in &routes {
        for key in ["method", "path"] {
            match route.get(key) {
                None => return Err(format!("KeyError: a route has no `{key}`")),
                Some(value) if !hashable(value) => {
                    return Err(format!("TypeError: a route `{key}` is not a string"));
                }
                Some(_) => {}
            }
        }
        if let Some(params) = route.get("params")
            && !falsy(params)
        {
            check_objects(params, "a route's `params`")?;
            for entry in entries(route, "params") {
                if ["name", "in"]
                    .iter()
                    .any(|key| entry.get(*key).is_some_and(|value| !hashable(value)))
                {
                    return Err("TypeError: a param `name` or `in` is not a string".to_string());
                }
            }
        }
    }
    Ok(routes)
}

/// Fails where the Python original raised on a served route's entry.
///
/// That is a route with no `success_response.status`, a response entry
/// with no `status`, or a `request_body` or `fields` value that is no
/// object or list.
pub fn check_served_route(route: &Value) -> Result<(), String> {
    let status = route
        .get("success_response")
        .filter(|response| response.is_object())
        .and_then(|response| response.get("status"))
        .ok_or_else(|| "KeyError: a route has no `success_response.status`".to_string())?;
    if !hashable(status) {
        return Err("TypeError: a `status` is not a number".to_string());
    }
    for key in ["additional_responses", "error_responses"] {
        let Some(list) = route.get(key) else {
            continue;
        };
        check_objects(list, key)?;
        for entry in entries(route, key) {
            match entry.get("status") {
                None => return Err(format!("KeyError: an entry of `{key}` has no `status`")),
                Some(value) if !hashable(value) => {
                    return Err("TypeError: a `status` is not a number".to_string());
                }
                Some(_) => {}
            }
        }
    }
    let Some(body) = route.get("request_body").filter(|body| !falsy(body)) else {
        return Ok(());
    };
    if !body.is_object() {
        return Err("AttributeError: `request_body` is not an object".to_string());
    }
    if let Some(fields) = body.get("fields")
        && !falsy(fields)
    {
        check_objects(fields, "`request_body.fields`")?;
        for field in entries(body, "fields") {
            if field.get("name").is_some_and(|name| !hashable(name)) {
                return Err("TypeError: a field `name` is not a string".to_string());
            }
        }
    }
    Ok(())
}

/// A status as Python compares it with an `int`.
///
/// A whole float such as `404.0` equals `404`, and `true` equals `1`.
fn status_number(value: &Value) -> Option<i64> {
    match value {
        Value::Bool(flag) => Some(i64::from(*flag)),
        Value::Number(number) => number.as_i64().or_else(|| {
            number
                .as_f64()
                .filter(|float| float.fract() == 0.0 && float.abs() < 1e15)
                .map(|float| {
                    #[allow(clippy::cast_possible_truncation)]
                    let whole = float as i64;
                    whole
                })
        }),
        _ => None,
    }
}

/// Check 6: each parameter's `(name, in)` pair is documented once.
///
/// `OpenAPI` requires the pair to be unique. A second entry would otherwise
/// collapse into the first before any check reads it, so it is reported.
pub fn duplicate_params(method: &str, path: &str, route: &Value) -> Vec<String> {
    // Python's `entry.get(..)` gives `None` for a missing key and for a JSON
    // `null`. So both are one key here.
    fn get<'v>(entry: &'v Value, key: &str) -> Option<&'v Value> {
        entry.get(key).filter(|value| !value.is_null())
    }
    let mut counts: Vec<((Option<&Value>, Option<&Value>), usize)> = Vec::new();
    for entry in entries(route, "params") {
        let entry_key = (get(entry, "name"), get(entry, "in"));
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
/// counts, as a bare extractor name does. It does not count when a local
/// item shadows it, or when an untrusted glob may bring it, as
/// `glob_unknowns` reads it. A reader
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
        .and_then(status_number)
    {
        statuses.insert(status);
    }
    for key in ["additional_responses", "error_responses"] {
        for entry in entries(route, key) {
            if let Some(status) = entry.get("status").and_then(status_number) {
                statuses.insert(status);
            }
        }
    }
    statuses
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_well_formed_contract_passes_the_checks() {
        let contract = json!({ "routes": [{
            "method": "GET",
            "path": "/a",
            "params": null,
            "success_response": { "status": 200 },
            "error_responses": [{ "status": 404 }],
            "request_body": { "required": true, "fields": [{ "name": "x" }] },
        }] });
        let routes = contract_routes(&contract).expect("the contract reads");
        assert_eq!(routes.len(), 1);
        assert!(check_served_route(routes[0]).is_ok());
        assert_eq!(declared_statuses(routes[0]), BTreeSet::from([200, 404]));
    }

    #[test]
    fn a_contract_python_could_not_read_is_an_error() {
        assert!(contract_routes(&json!({})).is_err());
        assert!(contract_routes(&json!({ "routes": [{ "path": "/a" }] })).is_err());
        assert!(contract_routes(&json!({ "routes": [["GET"]] })).is_err());
        let bad_param = json!({ "routes": [{ "method": "GET", "path": "/a", "params": [1] }] });
        assert!(contract_routes(&bad_param).is_err());
        assert!(check_served_route(&json!({ "method": "GET", "path": "/a" })).is_err());
        let no_status = json!({
            "success_response": { "status": 200 },
            "error_responses": [{ "description": "x" }],
        });
        assert!(check_served_route(&no_status).is_err());
        let list_body = json!({ "success_response": { "status": 200 }, "request_body": ["x"] });
        assert!(check_served_route(&list_body).is_err());
    }

    #[test]
    fn a_whole_float_status_counts_as_its_integer() {
        let route = json!({
            "success_response": { "status": 200.0 },
            "error_responses": [{ "status": "404" }, { "status": 409.5 }],
        });
        assert_eq!(declared_statuses(&route), BTreeSet::from([200]));
    }

    #[test]
    fn a_missing_and_a_null_param_name_are_one_key() {
        let route = json!({ "params": [{ "in": "query" }, { "in": "query", "name": null }] });
        assert_eq!(
            duplicate_params("GET", "/a", &route),
            vec!["  GET /a: `None` is documented 2 times in query".to_string()]
        );
    }
}
