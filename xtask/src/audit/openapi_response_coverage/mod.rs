//! `cargo xtask audit openapi-response-coverage`: check the API contract
//! against the handlers it describes.
//!
//! This is the port of `docs/audits/openapi-response-coverage.py`. Seven
//! checks, all mechanical:
//!
//! 1. Every HTTP status a handler can return is declared for that route.
//! 2. Every request-body field that is mandatory on the wire is marked
//!    required.
//! 3. Every request-body field the handler accepts is documented at all.
//! 4. Every query key a hand-rolled parser accepts is documented at all.
//! 5. Every body the handler cannot run without is marked required.
//! 6. Every `Query<T>` field is documented with its type and required flag.
//! 7. Every body and query type the audit reads resolves to a struct.
//!
//! The published `OpenAPI` document is generated from `docs/api-contract.json`.
//! So anything missing there is missing from every generated client. This
//! audit reads the router and the handlers in
//! `autumn-harvest-plugin/src/api.rs` and compares them against the
//! contract.
//!
//! For check 1 it collects the `StatusCode::` values each handler can
//! return. It adds the status implied by each `AutumnError::` constructor it
//! calls, as `AUTUMN_ERROR_STATUS` lists them. A constructor call chained
//! into `.with_status(..)` carries the overridden status instead, so it is
//! skipped there. The `StatusCode::` literal inside that `.with_status(..)`
//! call is what the plain scan already requires. A constructor passed bare,
//! as in `.map_err(AutumnError::bad_request_msg)`, names no call. So it
//! cannot chain `.with_status(..)`, and its status is always the
//! constructor's own.
//!
//! Checks 2 and 3 resolve each `Json<T>` extractor to its struct, with or
//! without a path such as `axum::Json`. A field is mandatory when it is
//! neither an `Option` nor carries a serde default, since axum rejects a
//! request that omits one. A field serde accepts but the contract omits is
//! missing from the generated client. So an ordinary request cannot be
//! typed. Checks 2 and 3 skip only a body the contract marks `free_form`. A
//! present `Option<Json<T>>` body is parsed strictly, so check 2 reads its
//! mandatory fields. An empty field list on any other body is checked.
//!
//! A handler can also take the raw `Bytes` and parse it itself with one of
//! `SERDE_PARSERS`, `serde_json::from_slice` or `serde_json::from_str`. An
//! import alias, such as `use serde_json::from_slice as decode;`, is read
//! too. Every scan reads the source after `resolve_aliases`. It reads each
//! `use .. as ..` and `type` alias of `Query`, `Json`, `Bytes` or a parser in
//! its scope, and reports a type alias it cannot read. Any other `serde_json`
//! call that gets the body, such as `from_reader`, fails closed. A standard
//! conversion of the whole body is a derived carrier that a parser reads.
//! Examples are `std::str::from_utf8(&body)` and
//! `String::from_utf8(body.to_vec())`. A standard conversion of any other
//! part of the body fails closed. Checks 2, 3 and 5 read that parse when it
//! reads a parameter of type `Bytes`, `&[u8]` or `Vec<u8>`. The parse can be
//! in the handler, or in a helper that the handler passes the body to, at
//! any depth up to `HELPER_DEPTH`. A recursive helper is read once per chain
//! of calls. In a helper, only the parameter at the position of the body
//! argument is a body. A move into another name, such as `let captured =
//! body;`, is followed. So is the result of a helper that gets the body and
//! returns a byte type, when `returns_carrier` proves that it returns the
//! body. Any other such result is a partial carrier, so a parse of it fails
//! closed. So is a value that holds such a call inside a branch, a block, a
//! match arm or a group. The audit does not model which branch runs.
//! A copy through a call, such as `body.to_vec()`, is read only inside a
//! standard conversion. A byte accessor such as `body.as_ref()` is no
//! handoff. The exception is a local method of that name that may run on
//! the body's type, as `local_methods` reads it. The type comes from a
//! turbofish, then from a typed `let` in the same statement, then from a
//! `Result<T, _>` return type. The last two apply only when the call ends
//! its expression, since a `.map(..)` after it yields another type. A
//! `Value` body is free-form, so checks 2 and 3 skip it. Check 5 still reads
//! it.
//!
//! Check 5 treats a bare `Json<T>` as mandatory, since axum rejects a
//! request without it. A `Result<Json<T>, _>` is mandatory when the handler
//! rejects its error. That is through `?`, `.map_err(..)?`, or an `Err` arm
//! that builds an error or returns the rejection it binds. An `Err` arm that
//! returns a helper rejects. So does an arm whose value is a call to a
//! helper that can build a rejection, unless it hands its error to that
//! helper. A move of the extractor into another name is followed. Such a
//! helper can recover the request, as the start route does (#808). A guard
//! on an arm is not evaluated, so a rejecting guarded arm counts. A
//! catch-all `_` arm counts as an `Err` arm. So do an `if let Err(..)` block
//! and the code after `let Err(..) = body else { .. };`. An `if` on
//! `.is_err()` or `.is_ok()` counts when its failing branch rejects. A
//! raw-byte parse is mandatory unless an `if` on `.is_empty()` lets an empty
//! body skip it. The parse must be in the arm that runs for a non-empty
//! body, and the empty-body arm must not reject. An earlier `if
//! body.is_empty() { .. }` also counts when its block returns a success and
//! no error. A success is `Ok(..)`, a 2xx status or `Json(..)`. Only a return
//! at the top level of that block counts. A return inside a nested `if`,
//! `match` or closure may not run. A parse result stored by `let parsed =
//! ..;` is read where it is used. It stays strict when it is unused or
//! passed on. A parse that turns its error into a value is optional too.
//! Examples are `.ok()`, `.unwrap_or_default()` and an `if let Ok(..)` whose
//! `else` does not reject. A `match` on the parse is optional when it has an
//! `Err` or catch-all arm and no such arm rejects. A fallback that rejects
//! the error, such as `.map_or_else(|e| reject(e), ..)`, keeps the parse
//! mandatory. So does a fallback that calls a helper that can build a
//! rejection. The helper's return type decides. A response, an error or a
//! `Result` can be a rejection, and a plain value type such as `Gadget` is
//! not. A helper the audit cannot find counts as a rejection. An
//! `.or_else(..)` whose fallback yields `Ok(..)` on every path makes a later
//! `?` tolerant. A guard or a tolerant call at a helper call site carries
//! into the helper. A parse can be in a closure or an `async` block that a
//! `let` binds, and that runs only where it is called or awaited. Then the
//! audit reads the guard at each call or `.await`, grouped or borrowed as in
//! `(parse)()` or `(&fut).await`. A plain move to another name, such as `let
//! run = parse;` or `let run = (parse);`, is followed to that name's calls.
//! The strictest one decides. One whose name is never used never runs, so
//! its parse does not count. Check 2 applies to every parse that does not
//! tolerate its error. A body that is present must then carry the mandatory
//! fields. A bare `Json<T>` and a rejecting `Result<Json<T>, _>` are such
//! parses.
//!
//! Check 4 reads the routes that take a `RawQuery` and parse the pairs by
//! hand. A key those parsers match is a parameter the route accepts. So a
//! key the contract omits cannot be expressed by a generated client. Only a
//! literal match arm at the top of a `pairs` loop is read, so a key computed
//! at runtime is invisible. An arm naming several spellings passes when the
//! contract documents any one of them. An alias needs no second entry in
//! the document.
//!
//! A `StatusCode::` or `AutumnError::` token is ignored inside a helper on
//! `GENERIC_HELPERS`. `map_error` translates a runtime error into whatever
//! status fits it, not the calling route. So its statuses belong to the
//! error, not to every route that reaches it. `conflict_from` is not on that
//! list. Its `HarvestError::Config` arm carries one fixed, literal
//! `StatusCode::CONFLICT` that belongs to every route calling it. So it is
//! traversed like any other helper. Its other arms still fall through to
//! `map_error`, which stays excluded. So no variable status leaks in
//! through that path.
//!
//! Each handler is followed one level into the helpers it calls, since a
//! status is often selected in a helper such as
//! `queue_pause_partial_status`. A helper called by a helper is not
//! followed, so this audit is a floor rather than a proof. The finding text
//! names the source line, since a status can reach a route through a helper
//! it shares.
//!
//! Check 6 compares every `Query<T>` struct of a route with its `in: query`
//! parameters. An `Option<Query<T>>` makes every field optional, since an
//! absent query string yields `None`. A `Result<Query<T>, _>` does the same
//! when the handler does not reject its error, as check 5 reads it.
//! `WIRE_TYPES` gives the `OpenAPI` type of a field after the audit removes
//! one `Option`. A field is optional when it is an `Option` or has a serde
//! default, on the field or on the struct. By default, serde ignores an
//! unknown query key. So a documented key that no struct has is a finding.
//!
//! The audit reads the serde attributes `default`, `skip`,
//! `skip_deserializing`, `skip_serializing`, `skip_serializing_if`, `rename
//! = ".."` and `alias = ".."`. A field is documented when the contract names
//! its wire name or any alias. Any other serde attribute, on the struct or
//! on a field, is a check 7 finding. The audit cannot read the wire layout
//! it makes. A field `deserialize_with` is read only when
//! `KNOWN_DESERIALIZERS` models it. A new custom deserializer must be added
//! to that table, or its field is a finding.
//!
//! Check 7 stops the audit from skipping what it cannot read. A
//! `from_slice` call it cannot read is a finding. So is a body type it
//! cannot resolve, a `Json<..>` or `Query<..>` extractor it cannot read, and
//! a struct it cannot find. A generic struct such as `struct Q<'a>` is one it
//! cannot find.
//!
//! A helper called by bare name is found as a free function. So an earlier
//! method of the same name does not hide it. `Type::f` and `x.f` reach
//! methods, and a struct path such as `module::S` picks the struct in that
//! module. A name the audit cannot pin to one definition fails closed. A
//! handler is found from its `async fn` line. So an earlier fn of the same
//! name does not hide it. Comments in a parameter list are removed before
//! any read.
//!
//! Exit code 1 on any finding. Run it standalone, or run the fixtures:
//!
//! ```sh
//! cargo xtask audit openapi-response-coverage
//! cargo xtask audit openapi-response-coverage --self-test
//! ```
//!
//! The submodules mirror the sections of the original script. `py` keeps
//! the Python string and regex semantics. `symbols` resolves names,
//! `aliases` rewrites aliases, and `scan` reads the router and the helpers.
//! `serde` reads struct layouts, `carriers` follows raw bodies, `rejects`
//! reads guards and rejections, and `query` holds checks 4 and 6.
//! `fixtures` holds the self-test cases.

// The port mirrors the Python original branch by branch, so that each check
// reads as the original does. These lints ask for a different shape.
#![allow(
    clippy::collapsible_if,
    clippy::if_same_then_else,
    clippy::needless_range_loop,
    clippy::option_if_let_else,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::useless_let_if_seq
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::process::ExitCode;
use std::rc::Rc;

use serde_json::Value;
use smop::prelude::*;

#[macro_use]
mod state;
mod aliases;
mod carriers;
mod fixtures;
mod mask;
mod py;
mod query;
mod rejects;
mod scan;
mod serde;
mod symbols;
mod syntax;
mod tables;

use self::aliases::{
    canonical_extractor_paths, canonical_paths, resolve_aliases, unreadable_aliases,
};
use self::carriers::{byte_parameters, raw_body_parses, wrapped_carriers};
use self::mask::masked_source;
use self::query::{
    QueryStruct, check_served_route, contract_routes, declared_statuses, documented_query_findings,
    duplicate_params, entries, query_params, query_struct_findings,
};
use self::rejects::{error_rejects, rejects_result_body, rejects_wrapped_body};
use self::scan::{called_helpers, key_arms, overridden_by_with_status, router_routes};
use self::serde::{accepted_fields, documented_as, mandatory_fields, unreadable_serde};
use self::state::{AliasIndex, Site, StructIndex};
use self::symbols::{
    crate_type_aliases, handler_parts, location, parts_at, struct_body, struct_index,
    type_aliases_in,
};
use self::tables::{JSON, MIN_ROUTES, OPTION, QUERY_EXTRACTOR, RESULT, autumn_error_status, named};

/// Arguments of `cargo xtask audit openapi-response-coverage`.
#[derive(Clone, Copy, Debug, Args)]
pub struct Args {
    /// Run each fixture through the audit and compare the findings.
    #[arg(long)]
    pub self_test: bool,
}

/// Runs `cargo xtask audit openapi-response-coverage`.
pub fn run(args: Args) -> Result<ExitCode> {
    if args.self_test {
        return self_test();
    }
    main()
}

/// The finding lists of one audit, by check.
pub type Found = BTreeMap<&'static str, Vec<String>>;

/// The check, its title and its advice, for each finding list, in print
/// order.
pub const CHECKS: &[(&str, &str, Option<&str>)] = &[
    (
        "statuses",
        "Undeclared statuses",
        Some(
            "Declare each in docs/api-contract.json. A status that carries a body belongs in additional_responses with its fields; one that does not belongs in error_responses.",
        ),
    ),
    ("mandatory", "Unmarked mandatory body fields", None),
    ("undocumented", "Undocumented body fields", None),
    (
        "query_keys",
        "Undocumented query keys",
        Some(
            "Add each to the route's `params` in docs/api-contract.json. An alias of a documented key needs no entry of its own.",
        ),
    ),
    (
        "body_required",
        "Mandatory bodies marked optional",
        Some("Set the route's `request_body.required` to true."),
    ),
    (
        "unresolved",
        "Unresolved types",
        Some(
            "Name each body type in the source, for example `from_slice::<T>(..)`. Keep each struct in `autumn-harvest` or `autumn-harvest-plugin`, and not generic.",
        ),
    ),
    (
        "query_params",
        "Typed query mismatches",
        Some(
            "Give each `Query<T>` field one entry in the route's `params`, with the same name, type and required flag. Add a field type with no OpenAPI type to `WIRE_TYPES`.",
        ),
    ),
];

/// Where `audit` finds its structs and type aliases.
///
/// The real run reads both crates. A fixture reads its own source only.
pub struct Finder {
    pub index: Option<Rc<StructIndex>>,
    pub aliases: Option<Rc<AliasIndex>>,
    pub local: Option<(String, String)>,
}

impl Finder {
    /// The finder of the real run: the structs and aliases of both crates.
    pub const fn crates() -> Self {
        Self {
            index: None,
            aliases: None,
            local: None,
        }
    }
}

/// A line index of a text, so a position maps to its line number quickly.
struct Lines(Vec<usize>);

impl Lines {
    fn new(text: &str) -> Self {
        Self(
            text.bytes()
                .enumerate()
                .filter(|(_, char)| *char == b'\n')
                .map(|(index, _)| index)
                .collect(),
        )
    }

    /// The one-based line of `position`, as `count("\n") + 1` reads it.
    fn line(&self, position: usize) -> usize {
        self.0.partition_point(|newline| *newline < position) + 1
    }
}

/// The source line text that a finding quotes, cut to 88 characters.
fn quoted(lines: &[&str], line: usize) -> String {
    lines
        .get(line.wrapping_sub(1))
        .map_or_else(String::new, |text| text.trim().chars().take(88).collect())
}

/// The last segment of a path, as `split("::")[-1]` reads it.
fn last_name(reference: &str) -> &str {
    reference.rsplit("::").next().unwrap_or(reference)
}

/// Every finding, by check. `finder` says where structs and aliases live.
pub fn audit(source: &str, contract: &Value, finder: &Finder) -> Result<Found> {
    let found = audit_inner(source, contract, finder);
    if let Some(failure) = py::take_failure() {
        bail!("{failure}");
    }
    found
}

fn audit_inner(original: &str, contract: &Value, finder: &Finder) -> Result<Found> {
    // A rename keeps every line, so line numbers still point at the source.
    let lines: Vec<&str> = original.split('\n').collect();
    let (resolved, unsettled) = resolve_aliases(canonical_extractor_paths(original));
    let source = state::intern(&canonical_paths(&resolved));
    // Comments and literals are blanked once, at the same length. Every scan
    // reads `code`. Only the route table and the query keys need literals.
    let code = masked_source(&source);
    let mut unreadable = unreadable_aliases(&code);
    unreadable.extend(unsettled.iter().map(|name| (0, code.len(), name.clone())));
    let local = finder
        .local
        .clone()
        .unwrap_or_else(|| ("autumn_harvest_plugin".to_string(), "api".to_string()));
    let type_aliases = finder.aliases.clone().unwrap_or_else(crate_type_aliases);
    state::set_ctx(|ctx| {
        ctx.source = Rc::clone(&code);
        ctx.type_aliases = type_aliases;
        ctx.structs.clone_from(&finder.index);
        ctx.local = local.clone();
        ctx.known_modules = None;
    });
    state::set_file_place(&code, location(&[&local.0, &local.1]));
    let globbed = aliases::glob_unknowns(&code);
    let routes_list = contract_routes(contract).map_err(|error| anyhow!(error))?;
    let mut by_route: HashMap<(String, String), &Value> = HashMap::new();
    for route in &routes_list {
        let method = route.get("method").and_then(Value::as_str).unwrap_or("");
        let path = route.get("path").and_then(Value::as_str).unwrap_or("");
        by_route.insert((method.to_string(), path.to_string()), route);
    }
    let routes = router_routes(&source)?;
    let line_index = Lines::new(&code);

    let mut findings: Vec<String> = Vec::new();
    let mut query_findings: Vec<String> = Vec::new();
    for (method, path, handler) in &routes {
        let Some(parts) = handler_parts(&code, handler) else {
            continue;
        };
        let Some(route) = by_route.get(&(method.clone(), path.clone())) else {
            continue;
        };
        check_served_route(route).map_err(|error| anyhow!(error))?;
        let declared = declared_statuses(route);
        let body = parts.block();
        let helpers = called_helpers(body, &parts.params.text);
        let mut bodies: Vec<String> = vec![parts.block.clone()];
        for helper in &helpers {
            if let Some(reached) = parts_at(&code, *helper) {
                bodies.push(reached.block.clone());
            }
        }
        // Every trusted raw reader, as `raw_reader` reads it, parses the
        // query by hand, so its key arms are read. The same gate exempts the
        // route from the documented-key check below.
        if parts.params.raw_reader {
            let documented = query_params(route);
            // A query key is a string literal, so these blocks keep literals.
            let mut keyed: Vec<String> = Vec::new();
            if let Some(found) = handler_parts(&source, handler) {
                keyed.push(found.block.clone());
            }
            for helper in &helpers {
                if let Some(found) = parts_at(&source, *helper) {
                    keyed.push(found.block.clone());
                }
            }
            for reached in &keyed {
                for arm in key_arms(reached) {
                    if arm.iter().any(|key| documented.contains_key(key)) {
                        continue;
                    }
                    query_findings.push(format!(
                        "  {method} {path}: `{}` is accepted by the query parser but the contract does not document it",
                        arm.join("` / `")
                    ));
                }
            }
        }
        for reached in &bodies {
            let offset = py::to_usize(symbols::block_start(&code, reached));
            for hit in py::finditer(r"(.{0,30})StatusCode::([A-Z_]+)", reached) {
                let Some(status) = named(hit.g(2)) else {
                    continue;
                };
                if declared.contains(&status) {
                    continue;
                }
                if hit.g(1).contains("==") || hit.g(1).contains("!=") {
                    continue;
                }
                let line = line_index.line(offset + hit.start());
                findings.push(format!(
                    "  {method} {path} returns {status}, undeclared\n    api.rs:{line}  {}",
                    quoted(&lines, line)
                ));
            }
            for hit in py::finditer(r"AutumnError::([a-z_]+)\(", reached) {
                let Some(status) = autumn_error_status(hit.g(1)) else {
                    continue;
                };
                if declared.contains(&status) || overridden_by_with_status(reached, hit.end() - 1) {
                    continue;
                }
                let line = line_index.line(offset + hit.start());
                findings.push(format!(
                    "  {method} {path} returns {status} via AutumnError::{}, undeclared\n    api.rs:{line}  {}",
                    hit.g(1),
                    quoted(&lines, line)
                ));
            }
            // A constructor passed bare, as in
            // `.map_err(AutumnError::bad_request_msg)`, names no call. So it
            // cannot chain `.with_status(..)`, and its status is always the
            // constructor's own.
            for hit in py::finditer(r"AutumnError::([a-z_]+)\)", reached) {
                let Some(status) = autumn_error_status(hit.g(1)) else {
                    continue;
                };
                if declared.contains(&status) {
                    continue;
                }
                let line = line_index.line(offset + hit.start());
                findings.push(format!(
                    "  {method} {path} returns {status} via AutumnError::{} (bare fn ref), undeclared\n    api.rs:{line}  {}",
                    hit.g(1),
                    quoted(&lines, line)
                ));
            }
        }
    }

    let mut body_findings: Vec<String> = Vec::new();
    let mut undocumented: Vec<String> = Vec::new();
    let mut required_findings: Vec<String> = Vec::new();
    let mut unresolved: Vec<String> = Vec::new();
    let mut typed_query: Vec<String> = Vec::new();
    // Every contract route is checked for duplicate parameters, whether or
    // not the router serves it.
    for route in &routes_list {
        let method = route.get("method").and_then(Value::as_str).unwrap_or("");
        let path = route.get("path").and_then(Value::as_str).unwrap_or("");
        typed_query.extend(duplicate_params(method, path, route));
    }
    for (method, path, handler) in &routes {
        let Some(parts) = handler_parts(&code, handler) else {
            continue;
        };
        let Some(route) = by_route.get(&(method.clone(), path.clone())) else {
            continue;
        };
        let params = &parts.params;
        let block = parts.block();
        let at = py::to_usize(symbols::block_start(&code, &format!("async fn {handler}(")));
        let written = &params.written;
        for (start, end, alias) in &unreadable {
            if *start <= at && at < *end && py::found(&format!(r"(?<![\w:]){alias}\b"), written) {
                unresolved.push(format!(
                    "  {method} {path}: cannot read the `{alias}` type alias"
                ));
            }
        }
        for (start, end, name) in globbed.iter() {
            if *start <= at && at < *end && py::found(&format!(r"(?<![\w:]){name}\b"), written) {
                unresolved.push(format!(
                    "  {method} {path}: cannot tell which `{name}` a glob import brings"
                ));
            }
        }
        let site = Site::new(&code, Some(at));
        let mut struct_blocks: Vec<(String, Rc<state::StructBlock>, bool)> = Vec::new();
        let unread_before = unresolved.len();
        for query in py::finditer(QUERY_EXTRACTOR, &params.text) {
            let reference = query.g(1);
            let name = last_name(reference).to_string();
            // An absent query string turns `Option<Query<T>>` into `None`, so
            // none of its fields is mandatory, unless the handler rejects it.
            // A `Result<Query<T>, _>` whose error the handler tolerates acts
            // the same.
            let prefix = &params.text[..query.start()];
            let mut wrapped = py::found(&format!(r"{OPTION}<\s*(?:[a-z_]+::)*$"), prefix);
            // An `Option<Query<T>>` whose `None` the handler rejects, as
            // `error_rejects` reads it for bodies too, keeps every field's
            // own required flag.
            let absent = py::search(
                &format!(r"\b([a-z_][a-z_0-9]*)\s*:\s*{OPTION}<\s*(?:[a-z_]+::)*$"),
                prefix,
            );
            if let Some(absent) = absent
                && error_rejects(absent.g(1), block, "option")
            {
                wrapped = false;
            }
            let result = py::search(
                &format!(r"\b([a-z_][a-z_0-9]*)\s*:\s*{RESULT}<\s*(?:[a-z_]+::)*$"),
                prefix,
            );
            if let Some(result) = result
                && !error_rejects(result.g(1), block, "result")
            {
                wrapped = true;
            }
            match struct_body(reference, Some(&site)) {
                None => unresolved.push(format!("  {method} {path}: cannot find struct {name}")),
                Some(found) => {
                    let unread = unreadable_serde(&found);
                    if unread.is_empty() {
                        struct_blocks.push((name, found, wrapped));
                    } else {
                        unresolved.extend(unread.iter().map(|attribute| {
                            format!("  {method} {path}: cannot read `{attribute}` in {name}")
                        }));
                    }
                }
            }
        }
        let extractors = py::finditer(QUERY_EXTRACTOR, &params.text).len();
        if py::finditer(r"(?<![\w:])Query<", &params.text).len() > extractors {
            unresolved.push(format!(
                "  {method} {path}: cannot read a `Query<..>` extractor"
            ));
        }
        // The forward check reads every `Query<T>` the audit can read. The
        // inverse check runs for every served route, with or without a
        // `Query<T>`. So a documented key that no extractor reads is a
        // finding. A route whose query the audit cannot read whole is left
        // out of the inverse check only. That is an unresolved `Query<T>`,
        // or a raw reader that check 4 reads.
        let unread_queries = unresolved.len() > unread_before;
        let queries: Vec<QueryStruct<'_>> = struct_blocks
            .iter()
            .map(|(name, found, wrapped)| (name.clone(), found.as_ref(), *wrapped))
            .collect();
        typed_query.extend(query_struct_findings(method, path, route, &queries));
        if !unread_queries && !params.raw_reader {
            typed_query.extend(documented_query_findings(method, path, route, &queries));
        }

        // A bare `Json<T>` means the body is mandatory. `Result<Json<T>, _>`
        // is mandatory when its error rejects. `Option<Json<T>>` is mandatory
        // when its `None` rejects, and optional otherwise. All three still
        // name the struct whose fields serde accepts, which is what check 3
        // needs. The binding is `Json(body)` or a plain name such as `mut
        // body`.
        let binding = format!(r"(?:{JSON}\(\s*[a-z_0-9]+\s*\)|(?:mut\s+)?[a-z_][a-z_0-9]*)");
        let bare = py::search(
            &format!(r"{binding}\s*:\s*{JSON}<\s*([A-Za-z0-9_:]+)\s*>"),
            &params.text,
        );
        let mut option_body = false;
        let extractor = bare.clone().or_else(|| {
            py::search(
                &format!(r"{RESULT}<\s*{JSON}<\s*([A-Za-z0-9_:]+)\s*>"),
                &params.text,
            )
            .or_else(|| {
                let found = py::search(
                    &format!(r"{OPTION}<\s*{JSON}<\s*([A-Za-z0-9_:]+)\s*>\s*>"),
                    &params.text,
                );
                option_body = found.is_some();
                found
            })
        });
        let json_count = py::finditer(r"(?<![\w:])Json<", &params.text).len();
        if json_count > usize::from(extractor.is_some()) {
            unresolved.push(format!(
                "  {method} {path}: cannot read a `Json<..>` extractor"
            ));
        }
        // The struct name, whether the body is mandatory, and the site, for
        // each parse.
        let mut parses: Vec<(String, bool, Site)> = Vec::new();
        let result_rejects = rejects_result_body(&params.text, block);
        if let Some(body_type) = extractor.as_ref().map(|found| found.g(1))
            && last_name(body_type) != "Value"
        {
            // A present `Option<Json<T>>` body is still parsed strictly, so
            // its mandatory fields are checked. A tolerant `Result` body is
            // not.
            let mandatory = bare.is_some() || result_rejects || option_body;
            parses.push((body_type.to_string(), mandatory, site.clone()));
        }
        // An `Option` body whose `None` the handler rejects is mandatory too.
        let mut mandatory_body =
            bare.is_some() || result_rejects || rejects_wrapped_body(&params.text, block);
        if !byte_parameters(&params.text).is_empty()
            || !params.possible.is_empty()
            || !wrapped_carriers(&params.text).is_empty()
        {
            for (name, optional, tolerant, parse_site) in raw_body_parses(&code, handler) {
                // A parse an empty body cannot skip makes the body mandatory,
                // whatever its type. A parse that does not tolerate its error
                // needs the mandatory fields of any body that is present.
                mandatory_body |= !optional;
                match name {
                    None => unresolved.push(format!(
                        "  {method} {path}: cannot read a `from_slice` call or resolve its body type"
                    )),
                    Some(name) if last_name(&name) != "Value" => {
                        parses.push((name, !tolerant, parse_site));
                    }
                    Some(_) => {}
                }
            }
        }

        let empty = Value::Object(serde_json::Map::new());
        let request_body = match route.get("request_body") {
            Some(Value::Object(body)) if !body.is_empty() => {
                route.get("request_body").unwrap_or(&empty)
            }
            _ => &empty,
        };
        if mandatory_body && request_body.get("required") != Some(&Value::Bool(true)) {
            required_findings.push(format!(
                "  {method} {path}: the body is mandatory in the handler but the contract does not mark it required"
            ));
        }
        let mut declared: HashMap<Option<String>, Value> = HashMap::new();
        for field in entries(request_body, "fields") {
            let name = field
                .get("name")
                .and_then(Value::as_str)
                .map(str::to_string);
            let required = field.get("required").cloned().unwrap_or(Value::Bool(false));
            declared.insert(name, required);
        }
        let is_documented = |name: &str| declared.contains_key(&Some(name.to_string()));
        let free_form = request_body.get("free_form") == Some(&Value::Bool(true));
        for (reference, mandatory, parse_site) in &parses {
            let name = last_name(reference);
            let Some(found) = struct_body(reference, Some(parse_site)) else {
                unresolved.push(format!("  {method} {path}: cannot find struct {name}"));
                continue;
            };
            let unread = unreadable_serde(&found);
            if !unread.is_empty() {
                unresolved.extend(unread.iter().map(|attribute| {
                    format!("  {method} {path}: cannot read `{attribute}` in {name}")
                }));
                continue;
            }
            if *mandatory && !free_form {
                for spellings in mandatory_fields(&found) {
                    let key = documented_as(&spellings, &is_documented).map(str::to_string);
                    if declared.get(&key) != Some(&Value::Bool(true)) {
                        body_findings.push(format!(
                            "  {method} {path}: `{}` is mandatory in {name} but the contract does not mark it required",
                            spellings[0]
                        ));
                    }
                }
            }
            // A free-form body is documented by prose, so checks 2 and 3 skip
            // its fields. An empty field list on any other body is checked.
            if !free_form {
                for spellings in accepted_fields(&found) {
                    if documented_as(&spellings, &is_documented).is_none() {
                        undocumented.push(format!(
                            "  {method} {path}: `{}` is accepted by {name} but the contract does not document it",
                            spellings[0]
                        ));
                    }
                }
            }
        }
    }

    let sorted = |items: Vec<String>| -> Vec<String> {
        items
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    };
    let mut found = Found::new();
    found.insert("statuses", sorted(findings));
    found.insert("mandatory", sorted(body_findings));
    found.insert("undocumented", sorted(undocumented));
    found.insert("query_keys", sorted(query_findings));
    found.insert("body_required", sorted(required_findings));
    found.insert("unresolved", sorted(unresolved));
    found.insert("query_params", sorted(typed_query));
    Ok(found)
}

/// The real run: the router and the handlers of `api.rs` against the
/// contract.
fn main() -> Result<ExitCode> {
    // Python read each file with universal newlines, so a CRLF checkout
    // gives the same findings.
    let source = py::universal_newlines(crate::util::read_repo_file(
        "autumn-harvest-plugin/src/api.rs",
    )?);
    let contract_text = crate::util::read_repo_file("docs/api-contract.json")?;
    let contract: Value =
        serde_json::from_str(&contract_text).context("parsing docs/api-contract.json")?;

    let routes = router_routes(&source)?;
    if routes.len() < MIN_ROUTES {
        println!(
            "openapi-response-coverage: the router parser found only {} routes; it has drifted from api.rs",
            routes.len()
        );
        return Ok(ExitCode::FAILURE);
    }

    let found = audit(&source, &contract, &Finder::crates())?;
    println!(
        "OpenAPI contract coverage — {} routes scanned",
        routes.len()
    );
    for (check, title, _) in CHECKS {
        println!("{title}: {}", found[check].len());
    }
    if found.values().all(Vec::is_empty) {
        return Ok(ExitCode::SUCCESS);
    }
    for (check, title, advice) in CHECKS {
        if !found[check].is_empty() {
            println!("\n{title}:\n{}", found[check].join("\n"));
            if let Some(advice) = advice {
                println!("\n{advice}");
            }
        }
    }
    println!("\nThen run cargo xtask dev regenerate-openapi.");
    Ok(ExitCode::FAILURE)
}

/// A `Finder` that reads structs and aliases from the fixture source only.
///
/// The source is the crate `crate`. Text after a `// crate <name>` line is
/// the root file of that scanned crate. So its `crate::` paths count from
/// that crate.
fn fixture_finder(source: &str) -> Finder {
    let mut files: Vec<(String, String)> = Vec::new();
    let splits = py::finditer(r"(?m)^// crate ([a-z_]+)$", source);
    let first_end = splits.first().map_or(source.len(), py::M::start);
    files.push((source[..first_end].to_string(), "crate".to_string()));
    for (index, split) in splits.iter().enumerate() {
        let next = splits.get(index + 1).map_or(source.len(), py::M::start);
        files.push((
            source[split.end()..next].to_string(),
            split.g(1).to_string(),
        ));
    }
    let mut index: StructIndex = HashMap::new();
    let mut aliases: AliasIndex = HashMap::new();
    for (text, crate_name) in &files {
        for (name, found) in struct_index(text, "", crate_name) {
            index.entry(name).or_default().extend(found);
        }
        for (name, found) in type_aliases_in(text, "", crate_name) {
            aliases.entry(name).or_default().extend(found);
        }
    }
    Finder {
        index: Some(Rc::new(index)),
        aliases: Some(Rc::new(aliases)),
        local: Some(("crate".to_string(), String::new())),
    }
}

/// The problems of one self-test case, or the error its audit raised.
fn case_problems(case: &fixtures::Case) -> Result<Vec<String>> {
    let source = fixtures::source(&case.source)?;
    let contract = serde_json::json!({ "routes": case.routes });
    let found = audit(source, &contract, &fixture_finder(source))?;
    let mut problems = Vec::new();
    // A route the fixture router lacks is skipped, so its test would pass
    // without a check.
    let known: BTreeSet<(String, String)> = router_routes(source)?
        .into_iter()
        .map(|(method, path, _)| (method, path))
        .collect();
    for route in &case.routes {
        let method = route.get("method").and_then(Value::as_str).unwrap_or("");
        let path = route.get("path").and_then(Value::as_str).unwrap_or("");
        if !known.contains(&(method.to_string(), path.to_string())) {
            problems.push(format!("{method} {path}: not in the fixture router"));
        }
    }
    for (check, reported) in &found {
        let wanted = case.expected.get(*check).cloned().unwrap_or_default();
        for needle in &wanted {
            let hits = reported
                .iter()
                .filter(|finding| finding.contains(needle.as_str()))
                .count();
            if hits != 1 {
                problems.push(format!(
                    "{check}: want one finding with {}",
                    py::repr(needle)
                ));
            }
        }
        for finding in reported {
            if !wanted
                .iter()
                .any(|needle| finding.contains(needle.as_str()))
            {
                problems.push(format!("{check}: unexpected {}", finding.trim()));
            }
        }
    }
    for check in case.expected.keys() {
        if !found.contains_key(check.as_str()) {
            problems.push(format!("{check}: no such check"));
        }
    }
    let listed: BTreeSet<&str> = CHECKS.iter().map(|(check, _, _)| *check).collect();
    if found.keys().copied().collect::<BTreeSet<_>>() != listed {
        problems.push("CHECKS does not list every check `audit` returns".to_string());
    }
    Ok(problems)
}

/// Runs each fixture through `audit` and compares the findings.
fn self_test() -> Result<ExitCode> {
    let mut failures = 0;
    for case in fixtures::cases()? {
        match case_problems(&case) {
            Err(error) => {
                println!("FAIL  {}\n      raised {error:#}", case.name);
                failures += 1;
            }
            Ok(problems) => {
                let status = if problems.is_empty() { "ok" } else { "FAIL" };
                println!("{status}  {}", case.name);
                for problem in &problems {
                    println!("      {problem}");
                }
                failures += usize::from(!problems.is_empty());
            }
        }
    }
    if failures == 0 {
        println!("\nOK: self-test passed.");
        Ok(ExitCode::SUCCESS)
    } else {
        println!("\n{failures} self-test failure(s).");
        Ok(ExitCode::FAILURE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_self_test_case_passes() {
        let mut failed = Vec::new();
        for case in fixtures::cases().expect("the fixture cases load") {
            match case_problems(&case) {
                Ok(problems) if problems.is_empty() => {}
                Ok(problems) => failed.push(format!("{}: {problems:?}", case.name)),
                Err(error) => failed.push(format!("{}: raised {error:#}", case.name)),
            }
        }
        assert!(failed.is_empty(), "{failed:#?}");
    }

    #[test]
    fn the_checks_list_every_finding_list() {
        let source = fixtures::source("FIXTURE_SOURCE").expect("the fixture exists");
        let contract = serde_json::json!({ "routes": [] });
        let found = audit(source, &contract, &fixture_finder(source)).expect("the audit runs");
        let listed: Vec<&str> = CHECKS.iter().map(|(check, _, _)| *check).collect();
        let mut keys: Vec<&str> = found.keys().copied().collect();
        keys.sort_unstable();
        let mut sorted = listed.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn a_line_index_counts_newlines_before_a_position() {
        let lines = Lines::new("a\nb\nc");
        assert_eq!(lines.line(0), 1);
        assert_eq!(lines.line(2), 2);
        assert_eq!(lines.line(4), 3);
    }

    #[test]
    fn a_quoted_line_is_cut_to_88_characters() {
        let long = format!("  {}  ", "x".repeat(100));
        assert_eq!(quoted(&[long.as_str()], 1).len(), 88);
    }

    #[test]
    fn the_status_tables_hold_the_expected_entries() {
        assert_eq!(named("IM_A_TEAPOT"), Some(418));
        assert_eq!(autumn_error_status("validation"), Some(422));
        assert!(
            tables::AUTUMN_ERROR_STATUS
                .iter()
                .all(|(_, status)| *status >= 400)
        );
    }
}
