#!/usr/bin/env python3
"""Check the API contract against the handlers it describes.

Four checks, all mechanical:

1. Every HTTP status a handler can return is declared for that route.
2. Every request-body field that is mandatory on the wire is marked required.
3. Every request-body field the handler accepts is documented at all.
4. Every query key a hand-rolled parser accepts is documented at all.

The published OpenAPI document is generated from `docs/api-contract.json`, so
anything missing there is missing from every generated client. This audit reads
the router and the handlers in `autumn-harvest-plugin/src/api.rs` and compares
them against the contract.

For check 1 it collects the `StatusCode::` values each handler can return,
plus the status implied by each `AutumnError::` constructor it calls (see
`AUTUMN_ERROR_STATUS`). A constructor call chained into `.with_status(..)`
carries the overridden status instead, so it is skipped there -- the
`StatusCode::` literal inside that `.with_status(..)` call is what the plain
scan already requires. A constructor passed bare, as in
`.map_err(AutumnError::bad_request_msg)`, names no call and so cannot chain
`.with_status(..)`: its status is always the constructor's own.

Checks 2 and 3 resolve each `Json<T>` extractor to its struct. A field is
mandatory when it is neither an `Option` nor carries a serde default, since axum
rejects a request that omits one. A field serde accepts but the contract omits
is missing from the generated client, so an ordinary request cannot be typed.

Check 4 reads the routes that take a `RawQuery` and parse the pairs by hand. A
key those parsers match is a parameter the route accepts, so a key the contract
omits cannot be expressed by a generated client. Only a literal match arm at the
top of a `pairs` loop is read, so a key computed at runtime is invisible. An arm
naming several spellings passes when the contract documents any one of them,
since an alias needs no second entry in the document.

A `StatusCode::` or `AutumnError::` token is ignored inside a helper on
GENERIC_HELPERS: `map_error` translates a runtime error into whatever status
fits it, not the calling route, so its statuses belong to the error, not to
every route that reaches it. `conflict_from` is not on that list. Its
`HarvestError::Config` arm carries one fixed, literal `StatusCode::CONFLICT`
that belongs to every route calling it, so it is traversed like any other
helper; its other arms still fall through to `map_error`, which stays
excluded, so no variable status leaks in through that path.

Each handler is followed one level into the helpers it calls, since a status is
often selected in a helper such as `queue_pause_partial_status`. A helper called
by a helper is not followed, so this audit is a floor rather than a proof. The
finding text names the source line, since a status can reach a route through a
helper it shares.

A route that parses its query with a typed `Query<T>` extractor is outside check
4, which reads match arms rather than struct fields.

Exit code 1 on any finding. Run standalone:

    python3 docs/audits/openapi-response-coverage.py
"""

from __future__ import annotations

import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
API = ROOT / "autumn-harvest-plugin" / "src" / "api.rs"
CONTRACT = ROOT / "docs" / "api-contract.json"
CRATES = ("autumn-harvest", "autumn-harvest-plugin")

# The status names the handlers use. An unlisted name is skipped rather than
# guessed at.
NAMED = {
    "OK": 200,
    "CREATED": 201,
    "ACCEPTED": 202,
    "NO_CONTENT": 204,
    "MULTI_STATUS": 207,
    "BAD_REQUEST": 400,
    "UNAUTHORIZED": 401,
    "FORBIDDEN": 403,
    "NOT_FOUND": 404,
    "CONFLICT": 409,
    "GONE": 410,
    "PAYLOAD_TOO_LARGE": 413,
    "UNPROCESSABLE_ENTITY": 422,
    "TOO_MANY_REQUESTS": 429,
    "INTERNAL_SERVER_ERROR": 500,
    "NOT_IMPLEMENTED": 501,
    "SERVICE_UNAVAILABLE": 503,
    "GATEWAY_TIMEOUT": 504,
}

VERBS = ("get", "post", "put", "patch", "delete")

# Helpers whose status depends on the runtime error they are handed rather than
# on the calling route. Following them would put every status they can produce
# on every route that calls them, which is noise, not coverage. `conflict_from`
# is not here: its `HarvestError::Config` arm has one fixed, literal 409, so it
# is traversed like an ordinary helper and that literal is picked up by every
# route that calls it. Its other arms delegate to `map_error`, which stays
# excluded, so that delegation contributes no status of its own.
GENERIC_HELPERS = frozenset({"map_error"})

# `AutumnError::<name>(..)` constructors used in this file, and the status each
# implies absent a `.with_status(..)` override. Sourced from autumn-web's
# `error.rs`; a constructor with no call site here is omitted rather than
# guessed at, matching how NAMED above only lists used status names.
AUTUMN_ERROR_STATUS = {
    "internal_server_error": 500,
    "internal_server_error_msg": 500,
    "not_found_msg": 404,
    "bad_request_msg": 400,
    "service_unavailable_msg": 503,
    "unauthorized_msg": 401,
    "validation": 422,
}

# A query-key match arm, naming one key or several spellings of one.
KEY_ARM = re.compile(r'^\s*("[a-z_0-9-]+"(?:\s*\|\s*"[a-z_0-9-]+")*)\s*=>')

# The parser must keep finding the whole router. A large drop means it drifted
# from the source and is no longer checking anything.
MIN_ROUTES = 150


def balanced(text: str, opener: str = "(", closer: str = ")") -> str:
    """The balanced group starting at the first character."""
    depth = 0
    for index, char in enumerate(text):
        if char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                return text[: index + 1]
    return text


def router_routes(source: str) -> list[tuple[str, str, str]]:
    """Every `(METHOD, path, handler)` registered in `harvest_api_router`."""
    start = source.index("pub fn harvest_api_router(")
    end = start + source[start:].index("\n}\n")
    router = source[start:end]

    routes: list[tuple[str, str, str]] = []
    at = router.find(".route(")
    while at >= 0:
        args = balanced(router[at + len(".route(") - 1 :])
        path = re.search(r'"([^"]+)"', args)
        if path:
            for verb in re.finditer(r"\b(%s)\(\s*([A-Za-z0-9_]+)" % "|".join(VERBS), args):
                routes.append((verb.group(1).upper(), path.group(1), verb.group(2)))
        at = router.find(".route(", at + len(".route("))
    return routes


def handler_body(source: str, name: str) -> str | None:
    """The block of `async fn <name>(..)`, or `None` when it is elsewhere."""
    at = source.find("async fn %s(" % name)
    if at < 0:
        return None
    brace = source.index("{", source.index(")", at))
    return balanced(source[brace:], "{", "}")


def function_body(source: str, name: str) -> str | None:
    """The block of a free function, async or not, generic or not."""
    found = re.search(r"\b(?:async )?fn %s\s*[(<]" % re.escape(name), source)
    if found is None:
        return None
    opener = source.find("(", found.start())
    brace = source.find("{", balanced_end(source, opener))
    if brace < 0:
        return None
    return balanced(source[brace:], "{", "}")


def balanced_end(source: str, opener: int) -> int:
    """The index just past the balanced `(..)` starting at `opener`."""
    return opener + len(balanced(source[opener:]))


def called_helpers(source: str, body: str) -> list[str]:
    """Functions defined in this file that the given body calls."""
    names = {name for name in re.findall(r"\b([a-z_][a-z_0-9]{3,})\s*\(", body)}
    return sorted(
        name
        for name in names - GENERIC_HELPERS
        if re.search(r"\b(?:async )?fn %s\s*[(<]" % re.escape(name), source)
    )


def handler_parameters(source: str, name: str) -> str | None:
    """The parameter list of `async fn <name>(..)`."""
    at = source.find("async fn %s(" % name)
    if at < 0:
        return None
    return balanced(source[source.index("(", at) :])


def struct_body(name: str) -> str | None:
    """The block of `struct <name> { .. }`, from either crate."""
    for crate in CRATES:
        for path in sorted((ROOT / crate / "src").rglob("*.rs")):
            source = path.read_text()
            found = re.search(r"\bstruct %s\s*\{" % re.escape(name), source)
            if found:
                return balanced(source[found.end() - 1 :], "{", "}")
    return None


def accepted_fields(struct: str) -> list[str]:
    """Field names serde will accept from the wire."""
    accepted: list[str] = []
    attributes: list[str] = []
    for line in struct.split("\n"):
        text = line.strip()
        if text.startswith("#["):
            attributes.append(text)
            continue
        if not text or text.startswith("//") or text in ("{", "}"):
            continue
        field = re.match(r"(?:pub\s+)?([a-z_0-9]+)\s*:\s*(.+?),?$", text)
        if not field:
            attributes = []
            continue
        joined = " ".join(attributes)
        skipped = re.search(r"serde\([^)]*\bskip\b", joined) and "skip_serializing_if" not in joined
        if not skipped:
            accepted.append(field.group(1))
        attributes = []
    return accepted


def mandatory_fields(struct: str) -> list[str]:
    """Field names a caller must send, given serde's rules."""
    mandatory: list[str] = []
    attributes: list[str] = []
    for line in struct.split("\n"):
        text = line.strip()
        if text.startswith("#["):
            attributes.append(text)
            continue
        if not text or text.startswith("//") or text in ("{", "}"):
            continue
        field = re.match(r"(?:pub\s+)?([a-z_0-9]+)\s*:\s*(.+?),?$", text)
        if not field:
            attributes = []
            continue
        name, declared_type = field.group(1), field.group(2)
        joined = " ".join(attributes)
        skipped = re.search(r"serde\([^)]*\bskip\b", joined) and "skip_serializing_if" not in joined
        if not skipped and "default" not in joined and not declared_type.startswith("Option<"):
            mandatory.append(name)
        attributes = []
    return mandatory


def key_arms(body: str) -> list[tuple[str, ...]]:
    """Query-key literals matched at the top of a `pairs` loop."""
    arms: list[tuple[str, ...]] = []
    bindings = set(
        re.findall(r"for\s*\(\s*([a-z_0-9]+)\s*,\s*[a-z_0-9]+\s*\)\s*in[^\n{]*pairs", body)
    )
    for binding in sorted(bindings):
        for found in re.finditer(r"match\s+%s\.as_str\(\)\s*\{" % re.escape(binding), body):
            block = balanced(body[found.end() - 1 :], "{", "}")
            # Depth 1 is the arm list itself. A nested match sits deeper, so a
            # value arm such as `"asc" => Order::Asc` is not read as a key.
            depth = 0
            for line in block.split("\n"):
                if depth == 1:
                    hit = KEY_ARM.match(line)
                    if hit:
                        arms.append(tuple(re.findall(r'"([a-z_0-9-]+)"', hit.group(1))))
                depth += line.count("{") - line.count("}")
    return arms


def overridden_by_with_status(body: str, call_open_paren: int) -> bool:
    """Whether a `.with_status(..)` call immediately follows a call whose
    argument list opens at `call_open_paren` (the `(` right after the
    constructor name).

    The constructor's own implied status is not what the route returns when
    this is true; the `StatusCode::` literal inside `.with_status(..)` is,
    and the plain scan already requires that literal to be declared.
    """
    call_text = balanced(body[call_open_paren:])
    after = body[call_open_paren + len(call_text) :]
    return after.lstrip().startswith(".with_status(")


# The OpenAPI type of each Rust scalar a query struct uses. An unlisted type is
# reported, not guessed at.
WIRE_TYPES = {
    "String": "string",
    "Uuid": "string",
    "uuid::Uuid": "string",
    "bool": "boolean",
    "f32": "number",
    "f64": "number",
    **{
        kind: "integer"
        for kind in (
            "i8 i16 i32 i64 i128 isize u8 u16 u32 u64 u128 usize".split()
        )
    },
}

# A parameter that carries the raw request body.
BYTE_PARAMETER = re.compile(
    r"\b([a-z_][a-z_0-9]*)\s*:\s*&?\s*(?:(?:axum::body::)?Bytes\b|\[u8\])"
)

# A body parse, with an optional turbofish type and the variable it reads.
FROM_SLICE = re.compile(
    r"serde_json::from_slice(?:::<\s*([A-Za-z0-9_:]+)\s*>)?\s*\(\s*&?\s*([a-z_][a-z_0-9]*)\s*\)"
)


def function_parts(source: str, name: str) -> tuple[str, str, str] | None:
    """The parameter list, return clause and block of a free function."""
    found = re.search(r"\b(?:async )?fn %s\s*[(<]" % re.escape(name), source)
    if found is None:
        return None
    opener = source.find("(", found.start())
    params = balanced(source[opener:])
    brace = source.find("{", opener + len(params))
    if brace < 0:
        return None
    returns = source[opener + len(params) : brace]
    return params, returns, balanced(source[brace:], "{", "}")


def byte_parameters(params: str) -> set[str]:
    """Names of the parameters that carry the raw request body."""
    return set(BYTE_PARAMETER.findall(params))


def raw_body_parses(source: str, handler: str) -> list[tuple[str | None, bool]]:
    """`(type, guarded)` for each raw-body parse in a handler and its helpers.

    The type comes from a turbofish, then from a `let` binding in the same
    statement, then from a `Result<T, _>` return type. It is `None` when none
    of those names it. A parse is guarded when an `.is_empty()` test on the
    same variable comes before it, since the handler then runs without a body.
    """
    handler_parts = function_parts(source, handler)
    if handler_parts is None:
        return []
    names = [handler] + called_helpers(source, handler_parts[2])
    parses: list[tuple[str | None, bool]] = []
    for name in names:
        parts = function_parts(source, name)
        if parts is None:
            continue
        params, returns, block = parts
        carriers = byte_parameters(params)
        for hit in FROM_SLICE.finditer(block):
            variable = hit.group(2)
            if variable not in carriers:
                continue
            before = block[: hit.start()]
            parses.append(
                (
                    parse_type(hit.group(1), before, returns),
                    re.search(r"\b%s\.is_empty\(\)" % re.escape(variable), before) is not None,
                )
            )
    return parses


def parse_type(turbofish: str | None, before: str, returns: str) -> str | None:
    """The struct a `from_slice` call yields, or `None` when it is unnamed."""
    if turbofish:
        return turbofish.split("::")[-1]
    statement = before[before.rfind(";") + 1 :]
    binding = re.search(r"\blet\s+(?:mut\s+)?[a-z_0-9]+\s*:\s*([A-Za-z0-9_:]+)\s*=", statement)
    if binding:
        return binding.group(1).split("::")[-1]
    result = re.search(r"->\s*Result<\s*([A-Za-z0-9_:]+)\s*,", returns)
    return result.group(1).split("::")[-1] if result else None


def struct_fields(struct: str) -> list[tuple[str, str, bool]]:
    """`(name, type, mandatory)` for each field serde reads from the wire."""
    fields: list[tuple[str, str, bool]] = []
    attributes: list[str] = []
    for line in struct.split("\n"):
        text = line.strip()
        if text.startswith("#["):
            attributes.append(text)
            continue
        if not text or text.startswith("//") or text in ("{", "}"):
            continue
        field = re.match(r"(?:pub(?:\([a-z]+\))?\s+)?([a-z_0-9]+)\s*:\s*(.+?),?$", text)
        if not field:
            attributes = []
            continue
        name, declared_type = field.group(1), field.group(2)
        joined = " ".join(attributes)
        skipped = re.search(r"serde\([^)]*\bskip\b", joined) and "skip_serializing_if" not in joined
        if not skipped:
            optional = "default" in joined or declared_type.startswith("Option<")
            fields.append((name, declared_type, not optional))
        attributes = []
    return fields


def wire_type(declared_type: str) -> str | None:
    """The OpenAPI type of a Rust field type, unwrapping one `Option`."""
    inner = re.fullmatch(r"Option<\s*(.+?)\s*>", declared_type)
    return WIRE_TYPES.get(inner.group(1) if inner else declared_type)


def query_struct_findings(
    method: str, path: str, params: str, route: dict, find_struct
) -> list[str]:
    """Check 6: a `Query<T>` struct and the route's query parameters agree."""
    extractor = re.search(r"Query\(\s*[a-z_0-9]+\s*\)\s*:\s*Query<([A-Za-z0-9_]+)>", params)
    if extractor is None:
        return []
    name = extractor.group(1)
    struct = find_struct(name)
    if struct is None:
        return []
    documented = {
        entry["name"]: entry for entry in route.get("params", []) if entry.get("in") == "query"
    }
    where = "  %s %s: `%%s`" % (method, path)
    found: list[str] = []
    fields = struct_fields(struct)
    for field, declared_type, mandatory in fields:
        entry = documented.get(field)
        if entry is None:
            found.append(
                where % field + " is accepted by %s but the contract does not document it" % name
            )
            continue
        kind = wire_type(declared_type)
        if kind is None:
            found.append(
                where % field + " has type %s, which maps to no OpenAPI type" % declared_type
            )
        elif entry.get("type") != kind:
            found.append(
                where % field
                + " is %s in %s but the contract says %s" % (kind, name, entry.get("type"))
            )
        if mandatory and entry.get("required") is not True:
            found.append(
                where % field
                + " is mandatory in %s but the contract marks it optional" % name
            )
        if not mandatory and entry.get("required") is True:
            found.append(
                where % field
                + " is optional in %s but the contract marks it required" % name
            )
    accepted = {field for field, _, _ in fields}
    for key in documented.keys() - accepted:
        found.append(where % key + " is documented but %s does not accept it" % name)
    return found


def declared_statuses(route: dict) -> set[int]:
    statuses = {route["success_response"]["status"]}
    statuses |= {entry["status"] for entry in route.get("additional_responses", [])}
    statuses |= {entry["status"] for entry in route.get("error_responses", [])}
    return statuses


def audit(source: str, contract: dict, find_struct) -> dict[str, list[str]]:
    """Every finding, by check. `find_struct` maps a struct name to its block."""
    lines = source.split("\n")
    by_route = {(r["method"], r["path"]): r for r in contract["routes"]}
    routes = router_routes(source)

    findings: list[str] = []
    query_findings: list[str] = []
    for method, path, handler in routes:
        body = handler_body(source, handler)
        route = by_route.get((method, path))
        if body is None or route is None:
            continue
        declared = declared_statuses(route)

        bodies = [body]
        for helper in called_helpers(source, body):
            reached = function_body(source, helper)
            if reached is not None:
                bodies.append(reached)

        params = handler_parameters(source, handler)
        if params is not None and "RawQuery" in params:
            documented = {entry["name"] for entry in route.get("params", [])}
            for reached in bodies:
                for arm in key_arms(reached):
                    if set(arm) & documented:
                        continue
                    query_findings.append(
                        "  %s %s: `%s` is accepted by the query parser but the "
                        "contract does not document it" % (method, path, "` / `".join(arm))
                    )

        for reached in bodies:
            offset = source.index(reached)
            for hit in re.finditer(r"(.{0,30})StatusCode::([A-Z_]+)", reached):
                status = NAMED.get(hit.group(2))
                if status is None or status in declared:
                    continue
                if "==" in hit.group(1) or "!=" in hit.group(1):
                    continue
                line = source[: offset + hit.start()].count("\n") + 1
                findings.append(
                    "  %s %s returns %d, undeclared\n    api.rs:%d  %s"
                    % (method, path, status, line, lines[line - 1].strip()[:88])
                )
            for hit in re.finditer(r"AutumnError::([a-z_]+)\(", reached):
                status = AUTUMN_ERROR_STATUS.get(hit.group(1))
                if status is None or status in declared:
                    continue
                if overridden_by_with_status(reached, hit.end() - 1):
                    continue
                line = source[: offset + hit.start()].count("\n") + 1
                findings.append(
                    "  %s %s returns %d via AutumnError::%s, undeclared\n"
                    "    api.rs:%d  %s"
                    % (
                        method,
                        path,
                        status,
                        hit.group(1),
                        line,
                        lines[line - 1].strip()[:88],
                    )
                )
            # A constructor passed bare, e.g. `.map_err(AutumnError::bad_request_msg)`,
            # names no call and so cannot chain `.with_status(..)`: its status is
            # always the constructor's own.
            for hit in re.finditer(r"AutumnError::([a-z_]+)\)", reached):
                status = AUTUMN_ERROR_STATUS.get(hit.group(1))
                if status is None or status in declared:
                    continue
                line = source[: offset + hit.start()].count("\n") + 1
                findings.append(
                    "  %s %s returns %d via AutumnError::%s (bare fn ref), "
                    "undeclared\n    api.rs:%d  %s"
                    % (
                        method,
                        path,
                        status,
                        hit.group(1),
                        line,
                        lines[line - 1].strip()[:88],
                    )
                )

    body_findings: list[str] = []
    undocumented: list[str] = []
    required_findings: list[str] = []
    unresolved: list[str] = []
    typed_query: list[str] = []
    for method, path, handler in routes:
        params = handler_parameters(source, handler)
        route = by_route.get((method, path))
        if params is None or route is None:
            continue
        typed_query += query_struct_findings(method, path, params, route, find_struct)

        # A bare `Json<T>` means the body is mandatory; `Result<Json<T>, _>` and
        # `Option<Json<T>>` leave that to the handler. All three still name the
        # struct whose fields serde accepts, which is what check 3 needs.
        bare = re.search(r"Json\(\s*[a-z_0-9]+\s*\)\s*:\s*Json<([A-Za-z0-9_]+)>", params)
        extractor = (
            bare
            or re.search(r"Result<Json<([A-Za-z0-9_]+)>", params)
            or re.search(r"Option<Json<([A-Za-z0-9_]+)>>", params)
        )
        # (struct name, whether the body is mandatory) for each parse.
        parses: list[tuple[str, bool]] = []
        if extractor is not None and extractor.group(1) != "Value":
            parses.append((extractor.group(1), bool(bare)))
        mandatory_body = bool(bare)
        if byte_parameters(params):
            for name, guarded in raw_body_parses(source, handler):
                if name is None:
                    unresolved.append(
                        "  %s %s: cannot resolve the body type of a "
                        "`serde_json::from_slice` call" % (method, path)
                    )
                elif name != "Value":
                    parses.append((name, not guarded))
                    mandatory_body |= not guarded

        request_body = route.get("request_body") or {}
        if mandatory_body and request_body.get("required") is not True:
            required_findings.append(
                "  %s %s: the body is mandatory in the handler but the contract "
                "does not mark it required" % (method, path)
            )
        declared = {
            field["name"]: field.get("required", False)
            for field in request_body.get("fields", []) or []
        }
        for name, mandatory in parses:
            struct = find_struct(name)
            if struct is None:
                continue
            for field in (mandatory_fields(struct) if mandatory else []):
                if declared.get(field) is not True:
                    body_findings.append(
                        "  %s %s: `%s` is mandatory in %s but the contract does not "
                        "mark it required" % (method, path, field, name)
                    )
            # An empty field list is a free-form body, documented by prose.
            if declared:
                for field in accepted_fields(struct):
                    if field not in declared:
                        undocumented.append(
                            "  %s %s: `%s` is accepted by %s but the contract does "
                            "not document it" % (method, path, field, name)
                        )

    return {
        "statuses": sorted(set(findings)),
        "mandatory": sorted(set(body_findings)),
        "undocumented": sorted(set(undocumented)),
        "query_keys": sorted(set(query_findings)),
        "body_required": sorted(set(required_findings)),
        "unresolved": sorted(set(unresolved)),
        "query_params": sorted(set(typed_query)),
    }


# (check, title, advice) for each finding list `audit` returns, in print order.
CHECKS = [
    (
        "statuses",
        "Undeclared statuses",
        "Declare each in docs/api-contract.json. A status that carries a body "
        "belongs in additional_responses with its fields; one that does not "
        "belongs in error_responses.",
    ),
    ("mandatory", "Unmarked mandatory body fields", None),
    ("undocumented", "Undocumented body fields", None),
    (
        "query_keys",
        "Undocumented query keys",
        "Add each to the route's `params` in docs/api-contract.json. An alias "
        "of a documented key needs no entry of its own.",
    ),
    (
        "body_required",
        "Mandatory bodies marked optional",
        "Set the route's `request_body.required` to true.",
    ),
    (
        "unresolved",
        "Unresolved body types",
        "Name the type in the source, for example `from_slice::<T>(..)`.",
    ),
    (
        "query_params",
        "Typed query mismatches",
        "Give each `Query<T>` field one entry in the route's `params`, with "
        "the same name, type and required flag.",
    ),
]


def main() -> int:
    source = API.read_text()
    contract = json.loads(CONTRACT.read_text())

    routes = router_routes(source)
    if len(routes) < MIN_ROUTES:
        print(
            "openapi-response-coverage: the router parser found only %d routes; "
            "it has drifted from api.rs" % len(routes)
        )
        return 1

    found = audit(source, contract, struct_body)
    print("OpenAPI contract coverage — %d routes scanned" % len(routes))
    for check, title, _ in CHECKS:
        print("%s: %d" % (title, len(found[check])))
    if not any(found.values()):
        return 0

    for check, title, advice in CHECKS:
        if found[check]:
            print("\n%s:\n" % title + "\n".join(found[check]))
            if advice:
                print("\n" + advice)
    print("\nThen run scripts/regenerate-openapi.sh.")
    return 1


# A fixture router and its handlers. Each self-test pairs this source with a
# small contract and names the findings it must produce.
FIXTURE_SOURCE = r'''
pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/things", post(create_thing))
        .route("/things/{id}", get(get_thing))
}

async fn create_thing(Json(body): Json<CreateThing>) -> Response {
    StatusCode::CREATED.into_response()
}

async fn get_thing(Path(id): Path<String>) -> Response {
    if id.is_empty() {
        return AutumnError::bad_request_msg("empty").into_response();
    }
    StatusCode::OK.into_response()
}

struct CreateThing {
    name: String,
    #[serde(default)]
    note: Option<String>,
}
'''


def fixture_struct(source: str):
    """A `find_struct` that reads structs from the fixture source only."""

    def find(name: str) -> str | None:
        found = re.search(r"\bstruct %s\s*\{" % re.escape(name), source)
        return balanced(source[found.end() - 1 :], "{", "}") if found else None

    return find


def fixture_route(method: str, path: str, status: int, **extra) -> dict:
    route = {"method": method, "path": path, "success_response": {"status": status}}
    route.update(extra)
    return route


def body_of(*fields: tuple[str, bool], required: bool = True) -> dict:
    return {
        "required": required,
        "fields": [{"name": name, "required": flag} for name, flag in fields],
    }


# Raw-byte bodies and typed queries. Each handler shows one way the source
# parses a body or a query.
FIXTURE_BYTES = r'''
pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/raw/strict", post(raw_strict))
        .route("/raw/optional", post(raw_optional))
        .route("/raw/helper", post(raw_helper))
        .route("/raw/annotated", post(raw_annotated))
        .route("/raw/returned", post(raw_returned))
        .route("/raw/untyped", post(raw_untyped))
        .route("/raw/value", post(raw_value))
        .route("/raw/other", post(raw_other))
        .route("/search", get(search))
        .route("/tagged", get(tagged))
}

async fn raw_strict(headers: HeaderMap, body: axum::body::Bytes) -> Response {
    let request = match serde_json::from_slice::<Widget>(&body) {
        Ok(request) => request,
        Err(error) => return reject(error),
    };
    StatusCode::OK.into_response()
}

async fn raw_optional(body: Bytes) -> Response {
    let request: Widget = if body.is_empty() {
        Widget::default()
    } else {
        serde_json::from_slice::<Widget>(&body).unwrap_or_default()
    };
    StatusCode::OK.into_response()
}

fn parse_widget(body: &[u8]) -> Result<Widget, Response> {
    let widget = serde_json::from_slice::<Widget>(body).map_err(reject)?;
    Ok(widget)
}

async fn raw_helper(body: Bytes) -> Response {
    let widget = match parse_widget(&body) {
        Ok(widget) => widget,
        Err(response) => return response,
    };
    StatusCode::OK.into_response()
}

async fn raw_annotated(body_bytes: Bytes) -> Response {
    let widget: Widget = match serde_json::from_slice(&body_bytes) {
        Ok(widget) => widget,
        Err(error) => return reject(error),
    };
    StatusCode::OK.into_response()
}

async fn parse_optional_widget(body: &axum::body::Bytes) -> Result<Widget, Response> {
    if body.is_empty() {
        return Ok(Widget::default());
    }
    match serde_json::from_slice(body) {
        Ok(widget) => Ok(widget),
        Err(error) => Err(reject(error)),
    }
}

async fn raw_returned(body: Bytes) -> Response {
    let widget = match parse_optional_widget(&body).await {
        Ok(widget) => widget,
        Err(response) => return response,
    };
    StatusCode::OK.into_response()
}

async fn raw_untyped(body: Bytes) -> Response {
    let widget = serde_json::from_slice(&body).map(consume);
    StatusCode::OK.into_response()
}

async fn raw_value(body: Bytes) -> Response {
    let value = serde_json::from_slice::<Value>(&body).ok();
    StatusCode::OK.into_response()
}

async fn raw_other(_body: Bytes) -> Response {
    let stored = load_stored();
    let widget: Widget = serde_json::from_slice(&stored).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn search(Query(query): Query<SearchQuery>) -> Response {
    StatusCode::OK.into_response()
}

async fn tagged(Query(query): Query<TaggedQuery>) -> Response {
    StatusCode::OK.into_response()
}

struct Widget {
    name: String,
    #[serde(default)]
    size: Option<u32>,
}

struct SearchQuery {
    term: String,
    #[serde(default)]
    exact: bool,
    limit: Option<u32>,
}

struct TaggedQuery {
    tags: Vec<String>,
}
'''

WIDGET_BODY = body_of(("name", True), ("size", False))
OPTIONAL_WIDGET = body_of(("name", True), ("size", False), required=False)


def query_param(name: str, kind: str, required: bool) -> dict:
    return {"name": name, "in": "query", "type": kind, "required": required}


SEARCH_PARAMS = [
    query_param("term", "string", True),
    query_param("exact", "boolean", False),
    query_param("limit", "integer", False),
]


# (name, source, routes, expected findings by check). Each expected string must
# appear in exactly one finding, and the check must report nothing else.
SELF_TESTS: list[tuple[str, str, list[dict], dict[str, list[str]]]] = [
    (
        "a complete contract is clean",
        FIXTURE_SOURCE,
        [
            fixture_route(
                "POST", "/things", 201, request_body=body_of(("name", True), ("note", False))
            ),
            fixture_route("GET", "/things/{id}", 200, error_responses=[{"status": 400}]),
        ],
        {},
    ),
    (
        "an AutumnError constructor status is required",
        FIXTURE_SOURCE,
        [
            fixture_route(
                "POST", "/things", 201, request_body=body_of(("name", True), ("note", False))
            ),
            fixture_route("GET", "/things/{id}", 200),
        ],
        {"statuses": ["GET /things/{id} returns 400 via AutumnError::bad_request_msg"]},
    ),
    (
        "a mandatory Json field is marked required",
        FIXTURE_SOURCE,
        [
            fixture_route(
                "POST", "/things", 201, request_body=body_of(("name", False), ("note", False))
            ),
            fixture_route("GET", "/things/{id}", 200, error_responses=[{"status": 400}]),
        ],
        {"mandatory": ["POST /things: `name` is mandatory in CreateThing"]},
    ),
    (
        "an accepted Json field is documented",
        FIXTURE_SOURCE,
        [
            fixture_route("POST", "/things", 201, request_body=body_of(("name", True))),
            fixture_route("GET", "/things/{id}", 200, error_responses=[{"status": 400}]),
        ],
        {"undocumented": ["POST /things: `note` is accepted by CreateThing"]},
    ),
    (
        "a bare Json body is marked required",
        FIXTURE_SOURCE,
        [
            fixture_route(
                "POST",
                "/things",
                201,
                request_body=body_of(("name", True), ("note", False), required=False),
            ),
        ],
        {"body_required": ["POST /things: the body is mandatory"]},
    ),
    (
        "raw-byte bodies that match the contract are clean",
        FIXTURE_BYTES,
        [
            fixture_route("POST", "/raw/strict", 200, request_body=WIDGET_BODY),
            fixture_route("POST", "/raw/optional", 200, request_body=OPTIONAL_WIDGET),
            fixture_route("POST", "/raw/helper", 200, request_body=WIDGET_BODY),
            fixture_route("POST", "/raw/annotated", 200, request_body=WIDGET_BODY),
            fixture_route("POST", "/raw/returned", 200, request_body=OPTIONAL_WIDGET),
            fixture_route("POST", "/raw/value", 200, request_body=body_of(required=False)),
            fixture_route("POST", "/raw/other", 200),
        ],
        {},
    ),
    (
        "an unguarded raw-byte body is marked required",
        FIXTURE_BYTES,
        [
            fixture_route(
                "POST",
                "/raw/strict",
                200,
                request_body=body_of(("name", False), ("size", False), required=False),
            ),
            fixture_route("POST", "/raw/optional", 200, request_body=OPTIONAL_WIDGET),
        ],
        {
            "body_required": ["POST /raw/strict: the body is mandatory"],
            "mandatory": ["POST /raw/strict: `name` is mandatory in Widget"],
        },
    ),
    (
        "a raw-byte body parsed in a helper is read",
        FIXTURE_BYTES,
        [
            fixture_route(
                "POST", "/raw/helper", 200, request_body=body_of(("name", True))
            ),
        ],
        {"undocumented": ["POST /raw/helper: `size` is accepted by Widget"]},
    ),
    (
        "a raw-byte body typed by its binding is read",
        FIXTURE_BYTES,
        [
            fixture_route(
                "POST",
                "/raw/annotated",
                200,
                request_body=body_of(("name", False), ("size", False)),
            ),
        ],
        {"mandatory": ["POST /raw/annotated: `name` is mandatory in Widget"]},
    ),
    (
        "a raw-byte body typed by its helper's return type is read",
        FIXTURE_BYTES,
        [
            fixture_route(
                "POST",
                "/raw/returned",
                200,
                request_body=body_of(("name", False), required=False),
            ),
        ],
        {"undocumented": ["POST /raw/returned: `size` is accepted by Widget"]},
    ),
    (
        "a raw-byte body of unknown type is reported",
        FIXTURE_BYTES,
        [fixture_route("POST", "/raw/untyped", 200, request_body=OPTIONAL_WIDGET)],
        {"unresolved": ["POST /raw/untyped: cannot resolve the body type"]},
    ),
    (
        "a typed query that matches the contract is clean",
        FIXTURE_BYTES,
        [fixture_route("GET", "/search", 200, params=SEARCH_PARAMS)],
        {},
    ),
    (
        "a typed query field is documented",
        FIXTURE_BYTES,
        [fixture_route("GET", "/search", 200, params=SEARCH_PARAMS[:2])],
        {"query_params": ["GET /search: `limit` is accepted by SearchQuery"]},
    ),
    (
        "a typed query field carries its wire type",
        FIXTURE_BYTES,
        [
            fixture_route(
                "GET",
                "/search",
                200,
                params=[SEARCH_PARAMS[0], query_param("exact", "string", False), SEARCH_PARAMS[2]],
            )
        ],
        {"query_params": ["GET /search: `exact` is boolean in SearchQuery but the contract says string"]},
    ),
    (
        "a typed query field carries its required flag",
        FIXTURE_BYTES,
        [
            fixture_route(
                "GET",
                "/search",
                200,
                params=[
                    query_param("term", "string", False),
                    SEARCH_PARAMS[1],
                    query_param("limit", "integer", True),
                ],
            )
        ],
        {
            "query_params": [
                "GET /search: `term` is mandatory in SearchQuery but the contract marks it optional",
                "GET /search: `limit` is optional in SearchQuery but the contract marks it required",
            ]
        },
    ),
    (
        "a documented query key the struct ignores is reported",
        FIXTURE_BYTES,
        [
            fixture_route(
                "GET", "/search", 200, params=SEARCH_PARAMS + [query_param("page", "integer", False)]
            )
        ],
        {"query_params": ["GET /search: `page` is documented but SearchQuery does not accept it"]},
    ),
    (
        "a query field with no wire type is reported",
        FIXTURE_BYTES,
        [fixture_route("GET", "/tagged", 200, params=[query_param("tags", "string", True)])],
        {"query_params": ["GET /tagged: `tags` has type Vec<String>, which maps to no OpenAPI type"]},
    ),
]


def self_test() -> int:
    """Run each fixture through `audit` and compare the findings."""
    failures = 0
    for name, source, routes, expected in SELF_TESTS:
        found = audit(source, {"routes": routes}, fixture_struct(source))
        problems: list[str] = []
        for check, reported in found.items():
            wanted = expected.get(check, [])
            for needle in wanted:
                hits = [finding for finding in reported if needle in finding]
                if len(hits) != 1:
                    problems.append("%s: want one finding with %r" % (check, needle))
            extra = [f for f in reported if not any(needle in f for needle in wanted)]
            problems += ["%s: unexpected %s" % (check, f.strip()) for f in extra]
        for check in expected.keys() - found.keys():
            problems.append("%s: no such check" % check)
        if found.keys() != {check for check, _, _ in CHECKS}:
            problems.append("CHECKS does not list every check `audit` returns")
        status = "ok" if not problems else "FAIL"
        print("%s  %s" % (status, name))
        for problem in problems:
            print("      " + problem)
        failures += bool(problems)
    print("\nOK: self-test passed." if not failures else "\n%d self-test failure(s)." % failures)
    return 1 if failures else 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        sys.exit(self_test())
    sys.exit(main())
