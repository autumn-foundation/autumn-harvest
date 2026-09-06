#!/usr/bin/env python3
"""Check the API contract against the handlers it describes.

Three checks, all mechanical:

1. Every HTTP status a handler can return is declared for that route.
2. Every request-body field that is mandatory on the wire is marked required.
3. Every request-body field the handler accepts is documented at all.

The published OpenAPI document is generated from `docs/api-contract.json`, so
anything missing there is missing from every generated client. This audit reads
the router and the handlers in `autumn-harvest-plugin/src/api.rs` and compares
them against the contract.

For check 1 it collects the `StatusCode::` values each handler can return.
Checks 2 and 3 resolve each `Json<T>` extractor to its struct. A field is
mandatory when it is neither an `Option` nor carries a serde default, since axum
rejects a request that omits one. A field serde accepts but the contract omits
is missing from the generated client, so an ordinary request cannot be typed.

A `StatusCode::` used in a comparison rather than a response is ignored, and so
is one inside a helper on GENERIC_HELPERS: `map_error` translates a runtime
error variant, so its statuses belong to the error, not to every route that
calls it.

Each handler is followed one level into the helpers it calls, since a status is
often selected in a helper such as `queue_pause_partial_status`. A helper called
by a helper is not followed, so this audit is a floor rather than a proof. The
finding text names the source line, since a status can reach a route through a
helper it shares.

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
# on every route that calls them, which is noise, not coverage.
GENERIC_HELPERS = frozenset({"map_error"})

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


def declared_statuses(route: dict) -> set[int]:
    statuses = {route["success_response"]["status"]}
    statuses |= {entry["status"] for entry in route.get("additional_responses", [])}
    statuses |= {entry["status"] for entry in route.get("error_responses", [])}
    return statuses


def main() -> int:
    source = API.read_text()
    lines = source.split("\n")
    contract = json.loads(CONTRACT.read_text())
    by_route = {(r["method"], r["path"]): r for r in contract["routes"]}

    routes = router_routes(source)
    if len(routes) < MIN_ROUTES:
        print(
            "openapi-response-coverage: the router parser found only %d routes; "
            "it has drifted from api.rs" % len(routes)
        )
        return 1

    findings: list[str] = []
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

    body_findings: list[str] = []
    undocumented: list[str] = []
    for method, path, handler in routes:
        params = handler_parameters(source, handler)
        route = by_route.get((method, path))
        if params is None or route is None:
            continue
        # A bare `Json<T>` means the body is mandatory; `Result<Json<T>, _>` and
        # `Option<Json<T>>` leave that to the handler. All three still name the
        # struct whose fields serde accepts, which is what check 3 needs.
        bare = re.search(r"Json\(\s*[a-z_0-9]+\s*\)\s*:\s*Json<([A-Za-z0-9_]+)>", params)
        extractor = (
            bare
            or re.search(r"Result<Json<([A-Za-z0-9_]+)>", params)
            or re.search(r"Option<Json<([A-Za-z0-9_]+)>>", params)
        )
        if extractor is None or extractor.group(1) == "Value":
            continue
        struct = struct_body(extractor.group(1))
        if struct is None:
            continue
        declared = {
            field["name"]: field.get("required", False)
            for field in (route.get("request_body") or {}).get("fields", []) or []
        }
        for name in (mandatory_fields(struct) if bare else []):
            if declared.get(name) is not True:
                body_findings.append(
                    "  %s %s: `%s` is mandatory in %s but the contract does not "
                    "mark it required" % (method, path, name, extractor.group(1))
                )
        # An empty field list is a free-form body, documented by prose.
        if declared:
            for name in accepted_fields(struct):
                if name not in declared:
                    undocumented.append(
                        "  %s %s: `%s` is accepted by %s but the contract does "
                        "not document it" % (method, path, name, extractor.group(1))
                    )

    print("OpenAPI contract coverage — %d routes scanned" % len(routes))
    print("Undeclared statuses: %d" % len(findings))
    print("Unmarked mandatory body fields: %d" % len(body_findings))
    print("Undocumented body fields: %d" % len(undocumented))
    if not findings and not body_findings and not undocumented:
        return 0

    if findings:
        print("\nUndeclared statuses:\n" + "\n".join(sorted(set(findings))))
        print(
            "\nDeclare each in docs/api-contract.json. A status that carries a "
            "body belongs in additional_responses with its fields; one that does "
            "not belongs in error_responses."
        )
    if body_findings:
        print("\nUnmarked mandatory body fields:\n" + "\n".join(sorted(set(body_findings))))
    if undocumented:
        print("\nUndocumented body fields:\n" + "\n".join(sorted(set(undocumented))))
    print("\nThen run scripts/regenerate-openapi.sh.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
