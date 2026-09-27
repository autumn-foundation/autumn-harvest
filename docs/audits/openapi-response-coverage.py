#!/usr/bin/env python3
"""Check the API contract against the handlers it describes.

Seven checks, all mechanical:

1. Every HTTP status a handler can return is declared for that route.
2. Every request-body field that is mandatory on the wire is marked required.
3. Every request-body field the handler accepts is documented at all.
4. Every query key a hand-rolled parser accepts is documented at all.
5. Every body the handler cannot run without is marked required.
6. Every `Query<T>` field is documented with its type and required flag.
7. Every body and query type the audit reads resolves to a struct.

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

Checks 2 and 3 resolve each `Json<T>` extractor to its struct, with or without
a path such as `axum::Json`. A field is mandatory when it is neither an
`Option` nor carries a serde default, since axum rejects a request that omits
one. A field serde accepts but the contract omits is missing from the
generated client, so an ordinary request cannot be typed. Check 3 skips only a
body the contract marks `free_form`. An empty field list on any other body is
checked.

A handler can also take the raw `Bytes` and call `serde_json::from_slice`
itself. Checks 2, 3 and 5 read that parse when it reads a parameter of type
`Bytes`, `&[u8]` or `Vec<u8>`. The parse can be in the handler, or in a helper
one level down that the handler passes the body to. In a helper, only the
parameter at the position of the body argument is a body. A copy of the body
under another name, such as `body.to_vec()`, is not read. The type comes from
a turbofish, then from a typed `let` in the same statement, then from a
`Result<T, _>` return type. The last two apply only when the call ends its
expression, since a `.map(..)` after it yields another type. A `Value` body is
free-form, so checks 2 and 3 skip it. Check 5 still reads it.

Check 5 treats a bare `Json<T>` as mandatory, since axum rejects a request
without it. A `Result<Json<T>, _>` is mandatory when the handler rejects its
error, through `?`, `.map_err(..)` or an `Err` arm that builds an error. A
raw-byte parse is mandatory unless an `if` on `.is_empty()` lets an empty body
skip it. The parse must be in the arm that runs for a non-empty body. An
earlier `if body.is_empty() { .. }` also counts when its block returns `Ok(..)`
and no error. A parse that turns its error into a value is
optional too, such as `.ok()`, `.unwrap_or_default()` or an `if let Ok(..)`
whose `else` does not reject. Check 2 applies only to a mandatory body.

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

Check 6 compares every `Query<T>` struct of a route with its `in: query`
parameters. `WIRE_TYPES` gives the OpenAPI type of a field after the audit
removes one `Option`. A field is optional when it is an `Option` or has a serde
default, on the field or on the struct. By default, serde ignores an unknown
query key, so a documented key that no struct has is a finding.

The audit reads the serde attributes `default`, `skip`, `skip_deserializing`,
`rename = ".."` and `alias = ".."`. A field is documented when the contract
names its wire name or any alias. `rename_all`, `flatten` and `rename(..)` are
check 7 findings, since the audit cannot read the wire names they make.

Check 7 stops the audit from skipping what it cannot read. These are findings:
a `from_slice` call it cannot read, a body type it cannot resolve, a
`Json<..>` or `Query<..>` extractor it cannot read, and a struct it cannot
find. A generic struct such as `struct Q<'a>` is one it cannot find.

A helper called by bare name is found as a free function, so an earlier method
of the same name does not hide it. A handler is found from its `async fn`
line, so an earlier fn of the same name does not hide it. Comments in a
parameter list are removed before any read.

Exit code 1 on any finding. Run standalone, or run the fixtures:

    python3 docs/audits/openapi-response-coverage.py
    python3 docs/audits/openapi-response-coverage.py --self-test
"""

from __future__ import annotations

import functools
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


def handler_parts(source: str, name: str) -> tuple[str, str, str] | None:
    """The parts of `async fn <name>(..)`, or `None` when it is elsewhere.

    The lookup starts at `async fn`, so an earlier fn of the same name, such
    as a method, does not hide the handler.
    """
    at = source.find("async fn %s(" % name)
    return None if at < 0 else parts_at(source, at)


def handler_body(source: str, name: str) -> str | None:
    """The block of `async fn <name>(..)`, or `None` when it is elsewhere."""
    parts = handler_parts(source, name)
    return parts[2] if parts else None


def function_body(source: str, name: str) -> str | None:
    """The block of a free function, async or not, generic or not."""
    parts = function_parts(source, name)
    return parts[2] if parts else None


@functools.lru_cache(maxsize=None)
def defined_functions(source: str) -> dict[str, int]:
    """Where each function in the source is defined, by name.

    A free function at the start of a line wins over an indented method of the
    same name, since a call by bare name reaches the free function. A name
    with no free function falls back to its first definition.
    """
    free: dict[str, int] = {}
    first: dict[str, int] = {}
    for found in re.finditer(r"\b(?:async )?fn ([A-Za-z_][A-Za-z_0-9]*)\s*[(<]", source):
        name = found.group(1)
        first.setdefault(name, found.start())
        line_start = source.rfind("\n", 0, found.start()) + 1
        prefix = source[line_start : found.start()]
        if re.fullmatch(r"(?:pub(?:\([^)]*\))?\s+)?(?:const\s+)?(?:unsafe\s+)?", prefix):
            free.setdefault(name, found.start())
    return {**first, **free}


def called_helpers(source: str, body: str) -> list[str]:
    """Functions defined in this file that the given body calls."""
    names = set(re.findall(r"\b([a-z_][a-z_0-9]{3,})\s*\(", body))
    return sorted((names - GENERIC_HELPERS) & defined_functions(source).keys())


def handler_parameters(source: str, name: str) -> str | None:
    """The parameter list of `async fn <name>(..)`."""
    parts = handler_parts(source, name)
    return parts[0] if parts else None


def struct_body(name: str) -> str | None:
    """The block of `struct <name> { .. }`, from either crate."""
    return defined_structs().get(name)


@functools.lru_cache(maxsize=None)
def defined_structs() -> dict[str, str]:
    """The first block of each struct in either crate, by name."""
    blocks: dict[str, str] = {}
    for crate in CRATES:
        for path in sorted((ROOT / crate / "src").rglob("*.rs")):
            source = path.read_text()
            for found in re.finditer(r"\bstruct ([A-Za-z_][A-Za-z_0-9]*)\s*\{", source):
                if found.group(1) not in blocks:
                    blocks[found.group(1)] = struct_text(source, found)
    return blocks


def struct_text(source: str, found: re.Match) -> str:
    """The attribute lines above a struct match, then its block.

    An attribute that rustfmt splits over several lines is read up to its
    opening `#[`, since its closing `)]` line alone does not start with `#[`.
    """
    lines = source[: found.start()].split("\n")[:-1]
    attributes: list[str] = []
    depth = 0
    while lines:
        text = lines[-1].strip()
        if depth == 0 and not (text.startswith(("#[", "//")) or text.endswith("]")):
            break
        depth += text.count("]") - text.count("[")
        attributes.insert(0, lines.pop().strip())
    block = balanced(source[found.end() - 1 :], "{", "}")
    return "\n".join(attributes + [block])


def accepted_fields(struct: str) -> list[tuple[str, ...]]:
    """The spellings of each field serde will accept from the wire."""
    return [spellings for _, _, _, spellings in struct_fields(struct)]


def mandatory_fields(struct: str) -> list[tuple[str, ...]]:
    """The spellings of each field a caller must send, given serde's rules."""
    return [spellings for _, _, mandatory, spellings in struct_fields(struct) if mandatory]


def documented_as(spellings: tuple[str, ...], documented) -> str | None:
    """The first spelling of a field that the contract documents, if any."""
    return next((name for name in spellings if name in documented), None)


def without_comment_lines(text: str) -> str:
    """The text without its comment lines, so a doc comment is not code."""
    return "\n".join(line for line in text.split("\n") if not line.strip().startswith("//"))


def unreadable_serde(struct: str) -> list[str]:
    """Serde attributes in a struct that change wire names in ways not read.

    `rename_all`, `flatten` and the `rename(..)` form are reported, not
    guessed at. A plain `rename = ".."` and `alias = ".."` are read.
    """
    found: set[str] = set()
    for attribute in re.findall(r"#\[serde\(([^\]]*)\)\]", without_comment_lines(struct)):
        found |= set(re.findall(r"\b(rename_all|flatten)\b", attribute))
        if re.search(r"\brename\s*\(", attribute):
            found.add("rename(..)")
    return sorted(found)


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


# The OpenAPI type of each Rust scalar a query struct uses. The audit reports an
# unlisted type. It does not guess one.
WIRE_TYPES = {
    "String": "string",
    "Uuid": "string",
    "uuid::Uuid": "string",
    "bool": "boolean",
    "f32": "number",
    "f64": "number",
    **{kind: "integer" for kind in ("i8 i16 i32 i64 i128 isize u8 u16 u32 u64 u128 usize".split())},
}

# A typed query extractor in any form, naming its struct. A `Query<` that this
# does not match is reported, not skipped.
QUERY_EXTRACTOR = re.compile(r"\bQuery<\s*([A-Za-z0-9_:]+)\s*>")

# A parameter that carries the raw request body.
BYTE_PARAMETER = re.compile(
    r"\b([a-z_][a-z_0-9]*)\s*:\s*&?\s*(?:(?:[a-z_]+::)*Bytes\b|\[u8\]|Vec<u8>)"
)

# A `from_slice` call: its turbofish, if any, then its argument list.
FROM_SLICE = re.compile(r"\bfrom_slice\s*(?:::<|\()")

# A token that marks a block as an error path: an `AutumnError`, an `Err(..)`,
# or a `StatusCode::` name for a 4xx or 5xx status. A 2xx status is no error.
ERROR_TOKENS = r"AutumnError::|\bErr\(|StatusCode::(?:%s)\b" % "|".join(
    sorted(name for name, status in NAMED.items() if status >= 400)
)

# A `Json` extractor, bare or with a path such as `axum::Json`.
JSON = r"(?:[a-z_]+::)*Json"


def function_parts(source: str, name: str) -> tuple[str, str, str] | None:
    """The parameter list, return clause and block of a free function."""
    start = defined_functions(source).get(name)
    return None if start is None else parts_at(source, start)


def parts_at(source: str, start: int) -> tuple[str, str, str] | None:
    """The parameter list, return clause and block of the fn at `start`.

    The block starts at the first `{` after the balanced parameter list, so a
    brace in a parameter comment does not end the search early. The returned
    parameter list has its comments removed, so a comment that names an
    extractor is not read as one.
    """
    opener = source.find("(", start)
    params = balanced(source[opener:])
    brace = source.find("{", opener + len(params))
    if brace < 0:
        return None
    returns = source[opener + len(params) : brace]
    code = re.sub(r"//[^\n]*|/\*.*?\*/", "", params, flags=re.S)
    return code, returns, balanced(source[brace:], "{", "}")


def byte_parameters(params: str) -> set[str]:
    """Names of the parameters that carry the raw request body."""
    return set(BYTE_PARAMETER.findall(params))


def raw_body_parses(source: str, handler: str) -> list[tuple[str | None, bool]]:
    """`(type, guarded)` for each raw-body parse in a handler and its helpers.

    A helper counts only when the handler passes it a body variable, and only
    the helper parameters that receive the body are read as bodies. The type
    comes from a turbofish, then from a `let` binding in the same statement,
    then from a `Result<T, _>` return type. It is `None` when none of those
    names it, or when the call reads the body in a form the audit cannot read.
    """
    handler_found = handler_parts(source, handler)
    if handler_found is None:
        return []
    handler_block = handler_found[2]
    carriers = byte_parameters(handler_found[0])
    params, returns, block = handler_found
    parses = block_parses(block, byte_parameters(params), returns)
    for helper in called_helpers(source, handler_block):
        parts = function_parts(source, helper)
        if parts is None:
            continue
        params, returns, block = parts
        receivers = receiving_parameters(handler_block, helper, params, carriers)
        parses += block_parses(block, receivers & byte_parameters(params), returns)
    return parses


def split_top_level(text: str) -> list[str]:
    """The comma-separated items of a list, ignoring commas in nested brackets."""
    items, depth, current = [], 0, ""
    for char in text:
        depth += char in "([{<"
        depth -= char in ")]}>"
        if char == "," and depth == 0:
            items.append(current.strip())
            current = ""
        else:
            current += char
    if current.strip():
        items.append(current.strip())
    return items


def receiving_parameters(block: str, helper: str, params: str, variables: set[str]) -> set[str]:
    """The `helper` parameters that a call in the block passes a variable to.

    Each argument maps to the parameter at its position. A `self` receiver is
    skipped, since a call does not pass it in the argument list.
    """
    # A pattern such as `Extension(state): ..` keeps its slot with no name, so
    # the parameters after it keep their positions.
    names: list[str | None] = []
    for item in split_top_level(params[1:-1]):
        if re.fullmatch(r"&?\s*(?:'[a-z_]+\s+)?(?:mut\s+)?self", item):
            continue
        plain = re.match(r"(?:mut\s+)?([a-z_][a-z_0-9]*)\s*:", item)
        names.append(plain.group(1) if plain else None)
    receivers: set[str] = set()
    for call in re.finditer(r"\b%s\s*\(" % re.escape(helper), block):
        arguments = split_top_level(balanced(block[call.end() - 1 :])[1:-1].replace("->", ""))
        for index, argument in enumerate(arguments):
            mentions = any(re.search(r"\b%s\b" % re.escape(v), argument) for v in variables)
            if mentions and index < len(names) and names[index]:
                receivers.add(names[index])
    return receivers


def block_parses(block: str, carriers: set[str], returns: str) -> list[tuple[str | None, bool]]:
    """`(type, guarded)` for each `from_slice` call that reads a carrier."""
    parses: list[tuple[str | None, bool]] = []
    for hit in FROM_SLICE.finditer(block):
        turbofish = None
        opener = hit.end() - 1
        if block[opener] == "<":
            turbofish = balanced(block[opener:], "<", ">")[1:-1].strip()
            opener = block.find("(", opener + len(turbofish) + 2)
        call = balanced(block[opener:])
        argument = call[1:-1].strip().rstrip(",").strip()
        root = re.match(r"&?\s*([a-z_][a-z_0-9]*)", argument)
        if root is None or root.group(1) not in carriers:
            continue
        before = block[: hit.start()]
        after = block[opener + len(call) :]
        guarded = guards(block, hit.start(), root.group(1)) or discards_error(before, after)
        # A body read through an index, a method or a generic type is not
        # something the audit can type, so it is reported.
        if re.sub(r"^&\s*", "", argument) != root.group(1):
            parses.append((None, guarded))
        elif turbofish is not None and not re.fullmatch(r"[A-Za-z0-9_:]+", turbofish):
            parses.append((None, guarded))
        else:
            parses.append((parse_type(turbofish, before, after, returns), guarded))
    return parses


def guards(block: str, position: int, variable: str) -> bool:
    """Whether an `if` on `<variable>.is_empty()` lets an empty body skip the parse.

    The test counts when the parse is in the arm that runs for a non-empty
    body. That is the `else` of `if body.is_empty()`, or the condition or
    block of `if !body.is_empty()`. A plain `if body.is_empty() { .. }` before
    the parse also counts when its block returns `Ok(..)` and no error. Any
    other use, such as a log field, is no guard.
    """
    pattern = r"\bif\s+(!\s*)?%s\.is_empty\(\)" % re.escape(variable)
    for test in re.finditer(pattern, block[:position]):
        opener = block.find("{", test.end())
        if opener < 0:
            continue
        taken = balanced(block[opener:], "{", "}")
        taken_end = end = opener + len(taken)
        while re.match(r"\s*else\b", block[end:]):
            branch = block.find("{", end)
            end = branch + len(balanced(block[branch:], "{", "}"))
        if test.group(1) and test.end() < position < taken_end:
            return True
        if not test.group(1) and taken_end < position < end:
            return True
        early_return = re.search(r"\breturn\s+Ok\(", taken)
        if not test.group(1) and early_return and not re.search(ERROR_TOKENS, taken):
            return True
    return False


def rejects_result_body(params: str, block: str) -> bool:
    """Whether a `Result<Json<T>, _>` body is mandatory, since its error rejects.

    The body is mandatory when the handler applies `?` or `.map_err(..)` to it,
    when the `Err` arm of a `match` on it builds an error, or when the `else`
    of a `let Ok(..) = body else` builds an error or returns. An `Err` arm that
    hands the request on, for example to replay a committed key, leaves it
    optional.
    """
    found = re.search(r"\b([a-z_][a-z_0-9]*)\s*:\s*Result<\s*%s<" % JSON, params)
    if found is None:
        return False
    variable = re.escape(found.group(1))
    if re.search(r"\b%s\s*(?:\?|\.map_err\s*\()" % variable, block):
        return True
    for match in re.finditer(r"\bmatch\s+%s\s*\{" % variable, block):
        arms = balanced(block[match.end() - 1 :], "{", "}")
        failure = re.search(r"\bErr\s*\(", arms)
        if failure and re.search(ERROR_TOKENS, match_arm(arms, failure.start())):
            return True
    for binding in re.finditer(r"\blet\s+Ok\s*\(.*?\)\s*=\s*%s\s+else\s*\{" % variable, block):
        otherwise = balanced(block[binding.end() - 1 :], "{", "}")
        if re.search(r"\breturn\b|" + ERROR_TOKENS, otherwise):
            return True
    return False


def match_arm(arms: str, start: int) -> str:
    """The text of the `match` arm whose pattern starts at `start`.

    The arm runs from its `=>` to the end of its block, or to the next comma
    at the top level of the arm list.
    """
    arrow = arms.find("=>", start)
    if arrow < 0:
        return ""
    rest = arms[arrow + 2 :]
    if rest.lstrip().startswith("{"):
        return balanced(rest[rest.index("{") :], "{", "}")
    depth = 0
    for index, char in enumerate(rest):
        depth += char in "([{"
        depth -= char in ")]}"
        if depth < 0 or (char == "," and depth == 0):
            return rest[:index]
    return rest


def discards_error(before: str, after: str) -> bool:
    """Whether a parse turns its error into a value, so an empty body still runs.

    `.ok()`, `.unwrap_or_default()`, `.unwrap_or(..)` and `.unwrap_or_else(..)`
    after the call do so. So does `if let Ok(..) =` before it, unless its
    `else` returns or builds an error. A `match` that handles `Err` is not
    read, so it counts as mandatory.
    """
    if re.match(r"\s*\.(?:ok|unwrap_or_default|unwrap_or|unwrap_or_else)\s*\(", after):
        return True
    tested = r"\b(?:if|while|&&)\s+let\s+Ok\s*\([^()]*\)\s*=\s*(?:serde_json::)?$"
    if re.search(tested, before) is None:
        return False
    opener = after.find("{")
    if opener < 0:
        return True
    rest = after[opener + len(balanced(after[opener:], "{", "}")) :]
    if not re.match(r"\s*else\b", rest):
        return True
    otherwise = balanced(rest[rest.index("{") :], "{", "}")
    return not re.search(r"\breturn\b|" + ERROR_TOKENS, otherwise)


def ends_expression(after: str) -> bool:
    """Whether the text after a `from_slice(..)` call leaves its type unchanged.

    `?`, `.map_err(..)`, `.unwrap()`, `.unwrap_or_default()` and `.expect(..)`
    keep the parsed type. The expression must then end, or open a `match` block.
    """
    rest = after
    while True:
        rest = rest.lstrip()
        if rest.startswith("?"):
            rest = rest[1:]
            continue
        suffix = re.match(r"\.(?:map_err|expect|unwrap|unwrap_or_default)\s*\(", rest)
        if suffix is None:
            break
        opener = suffix.end() - 1
        rest = rest[opener + len(balanced(rest[opener:])) :]
    return rest[:1] in (";", "}", "{")


def parse_type(turbofish: str | None, before: str, after: str, returns: str) -> str | None:
    """The struct a `from_slice` call yields, or `None` when it is unnamed.

    A typed `let` or a `Result<T, _>` return type names the call only when the
    call ends its expression, since a `.map(..)` after it yields another type.
    """
    if turbofish:
        return turbofish.split("::")[-1]
    if not ends_expression(after):
        return None
    statement = before[before.rfind(";") + 1 :]
    binding = re.search(r"\blet\s+(?:mut\s+)?[a-z_0-9]+\s*:\s*([A-Za-z0-9_:]+)\s*=", statement)
    if binding:
        return binding.group(1).split("::")[-1]
    if re.search(r"\blet\b", statement):
        return None
    result = re.search(r"->\s*Result<\s*([A-Za-z0-9_:]+)\s*,", returns)
    return result.group(1).split("::")[-1] if result else None


def struct_fields(struct: str) -> list[tuple[str, str, bool, tuple[str, ...]]]:
    """`(name, type, mandatory, spellings)` for each field serde reads.

    `name` is the wire name, after a serde `rename = ".."`. `spellings` holds
    that name, then each serde `alias`. The text before the first `{` holds the
    container attributes. A container `#[serde(default)]` makes every field
    optional.
    """
    opener = struct.index("{")
    container = without_comment_lines(struct[:opener])
    all_default = re.search(r"serde\([^)]*\bdefault\b", container) is not None
    fields: list[tuple[str, str, bool, tuple[str, ...]]] = []
    attributes: list[str] = []
    # An attribute that rustfmt splits over several lines stays open until its
    # brackets balance.
    open_attribute = ""
    for line in struct[opener:].split("\n"):
        text = re.sub(r"/\*.*?\*/", "", line).strip()
        if open_attribute or text.startswith("#["):
            open_attribute += " " + text
            if open_attribute.count("[") <= open_attribute.count("]"):
                attributes.append(open_attribute.strip())
                open_attribute = ""
            continue
        text = re.sub(r"//.*$", "", text).strip()
        if not text or text in ("{", "}"):
            continue
        field = re.match(r"(?:pub(?:\([^)]*\))?\s+)?(?:r#)?([a-z_0-9]+)\s*:\s*(.+?),?$", text)
        if not field:
            attributes = []
            continue
        name, declared_type = field.group(1), field.group(2)
        joined = " ".join(attributes)
        if not re.search(r"serde\([^)]*\bskip(?:_deserializing)?\b", joined):
            defaulted = all_default or re.search(r"serde\([^)]*\bdefault\b", joined)
            optional = defaulted or declared_type.startswith("Option<")
            renamed = re.search(r'serde\([^)]*\brename\s*=\s*"([^"]+)"', joined)
            wire = renamed.group(1) if renamed else name
            aliases = re.findall(r'\balias\s*=\s*"([^"]+)"', joined)
            fields.append((wire, declared_type, not optional, (wire, *aliases)))
        attributes = []
    return fields


def wire_type(declared_type: str) -> str | None:
    """The OpenAPI type of a Rust field type, unwrapping one `Option`."""
    inner = re.fullmatch(r"Option<\s*(.+?)\s*>", declared_type)
    return WIRE_TYPES.get(inner.group(1) if inner else declared_type)


def query_struct_findings(
    method: str, path: str, route: dict, queries: list[tuple[str, str]]
) -> list[str]:
    """Check 6: the `Query<T>` structs and the route's query parameters agree."""
    documented = {
        entry.get("name"): entry
        for entry in route.get("params") or []
        if entry.get("in") == "query" and entry.get("name")
    }
    where = "  %s %s: `%%s`" % (method, path)
    found: list[str] = []
    accepted: set[str] = set()
    for name, struct in queries:
        for field, declared_type, mandatory, spellings in struct_fields(struct):
            accepted |= set(spellings)
            entry = documented.get(documented_as(spellings, documented))
            if entry is None:
                found.append(
                    where % field
                    + " is accepted by %s but the contract does not document it" % name
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
                    where % field + " is mandatory in %s but the contract marks it optional" % name
                )
            if not mandatory and entry.get("required") is True:
                found.append(
                    where % field + " is optional in %s but the contract marks it required" % name
                )
    owners = " or ".join(name for name, _ in queries)
    for key in documented.keys() - accepted:
        found.append(where % key + " is documented but %s does not accept it" % owners)
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
    missing = "  %s %s: cannot find struct %s"
    unread = "  %s %s: cannot read `%s` in %s"
    for method, path, handler in routes:
        params = handler_parameters(source, handler)
        route = by_route.get((method, path))
        if params is None or route is None:
            continue
        queries: list[tuple[str, str]] = []
        for query in QUERY_EXTRACTOR.finditer(params):
            name = query.group(1).split("::")[-1]
            struct = find_struct(name)
            if struct is None:
                unresolved.append(missing % (method, path, name))
            elif unreadable_serde(struct):
                unresolved += [unread % (method, path, a, name) for a in unreadable_serde(struct)]
            else:
                queries.append((name, struct))
        if len(re.findall(r"\bQuery<", params)) > len(QUERY_EXTRACTOR.findall(params)):
            unresolved.append("  %s %s: cannot read a `Query<..>` extractor" % (method, path))
        if queries:
            typed_query += query_struct_findings(method, path, route, queries)

        # A bare `Json<T>` means the body is mandatory. `Result<Json<T>, _>` is
        # mandatory when its error rejects. `Option<Json<T>>` is optional. All
        # three still name the struct whose fields serde accepts, which is what
        # check 3 needs.
        bare = re.search(
            r"%s\(\s*[a-z_0-9]+\s*\)\s*:\s*%s<\s*([A-Za-z0-9_:]+)\s*>" % (JSON, JSON), params
        )
        extractor = (
            bare
            or re.search(r"Result<\s*%s<\s*([A-Za-z0-9_:]+)\s*>" % JSON, params)
            or re.search(r"Option<\s*%s<\s*([A-Za-z0-9_:]+)\s*>\s*>" % JSON, params)
        )
        if len(re.findall(r"\bJson<", params)) > (extractor is not None):
            unresolved.append("  %s %s: cannot read a `Json<..>` extractor" % (method, path))
        # (struct name, whether the body is mandatory) for each parse.
        parses: list[tuple[str, bool]] = []
        body_type = extractor.group(1).split("::")[-1] if extractor else None
        result_rejects = rejects_result_body(params, handler_body(source, handler) or "")
        if body_type is not None and body_type != "Value":
            parses.append((body_type, bool(bare) or result_rejects))
        mandatory_body = bool(bare) or result_rejects
        if byte_parameters(params):
            for name, guarded in raw_body_parses(source, handler):
                # An unguarded parse makes the body mandatory, whatever its type.
                mandatory_body |= not guarded
                if name is None:
                    unresolved.append(
                        "  %s %s: cannot read a `from_slice` call or resolve its "
                        "body type" % (method, path)
                    )
                elif name != "Value":
                    parses.append((name, not guarded))

        request_body = route.get("request_body") or {}
        if mandatory_body and request_body.get("required") is not True:
            required_findings.append(
                "  %s %s: the body is mandatory in the handler but the contract "
                "does not mark it required" % (method, path)
            )
        declared = {
            field.get("name"): field.get("required", False)
            for field in request_body.get("fields", []) or []
        }
        for name, mandatory in parses:
            struct = find_struct(name)
            if struct is None:
                unresolved.append(missing % (method, path, name))
                continue
            if unreadable_serde(struct):
                unresolved += [unread % (method, path, a, name) for a in unreadable_serde(struct)]
                continue
            for spellings in mandatory_fields(struct) if mandatory else []:
                if declared.get(documented_as(spellings, declared)) is not True:
                    body_findings.append(
                        "  %s %s: `%s` is mandatory in %s but the contract does not "
                        "mark it required" % (method, path, spellings[0], name)
                    )
            # A free-form body is documented by prose, so its fields are not
            # checked. An empty field list on any other body is checked.
            if not request_body.get("free_form"):
                for spellings in accepted_fields(struct):
                    if documented_as(spellings, declared) is None:
                        undocumented.append(
                            "  %s %s: `%s` is accepted by %s but the contract does "
                            "not document it" % (method, path, spellings[0], name)
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
        "Unresolved types",
        "Name each body type in the source, for example `from_slice::<T>(..)`. "
        "Keep each struct in `autumn-harvest` or `autumn-harvest-plugin`, and "
        "not generic.",
    ),
    (
        "query_params",
        "Typed query mismatches",
        "Give each `Query<T>` field one entry in the route's `params`, with "
        "the same name, type and required flag. Add a field type with no "
        "OpenAPI type to `WIRE_TYPES`.",
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
FIXTURE_SOURCE = r"""
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
"""


def fixture_struct(source: str):
    """A `find_struct` that reads structs from the fixture source only."""

    def find(name: str) -> str | None:
        found = re.search(r"\bstruct %s\s*\{" % re.escape(name), source)
        return struct_text(source, found) if found else None

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
FIXTURE_BYTES = r"""
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
        .route("/lost", post(lost))
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
    let value = serde_json::from_slice::<Value>(&body).map_err(reject)?;
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

async fn lost(Query(query): Query<LostQuery>, body: Bytes) -> Response {
    let widget = serde_json::from_slice::<LostBody>(&body).map_err(reject)?;
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
"""

WIDGET_BODY = body_of(("name", True), ("size", False))
OPTIONAL_WIDGET = body_of(("name", True), ("size", False), required=False)


def query_param(name: str, kind: str, required: bool) -> dict:
    return {"name": name, "in": "query", "type": kind, "required": required}


SEARCH_PARAMS = [
    query_param("term", "string", True),
    query_param("exact", "boolean", False),
    query_param("limit", "integer", False),
]


# Status literals, overrides, generic helpers, hand-parsed queries, and two
# body shapes the audit must not misread.
FIXTURE_STATUS = r"""
pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/s/literal", delete(s_literal))
        .route("/s/override", post(s_override))
        .route("/s/bare", get(s_bare))
        .route("/s/generic", get(s_generic))
        .route("/s/list", get(s_list))
        .route("/s/scoped", post(s_scoped))
        .route("/s/lenient", patch(s_lenient))
}

async fn s_literal(Path(id): Path<String>) -> Response {
    if state == StatusCode::GONE {
        return StatusCode::NOT_FOUND.into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn s_override() -> Response {
    AutumnError::bad_request_msg("busy").with_status(StatusCode::CONFLICT).into_response()
}

async fn s_bare(Path(id): Path<String>) -> Result<Response, AutumnError> {
    let thing = load(&id).map_err(AutumnError::not_found_msg)?;
    Ok(StatusCode::OK.into_response())
}

fn map_error(error: HarvestError) -> Response {
    StatusCode::SERVICE_UNAVAILABLE.into_response()
}

async fn s_generic() -> Response {
    map_error(run())
}

async fn s_list(RawQuery(raw): RawQuery) -> Response {
    for (key, value) in pairs {
        match key.as_str() {
            "limit" | "page_size" => {}
            "order" => match value.as_str() {
                "asc" => {}
                _ => {}
            },
            _ => {}
        }
    }
    StatusCode::OK.into_response()
}

async fn s_scoped(body: Bytes) -> Response {
    let limit: u32 = 10;
    let widget = serde_json::from_slice(&body).map(consume);
    StatusCode::OK.into_response()
}

async fn s_lenient(body: Result<Json<Thing>, JsonRejection>) -> Response {
    StatusCode::OK.into_response()
}

struct Thing {
    name: String,
}
"""


# Shapes that once slipped past the audit or failed it on correct code.
FIXTURE_EDGES = r"""
pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/e/wrapped", post(e_wrapped))
        .route("/e/sliced", post(e_sliced))
        .route("/e/vector", post(e_vector))
        .route("/e/crate-bytes", post(e_crate_bytes))
        .route("/e/rejects", post(e_rejects))
        .route("/e/logged", post(e_logged))
        .route("/e/cursor", post(e_cursor))
        .route("/e/optional-query", get(e_optional_query))
        .route("/e/plain-query", get(e_plain_query))
        .route("/e/two-queries", get(e_two_queries))
        .route("/e/commented", get(e_commented))
        .route("/e/defaulted", get(e_defaulted))
        .route("/e/shapes", get(e_shapes))
        .route("/e/json-path", post(e_json_path))
        .route("/e/json-list", post(e_json_list))
        .route("/e/negated-log", post(e_negated_log))
        .route("/e/sized", post(e_sized))
        .route("/e/helper-reject", post(e_helper_reject))
        .route("/e/mapped", post(e_mapped))
        .route("/e/collide", post(collide))
        .route("/e/braced", post(e_braced))
        .route("/e/negated-if", post(e_negated_if))
        .route("/e/renamed", get(e_renamed))
        .route("/e/aliased", get(e_aliased))
        .route("/e/renamed-all", get(e_renamed_all))
        .route("/e/flattened", post(e_flattened))
        .route("/e/tolerant-ok", post(e_tolerant_ok))
        .route("/e/tolerant-default", post(e_tolerant_default))
        .route("/e/tolerant-if-let", post(e_tolerant_if_let))
        .route("/e/wrapped-attribute", get(e_wrapped_attribute))
        .route("/e/empty-arm", post(e_empty_arm))
        .route("/e/negated-else", post(e_negated_else))
        .route("/e/two-carriers", post(e_two_carriers))
        .route("/e/if-let-reject", post(e_if_let_reject))
        .route("/e/forwarded", post(e_forwarded))
        .route("/e/result-reject", post(e_result_reject))
        .route("/e/result-replay", post(e_result_replay))
        .route("/e/split-container", get(e_split_container))
        .route("/e/split-default", get(e_split_default))
        .route("/e/shadowed", post(e_shadowed))
        .route("/e/empty-fields", post(e_empty_fields))
        .route("/e/err-first", post(e_err_first))
        .route("/e/let-else", post(e_let_else))
        .route("/e/err-succeeds", post(e_err_succeeds))
        .route("/e/documented", get(e_documented))
}

async fn e_wrapped(body: Bytes) -> Response {
    let widget = serde_json::from_slice::<Gadget>(
        &body,
    );
    StatusCode::OK.into_response()
}

async fn e_sliced(body: Bytes) -> Response {
    let widget: Gadget = serde_json::from_slice(&body[..]).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_vector(body: Bytes) -> Response {
    let widgets = serde_json::from_slice::<Vec<Gadget>>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_crate_bytes(body: bytes::Bytes) -> Response {
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_rejects(body: Bytes) -> Response {
    if body.is_empty() {
        return AutumnError::bad_request_msg("a body is required").into_response();
    }
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_logged(body: Bytes) -> Response {
    tracing::debug!(empty = body.is_empty(), "parsing");
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

fn decode_cursor(raw: &[u8]) -> Option<Cursor> {
    serde_json::from_slice::<Cursor>(raw).ok()
}

async fn e_cursor(body: Bytes) -> Response {
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    let cursor = decode_cursor(STORED);
    StatusCode::OK.into_response()
}

async fn e_optional_query(query: Option<Query<Filter>>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_plain_query(filter: axum::extract::Query<Filter>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_two_queries(Query(mut a): Query<Filter>, Query(b): Query<Paging>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_commented(Query(query): Query<Commented>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_defaulted(Query(query): Query<Defaulted>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_shapes(Query(query): Query<Shapes>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_json_path(axum::Json(body): axum::Json<crate::model::Gadget>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_json_list(Json(body): Json<Vec<Gadget>>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_negated_log(body: Bytes) -> Response {
    tracing::debug!(has_body = !body.is_empty(), "parsing");
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_sized(body: Bytes) -> Response {
    let size = if body.is_empty() { 0 } else { body.len() };
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_helper_reject(body: Bytes) -> Response {
    if body.is_empty() {
        return missing_body();
    }
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_mapped(body: Bytes) -> Response {
    let name: Value = serde_json::from_slice(&body).map(|gadget: Gadget| gadget.name);
    StatusCode::OK.into_response()
}

impl Other {
    fn collide(&self) -> u32 {
        7
    }
}

async fn collide(body: Bytes) -> Response {
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_braced(
    Path(id): Path<String>,
    // A comment with a brace, Json<T> and Query<T>: {"reason": "..."}.
    body: Bytes,
) -> Response {
    StatusCode::GONE.into_response()
}

async fn e_negated_if(body: Bytes) -> Response {
    if !body.is_empty() {
        tracing::debug!("a body arrived");
    }
    let widget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_renamed(Query(query): Query<Renamed>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_aliased(Query(query): Query<Aliased>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_renamed_all(Query(query): Query<RenamedAll>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_flattened(Json(body): Json<Flattened>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_tolerant_ok(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).ok();
    StatusCode::OK.into_response()
}

async fn e_tolerant_default(body: Bytes) -> Response {
    let gadget: Gadget = serde_json::from_slice(&body).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_tolerant_if_let(body: Bytes) -> Response {
    if let Ok(gadget) = serde_json::from_slice::<Gadget>(&body) {
        tracing::debug!(name = %gadget.name, "parsed");
    }
    StatusCode::OK.into_response()
}

async fn e_empty_arm(body: Bytes) -> Response {
    if body.is_empty() {
        let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    }
    StatusCode::OK.into_response()
}

async fn e_negated_else(body: Bytes) -> Response {
    if !body.is_empty() {
        tracing::debug!("a body arrived");
    } else {
        let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    }
    StatusCode::OK.into_response()
}

async fn forward(
    Extension(state): Extension<State>,
    Path((id, name)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_forwarded(Extension(state): Extension<State>, body: Bytes) -> Response {
    forward(Extension(state), Path((ID.into(), NAME.into())), body).await
}

fn decode_both(body: &[u8], stored: &[u8]) -> Result<Gadget, Response> {
    let cursor = serde_json::from_slice::<Cursor>(stored).map_err(reject)?;
    serde_json::from_slice::<Gadget>(body).map_err(reject)
}

async fn e_two_carriers(body: Bytes) -> Response {
    let gadget = decode_both(&body, STORED);
    StatusCode::OK.into_response()
}

async fn e_if_let_reject(body: Bytes) -> Response {
    if let Ok(gadget) = serde_json::from_slice::<Gadget>(&body) {
        tracing::debug!(name = %gadget.name, "parsed");
    } else {
        return Err(reject());
    }
    StatusCode::OK.into_response()
}

async fn e_result_reject(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(rejection) => {
            return AutumnError::bad_request_msg(rejection.body_text()).into_response();
        }
    };
    StatusCode::OK.into_response()
}

async fn e_result_replay(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(rejection) => return replay_committed(rejection).await,
    };
    StatusCode::OK.into_response()
}

async fn e_split_container(Query(query): Query<SplitContainer>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_split_default(Query(query): Query<SplitDefault>) -> Response {
    StatusCode::OK.into_response()
}

#[derive(Debug, Deserialize)]
#[serde(
    default,
    rename_all = "camelCase",
)]
struct SplitContainer {
    page_size: u32,
}

#[derive(Debug, Default, Deserialize)]
#[serde(
    default,
)]
struct SplitDefault {
    kind: String,
}

impl Codec {
    fn parse_gadget(&self, raw: &[u8]) -> u32 {
        7
    }
}

fn parse_gadget(body: &[u8]) -> Result<Gadget, Response> {
    serde_json::from_slice::<Gadget>(body).map_err(reject)
}

async fn e_shadowed(body: Bytes) -> Response {
    let gadget = parse_gadget(&body);
    StatusCode::OK.into_response()
}

async fn e_empty_fields(body: Option<Json<Gadget>>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_err_first(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Err(rejection) => return replay_committed(rejection).await,
        Ok(Json(gadget)) => {
            if gadget.name.is_empty() {
                return AutumnError::bad_request_msg("a name is required").into_response();
            }
            gadget
        }
    };
    StatusCode::OK.into_response()
}

async fn e_let_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Ok(Json(gadget)) = body else {
        return AutumnError::bad_request_msg("a body is required").into_response();
    };
    StatusCode::OK.into_response()
}

async fn e_err_succeeds(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(_) => return StatusCode::NO_CONTENT.into_response(),
    };
    StatusCode::OK.into_response()
}

async fn e_documented(Query(query): Query<Documented>) -> Response {
    StatusCode::OK.into_response()
}

/// Other types use `#[serde(default)]` and `#[serde(flatten)]`. This one does not.
struct Documented {
    kind: String,
}

async fn e_wrapped_attribute(Query(query): Query<WrappedAttribute>) -> Response {
    StatusCode::OK.into_response()
}

struct WrappedAttribute {
    #[serde(
        rename = "wire_name",
        alias = "old_name",
    )]
    rust_name: Option<String>,
}

struct Renamed {
    #[serde(rename = "wire_name")]
    rust_name: Option<String>,
}

struct Aliased {
    #[serde(default, alias = "kind")]
    task_type: Option<String>,
}

#[serde(rename_all = "camelCase")]
struct RenamedAll {
    page_size: Option<u32>,
}

struct Flattened {
    name: String,
    #[serde(flatten)]
    extra: Gadget,
}

struct Gadget {
    name: String,
}

struct Cursor {
    offset: u64,
}

struct Filter {
    kind: Option<String>,
}

struct Paging {
    page: Option<u32>,
}

struct Commented {
    kind: Option<String>, // A trailing note.
    /* A leading note. */ page: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Defaulted {
    kind: String,
}

struct Shapes {
    r#type: Option<String>,
    pub(in crate::api) page: Option<u32>,
    #[serde(skip_deserializing)]
    cache: Option<String>,
    #[serde(deserialize_with = "default_kind")]
    kind: String,
}
"""

GADGET_BODY = body_of(("name", True))


# (name, source, routes, expected findings by check). Each expected string must
# match exactly one finding. The check must report nothing else.
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
        {"statuses": ["GET /things/{id} returns 400 via AutumnError::bad_request_msg, undeclared"]},
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
            fixture_route("POST", "/raw/value", 200, request_body=body_of()),
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
            fixture_route("POST", "/raw/helper", 200, request_body=body_of(("name", True))),
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
        "an unguarded Value body is marked required",
        FIXTURE_BYTES,
        [fixture_route("POST", "/raw/value", 200, request_body=body_of(required=False))],
        {"body_required": ["POST /raw/value: the body is mandatory"]},
    ),
    (
        "a raw-byte body of unknown type is reported",
        FIXTURE_BYTES,
        [fixture_route("POST", "/raw/untyped", 200, request_body=WIDGET_BODY)],
        {"unresolved": ["POST /raw/untyped: cannot read a `from_slice` call"]},
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
        {
            "query_params": [
                "GET /search: `exact` is boolean in SearchQuery but the contract says string"
            ]
        },
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
                "GET /search: `term` is mandatory in SearchQuery",
                "GET /search: `limit` is optional in SearchQuery",
            ]
        },
    ),
    (
        "a documented query key the struct ignores is reported",
        FIXTURE_BYTES,
        [
            fixture_route(
                "GET",
                "/search",
                200,
                params=SEARCH_PARAMS + [query_param("page", "integer", False)],
            )
        ],
        {"query_params": ["GET /search: `page` is documented but SearchQuery does not accept it"]},
    ),
    (
        "a query field with no wire type is reported",
        FIXTURE_BYTES,
        [fixture_route("GET", "/tagged", 200, params=[query_param("tags", "string", True)])],
        {
            "query_params": [
                "GET /tagged: `tags` has type Vec<String>, which maps to no OpenAPI type"
            ]
        },
    ),
    (
        "a struct the audit cannot find is reported",
        FIXTURE_BYTES,
        [fixture_route("POST", "/lost", 200, request_body=body_of(required=True))],
        {
            "unresolved": [
                "POST /lost: cannot find struct LostQuery",
                "POST /lost: cannot find struct LostBody",
            ]
        },
    ),
    (
        "an undeclared StatusCode literal is reported, a comparison is not",
        FIXTURE_STATUS,
        [fixture_route("DELETE", "/s/literal", 204)],
        {"statuses": ["DELETE /s/literal returns 404, undeclared"]},
    ),
    (
        "a with_status override carries its own status",
        FIXTURE_STATUS,
        [fixture_route("POST", "/s/override", 200)],
        {"statuses": ["POST /s/override returns 409, undeclared"]},
    ),
    (
        "a bare AutumnError fn ref is reported",
        FIXTURE_STATUS,
        [fixture_route("GET", "/s/bare", 200)],
        {
            "statuses": [
                "GET /s/bare returns 404 via AutumnError::not_found_msg (bare fn ref), undeclared"
            ]
        },
    ),
    (
        "a generic helper contributes no status",
        FIXTURE_STATUS,
        [fixture_route("GET", "/s/generic", 200)],
        {},
    ),
    (
        "an undocumented RawQuery key is reported, an alias is not",
        FIXTURE_STATUS,
        [fixture_route("GET", "/s/list", 200, params=[{"name": "page_size", "in": "query"}])],
        {"query_keys": ["GET /s/list: `order` is accepted by the query parser"]},
    ),
    (
        "a typed let in an earlier statement does not type a parse",
        FIXTURE_STATUS,
        [fixture_route("POST", "/s/scoped", 200, request_body=body_of(required=True))],
        {"unresolved": ["POST /s/scoped: cannot read a `from_slice` call"]},
    ),
    (
        "a Result<Json<T>> body is not mandatory",
        FIXTURE_STATUS,
        [
            fixture_route(
                "PATCH", "/s/lenient", 200, request_body=body_of(("name", False), required=False)
            )
        ],
        {},
    ),
    (
        "a wrapped from_slice call is read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/wrapped", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/wrapped: the body is mandatory"]},
    ),
    (
        "a from_slice call the audit cannot read is reported",
        FIXTURE_EDGES,
        [
            fixture_route("POST", "/e/sliced", 200, request_body=GADGET_BODY),
            fixture_route("POST", "/e/vector", 200, request_body=GADGET_BODY),
        ],
        {
            "unresolved": [
                "POST /e/sliced: cannot read a `from_slice` call",
                "POST /e/vector: cannot read a `from_slice` call",
            ]
        },
    ),
    (
        "a Bytes type from another path is a body",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/crate-bytes", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/crate-bytes: the body is mandatory"]},
    ),
    (
        "an is_empty test that rejects or only logs is no guard",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/rejects",
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            ),
            fixture_route(
                "POST", "/e/logged", 200, request_body=body_of(("name", True), required=False)
            ),
        ],
        {
            "body_required": [
                "POST /e/rejects: the body is mandatory",
                "POST /e/logged: the body is mandatory",
            ]
        },
    ),
    (
        "a helper that does not get the body is not a body parse",
        FIXTURE_EDGES,
        [fixture_route("POST", "/e/cursor", 200, request_body=GADGET_BODY)],
        {},
    ),
    (
        "every Query extractor form is read",
        FIXTURE_EDGES,
        [
            fixture_route("GET", "/e/optional-query", 200, params=[]),
            fixture_route("GET", "/e/plain-query", 200, params=[]),
        ],
        {
            "query_params": [
                "GET /e/optional-query: `kind` is accepted by Filter",
                "GET /e/plain-query: `kind` is accepted by Filter",
            ]
        },
    ),
    (
        "two Query extractors on one route are both read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET",
                "/e/two-queries",
                200,
                params=[
                    query_param("kind", "string", False),
                    query_param("page", "integer", False),
                ],
            ),
        ],
        {},
    ),
    (
        "a comment beside a field is not part of it",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET",
                "/e/commented",
                200,
                params=[
                    query_param("kind", "string", False),
                    query_param("page", "integer", False),
                ],
            ),
        ],
        {},
    ),
    (
        "a container serde default makes every field optional",
        FIXTURE_EDGES,
        [fixture_route("GET", "/e/defaulted", 200, params=[query_param("kind", "string", False)])],
        {},
    ),
    (
        "raw names, scoped pub, skip_deserializing and default-named fns are read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET",
                "/e/shapes",
                200,
                params=[
                    query_param("type", "string", False),
                    query_param("page", "integer", False),
                    query_param("kind", "string", True),
                ],
            ),
        ],
        {},
    ),
    (
        "a path-qualified Json extractor is read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/json-path", 200, request_body=body_of(("name", False), required=False)
            )
        ],
        {
            "body_required": ["POST /e/json-path: the body is mandatory"],
            "mandatory": ["POST /e/json-path: `name` is mandatory in Gadget"],
        },
    ),
    (
        "a Json extractor the audit cannot read is reported",
        FIXTURE_EDGES,
        [fixture_route("POST", "/e/json-list", 200, request_body=body_of())],
        {"unresolved": ["POST /e/json-list: cannot read a `Json<..>` extractor"]},
    ),
    (
        "an is_empty test that does not skip the parse is no guard",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/negated-log", "/e/sized", "/e/helper-reject")
        ],
        {
            "body_required": [
                "POST /e/negated-log: the body is mandatory",
                "POST /e/sized: the body is mandatory",
                "POST /e/helper-reject: the body is mandatory",
            ]
        },
    ),
    (
        "a typed let over a transformed parse does not type it",
        FIXTURE_EDGES,
        [fixture_route("POST", "/e/mapped", 200, request_body=body_of())],
        {"unresolved": ["POST /e/mapped: cannot read a `from_slice` call"]},
    ),
    (
        "a handler is found past an earlier fn of the same name",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/collide", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/collide: the body is mandatory"]},
    ),
    (
        "a brace in a parameter comment does not hide the handler body",
        FIXTURE_EDGES,
        [fixture_route("POST", "/e/braced", 200)],
        {"statuses": ["POST /e/braced returns 410, undeclared"]},
    ),
    (
        "a negated is_empty test that does not wrap the parse is no guard",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/negated-if", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/negated-if: the body is mandatory"]},
    ),
    (
        "a serde rename sets the wire name",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET", "/e/renamed", 200, params=[query_param("rust_name", "string", False)]
            )
        ],
        {
            "query_params": [
                "GET /e/renamed: `wire_name` is accepted by Renamed",
                "GET /e/renamed: `rust_name` is documented but Renamed does not accept it",
            ]
        },
    ),
    (
        "a serde alias may be the documented name",
        FIXTURE_EDGES,
        [fixture_route("GET", "/e/aliased", 200, params=[query_param("kind", "string", False)])],
        {},
    ),
    (
        "rename_all and flatten are reported, not guessed",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET", "/e/renamed-all", 200, params=[query_param("page_size", "integer", False)]
            ),
            fixture_route("POST", "/e/flattened", 200, request_body=body_of(("name", True))),
        ],
        {
            "unresolved": [
                "GET /e/renamed-all: cannot read `rename_all` in RenamedAll",
                "POST /e/flattened: cannot read `flatten` in Flattened",
            ]
        },
    ),
    (
        "a parse that discards its error leaves the body optional",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", False), required=False))
            for path in ("/e/tolerant-ok", "/e/tolerant-default", "/e/tolerant-if-let")
        ],
        {},
    ),
    (
        "a serde attribute over several lines is read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET",
                "/e/wrapped-attribute",
                200,
                params=[query_param("rust_name", "string", False)],
            )
        ],
        {
            "query_params": [
                "GET /e/wrapped-attribute: `wire_name` is accepted by WrappedAttribute",
                "GET /e/wrapped-attribute: `rust_name` is documented but WrappedAttribute",
            ]
        },
    ),
    (
        "a parse in the arm that runs for an empty body is not guarded",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/empty-arm", "/e/negated-else")
        ],
        {
            "body_required": [
                "POST /e/empty-arm: the body is mandatory",
                "POST /e/negated-else: the body is mandatory",
            ]
        },
    ),
    (
        "only the helper parameter that gets the body is a body",
        FIXTURE_EDGES,
        [fixture_route("POST", "/e/two-carriers", 200, request_body=GADGET_BODY)],
        {},
    ),
    (
        "a pattern parameter keeps the positions of the ones after it",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/forwarded", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/forwarded: the body is mandatory"]},
    ),
    (
        "an if let Ok parse whose else rejects is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/if-let-reject",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/if-let-reject: the body is mandatory"]},
    ),
    (
        "a Result<Json<T>> body whose Err arm rejects is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/result-reject",
                200,
                request_body=body_of(("name", False), required=False),
                error_responses=[{"status": 400}],
            ),
            fixture_route(
                "POST",
                "/e/result-replay",
                200,
                request_body=body_of(("name", False), required=False),
            ),
        ],
        {
            "body_required": ["POST /e/result-reject: the body is mandatory"],
            "mandatory": ["POST /e/result-reject: `name` is mandatory in Gadget"],
        },
    ),
    (
        "a container attribute over several lines is read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET", "/e/split-container", 200, params=[query_param("page_size", "integer", True)]
            ),
            fixture_route(
                "GET", "/e/split-default", 200, params=[query_param("kind", "string", False)]
            ),
        ],
        {"unresolved": ["GET /e/split-container: cannot read `rename_all` in SplitContainer"]},
    ),
    (
        "a free helper is found past an earlier method of the same name",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/shadowed", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/shadowed: the body is mandatory"]},
    ),
    (
        "an empty field list is checked unless the body is free-form",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/empty-fields",
                200,
                request_body={"required": False, "free_form": False, "fields": []},
            )
        ],
        {"undocumented": ["POST /e/empty-fields: `name` is accepted by Gadget"]},
    ),
    (
        "a free-form body skips the field checks",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/empty-fields",
                200,
                request_body={"required": False, "free_form": True, "fields": []},
            )
        ],
        {},
    ),
    (
        "only the Err arm decides whether a Result<Json<T>> body is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/err-first",
                200,
                request_body=body_of(("name", False), required=False),
                error_responses=[{"status": 400}],
            )
        ],
        {},
    ),
    (
        "a let-else that rejects a Result<Json<T>> body makes it mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/let-else",
                200,
                request_body=body_of(("name", False), required=False),
                error_responses=[{"status": 400}],
            )
        ],
        {
            "body_required": ["POST /e/let-else: the body is mandatory"],
            "mandatory": ["POST /e/let-else: `name` is mandatory in Gadget"],
        },
    ),
    (
        "an Err arm that returns a success status does not reject",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/err-succeeds",
                200,
                request_body=body_of(("name", False), required=False),
                additional_responses=[{"status": 204}],
            )
        ],
        {},
    ),
    (
        "a doc comment that names a serde attribute is not one",
        FIXTURE_EDGES,
        [fixture_route("GET", "/e/documented", 200, params=[query_param("kind", "string", False)])],
        {"query_params": ["GET /e/documented: `kind` is mandatory in Documented"]},
    ),
    (
        "a malformed contract entry does not crash the audit",
        FIXTURE_EDGES,
        [
            fixture_route("GET", "/e/defaulted", 200, params=None),
            fixture_route("GET", "/e/shapes", 200, params=[{"in": "query"}]),
        ],
        {
            "query_params": [
                "GET /e/defaulted: `kind` is accepted by Defaulted",
                "GET /e/shapes: `type` is accepted by Shapes",
                "GET /e/shapes: `page` is accepted by Shapes",
                "GET /e/shapes: `kind` is accepted by Shapes",
            ]
        },
    ),
]


def self_test() -> int:
    """Run each fixture through `audit` and compare the findings."""
    failures = 0
    for name, source, routes, expected in SELF_TESTS:
        problems: list[str] = []
        try:
            found = audit(source, {"routes": routes}, fixture_struct(source))
        except Exception as error:  # noqa: BLE001 - report it as a failure
            print("FAIL  %s\n      raised %r" % (name, error))
            failures += 1
            continue
        # A route the fixture router lacks is skipped, so its test would pass
        # without a check.
        known = {(method, path) for method, path, _ in router_routes(source)}
        for route in routes:
            if (route["method"], route["path"]) not in known:
                problems.append(
                    "%s %s: not in the fixture router" % (route["method"], route["path"])
                )
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
