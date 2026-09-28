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
generated client, so an ordinary request cannot be typed. Checks 2 and 3 skip
only a body the contract marks `free_form`. A present `Option<Json<T>>` body is
parsed strictly, so check 2 reads its mandatory fields. An empty field list on
any other body is checked.

A handler can also take the raw `Bytes` and call `serde_json::from_slice`
itself. An import alias, such as `use serde_json::from_slice as decode;`, is
read too. Every scan reads the source after `resolve_aliases`. It reads each
`use .. as ..` and `type` alias of `Query`, `Json`, `Bytes` or `from_slice`
in its scope, and reports a type alias it cannot read. Checks 2, 3 and 5 read that parse when it reads a parameter of type
`Bytes`, `&[u8]` or `Vec<u8>`. The parse can be in the handler, or in a helper
that the handler passes the body to, at any depth up to `HELPER_DEPTH`. A
recursive helper is read once per chain of calls. In a helper, only the
parameter at the position of the body argument is a body. A move into another
name, such as `let captured = body;`, is followed. A copy through a call, such
as `body.to_vec()`, is not read. The type comes from a turbofish, then from a
typed `let` in the same statement, then from a `Result<T, _>` return type. The
last two apply only when the call ends its expression, since a `.map(..)` after
it yields another type. A `Value` body is free-form, so checks 2 and 3 skip it.
Check 5 still reads it.

Check 5 treats a bare `Json<T>` as mandatory, since axum rejects a request
without it. A `Result<Json<T>, _>` is mandatory when the handler rejects its
error, through `?`, `.map_err(..)?`, or an `Err` arm that builds an error or
returns the rejection it binds. An `Err` arm that returns a helper, or whose
value is a call to a helper that can build a rejection, rejects, unless it hands
its error to that helper. A move of the extractor into another name is followed.
Such a helper can recover the request, as the start route does (#808). A guard
on an arm is not evaluated, so a rejecting guarded arm counts. A catch-all `_`
arm counts as an `Err` arm, and so does an `if let Err(..)` block or the code
after `let Err(..) = body else { .. };`. An `if` on `.is_err()` or `.is_ok()`
counts when its failing branch rejects. A raw-byte parse is mandatory unless an
`if` on `.is_empty()` lets an empty body skip it. The parse must be in the arm
that runs for a non-empty body, and the empty-body arm must not reject. An
earlier `if body.is_empty() { .. }` also counts when its block returns a
success, such as `Ok(..)`, a 2xx status or `Json(..)`, and no error. Only a
return at the top level of that block counts. A return inside a nested `if`,
`match` or closure may not run. A parse result stored by `let parsed = ..;` is
read where it is used, and stays strict when it is unused or passed on. A parse
that turns its error into a value is optional too, such as `.ok()`,
`.unwrap_or_default()` or an `if let Ok(..)` whose `else` does not reject. A
`match` on the parse is optional when it has an `Err` or catch-all arm and no
such arm rejects. A fallback that rejects the error, such as
`.map_or_else(|e| reject(e), ..)`, keeps the parse mandatory. So does a fallback
that calls a helper that can build a rejection. The helper's return type
decides: a response, an error or a `Result` can be a rejection, and a plain
value type such as `Gadget` is not. A helper the audit cannot find counts as a
rejection. An `.or_else(..)` whose fallback yields `Ok(..)` on every path makes
a later `?` tolerant. A guard or a tolerant call at a helper call site carries
into the helper. Check 2 applies to every parse that does not tolerate its
error, since a body that is present must then carry the mandatory fields. A bare
`Json<T>` and a rejecting `Result<Json<T>, _>` are such parses.

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
parameters. An `Option<Query<T>>` makes every field optional, since an absent
query string yields `None`. A `Result<Query<T>, _>` does the same when the
handler does not reject its error, as check 5 reads it. `WIRE_TYPES` gives the OpenAPI type of a field after
the audit removes one `Option`. A field is optional when it is an `Option` or
has a serde default, on the field or on the struct. By default, serde ignores an
unknown query key, so a documented key that no struct has is a finding.

The audit reads the serde attributes `default`, `skip`, `skip_deserializing`,
`skip_serializing`, `skip_serializing_if`, `rename = ".."` and `alias = ".."`.
A field is documented when the contract names its wire name or any alias. Any
other serde attribute, on the struct or on a field, is a check 7 finding,
since the audit cannot read the wire layout it makes. A field
`deserialize_with` is read only when `KNOWN_DESERIALIZERS` models it. A new
custom deserializer must be added to that table, or its field is a finding.

Check 7 stops the audit from skipping what it cannot read. These are findings:
a `from_slice` call it cannot read, a body type it cannot resolve, a
`Json<..>` or `Query<..>` extractor it cannot read, and a struct it cannot
find. A generic struct such as `struct Q<'a>` is one it cannot find.

A helper called by bare name is found as a free function, so an earlier method
of the same name does not hide it. `Type::f` and `x.f` reach methods, and a
struct path such as `module::S` picks the struct in that module. A name the
audit cannot pin to one definition fails closed. A handler is found from its `async fn`
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

# Every `StatusCode` constant in the `http` crate, with its status. The table
# is complete, so an unusual status such as 418 is read, not skipped.
NAMED = {
    "CONTINUE": 100,
    "SWITCHING_PROTOCOLS": 101,
    "PROCESSING": 102,
    "EARLY_HINTS": 103,
    "OK": 200,
    "CREATED": 201,
    "ACCEPTED": 202,
    "NON_AUTHORITATIVE_INFORMATION": 203,
    "NO_CONTENT": 204,
    "RESET_CONTENT": 205,
    "PARTIAL_CONTENT": 206,
    "MULTI_STATUS": 207,
    "ALREADY_REPORTED": 208,
    "IM_USED": 226,
    "MULTIPLE_CHOICES": 300,
    "MOVED_PERMANENTLY": 301,
    "FOUND": 302,
    "SEE_OTHER": 303,
    "NOT_MODIFIED": 304,
    "USE_PROXY": 305,
    "TEMPORARY_REDIRECT": 307,
    "PERMANENT_REDIRECT": 308,
    "BAD_REQUEST": 400,
    "UNAUTHORIZED": 401,
    "PAYMENT_REQUIRED": 402,
    "FORBIDDEN": 403,
    "NOT_FOUND": 404,
    "METHOD_NOT_ALLOWED": 405,
    "NOT_ACCEPTABLE": 406,
    "PROXY_AUTHENTICATION_REQUIRED": 407,
    "REQUEST_TIMEOUT": 408,
    "CONFLICT": 409,
    "GONE": 410,
    "LENGTH_REQUIRED": 411,
    "PRECONDITION_FAILED": 412,
    "PAYLOAD_TOO_LARGE": 413,
    "URI_TOO_LONG": 414,
    "UNSUPPORTED_MEDIA_TYPE": 415,
    "RANGE_NOT_SATISFIABLE": 416,
    "EXPECTATION_FAILED": 417,
    "IM_A_TEAPOT": 418,
    "MISDIRECTED_REQUEST": 421,
    "UNPROCESSABLE_ENTITY": 422,
    "LOCKED": 423,
    "FAILED_DEPENDENCY": 424,
    "TOO_EARLY": 425,
    "UPGRADE_REQUIRED": 426,
    "PRECONDITION_REQUIRED": 428,
    "TOO_MANY_REQUESTS": 429,
    "REQUEST_HEADER_FIELDS_TOO_LARGE": 431,
    "UNAVAILABLE_FOR_LEGAL_REASONS": 451,
    "INTERNAL_SERVER_ERROR": 500,
    "NOT_IMPLEMENTED": 501,
    "BAD_GATEWAY": 502,
    "SERVICE_UNAVAILABLE": 503,
    "GATEWAY_TIMEOUT": 504,
    "HTTP_VERSION_NOT_SUPPORTED": 505,
    "VARIANT_ALSO_NEGOTIATES": 506,
    "INSUFFICIENT_STORAGE": 507,
    "LOOP_DETECTED": 508,
    "NOT_EXTENDED": 510,
    "NETWORK_AUTHENTICATION_REQUIRED": 511,
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
# guessed at.
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


# The crates the audit scans, by the name a path uses for them.
SCANNED_CRATES = frozenset({"autumn_harvest", "autumn_harvest_plugin"})

# The audited source's own crate and module. A fixture is the crate `crate`
# at its root. `audit` sets it.
LOCAL: list[str] = ["autumn_harvest_plugin", "api"]


def location(crate: str, *modules: str) -> str:
    """A definition's place: its crate, then its module path."""
    return "::".join(part for part in (crate, *modules) if part)


def path_segments(path: str) -> list[str]:
    """The names of a path, with the generic arguments of every segment cut.

    `Decoder::<Config>::` is `["Decoder"]`, and `a::B<T>::c` is `["a", "B",
    "c"]`. Brackets are matched by `split_expression`, so a `::` inside
    generic arguments does not split. Every struct, function and alias
    resolver reads a qualifier through this one step.
    """
    names = []
    for part in split_expression(path.strip(), "::", types=True):
        part = part.strip()
        if part and not part.startswith("<"):
            names.append(re.sub(r"\s*<.*$", "", part, flags=re.S).strip())
    return names


def qualified_match(qualifier: list[str], place: str, origin: str | None = None) -> bool:
    """Whether a path `qualifier`, written in module `origin`, reaches `place`.

    `place` and `origin` are a crate, then a module path, as `location`
    builds them. `origin` defaults to the audited source's own module. A
    qualifier that starts with `crate` names a module path from the root of
    the crate of `origin`. One that starts with a scanned crate's name names
    a path from that crate's root. `self` names `origin`, and each `super`
    one module up from it. Any other first name is relative, as in the 2018
    edition, so `a::b` reaches `origin::a::b` only. Every form must match
    `place` whole. A first name that is no module inside `origin` names an
    external crate, as `models::Filter` does with no import, so it matches
    nothing and its item is unresolved. An imported first name has already
    been expanded by `expanded_path`. `resolve_symbol` reads every qualifier
    through this step.
    """
    whole = place.split("::")
    base = (origin or location(LOCAL[0], LOCAL[1])).split("::")
    wanted = [part for part in qualifier if part]
    if wanted and wanted[0] == "crate":
        return whole == [base[0], *wanted[1:]]
    if wanted and wanted[0] in SCANNED_CRATES:
        return whole == wanted
    if wanted and wanted[0] in ("self", "super"):
        if wanted[0] == "self":
            wanted = wanted[1:]
        while wanted and wanted[0] == "super":
            if len(base) < 2:
                return False
            base, wanted = base[:-1], wanted[1:]
        return whole == base + wanted
    # Any other first name is relative, as in the 2018 edition: it must be a
    # module inside `origin`. Otherwise it names an external crate, which
    # matches nothing.
    return whole == base + wanted


@functools.lru_cache(maxsize=None)
def block_owners(source: str, keyword: str) -> list[tuple[int, int, str]]:
    """`(start, end, name)` for each `impl`, `trait` or `mod` block in `source`.

    For `impl`, the name is the type the block is for, as in `impl Trait for
    Type` or `impl<T> Type<T>`. For `trait` and `mod`, it is the declared name.
    """
    found = []
    for head in re.finditer(r"\b%s\b" % keyword, source):
        brace = source.find("{", head.end())
        stop = source.find(";", head.end())
        if brace < 0 or 0 <= stop < brace:
            continue
        header = source[head.end() : brace]
        if keyword == "impl":
            header = re.sub(r"^\s*<[^{]*?>", "", header)
            header = header.split(" for ", 1)[-1]
            header = re.sub(r"\bwhere\b.*", "", header, flags=re.S)
            if re.match(r"\s*&?\s*\[\s*u8\s*\]", header):
                end = brace + len(balanced(source[brace:], "{", "}"))
                found.append((brace, end, BYTE_SLICE))
                continue
        name = re.match(r"\s*&?\s*(?:dyn\s+)?(?:[A-Za-z_]\w*\s*::\s*)*([A-Za-z_]\w*)", header)
        if name:
            found.append((brace, brace + len(balanced(source[brace:], "{", "}")), name.group(1)))
    return found


# The owner name of an `impl` for `[u8]`, and the type a `[u8]` receiver has.
BYTE_SLICE = "[u8]"


def owner_at(owners: list[tuple[int, int, str]], position: int) -> str | None:
    """The innermost owner block that holds `position`, or `None`."""
    holding = [(start, name) for start, end, name in owners if start < position < end]
    return max(holding)[1] if holding else None


@functools.lru_cache(maxsize=None)
def function_index(source: str) -> dict[str, list[tuple[int, str | None, str, bool]]]:
    """Every definition of each function in `source`: `(start, owner, modules, pub)`.

    The owner is the type of the `impl` block or the name of the `trait` that
    holds the definition, or `None` for a free function. `modules` is the
    path of the inline `mod` blocks that hold it. `pub` tells whether the
    definition is public in any form. `resolve_symbol` reads this index for
    every fn lookup in the audit.
    """
    owners = block_owners(source, "impl") + block_owners(source, "trait")
    modules = block_owners(source, "mod")
    index: dict[str, list[tuple[int, str | None, str, bool]]] = {}
    for found in re.finditer(r"\b(?:async )?fn ([A-Za-z_][A-Za-z_0-9]*)\s*[(<]", source):
        # A declaration with no body, as in a trait, is no definition.
        opener = source.find("(", found.start())
        after = opener + len(balanced(source[opener:]))
        head = re.match(r"[^;{]*", source[after:])
        if source[after + head.end() : after + head.end() + 1] != "{":
            continue
        inline = [name for start, end, name in sorted(modules) if start < found.start() < end]
        public = re.search(PUBLIC_ITEM, source[max(0, found.start() - 80) : found.start()])
        entry = (found.start(), owner_at(owners, found.start()), "::".join(inline), bool(public))
        index.setdefault(found.group(1), []).append(entry)
    return index


# A `pub` item head that ends where its `fn` or `async fn` starts.
PUBLIC_ITEM = r"\bpub(?:\s*\([^)]*\))?\s+(?:(?:const|unsafe|extern\s+\"[^\"]*\")\s+)*$"

# A reference site: the file text a name is written in, and where. A `None`
# position is the file's own module.
Site = tuple[str, "int | None"]

# The place of each file text that a site can point into: its crate, then
# its module path. `struct_index` and `audit` fill it.
FILE_PLACES: dict[str, str] = {}

# The struct index that `resolve_symbol` reads. `audit` sets it. `None` means
# the structs of both crates.
STRUCTS: list[dict[str, list[tuple[str, str]]] | None] = [None]


class StructBlock(str):
    """A struct's attribute lines and block, with the site of its definition.

    Two structs can have the same block text in different modules, so the
    site travels with the block, not in a table keyed by its text.
    """

    def __new__(cls, text: str, site: Site) -> "StructBlock":
        block = super().__new__(cls, text)
        block.site = site
        return block

# Every module place that holds a known item, found once per audit.
KNOWN_MODULES: list[frozenset[str] | None] = [None]

# The roots of the standard library. No item under them runs serde_json.
STD_ROOTS = frozenset({"std", "core", "alloc"})


def site_origin(site: Site) -> str:
    """The module a site is in: its file's place, then its inline modules."""
    text, at = site
    base = FILE_PLACES.get(text, location(LOCAL[0], LOCAL[1]))
    if at is None:
        return base
    inline = [name for start, end, name in sorted(block_owners(text, "mod")) if start < at < end]
    return location(base, *inline)


@functools.lru_cache(maxsize=None)
def block_start(text: str, block: str) -> int:
    """Where `block` first starts in `text`, or -1."""
    return text.find(block)


def site_in(block: str, position: int = 0) -> Site:
    """The site of `position` in `block`, a slice of the audited source.

    A `Block` knows its start. Any other slice is found by its text.
    """
    start = getattr(block, "start", None)
    if start is None:
        start = block_start(SOURCE[0], block)
    return SOURCE[0], start + position if start >= 0 else None


def known_modules() -> frozenset[str]:
    """Every module place that holds a struct, a type alias or a function."""
    if KNOWN_MODULES[0] is None:
        places = {place for found in struct_candidates().values() for place, _ in found}
        places |= {alias[0] for found in TYPE_ALIASES[0].values() for alias in found}
        places |= {
            location(LOCAL[0], LOCAL[1], modules)
            for found in function_index(SOURCE[0]).values()
            for _, _, modules, _ in found
        }
        places |= {
            location(LOCAL[0], LOCAL[1], name) for _, _, name in block_owners(SOURCE[0], "mod")
        }
        prefixes = set()
        for place in places:
            parts = place.split("::")
            prefixes |= {"::".join(parts[: count + 1]) for count in range(len(parts))}
        KNOWN_MODULES[0] = frozenset(prefixes)
    return KNOWN_MODULES[0]


def struct_candidates() -> dict[str, list[tuple[str, str]]]:
    """The struct index that `resolve_symbol` reads."""
    return defined_structs() if STRUCTS[0] is None else STRUCTS[0]


def expanded_path(reference: str, site: Site) -> list[str] | None:
    """The names of `reference`, with an imported first name replaced by its path.

    The first name goes through the innermost `use` that binds it in scope at
    `site`, so both `Filter` and `schema::Filter` resolve through an import.
    The result is an absolute path, from `crate` or a scanned crate. `None`
    means the import names another crate. A name with no import is returned
    as written.
    """
    segments, used_at = written_path(reference, site)
    if used_at is None:
        return segments
    return absolute_path(segments, site_origin((site[0], used_at)))


def written_path(reference: str, site: Site) -> tuple[list[str], int | None]:
    """The names of `reference` with an imported first name replaced, and the import's place.

    The innermost `use` that binds the first name in scope at `site`
    decides, so `use crate::signed as axum;` makes `axum::Json` read as
    `crate::signed::Json`. A path from `::`, `crate`, `self`, `super` or
    `Self` has no import. The position is `None` when no import applies.
    Symbol lookups and the extractor path trust both expand through this.
    """
    segments = path_segments(reference)
    text, at = site
    if not segments or at is None or reference.strip().startswith("::"):
        return segments, None
    if segments[0] in ("crate", "self", "super", "Self"):
        return segments, None
    imported = innermost_use(text, segments[0], at)
    if imported is None:
        return segments, None
    path, used_at = imported
    return path_segments(path) + segments[1:], used_at


def absolute_path(segments: list[str], origin: str) -> list[str] | None:
    """An import path from `crate` or a scanned crate, or `None` for another crate.

    `self` and `super` count from `origin`, the module of the `use`. A first
    name that is a child module of `origin` is relative to it, as in a 2018
    edition `use`. Any other first name is another crate.
    """
    head = segments[0] if segments else ""
    if head == "crate" or head in SCANNED_CRATES:
        return segments
    base = origin.split("::")
    if head in ("self", "super"):
        rest = segments[1:] if head == "self" else segments
        while rest and rest[0] == "super":
            if len(base) < 2:
                return None
            base, rest = base[:-1], rest[1:]
        return ["crate", *base[1:], *rest]
    if origin + "::" + head in known_modules():
        return ["crate", *base[1:], *segments]
    return None


def innermost_use(text: str, name: str, at: int) -> tuple[str, int] | None:
    """The path and position of the innermost plain `use` that binds `name` at `at`."""
    found = None
    for (start, end), path, used_at in use_bindings(text).get(name, []):
        if start <= at < end and (found is None or start >= found[0]):
            found = (start, path, used_at)
    return found[1:] if found else None


@functools.lru_cache(maxsize=None)
def use_bindings(code: str) -> dict[str, list[tuple[tuple[int, int], str, int]]]:
    """`(scope, path, position)` for each plain, non-glob `use` leaf, by bound name."""
    found: dict[str, list[tuple[tuple[int, int], str, int]]] = {}
    for use in re.finditer(r"\buse\b[^;]*;", code):
        scope = alias_scope(code, use.start())
        for path, alias in use_leaves(use.group(0)):
            if not path.endswith("*"):
                binding = alias or path.rsplit("::", 1)[-1]
                found.setdefault(binding, []).append((scope, path, use.start()))
    return found


@functools.lru_cache(maxsize=None)
def glob_imports(code: str) -> list[tuple[tuple[int, int], str, int]]:
    """`(scope, module path, position)` for each glob `use` leaf in `code`.

    Both the extractor scan in `glob_unknowns` and `resolve_symbol` read
    globs through this one step.
    """
    found = []
    for use in re.finditer(r"\buse\b[^;]*;", code):
        scope = alias_scope(code, use.start())
        for path, _ in use_leaves(use.group(0)):
            if path.endswith("::*"):
                found.append((scope, path[:-3], use.start()))
    return found


def glob_candidates(found: list, site: Site, origin: str) -> list | None:
    """The candidates for a bare name that the glob imports at `site` leave.

    A definition in the site's own module wins over any glob, as in Rust.
    A trusted glob, one of `TRUSTED_GLOBS`, brings no request type. A glob
    of a scanned module, such as `super::*`, brings that module's items. Any
    other glob may bind the name, so the result is `None` and the caller
    fails closed. An empty list means no glob decides, so the name follows
    its bare rule. A name that no scanned item has is left to that rule
    too, since a glob can then bring only an item the audit cannot read.
    """
    text, at = site
    if at is None or not found:
        return []
    covering = [
        glob_module(text, path, used_at)
        for (start, end), path, used_at in glob_imports(text)
        if start <= at < end
    ]
    covering = [module for module in covering if module != TRUSTED]
    if not covering:
        return []
    own = [item for item in found if item[0] == origin]
    if own:
        return own
    if None in covering:
        return None
    return [item for item in found if item[0] in covering]


# What `glob_module` gives for a glob that brings nothing the audit reads.
TRUSTED = "trusted"


def glob_module(text: str, path: str, used_at: int) -> str | None:
    """What a glob `use path::*` at `used_at` brings: `TRUSTED`, a module place, or `None`.

    A glob of `TRUSTED_GLOBS` is `TRUSTED`. A glob of a module the audit
    scans, such as `super::*` or `crate::x::*`, is that module's place, so
    its items resolve through it. Any other glob is `None`: it may bring
    any name, so a bare name it covers fails closed. This is the one glob
    rule, for extractor names and for every kind of symbol.
    """
    if path in TRUSTED_GLOBS:
        return TRUSTED
    origin = site_origin((text, used_at))
    module = absolute_path(path_segments(path), origin)
    if module is None:
        return None
    place = "::".join([origin.split("::")[0] if module[0] == "crate" else module[0], *module[1:]])
    return place if place in known_modules() else None


def module_binds(code: str, place: str, name: str) -> bool:
    """Whether the module at `place` defines or imports `name`, as a glob of it brings.

    A struct or a type alias there counts. So does a local item or a `use`
    of that name inside an inline `mod` block of `code`.
    """
    if any(found == place for found, _ in struct_candidates().get(name, [])):
        return True
    if any(alias[0] == place for alias in TYPE_ALIASES[0].get(name, [])):
        return True
    for start, end, _ in block_owners(code, "mod"):
        if site_origin((code, start + 1)) != place:
            continue
        block = code[start:end]
        if re.search(r"\b(?:struct|enum|trait|union|type|fn)\s+%s\b" % re.escape(name), block):
            return True
        if any(scope[0] == start for scope, _, _ in use_bindings(code).get(name, [])):
            return True
    return False


def std_path(reference: str, site: Site) -> bool:
    """Whether `reference` names a standard library item, through any import."""
    segments = path_segments(reference)
    if not segments:
        return False
    text, at = site
    imported = innermost_use(text, segments[0], at) if at is not None else None
    head = path_segments(imported[0])[0] if imported else segments[0]
    return head in STD_ROOTS


def resolve_symbol(kind: str, reference: str, site: Site | None = None) -> list | None:
    """Every definition of `kind` that `reference`, written at `site`, reaches.

    `kind` is `"struct"`, `"fn"` or `"alias"`. This is the one resolver for
    all three. A struct is its block, a type alias its `TypeAlias` and a fn
    its start in the audited source. A fn reference `.name` is a method call,
    which reaches any method of that name.

    The first name goes through `expanded_path`, so an import decides first.
    An import from another crate reaches nothing. A qualified path keeps the
    definitions that `qualified_match` reaches from the site's module. For a
    fn, a type before the name, such as `Decoder::`, keeps that type's
    methods, and `Self::` keeps any method. A plain module path keeps free
    functions.

    `Self::` keeps the methods of the `impl` or `trait` block that holds the
    site. Outside such a block it keeps any method.

    A bare name with no import then goes through the glob imports in scope,
    as `glob_candidates` reads them. A glob that may bind the name makes it
    `None`, unless the site's own module defines it.

    A bare name with no import follows the visibility rule of its kind. A
    fn must be in the site's module or an ancestor, or be public. A struct
    may be anywhere. A type alias must be the only alias of that name, and be
    in the site's module or the crate root.

    `None` means the reference is ambiguous or not visible, so the caller
    fails closed. More than one struct or alias is `None` too. A caller of a
    fn treats more than one result as ambiguous where it needs one.
    """
    site = site or (SOURCE[0], None)
    origin = site_origin(site)
    if kind == "fn" and reference.strip().startswith("."):
        name = reference.strip()[1:].strip()
        return [start for start, owner, _, _ in function_index(SOURCE[0]).get(name, []) if owner]
    segments = expanded_path(reference, site)
    if not segments:
        return []
    bare = len(segments) == 1
    name, qualifier = segments[-1], segments[:-1]
    if kind == "struct":
        found = [(place, block, None, True) for place, block in struct_candidates().get(name, [])]
    elif kind == "alias":
        found = [(alias[0], alias, None, True) for alias in TYPE_ALIASES[0].get(name, [])]
    else:
        found = [
            (location(LOCAL[0], LOCAL[1], modules), start, owner, public)
            for start, owner, modules, public in function_index(SOURCE[0]).get(name, [])
        ]
        last = qualifier[-1] if qualifier else ""
        typed = last if last[:1].isupper() or last == BYTE_SLICE else None
        if typed:
            qualifier = qualifier[:-1]
        if typed == "Self":
            typed = enclosing_owner(site) or typed
        found = [item for item in found if (item[2] is None) == (typed is None)]
        found = [item for item in found if typed in (None, "Self", item[2])]
    globbed = glob_candidates(found, site, origin) if bare else False
    if globbed is None:
        return None
    if not bare:
        found = [item for item in found if qualified_match(qualifier, item[0], origin)]
    elif globbed:
        found = globbed
    elif kind == "fn":
        found = [
            item
            for item in found
            if item[3] or item[0] == origin or origin.startswith(item[0] + "::")
        ]
    elif kind == "alias" and len(found) == 1 and found[0][0] not in (origin, origin.split("::")[0]):
        return None
    if kind != "fn" and len(found) > 1:
        return None
    return [item[1] for item in found]


def called_helpers(source: str, body: str, params: str = "") -> list[int]:
    """Where each function in this file that `body` calls is defined.

    Every call is read: bare, path-qualified and method calls, each from its
    own site. A method call reaches what `method_targets` finds. A call that
    can reach more than one definition reads all of them, so no status a
    helper can return is dropped.
    """
    starts: set[int] = set()
    for call in re.finditer(FREE_CALL_NAME, body):
        if call.group(1) not in GENERIC_HELPERS | NOT_CALLS:
            starts.update(resolve_symbol("fn", call.group(1), site_in(body, call.start())) or [])
    for call in re.finditer(QUALIFIED_CALL, body):
        if call.group(2) in GENERIC_HELPERS:
            continue
        if call.group(1).strip() == ".":
            receiver = call_receiver(body, call.start())
            targets = method_targets(body, params, receiver, call.group(2), call.start())
            # A receiver of unknown type reads every method of that name. This
            # is deliberate: a status is never dropped for want of a type.
            if targets is None:
                targets = resolve_symbol("fn", "." + call.group(2)) or []
            starts.update(targets)
            continue
        reference = call.group(1) + call.group(2)
        starts.update(resolve_symbol("fn", reference, site_in(body, call.start())) or [])
    return sorted(starts)


def method_targets(
    block: str, params: str, receiver: str | None, method: str, at: int
) -> list[int] | None:
    """The methods that a call `receiver.method(..)` at `at` in `block` reaches, or `None`.

    `receiver_type` reads the receiver's written type from `block` or
    `params`, and the call reaches that type's methods only. A raw body as
    the receiver has its parameter's type, such as `Bytes`, `Vec` or `[u8]`,
    so it reaches only a method of an `impl` for that type. A `self`
    receiver reaches the methods of the enclosing `impl`. `None` means the
    receiver has no type the audit can know. The status scan, the handoff
    scan and the raw-body receiver scan all read method calls through this
    one step.
    """
    typed = receiver_type(block, params, receiver, at) if receiver else None
    if typed is None:
        return None
    return resolve_symbol("fn", typed + "::" + method, site_in(block, at)) or []


def call_receiver(block: str, dot: int) -> str | None:
    """The variable a method call at `dot` reads whole as its receiver, or `None`.

    The receiver expression, as `receiver_expression` finds it, goes through
    `argument_root`, the normalizer that argument handoffs use. So `body`,
    `(body)`, `(&body)` and `(*body).as_ref()` all name `body`.
    """
    root, whole = argument_root(receiver_expression(block, dot))
    return root if whole else None


def receiver_expression(block: str, dot: int) -> str:
    """The receiver expression of a method call at `dot`, as `receiver_start` bounds it."""
    return block[receiver_start(block, dot) : dot].strip()


def receiver_start(block: str, dot: int) -> int:
    """Where the postfix expression that ends at the `.` of a method call at `dot` starts.

    It reads back over names, grouped `( .. )` and indexed `[ .. ]` parts, a
    call's own name with its turbofish, `?`, and the `.` or `::` that join
    them. So the expression is `(&body)` in `(&body).decode()`, and
    `body.as_ref()` in `body.as_ref().decode()`. A prefix operator outside
    the group, as in `&body.decode()`, applies to the call, so it is not
    read.
    """

    def skip(index: int) -> int:
        while index > 0 and block[index - 1].isspace():
            index -= 1
        return index

    index = skip(dot)
    while index > 0:
        char = block[index - 1]
        if char in ")]":
            opener = matching_opener(block, index - 1)
            if opener is None:
                break
            index = opener
            # A call's own name, with its turbofish, belongs to the call.
            if char == ")" and block[index - 1 : index] == ">":
                depth = 0
                for back in range(index - 1, -1, -1):
                    depth += block[back] == ">"
                    depth -= block[back] == "<"
                    if depth == 0:
                        index = back - 2 if block[back - 2 : back] == "::" else back
                        break
            name = re.search(r"[A-Za-z_]\w*!?$", block[max(0, index - 64) : index])
            if name:
                index -= len(name.group(0))
        elif char.isalnum() or char == "_":
            while index > 0 and (block[index - 1].isalnum() or block[index - 1] == "_"):
                index -= 1
        elif char == "?":
            index -= 1
            continue
        else:
            break
        joint = skip(index)
        if joint > 1 and block[joint - 2 : joint] == "::":
            index = skip(joint - 2)
            continue
        if joint > 0 and block[joint - 1] == "." and block[joint - 2 : joint] != "..":
            index = skip(joint - 1)
            continue
        break
    return index


def matching_opener(text: str, closer: int) -> int | None:
    """Where the `(` or `[` that the closer at `closer` matches starts, or `None`."""
    pairs = {")": "(", "]": "["}
    opener, close, depth = pairs[text[closer]], text[closer], 0
    for index in range(closer, -1, -1):
        if text[index] == close:
            depth += 1
        elif text[index] == opener:
            depth -= 1
            if depth == 0:
                return index
    return None


def enclosing_owner(site: Site) -> str | None:
    """The type of the `impl`, or the `trait`, that holds a site in the audited source."""
    text, at = site
    if at is None or text != SOURCE[0]:
        return None
    return owner_at(block_owners(text, "impl") + block_owners(text, "trait"), at)


def handler_parameters(source: str, name: str) -> str | None:
    """The parameter list of `async fn <name>(..)`."""
    parts = handler_parts(source, name)
    return parts[0] if parts else None


def struct_body(reference: str, site: Site | None = None) -> str | None:
    """The block of the one struct `reference` names at `site`, or `None`."""
    found = resolve_symbol("struct", reference, site)
    return found[0] if found else None


def struct_index(
    source: str, module: str = "", crate: str = "crate"
) -> dict[str, list[tuple[str, str]]]:
    """Every struct in `source`: `(place, block)` by bare name.

    The place is the crate, then the module path, as `location` builds it.
    `module` is the path of the file itself. An inline `mod name { .. }` adds
    its name to the path of each struct inside it. Each block keeps its site,
    so a field type resolves from the struct's own module and imports.
    """
    code = code_only(source)
    FILE_PLACES[code] = location(crate, module)
    modules = block_owners(code, "mod")
    index: dict[str, list[tuple[str, str]]] = {}
    for found in re.finditer(r"\bstruct ([A-Za-z_][A-Za-z_0-9]*)\s*\{", code):
        inline = [name for start, end, name in sorted(modules) if start < found.start() < end]
        place = location(crate, module, *inline)
        block = StructBlock(struct_text(source, found), (code, found.start()))
        index.setdefault(found.group(1), []).append((place, block))
    return index


@functools.lru_cache(maxsize=None)
def defined_structs() -> dict[str, list[tuple[str, str]]]:
    """Every struct in either crate, as `struct_index` reads it, by bare name.

    A file's module path comes from its place under `src`, so `src/a/b.rs`
    and `src/a/b/mod.rs` are `a::b`, and `lib.rs` and `main.rs` are the root.
    """
    index: dict[str, list[tuple[str, str]]] = {}
    for crate in CRATES:
        root = ROOT / crate / "src"
        for path in sorted(root.rglob("*.rs")):
            text, place = path.read_text(), module_path(root, path)
            for name, found in struct_index(text, place, crate.replace("-", "_")).items():
                index.setdefault(name, []).extend(found)
    return index


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


# Serde container attributes that keep a struct's wire layout: an object with
# one key per field. Any other container attribute is reported.
LAYOUT_KEEPING = frozenset({"default", "deny_unknown_fields", "rename", "crate", "bound", "expecting"})


# A string literal in an attribute, with its escapes.
QUOTED = re.compile(r'"(?:\\.|[^"\\])*"')

# One serde item: `(key, form, value)`. `form` is `=`, `(` or `None`, and
# `value` is the literal after `=` without its quotes, or `None`.
SerdeItem = tuple[str, "str | None", "str | None"]


def serde_items(attributes: str) -> list[SerdeItem]:
    """Every item of every `#[serde(..)]` attribute in `attributes`.

    Each attribute is read bracket-balanced, so one that rustfmt splits over
    several lines is read whole. Its items are split at top-level commas by
    `split_expression`. Quoted values are set aside first, so a key word
    inside a value, as in `alias = "skip"`, is never read as a key. Only keys
    are compared. Values are literals.
    """
    items: list[SerdeItem] = []
    for found in re.finditer(r"#\s*\[\s*serde\s*\(", attributes):
        inner = balanced(attributes[found.end() - 1 :])[1:-1]
        values: list[str] = []

        def park(literal: re.Match) -> str:
            values.append(literal.group(0)[1:-1])
            return '"%d"' % (len(values) - 1)

        for item in split_expression(QUOTED.sub(park, inner), ","):
            key = re.match(r'\s*([a-z_]+)\s*(\(|=)?\s*(?:"(\d+)")?', item)
            if key is None:
                continue
            value = values[int(key.group(3))] if key.group(3) is not None else None
            items.append((key.group(1), key.group(2), value))
    return items


def struct_layout(
    struct: str,
) -> tuple[list[SerdeItem], list[tuple[str, str, list[SerdeItem], str | None]]]:
    """The container serde items, and each field's name, type, items and alias.

    Comments are removed first. An attribute or a type that rustfmt splits
    over several lines stays open until its brackets balance. Each field type
    is read through `resolve_field_type`, and the last value is the alias it
    could not read, or `None`. Every serde reader in the audit reads a struct
    through this one parse.
    """
    original = struct
    struct = code_only(struct, literals=False)
    opener = struct.index("{")
    container = serde_items(canonical_paths(without_comment_lines(struct[:opener])))
    fields: list[tuple[str, str, list[SerdeItem], str | None]] = []
    attributes: list[str] = []
    open_attribute = open_field = ""
    for line in struct[opener:].split("\n"):
        text = re.sub(r"/\*.*?\*/", "", line).strip()
        if open_attribute or text.startswith("#["):
            open_attribute += " " + text
            if open_attribute.count("[") <= open_attribute.count("]"):
                attributes.append(open_attribute.strip())
                open_attribute = ""
            continue
        text = re.sub(r"//.*$", "", text).strip()
        if open_field:
            text, open_field = open_field + " " + text, ""
        if not text or text in ("{", "}"):
            continue
        if text.count("<") + text.count("(") > text.count(">") + text.count(")"):
            open_field = text
            continue
        text = re.sub(r"\s*,\s*([>)])", r"\1", re.sub(r"([<(])\s+", r"\1", text))
        field = re.match(r"(?:pub(?:\([^)]*\))?\s+)?(?:r#)?([a-z_0-9]+)\s*:\s*(.+?),?$", text)
        if field:
            site = getattr(original, "site", (SOURCE[0], None))
            resolved, unread = canonical_type(field.group(2), site)
            items = serde_items(canonical_paths(" ".join(attributes)))
            fields.append((field.group(1), resolved, items, unread))
        attributes = []
    return container, fields


def unreadable_serde(struct: str) -> list[str]:
    """Serde attributes in a struct that change its layout in ways not read.

    A container attribute passes only when `LAYOUT_KEEPING` names it, and a
    container `rename` passes only in its `rename = ".."` form. Any other,
    such as `transparent`, `untagged`, `tag`, `from` or `rename_all`, is
    reported, so the audit fails closed. Field attributes are read by
    `unreadable_field_serde`.
    """
    container, fields = struct_layout(struct)
    found: set[str] = set()
    for key, form, _ in container:
        if key not in LAYOUT_KEEPING or key == "rename" and form != "=":
            found.add(key + "(..)" if form == "(" else key)
    for _, declared_type, items, unread in fields:
        found |= unreadable_field_serde(items, declared_type)
        if unread:
            found.add(unread)
    return sorted(found)


# Serde field attributes that keep a field's wire type. `skip` and
# `skip_deserializing` take the field off the wire, and `struct_fields` reads
# that.
FIELD_KEEPING = frozenset(
    {"rename", "alias", "default", "skip_serializing_if", "skip_serializing", "skip", "skip_deserializing"}
)

# Field deserializers the audit models, with the declared type each accepts.
# `deserialize_tristate` reads a `T` or `null` into `Some(..)`, and an absent
# field falls back to `None` through `default`. So the wire type is the inner
# `T`, as for an `Option<T>`. A new custom deserializer must be added here, or
# its field is a check 7 finding.
KNOWN_DESERIALIZERS = {
    "deserialize_tristate": r"Option<\s*Option<.+>\s*>",
}


def unreadable_field_serde(items: list[SerdeItem], declared_type: str) -> set[str]:
    """The serde attributes of one field that the audit cannot read.

    A key passes when `FIELD_KEEPING` names it, and `rename` only in its
    `rename = ".."` form. `deserialize_with` passes only for a deserializer in
    `KNOWN_DESERIALIZERS`, when the field has the type that entry accepts and
    the audited file defines the function with an `Option<Option<T>>` result.
    """
    found: set[str] = set()
    for name, form, value in items:
        if name == "deserialize_with" and modeled_deserializer(value, declared_type):
            continue
        if name in FIELD_KEEPING and not (name == "rename" and form != "="):
            continue
        found.add(name + "(..)" if form == "(" else name)
    return found


def modeled_deserializer(function: str | None, declared_type: str) -> bool:
    """Whether `KNOWN_DESERIALIZERS` models `function` for a field of this type."""
    shape = KNOWN_DESERIALIZERS.get(function or "")
    if shape is None or not re.fullmatch(shape, declared_type.strip()):
        return False
    defined = r"\bfn\s+%s\s*<[^(]*>\s*\([^)]*\)\s*->\s*Result<\s*Option<\s*Option<" % re.escape(function)
    return re.search(defined, SOURCE[0]) is not None


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
# unlisted type. It does not guess one. A type from outside the standard
# library is listed only by its trusted path, as `TRUSTED_TYPE_PATHS` holds.
WIRE_TYPES = {
    "String": "string",
    "uuid::Uuid": "string",
    "bool": "boolean",
    "f32": "number",
    "f64": "number",
    **{kind: "integer" for kind in ("i8 i16 i32 i64 i128 isize u8 u16 u32 u64 u128 usize".split())},
}

# A typed query extractor in any form, naming its struct. A `Query<` that this
# does not match is reported, not skipped.
QUERY_EXTRACTOR = re.compile(r"(?<![\w:])Query<\s*([A-Za-z0-9_:]+)\s*>")

# A parameter that carries the raw request body.
BYTE_PARAMETER = re.compile(
    r"\b([a-z_][a-z_0-9]*)\s*:\s*(?:&\s*(?:'[a-z_]+\s+)?(?:mut\s+)?)?"
    r"(?:(?<![\w:])Bytes\b|\[u8\]|(?<![\w:])Vec<u8>)"
)


# A `serde_json::from_slice` call, up to its turbofish or its argument list.
# `resolve_aliases` rewrites every import of it and every alias to this path
# first, so this one pattern reads them all. `Uuid::from_slice` and a local
# `from_slice` are no body parse.
FROM_SLICE_CALL = re.compile(r"\bserde_json::from_slice\s*(?:::<|\()")


@functools.lru_cache(maxsize=None)
def enclosing_block(block: str, position: int) -> tuple[int, int]:
    """`(start, end)` of the innermost `{ .. }` in `block` that holds `position`."""
    depth = 0
    for index in range(position - 1, -1, -1):
        depth += block[index] == "}"
        depth -= block[index] == "{"
        if depth < 0:
            return index, index + len(balanced(block[index:], "{", "}"))
    return 0, len(block)


# The `StatusCode` names for a 2xx status.
SUCCESS_NAMES = "|".join(sorted(name for name, status in NAMED.items() if 200 <= status < 300))

# The start of a returned value that is a success: `Ok(..)`, a 2xx `StatusCode`,
# a `Json(..)` body, which axum sends as 200, or a tuple that starts with a 2xx
# `StatusCode`.
SUCCESS_EXIT = r"Ok\s*\(|\(?\s*StatusCode::(?:%s)\b|(?:[a-z_]+::)*Json\s*\(" % SUCCESS_NAMES

# A token that marks a block as an error path: an `AutumnError`, an `Err(..)`,
# or a `StatusCode::` name for a status outside 2xx. A 2xx status is no error.
ERROR_TOKENS = r"AutumnError::|\bErr\(|StatusCode::(?!(?:%s)\b)[A-Z_]+\b" % SUCCESS_NAMES

# A call to a free function, with its name. A type path such as
# `Gadget::default()` starts with a capital, so it does not match.
FREE_CALL = r"(?:return\s+)?([a-z_][a-z_0-9]*)\s*\("

# The `ref` and `mut` markers a pattern binding can carry, as in `ref failed`.
BINDING = r"(?:ref\s+)?(?:mut\s+)?"

# A return type that can carry a rejection rather than a plain value. A type
# name matches whole or by suffix, so `ApiError` matches and `ResponseConfig`
# does not. A bare `Json<T>` is always sent as 200, so it is a success.
RESPONSE_TYPE = r"\w*(?:Response|Rejection|Error)\b|\b(?:StatusCode|Result)\b|\("

# The source that `audit` reads, so a helper can be looked up by name.
SOURCE = [""]

# One type alias: `(module path, parameters, target)`.
TypeAlias = tuple[str, list[str], str]

# The type aliases the audited structs can use, by bare name. `audit` sets it,
# from the fixture source or from both crates.
TYPE_ALIASES: list[dict[str, list[TypeAlias]]] = [{}]


def type_aliases_in(
    source: str, module: str = "", crate: str = "crate"
) -> dict[str, list[TypeAlias]]:
    """Each `type Name<P, ..> = Target;` in `source`, by name, with its module.

    `module` is the path of the file itself. An inline `mod name { .. }` adds
    its name, as `struct_index` does for structs. Only a module-level alias is
    read: one whose every enclosing block is a `mod`. An alias inside a fn or
    another block is local to it, and the extractor alias layer reads it in
    that scope. A target is kept as written. `canonical_type` cuts its
    standard paths after the alias pass, as for every other type text.
    """
    code = code_only(source)
    modules = block_owners(code, "mod")
    found: dict[str, list[TypeAlias]] = {}
    for alias in re.finditer(r"\btype\s+([A-Z]\w*)\s*(?:<([^=;]*)>)?\s*=\s*([^;]+);", code):
        inline = [name for start, end, name in sorted(modules) if start < alias.start() < end]
        head = code[: alias.start()]
        if head.count("{") - head.count("}") != len(inline):
            continue
        place = location(crate, module, *inline)
        parameters = [part.strip() for part in split_expression(alias.group(2) or "", ",", types=True)]
        target = alias.group(3).strip()
        found.setdefault(alias.group(1), []).append((place, parameters, target))
    return found


@functools.lru_cache(maxsize=None)
def crate_type_aliases() -> dict[str, list[TypeAlias]]:
    """The type aliases of both crates, with each file's module path."""
    found: dict[str, list[TypeAlias]] = {}
    for crate in CRATES:
        root = ROOT / crate / "src"
        for path in sorted(root.rglob("*.rs")):
            text, place = path.read_text(), module_path(root, path)
            for name, aliases in type_aliases_in(text, place, crate.replace("-", "_")).items():
                found.setdefault(name, []).extend(aliases)
    return found


def module_path(root, path) -> str:
    """The module path of a file under `src`: `a/b.rs` and `a/b/mod.rs` are `a::b`."""
    parts = list(path.relative_to(root).with_suffix("").parts)
    if parts[-1] in ("mod", "lib", "main"):
        parts = parts[:-1]
    return "::".join(parts)


def resolve_field_type(declared_type: str, site: Site | None = None) -> tuple[str, str | None]:
    """A field type with its type aliases resolved, and an alias it cannot read.

    A no-parameter alias is replaced by its target. An alias whose target is
    the same type applied to the same parameters, such as `type Maybe<T> =
    Option<T>`, is a rename. `resolve_symbol` finds the alias from the
    struct's `site`, so an import there decides first. The passes repeat up
    to `ALIAS_PASSES`. Any other alias, an ambiguous one, or a chain that does
    not settle, is returned by name, so the field is unreadable. A field
    spelled through an alias is never read as a required non-`Option` by
    mistake.
    """
    for _ in range(ALIAS_PASSES):
        head = re.fullmatch(r"\s*((?:[A-Za-z_]\w*\s*::\s*)*)([A-Z]\w*)\s*(?:<(.*)>)?\s*", declared_type)
        if head is None:
            return declared_type, None
        found = resolve_symbol("alias", head.group(1) + head.group(2), site)
        if found is None:
            return declared_type, head.group(2)
        if not found:
            return declared_type, None
        _, parameters, target = found[0]
        arguments = [part.strip() for part in split_expression(head.group(3) or "", ",", types=True)]
        if not parameters and not arguments:
            declared_type = target
            continue
        renamed = re.fullmatch(r"\s*([A-Za-z_][\w:]*)\s*<(.*)>\s*", target)
        inner = split_expression(renamed.group(2), ",", types=True) if renamed else []
        if renamed and [part.strip() for part in inner] == parameters and len(arguments) == len(parameters):
            declared_type = "%s<%s>" % (renamed.group(1), ", ".join(arguments))
            continue
        return declared_type, head.group(2)
    head = re.match(r"\s*(?:[A-Za-z_]\w*\s*::\s*)*([A-Za-z_]\w*)", declared_type)
    return declared_type, head.group(1) if head else declared_type

# A `Json` extractor, bare or with a path such as `axum::Json`.
JSON = r"(?<![\w:])Json"

# The prelude `Result` and `Option`, unqualified. `canonical_paths` cuts the
# standard paths first. A qualified name is the shadowing item that
# `alias_declarations` binds, so it is not read as the prelude type.
RESULT = r"(?<![\w:])Result"
OPTION = r"(?<![\w:])Option"


# A fn's parameter list, return clause and block, as `parts_at` reads them.
Parts = tuple[str, str, str]


def function_parts(
    source: str, reference: str, site: Site | None = None
) -> tuple[str, str, str] | None:
    """The parameter list, return clause and block of the function a call reaches.

    `resolve_symbol` reads the call `reference` from its `site`. `None` means
    the audit cannot find the function, or cannot tell which of several it
    is, so the caller fails closed.
    """
    starts = resolve_symbol("fn", reference, site) or []
    return parts_at(source, starts[0]) if len(starts) == 1 else None


def parts_at(source: str, start: int) -> tuple[str, str, str] | None:
    """The parameter list, return clause and block of the fn at `start`.

    The block starts at the first `{` after the balanced parameter list, so a
    brace in a parameter comment does not end the search early. The returned
    parameter list has its comments removed, so a comment that names an
    extractor is not read as one.
    """
    # Brackets are matched on a copy with literals blanked, so a `{` in a
    # string or a char literal does not end a block early or late.
    masked = masked_source(source)
    opener = masked.find("(", start)
    params = source[opener : opener + len(balanced(masked[opener:]))]
    brace = masked.find("{", opener + len(params))
    if brace < 0:
        return None
    returns = source[opener + len(params) : brace]
    # The masked copy blanks nested comments too.
    code = canonical_params(masked[opener : opener + len(params)], opener)
    block = source[brace : brace + len(balanced(masked[brace:], "{", "}"))]
    return code, returns, Block(block, brace)


class Params(str):
    """A canonical parameter list, with the names that may carry a raw body.

    `possible` holds each plainly named parameter whose type the audit can
    neither read as a byte carrier nor rule out as one. `written` is the list
    as the source spells it, for a finding that names an alias. `raw_reader`
    tells whether a top-level parameter is a trusted raw query reader, as
    `raw_reader` reads it.
    """

    def __new__(
        cls, text: str, possible: frozenset[str], written: str, raw_reader: bool = False
    ) -> "Params":
        params = super().__new__(cls, text)
        params.possible = possible
        params.written = written
        params.raw_reader = raw_reader
        return params


@functools.lru_cache(maxsize=None)
def canonical_params(params: str, opener: int) -> Params:
    """A parameter list with each parameter type in its `canonical_type` form.

    Each type is read at its own site in the audited source. A plainly named
    parameter whose type is no byte carrier is `possible` when its type has
    an alias the audit cannot read, or a head the audit does not know, as
    `known_non_bytes` reads it.
    """
    out, possible, readers = params, set(), False
    for start, end in reversed(parameter_spans(params)):
        item = params[start:end]
        colon = re.search(r"(?<!:):(?!:)", item)
        if colon is None:
            continue
        written = item[colon.end() :]
        site = (SOURCE[0], opener + start + colon.end())
        readers = readers or raw_reader(written, site)
        canonical, unread = canonical_type(written, site)
        pattern = item[: colon.start()].strip()
        named = re.fullmatch(r"(?:mut\s+)?([a-z_][a-z_0-9]*)", pattern)
        typed = "%s: %s" % (pattern, canonical)
        if named and not BYTE_PARAMETER.search(typed):
            if unread is not None or not known_non_bytes(canonical, opener + start):
                possible.add(named.group(1))
        out = out[: start] + " " + typed + out[end:]
    return Params(out, frozenset(possible), params, readers)


def parameter_spans(params: str) -> list[tuple[int, int]]:
    """The span of each top-level parameter in a parenthesized parameter list."""
    spans, depth, start = [], 0, 1
    for index in range(1, len(params) - 1):
        char = params[index]
        if char in "([{<":
            depth += 1
        elif char in ")]}" or char == ">" and params[index - 1] != "-":
            depth -= 1
        elif char == "," and depth == 0:
            spans.append((start, index))
            start = index + 1
    if params[start : len(params) - 1].strip():
        spans.append((start, len(params) - 1))
    return spans


def known_non_bytes(canonical: str, at: int) -> bool:
    """Whether a canonical parameter type is one the audit knows holds no raw body.

    A standard type, a wire scalar, a bare extractor, a tuple and a struct
    the audit scans are known. Any other head, such as an unknown path or a
    fixed-size array, may hold bytes.
    """
    text = re.sub(r"^(?:&\s*(?:'[a-z_]+\s+)?(?:mut\s+)?|impl\s+|dyn\s+)*", "", canonical.strip())
    if text.startswith("("):
        return True
    head = re.match(r"((?:[A-Za-z_]\w*\s*::\s*)*[A-Za-z_]\w*)", text)
    if head is None:
        return False
    name = re.sub(r"\s", "", head.group(1))
    if name in STD_TYPES or name in WIRE_TYPES or name in EXTRACTOR_NAMES:
        return True
    return bool(resolve_symbol("struct", name, (SOURCE[0], at)))


class Block(str):
    """A fn block, with where it starts in the audited source.

    Two fns can have the same block text in different modules, so a site
    inside a block counts from its own start, not from a text search.
    """

    def __new__(cls, text: str, start: int) -> "Block":
        block = super().__new__(cls, text)
        block.start = start
        return block


@functools.lru_cache(maxsize=None)
def masked_source(source: str) -> str:
    """`source` through `code_only`, computed once per source."""
    return code_only(source)


def byte_parameters(params: str) -> set[str]:
    """Names of the parameters that carry the raw request body.

    `params` is canonical, as `parts_at` returns it, so an alias such as
    `type RawBody = Vec<u8>;` is already its target here.
    """
    return set(BYTE_PARAMETER.findall(params))


def possible_carriers(params: str) -> frozenset[str]:
    """The parameters that may carry a raw body, as `canonical_params` reads them."""
    return getattr(params, "possible", frozenset())


# A comment, a string literal or a char literal. A raw string comes before a
# plain one, so `r#"..."#` is read whole.
# A raw string or a byte string prefix, such as `r#"`, `br"` or `b"`.
STRING_PREFIX = re.compile(r'b?r(#*)"|b"')

# A char literal. A lifetime such as `'a` has no closing quote, so it stays.
CHAR_LITERAL = re.compile(r"'(?:\\.|[^\\'])'")


@functools.lru_cache(maxsize=None)
def code_only(block: str, literals: bool = True) -> str:
    """`block` with comments and literals blanked, at the same length.

    A literal keeps its opening and closing delimiters, such as `b"` and `"`
    or `r#"` and `"#`, so a second pass leaves masked text unchanged. With
    `literals` false, only comments are blanked. Newlines stay, so offsets and
    line numbers do not change. A block comment can nest, as in Rust.
    """
    out = list(block)

    def blank(start: int, stop: int, head: int = 0, tail: int = 0) -> int:
        if head and not literals:
            return stop
        for index in range(start + head, stop - tail):
            if out[index] != "\n":
                out[index] = " "
        return stop

    index = 0
    while index < len(block):
        char = block[index]
        prefix = STRING_PREFIX.match(block, index)
        if block.startswith("//", index):
            stop = block.find("\n", index)
            index = blank(index, len(block) if stop < 0 else stop)
        elif block.startswith("/*", index):
            depth, stop = 0, index
            while stop < len(block):
                if block.startswith("/*", stop):
                    depth, stop = depth + 1, stop + 2
                elif block.startswith("*/", stop):
                    depth, stop = depth - 1, stop + 2
                    if depth == 0:
                        break
                else:
                    stop += 1
            index = blank(index, min(stop, len(block)))
        elif prefix and prefix.group(1) is not None:
            close = block.find('"' + prefix.group(1), prefix.end())
            tail = 1 + len(prefix.group(1))
            stop = len(block) if close < 0 else close + tail
            index = blank(index, stop, prefix.end() - index, tail if close >= 0 else 0)
        elif char == '"' or prefix:
            opener = block.index('"', index) + 1
            stop = opener
            while stop < len(block) and block[stop] != '"':
                stop += 2 if block[stop] == "\\" else 1
            closed = stop < len(block)
            index = blank(index, min(stop + 1, len(block)), opener - index, 1 if closed else 0)
        elif char == "'" and CHAR_LITERAL.match(block, index):
            index = blank(index, CHAR_LITERAL.match(block, index).end(), 1, 1)
        elif char.isalnum() or char == "_":
            # A whole word, so `r"` or `b"` inside a name is no literal.
            while index < len(block) and (block[index].isalnum() or block[index] == "_"):
                index += 1
        else:
            index += 1
    return "".join(out)


# One raw-body parse: `(type, optional, tolerant, site)`.
Parse = tuple["str | None", bool, bool, Site]


def raw_body_parses(source: str, handler: str) -> list[Parse]:
    """`(type, optional, tolerant, site)` for each raw-body parse in a handler and helpers.

    `optional` means an empty body can skip the parse, so the body is optional.
    `tolerant` means the parse turns its error into a value, so a present body
    need not carry the mandatory fields either. A guard or a tolerant call at
    the helper call site carries into the helper.

    A helper counts only when the handler passes it a body variable, and only
    the helper parameters that receive the body are read as bodies. A helper
    that passes the body on is followed too, as `carrier_parses` reads it. The type
    comes from a turbofish, then from a `let` binding in the same statement,
    then from a `Result<T, _>` return type. It is `None` when none of those
    names it, or when the call reads the body in a form the audit cannot read.
    The site is where the parse is, as `block_parses` reads it.
    """
    handler_found = handler_parts(source, handler)
    if handler_found is None:
        return []
    # `source` is masked, so a call in a comment or a string is no parse.
    handler_block = handler_found[2]
    # A byte carrier inside a `Result` or an `Option` is seeded where its
    # success binds it, as `wrapped_successes` reads it.
    seeds = {name: -1 for name in byte_parameters(handler_found[0])}
    for name, kind in wrapped_carriers(handler_found[0]).items():
        seeds.update(wrapped_successes(handler_block, name, kind))
    carriers = moved_names(handler_block, seeds)
    carriers.possible = possible_carriers(handler_found[0])
    if not dict(carriers):
        # No byte carrier, so only a parse of a possible one can be found.
        return block_parses(handler_block, carriers, handler_found[1])
    return carrier_parses(
        source, handler_block, carriers, handler_found[1], params=handler_found[0]
    )


# How many helper calls deep `carrier_parses` follows a body before it gives up.
HELPER_DEPTH = 8


def carrier_parses(
    source: str,
    block: str,
    carriers: dict[str, int],
    returns: str,
    optional: bool = False,
    tolerant: bool = False,
    path: frozenset[tuple[str, str]] = frozenset(),
    params: str = "",
) -> list[Parse]:
    """`(type, optional, tolerant, site)` for each parse of a carrier in `block` and below.

    Each helper that `block` passes a carrier to is read the same way, with
    the receiving parameter as its carrier. A guard or a tolerant call at a
    call site carries into every level below it. `path` holds each
    `(helper, parameter)` already on this chain of calls, so a recursive
    helper is read once. A chain deeper than `HELPER_DEPTH` is a `None` type,
    so the audit reports it and fails closed.
    """
    found = block_parses(block, carriers, returns)
    parses = [(kind, o or optional, t or tolerant, site) for kind, o, t, site in found]
    handed = handoffs(source, block, carriers, receivers=True, params=params)
    unknown = site_in(block)
    for helper, parts, name, guarded, discarded in handed:
        # A helper the audit cannot find or read gets the body all the same, so
        # it is an unresolved parse, as for a `Result` extractor handoff.
        if parts is None or name is None:
            parses.append((None, optional or guarded, tolerant or discarded, unknown))
            continue
        params, helper_returns, helper_block = parts
        if (helper, name) in path:
            continue
        # A method that gets the body as its receiver reads it as `self`. A
        # body handed to a parameter of another type cannot be followed, so
        # it is an unresolved parse.
        if name != "self" and name not in byte_parameters(params):
            parses.append((None, optional or guarded, tolerant or discarded, unknown))
            continue
        if len(path) >= HELPER_DEPTH:
            parses.append((None, optional or guarded, tolerant or discarded, unknown))
            continue
        parses += carrier_parses(
            source,
            helper_block,
            moved_names(helper_block, {name}),
            helper_returns,
            optional or guarded,
            tolerant or discarded,
            path | {(helper, name)},
            params,
        )
    return parses


class Carriers(dict):
    """Carrier names with where each was bound, and the names that are partial.

    A partial carrier holds part of the body, or the body inside another
    value. A parse of it, or a handoff of it, cannot be typed, so it fails
    closed.
    """

    partial: frozenset[str] = frozenset()
    possible: frozenset[str] = frozenset()
    # Each name a destructuring pattern binds anew to a carrier, with where.
    rebound: dict[str, int] = {}


def carrier_live(block: str, carriers: dict[str, int], name: str, position: int) -> bool:
    """Whether the carrier `name` holds the body at `position`.

    The first binding decides, as `live_binding` reads it. A destructuring
    pattern that binds the same name anew to a carrier, as `moved_names`
    records it, holds the body from its own binding on.
    """
    if live_binding(block, name, position, carriers[name]):
        return True
    rebind = getattr(carriers, "rebound", {}).get(name)
    return rebind is not None and position >= rebind and live_binding(block, name, position, rebind)


def carrier_partial(carriers: dict[str, int], name: str, position: int) -> bool:
    """Whether the carrier `name` holds only part of the body at `position`."""
    if name in partial_names(carriers):
        return True
    rebind = getattr(carriers, "rebound", {}).get(name)
    return rebind is not None and position >= rebind


def possible_names(carriers: dict[str, int]) -> frozenset[str]:
    """The parameters that may carry a raw body, which a parse reads unresolved."""
    return getattr(carriers, "possible", frozenset())


def partial_names(carriers: dict[str, int]) -> frozenset[str]:
    """The partial carriers of `carriers`, or none for a plain dict."""
    return getattr(carriers, "partial", frozenset())


def moved_names(block: str, names: set[str] | dict[str, int]) -> Carriers:
    """`names` plus each name that a `let` binds to one of them, with where.

    Each `let` value goes through `argument_passes`. A value that is a whole
    carrier, such as `body`, `&body` or `body.as_ref()`, makes a carrier
    alias, bound at that `let`. A value that holds a carrier in any other
    way, such as `&body[4..]`, `body.to_vec()` or a struct literal, makes a
    partial carrier. An alias of a partial carrier is partial too. `names`
    is a set of parameters, each bound at -1, or a map of each name to where
    it is bound. A `let` counts only while its source still
    holds the body, as `live_binding` reads it. A read through a call, such
    as `from_utf8(&body)`, is the call's own handoff, so it binds no carrier.
    """
    found = Carriers(names if isinstance(names, dict) else {name: -1 for name in names})
    found.rebound = {}
    partial: set[str] = set()
    lets = [
        (let.group(1), let.start(), let.end(), block[let.end() : statement_end(block, let.end())])
        for let in re.finditer(r"\blet\s+(?:mut\s+)?([a-z_][a-z_0-9]*)\s*(?::[^=;]*)?=(?!=)", block)
    ]
    patterns = destructuring_lets(block)
    while True:
        moved: dict[str, tuple[int, bool]] = {}
        for bound, start, opener, value in lets:
            if bound in found or bound in moved:
                continue
            for name, bound_at in list(found.items()):
                way = live_passes(block, value, opener, name, bound_at)
                if way and live_binding(block, name, start, bound_at):
                    moved[bound] = (start, way == "wrapped" or name in partial)
                    break
        # A carrier that flows into a destructuring pattern makes each name
        # the pattern binds a partial carrier, so a parse of it fails closed.
        for binds, start, opener, value in patterns:
            if all(bound in found or bound in moved for bound, _ in binds):
                if all(found.rebound.get(bound) == at for bound, at in binds if bound in found):
                    continue
            flows = any(
                live_passes(block, value, opener, name, bound_at)
                and carrier_live(block, found, name, start)
                for name, bound_at in list(found.items())
            )
            if not flows:
                continue
            for bound, at in binds:
                if bound in found:
                    if found.rebound.get(bound) != at:
                        found.rebound[bound] = at
                        moved.setdefault(bound, (found[bound], bound in partial))
                elif bound not in moved:
                    moved[bound] = (at, True)
        if not moved:
            found.partial = frozenset(partial)
            return found
        for bound, (start, is_partial) in moved.items():
            found[bound] = start
            if is_partial:
                partial.add(bound)


def live_passes(block: str, value: str, opener: int, name: str, bound_at: int) -> str | None:
    """How a `let` value at `opener` in `block` hands on the carrier `name`.

    A use of `name` that a pattern in the value binds anew, as in the arm
    `Ok(body) => body.into_request()`, is another value, so it is blanked
    first. `argument_passes` reads what is left.
    """
    uses = list(re.finditer(r"(?<![.\w])%s\b" % re.escape(name), value))
    if not uses:
        return None
    for use in reversed(uses):
        if not live_binding(block, name, opener + use.start(), bound_at):
            value = value[: use.start()] + " " * len(name) + value[use.end() :]
    return argument_passes(value, name)


@functools.lru_cache(maxsize=None)
def destructuring_lets(block: str) -> list[tuple[list[tuple[str, int]], int, int, str]]:
    """`(bound names, start, value start, value)` for each `let` whose pattern is not one name.

    It reads a plain `let`, an `if let` and a `while let` with a tuple,
    struct, slice, reference or variant pattern. Each bound name comes with
    where the pattern binds it. A name followed by `(`, `{`, `::` or a
    single `:` is a path or a field, not a binding.
    """
    found = []
    for let in re.finditer(r"\blet\s+", block):
        rest = block[let.end() :]
        if re.match(r"(?:mut\s+)?[a-z_][a-z_0-9]*\s*(?::(?!:)|=(?![=>]))", rest):
            continue
        depth, equals, colon = 0, None, None
        for index, char in enumerate(rest):
            if char in "([{":
                depth += 1
            elif char in ")]}":
                depth -= 1
                if depth < 0:
                    break
            elif depth == 0 and char == ";":
                break
            elif depth == 0 and char == ":" and rest[index - 1 : index + 2].count(":") == 1:
                colon = index if colon is None else colon
            elif depth == 0 and char == "=" and rest[index + 1 : index + 2] not in ("=", ">"):
                if rest[index - 1 : index] not in ("=", "!", "<", ">"):
                    equals = index
                    break
        if equals is None:
            continue
        pattern = rest[: colon if colon is not None else equals]
        opener = let.end() + equals + 1
        head = block[max(0, let.start() - 12) : let.start()]
        if re.search(r"\b(?:if|while)\s*$", head):
            value_end = opener
            depth = 0
            while value_end < len(block) and not (depth == 0 and block[value_end] == "{"):
                depth += block[value_end] in "(["
                depth -= block[value_end] in ")]"
                value_end += 1
        else:
            value_end = statement_end(block, opener)
        binds = [
            (name.group(0), let.end() + name.start())
            for name in re.finditer(r"(?<![\w:.])[a-z_][a-z_0-9]*\b", pattern)
            if name.group(0) not in ("ref", "mut", "box", "_")
            and not re.match(r"\s*(?:\(|\{|::|:(?!:))", pattern[name.end() :])
        ]
        if binds:
            found.append((binds, let.start(), opener, block[opener:value_end]))
    return found


def statement_end(text: str, start: int) -> int:
    """Where the statement that runs from `start` ends: its top-level `;` or block end."""
    depth = 0
    for index in range(start, len(text)):
        char = text[index]
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth < 0:
                return index
        elif char == ";" and depth == 0:
            return index
    return len(text)


def split_expression(
    text: str, *separators: str, types: bool = False, found: list[str] | None = None
) -> list[str]:
    """`text` split at each top-level separator, such as `,`, `&&` or `||`.

    A separator inside `( )`, `[ ]` or `{ }` is not at the top level. A `<`
    opens a generic only in a type (`types`) or right after `::`, as in a
    turbofish. Anywhere else it is a comparison. Every argument list,
    parameter list and boolean guard is split here. Each separator met is
    added to `found`, when it is given.
    """
    items, stack, current, index = [], [], "", 0
    while index < len(text):
        char = text[index]
        if char in "([{":
            stack.append(char)
        elif char == "<" and (types or text[:index].rstrip().endswith("::")):
            stack.append(char)
        elif char == ">" and stack and stack[-1] == "<" and text[index - 1 : index] != "=":
            stack.pop()
        elif char in ")]}" and stack:
            while stack and stack.pop() == "<":
                pass
        elif not stack:
            separator = next((s for s in separators if text.startswith(s, index)), None)
            if separator is not None:
                if found is not None:
                    found.append(separator)
                items.append(current.strip())
                current, index = "", index + len(separator)
                continue
        current += char
        index += 1
    if current.strip():
        items.append(current.strip())
    return items


def condition_keeps(joined: str, value: bool) -> bool:
    """Whether `TEST <joined>` has the test's `value` whenever the test has it.

    `joined` is the rest of an `if` condition after its test, such as
    `&& ready()` or `|| (a() && b())`. Only top-level `&&` and `||` count, as
    `split_expression` reads them, and `&&` binds tighter than `||`. A true
    test keeps the condition true when no operator follows or the first one is
    `||`. A false test keeps it false when every operator is `&&`. Any other
    text after the test, such as `== false`, keeps nothing. Every guard reader
    in the audit asks this.
    """
    joined = joined.strip()
    if not joined:
        return True
    if not joined.startswith(("&&", "||")):
        return False
    operators: list[str] = []
    split_expression(joined, "&&", "||", found=operators)
    if value:
        return operators[0] == "||"
    return all(operator == "&&" for operator in operators)


def split_top_level(text: str) -> list[str]:
    """The comma-separated items of an argument list, as `split_expression` reads it."""
    return split_expression(text, ",")


# A word before `(` that is a keyword, not a call.
NOT_CALLS = frozenset({"if", "match", "while", "for", "return", "in", "loop", "move", "fn", "let"})

# An optional turbofish between a helper name and its `(`, as in `decode::<T>(`.
TURBOFISH = r"(?:\s*::\s*<[^()]*?>)?"

# A free call: a lowercase name, not a method or a path, with an optional
# turbofish. Group 1 is the name. `handoffs` and `receiving_parameters` read
# every helper call through this one pattern.
FREE_CALL_HEAD = r"(?<![\w.:])([a-z_][a-z_0-9]*)%s" % TURBOFISH
FREE_CALL_NAME = FREE_CALL_HEAD + r"\s*\("


# Views that read a whole carrier: `.as_ref()`, `.as_slice()`, `.deref()`,
# `.clone()` or a full `[..]` slice.
WHOLE_VIEW = r"(?:\.\s*(?:as_ref|as_slice|as_bytes|deref|clone)\s*\(\s*\)|\[\s*\.\.\s*\])"


def argument_root(argument: str) -> tuple[str | None, bool]:
    """The variable an argument reads, and whether it reads all of it.

    `&`, `&mut`, `*` and grouping parentheses are stripped, and so are whole
    views in `WHOLE_VIEW`. What is left must be a plain name, which is then
    the root, read whole. A name followed by anything else, such as
    `body[4..]` or `body.to_vec()`, is the root, read in a form the audit
    cannot type. Any other argument has no root. Every carrier and argument
    reader in the audit strips borrows through this one step.
    """
    text = argument.strip().rstrip(",").strip()
    while True:
        lead = re.match(r"(?:&\s*(?:mut\b)?|\*)\s*", text)
        if lead and lead.end():
            text = text[lead.end() :].strip()
            continue
        if text.startswith("(") and len(balanced(text)) == len(text):
            text = text[1:-1].strip()
            continue
        view = re.search(r"\s*%s$" % WHOLE_VIEW, text)
        if view and view.start():
            text = text[: view.start()].strip()
            continue
        break
    if re.fullmatch(r"[a-z_][a-z_0-9]*", text) and text not in ("mut", "self") or text == "self":
        return text, True
    head = re.match(r"([a-z_][a-z_0-9]*)\b", text)
    return (head.group(1), False) if head and head.group(1) != "mut" else (None, False)


# Reads of a carrier that yield a plain value, not the carrier: a length, a
# test or one element. `helper(body.len())` hands `helper` no body.
SCALAR_READS = frozenset(
    {
        "len", "is_empty", "starts_with", "ends_with", "contains", "first", "last", "get",
        "capacity", "is_ok", "is_err", "is_ok_and", "is_err_and", "is_some", "is_none",
    }
)


def argument_passes(argument: str, variable: str) -> str | None:
    """How a call argument hands on `variable`: `"direct"`, `"wrapped"` or `None`.

    It is direct when the whole argument, borrows stripped, is the variable,
    as `argument_root` reads it. It is wrapped when the variable appears
    anywhere else in it, as in a struct literal, a tuple, an array, a
    closure or a macro, except inside the argument list of an inner call,
    which is its own handoff, or as the receiver of one of `SCALAR_READS`.
    A wrapped handoff cannot be followed, so the caller fails closed.
    """
    if argument_root(argument) == (variable, True):
        return "direct"
    visible = outside_calls(argument, macros=False)
    for found in re.finditer(r"(?<![.\w])%s\b" % re.escape(variable), visible):
        read = re.match(r"\s*\.\s*([a-z_]\w*)\s*\(", visible[found.end() :])
        if read is None or read.group(1) not in SCALAR_READS:
            return "wrapped"
    return None


def outside_calls(argument: str, macros: bool = True) -> str:
    """`argument` with the argument list of each call in it blanked.

    A name inside `normalize(body)` is passed to `normalize`, not to the call
    that receives its result, so only a name outside every inner call is
    passed on. A receiver such as `body` in `body.as_ref()` stays. With
    `macros` false, a macro's arguments stay, since a macro is no call.
    """
    out = argument
    for call in re.finditer(r"[\w!>]\s*\(" if macros else r"[\w>]\s*\(", argument):
        opener = call.end() - 1
        inner = balanced(argument[opener:])
        out = out[: opener + 1] + " " * (len(inner) - 2) + out[opener + len(inner) - 1 :]
    return out


# Methods on `Bytes`, `[u8]` and `Vec<u8>` that read or copy the bytes and
# run no deserializer. A raw body as their receiver is no handoff. No
# conversion is here, since a local `From` impl can run any code.
BYTE_ACCESSORS = frozenset(
    {
        "as_ref", "as_slice", "as_bytes", "borrow", "chunks", "clone", "contains",
        "copy_to_bytes", "deref", "ends_with", "first", "get", "is_empty",
        "iter", "last", "len", "slice", "split_at", "split_off", "split_to",
        "starts_with", "to_owned", "to_vec", "windows",
    }
)


# Each conversion method on a raw body, and the constructor it runs.
CONVERSIONS = {"into": "from", "try_into": "try_from"}


def conversion_parts(
    source: str, block: str, method: str, start: int
) -> tuple[Parts | None, str | None]:
    """The `from` or `try_from` fn that a conversion `method` runs, and its parameter.

    `start` is where the receiver of the conversion call starts in `block`.

    The target type must be written in the same statement, as in `let x: T =
    body.into();`. `resolve_symbol` then finds `T::from`, or `T::try_from`
    for `try_into`, from the call site. A `Result<T, _>` annotation names
    `T`. A target the audit cannot see, or a constructor it cannot find, is
    `(None, None)`, so the handoff fails closed.
    """
    annotated = r"\blet\s+(?:mut\s+)?[a-z_]\w*\s*:\s*([^=;]+?)\s*=\s*$"
    typed = re.search(annotated, block[:start])
    if typed is None:
        return None, None
    target, _ = canonical_type(typed.group(1), site_in(block, typed.start(1)))
    wrapped = re.fullmatch(r"Result<\s*(.+?)\s*,.*>", target, re.S)
    head = re.match(r"((?:[A-Za-z_]\w*\s*::\s*)*[A-Z]\w*)", wrapped.group(1) if wrapped else target)
    if head is None:
        return None, None
    reference = "%s::%s" % (head.group(1), CONVERSIONS[method])
    starts = resolve_symbol("fn", reference, site_in(block, start)) or []
    parts = parts_at(source, starts[0]) if len(starts) == 1 else None
    first = re.match(r"\(\s*(?:mut\s+)?([a-z_]\w*)\s*:", parts[0]) if parts else None
    return (parts, first.group(1)) if first else (None, None)


def handoffs(
    source: str, block: str, carriers: dict[str, int], receivers: bool = False, params: str = ""
) -> list[tuple[str, tuple[str, str, str] | None, str | None, bool, bool]]:
    """`(helper, parts, parameter, optional, tolerant)` for each carrier handoff.

    A handoff is a call in `block` that gets a live carrier as an argument,
    outside any inner call. `call_sites` resolves each call at its own site. `parts` is `None` when the audit cannot
    find the helper, and `parameter` is `None` when the receiving parameter
    has no plain name. Both the raw-body scan and the `Result` extractor scan
    read their helpers through this one step.

    With `receivers`, a raw body that is the receiver of a method call, as in
    `body.decode::<T>()`, is handed to that method's `self`, unless the method
    is one of `BYTE_ACCESSORS`. The method is found through the symbol index.
    An unknown or ambiguous one has no `parts`, so the caller fails closed.

    A method call such as `x.decode(body)` reaches only the method of the
    type that `receiver_type` finds for `x` in `block` or `params`. The audit
    infers no other type, so a receiver of unknown type has no `parts`.
    """
    # Each call resolves at its own site, since block-scoped imports can bind
    # one name to different helpers. Calls that reach the same definition
    # and parameter merge: optional and tolerant only when every call is.
    merged: dict[tuple[object, str | None], tuple[str, Parts | None, bool, bool]] = {}
    for helper, parts, at, qualified in call_sites(source, block, params):
        helper_params = parts[0] if parts else ""
        states = receiving_parameters(block, helper, helper_params, carriers, qualified, at)
        identity = parts[2].start if parts else (helper, at)
        for name, (optional, tolerant) in states.items():
            if (identity, name) in merged:
                _, _, was_optional, was_tolerant = merged[identity, name]
                optional, tolerant = optional and was_optional, tolerant and was_tolerant
            merged[identity, name] = (helper, parts, optional, tolerant)
    found = [(helper, parts, name, o, t) for (_, name), (helper, parts, o, t) in merged.items()]
    # A method call whose receiver holds a carrier hands it to `self`. The
    # receiver goes through `argument_passes`, as an argument does, so a
    # grouped or borrowed receiver such as `(&body)` is read like `body`, and
    # one that holds the carrier in any other way fails closed.
    for call in qualified_calls(block) if receivers else []:
        if call.group(1).strip() != "." or call.group(2) in BYTE_ACCESSORS:
            continue
        method, expression = call.group(2), receiver_expression(block, call.start())
        for variable in carriers:
            way = argument_passes(expression, variable)
            if way is None or not carrier_live(block, carriers, variable, call.start()):
                continue
            if in_dead_closure(block, call.start()):
                continue
            after = block[call.end() - 1 + len(balanced(block[call.end() - 1 :])) :]
            tolerant = discards_error(block[: call.start()], after)
            allowed = empty_allowed(block, call.start(), variable, tolerant)
            if way == "wrapped":
                found.append((method, None, "self", allowed, tolerant))
                continue
            # A partial carrier reaches no method the audit can follow. A whole
            # one reaches only a method of an `impl` for its own type.
            partial = carrier_partial(carriers, variable, call.start())
            starts = (
                None if partial else method_targets(block, params, variable, method, call.start())
            )
            parts = parts_at(source, starts[0]) if starts and len(starts) == 1 else None
            if method in CONVERSIONS:
                # A conversion hands the body to the constructor it runs.
                start = receiver_start(block, call.start())
                converted, parameter = (
                    (None, None) if partial else conversion_parts(source, block, method, start)
                )
                found.append((method, converted, parameter, allowed, tolerant))
                continue
            by_self = r"\(\s*&?\s*(?:'[a-z_]+\s+)?(?:mut\s+)?self\b"
            self_param = parts is not None and re.match(by_self, parts[0])
            found.append((method, parts if self_param else None, "self", allowed, tolerant))
    return found


def call_sites(
    source: str, block: str, params: str
) -> list[tuple[str, tuple[str, str, str] | None, int, str | None]]:
    """`(helper, parts, start, path)` for each call in `block` that can take a body.

    A bare call, a path-qualified call and a method call each resolve at
    their own site. A call in a closure that never runs is skipped. `path`
    is `None` for a bare call, the written path for a qualified one, and `.`
    for a method call, as `receiving_parameters` reads it. A method call
    reaches what `method_targets` finds, and one with no known receiver type
    has no `parts`, so a handoff to it fails closed.
    """
    found = []
    for call in re.finditer(FREE_CALL_NAME, block):
        helper = call.group(1)
        site = site_in(block, call.start())
        if helper in NOT_CALLS | GENERIC_HELPERS or std_path(helper, site):
            continue
        if in_dead_closure(block, call.start()):
            continue
        found.append((helper, function_parts(source, helper, site), call.start(), None))
    for call in qualified_calls(block):
        helper, path = call.group(2), re.sub(r"\s", "", call.group(1))
        if helper in GENERIC_HELPERS or not handoff_path(call, block):
            continue
        if in_dead_closure(block, call.start()):
            continue
        if path == ".":
            receiver = call_receiver(block, call.start())
            starts = method_targets(block, params, receiver, helper, call.start()) or []
            parts = parts_at(source, starts[0]) if len(starts) == 1 else None
        else:
            site = site_in(block, call.start())
            parts = function_parts(source, path.rstrip(":") + "::" + helper, site)
        found.append((helper, parts, call.start(), path))
    return found


# A path-qualified call or a method call. Group 1 is the path or the `.`, and
# group 2 the last segment, as in `Decoder::decode(` or `decoder.decode(`.
QUALIFIED_CALL = (
    r"((?:[A-Za-z_]\w*(?:\s*(?:::\s*)?<[^()]*?>)?\s*::\s*)+|\.\s*)([a-z_][a-z_0-9]*)%s\s*\("
    % TURBOFISH
)


def handoff_path(call: re.Match, block: str) -> bool:
    """Whether a `QUALIFIED_CALL` match in `block` can hand a body to a JSON parse.

    `serde_json::from_slice` is the parse itself, which `block_parses` reads.
    A path rooted at `std`, `core` or `alloc`, such as `std::str::from_utf8`,
    cannot run serde_json, so it is no handoff. `std_path` reads the root
    through an import, as `use std::str as text;` binds it. Any other path or
    method can hand the body on.
    """
    path = call.group(1).replace(" ", "").lstrip(":")
    if path == "serde_json::" or path == ".":
        return path == "."
    return not std_path(path + call.group(2), site_in(block, call.start()))


@functools.lru_cache(maxsize=None)
def qualified_calls(block: str) -> list[re.Match]:
    """Every `QUALIFIED_CALL` match in `block`, found once."""
    return list(re.finditer(QUALIFIED_CALL, block))


def receiver_type(block: str, params: str, receiver: str, position: int) -> str | None:
    """The written type of a method call's receiver, or `None`.

    `self` is `Self`. Otherwise the last `let` of that name before `position`
    decides: its type annotation, or a value such as `Type::new(..)` or
    `Type { .. }`. With no `let`, a parameter with a written type decides.
    Any other receiver has no type the audit can know.
    """
    if receiver == "self":
        return "Self"
    name = re.escape(receiver)
    binding = r"\blet\s+(?:mut\s+)?%s\s*(?::\s*([^=;]+?))?\s*=\s*([^;]+)" % name
    bindings = list(re.finditer(binding, block[:position]))
    head = r"&?\s*(?:'[a-z_]+\s+)?(?:mut\s+)?((?:[A-Za-z_]\w*\s*::\s*)*[A-Z]\w*|\[\s*u8\s*\])"
    if bindings:
        annotated, value = bindings[-1].group(1), bindings[-1].group(2)
        if annotated:
            at = bindings[-1].start(1)
            canonical, _ = canonical_type(annotated, site_in(block, at))
            typed = re.match(head, canonical)
            return re.sub(r"\s", "", typed.group(1)) if typed else None
        generic = r"(?:\s*::\s*<[^()]*?>)?"
        built = re.match(head + generic + r"\s*(?:::\s*[a-z_]\w*\s*%s\s*\(|\{)" % generic, value.strip())
        return built.group(1) if built else None
    declared = re.search(r"(?<![\w.])%s\s*:\s*%s" % (name, head), params)
    return re.sub(r"\s", "", declared.group(1)) if declared else None


def receiving_parameters(
    block: str,
    helper: str,
    params: str,
    variables: dict[str, int],
    qualified: str | None = None,
    at: int | None = None,
) -> dict[str | None, tuple[bool, bool]]:
    """`(optional, tolerant)` for each `helper` parameter that gets a variable.

    Each argument maps to the parameter at its position. A `self` receiver is
    skipped, since a call does not pass it in the argument list. A parameter
    is optional when every call that fills it is guarded by `.is_empty()` or
    turns the error into a value. It is tolerant when every such call turns
    the error into a value. `argument_passes` decides how an argument hands
    a variable on. A wrapped one, and an argument with no named parameter,
    map to `None`, so the caller fails closed. With `qualified`, the calls
    read are the calls of `helper` through that path, or `.` for a method
    call. With `at`, only the call that starts there is read.
    """
    # A pattern such as `Extension(state): ..` keeps its slot with no name, so
    # the parameters after it keep their positions.
    names: list[str | None] = []
    for item in split_expression(params[1:-1] if params else "", ",", types=True):
        if re.fullmatch(r"&?\s*(?:'[a-z_]+\s+)?(?:mut\s+)?self", item):
            continue
        plain = re.match(r"(?:mut\s+)?([a-z_][a-z_0-9]*)\s*:", item)
        names.append(plain.group(1) if plain else None)
    states: dict[str, tuple[bool, bool]] = {}
    if qualified:
        calls = [
            call
            for call in qualified_calls(block)
            if call.group(2) == helper
            and re.sub(r"\s", "", call.group(1)) == qualified
            and handoff_path(call, block)
        ]
    else:
        calls = list(re.finditer(r"(?<![\w.:])%s%s\s*\(" % (re.escape(helper), TURBOFISH), block))
    for call in calls:
        if at is not None and call.start() != at:
            continue
        raw = balanced(block[call.end() - 1 :])
        arguments = split_top_level(raw[1:-1].replace("->", ""))
        tolerant = discards_error(block[: call.start()], block[call.end() - 1 + len(raw) :])
        for index, argument in enumerate(arguments):
            live = [v for v in variables if carrier_live(block, variables, v, call.start())]
            ways = {v: argument_passes(argument, v) for v in live}
            ways = {
                v: "wrapped" if way and carrier_partial(variables, v, call.start()) else way
                for v, way in ways.items()
            }
            passed = [v for v, way in ways.items() if way]
            name = names[index] if index < len(names) else None
            if not passed:
                continue
            # A wrapped carrier reaches no parameter the audit can follow.
            if any(way == "wrapped" for way in ways.values()):
                name = None
            allowed = any(
                empty_allowed(block, call.start(), variable, tolerant) for variable in passed
            )
            was_optional, was_tolerant = states.get(name, (True, True))
            states[name] = (was_optional and allowed, was_tolerant and tolerant)
    return states


@functools.lru_cache(maxsize=None)
def closures(block: str) -> list[tuple[int, int, int]]:
    """`(start, body start, body end)` for each closure expression in `block`.

    The body is a block, or an expression up to the first top-level `,` or
    `;`, as `closure_body_end` reads it. An operator such as `a || b` can
    read as one too. `in_dead_closure` acts only on a closure that a `let`
    binds directly, which an operator never is.
    """
    found = []
    for head in re.finditer(r"(?<![|\w])(?:move\s+)?\|([^|]*)\|", block):
        opener, stop = closure_body_end(block, head.end())
        found.append((head.start(), opener, stop))
    return found


def closure_body_end(text: str, after: int) -> tuple[int, int]:
    """Where the body of a closure whose parameters end at `after` starts and ends."""
    body = text[after:].lstrip()
    opener = after + len(text[after:]) - len(body)
    if body.startswith("{"):
        return opener, opener + len(balanced(body, "{", "}"))
    rest = scope_rest(body)
    cut = [index for index, char in enumerate(rest) if char in ",;"]
    return opener, opener + (cut[0] if cut else len(rest))


def in_dead_closure(block: str, position: int) -> bool:
    """Whether `position` is inside a closure in `block` that never runs.

    Each enclosing closure is its own site. One that is called in place,
    passed to a helper or a combinator such as `map`, returned or stored
    runs, or may run, so its body counts as today, which fails closed. Only
    a closure bound by `let` whose name is never read after it, as
    `live_binding` reads the name, never runs. So does one bound to `_`.
    """
    for start, opener, stop in closures(block):
        if not opener <= position < stop:
            continue
        bound = re.search(
            r"\blet\s+(?:mut\s+)?([a-z_][a-z_0-9]*)\s*(?::[^=;]*)?=\s*$", block[:start]
        )
        if bound is None:
            continue
        name = bound.group(1)
        if name == "_":
            return True
        uses = [
            use.start()
            for use in re.finditer(r"(?<![.\w])%s\b" % re.escape(name), block[stop:])
            if live_binding(block, name, stop + use.start(), bound.start())
        ]
        if not uses:
            return True
    return False


def block_parses(block: str, carriers: dict[str, int], returns: str) -> list[Parse]:
    """`(type, optional, tolerant, site)` for each `from_slice` call that reads a carrier.

    `carriers` maps each body name to where it was bound. A name that a later
    `let` gave a new value is no carrier after that `let`. A partial carrier,
    as `moved_names` marks it, has no type the audit can read. The site is
    where the call is, so its type resolves from that module and its imports.
    A call in a closure that never runs, as `in_dead_closure` reads it, is
    no parse.
    """
    parses: list[Parse] = []
    for hit in FROM_SLICE_CALL.finditer(block):
        if in_dead_closure(block, hit.start()):
            continue
        turbofish = None
        opener = hit.end() - 1
        if block[opener] == "<":
            turbofish = balanced(block[opener:], "<", ">")[1:-1].strip()
            opener = block.find("(", opener + len(turbofish) + 2)
        call = balanced(block[opener:])
        argument = call[1:-1].strip().rstrip(",").strip()
        root, whole = argument_root(argument)
        possible = root in possible_names(carriers) and root not in carriers
        if root is None or root not in carriers and not possible:
            continue
        if root in carriers and not carrier_live(block, carriers, root, hit.start()):
            continue
        if root not in carriers and not live_binding(block, root, hit.start()):
            continue
        before = block[: hit.start()]
        after = block[opener + len(call) :]
        tolerant = discards_error(before, after)
        # A result stored by `let parsed = ..;` is read where it is used. It stays
        # strict when it is never used or is passed on, since a callee may reject.
        stored = re.search(r"\blet\s+(?:mut\s+)?([a-z_][a-z_0-9]*)\s*(?::[^=;]*)?=\s*$", before)
        if not tolerant and stored and after.lstrip().startswith(";"):
            name = re.escape(stored.group(1))
            used = re.search(r"(?<![.\w])%s\b" % name, after)
            passed = re.search(r"[(,]\s*&?\s*(?:mut\s+)?%s\b" % name, after)
            tolerant = bool(used) and not passed and not error_rejects(stored.group(1), after)
        optional = empty_allowed(block, hit.start(), root, tolerant)
        # A body read through an index, a method or a generic type is not
        # something the audit can type, so it is reported.
        partial = root in carriers and carrier_partial(carriers, root, hit.start())
        if not whole or possible or partial:
            kind = None
        elif turbofish is not None and not re.fullmatch(r"[A-Za-z0-9_:]+", turbofish):
            kind = None
        else:
            kind = parse_type(turbofish, before, after, returns)
        parses.append((kind, optional, tolerant, site_in(block, hit.start())))
    return parses


def guards(block: str, position: int, variable: str) -> bool:
    """Whether an `if` on `<variable>.is_empty()` lets an empty body skip the parse.

    The test counts when the parse is in the arm that runs for a non-empty
    body. That is the `else` of `if body.is_empty()`, or the condition or
    block of `if !body.is_empty()` when its `else` does not reject. A plain
    `if body.is_empty() { .. }` before the parse also counts when its block
    returns a success, as `success_value` reads it, and no error. Any other use, such as a
    log field, is no guard.
    """
    pattern = r"\bif\s+(!\s*)?%s\.is_empty\(\)" % re.escape(variable)
    for test in re.finditer(pattern, block[:position]):
        opener = block.find("{", test.end())
        if opener < 0:
            continue
        # The whole condition must hold for every empty body, or for none.
        # `body.is_empty() && x` can skip the arm, and `!body.is_empty() || x`
        # can take it with an empty body. `condition_keeps` decides.
        if not condition_keeps(block[test.end() : opener], not test.group(1)):
            continue
        taken = balanced(block[opener:], "{", "}")
        taken_end = end = opener + len(taken)
        while re.match(r"\s*else\b", block[end:]):
            branch = block.find("{", end)
            end = branch + len(balanced(block[branch:], "{", "}"))
        # A parse in the arm of `if !body.is_empty()` is guarded only when the
        # `else`, which runs for an empty body, lets the request through.
        if test.group(1) and test.end() < position < taken_end:
            if not rejecting_exit(block[taken_end:end]):
                return True
        # The `else` of `if body.is_empty()` is a guard only when the empty-body
        # arm itself lets the request through.
        if not test.group(1) and taken_end < position < end and not rejecting_exit(taken):
            return True
        # An early return counts only for a parse after the whole `if`. A
        # parse inside the empty-body arm runs before that return.
        early_return = returns_success(unconditional(taken))
        after_if = position >= end and same_scope(block, test.start(), position)
        if not test.group(1) and after_if and early_return and not re.search(ERROR_TOKENS, taken):
            return True
    return False


def empty_allowed(block: str, position: int, variable: str, tolerant: bool) -> bool:
    """Whether an empty body can reach a success past the parse at `position`.

    This verdict is kept apart from `tolerant`, which says only that a parse
    error turns into a value. An `.is_empty()` guard, as `guards` reads it,
    lets an empty body skip the parse. A tolerant parse lets it through
    too, unless an earlier empty-body test rejects it, as `rejects_empty`
    reads it.
    """
    if guards(block, position, variable):
        return True
    return tolerant and not rejects_empty(block, position, variable)


def rejects_empty(block: str, position: int, variable: str) -> bool:
    """Whether an `if` on `<variable>.is_empty()` rejects every empty body before `position`.

    The arm that runs for an empty body is the block of `if body.is_empty()`,
    or the `else` of `if !body.is_empty()`, when `condition_keeps` holds for
    the whole condition. It counts when that arm rejects, as
    `rejecting_exit` reads it, and the parse is in the other arm or after the
    whole `if` in the same scope.
    """
    pattern = r"\bif\s+(!\s*)?%s\.is_empty\(\)" % re.escape(variable)
    for test in re.finditer(pattern, block[:position]):
        opener = block.find("{", test.end())
        if opener < 0 or not condition_keeps(block[test.end() : opener], not test.group(1)):
            continue
        taken = balanced(block[opener:], "{", "}")
        taken_end = end = opener + len(taken)
        otherwise = None
        if re.match(r"\s*else\s*\{", block[end:]):
            branch = block.find("{", end)
            otherwise = balanced(block[branch:], "{", "}")
            end = branch + len(otherwise)
        if test.group(1):
            empty_arm, other_arm = otherwise, (opener, taken_end)
        else:
            empty_arm, other_arm = taken, (taken_end, end)
        if empty_arm is None or not rejecting_exit(empty_arm):
            continue
        in_other = other_arm[0] < position < other_arm[1]
        after_if = position >= end and same_scope(block, test.start(), position)
        if in_other or after_if:
            return True
    return False


def rejects_result_body(params: str, block: str) -> bool:
    """Whether a `Result<Json<T>, _>` body is mandatory, since its error rejects.

    The body is mandatory when a method chain on it ends in `?`, or reaches
    `.unwrap()` or `.expect(..)`, before any call that turns the error into a
    value. It is also mandatory when an `if` on `.is_err()` or `.is_ok()`
    sends the error to a rejection, or when any `Err` or catch-all arm of a
    `match` on it rejects, or when an `if let Err(..) = body` block rejects, or when the `else` of a `let Ok(..) = body else` or an
    `if let Ok(..) = body` rejects, as `rejecting_exit` reads it. It is mandatory
    too when the handler returns it, or a chain on it that keeps the error, as
    its value. An `Err` arm that
    hands the request on, for example to replay a committed key, leaves it
    optional.
    """
    found = re.search(r"\b([a-z_][a-z_0-9]*)\s*:\s*%s<\s*%s<" % (RESULT, JSON), params)
    return found is not None and error_rejects(found.group(1), block)


# A raw byte carrier type, as the inner type of a wrapper.
BYTE_TYPE = r"(?:(?<![\w:])Bytes\b|(?<![\w:])Vec<u8>|&?\s*\[u8\])"


def wrapped_carriers(params: str) -> dict[str, str]:
    """Each `Result<Bytes, _>` or `Option<Bytes>` parameter, with its kind.

    The kind is `"result"` or `"option"`, as `FAILURE_VARIANTS` reads it.
    `Vec<u8>` and `[u8]` count as `Bytes` does.
    """
    found = {}
    for kind, wrapper in (("result", RESULT), ("option", OPTION)):
        typed = r"\b([a-z_][a-z_0-9]*)\s*:\s*%s<\s*%s" % (wrapper, BYTE_TYPE)
        found.update({hit.group(1): kind for hit in re.finditer(typed, params)})
    return found


def rejects_wrapped_body(params: str, block: str) -> bool:
    """Whether a wrapped body is mandatory, since its failure rejects.

    It reads `Option<Json<T>>`, `Option<Bytes>` and `Result<Bytes, _>`.
    `error_rejects` reads each with its failure, `None` or `Err`, so the
    same forms decide: `ok_or(..)?`, `?`, `.unwrap()`, `.expect(..)`, a
    failure or catch-all arm that rejects, `if let` on the failure, a
    let-else on the success, and an `if` on an inspection such as
    `.is_none()`. `rejects_result_body` reads `Result<Json<T>, _>`.
    """
    found = re.finditer(r"\b([a-z_][a-z_0-9]*)\s*:\s*%s<\s*%s<" % (OPTION, JSON), params)
    wrapped = {option.group(1): "option" for option in found}
    wrapped.update(wrapped_carriers(params))
    return any(error_rejects(name, block, kind=kind) for name, kind in wrapped.items())


# One step of a chain that turns a wrapper into its value, or leaves it one.
UNWRAP_STEP = (
    r"(?:\?|\.\s*await\b|\.\s*(?:unwrap|expect|ok_or|ok_or_else|unwrap_or|unwrap_or_default"
    r"|unwrap_or_else)\s*\((?:[^()]|\([^()]*\))*\))"
)


def wrapped_successes(block: str, name: str, kind: str) -> dict[str, int]:
    """Each name that holds the byte carrier inside the wrapper `name`, with where.

    The success variant, `Ok` or `Some`, binds it in a `let`, `if let`,
    `while let`, let-else or `match` arm. A `let` whose value is `name`
    through `?`, `.unwrap()`, `.expect(..)`, `.ok_or*(..)` or `.unwrap_or*(..)`
    binds it whole too. Each name is bound where its pattern is, so
    `live_binding` reads it in that scope. A use of `name` that a later
    binding shadows is left out.
    """
    success = FAILURE_VARIANTS[kind][0]
    variable = re.escape(name)
    inner = r"%s%s\s*\(\s*%s([a-z_][a-z_0-9]*)\s*\)" % (PATTERN_LEAD, success, BINDING)
    found: dict[str, int] = {}
    held = r"\b(?:if\s+|while\s+)?let\s+%s\s*=\s*(?:&\s*(?:mut\s+)?)?%s\b" % (inner, variable)
    for hit in re.finditer(held, block):
        if live_binding(block, name, hit.end()):
            found.setdefault(hit.group(1), hit.start(1))
    for scrutinee in re.finditer(r"\bmatch\s+(?:&\s*(?:mut\s+)?)?%s\s*\{" % variable, block):
        if not live_binding(block, name, scrutinee.start()):
            continue
        arms = balanced(block[scrutinee.end() - 1 :], "{", "}")
        offset = scrutinee.end() - 1
        for arm in re.finditer(inner + r"\s*(?:if\b[^{}]*?)?=>", arms):
            found.setdefault(arm.group(1), offset + arm.start(1))
    whole = r"\blet\s+(?:mut\s+)?([a-z_][a-z_0-9]*)\s*(?::[^=;]*)?=\s*%s(?:\s*%s)+\s*;" % (
        variable,
        UNWRAP_STEP,
    )
    for hit in re.finditer(whole, block):
        if live_binding(block, name, hit.start()):
            found.setdefault(hit.group(1), hit.start())
    return found


# For each extractor kind: the success variant, and a pattern for the failure
# variant whose group 1 is the name the pattern binds, if any.
FAILURE_VARIANTS = {
    "result": ("Ok", "Err", r"Err\s*\(\s*%s([a-z_][a-z_0-9]*)?" % BINDING),
    "option": ("Some", "None", r"None()\b"),
}


def error_rejects(
    name: str,
    block: str,
    seen: frozenset[str] = frozenset(),
    bound_at: int = -1,
    path: frozenset[tuple[str, str]] = frozenset(),
    kind: str = "result",
) -> bool:
    """Whether the handler rejects the failure of the extractor `name`.

    `kind` is `"result"` for a `Result` extractor, whose failure is `Err`, or
    `"option"` for an `Option` one, whose failure is `None`. Every form below
    reads the failure variant of that kind, as `FAILURE_VARIANTS` names it.

    `rejects_result_body` gives the forms it reads. A move into another name,
    such as `let captured = body;`, is followed. A pattern can read the
    extractor by value, borrowed, or through `as_ref()` or `as_mut()`.
    `block` has its comments and literals masked, so a call in them is no use.
    A later `let` that gives `name` a new value ends the extractor, as
    `live_binding` reads it. `bound_at` is where a move created `name`.

    A helper that gets the extractor, as `handoffs` reads it, is read the same
    way, with `path` and `HELPER_DEPTH` as in `carrier_parses`. A helper the
    audit cannot find or read makes the body mandatory, so the audit fails
    closed.
    """
    variable = re.escape(name)
    success, failure_name, failure = FAILURE_VARIANTS[kind]

    def live(position: int) -> bool:
        return live_binding(block, name, position, bound_at)

    moved = r"\blet\s+(?:mut\s+)?([a-z_][a-z_0-9]*)\s*(?::[^=;]*)?=\s*%s\s*;" % variable
    for alias in re.finditer(moved, block):
        if alias.group(1) not in seen | {name} and live(alias.start()):
            if error_rejects(alias.group(1), block, seen | {name}, alias.start(), path, kind):
                return True
    borrow = r"(?:&\s*(?:mut\s+)?)?"
    method = r"(?:\s*\.\s*as_(?:ref|mut)\s*\(\s*\))?"
    # The extractor as a pattern reads it: by value, borrowed or through `as_ref`.
    read = borrow + variable + method
    for use in re.finditer(r"(?<![.\w])%s\b" % variable, block):
        if not live(use.start()) or binds_name(block, use.start(), use.end()):
            continue
        verdict, inspection, rest = walk_chain(block[use.end() :], kind)
        # A chain that still holds the error and is the value of the block
        # hands that error to the caller, so it rejects like `?` does.
        if verdict == "open" and yields_block_value(block[: use.start()], rest):
            verdict = "reject"
        # Any other use that still holds the error is strict, unless it is a
        # form that another part of this function reads.
        if verdict == "open" and not read_elsewhere(block, use.start(), rest):
            verdict = "reject"
        if verdict == "reject" and not error_exits_before(block, variable, read, use.start()):
            return True
        if inspection and inspection_rejects(block[: use.start()], inspection, rest):
            return True
    scrutinee = r"\bmatch\s+%s\s*\{" % read
    for match in re.finditer(scrutinee, block):
        if not live(match.start()):
            continue
        arms = balanced(block[match.end() - 1 :], "{", "}")
        for arm in error_arms(arms, kind):
            if arm_rejects(match_arm(arms, arm.start()), arm.group(1)):
                return True
    held = r"\blet\s+%s%s\s*\([^;=]*?\)\s*=\s*%s\s*else\s*\{" % (PATTERN_LEAD, success, read)
    for binding in re.finditer(held, block):
        otherwise = balanced(block[binding.end() - 1 :], "{", "}")
        if live(binding.start()) and rejecting_exit(otherwise):
            return True
    # After `let Err(e) = body else { .. };`, the rest of the scope runs only on
    # failure, so it is the failure arm.
    bound_err = r"\blet\s+%s%s[^=]*=\s*%s\s*else\s*\{" % (PATTERN_LEAD, failure, read)
    for binding in re.finditer(bound_err, block):
        otherwise = balanced(block[binding.end() - 1 :], "{", "}")
        rest = block[binding.end() - 1 + len(otherwise) :].lstrip().lstrip(";")
        if live(binding.start()) and arm_rejects(scope_rest(rest), binding.group(1)):
            return True
    failed = r"\bif\s+let\s+%s%s[^=]*=\s*%s\s*\{" % (PATTERN_LEAD, failure, read)
    for tested in re.finditer(failed, block):
        taken = balanced(block[tested.end() - 1 :], "{", "}")
        if not live(tested.start()):
            continue
        if rejecting_exit(taken) or rejecting_arm(taken, tested.group(1)):
            return True
    kept = r"\bif\s+let\s+%s%s\s*\([^;=]*?\)\s*=\s*%s\s*\{" % (PATTERN_LEAD, success, read)
    for tested in re.finditer(kept, block):
        taken = balanced(block[tested.end() - 1 :], "{", "}")
        rest = block[tested.end() - 1 + len(taken) :]
        if live(tested.start()) and re.match(r"\s*else\s*\{", rest):
            otherwise = balanced(rest[rest.index("{") :], "{", "}")
            if rejecting_exit(otherwise):
                return True
    for helper, parts, parameter, _, _ in handoffs(SOURCE[0], block, {name: bound_at}):
        if parts is None or parameter is None or len(path) >= HELPER_DEPTH:
            return True
        if (helper, parameter) not in path:
            if error_rejects(parameter, parts[2], path=path | {(helper, parameter)}, kind=kind):
                return True
    return False


def live_binding(block: str, name: str, position: int, bound_at: int = -1) -> bool:
    """Whether `name` at `position` still holds the value bound at `bound_at`.

    `bound_at` is where a `let` created `name`, or -1 for a parameter. A later
    `let` that gives `name` a new value ends it for the rest of that scope, as
    `shadowing_lets` reads it. A pattern that binds `name` ends it in the
    code that pattern guards, as `pattern_scopes` reads it. The
    Result-extractor scan and the raw-body carriers both ask this.
    """
    if any(
        bound_at < start and end <= position and same_scope(block, start, position)
        for start, end in shadowing_lets(block, name)
    ):
        return False
    return not any(
        bound_at < bind and scope_start <= position < scope_end
        for bind, _, scope_start, scope_end in pattern_scopes(block, name)
    )


@functools.lru_cache(maxsize=None)
def pattern_scopes(block: str, name: str) -> list[tuple[int, int, int, int]]:
    """`(start, end, scope start, scope end)` for each pattern that binds `name`.

    A pattern is a `match` arm before `=>`, or the pattern of an `if let`, a
    `while let` or a destructuring `let`, before its `=`. A plain
    `let name = ..` is not read here, since `shadowing_lets` reads it. The
    scope is the arm, the block of the `if let` or `while let`, or the rest of
    the block after the `let` statement.
    """
    found: list[tuple[int, int, int, int]] = []
    for hit in re.finditer(r"(?<![.\w])%s\b" % re.escape(name), block):
        ahead = re.match(r"[^;{}]*?(=>|(?<![=!<>])=(?![=>]))", block[hit.end() :])
        if ahead is None or re.match(r"\s*:(?!:)", block[hit.end() :]):
            continue
        marker = hit.end() + ahead.start(1)
        after = marker + len(ahead.group(1))
        # A `,` after more closers than openers leaves the pattern, as in an
        # arm value `handle(body),` before the next arm's `=>`.
        depth = 0
        for char in block[hit.end() : marker]:
            depth += char in "(["
            depth -= char in ")]"
            if char == "," and depth < 0:
                break
        else:
            depth = None
        if depth is not None:
            continue
        statement = block.rfind(";", 0, hit.start())
        head = block[max(statement, block.rfind("{", 0, hit.start()), block.rfind("}", 0, hit.start())) + 1 : hit.start()]
        if ahead.group(1) == "=>":
            rest = block[after:]
            opener = len(rest) - len(rest.lstrip())
            if rest[opener : opener + 1] == "{":
                end = after + opener + len(balanced(rest[opener:], "{", "}"))
            else:
                end = after + len(scope_rest(rest.replace(",", ";")))
            found.append((hit.start(), hit.end(), after, end))
        elif re.search(r"\b(?:if|while)\s+let\b", head):
            opener = block.find("{", after)
            if opener >= 0:
                found.append((hit.start(), hit.end(), opener, opener + len(balanced(block[opener:], "{", "}"))))
        elif re.search(r"\blet\b", head) and not re.fullmatch(r"\s*let\s+(?:mut\s+)?", head):
            stop = after + len(scope_rest(block[after:]).split(";")[0])
            found.append((hit.start(), hit.end(), stop, enclosing_block(block, hit.start())[1]))
    return found


@functools.lru_cache(maxsize=None)
def shadowing_lets(block: str, name: str) -> list[tuple[int, int]]:
    """`(start, end)` of each `let <name> = ..;` that makes `name` a new value.

    After that statement, `name` in the same scope is no longer the extractor.
    A value that is `name` itself, or a method chain on it that still holds
    the error, such as `let body = body;` or `let body = body.map_err(f);`,
    keeps the extractor, so it is no shadow. Any other value, such as
    `match body { .. }`, is a new value.
    """
    variable = re.escape(name)
    found: list[tuple[int, int]] = []
    for binding in re.finditer(r"\blet\s+(?:mut\s+)?%s\s*(?::[^=;]*)?=(?!=)" % variable, block):
        depth, end = 0, len(block)
        for index in range(binding.end(), len(block)):
            char = block[index]
            depth += char in "([{"
            depth -= char in ")]}"
            if depth < 0 or (depth == 0 and char == ";"):
                end = index
                break
        value = block[binding.end() : end].strip()
        lead = re.match(r"&?\s*(?:mut\s+)?%s\b" % variable, value)
        if lead:
            verdict, _, rest = walk_chain(value[lead.end() :])
            if verdict != "tolerate" and not rest.strip():
                continue
        found.append((binding.start(), end))
    return found


def scope_rest(text: str) -> str:
    """`text` up to the end of the block it starts in."""
    depth = 0
    for index, char in enumerate(text):
        depth += char in "([{"
        depth -= char in ")]}"
        if depth < 0:
            return text[:index]
    return text


def binds_name(block: str, start: int, end: int) -> bool:
    """Whether the name at `start..end` is bound there, not read.

    That is the pattern of a `let`, as in `let body = ..`, or a field name or
    a type ascription, as in `body: T`. A field shorthand such as
    `Payload { body }` still reads the name.
    """
    bound = re.search(r"\blet\s+(?:mut\s+)?$", block[:start]) is not None
    if bound or re.match(r"\s*:(?!:)", block[end:]) is not None:
        return True
    name = block[start:end]
    return any(bind == start for bind, _, _, _ in pattern_scopes(block, name))


def read_elsewhere(block: str, position: int, rest: str) -> bool:
    """Whether an extractor use at `position` is a form `error_rejects` reads.

    `rest` is the text after the use and its method chain. The forms are the
    whole value of a `match`, an `if let` or a `while let`, a let-else, a
    move into another name, and a direct argument of a helper that the audit
    can find. Any other use, such as an argument of a macro or a method, or a
    field read, is not read. The audit treats it as strict and fails closed.
    """
    before = block[:position]
    borrow = r"\s*(?:&\s*(?:mut\s+)?)?$"
    scrutinee = r"(?:\bmatch|\b(?:if|while)\s+let\s[^=;{}]*=)" + borrow
    if re.search(scrutinee, before) and rest.lstrip().startswith("{"):
        return True
    if re.search(r"\blet\s[^=;{}]*=" + borrow, before) and re.match(r"\s*(?:;|else\b)", rest):
        return True
    depth = 0
    for index in range(position - 1, -1, -1):
        depth += before[index] in ")]}"
        depth -= before[index] in "([{"
        if depth < 0:
            if before[index] != "(":
                return False
            call = re.search(FREE_CALL_HEAD + r"\s*$", before[:index])
            if call is None or call.group(1) in NOT_CALLS:
                return False
            return function_parts(SOURCE[0], call.group(1), site_in(block, index)) is not None
        if depth == 0 and before[index] in ",;":
            if before[index] == ";":
                return False
    return False


def yields_block_value(before: str, rest: str) -> bool:
    """Whether an expression is the value of its function block.

    The expression is the tail of the block, or the operand of `return`.
    `before` is the text before the expression, and `rest` the text after it.
    A tail inside a nested block, such as an `if` arm, is not read.
    """
    tail = rest.strip() == "}"
    returned = re.search(r"\breturn\s*$", before) is not None and re.match(r"\s*[;}]", rest)
    return tail or bool(returned)


def error_exits_before(block: str, variable: str, read: str, position: int) -> bool:
    """Whether an earlier `if` sends the extractor error to a success exit.

    The `if` tests `is_err()`, `!is_ok()` or `let Err(..)`, and its block
    returns a success on every path. A later `?` or `unwrap` then sees only
    `Ok`, so it rejects nothing. The `if` must be in the scope of the use.
    """
    # The rest of the condition must keep the arm entered on every error, as
    # `condition_keeps` reads it.
    tested = r"\bif\s+(?:!\s*%s\s*\.\s*is_ok|%s\s*\.\s*is_err)\s*\(\s*\)([^{]*)\{" % (
        variable,
        variable,
    )
    failed = r"\bif\s+let\s+%sErr\s*\([^=]*=\s*%s\s*\{" % (PATTERN_LEAD, read)
    for test in re.finditer(tested + "|" + failed, block[:position]):
        if test.group(1) is not None and not condition_keeps(test.group(1), True):
            continue
        taken = balanced(block[test.end() - 1 :], "{", "}")
        if test.end() - 1 + len(taken) > position or not same_scope(block, test.start(), position):
            continue
        exits = returns_success(unconditional(taken))
        if exits and not rejecting_exit(taken):
            return True
    return False


def rejecting_exit(block: str) -> bool:
    """Whether a fallback block rejects the request.

    It rejects when it builds an error, panics, or returns anything other
    than a success, as `success_value` reads it. A 4xx or 5xx `StatusCode` is an error
    token, so it rejects. It also rejects when its value is a call to a helper
    that can build a rejection, as `builds_rejection` reads it.
    """
    if re.search(ERROR_TOKENS + r"|\b(?:panic|unreachable|todo)!", block):
        return True
    for returned in re.findall(r"\breturn\b\s*([^;}]*)", block):
        if not success_value(returned):
            return True
    return builds_rejection(closure_value(block))


def arm_rejects(arm: str, bound: str | None) -> bool:
    """Whether an arm that receives an extractor error rejects the request.

    It rejects as `rejecting_arm` reads it. It also rejects when it exits as
    `rejecting_exit` reads it, or when its value is a call to a free function.
    A call that hands the bound error to a helper is the one exception, since
    such a helper can recover the request, as the start route does (#808).
    The exception covers that call only. Any other exit of the arm that
    rejects still makes the arm reject. A type path such as
    `Gadget::default()` builds a value.
    """
    return exits_reject(arm, bound, closure=False)


def exits_reject(body: str, bound: str | None, closure: bool) -> bool:
    """Whether a block that receives an error rejects through one of its exits.

    `body` is an `Err` arm, or a fallback closure when `closure` is true. It
    rejects as `rejecting_arm` reads it, or when it panics. Its exits are each
    `return` and its tail value. A `return` in an arm leaves the handler, so
    it must be a success, as `success_value` reads it. A `return` in a closure
    gives the closure value, so it is read like a tail. A tail rejects when
    `builds_rejection` reads it as one. A call that only observes the error,
    such as `observe(error);`, is no exit, so it decides nothing. In an arm,
    an exit that hands the error to a helper can recover the request, as the
    start route does (#808). In a closure, the helper's return type decides.
    """
    if rejecting_arm(body, bound) or re.search(r"\b(?:panic|unreachable|todo)!", body):
        return True
    handed = None
    if bound is not None and bound != "_" and not closure:
        handed = r"(?:return\s+)?[A-Za-z_][\w:]*\s*\([^;]*\b%s\b" % re.escape(bound)
    for returned in re.findall(r"\breturn\b\s*([^;}]*)", body):
        if handed and re.match(handed, returned.strip()):
            continue
        if builds_rejection(returned.strip()) if closure else not success_value(returned):
            return True
    value = closure_value(body)
    return not (handed and re.match(handed, value)) and builds_rejection(value)


def rejecting_arm(arm: str, bound: str | None) -> bool:
    """Whether an `Err` arm rejects the request.

    It rejects when it builds an error, or when it returns or converts the
    rejection it binds, such as `rejection.into_response()`. Passing the
    rejection to a helper, as the start route does to replay a key, is not
    rejection.
    """
    if re.search(ERROR_TOKENS, arm):
        return True
    if bound is None or bound == "_":
        return False
    name = re.escape(bound)
    returned = r"\breturn\s+(?:Err\(\s*)?%s\b(?!\s*[,)])" % name
    converted = r"\b%s\s*\.\s*(?:into_response|into)\s*\(\s*\)" % name
    return re.search(returned + "|" + converted, arm) is not None


# What a pattern can carry in front of a variant: `&`, `&mut`, `ref`,
# `ref mut` and grouping parentheses. `pattern_alternatives` strips it, and
# every `let`, `if let` and `while let` reader allows it before `Ok(` or `Err(`.
PATTERN_LEAD = r"(?:&\s*(?:mut\s+)?|\(\s*)*"


def pattern_alternatives(text: str, start: int, end: int) -> list[int]:
    """Where each alternative of the pattern `text[start:end]` begins.

    A leading `&`, `&mut`, `ref` or `ref mut` is skipped. Grouping parentheses
    are opened, and a top-level `|` splits alternatives, at any depth of
    grouping. A guard such as `if cond` is not part of the pattern.
    """
    lead = re.match(r"(?:\s|&|\bmut\b|\bref\b)*", text[start:end])
    start += lead.end()
    depth = 0
    for index in range(start, end):
        depth += text[index] in "([{"
        depth -= text[index] in ")]}"
        if depth == 0 and re.match(r"\bif\b", text[index:end]) and not re.match(r"\w", text[index - 1]):
            end = index
            break
    while end > start and text[end - 1].isspace():
        end -= 1
    pieces = [0]
    depth = 0
    for index in range(start, end):
        depth += text[index] in "([{"
        depth -= text[index] in ")]}"
        if depth == 0 and text[index] == "|":
            pieces.append(index + 1 - start)
    if len(pieces) > 1:
        bounds = pieces + [end - start + 1]
        return [
            at
            for first, stop in zip(bounds, bounds[1:])
            for at in pattern_alternatives(text, start + first, start + stop - 1)
        ]
    if start < end and text[start] == "(" and len(balanced(text[start:end])) == end - start:
        return pattern_alternatives(text, start + 1, end - 1)
    return [start]


def error_arms(arms: str, kind: str = "result") -> list[re.Match]:
    """Each top-level arm pattern of this `match` that can receive the failure.

    The failure is `Err` for a `Result` and `None` for an `Option`, as
    `FAILURE_VARIANTS` gives it for `kind`.

    That is an `Err(..)` pattern, a catch-all `_` or binding, or a binding
    such as `error @ Err(_)`. `pattern_alternatives` reads each arm, so a
    borrowed `&Err(ref e)`, a grouped `(Err(e))` and an `Ok(_) | Err(e)`
    alternative all count. A guard is not evaluated, so a rejecting arm
    counts even when a guard limits it to some errors. Group 1 is the name the
    arm binds, if any. `arms` includes the outer braces. A guarded arm can
    fall through to a later arm, so every one is returned.
    """
    _, failure_name, failure_pattern = FAILURE_VARIANTS[kind]
    catch_all = re.compile(
        BINDING
        + r"([a-z_][a-z_0-9]*)(?=\s*(?:if\b[^{}]*?)?=>|\s*@\s*(?:%s\b|_))" % failure_name
    )
    failure = re.compile(failure_pattern)
    found: list[re.Match] = []
    depth, arm_start = 0, None
    for index, char in enumerate(arms):
        if char in "([{":
            depth += 1
            if depth == 1:
                arm_start = index + 1
        elif char in ")]}":
            depth -= 1
            if depth == 1 and char == "}":
                arm_start = index + 1
        elif depth == 1 and char == ",":
            arm_start = index + 1
        elif depth == 1 and arms.startswith("=>", index) and arm_start is not None:
            for at in pattern_alternatives(arms, arm_start, index):
                pattern = failure.match(arms, at) or catch_all.match(arms, at)
                if pattern:
                    found.append(pattern)
            arm_start = None
    return found


def chain_state(after: str) -> str:
    """What a method chain on a `Result` does with its error.

    The chain moves from a `Result` to an `Option` through `.ok()`, back
    through `.ok_or(..)` or `.ok_or_else(..)`, and to a plain value through an
    `.unwrap_or*` call. A `?`, `.unwrap()` or `.expect(..)` on a `Result` or an
    `Option` stops the handler, so the chain is `"reject"`. An inspection such
    as `.is_ok()` also yields a plain value. An `.or(..)` or `.or_else(..)`
    whose fallback always returns `Ok(..)` recovers the `Result`, so a later
    `?` cannot reject. A chain that ends on an `Option`, a value or a recovered
    `Result` is `"tolerate"`. One that ends on a `Result` is
    `"open"`, since the code after it decides. Other methods carry the error
    on.
    """
    return walk_chain(after)[0]


# Inspections that turn a `Result` or an `Option` into a boolean.
INSPECTIONS = frozenset({"is_ok", "is_err", "is_ok_and", "is_err_and", "is_some", "is_none"})


def walk_chain(after: str, start: str = "result") -> tuple[str, str | None, str]:
    """`(verdict, last inspection, text after the chain)` for `chain_state`.

    `start` is `"result"` for a `Result` extractor or `"option"` for an
    `Option` one. For an `Option`, the chain holds the failure while it is
    the `Option`, or a `Result` that `ok_or(..)` made from it.
    """
    state, inspection = start, None
    holding = {"result"} if start == "result" else {"option", "result"}
    rest = after
    while True:
        rest = rest.lstrip()
        # `.await` passes the `Result` of an async call through unchanged.
        awaited = re.match(r"\.\s*await\b", rest)
        if awaited:
            rest = rest[awaited.end() :]
            continue
        # A `Result` that `or_else` has recovered holds no error, so `?` on it
        # cannot reject.
        if rest.startswith("?") and state == "recovered":
            state, rest = "value", rest[1:]
            continue
        if rest.startswith("?"):
            return "reject", inspection, rest
        call = re.match(r"\.\s*([a-z_][a-z_0-9]*)\s*\(", rest)
        if call is None:
            return ("open" if state in holding else "tolerate"), inspection, rest
        method = call.group(1)
        if method in ("unwrap", "expect") and state not in ("value", "recovered"):
            return "reject", inspection, rest
        if method in ("or", "or_else") and state == "result":
            if always_ok(balanced(rest[call.end() - 1 :])):
                state = "recovered"
        if method == "ok" and state == "result":
            state = "option"
        elif method in ("ok_or", "ok_or_else") and state == "option":
            state = "result"
        elif method in INSPECTIONS:
            state, inspection = "value", method
        elif method in (
            "unwrap_or",
            "unwrap_or_default",
            "unwrap_or_else",
            "map_or",
            "map_or_else",
        ):
            arguments = balanced(rest[call.end() - 1 :])
            if state in ("result", "option") and fallback_rejects(method, arguments, state):
                return "reject", inspection, rest
            state = "value"
        opener = call.end() - 1
        rest = rest[opener + len(balanced(rest[opener:])) :]


def fallback_rejects(method: str, arguments: str, state: str = "result") -> bool:
    """Whether the fallback of `unwrap_or*` or `map_or*` rejects.

    `state` is `"result"` or `"option"`. The fallback is the first argument. It
    rejects when it builds an error or panics. A closure is read by its exits,
    as `exits_reject` reads it, so a call that only observes the error decides
    nothing. On a `Result`, a function path given to `unwrap_or_else` or `map_or_else`
    receives the error, so it rejects. On an `Option`, the path's return type
    decides.
    """
    items = split_top_level(arguments[1:-1])
    fallback = items[0] if items else ""
    if re.search(ERROR_TOKENS + r"|\b(?:panic|unreachable|todo)!", fallback):
        return True
    closure = re.match(r"(?:move\s+)?\|\s*([a-z_][a-z_0-9]*)?[^|]*\|(.*)", fallback, re.S)
    if closure is None:
        if builds_rejection(fallback):
            return True
        if not method.endswith("_else") or re.fullmatch(r"[A-Za-z_][\w:]*", fallback) is None:
            return False
        # On an `Option` the path receives no error, so its return type decides.
        return state == "result" or builds_rejection(fallback + "(")
    # The closure is read by its exits, as `exits_reject` reads an `Err` arm.
    return exits_reject(closure.group(2), closure.group(1), closure=True)


def success_value(value: str) -> bool:
    """Whether an exit value is a success, not a rejection.

    A value that `SUCCESS_EXIT` matches is a success. Any other value is a
    success only when it is a helper call that `builds_rejection` rejects as
    a rejection, such as a helper that returns a bare `Json<T>`. Every exit is
    read through this one rule.
    """
    value = value.strip()
    if re.match(SUCCESS_EXIT, value):
        return True
    return re.match(FREE_CALL, value) is not None and not builds_rejection(value)


def returns_success(block: str) -> bool:
    """Whether a `return` in `block` gives a success, as `success_value` reads it."""
    return any(success_value(value) for value in re.findall(r"\breturn\b\s*([^;}]*)", block))


def builds_rejection(value: str) -> bool:
    """Whether an expression is a call to a helper that can build a rejection.

    The helper's return type decides. A plain value type such as `Gadget` is
    no rejection. A response, error or `Result` type is one. A helper the
    audit cannot find counts as one, so the audit fails closed.
    """
    call = re.match(FREE_CALL, value)
    if call is None:
        return False
    parts = function_parts(SOURCE[0], call.group(1), site_in(value))
    return parts is None or re.search(RESPONSE_TYPE, parts[1]) is not None


def closure_value(body: str) -> str:
    """The expression a closure body yields: the body, or the tail of its block."""
    body = body.strip()
    if not body.startswith("{"):
        return body
    inner = balanced(body, "{", "}")[1:-1]
    depth, tail = 0, 0
    for index, char in enumerate(inner):
        depth += char in "([{"
        depth -= char in ")]}"
        if char == ";" and depth == 0:
            tail = index + 1
    return inner[tail:].strip()


def always_ok(arguments: str) -> bool:
    """Whether the fallback of `or` or `or_else` yields `Ok(..)` on every path.

    The fallback is `Ok(..)` itself, or a closure whose value is `Ok(..)`. A
    branch, a `return`, a `?`, a panic or an error token in it may fail, so it
    does not count.
    """
    fallback = arguments[1:-1].strip()
    closure = re.match(r"(?:move\s+)?\|[^|]*\|(.*)", fallback, re.S)
    value = closure_value(closure.group(1)) if closure else fallback
    ok = re.match(r"Ok\s*(?:::<[^()]*>\s*)?\(", value)
    if ok is None or len(balanced(value[ok.end() - 1 :])) != len(value) - ok.end() + 1:
        return False
    risky = ERROR_TOKENS + r"|\b(?:if|match|return|panic!|unreachable!|todo!)|\?"
    return re.search(risky, fallback) is None


def inspection_rejects(before: str, inspection: str, rest: str) -> bool:
    """Whether an `if` on an inspected parse sends a failed parse to a rejection.

    `if parse.is_err() { .. }` runs its block on failure. `if parse.is_ok()`
    runs its `else` on failure. A leading `!` swaps the two. The parse is
    mandatory when that failure arm rejects, as `rejecting_exit` reads it.

    A term true on every failure keeps a `||` condition true. A term false on
    every failure keeps an `&&` condition false, unless a `||` follows. Any
    other join, or `is_err_and`, whose predicate may be false, leaves no arm
    certain.
    """
    condition = re.search(r"\bif\s+(!\s*)?(?:serde_json::)?$", before)
    opener = rest.find("{")
    if condition is None or opener < 0 or inspection == "is_err_and":
        return False
    on_failure = inspection in ("is_err", "is_none")
    if condition.group(1):
        on_failure = not on_failure
    if not condition_keeps(rest[:opener], on_failure):
        return False
    taken = balanced(rest[opener:], "{", "}")
    after_taken = rest[opener + len(taken) :]
    otherwise = None
    if re.match(r"\s*else\s*\{", after_taken):
        otherwise = balanced(after_taken[after_taken.index("{") :], "{", "}")
    failure_arm = taken if on_failure else otherwise
    # With no `else`, a failure falls through past the `if`. When the arm for
    # success always returns, the rest of the scope runs only on failure.
    if failure_arm is None and re.search(r"\breturn\b", unconditional(taken)):
        failure_arm = scope_rest(after_taken)
    return failure_arm is not None and rejecting_exit(failure_arm)


def chain_rejects(after: str) -> bool:
    """Whether a method chain on a `Result` stops the handler on its error."""
    return chain_state(after) == "reject"


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

    A method chain after the call does so when `chain_state` ends it on an
    `Option` or a value, as `.ok()` or `.unwrap_or_default()` does. So does
    `if let Ok(..) =` before it, unless its `else` returns or builds an error.
    A `match` on the parse tolerates it when it has an `Err` or catch-all arm
    and no such arm rejects.
    """
    verdict, inspection, rest = walk_chain(after)
    if verdict == "tolerate":
        return not (inspection and inspection_rejects(before, inspection, rest))
    arms = re.match(r"\s*\{", after)
    if arms and re.search(r"\bmatch\s+(?:serde_json::)?$", before):
        arms = balanced(after[arms.end() - 1 :], "{", "}")
        failures = error_arms(arms)
        # Unlike `arm_rejects`, a helper handed the parse error rejects here.
        # The #808 replay exception covers only a `Result` extractor.
        return bool(failures) and not any(
            rejecting_exit(arm)
            or rejecting_arm(arm, failure.group(1))
            or builds_rejection(closure_value(arm))
            for failure in failures
            for arm in [match_arm(arms, failure.start())]
        )
    # A standalone `let Ok(..) = parse else { .. }` tolerates the error when its
    # fallback lets the request through. An `if let` has a block, not `else`,
    # right after the call, so it never matches here.
    bound = r"\blet\s+Ok\s*\([^()]*\)\s*=\s*(?:serde_json::)?$"
    fallback = re.match(r"\s*else\s*\{", after)
    if fallback and re.search(bound, before):
        block = balanced(after[fallback.end() - 1 :], "{", "}")
        return not rejecting_exit(block)
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
    return not rejecting_exit(otherwise)


def same_scope(block: str, start: int, position: int) -> bool:
    """Whether the block that holds `start` is still open at `position`.

    An `if` nested inside another conditional closes with it, so a parse after
    that conditional runs on paths the `if` never saw.
    """
    depth = 0
    for char in block[start:position]:
        depth += char == "{"
        depth -= char == "}"
        if depth < 0:
            return False
    return True


def unconditional(block: str) -> str:
    """The top-level statements of a block, without any nested block.

    A `return` inside a nested `if`, `match` or closure does not run on every
    path, so only a top-level one exits the block for certain.
    """
    inner = block[1:-1] if block.startswith("{") else block
    while "{" in inner:
        opener = inner.index("{")
        nested = balanced(inner[opener:], "{", "}")
        inner = inner[:opener] + inner[opener + len(nested) :]
    return inner


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
        return turbofish
    if not ends_expression(after):
        return None
    statement = before[before.rfind(";") + 1 :]
    binding = re.search(r"\blet\s+(?:mut\s+)?[a-z_0-9]+\s*:\s*([A-Za-z0-9_:]+)\s*=", statement)
    if binding:
        return binding.group(1)
    if re.search(r"\blet\b", statement):
        return None
    result = re.search(r"->\s*Result<\s*([A-Za-z0-9_:]+)\s*,", returns)
    return result.group(1) if result else None


def struct_fields(struct: str) -> list[tuple[str, str, bool, tuple[str, ...]]]:
    """`(name, type, mandatory, spellings)` for each field serde reads.

    `name` is the wire name, after a serde `rename = ".."`. `spellings` holds
    that name, then each serde `alias`. A container `#[serde(default)]` makes
    every field optional. The serde items come from `struct_layout`, so only
    keys are compared.
    """
    container, layout = struct_layout(struct)
    all_default = any(key == "default" for key, _, _ in container)
    fields: list[tuple[str, str, bool, tuple[str, ...]]] = []
    for name, declared_type, items, _ in layout:
        keys = {key for key, _, _ in items}
        if keys & {"skip", "skip_deserializing"}:
            continue
        optional = all_default or "default" in keys or declared_type.startswith("Option<")
        renamed = [value for key, form, value in items if key == "rename" and form == "="]
        wire = renamed[0] if renamed and renamed[0] is not None else name
        aliases = [value for key, _, value in items if key == "alias" and value is not None]
        fields.append((wire, declared_type, not optional, (wire, *aliases)))
    return fields


def wire_type(declared_type: str) -> str | None:
    """The OpenAPI type of a Rust field type, unwrapping one `Option`."""
    inner = re.fullmatch(r"Option<\s*(.+?)\s*>", declared_type)
    return WIRE_TYPES.get(inner.group(1) if inner else declared_type)


def duplicate_params(method: str, path: str, route: dict) -> list[str]:
    """Check 6: each parameter's `(name, in)` pair is documented once.

    OpenAPI requires the pair to be unique. A second entry would otherwise
    collapse into the first before any check reads it, so it is reported.
    """
    counts: dict[tuple[str, str], int] = {}
    for entry in route.get("params") or []:
        key = (entry.get("name"), entry.get("in"))
        counts[key] = counts.get(key, 0) + 1
    return [
        "  %s %s: `%s` is documented %d times in %s" % (method, path, name, count, where)
        for (name, where), count in sorted(counts.items(), key=str)
        if count > 1
    ]


# The trusted paths of the extractors that read the raw query string or the
# whole request, so the keys they accept are not typed. `Query<T>` is not one.
RAW_READER_PATHS = frozenset(
    {
        "axum::extract::RawQuery", "axum::http::Uri", "axum::extract::OriginalUri",
        "axum::extract::Request", "axum::http::Request", "axum::http::request::Parts",
    }
)

# The bare names of those extractors, which a local item can shadow.
RAW_READER_NAMES = frozenset(path.rsplit("::", 1)[-1] for path in RAW_READER_PATHS)


def raw_reader(written: str, site: Site) -> bool:
    """Whether a parameter type is a trusted raw query reader at its top level.

    The head of the type goes through `written_path`, so an import decides.
    It counts when it expands to one of `RAW_READER_PATHS`, after the
    `autumn_web::reexports::` prefix is cut. A bare name with no import
    counts, as a bare extractor name does, unless a local item shadows it or
    an untrusted glob may bring it, as `glob_unknowns` reads it.
    A reader nested in a generic argument, as in `State<Request>`, or one
    from an untrusted path, does not count.
    """
    text = re.sub(r"^(?:&\s*(?:'[a-z_]+\s+)?(?:mut\s+)?)*", "", written.strip())
    head = re.match(r"(?:::\s*)?[A-Za-z_]\w*(?:\s*::\s*[A-Za-z_]\w*)*", text)
    if head is None:
        return False
    segments, used_at = written_path(head.group(0), site)
    path = re.sub(r"^autumn_web::reexports::", "", "::".join(segments))
    if len(segments) == 1:
        # An untrusted glob that may bring this name makes it unknown.
        text, at = site
        hidden = any(
            start <= at < end and name == path for start, end, name in glob_unknowns(text)
        )
        return path in RAW_READER_NAMES and not hidden
    return path in RAW_READER_PATHS


def query_params(route: dict) -> dict[str, dict]:
    """The route's documented `in: query` parameters, by name.

    Checks 4 and 6 read query keys only through this, so a path or header
    parameter of the same name never documents a query key.
    """
    return {
        entry.get("name"): entry
        for entry in route.get("params") or []
        if entry.get("in") == "query" and entry.get("name")
    }


def query_struct_findings(
    method: str, path: str, route: dict, queries: list[tuple[str, str, bool]]
) -> list[str]:
    """Check 6: the `Query<T>` structs and the route's query parameters agree.

    This is the forward check. It reads each field of each extractor.
    `documented_query_findings` is the inverse check, gated on its own. Each
    extractor reads the whole query string, so a key that several structs
    accept is judged once for the route. It is mandatory when any extractor
    that is not wrapped requires it. Its OpenAPI type must be the same in every
    struct, or the key is a finding.
    """
    documented = query_params(route)
    where = "  %s %s: `%%s`" % (method, path)
    found: list[str] = []
    # (struct, field, declared type, mandatory) for each documented key.
    readers: dict[str, list[tuple[str, str, str, bool]]] = {}
    for name, struct, wrapped in queries:
        for field, declared_type, mandatory, spellings in struct_fields(struct):
            key = documented_as(spellings, documented)
            if documented.get(key) is None:
                found.append(
                    where % field
                    + " is accepted by %s but the contract does not document it" % name
                )
                continue
            readers.setdefault(key, []).append(
                (name, field, declared_type, mandatory and not wrapped)
            )
    for key, fields in readers.items():
        entry = documented[key]
        kinds = {wire_type(declared_type) for _, _, declared_type, _ in fields}
        for name, field, declared_type, _ in fields:
            if wire_type(declared_type) is None:
                found.append(
                    where % field + " has type %s, which maps to no OpenAPI type" % declared_type
                )
        kinds.discard(None)
        if len(kinds) > 1:
            found.append(
                where % key
                + " has conflicting types: "
                + ", ".join(
                    "%s in %s" % (wire_type(declared_type), name)
                    for name, _, declared_type, _ in fields
                )
            )
        elif kinds and entry.get("type") not in kinds:
            name, field, declared_type, _ = fields[0]
            found.append(
                where % field
                + " is %s in %s but the contract says %s"
                % (wire_type(declared_type), name, entry.get("type"))
            )
        strict = [(name, field) for name, field, _, mandatory in fields if mandatory]
        if strict and entry.get("required") is not True:
            name, field = strict[0]
            found.append(
                where % field + " is mandatory in %s but the contract marks it optional" % name
            )
        if not strict and entry.get("required") is True:
            field = fields[0][1]
            owners = " or ".join(sorted({reader for reader, _, _, _ in fields}))
            found.append(
                where % field + " is optional in %s but the contract marks it required" % owners
            )
    return found


def documented_query_findings(
    method: str, path: str, route: dict, queries: list[tuple[str, str, bool]]
) -> list[str]:
    """Check 6, inverse: each documented query key is one an extractor accepts.

    It runs for every served route, so a route with no `Query<T>` accepts no
    documented query key. A route whose query the audit cannot read whole
    skips it, as `audit` gates it.
    """
    documented = set(query_params(route))
    accepted = {
        spelling
        for _, struct, _ in queries
        for _, _, _, spellings in struct_fields(struct)
        for spelling in spellings
    }
    owners = " or ".join(name for name, _, _ in queries)
    readers = "%s does not accept it" % owners if owners else "no extractor of the route accepts it"
    where = "  %s %s: `%%s`" % (method, path)
    return [where % key + " is documented but " + readers for key in documented - accepted]


def declared_statuses(route: dict) -> set[int]:
    statuses = {route["success_response"]["status"]}
    statuses |= {entry["status"] for entry in route.get("additional_responses", [])}
    statuses |= {entry["status"] for entry in route.get("error_responses", [])}
    return statuses


# A path through `std` or `core` to `Result` or `Option`, and the type before
# a variant, such as `std::result::Result::Err`. Only spaces and tabs are
# matched, so a rename keeps every line.
# Each standard type the audit reads by name: the crate roots and the module
# that define it. `canonical_paths` cuts every such path to the bare name.
STD_TYPES: dict[str, tuple[tuple[str, ...], str]] = {
    "Option": (("std", "core"), "option"),
    "Result": (("std", "core"), "result"),
    "Vec": (("std", "alloc"), "vec"),
    "String": (("std", "alloc"), "string"),
    "Box": (("std", "alloc"), "boxed"),
    **{
        name: (("std", "core"), "primitive")
        for name in (
            "i8 i16 i32 i64 i128 isize u8 u16 u32 u64 u128 usize f32 f64 bool char".split()
        )
    },
}

# A standard path to one of `STD_TYPES`, up to its bare name.
PRELUDE_PATH = re.compile(
    "|".join(
        r"(?<![\w:])(?:::[ \t]*)?(?:%s)[ \t]*::[ \t]*%s[ \t]*::[ \t]*(?=%s\b)"
        % ("|".join(roots), module, "|".join(names))
        for (roots, module), names in sorted(
            {
                (roots, module): [n for n, spec in STD_TYPES.items() if spec == (roots, module)]
                for roots, module in STD_TYPES.values()
            }.items()
        )
    )
)
VARIANT_PATH = re.compile(r"\b(?:Result|Option)[ \t]*::[ \t]*(?=(?:Ok|Err|Some|None)\b)")


def canonical_paths(text: str) -> str:
    """`text` with each standard path to a type in `STD_TYPES`, or a variant, cut.

    `std::option::Option<T>` is `Option<T>`, `alloc::vec::Vec<u8>` is
    `Vec<u8>`, `core::primitive::u32` is `u32`, and
    `std::result::Result::Err(e)` is `Err(e)`. Every check then reads the
    short name only, so no check needs its own rule for a path. A standard
    path always names the real type. Only a bare name can be shadowed, and
    `alias_declarations` binds those before this cut runs.
    """
    return VARIANT_PATH.sub("", PRELUDE_PATH.sub("", text))


# The names an alias can stand for, and the text the audit reads for each.
ALIAS_TARGETS = {
    "Query": "Query",
    "Json": "Json",
    "Bytes": "Bytes",
    "from_slice": "serde_json::from_slice",
}


# The import paths the audit trusts for each aliased kind. An alias of a
# same-named item from any other path, such as `crate::signed::Query`, is not
# rewritten, so it is no extractor.
SUPPORTED_PATHS = {
    "axum::extract::Query": "Query",
    "axum::Json": "Json",
    "axum::extract::Json": "Json",
    "bytes::Bytes": "Bytes",
    "axum::body::Bytes": "Bytes",
    "serde_json::from_slice": "from_slice",
}


def extractor_kind(path: str) -> str | None:
    """The supported kind a path names, or `None`.

    A bare `Query`, `Json` or `Bytes` names the one in scope, which the `use`
    aliases have already made canonical. A qualified path counts only when
    `SUPPORTED_PATHS` trusts it, after the `autumn_web::reexports::` prefix
    is cut. Both the `use` aliases and the `type` aliases ask this.
    """
    path = re.sub(r"\s", "", path).lstrip(":")
    path = re.sub(r"^autumn_web::reexports::", "", path)
    if "::" not in path:
        return path if path in ("Query", "Json", "Bytes") else None
    return SUPPORTED_PATHS.get(path)


def canonical_extractor_paths(source: str) -> str:
    """`source` with each trusted qualified extractor path cut to its bare name.

    The first name of each path goes through the scoped `use` bindings first,
    as `written_path` reads them. The expanded path is then trusted or not,
    as `extractor_kind` reads it. This is the one path-trust decision.
    `axum::Json<T>` becomes `Json<T>`, and so does
    `autumn_web::reexports::axum::Json`. A trusted `from_slice` path becomes
    `serde_json::from_slice`. A path it does not trust, such as
    `crate::signed::Query<T>`, is left whole. If an import rebinds its
    first name, the path is spelled out in full, so `axum::Json` under `use
    crate::signed as axum;` reads as `crate::signed::Json`. Every extractor
    reader matches only a bare name, so that type is no extractor. A `use` statement
    keeps its paths, since the alias layer reads them. Comments and literals
    are left alone, and newlines stay.
    """
    code = code_only(source)
    uses = [found.span() for found in re.finditer(r"\buse\b[^;]*;", code)]
    qualified = r"(?<![\w:])(?:::\s*)?(?:[A-Za-z_]\w*\s*::\s*)+(Query|Json|Bytes|from_slice)\b"
    for found in reversed(list(re.finditer(qualified, code))):
        if any(start <= found.start() < end for start, end in uses):
            continue
        written = found.group(0)
        segments, used_at = written_path(written, (code, found.start()))
        kind = extractor_kind("::".join(segments))
        if kind is not None:
            trusted = ALIAS_TARGETS[kind]
        elif used_at is not None:
            # A rebound first name, as in `use crate::signed as axum;`, names
            # that module. The path is spelled out, so no reader trusts it.
            trusted = "::".join(segments)
        else:
            continue
        source = source[: found.start()] + trusted + source[found.end() :]
    return source


def use_leaves(statement: str) -> list[tuple[str, str | None]]:
    """`(path, alias)` for each leaf of a `use` tree.

    A group such as `serde_json::{self as json, Value}` or
    `axum::{extract::{Query as Q}}` is read at any depth. A `self` leaf names
    its group's path. The prefix `autumn_web::reexports::` is cut, since that
    crate re-exports axum unchanged.
    """
    tree = re.sub(r"^\s*(?:pub(?:\([^)]*\))?\s+)?use\s+", "", statement).rstrip(";").strip()

    def expand(prefix: str, text: str) -> list[tuple[str, str | None]]:
        text = text.strip()
        group = re.fullmatch(r"((?:::)?(?:[\w]+\s*::\s*)*)\{(.*)\}", text, re.S)
        if group:
            base = prefix + re.sub(r"\s", "", group.group(1))
            return [leaf for item in split_expression(group.group(2), ",") for leaf in expand(base, item)]
        named = re.fullmatch(r"(.*?)\s+as\s+([A-Za-z_]\w*)", text, re.S)
        path, alias = (named.group(1), named.group(2)) if named else (text, None)
        path = re.sub(r"\s", "", prefix + path)
        path = re.sub(r"::self$", "", path).lstrip(":")
        return [(re.sub(r"^autumn_web::reexports::", "", path), alias)]

    return expand("", tree)


@functools.lru_cache(maxsize=None)
def alias_scope(code: str, position: int) -> tuple[int, int]:
    """Where a declaration at `position` holds: the file at depth 0, else its block."""
    head = code[:position]
    if head.count("{") == head.count("}"):
        return 0, len(code)
    return enclosing_block(code, position)


def canonical_type(declared_type: str, site: Site | None = None) -> tuple[str, str | None]:
    """A type spelling in its canonical form, and the alias it cannot read, if any.

    Every type the audit reads goes through this one pipeline: handler and
    helper parameters, receiver and `let` types, and struct fields. At the
    type's `site` it applies, in order:

    1. prelude and extractor shadowing, as `shadow_bound` reads it;
    2. scoped `use` expansion of standard items, as `std_imports` reads it;
    3. type aliases, to a fixed point, as `resolve_field_type` reads them;
    4. the `STD_TYPES` cut of `canonical_paths`.

    Every reader then matches only the canonical text.
    """
    text, at = site or (SOURCE[0], None)
    return canonical_type_at(declared_type.strip(), text, at)


@functools.lru_cache(maxsize=None)
def canonical_type_at(declared_type: str, text: str, at: int | None) -> tuple[str, str | None]:
    """`canonical_type` for a site given as its text and position, found once."""
    site = (text, at)
    declared_type = std_imports(shadow_bound(declared_type, site), site)
    # A reference or a lifetime before the type stays, and the alias pass
    # reads the type after it.
    lead = re.match(r"(?:&\s*(?:'[a-z_]+\s+)?(?:mut\s+)?)?", declared_type).group(0)
    resolved, unread = resolve_field_type(declared_type[len(lead) :], site)
    return canonical_paths(lead + resolved), unread


# Paths of types from outside the standard library that the audit reads by
# path. A type counts as one only when its spelling resolves to the path.
TRUSTED_TYPE_PATHS = frozenset({"uuid::Uuid"})


def std_imports(declared_type: str, site: Site) -> str:
    """A type with each name that a `use` binds to a standard or trusted item spelled out.

    `use std::option::Option as Maybe;` makes `Maybe<T>` read as
    `std::option::Option<T>`, which the `STD_TYPES` cut then shortens.
    `use uuid::Uuid;` makes `Uuid` read as `uuid::Uuid`, one of
    `TRUSTED_TYPE_PATHS`. A written trusted path whose first name an import
    rebinds, as under `use crate::fake as uuid;`, is spelled out too, so it
    no longer reads as trusted. `written_path` reads each import. A name
    bound to any other item is left as written, since `resolve_symbol`
    reads its import itself. A bare `Uuid` with no import stays bare and is
    no wire type, so the audit fails closed on it.
    """
    text, at = site
    if at is None:
        return declared_type
    names = r"(?<![\w:])(?:::\s*)?[A-Za-z_]\w*(?:\s*::\s*[A-Za-z_]\w*)*"
    for found in reversed(list(re.finditer(names, declared_type))):
        segments, used_at = written_path(found.group(0), site)
        # The audited source has its standard paths cut already, `use`
        # statements too, so a bare standard name there is one as well.
        cut = len(segments) == 1 and segments[0] in STD_TYPES
        trusted = "::".join(segments) in TRUSTED_TYPE_PATHS
        rebound = re.sub(r"\s", "", found.group(0)) in TRUSTED_TYPE_PATHS
        standard = bool(segments) and segments[0] in STD_ROOTS
        if used_at is not None and (standard or cut or trusted or rebound):
            spelled = "::".join(segments)
            declared_type = declared_type[: found.start()] + spelled + declared_type[found.end() :]
    return declared_type


def shadow_bound(declared_type: str, site: Site) -> str:
    """A type with each prelude or extractor name that its site shadows replaced.

    `alias_declarations` binds the names. A name shadowed by a local item or
    an import reads as that item's path, so a shadowed `Option<T>` is no
    `Option`, and the field stays mandatory. The audited source needs no
    pass here, since `resolve_aliases` has already rewritten it whole.
    """
    text, at = site
    if at is None or text == SOURCE[0]:
        return declared_type
    for name in PRELUDE_NAMES + EXTRACTOR_NAMES:
        pattern = r"(?<![\w:.])%s\b" % name
        if not re.search(pattern, declared_type):
            continue
        bound = [
            (start, replacement)
            for (start, end), _, declared, replacement in alias_declarations(text)
            if declared == pattern and start <= at < end and replacement
        ]
        if bound:
            declared_type = re.sub(pattern, max(bound)[1], declared_type)
    return declared_type


@functools.lru_cache(maxsize=None)
def alias_declarations(code: str) -> list[tuple[tuple[int, int], tuple[int, int], str, str | None]]:
    """`(scope, declaration span, pattern, replacement)` for each alias in `code`.

    It reads `use .. as ..` for `Query`, `Json`, `Bytes` and serde_json's
    `from_slice`, a module alias of `serde_json`, a plain or glob import of
    `from_slice`, and `type X<..> = Y<..>;`. An import or a local item that
    binds an extractor's name to something else shadows it in its scope. A
    type alias to one of those types with the same parameters is a rename.
    One with no parameters is replaced by its target. Any other type alias that names an extractor has
    no replacement (`None`), so the audit reports it.
    """
    found = []
    for use in re.finditer(r"\buse\b[^;]*;", code):
        scope, span = alias_scope(code, use.start()), use.span()
        for path, alias in use_leaves(use.group(0)):
            kind = extractor_kind(path) if "::" in path else None
            call = r"(?=\s*(?:::<|\())"
            binding = alias or path.rsplit("::", 1)[-1]
            if binding in EXTRACTOR_NAMES and kind != binding and kind not in EXTRACTOR_NAMES:
                # A plain import of another item with an extractor's name binds
                # that name in scope. It is then that item, not the extractor.
                found.append((scope, span, r"(?<![\w:.])%s\b" % binding, path))
            elif binding in PRELUDE_NAMES and path not in PRELUDE_PATHS:
                # An import of another item with a prelude name hides the
                # prelude type in scope, as an extractor name does.
                found.append((scope, span, r"(?<![\w:.])%s\b" % binding, path))
            elif kind and alias:
                pattern = r"(?<![\w:.])%s\b%s" % (alias, call if kind == "from_slice" else "")
                found.append((scope, span, pattern, ALIAS_TARGETS[kind]))
            elif path == "serde_json" and alias:
                found.append((scope, span, r"(?<![\w:.])%s(?=\s*::)" % alias, "serde_json"))
            elif re.fullmatch(r"(?:std|core|alloc)(?:::[a-z_]\w*)+", path):
                # A standard-library module keeps its root, so `handoff_path`
                # can see that `s::from_utf8` is `std::str::from_utf8`.
                name = alias or path.rsplit("::", 1)[-1]
                found.append((scope, span, r"(?<![\w:.])%s(?=\s*::)" % name, path))
            elif path in ("serde_json::from_slice", "serde_json::*") and not alias:
                pattern = r"(?<![\w:.])from_slice%s" % call
                found.append((scope, span, pattern, ALIAS_TARGETS["from_slice"]))
    # A local item named like an extractor or a prelude type shadows it in its
    # scope, as an untrusted import does. A `type` alias that renames the type
    # to its own name is no shadow.
    shadowable = EXTRACTOR_NAMES + PRELUDE_NAMES + tuple(sorted(RAW_READER_NAMES))
    item = r"\b(struct|enum|trait|union|type)\s+(%s)\b" % "|".join(shadowable)
    for local in re.finditer(item, code):
        if local.group(1) == "type":
            target = re.match(r"\s*(<[^=;]*?>)?\s*=\s*([^;]+);", code[local.end() :])
            renamed = canonical_paths(target.group(2).strip()) if target else ""
            if target and re.fullmatch(r"%s\s*(<.*>)?" % local.group(2), renamed):
                continue
        scope = alias_scope(code, local.start())
        span = (local.start(), local.end())
        found.append((scope, span, r"(?<![\w:.])%s\b" % local.group(2), "self::" + local.group(2)))
    for declared in re.finditer(r"\btype\s+([A-Z]\w*)\s*(<[^=;]*?>)?\s*=\s*([^;]+);", code):
        name, parameters, target = declared.group(1), declared.group(2) or "", declared.group(3).strip()
        scope, span = alias_scope(code, declared.start()), declared.span()
        plain = re.fullmatch(r"((?:[A-Za-z_]\w*\s*::\s*)*(?:Query|Json|Bytes))\s*(<.*>)?", target)
        kind = extractor_kind(plain.group(1)) if plain else None
        # Only a supported extractor counts. A same-named type from another
        # path, such as `crate::signed::Query`, is left alone.
        named = [
            hit
            for hit in re.finditer(r"((?:[A-Za-z_]\w*\s*::\s*)*)\b(Query|Json|Bytes)\b", target)
            if extractor_kind(hit.group(0))
        ]
        if kind and re.sub(r"\s", "", parameters) == re.sub(r"\s", "", plain.group(2) or ""):
            found.append((scope, span, r"(?<![\w:])%s\b" % name, kind))
        elif kind and not parameters:
            target = kind + (plain.group(2) or "")
            found.append((scope, span, r"(?<![\w:])%s\b(?!\s*<)" % name, target))
        elif named:
            found.append((scope, span, name, None))
    return found


# The extractor names an import can bind.
EXTRACTOR_NAMES = ("Query", "Json", "Bytes")

# The prelude type names that the audit reads by name.
PRELUDE_NAMES = ("Option", "Result", "Vec", "String")

# The standard paths of the prelude types. Importing one binds the prelude type.
PRELUDE_PATHS = frozenset(
    "%s::%s::%s" % (root, module, name)
    for name, (roots, module) in STD_TYPES.items()
    for root in roots
)

# Glob imports that bring the supported extractors, or nothing that clashes.
TRUSTED_GLOBS = frozenset({"axum", "axum::extract", "axum::body", "bytes", "serde_json"})

# Every name the audit trusts by its bare spelling: the extractors, the raw
# query readers and the standard types it reads by name. An untrusted glob
# may bring its own item of any of them, so `glob_unknowns` reads them all.
BARE_TRUSTED_NAMES = tuple(
    sorted(
        set(EXTRACTOR_NAMES)
        | RAW_READER_NAMES
        | {name for name in STD_TYPES if name[:1].isupper()}
    )
)


@functools.lru_cache(maxsize=None)
def glob_unknowns(code: str) -> list[tuple[int, int, str]]:
    """`(scope start, scope end, name)` for each bare-trusted name a glob may hide.

    The names are `BARE_TRUSTED_NAMES`. `glob_module` reads each glob. An
    untrusted one, such as `use crate::models::*;` for a module the audit
    does not scan, may bring its own `Query`, `Request` or `Option`. A glob
    of a scanned module brings such a name only when that module binds it,
    as `module_binds` reads it. In the glob's scope, a bare use of such a
    name cannot be told apart, unless a plain import in the same scope binds
    it, since a plain import wins over a glob.
    """
    bound: dict[tuple[int, int], set[str]] = {}
    globs = [
        (scope, glob_module(code, base, used_at)) for scope, base, used_at in glob_imports(code)
    ]
    for use in re.finditer(r"\buse\b[^;]*;", code):
        scope = alias_scope(code, use.start())
        for path, alias in use_leaves(use.group(0)):
            if not path.endswith("::*"):
                bound.setdefault(scope, set()).add(alias or path.rsplit("::", 1)[-1])
    return [
        (scope[0], scope[1], name)
        for scope, module in globs
        if module != TRUSTED
        for name in BARE_TRUSTED_NAMES
        if name not in bound.get(scope, set())
        and (module is None or module_binds(code, module, name))
    ]


# How many passes `resolve_aliases` makes before it gives up on a chain.
ALIAS_PASSES = 8


def resolve_aliases(source: str) -> tuple[str, list[str]]:
    """`source` with every alias resolved, and the names that did not settle.

    Each pass replaces the aliases that `alias_declarations` reads. A chain
    such as `use Query as Q; type ApiQuery<T> = Q<T>;` needs one pass per
    link, so the passes repeat until the text stops changing. A cycle, or a
    chain longer than `ALIAS_PASSES`, never settles. Its names are returned,
    so the audit reports each one and fails closed.
    """
    for _ in range(ALIAS_PASSES):
        resolved = resolve_aliases_once(source)
        if resolved == source:
            return source, []
        source = resolved
    # Every alias name left in the text may be a link that never settled.
    code = code_only(source)
    declared = re.findall(r"\btype\s+([A-Za-z_]\w*)|\bas\s+([A-Za-z_]\w*)", code)
    return source, sorted({name for pair in declared for name in pair if name})


def value_shadowed(code: str, name: str, position: int, bound_at: int) -> bool:
    """Whether a local value binding hides `name` at `position`.

    A `let` or a pattern that binds it, as `live_binding` reads them, hides
    it. So does a closure parameter or a fn parameter of that name, in the
    closure body or the fn body.
    """
    if not live_binding(code, name, position, bound_at):
        return True
    variable = re.escape(name)
    for closure in re.finditer(r"(?<![|\w])\|([^|]*)\|", code[:position]):
        if re.search(r"(?<![\w.])%s\b" % variable, closure.group(1)):
            _, stop = closure_body_end(code, closure.end())
            if closure.start() < position < stop:
                return True
    for fn in re.finditer(r"\bfn\s+\w+\s*(?:<[^>]*>)?\s*\(", code[:position]):
        params = balanced(code[fn.end() - 1 :])
        if re.search(r"(?<![\w.])%s\s*:" % variable, params):
            brace = code.find("{", fn.end() - 1 + len(params))
            if brace >= 0 and brace < position < brace + len(balanced(code[brace:], "{", "}")):
                return True
    return False


def resolve_aliases_once(source: str) -> str:
    """`source` with each alias in `alias_declarations` replaced in its scope.

    A module-level alias holds in the whole file. One in a block holds only in
    that block, before or after the statement, as in Rust. Names in comments
    and literals are not replaced. Newlines stay, so line numbers still point
    at the source. Every scan reads the result, so no scan keeps its own alias
    rule.
    """
    code = code_only(source)
    edits: dict[int, tuple[int, str]] = {}
    for (start, end), (skip_start, skip_end), pattern, replacement in alias_declarations(code):
        if replacement is None:
            continue
        # A fn alias names a value, so a local binding of the same name, a
        # `let`, a pattern or a closure or fn parameter, stops it in its scope.
        value = replacement == ALIAS_TARGETS["from_slice"]
        for hit in re.finditer(pattern, code[:end]):
            if not start <= hit.start() or skip_start <= hit.start() < skip_end:
                continue
            if value and value_shadowed(code, hit.group(0).strip(), hit.start(), skip_end):
                continue
            edits.setdefault(hit.start(), (hit.end(), replacement))
    for position in sorted(edits, reverse=True):
        stop, replacement = edits[position]
        source = source[:position] + replacement + source[stop:]
    return source


def unreadable_aliases(code: str) -> list[tuple[int, int, str]]:
    """`(scope start, scope end, name)` for each type alias the audit cannot read.

    An alias whose target names an unreadable alias is unreadable too, as in
    `type Inner<T> = Query<Vec<T>>; type Outer<T> = Inner<T>;`. The set grows
    until nothing changes.
    """
    found = [
        (scope[0], scope[1], pattern)
        for scope, _, pattern, replacement in alias_declarations(code)
        if replacement is None
    ]
    declared = [
        (alias_scope(code, hit.start()), hit.group(1), hit.group(2))
        for hit in re.finditer(r"\btype\s+([A-Za-z_]\w*)[^=;]*=\s*([^;]+);", code)
    ]
    while True:
        names = {name for _, _, name in found}
        grown = [
            (scope[0], scope[1], name)
            for scope, name, target in declared
            if name not in names and any(re.search(r"\b%s\b" % re.escape(n), target) for n in names)
        ]
        if not grown:
            return found
        found += grown


def audit(source: str, contract: dict, find_struct) -> dict[str, list[str]]:
    """Every finding, by check. `find_struct` maps a struct name and its site to a block."""
    # A rename keeps every line, so line numbers still point at the source.
    lines = source.split("\n")
    source, unsettled = resolve_aliases(canonical_extractor_paths(source))
    source = canonical_paths(source)
    # Comments and literals are blanked once, at the same length. Every scan
    # reads `code`. Only the route table and the query keys need literals.
    code = masked_source(source)
    unreadable = unreadable_aliases(code) + [(0, len(code), name) for name in unsettled]
    SOURCE[0] = code
    aliases = getattr(find_struct, "aliases", None)
    TYPE_ALIASES[0] = crate_type_aliases() if aliases is None else aliases
    STRUCTS[0] = getattr(find_struct, "index", None)
    LOCAL[:] = getattr(find_struct, "local", ("autumn_harvest_plugin", "api"))
    FILE_PLACES[code] = location(LOCAL[0], LOCAL[1])
    KNOWN_MODULES[0] = None
    globbed = glob_unknowns(code)
    by_route = {(r["method"], r["path"]): r for r in contract["routes"]}
    routes = router_routes(source)

    findings: list[str] = []
    query_findings: list[str] = []
    for method, path, handler in routes:
        body = handler_body(code, handler)
        route = by_route.get((method, path))
        if body is None or route is None:
            continue
        declared = declared_statuses(route)

        helpers = called_helpers(code, body, handler_parameters(code, handler) or "")
        bodies = [body]
        for helper in helpers:
            reached = parts_at(code, helper)
            if reached is not None:
                bodies.append(reached[2])

        params = handler_parameters(code, handler)
        # Every trusted raw reader, as `raw_reader` reads it, parses the
        # query by hand, so its key arms are read. The same gate exempts the
        # route from the documented-key check below.
        if params is not None and getattr(params, "raw_reader", False):
            documented = set(query_params(route))
            # A query key is a string literal, so these blocks keep literals.
            keyed = [handler_body(source, handler)]
            keyed += [parts[2] for parts in map(lambda at: parts_at(source, at), helpers) if parts]
            for reached in filter(None, keyed):
                for arm in key_arms(reached):
                    if set(arm) & documented:
                        continue
                    query_findings.append(
                        "  %s %s: `%s` is accepted by the query parser but the "
                        "contract does not document it" % (method, path, "` / `".join(arm))
                    )

        for reached in bodies:
            offset = code.index(reached)
            for hit in re.finditer(r"(.{0,30})StatusCode::([A-Z_]+)", reached):
                status = NAMED.get(hit.group(2))
                if status is None or status in declared:
                    continue
                if "==" in hit.group(1) or "!=" in hit.group(1):
                    continue
                line = code[: offset + hit.start()].count("\n") + 1
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
                line = code[: offset + hit.start()].count("\n") + 1
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
                line = code[: offset + hit.start()].count("\n") + 1
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
    # Every contract route is checked for duplicate parameters, whether or
    # not the router serves it.
    for route in contract["routes"]:
        typed_query += duplicate_params(route["method"], route["path"], route)
    missing = "  %s %s: cannot find struct %s"
    unread = "  %s %s: cannot read `%s` in %s"
    for method, path, handler in routes:
        params = handler_parameters(code, handler)
        block = handler_body(code, handler) or ""
        route = by_route.get((method, path))
        if params is None or route is None:
            continue
        at = code.find("async fn %s(" % handler)
        written = getattr(params, "written", params)
        for start, end, alias in unreadable:
            if start <= at < end and re.search(r"(?<![\w:])%s\b" % alias, written):
                unresolved.append("  %s %s: cannot read the `%s` type alias" % (method, path, alias))
        for start, end, name in globbed:
            if start <= at < end and re.search(r"(?<![\w:])%s\b" % name, written):
                unresolved.append(
                    "  %s %s: cannot tell which `%s` a glob import brings" % (method, path, name)
                )
        queries: list[tuple[str, str, bool]] = []
        unread_queries = len(unresolved)
        for query in QUERY_EXTRACTOR.finditer(params):
            reference = query.group(1)
            name = reference.split("::")[-1]
            # An absent query string turns `Option<Query<T>>` into `None`, so
            # none of its fields is mandatory.
            # A `Result<Query<T>, _>` whose error the handler tolerates acts the same.
            prefix = params[: query.start()]
            wrapped = re.search(r"%s<\s*(?:[a-z_]+::)*$" % OPTION, prefix) is not None
            result = re.search(
                r"\b([a-z_][a-z_0-9]*)\s*:\s*%s<\s*(?:[a-z_]+::)*$" % RESULT, prefix
            )
            if result and not error_rejects(result.group(1), block):
                wrapped = True
            struct = find_struct(reference, (code, at))
            if struct is None:
                unresolved.append(missing % (method, path, name))
            elif unreadable_serde(struct):
                unresolved += [unread % (method, path, a, name) for a in unreadable_serde(struct)]
            else:
                queries.append((name, struct, wrapped))
        if len(re.findall(r"(?<![\w:])Query<", params)) > len(QUERY_EXTRACTOR.findall(params)):
            unresolved.append("  %s %s: cannot read a `Query<..>` extractor" % (method, path))
        # The forward check reads every `Query<T>` the audit can read. The
        # inverse check runs for every served route, with or without a
        # `Query<T>`, so a documented key that no extractor reads is a finding.
        # A route whose query the audit cannot read whole is left out of the
        # inverse check only: an unresolved `Query<T>`, or a raw reader that
        # check 4 reads.
        unread_queries = len(unresolved) > unread_queries
        typed_query += query_struct_findings(method, path, route, queries)
        if not unread_queries and not getattr(params, "raw_reader", False):
            typed_query += documented_query_findings(method, path, route, queries)

        # A bare `Json<T>` means the body is mandatory. `Result<Json<T>, _>` is
        # mandatory when its error rejects. `Option<Json<T>>` is mandatory when
        # its `None` rejects, and optional otherwise. All
        # three still name the struct whose fields serde accepts, which is what
        # check 3 needs.
        # The binding is `Json(body)` or a plain name such as `mut body`.
        binding = r"(?:%s\(\s*[a-z_0-9]+\s*\)|(?:mut\s+)?[a-z_][a-z_0-9]*)" % JSON
        bare = re.search(r"%s\s*:\s*%s<\s*([A-Za-z0-9_:]+)\s*>" % (binding, JSON), params)
        extractor = (
            bare
            or re.search(r"%s<\s*%s<\s*([A-Za-z0-9_:]+)\s*>" % (RESULT, JSON), params)
            or re.search(r"%s<\s*%s<\s*([A-Za-z0-9_:]+)\s*>\s*>" % (OPTION, JSON), params)
        )
        if len(re.findall(r"(?<![\w:])Json<", params)) > (extractor is not None):
            unresolved.append("  %s %s: cannot read a `Json<..>` extractor" % (method, path))
        # (struct name, whether the body is mandatory, site) for each parse.
        parses: list[tuple[str, bool, Site]] = []
        body_type = extractor.group(1) if extractor else None
        result_rejects = rejects_result_body(params, block)
        option_body = bool(extractor) and extractor.re.pattern.startswith(OPTION)
        if body_type is not None and body_type.split("::")[-1] != "Value":
            # A present `Option<Json<T>>` body is still parsed strictly, so its
            # mandatory fields are checked. A tolerant `Result` body is not.
            parses.append((body_type, bool(bare) or result_rejects or option_body, (code, at)))
        # An `Option` body whose `None` the handler rejects is mandatory too.
        mandatory_body = bool(bare) or result_rejects or rejects_wrapped_body(params, block)
        if byte_parameters(params) or possible_carriers(params) or wrapped_carriers(params):
            for name, optional, tolerant, site in raw_body_parses(code, handler):
                # A parse an empty body cannot skip makes the body mandatory,
                # whatever its type. A parse that does not tolerate its error
                # needs the mandatory fields of any body that is present.
                mandatory_body |= not optional
                if name is None:
                    unresolved.append(
                        "  %s %s: cannot read a `from_slice` call or resolve its "
                        "body type" % (method, path)
                    )
                elif name.split("::")[-1] != "Value":
                    parses.append((name, not tolerant, site))

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
        for reference, mandatory, site in parses:
            name = reference.split("::")[-1]
            struct = find_struct(reference, site)
            if struct is None:
                unresolved.append(missing % (method, path, name))
                continue
            if unreadable_serde(struct):
                unresolved += [unread % (method, path, a, name) for a in unreadable_serde(struct)]
                continue
            free_form = request_body.get("free_form") is True
            for spellings in mandatory_fields(struct) if mandatory and not free_form else []:
                if declared.get(documented_as(spellings, declared)) is not True:
                    body_findings.append(
                        "  %s %s: `%s` is mandatory in %s but the contract does not "
                        "mark it required" % (method, path, spellings[0], name)
                    )
            # A free-form body is documented by prose, so checks 2 and 3 skip
            # its fields. An empty field list on any other body is checked.
            if not free_form:
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


# A fixture line that starts a file of another scanned crate.
OTHER_CRATE = re.compile(r"^// crate ([a-z_]+)$", re.M)


def fixture_struct(source: str):
    """A `find_struct` that reads structs from the fixture source only.

    The source is the crate `crate`. Text after a `// crate <name>` line is
    the root file of that scanned crate, so its `crate::` paths count from
    that crate.
    """

    def find(reference: str, site: Site | None = None) -> str | None:
        return struct_body(reference, site)

    parts = OTHER_CRATE.split(source)
    files = [(parts[0], "crate")] + list(zip(parts[2::2], parts[1::2]))
    find.index, find.aliases = {}, {}
    for text, crate in files:
        for name, found in struct_index(text, "", crate).items():
            find.index.setdefault(name, []).extend(found)
        for name, aliases in type_aliases_in(text, "", crate).items():
            find.aliases.setdefault(name, []).extend(aliases)
    find.local = ("crate", "")
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
        .route("/s/uri-list", get(s_uri_list))
        .route("/s/uri-path-key/{id}", get(s_uri_path_key))
        .route("/s/request-list", get(s_request_list))
        .route("/s/parts-list", get(s_parts_list))
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

async fn s_uri_list(uri: Uri) -> Response {
    for (key, value) in query_pairs(uri.query()) {
        match key.as_str() {
            "limit" => {}
            "sort" => {}
            _ => {}
        }
    }
    StatusCode::OK.into_response()
}

async fn s_uri_path_key(Path(id): Path<String>, uri: Uri) -> Response {
    for (key, value) in query_pairs(uri.query()) {
        match key.as_str() {
            "id" => {}
            _ => {}
        }
    }
    StatusCode::OK.into_response()
}

async fn s_request_list(request: Request) -> Response {
    for (key, value) in query_pairs(request.uri().query()) {
        match key.as_str() {
            "limit" => {}
            "sort" => {}
            _ => {}
        }
    }
    StatusCode::OK.into_response()
}

async fn s_parts_list(parts: Parts) -> Response {
    for (key, value) in query_pairs(parts.uri.query()) {
        match key.as_str() {
            "limit" => {}
            "sort" => {}
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


# An alias chain longer than `ALIAS_PASSES`, which never settles.
FIXTURE_ALIAS_CHAIN = r"""
use axum::extract::Query as Link1;
type Link2<T> = Link1<T>;
type Link3<T> = Link2<T>;
type Link4<T> = Link3<T>;
type Link5<T> = Link4<T>;
type Link6<T> = Link5<T>;
type Link7<T> = Link6<T>;
type Link8<T> = Link7<T>;
type Link9<T> = Link8<T>;
type Link10<T> = Link9<T>;

pub fn harvest_api_router() -> Router {
    Router::new().route("/c/long-chain", get(c_long_chain))
}

async fn c_long_chain(query: Link10<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

struct Cursor {
    offset: Option<u32>,
}
"""


# Local items named like extractors: a module-level struct, and a type alias
# inside a fn.
FIXTURE_LOCAL_ITEMS = r"""
struct Query<T>(T);

fn helper() {
    type Json<T> = Vec<T>;
}

pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/l/local-query", get(l_local_query))
        .route("/l/fn-local-json", post(l_fn_local_json))
}

async fn l_local_query(Query(filter): Query<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn l_fn_local_json(Json(body): Json<Gadget>) -> Response {
    StatusCode::OK.into_response()
}

struct Cursor {
    offset: Option<u32>,
}

struct Gadget {
    name: String,
}
"""


# A plain import of an unrelated `Query`, and a glob from an unknown module.
FIXTURE_IMPORTS = r"""
use crate::signed::Query;
use crate::models::*;

pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/i/shadowed-query", get(i_shadowed_query))
        .route("/i/glob-json", post(i_glob_json))
}

async fn i_shadowed_query(Query(filter): Query<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn i_glob_json(Json(body): Json<Gadget>) -> Response {
    StatusCode::OK.into_response()
}

struct Cursor {
    offset: Option<u32>,
}

struct Gadget {
    name: String,
}
"""


# Two type aliases named `Maybe`, one in a module and one at the root.
FIXTURE_ALIAS_SCOPES = r"""
mod domain {
    type Maybe<T> = Vec<T>;
}

type Maybe<T> = Option<T>;
type Opt<T> = std::option::Option<T>;

mod consumers {
    use crate::domain::Maybe;
    use other_crate::Opt;

    struct ImportedAliasField {
        labels: Maybe<String>,
        note: Opt<String>,
    }
}

fn with_block_alias() {
    type Hidden<T> = Option<T>;
}

pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/s/qualified-alias", post(s_qualified_alias))
        .route("/s/ambiguous-alias", post(s_ambiguous_alias))
        .route("/s/qualified-target", post(s_qualified_target))
        .route("/s/imported-alias", post(s_imported_alias))
        .route("/s/local-option", get(s_local_option))
        .route("/s/std-paths", get(s_std_paths))
        .route("/s/spoofed-query", get(s_spoofed_query))
        .route("/s/std-alias-field", get(s_std_alias_field))
        .route("/s/no-query/{id}", get(s_no_query))
        .route("/s/raw-and-query", get(s_raw_and_query))
        .route("/s/trusted-id", get(s_trusted_id))
        .route("/s/local-id", get(s_local_id))
        .route("/s/rebound-id", get(s_rebound_id))
        .route("/s/bare-id", get(s_bare_id))
        .route("/s/state-request", get(s_state_request))
        .route("/s/fake-raw", get(s_fake_raw))
        .route("/s/trusted-request", get(s_trusted_request))
        .route("/s/reexport-raw", get(s_reexport_raw))
        .route("/s/full-reexport-raw", get(s_full_reexport_raw))
        .route("/s/local-request", get(s_local_request))
        .route("/s/glob-request", get(s_glob_request))
        .route("/s/raw-query", get(s_raw_query))
        .route("/s/unread-query", get(s_unread_query))
        .route("/s/std-option-param", post(s_std_option_param))
        .route("/s/std-alias-param", post(s_std_alias_param))
        .route("/s/spoofed-json", post(s_spoofed_json))
        .route("/s/reexported-query", get(s_reexported_query))
        .route("/s/imported-option", get(s_imported_option))
        .route("/s/deep-option", get(s_deep_option))
        .route("/s/imported-result", post(s_imported_result))
        .route("/s/block-alias", post(s_block_alias))
        .route("/s/dup-params/{id}", get(s_dup_params))
}

async fn s_imported_alias(Json(body): Json<consumers::ImportedAliasField>) -> Response {
    StatusCode::OK.into_response()
}

mod local_option {
    struct Option<T>(T);
    struct String;

    struct LocalOptionPage {
        limit: Option<u32>,
    }

    async fn s_std_option_param(body: std::option::Option<Json<Paging>>) -> Response {
        StatusCode::OK.into_response()
    }

    struct StdPathsPage {
        limit: std::option::Option<core::primitive::u32>,
        name: std::string::String,
        label: alloc::string::String,
        exact: ::core::primitive::bool,
    }
}

async fn s_std_paths(Query(page): Query<local_option::StdPathsPage>) -> Response {
    StatusCode::OK.into_response()
}

mod ids_trusted {
    use uuid::Uuid;

    struct TrustedIdPage {
        id: Uuid,
        other: uuid::Uuid,
    }
}

mod ids_local {
    struct Uuid(u128);

    struct LocalIdPage {
        id: Uuid,
    }
}

mod ids_rebound {
    use crate::fake as uuid;

    struct ReboundIdPage {
        id: uuid::Uuid,
    }
}

struct BareIdPage {
    id: Uuid,
}

async fn s_trusted_id(Query(page): Query<ids_trusted::TrustedIdPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_local_id(Query(page): Query<ids_local::LocalIdPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_rebound_id(Query(page): Query<ids_rebound::ReboundIdPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_bare_id(Query(page): Query<BareIdPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_state_request(State(state): State<Request>) -> Response {
    StatusCode::OK.into_response()
}

mod fake_reader {
    use crate::fake::RawQuery;

    async fn s_fake_raw(RawQuery(raw): RawQuery) -> Response {
        StatusCode::OK.into_response()
    }
}

mod trusted_reader {
    use axum::extract::Request;

    async fn s_trusted_request(request: Request) -> Response {
        StatusCode::OK.into_response()
    }
}

mod local_request {
    struct Request {
        id: u32,
    }

    async fn s_local_request(request: Request) -> Response {
        StatusCode::OK.into_response()
    }
}

mod glob_reader {
    use other_crate::*;

    async fn s_glob_request(request: Request) -> Response {
        StatusCode::OK.into_response()
    }
}

mod reexport_reader {
    use autumn_web::reexports::axum;

    async fn s_reexport_raw(axum::extract::RawQuery(raw): axum::extract::RawQuery) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn s_full_reexport_raw(
    autumn_web::reexports::axum::extract::RawQuery(raw):
        autumn_web::reexports::axum::extract::RawQuery,
) -> Response {
    StatusCode::OK.into_response()
}

async fn s_raw_and_query(
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
    Query(page): Query<Paging>,
) -> Response {
    StatusCode::OK.into_response()
}

async fn s_no_query(Path(id): Path<String>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_raw_query(axum::extract::RawQuery(raw): axum::extract::RawQuery) -> Response {
    StatusCode::OK.into_response()
}

async fn s_unread_query(Query(page): Query<MissingPage>) -> Response {
    StatusCode::OK.into_response()
}

mod std_alias {
    use std::option::Option as Maybe;

    struct StdAliasPage {
        limit: Maybe<u32>,
    }

    async fn s_std_alias_param(body: Maybe<Json<Paging>>) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn s_std_alias_field(Query(page): Query<std_alias::StdAliasPage>) -> Response {
    StatusCode::OK.into_response()
}

mod spoofed {
    use crate::signed as axum;

    async fn s_spoofed_query(page: axum::extract::Query<Paging>) -> Response {
        StatusCode::OK.into_response()
    }

    async fn s_spoofed_json(body: axum::Json<Paging>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod reexported {
    use autumn_web::reexports::axum;

    async fn s_reexported_query(page: axum::extract::Query<Paging>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod imported_option {
    use other_crate::Option;

    struct ImportedOptionPage {
        limit: Option<u32>,
    }

    mod deeper {
        struct Option<T>(T);

        struct DeepOptionPage {
            limit: Option<u32>,
        }
    }
}

async fn s_deep_option(Query(page): Query<imported_option::deeper::DeepOptionPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_local_option(Query(page): Query<local_option::LocalOptionPage>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_imported_option(Query(page): Query<imported_option::ImportedOptionPage>) -> Response {
    StatusCode::OK.into_response()
}

mod imported_result {
    use other_crate::Result;

    async fn s_imported_result(body: Result<Json<Paging>, JsonRejection>) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn s_qualified_target(Json(body): Json<QualifiedTarget>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_block_alias(Json(body): Json<BlockAliasField>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_dup_params(Path(id): Path<String>, Query(page): Query<Paging>) -> Response {
    StatusCode::OK.into_response()
}

struct QualifiedTarget {
    note: Opt<String>,
}

struct BlockAliasField {
    note: Hidden<String>,
}

struct Paging {
    limit: Option<u32>,
}

async fn s_qualified_alias(Json(body): Json<QualifiedAliasField>) -> Response {
    StatusCode::OK.into_response()
}

async fn s_ambiguous_alias(Json(body): Json<AmbiguousAliasField>) -> Response {
    StatusCode::OK.into_response()
}

struct QualifiedAliasField {
    labels: domain::Maybe<String>,
}

struct AmbiguousAliasField {
    note: Maybe<String>,
}
"""


# Names with more than one definition: structs in two modules, and methods
# of the same name on two types.
# A struct in another scanned crate, whose `crate::` import counts from that
# crate's root.
FIXTURE_CRATES = r"""
pub fn harvest_api_router() -> Router {
    Router::new().route("/c/other-crate", post(c_other_crate))
}

async fn c_other_crate(Json(body): Json<autumn_harvest::other::Payload>) -> Response {
    StatusCode::OK.into_response()
}

// crate autumn_harvest

mod shared {
    type Tags<T> = Option<T>;
}

mod other {
    use crate::shared::Tags;

    struct Payload {
        tags: Tags<String>,
    }
}
"""


FIXTURE_NAMES = r"""
type Maybe<T> = Option<T>;
type MaybeTags = Option<Vec<String>>;
type Loose<T> = Option<Vec<T>>;

pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/n/qualified-struct", get(n_qualified_struct))
        .route("/n/ambiguous-struct", get(n_ambiguous_struct))
        .route("/n/lenient-decoder", post(n_lenient_decoder))
        .route("/n/strict-decoder", post(n_strict_decoder))
        .route("/n/ambiguous-method", post(n_ambiguous_method))
        .route("/n/wrong-prefix-struct", get(n_wrong_prefix_struct))
        .route("/n/extern-root-struct", get(n_extern_root_struct))
        .route("/n/extern-root-helper", post(n_extern_root_helper))
        .route("/n/child-path-struct", get(n_child_path_struct))
        .route("/n/aliased-root-struct", get(n_aliased_root_struct))
        .route("/n/crate-path-struct", get(n_crate_path_struct))
        .route("/n/module-helper", post(n_module_helper))
        .route("/n/turbofish-type-helper", post(n_turbofish_type_helper))
        .route("/n/external-import", get(n_external_import))
        .route("/n/imported-helper", post(n_imported_helper))
        .route("/n/external-helper", post(n_external_helper))
        .route("/n/invisible-helper", post(n_invisible_helper))
        .route("/n/helper-parse-type", post(n_helper_parse_type))
        .route("/n/external-module-alias", get(n_external_module_alias))
        .route("/n/local-module-alias", get(n_local_module_alias))
        .route("/n/external-module-helper", post(n_external_module_helper))
        .route("/n/block-imports", post(n_block_imports))
        .route("/n/glob-struct", get(n_glob_struct))
        .route("/n/glob-local", get(n_glob_local))
        .route("/n/glob-own", get(n_glob_own))
        .route("/n/glob-alias", post(n_glob_alias))
        .route("/n/glob-helper", post(n_glob_helper))
        .route("/n/glob-shadow", post(n_glob_shadow))
        .route("/n/block-module-imports", post(n_block_module_imports))
        .route("/n/typed-reply", get(n_typed_reply))
        .route("/n/unknown-reply", get(n_unknown_reply))
        .route("/n/self-method", post(n_self_method))
        .route("/n/local-import", get(n_local_import))
        .route("/n/super-path", get(n_super_path))
        .route("/n/self-path", get(n_self_path))
        .route("/n/typed-receiver", post(n_typed_receiver))
        .route("/n/let-receiver", post(n_let_receiver))
        .route("/n/alias-receiver", post(n_alias_receiver))
        .route("/n/grouped-let-receiver", post(n_grouped_let_receiver))
        .route("/n/untyped-receiver", post(n_untyped_receiver))
        .route("/n/maybe-fields", post(n_maybe_fields))
        .route("/n/loose-field", post(n_loose_field))
}

mod module_a {
    struct Filter {
        wanted: u32,
    }
}

mod module_b {
    struct Filter {
        wanted: Option<u32>,
    }
}

async fn n_qualified_struct(Query(filter): Query<module_b::Filter>) -> Response {
    StatusCode::OK.into_response()
}

mod unrelated {
    mod models {
        struct RemoteFilter {
            wanted: u32,
        }

        fn decode_models(raw: &[u8]) -> Response {
            StatusCode::OK.into_response()
        }
    }
}

async fn n_extern_root_struct(Query(filter): Query<models::RemoteFilter>) -> Response {
    StatusCode::OK.into_response()
}

async fn n_extern_root_helper(body: Bytes) -> Response {
    models::decode_models(&body)
}

mod outer_scope {
    mod inner {
        struct InnerPage {
            wanted: Option<u32>,
        }
    }

    async fn n_child_path_struct(Query(page): Query<inner::InnerPage>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod aliased_root {
    use crate::unrelated::models as models;

    async fn n_aliased_root_struct(Query(filter): Query<models::RemoteFilter>) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn n_wrong_prefix_struct(Query(filter): Query<other::module_b::Filter>) -> Response {
    StatusCode::OK.into_response()
}

async fn n_crate_path_struct(Query(filter): Query<crate::module_b::Filter>) -> Response {
    StatusCode::OK.into_response()
}

fn decode_raw(raw: &[u8]) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

mod helpers {
    fn decode_raw(raw: &[u8]) -> Response {
        StatusCode::OK.into_response()
    }
}

impl StrictReader {
    fn read(&self, raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

impl LenientReader {
    fn read(&self, raw: &[u8]) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn n_typed_receiver(body: Bytes, reader: LenientReader) -> Response {
    reader.read(&body)
}

type ReaderAlias = StrictReader;

async fn n_alias_receiver(body: Bytes) -> Response {
    let reader: ReaderAlias = make_reader();
    reader.read(&body)
}

async fn n_grouped_let_receiver(body: Bytes) -> Response {
    let reader = StrictReader::new();
    (&reader).read(&body)
}

async fn n_let_receiver(body: Bytes) -> Response {
    let reader = StrictReader::new();
    reader.read(&body)
}

async fn n_untyped_receiver(body: Bytes) -> Response {
    let reader = make_reader();
    reader.read(&body)
}

struct Widget {
    size: u32,
}

struct Local {
    size: u32,
}

mod caller {
    use crate::helpers::decode_raw;

    async fn n_imported_helper(body: Bytes) -> Response {
        decode_raw(&body)
    }
}

mod outsider {
    use other_crate::decode_raw;

    async fn n_external_helper(body: Bytes) -> Response {
        decode_raw(&body)
    }
}

mod sibling {
    fn private_decode(raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

async fn n_invisible_helper(body: Bytes) -> Response {
    private_decode(&body)
}

mod parsers {
    use crate::module_b::Filter;

    pub fn parse_filter(raw: &[u8]) -> Filter {
        serde_json::from_slice::<Filter>(raw).unwrap()
    }
}

use crate::parsers::parse_filter;

mod schema_alias {
    use other_crate::models as module_b;

    async fn n_external_module_alias(Query(filter): Query<module_b::Filter>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod schema_local {
    use crate::module_b as schema;

    async fn n_local_module_alias(Query(filter): Query<schema::Filter>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod helper_alias {
    use other_crate::codecs as helpers;

    async fn n_external_module_helper(body: Bytes) -> Response {
        helpers::decode_raw(&body)
    }
}

mod glob_external {
    use axum::body::Bytes;
    use axum::extract::Query;
    use axum::Json;
    use other_crate::*;

    async fn n_glob_struct(Query(widget): Query<Widget>) -> Response {
        StatusCode::OK.into_response()
    }

    struct GlobAliasField {
        tags: MaybeTags,
    }

    async fn n_glob_alias(Json(body): Json<GlobAliasField>) -> Response {
        StatusCode::OK.into_response()
    }

    async fn n_glob_helper(body: Bytes) -> Response {
        decode_raw(&body)
    }
}

mod glob_local {
    use crate::module_b::*;

    async fn n_glob_local(Query(filter): Query<Filter>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod shadow_source {
    struct Json(Vec<u8>);
}

mod glob_shadow {
    use crate::shadow_source::*;

    async fn n_glob_shadow(Json(body): Json<Widget>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod glob_own {
    use axum::extract::Query;
    use other_crate::*;

    struct Owned {
        size: u32,
    }

    async fn n_glob_own(Query(owned): Query<Owned>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod lenient_codec {
    fn unpack(raw: &[u8]) -> Response {
        StatusCode::OK.into_response()
    }
}

mod strict_codec {
    fn unpack(raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

async fn n_block_imports(body: Bytes) -> Response {
    {
        use crate::lenient_codec::unpack;
        unpack(&body);
    }
    {
        use crate::strict_codec::unpack;
        unpack(&body)
    }
}

async fn n_block_module_imports(body: Bytes) -> Response {
    {
        use crate::lenient_codec as codec;
        codec::unpack(&body);
    }
    {
        use crate::strict_codec as codec;
        codec::unpack(&body)
    }
}

impl PlainReply {
    fn reply(&self) -> Response {
        StatusCode::OK.into_response()
    }
}

impl PickyReply {
    fn reply(&self) -> Response {
        StatusCode::CONFLICT.into_response()
    }
}

async fn n_typed_reply(replier: PlainReply) -> Response {
    replier.reply()
}

async fn n_unknown_reply() -> Response {
    let replier = make_replier();
    replier.reply()
}

impl Relay {
    fn relay(&self, raw: &[u8]) -> Response {
        self.finish(raw)
    }

    fn finish(&self, raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

impl Sink {
    fn finish(&self, raw: &[u8]) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn n_self_method(body: Bytes, relay: Relay) -> Response {
    relay.relay(&body)
}

async fn n_helper_parse_type(body: Bytes) -> Response {
    let filter = parse_filter(&body);
    StatusCode::OK.into_response()
}

mod remote {
    use other_crate::Widget;

    async fn n_external_import(Query(widget): Query<Widget>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod local_import {
    use crate::module_b::Filter;

    async fn n_local_import(Query(filter): Query<Filter>) -> Response {
        StatusCode::OK.into_response()
    }
}

mod nested {
    struct Local {
        wanted: Option<u32>,
    }

    mod inner {
        async fn n_super_path(Query(filter): Query<super::super::module_a::Filter>) -> Response {
            StatusCode::OK.into_response()
        }
    }

    async fn n_self_path(Query(local): Query<self::Local>) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn n_turbofish_type_helper(body: Bytes) -> Response {
    StrictDecoder::<Config>::decode(&body)
}

async fn n_module_helper(body: Bytes) -> Response {
    helpers::decode_raw(&body)
}

async fn n_ambiguous_struct(Query(filter): Query<Filter>) -> Response {
    StatusCode::OK.into_response()
}

impl StrictDecoder {
    fn decode(raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

impl LenientDecoder {
    fn decode(raw: &[u8]) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn n_lenient_decoder(body: Bytes) -> Response {
    LenientDecoder::decode(&body)
}

async fn n_strict_decoder(body: Bytes) -> Response {
    StrictDecoder::decode(&body)
}

async fn n_ambiguous_method(body: Bytes, decoder: Decoder) -> Response {
    decoder.decode(&body)
}

async fn n_maybe_fields(Json(body): Json<MaybeFields>) -> Response {
    StatusCode::OK.into_response()
}

async fn n_loose_field(Json(body): Json<LooseField>) -> Response {
    StatusCode::OK.into_response()
}

struct Gadget {
    name: String,
}

struct MaybeFields {
    name: String,
    note: Maybe<String>,
    tags: MaybeTags,
}

struct LooseField {
    tags: Loose<String>,
}
"""


# Shapes that once slipped past the audit or failed it on correct code.
FIXTURE_EDGES = r"""
use serde_json::from_slice as decode;
use serde_json as json;
use axum::extract::Query as AxumQuery;
use axum::{extract::State, Json as AxumJson};
use bytes::Bytes as RequestBytes;

type ApiQuery<T> = Query<T>;
use axum::extract::Query as ChainQ;
type ChainQuery<T> = ChainQ<T>;
use serde_json::{self as sj, Value};
use axum::{extract::{Query as NestedQ, Path}};
use crate::signed::Query as SignedQ;
use std::str as text_mod;
type InnerOdd<T> = Query<Vec<T>>;
type OuterOdd<T> = InnerOdd<T>;
type OddQuery<T> = Result<Query<T>, QueryRejection>;

pub fn harvest_api_router() -> Router {
    Router::new()
        .route("/e/wrapped", post(e_wrapped))
        .route("/e/sliced", post(e_sliced))
        .route("/e/borrowed-alias", post(e_borrowed_alias))
        .route("/e/view-alias", post(e_view_alias))
        .route("/e/partial-alias", post(e_partial_alias))
        .route("/e/copied-alias", post(e_copied_alias))
        .route("/e/arm-rebinds", post(e_arm_rebinds))
        .route("/e/into-typed", post(e_into_typed))
        .route("/e/std-vec-body", post(e_std_vec_body))
        .route("/e/alloc-vec-body", post(e_alloc_vec_body))
        .route("/e/try-into-typed", post(e_try_into_typed))
        .route("/e/into-untyped", post(e_into_untyped))
        .route("/e/from-path", post(e_from_path))
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
        .route("/e/call-guard", post(e_call_guard))
        .route("/e/guarded-fields", post(e_guarded_fields))
        .route("/e/map-err-ok", post(e_map_err_ok))
        .route("/e/return-rejection", post(e_return_rejection))
        .route("/e/closure-return", post(e_closure_return))
        .route("/e/optional-strict-query", get(e_optional_strict_query))
        .route("/e/unwrapped", post(e_unwrapped))
        .route("/e/borrowed-match", post(e_borrowed_match))
        .route("/e/parse-then-return", post(e_parse_then_return))
        .route("/e/nested-err", post(e_nested_err))
        .route("/e/split-calls", post(e_split_calls))
        .route("/e/lifetime", post(e_lifetime))
        .route("/e/conditional-return", post(e_conditional_return))
        .route("/e/second-err", post(e_second_err))
        .route("/e/combinator", post(e_combinator))
        .route("/e/tolerant-chain", post(e_tolerant_chain))
        .route("/e/nested-guard", post(e_nested_guard))
        .route("/e/ok-then-err", post(e_ok_then_err))
        .route("/e/wrapped-field", get(e_wrapped_field))
        .route("/e/compound-guard", post(e_compound_guard))
        .route("/e/either-guard", post(e_either_guard))
        .route("/e/inspected", post(e_inspected))
        .route("/e/mixed-calls", post(e_mixed_calls))
        .route("/e/compared-guard", post(e_compared_guard))
        .route("/e/let-else-ok", post(e_let_else_ok))
        .route("/e/if-let-else-ok", post(e_if_let_else_ok))
        .route("/e/empty-arm-rejects", post(e_empty_arm_rejects))
        .route("/e/if-let-json", post(e_if_let_json))
        .route("/e/raw-let-else", post(e_raw_let_else))
        .route("/e/map-or", post(e_map_or))
        .route("/e/is-err-reject", post(e_is_err_reject))
        .route("/e/is-ok-reject", post(e_is_ok_reject))
        .route("/e/teapot-else", post(e_teapot_else))
        .route("/e/teapot-arm", post(e_teapot_arm))
        .route("/e/raw-match-ok", post(e_raw_match_ok))
        .route("/e/raw-match-reject", post(e_raw_match_reject))
        .route("/e/query-result-ok", get(e_query_result_ok))
        .route("/e/query-result-reject", get(e_query_result_reject))
        .route("/e/map-or-else-reject", post(e_map_or_else_reject))
        .route("/e/json-map-or-else", post(e_json_map_or_else))
        .route("/e/fn-ref-fallback", post(e_fn_ref_fallback))
        .route("/e/closure-default", post(e_closure_default))
        .route("/e/json-is-err", post(e_json_is_err))
        .route("/e/query-is-err", get(e_query_is_err))
        .route("/e/json-wildcard", post(e_json_wildcard))
        .route("/e/raw-wildcard-ok", post(e_raw_wildcard_ok))
        .route("/e/json-if-let-err", post(e_json_if_let_err))
        .route("/e/query-if-let-err", get(e_query_if_let_err))
        .route("/e/json-if-let-err-ok", post(e_json_if_let_err_ok))
        .route("/e/negated-else-rejects", post(e_negated_else_rejects))
        .route("/e/named-json", post(e_named_json))
        .route("/e/status-early-return", post(e_status_early_return))
        .route("/e/at-binding", post(e_at_binding))
        .route("/e/json-helper-exit", post(e_json_helper_exit))
        .route("/e/json-or-else", post(e_json_or_else))
        .route("/e/raw-or-else", post(e_raw_or_else))
        .route("/e/raw-or-else-err", post(e_raw_or_else_err))
        .route("/e/ignored-error-helper", post(e_ignored_error_helper))
        .route("/e/or-else-branch", post(e_or_else_branch))
        .route("/e/or-else-block", post(e_or_else_block))
        .route("/e/json-tail-helper", post(e_json_tail_helper))
        .route("/e/json-tail-value", post(e_json_tail_value))
        .route("/e/json-alias", post(e_json_alias))
        .route("/e/json-tail-result", post(e_json_tail_result))
        .route("/e/json-return-map", post(e_json_return_map))
        .route("/e/json-shadowed", post(e_json_shadowed))
        .route("/e/json-default-guard", post(e_json_default_guard))
        .route("/e/raw-shadowed", post(e_raw_shadowed))
        .route("/e/raw-two-level", post(e_raw_two_level))
        .route("/e/raw-recursive", post(e_raw_recursive))
        .route("/e/json-handoff", post(e_json_handoff))
        .route("/e/raw-turbofish-helper", post(e_raw_turbofish_helper))
        .route("/e/err-observe-then-reject", post(e_err_observe_then_reject))
        .route("/e/alias-out-of-scope", post(e_alias_out_of_scope))
        .route("/e/matches-macro", post(e_matches_macro))
        .route("/e/unknown-macro", post(e_unknown_macro))
        .route("/e/rebound-match", post(e_rebound_match))
        .route("/e/local-query-elsewhere", get(e_local_query_elsewhere))
        .route("/e/renamed-bytes", post(e_renamed_bytes))
        .route("/e/type-alias-query", get(e_type_alias_query))
        .route("/e/odd-alias-query", get(e_odd_alias_query))
        .route("/e/transparent-body", post(e_transparent_body))
        .route("/e/comparison-argument", post(e_comparison_argument))
        .route("/e/nested-or-guard", post(e_nested_or_guard))
        .route("/e/alias-chain-query", get(e_alias_chain_query))
        .route("/e/observed-fallback", post(e_observed_fallback))
        .route("/e/grouped-module-alias", post(e_grouped_module_alias))
        .route("/e/nested-group-query", get(e_nested_group_query))
        .route("/e/foreign-query-alias", get(e_foreign_query_alias))
        .route("/e/unreadable-alias-chain", get(e_unreadable_alias_chain))
        .route("/e/raw-unknown-helper", post(e_raw_unknown_helper))
        .route("/e/tristate-field", post(e_tristate_field))
        .route("/e/tristate-plain-option", post(e_tristate_plain_option))
        .route("/e/unknown-deserializer", post(e_unknown_deserializer))
        .route("/e/with-field", post(e_with_field))
        .route("/e/multiline-field-serde", post(e_multiline_field_serde))
        .route("/e/serde-keyword-values", post(e_serde_keyword_values))
        .route("/e/borrowed-err-arm", post(e_borrowed_err_arm))
        .route("/e/grouped-err-arm", post(e_grouped_err_arm))
        .route("/e/borrowed-if-let-err", post(e_borrowed_if_let_err))
        .route("/e/raw-associated-helper", post(e_raw_associated_helper))
        .route("/e/raw-method-helper", post(e_raw_method_helper))
        .route("/e/raw-unknown-method", post(e_raw_unknown_method))
        .route("/e/raw-std-reader", post(e_raw_std_reader))
        .route("/e/extension-decode", post(e_extension_decode))
        .route("/e/grouped-receiver", post(e_grouped_receiver))
        .route("/e/borrowed-receiver", post(e_borrowed_receiver))
        .route("/e/nested-receiver", post(e_nested_receiver))
        .route("/e/sliced-receiver", post(e_sliced_receiver))
        .route("/e/grouped-argument", post(e_grouped_argument))
        .route("/e/unrelated-receiver", post(e_unrelated_receiver))
        .route("/e/alias-body", post(e_alias_body))
        .route("/e/dead-closure", post(e_dead_closure))
        .route("/e/tuple-carrier", post(e_tuple_carrier))
        .route("/e/struct-carrier", post(e_struct_carrier))
        .route("/e/rebound-carrier", post(e_rebound_carrier))
        .route("/e/rebound-other", post(e_rebound_other))
        .route("/e/result-bytes", post(e_result_bytes))
        .route("/e/option-bytes-match", post(e_option_bytes_match))
        .route("/e/result-bytes-if-let", post(e_result_bytes_if_let))
        .route("/e/result-bytes-tolerant", post(e_result_bytes_tolerant))
        .route("/e/option-bytes-unwrapped", post(e_option_bytes_unwrapped))
        .route("/e/empty-rejected-tolerant", post(e_empty_rejected_tolerant))
        .route("/e/empty-rejected-helper", post(e_empty_rejected_helper))
        .route("/e/empty-else-rejected", post(e_empty_else_rejected))
        .route("/e/option-ok-or", post(e_option_ok_or))
        .route("/e/option-expect", post(e_option_expect))
        .route("/e/option-match", post(e_option_match))
        .route("/e/option-let-else", post(e_option_let_else))
        .route("/e/option-if-none", post(e_option_if_none))
        .route("/e/option-bytes", post(e_option_bytes))
        .route("/e/option-body-default", post(e_option_body_default))
        .route("/e/option-unread-use", post(e_option_unread_use))
        .route("/e/dropped-closure", post(e_dropped_closure))
        .route("/e/dead-helper-closure", post(e_dead_helper_closure))
        .route("/e/called-closure", post(e_called_closure))
        .route("/e/stored-closure", post(e_stored_closure))
        .route("/e/alias-helper", post(e_alias_helper))
        .route("/e/opaque-body", post(e_opaque_body))
        .route("/e/unknown-type-body", post(e_unknown_type_body))
        .route("/e/alias-conversion", post(e_alias_conversion))
        .route("/e/struct-named-alias", post(e_struct_named_alias))
        .route("/e/slice-receiver", post(e_slice_receiver))
        .route("/e/spoofed-parse", post(e_spoofed_parse))
        .route("/e/nested-or-inspection", post(e_nested_or_inspection))
        .route("/e/shadowed-decode-alias", post(e_shadowed_decode_alias))
        .route("/e/wrapped-carrier", post(e_wrapped_carrier))
        .route("/e/nonbyte-param", post(e_nonbyte_param))
        .route("/e/scalar-read-argument", post(e_scalar_read_argument))
        .route("/e/mut-borrow-parse", post(e_mut_borrow_parse))
        .route("/e/partial-slice-parse", post(e_partial_slice_parse))
        .route("/e/signed-type-alias", get(e_signed_type_alias))
        .route("/e/foreign-qualified-query", get(e_foreign_qualified_query))
        .route("/e/inline-axum-query", get(e_inline_axum_query))
        .route("/e/or-and-empty-guard", post(e_or_and_empty_guard))
        .route("/e/nested-and-error-exit", post(e_nested_and_error_exit))
        .route("/e/unknown-receiver-method", post(e_unknown_receiver_method))
        .route("/e/accessor-receiver", post(e_accessor_receiver))
        .route("/e/raw-aliased-std-reader", post(e_raw_aliased_std_reader))
        .route("/e/raw-imported-std-reader", post(e_raw_imported_std_reader))
        .route("/e/raw-unknown-associated", post(e_raw_unknown_associated))
        .route("/e/json-handoff-unknown", post(e_json_handoff_unknown))
        .route("/e/json-handoff-tolerant", post(e_json_handoff_tolerant))
        .route("/e/raw-nested-argument", post(e_raw_nested_argument))
        .route("/e/shared-query-key", get(e_shared_query_key))
        .route("/e/conflicting-query-key", get(e_conflicting_query_key))
        .route("/e/helper-in-comment", get(e_helper_in_comment))
        .route("/e/json-std-err", post(e_json_std_err))
        .route("/e/json-core-err", post(e_json_core_err))
        .route("/e/qualified-option-body", post(e_qualified_option_body))
        .route("/e/qualified-option-query", get(e_qualified_option_query))
        .route("/e/is-err-tail-helper", post(e_is_err_tail_helper))
        .route("/e/helper-default", post(e_helper_default))
        .route("/e/unknown-helper", post(e_unknown_helper))
        .route("/e/uuid-bytes", post(e_uuid_bytes))
        .route("/e/option-fallback", post(e_option_fallback))
        .route("/e/option-default", post(e_option_default))
        .route("/e/commented-parse", post(e_commented_parse))
        .route("/e/let-else-json", post(e_let_else_json))
        .route("/e/map-or-helper", post(e_map_or_helper))
        .route("/e/match-mut", post(e_match_mut))
        .route("/e/await-default", post(e_await_default))
        .route("/e/borrowed-let-else", post(e_borrowed_let_else))
        .route("/e/as-ref-if-let-err", post(e_as_ref_if_let_err))
        .route("/e/is-err-and", post(e_is_err_and))
        .route("/e/is-err-or", post(e_is_err_or))
        .route("/e/is-ok-and-then", post(e_is_ok_and_then))
        .route("/e/is-ok-and-or", post(e_is_ok_and_or))
        .route("/e/exit-then-unwrap", post(e_exit_then_unwrap))
        .route("/e/log-then-unwrap", post(e_log_then_unwrap))
        .route("/e/nested-comment", post(e_nested_comment))
        .route("/e/raw-alias", post(e_raw_alias))
        .route("/e/json-helper-default", post(e_json_helper_default))
        .route("/e/let-err-else", post(e_let_err_else))
        .route("/e/aliased-decode", post(e_aliased_decode))
        .route("/e/module-alias", post(e_module_alias))
        .route("/e/ok-return-then-reject", post(e_ok_return_then_reject))
        .route("/e/raw-guarded-arms", post(e_raw_guarded_arms))
        .route("/e/stored-parse", post(e_stored_parse))
        .route("/e/stored-parse-strict", post(e_stored_parse_strict))
        .route("/e/or-exit-then-unwrap", post(e_or_exit_then_unwrap))
        .route("/e/nested-param-comment", post(e_nested_param_comment))
        .route("/e/local-from-slice", post(e_local_from_slice))
        .route("/e/closure-return-reject", post(e_closure_return_reject))
        .route("/e/multiline-let-else", post(e_multiline_let_else))
        .route("/e/closure-return-value", post(e_closure_return_value))
        .route("/e/commented-unwrap", post(e_commented_unwrap))
        .route("/e/ref-catch-all", post(e_ref_catch_all))
        .route("/e/ref-err-handed", post(e_ref_err_handed))
        .route("/e/nested-struct-comment", post(e_nested_struct_comment))
        .route("/e/short-helper", post(e_short_helper))
        .route("/e/response-config", post(e_response_config))
        .route("/e/suffix-error", post(e_suffix_error))
        .route("/e/commented-status", get(e_commented_status))
        .route("/e/std-result", post(e_std_result))
        .route("/e/aliased-query", get(e_aliased_query))
        .route("/e/aliased-json", post(e_aliased_json))
        .route("/e/std-result-query", get(e_std_result_query))
}

async fn e_wrapped(body: Bytes) -> Response {
    let widget = serde_json::from_slice::<Gadget>(
        &body,
    );
    StatusCode::OK.into_response()
}

async fn e_sliced(body: Bytes) -> Response {
    let widget: Gadget = serde_json::from_slice(&body[1..]).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_borrowed_alias(body: Bytes) -> Response {
    let bytes: &[u8] = &body;
    let gadget = serde_json::from_slice::<Gadget>(bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_view_alias(body: Bytes) -> Response {
    let bytes = body.as_ref();
    let gadget: Gadget = serde_json::from_slice(bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_partial_alias(body: Bytes) -> Response {
    let tail = &body[1..];
    let gadget = serde_json::from_slice::<Gadget>(tail).unwrap();
    StatusCode::OK.into_response()
}

async fn e_copied_alias(body: Bytes) -> Response {
    let owned = body.to_vec();
    let gadget = parse_gadget(&owned);
    StatusCode::OK.into_response()
}

impl From<Bytes> for Parcel {
    fn from(raw: Bytes) -> Self {
        let gadget = serde_json::from_slice::<Gadget>(&raw).unwrap();
        Parcel { name: gadget.name }
    }
}

impl TryFrom<Bytes> for Parcel {
    type Error = Response;

    fn try_from(raw: Bytes) -> Result<Self, Response> {
        let gadget = serde_json::from_slice::<Gadget>(&raw).map_err(reject)?;
        Ok(Parcel { name: gadget.name })
    }
}

async fn e_std_vec_body(body: std::vec::Vec<u8>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_alloc_vec_body(body: alloc::vec::Vec<u8>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_into_typed(body: Bytes) -> Response {
    let parcel: Parcel = body.into();
    StatusCode::OK.into_response()
}

async fn e_try_into_typed(body: Bytes) -> Response {
    let parcel: Result<Parcel, Response> = body.try_into();
    parcel?;
    StatusCode::OK.into_response()
}

async fn e_into_untyped(body: Bytes) -> Response {
    consume_parcel(body.into());
    StatusCode::OK.into_response()
}

async fn e_from_path(body: Bytes) -> Response {
    let parcel = Parcel::from(body);
    StatusCode::OK.into_response()
}

async fn e_arm_rebinds(body: Bytes) -> Response {
    let request = match serde_json::from_slice::<Gadget>(&body) {
        Ok(body) => body.into_request(),
        Err(_) => return Err(reject()),
    };
    forward_request(&request);
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

async fn e_json_path(axum::Json(body): axum::Json<crate::Gadget>) -> Response {
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

async fn e_call_guard(body: Bytes) -> Response {
    if !body.is_empty() {
        let gadget = parse_gadget(&body)?;
    }
    StatusCode::OK.into_response()
}

async fn e_guarded_fields(body: Bytes) -> Response {
    let gadget: Gadget = if body.is_empty() {
        Gadget::default()
    } else {
        serde_json::from_slice(&body).map_err(reject)?
    };
    StatusCode::OK.into_response()
}

async fn e_map_err_ok(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = body.map_err(log_rejection).ok();
    StatusCode::OK.into_response()
}

async fn e_return_rejection(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(rejection) => return rejection.into_response(),
    };
    StatusCode::OK.into_response()
}

async fn e_closure_return(body: Bytes) -> Response {
    if body.is_empty() {
        let fallback = || -> Result<Gadget, Response> { return Ok(Gadget::default()) };
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    StatusCode::OK.into_response()
}

async fn e_optional_strict_query(query: Option<Query<StrictQuery>>) -> Response {
    StatusCode::OK.into_response()
}

struct StrictQuery {
    term: String,
    limit: Option<u32>,
}

async fn e_unwrapped(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Json(gadget) = body.expect("a valid body");
    StatusCode::OK.into_response()
}

async fn e_borrowed_match(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match &body {
        Ok(_) => {}
        Err(_) => return AutumnError::bad_request_msg("invalid body").into_response(),
    }
    StatusCode::OK.into_response()
}

async fn e_parse_then_return(body: Bytes) -> Result<Response, Response> {
    if body.is_empty() {
        let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
        return Ok(StatusCode::OK.into_response());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_nested_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => match check(&gadget) {
            Err(_) => fallback(),
            Ok(value) => value,
        },
        Err(rejection) => return rejection.into_response(),
    };
    StatusCode::OK.into_response()
}

fn decode_pair(first: &[u8], second: &[u8]) -> Result<Gadget, Response> {
    let gadget = serde_json::from_slice::<Gadget>(first).map_err(reject)?;
    let cursor = serde_json::from_slice::<Cursor>(second).map_err(reject)?;
    Ok(gadget)
}

async fn e_split_calls(body: Bytes) -> Response {
    if !body.is_empty() {
        let gadget = decode_pair(&body, STORED)?;
    }
    let other = decode_pair(STORED, &body).ok();
    StatusCode::OK.into_response()
}

fn parse_borrowed<'a>(body: &'a [u8]) -> Result<Gadget, Response> {
    serde_json::from_slice::<Gadget>(body).map_err(reject)
}

async fn e_lifetime(body: Bytes) -> Response {
    let gadget = parse_borrowed(&body)?;
    StatusCode::OK.into_response()
}

async fn e_conditional_return(body: Bytes) -> Result<Response, Response> {
    if body.is_empty() {
        if allow_missing() {
            return Ok(StatusCode::OK.into_response());
        }
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_second_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(e) if replayable(&e) => return replay_committed(e).await,
        Err(e) => return e.into_response(),
    };
    StatusCode::OK.into_response()
}

async fn e_combinator(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, Response> {
    let name = body.map(|Json(gadget)| gadget.name).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_tolerant_chain(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let name = body.map(|Json(gadget)| gadget.name).ok();
    StatusCode::OK.into_response()
}

async fn e_nested_guard(body: Bytes) -> Result<Response, Response> {
    if allow_missing() {
        if body.is_empty() {
            return Ok(StatusCode::OK.into_response());
        }
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_ok_then_err(body: Bytes) -> Result<Response, Response> {
    let gadget = serde_json::from_slice::<Gadget>(&body).ok().ok_or_else(missing)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_wrapped_field(Query(query): Query<WrappedField>) -> Response {
    StatusCode::OK.into_response()
}

struct WrappedField {
    page: Option<
        u32,
    >,
}

async fn e_compound_guard(body: Bytes) -> Result<Response, Response> {
    if body.is_empty() && allow_missing() {
        return Ok(StatusCode::OK.into_response());
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_either_guard(body: Bytes) -> Result<Response, Response> {
    if body.is_empty() || dry_run() {
        return Ok(StatusCode::OK.into_response());
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_inspected(body: Bytes) -> Response {
    let valid = serde_json::from_slice::<Gadget>(&body).is_ok();
    StatusCode::OK.into_response()
}

async fn e_mixed_calls(body: Bytes) -> Response {
    if !body.is_empty() {
        let gadget = parse_gadget(&body)?;
    }
    let again = parse_gadget(&body).ok();
    StatusCode::OK.into_response()
}

async fn e_compared_guard(body: Bytes) -> Result<Response, Response> {
    if body.is_empty() == false {
        return Ok(StatusCode::OK.into_response());
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).map_err(reject)?;
    Ok(StatusCode::OK.into_response())
}

async fn e_let_else_ok(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, Response> {
    let Ok(Json(gadget)) = body else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    Ok(StatusCode::OK.into_response())
}

async fn e_if_let_else_ok(body: Bytes) -> Result<Response, Response> {
    if let Ok(gadget) = serde_json::from_slice::<Gadget>(&body) {
        tracing::debug!(name = %gadget.name, "parsed");
    } else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_empty_arm_rejects(body: Bytes) -> Result<Response, Response> {
    let gadget = if body.is_empty() {
        return Err(reject());
    } else {
        serde_json::from_slice::<Gadget>(&body).map_err(reject)?
    };
    Ok(StatusCode::OK.into_response())
}

async fn e_if_let_json(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, Response> {
    if let Ok(Json(gadget)) = body {
        tracing::debug!(name = %gadget.name, "parsed");
    } else {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_raw_let_else(body: Bytes) -> Result<Response, Response> {
    let Ok(gadget) = serde_json::from_slice::<Gadget>(&body) else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    Ok(StatusCode::OK.into_response())
}

async fn e_map_or(body: Bytes) -> Response {
    let name = serde_json::from_slice::<Gadget>(&body).map_or(String::new(), |g| g.name);
    StatusCode::OK.into_response()
}

async fn e_is_err_reject(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_err() {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_is_ok_reject(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_ok() {
        tracing::debug!("valid");
    } else {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_teapot_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Ok(Json(gadget)) = body else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    StatusCode::OK.into_response()
}

async fn e_teapot_arm(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn e_raw_match_ok(body: Bytes) -> Result<Response, Response> {
    let gadget = match serde_json::from_slice::<Gadget>(&body) {
        Ok(gadget) => gadget,
        Err(_) => return Ok(StatusCode::NO_CONTENT.into_response()),
    };
    Ok(StatusCode::OK.into_response())
}

async fn e_raw_match_reject(body: Bytes) -> Result<Response, Response> {
    let gadget = match serde_json::from_slice::<Gadget>(&body) {
        Ok(gadget) => gadget,
        Err(_) => return Err(reject()),
    };
    Ok(StatusCode::OK.into_response())
}

async fn e_query_result_ok(query: Result<Query<Documented>, QueryRejection>) -> Response {
    let kind = match query {
        Ok(Query(query)) => query.kind,
        Err(_) => String::from("all"),
    };
    StatusCode::OK.into_response()
}

async fn e_query_result_reject(
    query: Result<axum::extract::Query<Documented>, QueryRejection>,
) -> Result<Response, Response> {
    let Query(query) = query.map_err(|_| reject())?;
    Ok(StatusCode::OK.into_response())
}

async fn e_map_or_else_reject(body: Bytes) -> Response {
    serde_json::from_slice::<Gadget>(&body).map_or_else(|error| reject(error), |gadget| accept(gadget))
}

async fn e_json_map_or_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    body.map_or_else(|rejection| rejection.into_response(), |Json(gadget)| accept(gadget))
}

async fn e_fn_ref_fallback(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap_or_else(rejected_gadget);
    StatusCode::OK.into_response()
}

async fn e_closure_default(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap_or_else(|_| Gadget::default());
    StatusCode::OK.into_response()
}

async fn e_json_is_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_query_is_err(query: Result<Query<Documented>, QueryRejection>) -> Response {
    if !query.is_ok() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_json_wildcard(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => accept(gadget),
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn e_raw_wildcard_ok(body: Bytes) -> Response {
    match serde_json::from_slice::<Gadget>(&body) {
        Ok(gadget) => accept(gadget),
        _ => StatusCode::NO_CONTENT.into_response(),
    }
}

async fn e_json_if_let_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if let Err(rejection) = body {
        return rejection.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_query_if_let_err(query: Result<Query<Documented>, QueryRejection>) -> Response {
    if let Err(_) = query {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_json_if_let_err_ok(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if let Err(error) = body {
        tracing::debug!(%error, "no body");
    }
    StatusCode::OK.into_response()
}

async fn e_negated_else_rejects(body: Bytes) -> Result<Response, Response> {
    if !body.is_empty() {
        let gadget = serde_json::from_slice::<Gadget>(&body).map_err(|_| reject())?;
    } else {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_named_json(mut body: axum::Json<Gadget>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_status_early_return(body: Bytes) -> Response {
    if body.is_empty() {
        return StatusCode::NO_CONTENT.into_response();
    }
    let gadget = match serde_json::from_slice::<Gadget>(&body) {
        Ok(gadget) => gadget,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    StatusCode::OK.into_response()
}

async fn e_at_binding(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => accept(gadget),
        error @ Err(_) => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn e_json_helper_exit(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(_) => return invalid_body(),
    };
    StatusCode::OK.into_response()
}

async fn e_json_or_else(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, Response> {
    let Json(gadget) = body.or_else(|_| Ok::<_, JsonRejection>(Json(Gadget::default())))?;
    Ok(StatusCode::OK.into_response())
}

async fn e_raw_or_else(body: Bytes) -> Result<Response, Response> {
    let gadget = serde_json::from_slice::<Gadget>(&body).or_else(|_| Ok(Gadget::default()))?;
    Ok(StatusCode::OK.into_response())
}

async fn e_raw_or_else_err(body: Bytes) -> Result<Response, Response> {
    let gadget = serde_json::from_slice::<Gadget>(&body).or_else(|e| Err(reject(e)))?;
    Ok(StatusCode::OK.into_response())
}

async fn e_ignored_error_helper(body: Bytes) -> Response {
    serde_json::from_slice::<Gadget>(&body).map_or_else(|_| invalid_body(), |gadget| accept(gadget))
}

async fn e_or_else_branch(body: Bytes) -> Result<Response, Response> {
    let gadget = serde_json::from_slice::<Gadget>(&body)
        .or_else(|e| if !e.is_eof() { Ok(Gadget::default()) } else { propagate(e) })?;
    Ok(StatusCode::OK.into_response())
}

async fn e_or_else_block(body: Bytes) -> Result<Response, Response> {
    let gadget = serde_json::from_slice::<Gadget>(&body).or_else(|error| {
        tracing::debug!(%error, "default");
        Ok(Gadget::default())
    })?;
    Ok(StatusCode::OK.into_response())
}

async fn e_json_tail_helper(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => accept(gadget),
        Err(_) => invalid_body(),
    }
}

async fn e_json_tail_value(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        Err(_) => Gadget::default(),
    };
    StatusCode::OK.into_response()
}

async fn e_json_alias(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, JsonRejection> {
    let captured = body;
    let Json(gadget) = captured?;
    Ok(StatusCode::OK.into_response())
}

async fn e_json_tail_result(body: Result<Json<Gadget>, JsonRejection>) -> Result<Json<Gadget>, JsonRejection> {
    body
}

async fn e_json_return_map(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, JsonRejection> {
    return body.map(|Json(gadget)| StatusCode::OK.into_response());
}

async fn e_json_shadowed(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let value = match body {
        Ok(Json(gadget)) => gadget,
        Err(_) => return StatusCode::OK.into_response(),
    };
    let body = Some(value);
    let gadget = body.unwrap();
    StatusCode::OK.into_response()
}

async fn e_json_default_guard(body: Result<Json<Gadget>, JsonRejection>) -> Json<Gadget> {
    if body.is_err() {
        return default_json();
    }
    body.unwrap()
}

async fn e_raw_shadowed(body: Bytes) -> Response {
    let body = b"{}";
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_raw_two_level(body: Bytes) -> Response {
    decode_outer(&body)
}

fn decode_outer(raw: &[u8]) -> Response {
    decode_inner(raw)
}

fn decode_inner(bytes: &[u8]) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_raw_recursive(body: Bytes) -> Response {
    decode_ping(&body, 3)
}

fn decode_ping(raw: &[u8], depth: u32) -> Response {
    decode_pong(raw, depth)
}

fn decode_pong(raw: &[u8], depth: u32) -> Response {
    if depth > 0 {
        return decode_ping(raw, depth - 1);
    }
    let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_json_handoff(body: Result<Json<Gadget>, JsonRejection>) -> Result<Response, JsonRejection> {
    consume_body(body)
}

fn consume_body(input: Result<Json<Gadget>, JsonRejection>) -> Result<Response, JsonRejection> {
    let Json(gadget) = input?;
    Ok(StatusCode::OK.into_response())
}

async fn e_json_handoff_unknown(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    elsewhere_defined(body)
}

async fn e_json_handoff_tolerant(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    peek_body(body)
}

fn peek_body(input: Result<Json<Gadget>, JsonRejection>) -> Response {
    let seen = input.is_ok();
    StatusCode::OK.into_response()
}

async fn e_raw_nested_argument(body: Bytes) -> Response {
    decode_normalized(&normalize_empty(&body))
}

fn normalize_empty(raw: &[u8]) -> Vec<u8> {
    if raw.is_empty() {
        return Vec::new();
    }
    raw.to_vec()
}

fn decode_normalized(raw: &[u8]) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_shared_query_key(Query(page): Query<StrictLimit>, Query(hint): Query<LooseLimit>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_conflicting_query_key(Query(page): Query<StrictLimit>, Query(named): Query<NamedLimit>) -> Response {
    StatusCode::OK.into_response()
}

struct StrictLimit {
    limit: u32,
}

struct LooseLimit {
    limit: Option<u32>,
}

struct NamedLimit {
    limit: String,
}

async fn e_raw_turbofish_helper(body: Bytes) -> Response {
    decode_as::<Gadget>(&body)
}

fn decode_as<T>(raw: &[u8]) -> Response {
    let value = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_err_observe_then_reject(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => StatusCode::OK.into_response(),
        Err(error) => {
            observe_rejection(error);
            invalid_body()
        }
    }
}

fn observe_rejection(error: JsonRejection) {}

fn scoped_alias_parse(raw: &[u8]) -> Gadget {
    use serde_json::from_slice as parse_scoped;
    parse_scoped(raw).unwrap()
}

async fn e_alias_out_of_scope(body: Bytes) -> Response {
    let size = parse_scoped(&body);
    StatusCode::OK.into_response()
}

fn parse_scoped(raw: &[u8]) -> usize {
    raw.len()
}

async fn e_matches_macro(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if matches!(body, Err(_)) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_unknown_macro(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    my_check!(body);
    StatusCode::OK.into_response()
}

async fn e_rebound_match(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let body = match body {
        Ok(Json(gadget)) => gadget,
        Err(_) => Gadget::default(),
    };
    let name = body.name.clone();
    StatusCode::OK.into_response()
}

fn local_query_user() {
    use axum::extract::Query as LocalQuery;
}

async fn e_local_query_elsewhere(filter: LocalQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_renamed_bytes(body: RequestBytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_type_alias_query(query: ApiQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_odd_alias_query(query: OddQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_transparent_body(Json(body): Json<Transparent>) -> Response {
    StatusCode::OK.into_response()
}

#[serde(transparent)]
struct Transparent {
    inner: String,
}

async fn e_comparison_argument(body: Bytes) -> Response {
    decode_limited(limit < maximum, &body)
}

fn decode_limited(small: bool, raw: &[u8]) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_or_guard(body: Bytes) -> Response {
    if !body.is_empty() && (feature_on() || fallback_on()) {
        let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    }
    StatusCode::OK.into_response()
}

async fn e_alias_chain_query(query: ChainQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_observed_fallback(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Json(gadget) = body.unwrap_or_else(|error| {
        observe_rejection(error);
        Json(Gadget::default())
    });
    StatusCode::OK.into_response()
}

async fn e_grouped_module_alias(body: Bytes) -> Response {
    let gadget = sj::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_group_query(query: NestedQ<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_foreign_query_alias(query: SignedQ<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_unreadable_alias_chain(query: OuterOdd<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_raw_unknown_helper(body: Bytes) -> Response {
    imported_decode(&body)
}

async fn e_tristate_field(Json(body): Json<Tristate>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_tristate_plain_option(Json(body): Json<TristatePlain>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_unknown_deserializer(Json(body): Json<CustomDeserializer>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_with_field(Json(body): Json<WithField>) -> Response {
    StatusCode::OK.into_response()
}

fn deserialize_tristate<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    T: serde::Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Some(Option::<T>::deserialize(deserializer)?))
}

struct Tristate {
    #[serde(default, deserialize_with = "deserialize_tristate")]
    note: Option<Option<String>>,
}

struct TristatePlain {
    #[serde(default, deserialize_with = "deserialize_tristate")]
    note: Option<String>,
}

struct CustomDeserializer {
    #[serde(deserialize_with = "parse_loosely")]
    note: String,
}

struct WithField {
    #[serde(with = "custom_format")]
    note: String,
}

async fn e_multiline_field_serde(Json(body): Json<MultilineSerde>) -> Response {
    StatusCode::OK.into_response()
}

struct MultilineSerde {
    name: String,
    #[serde(
        flatten
    )]
    extra: Extra,
}

async fn e_serde_keyword_values(Json(body): Json<KeywordValues>) -> Response {
    StatusCode::OK.into_response()
}

struct KeywordValues {
    #[serde(alias = "skip")]
    name: String,
    #[serde(alias = "default")]
    kind: String,
}

async fn e_borrowed_err_arm(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match &body {
        &Err(ref error) => return invalid_body(),
        &Ok(_) => StatusCode::OK.into_response(),
    }
}

async fn e_grouped_err_arm(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => StatusCode::OK.into_response(),
        (Err(error)) => invalid_body(),
    }
}

async fn e_borrowed_if_let_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if let &Err(ref error) = &body {
        return invalid_body();
    }
    StatusCode::OK.into_response()
}

async fn e_raw_associated_helper(body: Bytes) -> Response {
    GadgetDecoder::decode_strict(&body)
}

async fn e_raw_method_helper(body: Bytes, decoder: GadgetDecoder) -> Response {
    decoder.decode_owned(&body)
}

async fn e_mut_borrow_parse(mut body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&mut body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_partial_slice_parse(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body[4..]).unwrap();
    StatusCode::OK.into_response()
}

type SignedQuery<T> = crate::signed::Query<T>;

async fn e_foreign_qualified_query(query: crate::signed::Query<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_inline_axum_query(axum::extract::Query(cursor): axum::extract::Query<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_signed_type_alias(query: SignedQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_shadowed_decode_alias(body: Bytes) -> Result<Response, Response> {
    let decode = |raw: &[u8]| -> Result<Gadget, Response> { Err(invalid_body()) };
    let gadget: Gadget = decode(&body)?;
    Ok(StatusCode::OK.into_response())
}

struct Envelope {
    bytes: Bytes,
}

fn decode_envelope(envelope: Envelope) -> Response {
    StatusCode::OK.into_response()
}

fn record_size(size: usize) {}

async fn e_wrapped_carrier(body: Bytes) -> Response {
    decode_envelope(Envelope { bytes: body })
}

async fn e_nonbyte_param(body: Bytes) -> Response {
    record_size(body);
    StatusCode::OK.into_response()
}

async fn e_scalar_read_argument(body: Bytes) -> Response {
    record_size(body.len());
    StatusCode::OK.into_response()
}

async fn e_nested_or_inspection(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_ok() && (feature_on() || fallback_on()) {
        StatusCode::OK.into_response()
    } else {
        invalid_body()
    }
}

async fn e_or_and_empty_guard(body: Bytes) -> Response {
    if body.is_empty() || feature_on() && fallback_on() {
        return StatusCode::NO_CONTENT.into_response();
    }
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_and_error_exit(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() || (feature_on() && fallback_on()) {
        return StatusCode::OK.into_response();
    }
    let Json(gadget) = body.unwrap();
    StatusCode::OK.into_response()
}

trait DecodeExt {
    fn decode_body<T>(&self) -> T;
}

impl DecodeExt for Bytes {
    fn decode_body<T>(&self) -> T {
        serde_json::from_slice::<Gadget>(self).unwrap()
    }
}

async fn e_extension_decode(body: Bytes) -> Response {
    let gadget = body.decode_body::<Gadget>();
    StatusCode::OK.into_response()
}

type RawBody = Vec<u8>;
type RawSlice = [u8];
type Opaque<T> = Wrapper<T>;

async fn e_empty_rejected_tolerant(body: Bytes) -> Response {
    if body.is_empty() {
        return Err(reject());
    }
    let gadget: Gadget = serde_json::from_slice(&body).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_empty_rejected_helper(body: Bytes) -> Response {
    if body.is_empty() {
        return Err(reject());
    }
    let gadget = parse_gadget(&body).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_empty_else_rejected(body: Bytes) -> Response {
    if !body.is_empty() {
        let gadget: Gadget = serde_json::from_slice(&body).unwrap_or_default();
    } else {
        return Err(reject());
    }
    StatusCode::OK.into_response()
}

async fn e_option_ok_or(body: Option<Json<Gadget>>) -> Response {
    let Json(gadget) = body.ok_or(reject())?;
    StatusCode::OK.into_response()
}

async fn e_option_expect(body: Option<Json<Gadget>>) -> Response {
    let Json(gadget) = body.expect("a body");
    StatusCode::OK.into_response()
}

async fn e_option_match(body: Option<Json<Gadget>>) -> Response {
    match body {
        Some(Json(gadget)) => {}
        None => return Err(reject()),
    }
    StatusCode::OK.into_response()
}

async fn e_option_let_else(body: Option<Json<Gadget>>) -> Response {
    let Some(Json(gadget)) = body else {
        return Err(reject());
    };
    StatusCode::OK.into_response()
}

async fn e_option_if_none(body: Option<Json<Gadget>>) -> Response {
    if body.is_none() {
        return Err(reject());
    }
    StatusCode::OK.into_response()
}

async fn e_option_bytes(body: Option<Bytes>) -> Response {
    let raw = body.ok_or_else(reject)?;
    StatusCode::OK.into_response()
}

async fn e_option_unread_use(body: Option<Json<Gadget>>) -> Response {
    tracing::debug!(present = ?body);
    StatusCode::OK.into_response()
}

async fn e_option_body_default(body: Option<Json<Gadget>>) -> Response {
    let gadget = body.map(|Json(gadget)| gadget).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_result_bytes(body: Result<Bytes, BytesRejection>) -> Response {
    let raw = body?;
    let gadget = serde_json::from_slice::<Gadget>(&raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_option_bytes_match(body: Option<Bytes>) -> Response {
    match body {
        Some(raw) => {
            let gadget = serde_json::from_slice::<Gadget>(&raw).unwrap();
        }
        None => {}
    }
    StatusCode::OK.into_response()
}

async fn e_result_bytes_if_let(body: Result<Bytes, BytesRejection>) -> Response {
    if let Ok(raw) = body {
        let gadget: Gadget = serde_json::from_slice(&raw).unwrap_or_default();
    }
    StatusCode::OK.into_response()
}

async fn e_result_bytes_tolerant(body: Result<Bytes, BytesRejection>) -> Response {
    let raw = body?;
    let gadget: Gadget = serde_json::from_slice(&raw).unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_option_bytes_unwrapped(body: Option<Bytes>) -> Response {
    let raw = body.unwrap_or_default();
    let gadget = serde_json::from_slice::<Gadget>(&raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_tuple_carrier(body: Bytes) -> Response {
    let (bytes,) = (body,);
    let gadget = serde_json::from_slice::<Gadget>(&bytes).unwrap();
    StatusCode::OK.into_response()
}

async fn e_struct_carrier(body: Bytes) -> Response {
    let Holder { raw } = Holder { raw: body };
    let gadget = serde_json::from_slice::<Gadget>(&raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_rebound_carrier(body: Bytes) -> Response {
    let (body, extra) = (body, 1);
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_rebound_other(body: Bytes) -> Response {
    let (body, extra) = (other_value(), 1);
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_dead_closure(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_dropped_closure(body: Bytes) -> Response {
    let _ = || serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_dead_helper_closure(body: Bytes) -> Response {
    let later = || parse_gadget(&body);
    StatusCode::OK.into_response()
}

async fn e_called_closure(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body).unwrap();
    let gadget = parse();
    StatusCode::OK.into_response()
}

async fn e_stored_closure(body: Bytes) -> Response {
    let parse = || serde_json::from_slice::<Gadget>(&body).unwrap();
    register(parse);
    StatusCode::OK.into_response()
}

async fn e_alias_body(body: RawBody) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

fn parse_raw_slice(raw: &RawSlice) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
    StatusCode::OK.into_response()
}

async fn e_alias_helper(body: Bytes) -> Response {
    parse_raw_slice(&body)
}

async fn e_opaque_body(body: Opaque<u8>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

async fn e_unknown_type_body(body: other_crate::Blob) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

type ParcelAlias = Parcel;

async fn e_alias_conversion(body: Bytes) -> Response {
    let parcel: ParcelAlias = body.into();
    StatusCode::OK.into_response()
}

mod shapes {
    struct Blob {
        size: u32,
    }
}

type Blob<T> = Wrapper<T, u8>;

async fn e_struct_named_alias(body: Blob<u8>) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
    StatusCode::OK.into_response()
}

impl Inspector {
    fn peek_body(&self) -> Response {
        StatusCode::OK.into_response()
    }
}

async fn e_unrelated_receiver(body: Bytes) -> Response {
    body.peek_body()
}

trait DecodeSlice {
    fn decode_slice(&self) -> Response;
}

impl DecodeSlice for [u8] {
    fn decode_slice(&self) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(self).unwrap();
        StatusCode::OK.into_response()
    }
}

fn read_slice(raw: &[u8]) -> Response {
    raw.decode_slice()
}

async fn e_slice_receiver(body: Bytes) -> Response {
    read_slice(&body)
}

mod spoofed_parse {
    use crate::fake as serde_json;

    async fn e_spoofed_parse(body: Bytes) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(&body).unwrap();
        StatusCode::OK.into_response()
    }
}

async fn e_grouped_receiver(body: Bytes) -> Response {
    let gadget = (body).decode_body::<Gadget>();
    StatusCode::OK.into_response()
}

async fn e_borrowed_receiver(body: Bytes) -> Response {
    let gadget = (&body).decode_body::<Gadget>();
    StatusCode::OK.into_response()
}

async fn e_nested_receiver(body: Bytes) -> Response {
    let gadget = (&(body)).decode_body::<Gadget>();
    StatusCode::OK.into_response()
}

async fn e_sliced_receiver(body: Bytes) -> Response {
    let gadget = (&body[1..]).decode_body::<Gadget>();
    StatusCode::OK.into_response()
}

async fn e_grouped_argument(body: Bytes) -> Response {
    let gadget = parse_gadget((&body))?;
    StatusCode::OK.into_response()
}

async fn e_unknown_receiver_method(body: Bytes) -> Response {
    let gadget = body.decode_elsewhere::<Gadget>();
    StatusCode::OK.into_response()
}

async fn e_accessor_receiver(body: Bytes) -> Response {
    let size = body.len();
    let copy = body.to_vec();
    StatusCode::OK.into_response()
}

async fn e_raw_std_reader(body: Bytes) -> Response {
    let text = std::str::from_utf8(&body).unwrap_or("");
    StatusCode::OK.into_response()
}

async fn e_raw_aliased_std_reader(body: Bytes) -> Response {
    let text = text_mod::from_utf8(&body).unwrap_or("");
    StatusCode::OK.into_response()
}

mod std_leaf {
    use std::str::from_utf8;

    async fn e_raw_imported_std_reader(body: Bytes) -> Response {
        let text = from_utf8(&body).unwrap_or("");
        StatusCode::OK.into_response()
    }
}

async fn e_raw_unknown_associated(body: Bytes) -> Response {
    Codec::decode_bytes(&body)
}

async fn e_raw_unknown_method(body: Bytes, codec: Codec) -> Response {
    codec.decode_elsewhere(&body)
}

impl GadgetDecoder {
    fn decode_strict(raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }

    fn decode_owned(&self, raw: &[u8]) -> Response {
        let gadget = serde_json::from_slice::<Gadget>(raw).unwrap();
        StatusCode::OK.into_response()
    }
}

async fn e_helper_in_comment() -> Response {
    // teapot() is not called here.
    let note = "teapot() is not called either";
    StatusCode::OK.into_response()
}

fn teapot() -> Response {
    StatusCode::IM_A_TEAPOT.into_response()
}

async fn e_json_std_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        std::result::Result::Ok(Json(gadget)) => StatusCode::OK.into_response(),
        std::result::Result::Err(e) => return e.into_response(),
    }
}

async fn e_json_core_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        ::core::result::Result::Ok(Json(gadget)) => StatusCode::OK.into_response(),
        Result::Err(_) => invalid_body(),
    }
}

async fn e_qualified_option_body(Json(body): Json<QualifiedNote>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_qualified_option_query(Query(query): Query<QualifiedPage>) -> Response {
    StatusCode::OK.into_response()
}

struct QualifiedNote {
    name: String,
    note: std::option::Option<String>,
}

struct QualifiedPage {
    page: ::core::option::Option<u32>,
}

async fn e_is_err_tail_helper(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() {
        invalid_body()
    } else {
        StatusCode::OK.into_response()
    }
}

async fn e_helper_default(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap_or_else(|_| default_gadget());
    StatusCode::OK.into_response()
}

async fn e_unknown_helper(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).unwrap_or_else(|_| undefined_helper());
    StatusCode::OK.into_response()
}

async fn e_uuid_bytes(body: Bytes) -> Response {
    let id_bytes = [0u8; 16];
    let id = Uuid::from_slice(&id_bytes);
    StatusCode::OK.into_response()
}

async fn e_option_fallback(body: Bytes) -> Response {
    serde_json::from_slice::<Gadget>(&body).ok().map_or_else(|| invalid_body(), |gadget| accept(gadget))
}

async fn e_option_default(body: Bytes) -> Response {
    let gadget = serde_json::from_slice::<Gadget>(&body).ok().unwrap_or_else(Default::default);
    StatusCode::OK.into_response()
}

async fn e_commented_parse(body: Bytes) -> Response {
    // serde_json::from_slice::<Gadget>(&body)?
    /* serde_json::from_slice::<Gadget>(&body)? */
    let note = "serde_json::from_slice::<Gadget>(&body)? {";
    let raw = r#"serde_json::from_slice::<Gadget>(&body)"#;
    let brace = '{';
    StatusCode::OK.into_response()
}

async fn e_let_else_json(body: Result<Json<Gadget>, JsonRejection>) -> Json<Gadget> {
    let Ok(Json(gadget)) = body else {
        return Json(Gadget::default());
    };
    Json(gadget)
}

async fn e_map_or_helper(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    body.map_or(invalid_body(), |Json(gadget)| accept(gadget))
}

async fn e_match_mut(mut body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match &mut body {
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        Ok(Json(gadget)) => gadget.name.clear(),
    }
    StatusCode::OK.into_response()
}

async fn e_await_default(body: Bytes) -> Response {
    let gadget = parse_gadget_async(&body).await.unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn parse_gadget_async(body: &Bytes) -> Result<Gadget, serde_json::Error> {
    serde_json::from_slice(body)
}

async fn e_borrowed_let_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Ok(Json(gadget)) = &body else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    StatusCode::OK.into_response()
}

async fn e_as_ref_if_let_err(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if let Err(_) = body.as_ref() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    StatusCode::OK.into_response()
}

async fn e_is_err_and(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_err_and(|_| false) {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_is_err_or(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_err() || force_reject() {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_is_ok_and_then(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_ok() && ready() {
        tracing::debug!("valid");
    } else {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_is_ok_and_or(body: Bytes) -> Result<Response, Response> {
    if serde_json::from_slice::<Gadget>(&body).is_ok() && ready() || lenient() {
        tracing::debug!("valid");
    } else {
        return Err(reject());
    }
    Ok(StatusCode::OK.into_response())
}

async fn e_exit_then_unwrap(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() {
        return StatusCode::NO_CONTENT.into_response();
    }
    let Json(gadget) = body.unwrap();
    StatusCode::OK.into_response()
}

async fn e_log_then_unwrap(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() {
        tracing::warn!("no body");
    }
    let Json(gadget) = body.unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_comment(body: Bytes) -> Response {
    /* outer /* nested */ serde_json::from_slice::<Gadget>(&body)?; */
    StatusCode::OK.into_response()
}

async fn e_raw_alias(body: Bytes) -> Result<Response, Response> {
    let captured = body;
    let gadget = serde_json::from_slice::<Gadget>(&captured).map_err(|_| reject())?;
    Ok(StatusCode::OK.into_response())
}

async fn e_json_helper_default(body: Result<Json<Gadget>, JsonRejection>) -> Json<Gadget> {
    match body {
        Ok(Json(gadget)) => Json(gadget),
        Err(_) => default_json(),
    }
}

fn default_json() -> Json<Gadget> {
    Json(Gadget::default())
}

async fn e_let_err_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Err(rejection) = body else {
        return StatusCode::OK.into_response();
    };
    rejection.into_response()
}

async fn e_aliased_decode(body: Bytes) -> Result<Response, Response> {
    let gadget = decode::<Gadget>(&body).map_err(|_| reject())?;
    Ok(StatusCode::OK.into_response())
}

async fn e_module_alias(body: Bytes) -> Result<Response, Response> {
    let gadget = json::from_slice::<Gadget>(&body).map_err(|_| reject())?;
    Ok(StatusCode::OK.into_response())
}

async fn e_ok_return_then_reject(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_ok() {
        return StatusCode::OK.into_response();
    }
    StatusCode::BAD_REQUEST.into_response()
}

async fn e_raw_guarded_arms(body: Bytes) -> Response {
    let gadget = match serde_json::from_slice::<Gadget>(&body) {
        Ok(gadget) => gadget,
        Err(e) if !e.is_eof() => return StatusCode::BAD_REQUEST.into_response(),
        Err(_) => Gadget::default(),
    };
    StatusCode::OK.into_response()
}

async fn e_stored_parse(body: Bytes) -> Response {
    let parsed = serde_json::from_slice::<Gadget>(&body);
    let gadget = parsed.unwrap_or_default();
    StatusCode::OK.into_response()
}

async fn e_stored_parse_strict(body: Bytes) -> Result<Response, Response> {
    let parsed = serde_json::from_slice::<Gadget>(&body);
    let gadget = parsed.map_err(|_| reject())?;
    Ok(StatusCode::OK.into_response())
}

async fn e_or_exit_then_unwrap(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    if body.is_err() || maintenance() {
        return StatusCode::NO_CONTENT.into_response();
    }
    let Json(gadget) = body.unwrap();
    StatusCode::OK.into_response()
}

async fn e_nested_param_comment(/* outer /* inner */ ghost: Json<Gadget>, */) -> Response {
    StatusCode::OK.into_response()
}

async fn e_local_from_slice(body: Bytes) -> Response {
    let gadget = from_slice::<Gadget>(&body);
    StatusCode::OK.into_response()
}

fn from_slice<T>(raw: &[u8]) -> Option<T> {
    None
}

async fn e_closure_return_reject(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    body.map_or_else(|_| { return invalid_body(); }, |Json(gadget)| accept(gadget))
}

async fn e_multiline_let_else(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let Ok(Json(Gadget {
        name,
    })) = body else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    StatusCode::OK.into_response()
}

async fn e_closure_return_value(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = body.map(|Json(gadget)| gadget).unwrap_or_else(|_| {
        return Gadget::default();
    });
    accept(gadget)
}

async fn e_commented_unwrap(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    // body.unwrap() would reject a missing body.
    let _note = "body.unwrap()";
    match body {
        Ok(Json(gadget)) => accept(gadget),
        Err(_) => StatusCode::OK.into_response(),
    }
}

async fn e_ref_catch_all(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = match body {
        Ok(Json(gadget)) => gadget,
        ref failed => return StatusCode::BAD_REQUEST.into_response(),
    };
    accept(gadget)
}

async fn e_ref_err_handed(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    match body {
        Ok(Json(gadget)) => accept(gadget),
        Err(ref rejection) => replay(rejection),
    }
}

async fn e_nested_struct_comment(Json(haunted): Json<Haunted>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_short_helper(body: Bytes) -> Response {
    let gadget = match dec(&body) {
        Ok(gadget) => gadget,
        Err(response) => return response,
    };
    StatusCode::OK.into_response()
}

fn dec(body: &Bytes) -> Result<Gadget, Response> {
    serde_json::from_slice::<Gadget>(body).map_err(|_| StatusCode::BAD_REQUEST.into_response())
}

async fn e_response_config(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = body.map(|Json(gadget)| gadget).unwrap_or_else(|_| default_response_config());
    StatusCode::OK.into_response()
}

fn default_response_config() -> ResponseConfig {
    ResponseConfig::default()
}

async fn e_suffix_error(body: Result<Json<Gadget>, JsonRejection>) -> Response {
    let gadget = body.map(|Json(gadget)| gadget).unwrap_or_else(|_| gadget_error());
    StatusCode::OK.into_response()
}

fn gadget_error() -> GadgetError {
    GadgetError::default()
}

async fn e_commented_status() -> Response {
    // StatusCode::IM_A_TEAPOT is never sent.
    let _note = "StatusCode::IM_A_TEAPOT";
    StatusCode::OK.into_response()
}

async fn e_std_result(body: std::result::Result<Json<Gadget>, JsonRejection>) -> Response {
    let Json(gadget) = body.unwrap();
    StatusCode::OK.into_response()
}

async fn e_aliased_query(AxumQuery(cursor): AxumQuery<Cursor>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_aliased_json(AxumJson(gadget): AxumJson<Gadget>) -> Response {
    StatusCode::OK.into_response()
}

async fn e_std_result_query(query: std::result::Result<Query<Cursor>, QueryRejection>) -> Response {
    StatusCode::OK.into_response()
}

fn invalid_body() -> Response {
    rejection_response()
}

fn default_gadget() -> Gadget {
    Gadget::default()
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

struct Haunted {
    name: String,
    /* outer /* nested */ ghost: String, */
    /*
    phantom: String,
    */
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
    #[serde(alias = "default_kind")]
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
                request_body=body_of(("name", True), required=False),
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
        "a hand-parsed query key is documented only by an in: query parameter",
        FIXTURE_STATUS,
        [
            fixture_route(
                "GET",
                "/s/uri-path-key/{id}",
                200,
                params=[{"name": "id", "in": "path", "type": "string", "required": True}],
            )
        ],
        {"query_keys": ["GET /s/uri-path-key/{id}: `id` is accepted by the query parser"]},
    ),
    (
        "every trusted raw reader has its hand-parsed query keys read",
        FIXTURE_STATUS,
        [
            fixture_route("GET", path, 200, params=[query_param("limit", "integer", False)])
            for path in ("/s/uri-list", "/s/request-list", "/s/parts-list")
        ],
        {
            "query_keys": [
                "GET /s/uri-list: `sort` is accepted by the query parser",
                "GET /s/request-list: `sort` is accepted by the query parser",
                "GET /s/parts-list: `sort` is accepted by the query parser",
            ]
        },
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
        "a let that borrows or views the body is a carrier alias",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in ("/e/borrowed-alias", "/e/view-alias")
        ],
        {
            "body_required": [
                "POST /e/borrowed-alias: the body is mandatory",
                "POST /e/view-alias: the body is mandatory",
            ],
            "mandatory": [
                "POST /e/borrowed-alias: `name` is mandatory in Gadget",
                "POST /e/view-alias: `name` is mandatory in Gadget",
            ],
        },
    ),
    (
        "a let that holds part of the body makes its later parse unresolved",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in ("/e/partial-alias", "/e/copied-alias")
        ]
        + [fixture_route("POST", "/e/arm-rebinds", 200, request_body=GADGET_BODY)],
        {
            "body_required": [
                "POST /e/partial-alias: the body is mandatory",
                "POST /e/copied-alias: the body is mandatory",
            ],
            "unresolved": [
                "POST /e/partial-alias: cannot read a `from_slice` call",
                "POST /e/copied-alias: cannot read a `from_slice` call",
            ],
        },
    ),
    (
        "a standard path to Vec<u8> is a body carrier",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in ("/e/std-vec-body", "/e/alloc-vec-body")
        ],
        {
            "body_required": [
                "POST /e/std-vec-body: the body is mandatory",
                "POST /e/alloc-vec-body: the body is mandatory",
            ],
            "mandatory": [
                "POST /e/std-vec-body: `name` is mandatory in Gadget",
                "POST /e/alloc-vec-body: `name` is mandatory in Gadget",
            ],
        },
    ),
    (
        "a conversion of the body runs its From impl, or fails closed",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in ("/e/into-typed", "/e/try-into-typed", "/e/into-untyped", "/e/from-path")
        ],
        {
            "body_required": [
                "POST /e/into-typed: the body is mandatory",
                "POST /e/try-into-typed: the body is mandatory",
                "POST /e/into-untyped: the body is mandatory",
                "POST /e/from-path: the body is mandatory",
            ],
            "mandatory": [
                "POST /e/into-typed: `name` is mandatory in Gadget",
                "POST /e/try-into-typed: `name` is mandatory in Gadget",
                "POST /e/from-path: `name` is mandatory in Gadget",
            ],
            "unresolved": ["POST /e/into-untyped: cannot read a `from_slice` call"],
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
        {
            "undocumented": ["POST /e/empty-fields: `name` is accepted by Gadget"],
            "mandatory": ["POST /e/empty-fields: `name` is mandatory in Gadget"],
        },
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
        "a guard around a helper call keeps the helper parse optional",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/call-guard", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {},
    ),
    (
        "a guarded body still needs its mandatory fields",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/guarded-fields",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {"mandatory": ["POST /e/guarded-fields: `name` is mandatory in Gadget"]},
    ),
    (
        "map_err without a ? does not reject",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/map-err-ok", 200, request_body=body_of(("name", False), required=False)
            )
        ],
        {},
    ),
    (
        "an Err arm that returns the rejection itself rejects",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/return-rejection",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/return-rejection: the body is mandatory"]},
    ),
    (
        "a present Option<Json<T>> body needs its mandatory fields",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/empty-fields",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {"mandatory": ["POST /e/empty-fields: `name` is mandatory in Gadget"]},
    ),
    (
        "a return inside a closure is no early exit",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/closure-return",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/closure-return: the body is mandatory"]},
    ),
    (
        "an Option<Query<T>> makes every field optional",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET",
                "/e/optional-strict-query",
                200,
                params=[
                    query_param("term", "string", False),
                    query_param("limit", "integer", False),
                ],
            )
        ],
        {},
    ),
    (
        "unwrap or expect on a Result<Json<T>> body rejects",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/unwrapped", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/unwrapped: the body is mandatory"]},
    ),
    (
        "a borrowed match on a Result<Json<T>> body is read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/borrowed-match",
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
        ],
        {"body_required": ["POST /e/borrowed-match: the body is mandatory"]},
    ),
    (
        "an early return after the parse is no guard",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/parse-then-return",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/parse-then-return: the body is mandatory"]},
    ),
    (
        "only the top-level Err arm decides a Result<Json<T>> body",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/nested-err", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/nested-err: the body is mandatory"]},
    ),
    (
        "each helper parameter keeps the state of the calls that fill it",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/split-calls",
                200,
                request_body=body_of(("name", True), ("offset", False), required=False),
            )
        ],
        {},
    ),
    (
        "a byte parameter with a lifetime carries the body",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/lifetime", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/lifetime: the body is mandatory"]},
    ),
    (
        "a conditional early return is no guard",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/conditional-return",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/conditional-return: the body is mandatory"]},
    ),
    (
        "every top-level Err arm is read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/second-err", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/second-err: the body is mandatory"]},
    ),
    (
        "a combinator chain ending in ? rejects, one ending in ok does not",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/combinator", 200, request_body=body_of(("name", True), required=False)
            ),
            fixture_route(
                "POST",
                "/e/tolerant-chain",
                200,
                request_body=body_of(("name", False), required=False),
            ),
        ],
        {"body_required": ["POST /e/combinator: the body is mandatory"]},
    ),
    (
        "an empty-body test inside another if does not guard",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/nested-guard", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/nested-guard: the body is mandatory"]},
    ),
    (
        "an error turned back from ok is not tolerated",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/ok-then-err", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/ok-then-err: the body is mandatory"]},
    ),
    (
        "a field type wrapped over several lines is read as one field",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET", "/e/wrapped-field", 200, params=[query_param("page", "integer", False)]
            )
        ],
        {},
    ),
    (
        "an empty-body test joined by && is no guard, one joined by || is",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/compound-guard",
                200,
                request_body=body_of(("name", True), required=False),
            ),
            fixture_route(
                "POST", "/e/either-guard", 200, request_body=body_of(("name", True), required=False)
            ),
        ],
        {"body_required": ["POST /e/compound-guard: the body is mandatory"]},
    ),
    (
        "a parse inspected with is_ok is tolerant",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/inspected", 200, request_body=body_of(("name", False), required=False)
            )
        ],
        {},
    ),
    (
        "a helper reached by a guarded call and a tolerant call is optional",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/mixed-calls", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {},
    ),
    (
        "an empty-body test compared to a value is no guard",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/compared-guard",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/compared-guard: the body is mandatory"]},
    ),
    (
        "an else that returns success does not reject",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", False), required=False),
                additional_responses=[{"status": 204}],
            )
            for path in ("/e/let-else-ok", "/e/if-let-else-ok")
        ],
        {},
    ),
    (
        "an else arm is no guard when the empty-body arm rejects",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/empty-arm-rejects",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/empty-arm-rejects: the body is mandatory"]},
    ),
    (
        "an if let Ok(Json(..)) whose else rejects makes the body mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", "/e/if-let-json", 200, request_body=body_of(("name", True), required=False)
            )
        ],
        {"body_required": ["POST /e/if-let-json: the body is mandatory"]},
    ),
    (
        "a raw let-else that returns success and a map_or chain are tolerant",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", False), required=False),
                additional_responses=[{"status": 204}],
            )
            for path in ("/e/raw-let-else", "/e/map-or")
        ],
        {},
    ),
    (
        "an inspection that decides a rejecting branch is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/is-err-reject", "/e/is-ok-reject")
        ],
        {
            "body_required": [
                "POST /e/is-err-reject: the body is mandatory",
                "POST /e/is-ok-reject: the body is mandatory",
            ]
        },
    ),
    (
        "a non-2xx status outside the common names rejects and is reported",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/teapot-else", "/e/teapot-arm")
        ],
        {
            "statuses": [
                "POST /e/teapot-else returns 418, undeclared",
                "POST /e/teapot-arm returns 405, undeclared",
            ],
            "body_required": [
                "POST /e/teapot-else: the body is mandatory",
                "POST /e/teapot-arm: the body is mandatory",
            ],
        },
    ),
    (
        "a declared non-2xx status outside the common names is not reported",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/teapot-else",
                200,
                request_body=body_of(("name", True), required=True),
                error_responses=[{"status": 418}],
            )
        ],
        {},
    ),
    (
        "a raw match whose Err arm lets the request through is tolerant",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/raw-match-ok",
                200,
                request_body=body_of(("name", False), required=False),
                additional_responses=[{"status": 204}],
            )
        ],
        {},
    ),
    (
        "a raw match whose Err arm rejects is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/raw-match-reject",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/raw-match-reject: the body is mandatory"]},
    ),
    (
        "a Result<Query<T>> is optional when its error is tolerated, strict when it rejects",
        FIXTURE_EDGES,
        [
            fixture_route("GET", path, 200, params=[query_param("kind", "string", False)])
            for path in ("/e/query-result-ok", "/e/query-result-reject")
        ],
        {"query_params": ["GET /e/query-result-reject: `kind` is mandatory in Documented"]},
    ),
    (
        "a fallback that rejects the parse error is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/map-or-else-reject", "/e/json-map-or-else", "/e/fn-ref-fallback")
        ],
        {
            "body_required": [
                "POST /e/map-or-else-reject: the body is mandatory",
                "POST /e/json-map-or-else: the body is mandatory",
                "POST /e/fn-ref-fallback: the body is mandatory",
            ]
        },
    ),
    (
        "a closure fallback that builds a value is tolerant",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/closure-default",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "an inspection or a catch-all arm that rejects a Result extractor is strict",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
            for path in ("/e/json-is-err", "/e/json-wildcard")
        ]
        + [
            fixture_route(
                "GET",
                "/e/query-is-err",
                200,
                params=[query_param("kind", "string", False)],
                error_responses=[{"status": 400}],
            )
        ],
        {
            "body_required": [
                "POST /e/json-is-err: the body is mandatory",
                "POST /e/json-wildcard: the body is mandatory",
            ],
            "query_params": ["GET /e/query-is-err: `kind` is mandatory in Documented"],
        },
    ),
    (
        "a raw match whose catch-all arm lets the request through is tolerant",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/raw-wildcard-ok",
                200,
                request_body=body_of(("name", False), required=False),
                additional_responses=[{"status": 204}],
            )
        ],
        {},
    ),
    (
        "an if let Err branch that rejects a Result extractor is strict",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/json-if-let-err",
                200,
                request_body=body_of(("name", True), required=False),
            ),
            fixture_route(
                "GET",
                "/e/query-if-let-err",
                200,
                params=[query_param("kind", "string", False)],
                error_responses=[{"status": 400}],
            ),
            fixture_route(
                "POST",
                "/e/json-if-let-err-ok",
                200,
                request_body=body_of(("name", True), required=False),
            ),
        ],
        {
            "body_required": ["POST /e/json-if-let-err: the body is mandatory"],
            "query_params": ["GET /e/query-if-let-err: `kind` is mandatory in Documented"],
        },
    ),
    (
        "a negated guard whose empty-body arm rejects is no guard",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/negated-else-rejects",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/negated-else-rejects: the body is mandatory"]},
    ),
    (
        "a named Json<T> is read and mandatory, as is an at-binding Err arm that rejects",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
            for path in ("/e/named-json", "/e/at-binding")
        ],
        {
            "body_required": [
                "POST /e/named-json: the body is mandatory",
                "POST /e/at-binding: the body is mandatory",
            ]
        },
    ),
    (
        "an early return of a 2xx StatusCode lets an empty body through",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/status-early-return",
                200,
                request_body=body_of(("name", True), required=False),
                additional_responses=[{"status": 204}],
                error_responses=[{"status": 400}],
            )
        ],
        {},
    ),
    (
        "an Err arm that returns a helper not given the error rejects",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/json-helper-exit",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/json-helper-exit: the body is mandatory"]},
    ),
    (
        "an or_else that always recovers makes a later ? tolerant",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", False), required=False))
            for path in ("/e/json-or-else", "/e/raw-or-else")
        ],
        {},
    ),
    (
        "an or_else that can still fail leaves a later ? rejecting",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/raw-or-else-err",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/raw-or-else-err: the body is mandatory"]},
    ),
    (
        "a fallback that calls a helper or can still fail is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/ignored-error-helper", "/e/or-else-branch")
        ],
        {
            "body_required": [
                "POST /e/ignored-error-helper: the body is mandatory",
                "POST /e/or-else-branch: the body is mandatory",
            ]
        },
    ),
    (
        "an or_else block whose value is Ok(..) recovers",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/or-else-block",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "a tail helper call in an Err arm and a moved extractor both reject",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/json-tail-helper", "/e/json-alias")
        ],
        {
            "body_required": [
                "POST /e/json-tail-helper: the body is mandatory",
                "POST /e/json-alias: the body is mandatory",
            ]
        },
    ),
    (
        "a Result extractor returned as the handler value rejects",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/json-tail-result", "/e/json-return-map")
        ],
        {
            "body_required": [
                "POST /e/json-tail-result: the body is mandatory",
                "POST /e/json-return-map: the body is mandatory",
            ]
        },
    ),
    (
        "a shadowed extractor name is no extractor use",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/json-shadowed",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "an is_err guard that returns a helper's success value recovers",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/json-default-guard",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "a shadowed raw-body name is no request body",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/raw-shadowed",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "a raw body passed through two helpers, or a recursive one, is read",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/raw-two-level", "/e/raw-recursive")
        ],
        {
            "body_required": [
                "POST /e/raw-two-level: the body is mandatory",
                "POST /e/raw-recursive: the body is mandatory",
            ]
        },
    ),
    (
        "a Result extractor handed to a helper is read in the helper",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/json-handoff", "/e/json-handoff-unknown")
        ]
        + [
            fixture_route(
                "POST",
                "/e/json-handoff-tolerant",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {
            "body_required": [
                "POST /e/json-handoff: the body is mandatory",
                "POST /e/json-handoff-unknown: the body is mandatory",
            ]
        },
    ),
    (
        "a raw body inside another call's argument is not passed to the outer call",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/raw-nested-argument",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "a query key is required when any strict extractor requires it",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET",
                "/e/shared-query-key",
                200,
                params=[query_param("limit", "integer", True)],
            ),
            fixture_route(
                "GET",
                "/e/conflicting-query-key",
                200,
                params=[query_param("limit", "integer", True)],
            ),
        ],
        {
            "query_params": [
                "GET /e/conflicting-query-key: `limit` has conflicting types",
            ]
        },
    ),
    (
        "a helper called with a turbofish gets the raw body",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/raw-turbofish-helper",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/raw-turbofish-helper: the body is mandatory"]},
    ),
    (
        "an Err arm that hands its error on and then rejects is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/err-observe-then-reject",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/err-observe-then-reject: the body is mandatory"]},
    ),
    (
        "a from_slice alias in another function is out of scope",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/alias-out-of-scope",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "a Result extractor in a macro is strict",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
            for path in ("/e/matches-macro", "/e/unknown-macro")
        ],
        {
            "body_required": [
                "POST /e/matches-macro: the body is mandatory",
                "POST /e/unknown-macro: the body is mandatory",
            ]
        },
    ),
    (
        "a name rebound to a match value is no longer the extractor",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/rebound-match",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "an extractor alias in another function is out of scope",
        FIXTURE_EDGES,
        [fixture_route("GET", "/e/local-query-elsewhere", 200, params=[])],
        {},
    ),
    (
        "an imported alias of Bytes carries the raw body",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/renamed-bytes",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/renamed-bytes: the body is mandatory"]},
    ),
    (
        "a type alias of Query is read, and one the audit cannot read is reported",
        FIXTURE_EDGES,
        [
            fixture_route("GET", "/e/type-alias-query", 200, params=[]),
            fixture_route("GET", "/e/odd-alias-query", 200, params=[]),
        ],
        {
            "query_params": ["GET /e/type-alias-query: `offset` is accepted by Cursor"],
            "unresolved": ["GET /e/odd-alias-query: cannot read the `OddQuery` type alias"],
        },
    ),
    (
        "a serde container attribute that changes the layout is unreadable",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/transparent-body",
                200,
                request_body=body_of(("inner", True)),
            )
        ],
        {"unresolved": ["POST /e/transparent-body: cannot read `transparent` in Transparent"]},
    ),
    (
        "a comparison in an earlier argument does not swallow the body argument",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/comparison-argument",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/comparison-argument: the body is mandatory"]},
    ),
    (
        "a nested || inside an is_empty guard keeps the guard",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/nested-or-guard",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {},
    ),
    (
        "an alias of an alias of Query is read",
        FIXTURE_EDGES,
        [fixture_route("GET", "/e/alias-chain-query", 200, params=[])],
        {"query_params": ["GET /e/alias-chain-query: `offset` is accepted by Cursor"]},
    ),
    (
        "an alias chain that never settles is reported",
        FIXTURE_ALIAS_CHAIN,
        [fixture_route("GET", "/c/long-chain", 200, params=[])],
        {"unresolved": ["GET /c/long-chain: cannot read the `Link10` type alias"]},
    ),
    (
        "a fallback that observes its error and then recovers is tolerant",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/observed-fallback",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "a grouped use tree with self as, or a nested group, is read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/grouped-module-alias",
                200,
                request_body=body_of(("name", True), required=False),
            ),
            fixture_route("GET", "/e/nested-group-query", 200, params=[]),
        ],
        {
            "body_required": ["POST /e/grouped-module-alias: the body is mandatory"],
            "query_params": ["GET /e/nested-group-query: `offset` is accepted by Cursor"],
        },
    ),
    (
        "an alias of a Query from another crate is no extractor",
        FIXTURE_EDGES,
        [fixture_route("GET", "/e/foreign-query-alias", 200, params=[])],
        {},
    ),
    (
        "an alias of an unreadable alias is unreadable",
        FIXTURE_EDGES,
        [fixture_route("GET", "/e/unreadable-alias-chain", 200, params=[])],
        {"unresolved": ["GET /e/unreadable-alias-chain: cannot read the `OuterOdd` type alias"]},
    ),
    (
        "a raw body handed to a helper the audit cannot find fails closed",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/raw-unknown-helper",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {
            "body_required": ["POST /e/raw-unknown-helper: the body is mandatory"],
            "unresolved": ["POST /e/raw-unknown-helper: cannot read a `from_slice` call"],
        },
    ),
    (
        "a modeled tristate field reads clean, other field deserializers do not",
        FIXTURE_EDGES,
        [
            fixture_route("POST", "/e/tristate-field", 200, request_body=body_of(("note", False))),
            fixture_route(
                "POST", "/e/tristate-plain-option", 200, request_body=body_of(("note", False))
            ),
            fixture_route(
                "POST", "/e/unknown-deserializer", 200, request_body=body_of(("note", True))
            ),
            fixture_route("POST", "/e/with-field", 200, request_body=body_of(("note", True))),
        ],
        {
            "unresolved": [
                "POST /e/tristate-plain-option: cannot read `deserialize_with` in TristatePlain",
                "POST /e/unknown-deserializer: cannot read `deserialize_with` in CustomDeserializer",
                "POST /e/with-field: cannot read `with` in WithField",
            ]
        },
    ),
    (
        "a serde attribute split over several lines is read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/multiline-field-serde",
                200,
                request_body=body_of(("name", True)),
            )
        ],
        {"unresolved": ["POST /e/multiline-field-serde: cannot read `flatten` in MultilineSerde"]},
    ),
    (
        "a serde key word inside a quoted value is no key",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/serde-keyword-values",
                200,
                request_body=body_of(("kind", False)),
            )
        ],
        {
            "mandatory": [
                "POST /e/serde-keyword-values: `name` is mandatory in KeywordValues",
                "POST /e/serde-keyword-values: `kind` is mandatory in KeywordValues",
            ],
            "undocumented": ["POST /e/serde-keyword-values: `name` is accepted by KeywordValues"],
        },
    ),
    (
        "a borrowed or grouped Err pattern that rejects is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/borrowed-err-arm", "/e/grouped-err-arm", "/e/borrowed-if-let-err")
        ],
        {
            "body_required": [
                "POST /e/borrowed-err-arm: the body is mandatory",
                "POST /e/grouped-err-arm: the body is mandatory",
                "POST /e/borrowed-if-let-err: the body is mandatory",
            ]
        },
    ),
    (
        "a raw body passed to an associated fn or a method is followed",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/raw-associated-helper", "/e/raw-method-helper")
        ]
        + [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", False), required=False),
            )
            for path in (
                "/e/raw-unknown-method",
                "/e/raw-unknown-associated",
                "/e/raw-std-reader",
                "/e/raw-aliased-std-reader",
                "/e/raw-imported-std-reader",
            )
        ],
        {
            "body_required": [
                "POST /e/raw-associated-helper: the body is mandatory",
                "POST /e/raw-method-helper: the body is mandatory",
                "POST /e/raw-unknown-method: the body is mandatory",
                "POST /e/raw-unknown-associated: the body is mandatory",
            ],
            "unresolved": [
                "POST /e/raw-unknown-method: cannot read a `from_slice` call",
                "POST /e/raw-unknown-associated: cannot read a `from_slice` call",
            ],
        },
    ),
    (
        "a qualified struct picks its module, and an ambiguous bare name fails closed",
        FIXTURE_NAMES,
        [
            fixture_route(
                "GET",
                "/n/qualified-struct",
                200,
                params=[query_param("wanted", "integer", False)],
            ),
            fixture_route(
                "GET",
                "/n/ambiguous-struct",
                200,
                params=[query_param("wanted", "integer", False)],
            ),
        ],
        {"unresolved": ["GET /n/ambiguous-struct: cannot find struct Filter"]},
    ),
    (
        "a relative first name must be a module inside the site, or it is an external crate",
        FIXTURE_NAMES,
        [
            fixture_route(
                "GET", path, 200, params=[query_param("wanted", "integer", True)]
            )
            for path in ("/n/extern-root-struct", "/n/aliased-root-struct")
        ]
        + [
            fixture_route(
                "POST",
                "/n/extern-root-helper",
                200,
                request_body=body_of(("name", False), required=False),
            ),
            fixture_route(
                "GET",
                "/n/child-path-struct",
                200,
                params=[query_param("wanted", "integer", False)],
            ),
        ],
        {
            "body_required": ["POST /n/extern-root-helper: the body is mandatory"],
            "unresolved": [
                "GET /n/extern-root-struct: cannot find struct RemoteFilter",
                "POST /n/extern-root-helper: cannot read a `from_slice` call",
            ],
        },
    ),
    (
        "a qualified path matches its whole module suffix",
        FIXTURE_NAMES,
        [
            fixture_route(
                "GET",
                "/n/wrong-prefix-struct",
                200,
                params=[query_param("wanted", "integer", False)],
            ),
            fixture_route(
                "GET",
                "/n/crate-path-struct",
                200,
                params=[query_param("wanted", "integer", False)],
            ),
            fixture_route(
                "POST",
                "/n/module-helper",
                200,
                request_body=body_of(("name", False), required=False),
            ),
        ],
        {"unresolved": ["GET /n/wrong-prefix-struct: cannot find struct Filter"]},
    ),
    (
        "a method call resolves through its receiver's written type, or fails closed",
        FIXTURE_NAMES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", True), required=False),
            )
            for path in (
                "/n/typed-receiver",
                "/n/let-receiver",
                "/n/untyped-receiver",
                "/n/alias-receiver",
                "/n/grouped-let-receiver",
            )
        ],
        {
            "body_required": [
                "POST /n/let-receiver: the body is mandatory",
                "POST /n/alias-receiver: the body is mandatory",
                "POST /n/grouped-let-receiver: the body is mandatory",
                "POST /n/untyped-receiver: the body is mandatory",
            ],
            "unresolved": ["POST /n/untyped-receiver: cannot read a `from_slice` call"],
        },
    ),
    (
        "a bare name resolves through its import, and self and super from the module",
        FIXTURE_NAMES,
        [
            fixture_route("GET", "/n/external-import", 200, params=[]),
            fixture_route(
                "GET", "/n/local-import", 200, params=[query_param("wanted", "integer", False)]
            ),
            fixture_route(
                "GET", "/n/super-path", 200, params=[query_param("wanted", "integer", True)]
            ),
            fixture_route(
                "GET", "/n/self-path", 200, params=[query_param("wanted", "integer", False)]
            ),
        ],
        {"unresolved": ["GET /n/external-import: cannot find struct Widget"]},
    ),
    (
        "a bare helper call resolves through its import, or a visible definition",
        FIXTURE_NAMES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in ("/n/imported-helper", "/n/external-helper", "/n/invisible-helper")
        ],
        {
            "body_required": [
                "POST /n/external-helper: the body is mandatory",
                "POST /n/invisible-helper: the body is mandatory",
            ],
            "unresolved": [
                "POST /n/external-helper: cannot read a `from_slice` call",
                "POST /n/invisible-helper: cannot read a `from_slice` call",
            ],
        },
    ),
    (
        "a qualified path resolves its first name through an import",
        FIXTURE_NAMES,
        [
            fixture_route("GET", "/n/external-module-alias", 200, params=[]),
            fixture_route(
                "GET",
                "/n/local-module-alias",
                200,
                params=[query_param("wanted", "integer", False)],
            ),
            fixture_route(
                "POST",
                "/n/external-module-helper",
                200,
                request_body=body_of(("name", False), required=False),
            ),
        ],
        {
            "body_required": ["POST /n/external-module-helper: the body is mandatory"],
            "unresolved": [
                "GET /n/external-module-alias: cannot find struct Filter",
                "POST /n/external-module-helper: cannot read a `from_slice` call",
            ],
        },
    ),
    (
        "an untrusted glob fails closed for a bare name, and a local glob brings its items",
        FIXTURE_NAMES,
        [
            fixture_route(
                "GET", "/n/glob-struct", 200, params=[query_param("size", "integer", True)]
            ),
            fixture_route(
                "GET", "/n/glob-local", 200, params=[query_param("wanted", "integer", False)]
            ),
            fixture_route("GET", "/n/glob-own", 200, params=[query_param("size", "integer", True)]),
            fixture_route("POST", "/n/glob-alias", 200, request_body=body_of(("tags", False))),
            fixture_route(
                "POST", "/n/glob-helper", 200, request_body=body_of(("name", True))
            ),
            fixture_route("POST", "/n/glob-shadow", 200, request_body=body_of(("size", True))),
        ],
        {
            "unresolved": [
                "POST /n/glob-shadow: cannot tell which `Json` a glob import brings",
                "GET /n/glob-struct: cannot find struct Widget",
                "POST /n/glob-alias: cannot read `MaybeTags` in GlobAliasField",
                "POST /n/glob-helper: cannot read a `from_slice` call",
            ],
        },
    ),
    (
        "each call resolves at its own site, so block-scoped imports stay apart",
        FIXTURE_NAMES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in ("/n/block-imports", "/n/block-module-imports")
        ],
        {
            "body_required": [
                "POST /n/block-imports: the body is mandatory",
                "POST /n/block-module-imports: the body is mandatory",
            ],
            "mandatory": [
                "POST /n/block-imports: `name` is mandatory in Gadget",
                "POST /n/block-module-imports: `name` is mandatory in Gadget",
            ],
        },
    ),
    (
        "a method call reaches its receiver type's method, and self its own impl",
        FIXTURE_NAMES,
        [
            fixture_route("GET", "/n/typed-reply", 200),
            fixture_route("GET", "/n/unknown-reply", 200),
            fixture_route("POST", "/n/self-method", 200, request_body=GADGET_BODY),
        ],
        {"statuses": ["GET /n/unknown-reply returns 409, undeclared"]},
    ),
    (
        "a parse type in a helper resolves from the helper's module",
        FIXTURE_NAMES,
        [
            fixture_route(
                "POST", "/n/helper-parse-type", 200, request_body=body_of(("wanted", False))
            )
        ],
        {},
    ),
    (
        "a turbofish on a type segment does not hide the helper",
        FIXTURE_NAMES,
        [
            fixture_route(
                "POST",
                "/n/turbofish-type-helper",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /n/turbofish-type-helper: the body is mandatory"]},
    ),
    (
        "a qualified method picks its impl type, and an ambiguous method fails closed",
        FIXTURE_NAMES,
        [
            fixture_route(
                "POST",
                "/n/lenient-decoder",
                200,
                request_body=body_of(("name", False), required=False),
            ),
            fixture_route(
                "POST",
                "/n/strict-decoder",
                200,
                request_body=body_of(("name", True), required=False),
            ),
            fixture_route(
                "POST",
                "/n/ambiguous-method",
                200,
                request_body=body_of(("name", False), required=False),
            ),
        ],
        {
            "body_required": [
                "POST /n/strict-decoder: the body is mandatory",
                "POST /n/ambiguous-method: the body is mandatory",
            ],
            "unresolved": ["POST /n/ambiguous-method: cannot read a `from_slice` call"],
        },
    ),
    (
        "an alias of Option makes a field optional, and an alias the audit cannot read is reported",
        FIXTURE_NAMES,
        [
            fixture_route(
                "POST",
                "/n/maybe-fields",
                200,
                request_body=body_of(("name", True), ("note", False), ("tags", False)),
            ),
            fixture_route(
                "POST", "/n/loose-field", 200, request_body=body_of(("tags", False))
            ),
        ],
        {"unresolved": ["POST /n/loose-field: cannot read `Loose` in LooseField"]},
    ),
    (
        "a local item named like an extractor shadows it in its scope only",
        FIXTURE_LOCAL_ITEMS,
        [
            fixture_route("GET", "/l/local-query", 200, params=[]),
            fixture_route("POST", "/l/fn-local-json", 200, request_body=body_of(("name", False))),
        ],
        {"mandatory": ["POST /l/fn-local-json: `name` is mandatory in Gadget"]},
    ),
    (
        "a plain import binds its name, and a glob from an unknown module fails closed",
        FIXTURE_IMPORTS,
        [
            fixture_route("GET", "/i/shadowed-query", 200, params=[]),
            fixture_route(
                "POST", "/i/glob-json", 200, request_body=body_of(("name", True))
            ),
        ],
        {"unresolved": ["POST /i/glob-json: cannot tell which `Json` a glob import brings"]},
    ),
    (
        "a qualified field alias picks its module, and an ambiguous bare alias fails closed",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route(
                "POST", "/s/qualified-alias", 200, request_body=body_of(("labels", False))
            ),
            fixture_route("POST", "/s/ambiguous-alias", 200, request_body=body_of(("note", False))),
        ],
        {
            "mandatory": ["POST /s/qualified-alias: `labels` is mandatory in QualifiedAliasField"],
            "unresolved": ["POST /s/ambiguous-alias: cannot read `Maybe` in AmbiguousAliasField"],
        },
    ),
    (
        "a crate path at a struct counts from the struct's own crate",
        FIXTURE_CRATES,
        [fixture_route("POST", "/c/other-crate", 200, request_body=body_of(("tags", False)))],
        {},
    ),
    (
        "a local or imported item named like a prelude type shadows it",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route(
                "GET", path, 200, params=[query_param("limit", "integer", False)]
            )
            for path in ("/s/local-option", "/s/imported-option", "/s/deep-option")
        ]
        + [
            fixture_route(
                "POST", "/s/imported-result", 200, request_body=body_of(("limit", False))
            )
        ],
        {
            "query_params": [
                "GET /s/local-option: `limit` has type self::Option<u32>",
                "GET /s/local-option: `limit` is mandatory in LocalOptionPage",
                "GET /s/imported-option: `limit` has type other_crate::Option<u32>",
                "GET /s/imported-option: `limit` is mandatory in ImportedOptionPage",
                "GET /s/deep-option: `limit` has type self::Option<u32>",
                "GET /s/deep-option: `limit` is mandatory in DeepOptionPage",
            ],
            "unresolved": ["POST /s/imported-result: cannot read a `Json<..>` extractor"],
        },
    ),
    (
        "a standard path always names the real type, even where the bare name is shadowed",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route(
                "GET",
                "/s/std-paths",
                200,
                params=[
                    query_param("limit", "integer", False),
                    query_param("name", "string", True),
                    query_param("label", "string", True),
                    query_param("exact", "boolean", True),
                ],
            )
        ],
        {},
    ),
    (
        "Uuid is a string only when it resolves to uuid::Uuid",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route(
                "GET",
                "/s/trusted-id",
                200,
                params=[query_param("id", "string", True), query_param("other", "string", True)],
            )
        ]
        + [
            fixture_route("GET", path, 200, params=[query_param("id", "string", True)])
            for path in ("/s/local-id", "/s/rebound-id", "/s/bare-id")
        ],
        {
            "query_params": [
                "GET /s/local-id: `id` has type Uuid, which maps to no OpenAPI type",
                "GET /s/rebound-id: `id` has type crate::fake::Uuid, which maps to no OpenAPI type",
                "GET /s/bare-id: `id` has type Uuid, which maps to no OpenAPI type",
            ]
        },
    ),
    (
        "only a trusted top-level raw reader exempts a route from the documented key check",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route("GET", path, 200, params=[query_param("limit", "integer", False)])
            for path in (
                "/s/state-request",
                "/s/fake-raw",
                "/s/local-request",
                "/s/glob-request",
                "/s/trusted-request",
                "/s/reexport-raw",
                "/s/full-reexport-raw",
            )
        ],
        {
            "query_params": [
                "GET /s/state-request: `limit` is documented but no extractor of the route",
                "GET /s/fake-raw: `limit` is documented but no extractor of the route",
                "GET /s/local-request: `limit` is documented but no extractor of the route",
                "GET /s/glob-request: `limit` is documented but no extractor of the route",
            ],
            "unresolved": ["GET /s/glob-request: cannot tell which `Request` a glob import brings"],
        },
    ),
    (
        "a raw reader skips only the inverse check, and every Query<T> is still read",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route(
                "GET",
                "/s/raw-and-query",
                200,
                params=[
                    query_param("limit", "string", False),
                    query_param("extra", "string", False),
                ],
            )
        ],
        {
            "query_params": [
                "GET /s/raw-and-query: `limit` is integer in Paging but the contract says string"
            ]
        },
    ),
    (
        "a documented query key needs an extractor that reads it, on every served route",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route(
                "GET",
                "/s/no-query/{id}",
                200,
                params=[
                    {"name": "id", "in": "path", "type": "string", "required": True},
                    query_param("limit", "integer", False),
                ],
            ),
            fixture_route(
                "GET", "/s/raw-query", 200, params=[query_param("limit", "integer", False)]
            ),
            fixture_route(
                "GET", "/s/unread-query", 200, params=[query_param("limit", "integer", False)]
            ),
        ],
        {
            "query_params": [
                "GET /s/no-query/{id}: `limit` is documented but no extractor of the route"
            ],
            "unresolved": ["GET /s/unread-query: cannot find struct MissingPage"],
        },
    ),
    (
        "an aliased import of a standard type is that type, in a field and in a parameter",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route(
                "GET",
                "/s/std-alias-field",
                200,
                params=[query_param("limit", "integer", False)],
            ),
            fixture_route(
                "POST",
                "/s/std-alias-param",
                200,
                request_body=body_of(("limit", False), required=False),
            ),
            fixture_route(
                "POST",
                "/s/std-option-param",
                200,
                request_body=body_of(("limit", False), required=False),
            ),
        ],
        {},
    ),
    (
        "an extractor path is trusted only after its first name goes through the imports",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route("GET", "/s/spoofed-query", 200, params=[]),
            fixture_route(
                "POST",
                "/s/spoofed-json",
                200,
                request_body=body_of(("limit", False), required=False),
            ),
            fixture_route(
                "GET",
                "/s/reexported-query",
                200,
                params=[query_param("limit", "integer", False)],
            ),
        ],
        {},
    ),
    (
        "a field alias resolves through the import at the struct",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route(
                "POST", "/s/imported-alias", 200, request_body=body_of(("labels", False))
            )
        ],
        {
            "mandatory": [
                "POST /s/imported-alias: `labels` is mandatory in ImportedAliasField",
                "POST /s/imported-alias: `note` is mandatory in ImportedAliasField",
            ],
            "undocumented": ["POST /s/imported-alias: `note` is accepted by ImportedAliasField"],
        },
    ),
    (
        "an alias target is canonical, and a block-local alias is not a field alias",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route(
                "POST", "/s/qualified-target", 200, request_body=body_of(("note", False))
            ),
            fixture_route("POST", "/s/block-alias", 200, request_body=body_of(("note", False))),
        ],
        {"mandatory": ["POST /s/block-alias: `note` is mandatory in BlockAliasField"]},
    ),
    (
        "a parameter documented twice with the same name and location is a finding",
        FIXTURE_ALIAS_SCOPES,
        [
            fixture_route(
                "GET",
                "/s/dup-params/{id}",
                200,
                params=[
                    query_param("limit", "integer", False),
                    query_param("limit", "integer", False),
                    {"name": "id", "in": "path", "type": "string", "required": True},
                    {"name": "id", "in": "path", "type": "string", "required": True},
                ],
            )
        ],
        {
            "query_params": [
                "GET /s/dup-params/{id}: `limit` is documented 2 times in query",
                "GET /s/dup-params/{id}: `id` is documented 2 times in path",
            ]
        },
    ),
    (
        "a tolerant parse leaves the body optional only when an empty body can succeed",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in (
                "/e/empty-rejected-tolerant",
                "/e/empty-rejected-helper",
                "/e/empty-else-rejected",
            )
        ],
        {
            "body_required": [
                "POST /e/empty-rejected-tolerant: the body is mandatory",
                "POST /e/empty-rejected-helper: the body is mandatory",
                "POST /e/empty-else-rejected: the body is mandatory",
            ]
        },
    ),
    (
        "an Option body whose None rejects is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", True), required=False)
            )
            for path in (
                "/e/option-ok-or",
                "/e/option-expect",
                "/e/option-match",
                "/e/option-let-else",
                "/e/option-if-none",
                "/e/option-bytes",
                "/e/option-body-default",
                "/e/option-unread-use",
            )
        ],
        {
            "body_required": [
                "POST /e/option-ok-or: the body is mandatory",
                "POST /e/option-expect: the body is mandatory",
                "POST /e/option-match: the body is mandatory",
                "POST /e/option-let-else: the body is mandatory",
                "POST /e/option-if-none: the body is mandatory",
                "POST /e/option-bytes: the body is mandatory",
                "POST /e/option-unread-use: the body is mandatory",
            ]
        },
    ),
    (
        "a byte carrier inside a Result or an Option is followed from its success",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in (
                "/e/result-bytes",
                "/e/option-bytes-match",
                "/e/result-bytes-if-let",
                "/e/result-bytes-tolerant",
                "/e/option-bytes-unwrapped",
            )
        ],
        {
            "body_required": [
                "POST /e/result-bytes: the body is mandatory",
                "POST /e/option-bytes-match: the body is mandatory",
                "POST /e/option-bytes-unwrapped: the body is mandatory",
                "POST /e/result-bytes-tolerant: the body is mandatory",
            ],
            "mandatory": [
                "POST /e/result-bytes: `name` is mandatory in Gadget",
                "POST /e/option-bytes-match: `name` is mandatory in Gadget",
                "POST /e/option-bytes-unwrapped: `name` is mandatory in Gadget",
            ],
        },
    ),
    (
        "a carrier that flows into a destructuring pattern fails closed",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in (
                "/e/tuple-carrier",
                "/e/struct-carrier",
                "/e/rebound-carrier",
                "/e/rebound-other",
            )
        ],
        {
            "body_required": [
                "POST /e/tuple-carrier: the body is mandatory",
                "POST /e/struct-carrier: the body is mandatory",
                "POST /e/rebound-carrier: the body is mandatory",
            ],
            "unresolved": [
                "POST /e/tuple-carrier: cannot read a `from_slice` call",
                "POST /e/struct-carrier: cannot read a `from_slice` call",
                "POST /e/rebound-carrier: cannot read a `from_slice` call",
            ],
        },
    ),
    (
        "a parse in a closure counts only when the closure may run",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in (
                "/e/dead-closure",
                "/e/dropped-closure",
                "/e/dead-helper-closure",
                "/e/called-closure",
                "/e/stored-closure",
            )
        ],
        {
            "body_required": [
                "POST /e/called-closure: the body is mandatory",
                "POST /e/stored-closure: the body is mandatory",
            ],
            "mandatory": [
                "POST /e/called-closure: `name` is mandatory in Gadget",
                "POST /e/stored-closure: `name` is mandatory in Gadget",
            ],
        },
    ),
    (
        "a parameter type is read through its aliases, and one the audit cannot read fails closed",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in (
                "/e/alias-body",
                "/e/alias-helper",
                "/e/opaque-body",
                "/e/unknown-type-body",
                "/e/alias-conversion",
                "/e/struct-named-alias",
            )
        ],
        {
            "body_required": [
                "POST /e/alias-body: the body is mandatory",
                "POST /e/alias-helper: the body is mandatory",
                "POST /e/opaque-body: the body is mandatory",
                "POST /e/unknown-type-body: the body is mandatory",
                "POST /e/alias-conversion: the body is mandatory",
                "POST /e/struct-named-alias: the body is mandatory",
            ],
            "mandatory": [
                "POST /e/alias-body: `name` is mandatory in Gadget",
                "POST /e/alias-helper: `name` is mandatory in Gadget",
                "POST /e/alias-conversion: `name` is mandatory in Gadget",
            ],
            "unresolved": [
                "POST /e/opaque-body: cannot read a `from_slice` call",
                "POST /e/unknown-type-body: cannot read a `from_slice` call",
                "POST /e/struct-named-alias: cannot read a `from_slice` call",
            ],
        },
    ),
    (
        "a raw body as a receiver reaches only a method of an impl for its own type",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in ("/e/unrelated-receiver", "/e/slice-receiver")
        ],
        {
            "body_required": [
                "POST /e/unrelated-receiver: the body is mandatory",
                "POST /e/slice-receiver: the body is mandatory",
            ],
            "mandatory": ["POST /e/slice-receiver: `name` is mandatory in Gadget"],
            "unresolved": ["POST /e/unrelated-receiver: cannot read a `from_slice` call"],
        },
    ),
    (
        "a qualified path is trusted only after its first name goes through the imports",
        FIXTURE_EDGES,
        [fixture_route("POST", "/e/spoofed-parse", 200, request_body=GADGET_BODY)],
        {"unresolved": ["POST /e/spoofed-parse: cannot read a `from_slice` call"]},
    ),
    (
        "a grouped or borrowed receiver is read like an argument, or fails closed",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in (
                "/e/grouped-receiver",
                "/e/borrowed-receiver",
                "/e/nested-receiver",
                "/e/sliced-receiver",
                "/e/grouped-argument",
            )
        ],
        {
            "body_required": [
                "POST /e/grouped-receiver: the body is mandatory",
                "POST /e/borrowed-receiver: the body is mandatory",
                "POST /e/nested-receiver: the body is mandatory",
                "POST /e/sliced-receiver: the body is mandatory",
                "POST /e/grouped-argument: the body is mandatory",
            ],
            "mandatory": [
                "POST /e/grouped-receiver: `name` is mandatory in Gadget",
                "POST /e/borrowed-receiver: `name` is mandatory in Gadget",
                "POST /e/nested-receiver: `name` is mandatory in Gadget",
                "POST /e/grouped-argument: `name` is mandatory in Gadget",
            ],
            "unresolved": ["POST /e/sliced-receiver: cannot read a `from_slice` call"],
        },
    ),
    (
        "a raw body as the receiver of a method maps to self, unless the method is an accessor",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/extension-decode",
                200,
                request_body=body_of(("name", True), required=False),
            ),
            fixture_route(
                "POST",
                "/e/unknown-receiver-method",
                200,
                request_body=body_of(("name", False), required=False),
            ),
            fixture_route(
                "POST",
                "/e/accessor-receiver",
                200,
                request_body=body_of(("name", False), required=False),
            ),
        ],
        {
            "body_required": [
                "POST /e/extension-decode: the body is mandatory",
                "POST /e/unknown-receiver-method: the body is mandatory",
            ],
            "unresolved": ["POST /e/unknown-receiver-method: cannot read a `from_slice` call"],
        },
    ),
    (
        "a borrowed carrier is read through its borrow, and a partial slice fails closed",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/mut-borrow-parse",
                200,
                request_body=body_of(("name", True), required=False),
            ),
            fixture_route(
                "POST",
                "/e/partial-slice-parse",
                200,
                request_body=body_of(("name", True), required=False),
            ),
        ],
        {
            "body_required": [
                "POST /e/mut-borrow-parse: the body is mandatory",
                "POST /e/partial-slice-parse: the body is mandatory",
            ],
            "unresolved": ["POST /e/partial-slice-parse: cannot read a `from_slice` call"],
        },
    ),
    (
        "a parameter type is an extractor only through a trusted path",
        FIXTURE_EDGES,
        [
            fixture_route("GET", "/e/foreign-qualified-query", 200, params=[]),
            fixture_route("GET", "/e/inline-axum-query", 200, params=[]),
        ],
        {"query_params": ["GET /e/inline-axum-query: `offset` is accepted by Cursor"]},
    ),
    (
        "a type alias of a Query from another crate is no extractor",
        FIXTURE_EDGES,
        [fixture_route("GET", "/e/signed-type-alias", 200, params=[])],
        {},
    ),
    (
        "a local binding that shadows a from_slice alias is not rewritten",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/shadowed-decode-alias",
                200,
                request_body=body_of(("name", False)),
            )
        ],
        {"unresolved": ["POST /e/shadowed-decode-alias: cannot read a `from_slice` call"]},
    ),
    (
        "a wrapped carrier or one sent to a non-byte parameter fails closed",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST", path, 200, request_body=body_of(("name", False), required=False)
            )
            for path in ("/e/wrapped-carrier", "/e/nonbyte-param", "/e/scalar-read-argument")
        ],
        {
            "body_required": [
                "POST /e/wrapped-carrier: the body is mandatory",
                "POST /e/nonbyte-param: the body is mandatory",
            ],
            "unresolved": [
                "POST /e/wrapped-carrier: cannot read a `from_slice` call",
                "POST /e/nonbyte-param: cannot read a `from_slice` call",
            ],
        },
    ),
    (
        "a guard is judged by its top-level && and || only, on every guard path",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/nested-or-inspection",
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            ),
            fixture_route(
                "POST",
                "/e/or-and-empty-guard",
                200,
                request_body=body_of(("name", True), required=False),
                additional_responses=[{"status": 204}],
            ),
            fixture_route(
                "POST",
                "/e/nested-and-error-exit",
                200,
                request_body=body_of(("name", True), required=False),
            ),
        ],
        {"body_required": ["POST /e/nested-or-inspection: the body is mandatory"]},
    ),
    (
        "a helper named in a comment or a string is not called",
        FIXTURE_EDGES,
        [fixture_route("GET", "/e/helper-in-comment", 200)],
        {},
    ),
    (
        "a path-qualified Err arm that rejects makes the body mandatory",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/json-std-err", "/e/json-core-err")
        ],
        {
            "body_required": [
                "POST /e/json-std-err: the body is mandatory",
                "POST /e/json-core-err: the body is mandatory",
            ]
        },
    ),
    (
        "a path-qualified Option field is optional",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/qualified-option-body",
                200,
                request_body=body_of(("name", True), ("note", False)),
            ),
            fixture_route(
                "GET",
                "/e/qualified-option-query",
                200,
                params=[query_param("page", "integer", False)],
            ),
        ],
        {},
    ),
    (
        "an Err arm whose value is a type path builds a value",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/json-tail-value",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "a helper that returns a response, or that cannot be found, rejects",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
            for path in ("/e/is-err-tail-helper", "/e/unknown-helper")
        ],
        {
            "body_required": [
                "POST /e/is-err-tail-helper: the body is mandatory",
                "POST /e/unknown-helper: the body is mandatory",
            ]
        },
    ),
    (
        "a helper that returns a plain value builds a value",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/helper-default",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "a from_slice on another type is no body parse",
        FIXTURE_EDGES,
        [fixture_route("POST", "/e/uuid-bytes", 200)],
        {},
    ),
    (
        "a fallback on an Option still rejects through a helper",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/option-fallback",
                200,
                request_body=body_of(("name", True), required=False),
            ),
            fixture_route(
                "POST",
                "/e/option-default",
                200,
                request_body=body_of(("name", False), required=False),
            ),
        ],
        {"body_required": ["POST /e/option-fallback: the body is mandatory"]},
    ),
    (
        "a from_slice call in a comment or a string is no body parse",
        FIXTURE_EDGES,
        [fixture_route("POST", "/e/commented-parse", 200)],
        {},
    ),
    (
        "a let-else that returns a Json response lets the request through",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/let-else-json",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "an eager map_or fallback that calls a rejecting helper is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/map-or-helper",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/map-or-helper: the body is mandatory"]},
    ),
    (
        "a match on a mutable borrow of a Result extractor is read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/match-mut",
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
        ],
        {"body_required": ["POST /e/match-mut: the body is mandatory"]},
    ),
    (
        "an awaited helper parse with a fallback is tolerant",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/await-default",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "a let-else or if-let on a borrowed Result extractor is read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
            for path in ("/e/borrowed-let-else", "/e/as-ref-if-let-err")
        ],
        {
            "body_required": [
                "POST /e/borrowed-let-else: the body is mandatory",
                "POST /e/as-ref-if-let-err: the body is mandatory",
            ]
        },
    ),
    (
        "a joined inspection whose failure always takes the rejecting arm is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/is-err-or", "/e/is-ok-and-then")
        ],
        {
            "body_required": [
                "POST /e/is-err-or: the body is mandatory",
                "POST /e/is-ok-and-then: the body is mandatory",
            ]
        },
    ),
    (
        "an inspection that a failure may bypass is tolerant",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", False), required=False))
            for path in ("/e/is-err-and", "/e/is-ok-and-or")
        ],
        {},
    ),
    (
        "an unwrap after a guard that exits on error with success is tolerant",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/exit-then-unwrap",
                200,
                request_body=body_of(("name", False), required=False),
                additional_responses=[{"status": 204}],
            ),
            fixture_route(
                "POST",
                "/e/log-then-unwrap",
                200,
                request_body=body_of(("name", True), required=False),
            ),
        ],
        {"body_required": ["POST /e/log-then-unwrap: the body is mandatory"]},
    ),
    (
        "a parse inside a nested block comment is no parse",
        FIXTURE_EDGES,
        [fixture_route("POST", "/e/nested-comment", 200)],
        {},
    ),
    (
        "a moved raw body and a let-Err-else that rejects are mandatory",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200, request_body=body_of(("name", True), required=False))
            for path in ("/e/raw-alias", "/e/let-err-else")
        ],
        {
            "body_required": [
                "POST /e/raw-alias: the body is mandatory",
                "POST /e/let-err-else: the body is mandatory",
            ]
        },
    ),
    (
        "a helper that returns Json<T> builds a success",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/json-helper-default",
                200,
                request_body=body_of(("name", False), required=False),
            )
        ],
        {},
    ),
    (
        "an aliased deserializer and a fallthrough after a positive guard are read",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
            for path in ("/e/aliased-decode", "/e/module-alias", "/e/ok-return-then-reject")
        ],
        {
            "body_required": [
                "POST /e/aliased-decode: the body is mandatory",
                "POST /e/module-alias: the body is mandatory",
                "POST /e/ok-return-then-reject: the body is mandatory",
            ]
        },
    ),
    (
        "a guarded rejecting Err arm makes a raw parse mandatory, since no guard is evaluated",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/raw-guarded-arms",
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
        ],
        {"body_required": ["POST /e/raw-guarded-arms: the body is mandatory"]},
    ),
    (
        "a stored parse result is read where it is used",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/stored-parse",
                200,
                request_body=body_of(("name", False), required=False),
            ),
            fixture_route(
                "POST",
                "/e/stored-parse-strict",
                200,
                request_body=body_of(("name", True), required=False),
            ),
        ],
        {"body_required": ["POST /e/stored-parse-strict: the body is mandatory"]},
    ),
    (
        "an unwrap after an OR guard that exits on every error with success is tolerant",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/or-exit-then-unwrap",
                200,
                request_body=body_of(("name", False), required=False),
                additional_responses=[{"status": 204}],
            )
        ],
        {},
    ),
    (
        "a nested comment in a parameter list and an unimported from_slice are no body",
        FIXTURE_EDGES,
        [
            fixture_route("POST", path, 200)
            for path in ("/e/nested-param-comment", "/e/local-from-slice")
        ],
        {},
    ),
    (
        "a closure that returns a rejection and a multi-line let-else are mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
            for path in ("/e/closure-return-reject", "/e/multiline-let-else")
        ],
        {
            "body_required": [
                "POST /e/closure-return-reject: the body is mandatory",
                "POST /e/multiline-let-else: the body is mandatory",
            ]
        },
    ),
    (
        "a ref or mut catch-all arm that rejects is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/ref-catch-all",
                200,
                request_body=body_of(("name", True), required=False),
                error_responses=[{"status": 400}],
            )
        ],
        {"body_required": ["POST /e/ref-catch-all: the body is mandatory"]},
    ),
    (
        "a value return, a commented unwrap and a ref Err hand-on stay optional",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", True), required=False),
            )
            for path in ("/e/closure-return-value", "/e/commented-unwrap", "/e/ref-err-handed")
        ],
        {},
    ),
    (
        "a nested or multi-line block comment in a struct hides its fields",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/nested-struct-comment",
                200,
                request_body=body_of(("name", True)),
            )
        ],
        {},
    ),
    (
        "a helper with a short name is followed",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/short-helper",
                200,
                request_body=body_of(),
                error_responses=[{"status": 400}],
            )
        ],
        {
            "mandatory": ["POST /e/short-helper: `name` is mandatory in Gadget"],
            "undocumented": ["POST /e/short-helper: `name` is accepted by Gadget"],
        },
    ),
    (
        "a return type matches by whole name or suffix, not by substring",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                path,
                200,
                request_body=body_of(("name", True), required=False),
            )
            for path in ("/e/response-config", "/e/suffix-error")
        ],
        {"body_required": ["POST /e/suffix-error: the body is mandatory"]},
    ),
    (
        "a status in a comment or a string is not returned",
        FIXTURE_EDGES,
        [fixture_route("GET", "/e/commented-status", 200)],
        {},
    ),
    (
        "a path-qualified Result<Json<T>> body that unwraps is mandatory",
        FIXTURE_EDGES,
        [
            fixture_route(
                "POST",
                "/e/std-result",
                200,
                request_body=body_of(("name", True), required=False),
            )
        ],
        {"body_required": ["POST /e/std-result: the body is mandatory"]},
    ),
    (
        "a path-qualified Result<Query<T>> that is not rejected makes fields optional",
        FIXTURE_EDGES,
        [
            fixture_route(
                "GET",
                "/e/std-result-query",
                200,
                params=[query_param("offset", "integer", True)],
            )
        ],
        {
            "query_params": [
                "GET /e/std-result-query: `offset` is optional in Cursor but the contract"
            ]
        },
    ),
    (
        "an imported alias of Query or Json is read",
        FIXTURE_EDGES,
        [
            fixture_route("GET", "/e/aliased-query", 200, params=[]),
            fixture_route("POST", "/e/aliased-json", 200, request_body=body_of(required=False)),
        ],
        {
            "query_params": ["GET /e/aliased-query: `offset` is accepted by Cursor"],
            "mandatory": ["POST /e/aliased-json: `name` is mandatory in Gadget"],
            "undocumented": ["POST /e/aliased-json: `name` is accepted by Gadget"],
            "body_required": ["POST /e/aliased-json: the body is mandatory"],
        },
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
