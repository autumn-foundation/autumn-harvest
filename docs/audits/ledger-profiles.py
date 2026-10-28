#!/usr/bin/env python3
"""Fail on a SQL query that has no Ledger profile.

Ledger is the query-cost pass of this repository. It measures what a query
costs in shared buffers, and it records the result in a `docs/performance-*`
page. Before this gate, only the queries that Ledger chose to measure had any
record. A new query could ship with no record of what it reads, what it
writes, or how often it runs.

A Ledger profile is that record (issue #2364). It is a TOML table in
`docs/ledger/`. The catalog has one file for each source file, at the same
path under the crate name. The profile of `enqueue` in
`autumn-harvest/src/queue.rs` is the table `["enqueue"]` in
`docs/ledger/autumn-harvest/queue.toml`. See `docs/ledger/README.md` for the
fields.

A query site is a call that sends SQL to Postgres or SQLite: `sql_query`,
the Diesel DSL (`load`, `get_result`, `execute` and the others), or a
`rusqlite` call (`prepare`, `query_row`, `execute_batch` and the others).
The profile belongs to the function that holds the call. Code under
`#[cfg(test)]` and `#[test]` is out of scope.

The gate reports these findings:

- a function with a query site and no profile
- a profile with no matching function, so a rename cannot leave a stale one
- a catalog file with no source file
- a `driver` that is not one of DRIVERS
- a `backend`, `kind` or `tables` list that omits what the scan detects
- a `measured` page that does not exist or does not name the function

The scan detects `backend`, `kind` and `tables`. A profile must list at least
what the scan detects. It can list more, because the scan cannot follow a
query that another function builds. The `driver` is the one field that only
the author can set: what makes the query run, and so how often.

Usage:
    python3 docs/audits/ledger-profiles.py
    python3 docs/audits/ledger-profiles.py --scaffold   # print stubs for missing profiles
    python3 docs/audits/ledger-profiles.py --self-test
"""
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CATALOG = Path("docs/ledger")

# The crates whose `src/` trees ship queries. `autumn-harvest-redis` sends
# Redis commands, not SQL. `autumn-harvest-macros` generates calls into the
# engine API, and the engine functions hold the queries.
CRATES = (
    "autumn-harvest",
    "autumn-harvest-plugin",
    "autumn-harvest-sqlite",
    "autumn-harvest-cli",
    "autumn-harvest-agent",
)

DRIVERS = {
    "per_task": "runs for each task claim, completion, heartbeat or decision commit",
    "per_tick": "runs on each pass of a background loop, scanner or sweeper",
    "per_request": "runs for each API, UI, CLI or client call",
    "setup": "runs at startup, in a migration, or in an operator maintenance operation",
    "test_support": "runs only in a test or verification harness",
}
KINDS = ("read", "write", "ddl")
BACKENDS = ("postgres", "sqlite")
FIELDS = {"driver", "backend", "kind", "tables", "measured"}
UNMEASURED = "unmeasured"

# An optional turbofish, such as `::<i64>` or `::<(Uuid, String)>`. A tuple
# type has parentheses, so the pattern stops at the first `>(` instead.
TURBOFISH = r"(?:::\s*<[^;{}]*?>\s*)?"
# Calls that only Diesel makes, as methods or in the fully qualified form.
POSTGRES_CALL = re.compile(
    r"\bsql_query\s*\(|\binsert_into\s*\(|\bdiesel::(?:update|delete)\s*\("
    r"|\b(?:SimpleAsyncConnection|RunQueryDsl)::(?:batch_execute|execute|load|get_result"
    r"|get_results|first)\s*\("
    r"|\.(?:get_result|get_results|load|load_stream|first|batch_execute)"
    r"\s*" + TURBOFISH + r"\("
)
# Calls that only rusqlite makes.
SQLITE_CALL = re.compile(
    r"\.(?:prepare|prepare_cached|query_row|query_row_and_then|query_map"
    r"|query_and_then|execute_batch|pragma_update|pragma_update_and_check"
    r"|pragma_query_value)\s*\("
)
# Both libraries name a call `execute`.
EITHER_CALL = re.compile(r"\.execute\s*" + TURBOFISH + r"\(")
# An atomic `load(Ordering::...)` is not a query.
ATOMIC_ARG = re.compile(r"\b(?:Ordering|SeqCst|Acquire|Relaxed|AcqRel|Release)\b")
DB_FILE = re.compile(r"\b(?:diesel|diesel_async|rusqlite|AsyncPgConnection|RunQueryDsl)\b")

WRITE_SQL = re.compile(
    r"\bINSERT\s+INTO\b|\bUPDATE\s+(?:ONLY\s+)?[\w.]+(?:\s+(?:AS\s+)?\w+)?\s+SET\b|\bDELETE\s+FROM\b"
    r"|\bON\s+CONFLICT\b|\bREPLACE\s+INTO\b"
)
WRITE_DSL = re.compile(r"\binsert_into\s*\(|\bdiesel::(?:update|delete)\s*\(")
DDL_SQL = re.compile(
    r"\b(?:CREATE|ALTER|DROP)\s+(?:UNIQUE\s+|OR\s+REPLACE\s+|TEMP\s+|TEMPORARY\s+"
    r"|UNLOGGED\s+)?(?:TABLE|INDEX|TRIGGER|FUNCTION|VIEW|SCHEMA|EXTENSION|TYPE"
    r"|SEQUENCE|POLICY|ROLE|PUBLICATION|SUBSCRIPTION)\b|\bTRUNCATE\b"
)
READ_SQL = re.compile(r"\bSELECT\b|\bRETURNING\b|\bPRAGMA\b")
READ_DSL = re.compile(
    r"\bdiesel::select\s*\(|\.(?:load|load_stream|first|get_result|get_results|query_row|query_map)"
    r"\s*" + TURBOFISH + r"\("
    r"(?!\s*\)|\s*(?:\w+::)*(?:Ordering|SeqCst|Acquire|Relaxed|AcqRel|Release)\b)"
)
CREATE_TABLE = re.compile(
    r"\bCREATE\s+(?:UNLOGGED\s+|TEMP\s+|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:\w+\.)?\"?([A-Za-z_]\w*)",
    re.IGNORECASE,
)
IDENT = re.compile(r"\b[A-Za-z_]\w*\b")
TEST_ATTR = re.compile(r"^#\[\s*(?:test|(?:\w+::)+test|cfg\(\s*(?:test|all\(\s*test\b))")
ITEM = re.compile(
    r"#!?\s*\[|\bfn\s+(?:r#)?([A-Za-z_]\w*)|\bimpl\b|\btrait\s+([A-Za-z_]\w*)"
    r"|\bmod\s+([A-Za-z_]\w*)|\bmacro_rules!\s*([A-Za-z_]\w*)|[{};]"
)


def lex(source):
    """Return `(code, full)`: comments blanked, and string bodies blanked in `code` only.

    Both views keep every offset and newline, so a position in one is the
    same position in the other and in the source.
    """
    code = list(source)
    full = list(source)
    i, n = 0, len(source)

    def blank(view, start, end):
        for k in range(start, end):
            if view[k] != "\n":
                view[k] = " "

    while i < n:
        c = source[i]
        if source.startswith("//", i):
            end = source.find("\n", i)
            end = n if end < 0 else end
            blank(code, i, end)
            blank(full, i, end)
            i = end
        elif source.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if source.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif source.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            blank(code, i, j)
            blank(full, i, j)
            i = j
        elif c in "rbc" and (i == 0 or not (source[i - 1].isalnum() or source[i - 1] == "_")):
            m = re.compile(r'(?:b|c)?r(#*)"').match(source, i)
            if m:
                close = '"' + m.group(1)
                end = source.find(close, m.end())
                end = n if end < 0 else end + len(close)
                blank(code, m.end(), end - len(close))
                i = end
                continue
            m = re.compile(r'[bc]"').match(source, i)
            if m:
                i = skip_string(source, m.end() - 1, code)
                continue
            i += 1
        elif c == '"':
            i = skip_string(source, i, code)
        elif c == "'":
            if source.startswith("\\", i + 1):
                end = source.find("'", i + 2)
                while end > 0 and source[end - 1] == "\\" and source[end - 2] != "\\":
                    end = source.find("'", end + 1)
                i = n if end < 0 else end + 1
            elif i + 2 < n and source[i + 2] == "'":
                i += 3
            else:
                i += 1
        else:
            i += 1
    return "".join(code), "".join(full)


def skip_string(source, start, code):
    """Blank the body of the string that opens at `start` in `code`; return the end."""
    j, n = start + 1, len(source)
    while j < n and source[j] != '"':
        j += 2 if source[j] == "\\" else 1
    for k in range(start + 1, min(j, n)):
        if code[k] != "\n":
            code[k] = " "
    return j + 1


def strip_angles(text):
    """Drop generic arguments, so `Foo<Bar<T>>` becomes `Foo`."""
    out, depth = [], 0
    for ch in text:
        if ch == "<":
            depth += 1
        elif ch == ">" and depth:
            depth -= 1
        elif not depth:
            out.append(ch)
    return "".join(out)


def impl_name(header):
    """Return the self type of an `impl` header, as its last path segment."""
    header = strip_angles(header.replace("->", "  "))
    header = re.split(r"\bwhere\b", header)[0]
    target = re.split(r"\bfor\b", header)[-1]
    names = re.findall(r"[A-Za-z_]\w*", target.replace("dyn", " ").replace("mut", " "))
    return names[-1] if names else "impl"


class Frame:
    def __init__(self, kind, name, start, test):
        self.kind, self.name, self.start, self.end, self.test = kind, name, start, None, test
        self.parent = None

    def chain(self):
        names, frame = [], self
        while frame:
            if frame.kind in ("fn", "impl", "trait", "mod"):
                names.append(frame.name)
            elif frame.kind == "macro":
                names.append(frame.name + "!")
            frame = frame.parent
        return "::".join(reversed(names))

    def owner(self):
        """The innermost enclosing function or macro, which owns a query site."""
        frame = self
        while frame and frame.kind not in ("fn", "macro"):
            frame = frame.parent
        return frame


def frames_of(code):
    """Walk items and braces; return `(frames, test_file, test_mods)`."""
    root = Frame("file", "", 0, False)
    stack, frames = [root], []
    pending, pending_test, test_file, test_mods = None, False, False, []
    i = 0
    while True:
        m = ITEM.search(code, i)
        if not m:
            break
        tok = m.group(0)
        i = m.end()
        if tok.startswith("#"):
            depth, j = 1, i
            while j < len(code) and depth:
                depth += {"[": 1, "]": -1}.get(code[j], 0)
                j += 1
            attr = "#[" + code[i:j].strip()
            if TEST_ATTR.match(re.sub(r"\s+", "", attr)):
                if tok.startswith("#!") and len(stack) == 1:
                    test_file = True
                pending_test = True
            i = j
        elif tok == "{":
            kind, name = pending if pending else ("block", "")
            frame = Frame(kind, name, m.start(), stack[-1].test or pending_test)
            frame.parent = stack[-1]
            stack.append(frame)
            frames.append(frame)
            pending, pending_test = None, False
        elif tok == "}":
            if len(stack) > 1:
                stack.pop().end = m.end()
        elif tok == ";":
            if pending and pending[0] == "mod" and pending_test:
                test_mods.append(pending[1])
            pending, pending_test = None, False
        elif tok == "impl":
            before = code[: m.start()].rstrip()
            if not before or before[-1] in "{};]" or re.search(r"\b(?:unsafe|default)$", before):
                brace = re.compile(r"[{;]").search(code, i)
                end = brace.start() if brace else len(code)
                pending = ("impl", impl_name(code[i:end]))
        elif m.group(1):
            pending = ("fn", m.group(1))
        elif m.group(2):
            pending = ("trait", m.group(2))
        elif m.group(3):
            pending = ("mod", m.group(3))
        elif m.group(4):
            pending = ("macro", m.group(4))
    for frame in stack[1:]:
        frame.end = len(code)
    return frames, test_file, test_mods


def innermost(frames, pos):
    best = None
    for frame in frames:
        if frame.start <= pos < frame.end and (best is None or frame.start >= best.start):
            best = frame
    return best


def call_arg(code, open_paren):
    """Return the argument text of the call whose `(` is at `open_paren`."""
    depth, j = 0, open_paren
    while j < len(code):
        depth += {"(": 1, ")": -1}.get(code[j], 0)
        if depth == 0:
            return code[open_paren + 1 : j]
        j += 1
    return code[open_paren + 1 :]


def top_level_args(arg):
    """Count the arguments of a call, ignoring commas inside nested brackets."""
    depth, count = 0, 1
    for ch in arg:
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            count += 1
    return count - (1 if arg.rstrip().endswith(",") else 0)


def query_sites(code):
    """Yield `(offset, backend)` for each query call; `backend` is None when either fits.

    A Diesel method takes the connection as its one argument, so a method of
    the same name with more arguments is some other `load` or `first`.
    """
    for pattern, backend in ((POSTGRES_CALL, "postgres"), (SQLITE_CALL, "sqlite"), (EITHER_CALL, None)):
        for m in pattern.finditer(code):
            arg = call_arg(code, m.end() - 1)
            if not arg.strip() or ATOMIC_ARG.search(arg):
                continue
            args = top_level_args(arg)
            if backend == "postgres" and m.group(0).startswith(".") and args != 1:

                continue
            if backend is None and args > 1:
                backend = "sqlite"
            yield m.start(), backend


# A same-file function or macro whose name says it holds SQL: `claim_query_for()`.
SQL_HOLDER = re.compile(r"query|sql|stmt|ddl|statement")
CONST_ITEM = re.compile(r"\b(?:const|static)\s+([A-Z][A-Z0-9_]*)\s*:[^=;]*=")


def item_end(code, start):
    """Return the offset of the `;` that ends the item, skipping nested brackets."""
    depth = 0
    for j in range(start, len(code)):
        ch = code[j]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == ";" and depth <= 0:
            return j
    return len(code)


def sql_holders(code, frames):
    """Map each same-file SQL-holding function, macro or constant name to its spans."""
    holders = {}
    for frame in frames:
        if frame.kind in ("fn", "macro") and not frame.test and SQL_HOLDER.search(frame.name.lower()):
            holders.setdefault(frame.name, []).append((frame.start, frame.end))
    for m in CONST_ITEM.finditer(code):
        holders.setdefault(m.group(1), []).append((m.end(), item_end(code, m.end())))
    return holders


def with_holders(full, span, holders, depth=6):
    """Return `span` plus the span of each SQL holder that it names, `depth` levels deep."""
    seen, spans, frontier = set(), [span], [span]
    for _ in range(depth):
        nxt = []
        for start, end in frontier:
            for name in set(IDENT.findall(full, start, end)) - seen:
                if name in holders:
                    seen.add(name)
                    nxt += holders[name]
        spans += nxt
        frontier = nxt
    return spans


def sql_view(code, full):
    """Keep only string contents, so `"UPDATE t ", "SET"` reads as SQL text.

    A line continuation or a `\\n` escape inside a string becomes spaces. The
    view keeps every offset, like the other two.
    """
    text = "".join(f if c != f else " " for c, f in zip(code, full))
    return re.sub(r"\\(?:\s+|[nt])", lambda m: " " * len(m.group(0)), text)


def scan_source(source, tables):
    """Return `{key: {"line", "backend", "kind", "tables"}}` for one Rust file.

    The SQL of a call often lives in a builder function or a constant of the
    same file, such as `sql_query(claim_query())`. The scan reads those too,
    when a function or macro name contains `query`, `sql`, `stmt`, `ddl` or
    `statement`, and for an upper-case constant or static.
    """
    code, full = lex(source)
    if not DB_FILE.search(code):
        return {}
    frames, test_file, _ = frames_of(code)
    if test_file:
        return {}
    holders = sql_holders(code, frames)
    sql = sql_view(code, full)
    sqlite_file = bool(re.search(r"\brusqlite\b", code))
    postgres_file = bool(re.search(r"\b(?:diesel|diesel_async|AsyncPgConnection)\b", code))
    found = {}
    for pos, backend in query_sites(code):
        frame = innermost(frames, pos)
        owner = frame.owner() if frame else None
        if owner is None or frame.test:
            continue
        body_code = code[owner.start : owner.end]
        spans = with_holders(full, (owner.start, owner.end), holders)
        body = "\n".join(full[a:b] for a, b in spans)
        body_sql = " ".join(re.sub(r"\s+", " ", sql[a:b]) for a, b in spans)
        if backend is None:
            if sqlite_file and not postgres_file:
                backend = "sqlite"
            elif postgres_file and not sqlite_file:
                backend = "postgres"
            elif SQLITE_CALL.search(body_code) or re.search(r"\brusqlite\b|\bConnection\b|\bTransaction\b", body_code):
                backend = "sqlite"
            else:
                backend = "postgres"
        key = owner.chain()
        entry = found.setdefault(
            key,
            {"line": source.count("\n", 0, pos) + 1, "backend": set(), "kind": set(), "tables": set()},
        )
        entry["backend"].add(backend)
        entry["kind"] |= kinds_of(body_sql, body_code)
        entry["tables"] |= {t for t in IDENT.findall(body) if t in tables}
    return found


def kinds_of(body_sql, body_code):
    """Detect `ddl`, `write` and `read` from the SQL text and the Diesel calls."""
    kinds = set()
    if DDL_SQL.search(body_sql):
        kinds.add("ddl")
    if WRITE_SQL.search(body_sql) or WRITE_DSL.search(body_code):
        kinds.add("write")
    if READ_SQL.search(body_sql) or READ_DSL.search(body_code):
        kinds.add("read")
    return kinds


def known_tables(paths_and_sources):
    """Every table that a migration or a shipped `CREATE TABLE` names."""
    tables = set()
    for path, source in paths_and_sources:
        if path.endswith(".rs"):
            code, full = lex(source)
            frames, test_file, _ = frames_of(code)
            if test_file:
                continue
            text = list(full)
            for frame in frames:
                if frame.test and not (frame.parent and frame.parent.test):
                    for k in range(frame.start, frame.end):
                        if text[k] != "\n":
                            text[k] = " "
            source = "".join(text)
        tables |= {t for t in CREATE_TABLE.findall(source) if len(t) > 3}
    return tables


def catalog_path(source_path):
    """`autumn-harvest/src/dst/mod.rs` -> `docs/ledger/autumn-harvest/dst/mod.toml`."""
    crate, _, rest = source_path.partition("/src/")
    return (CATALOG / crate / rest).with_suffix(".toml").as_posix()


def check_profile(where, key, profile, detected, read_text):
    found = []
    unknown = set(profile) - FIELDS
    if unknown:
        found.append(f"{where}: [\"{key}\"] has unknown field(s) {sorted(unknown)}")
    driver = profile.get("driver")
    if driver not in DRIVERS:
        found.append(f"{where}: [\"{key}\"] driver {driver!r} is not one of {sorted(DRIVERS)}")
    for field, allowed in (("backend", BACKENDS), ("kind", KINDS), ("tables", None)):
        value = profile.get(field)
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            found.append(f"{where}: [\"{key}\"] {field} must be a list of strings")
            continue
        bad = [v for v in value if allowed and v not in allowed]
        if bad:
            found.append(f"{where}: [\"{key}\"] {field} {bad} is not one of {list(allowed)}")
        if detected:
            missing = sorted(set(detected[field]) - set(value))
            if missing:
                found.append(f"{where}: [\"{key}\"] {field} omits detected {missing}")
    measured = profile.get("measured")
    if not isinstance(measured, str) or not measured:
        found.append(f"{where}: [\"{key}\"] measured must be {UNMEASURED!r} or a docs page")
    elif measured != UNMEASURED:
        page = measured.split("#", 1)[0]
        text = read_text(page)
        name = key.rsplit("::", 1)[-1].rstrip("!")
        if text is None:
            found.append(f"{where}: [\"{key}\"] measured page {page} does not exist")
        elif not re.search(r"\b" + re.escape(name) + r"\b", text):
            found.append(f"{where}: [\"{key}\"] measured page {page} does not name `{name}`")
    return found


def findings(sources, catalogs, read_text):
    """Compare scanned query sites with the catalog; return one message per defect.

    `sources` maps a source path to its text. `catalogs` maps a catalog path
    to its parsed TOML. `read_text` returns a repository file's text, or None.
    """
    tables = known_tables(list(sources.items()))
    excluded = set()
    for path, source in sources.items():
        _, _, test_mods = frames_of(lex(source)[0])
        base = Path(path).parent if Path(path).name in ("lib.rs", "main.rs", "mod.rs") else Path(path).with_suffix("")
        for mod in test_mods:
            excluded.add((base / f"{mod}.rs").as_posix())
            excluded.add((base / mod / "mod.rs").as_posix())
    found = []
    expected = set()
    for path in sorted(sources):
        if path in excluded or "/src/" not in path:
            continue
        detected = scan_source(sources[path], tables)
        if not detected and catalog_path(path) not in catalogs:
            continue
        where = catalog_path(path)
        expected.add(where)
        catalog = catalogs.get(where, {})
        for key, entry in sorted(detected.items()):
            if key not in catalog:
                found.append(f"{where}: no profile for `{key}` ({path}:{entry['line']})")
                continue
            found += check_profile(where, key, catalog[key], entry, read_text)
        for key in sorted(set(catalog) - set(detected)):
            found.append(f"{where}: profile [\"{key}\"] matches no query site in {path}")
    for where in sorted(set(catalogs) - expected):
        found.append(f"{where}: no source file has query sites for this catalog")
    return found


def scaffold(sources, catalogs):
    """Return TOML stubs for each function that has no profile yet."""
    tables = known_tables(list(sources.items()))
    out = []
    for path in sorted(sources):
        where = catalog_path(path)
        catalog = catalogs.get(where, {})
        missing = {k: v for k, v in scan_source(sources[path], tables).items() if k not in catalog}
        if missing:
            out.append(f"# {where}")
            out += [render(key, entry, "CHOOSE") for key, entry in sorted(missing.items())]
    return "\n".join(out)


def render(key, entry, driver, measured=UNMEASURED):
    def items(values):
        return "[" + ", ".join(f'"{v}"' for v in sorted(values)) + "]"

    return (
        f'["{key}"]\n'
        f'driver = "{driver}"\n'
        f"backend = {items(entry['backend'])}\n"
        f"kind = {items(entry['kind'])}\n"
        f"tables = {items(entry['tables'])}\n"
        f'measured = "{measured}"\n'
    )


def load_repo():
    sources = {}
    for crate in CRATES:
        for path in sorted((ROOT / crate / "src").rglob("*.rs")):
            sources[path.relative_to(ROOT).as_posix()] = path.read_text(encoding="utf-8")
    for path in sorted(ROOT.glob("*/migrations/**/up.sql")):
        sources[path.relative_to(ROOT).as_posix()] = path.read_text(encoding="utf-8")
    catalogs = {}
    for path in sorted((ROOT / CATALOG).rglob("*.toml")):
        with path.open("rb") as handle:
            catalogs[path.relative_to(ROOT).as_posix()] = tomllib.load(handle)
    return sources, catalogs


def read_repo_text(rel):
    path = ROOT / rel
    return path.read_text(encoding="utf-8") if path.is_file() else None


def run(argv):
    sources, catalogs = load_repo()
    if "--scaffold" in argv:
        print(scaffold(sources, catalogs))
        return 0
    found = findings(sources, catalogs, read_repo_text)
    for message in found:
        print(message)
    profiles = sum(len(c) for c in catalogs.values())
    print(f"ledger-profiles: {profiles} profiles in {len(catalogs)} files, {len(found)} findings")
    if found:
        print(
            "Fix: add or update the profile in docs/ledger/. "
            "`--scaffold` prints a stub for each missing one. See docs/ledger/README.md."
        )
    return 1 if found else 0


FIXTURE = r'''
use diesel_async::RunQueryDsl;

const NOTE: &str = "a string that says .execute(conn) is not a call";

pub async fn claim(conn: &mut AsyncPgConnection) -> QueryResult<usize> {
    // conn.batch_execute("DROP TABLE harvest_jobs") in a comment is not a call.
    let n = counter.load(Ordering::SeqCst);
    sql_query("UPDATE harvest_jobs SET state = 'RUNNING' WHERE id = $1")
        .execute(conn)
        .await
}

impl<'a> Reader for Store<'a> {
    async fn depth(&self, conn: &mut AsyncPgConnection) -> i64 {
        let label = 'x';
        harvest_jobs::table.count().get_result(conn).await.unwrap_or(0)
    }
}

macro_rules! count_rows {
    ($conn:expr) => {
        sql_query(r#"SELECT count(*) FROM harvest_jobs"#).load($conn)
    };
}

fn helper(items: &[u8]) -> Option<&u8> {
    items.first()
}

const PURGE_SQL: &str = "DELETE FROM harvest_archive WHERE id = $1";

fn archive_query() -> String {
    format!("SELECT id FROM harvest_archive LIMIT {}", 10)
}

pub async fn newest(conn: &mut AsyncPgConnection) -> QueryResult<(i64, String)> {
    harvest_jobs::table.select((id, state)).first::<(i64, String)>(conn).await
}

macro_rules! park_sql {
    () => {
        concat!("UPDATE harvest_jobs j \
         ", "SET state = 'PARKED'")
    };
}

pub async fn park(conn: &mut AsyncPgConnection) -> QueryResult<usize> {
    sql_query(park_sql!()).execute(conn).await
}

pub async fn purge(conn: &mut AsyncPgConnection) -> QueryResult<()> {
    let rows: Vec<IdRow> = sql_query(archive_query()).load(conn).await?;
    cache.load(&slot, rows.len());
    sql_query(PURGE_SQL).execute(conn).await.map(drop)
}

#[cfg(test)]
mod tests {
    #[test]
    fn seeds() {
        sql_query("INSERT INTO harvest_jobs DEFAULT VALUES").execute(conn);
    }
}
'''

SQLITE_FIXTURE = r'''
use rusqlite::Connection;

pub fn open(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS harvest_jobs (id INTEGER)")?;
    conn.execute("DELETE FROM harvest_jobs", [])?;
    Ok(())
}
'''

MIGRATION = """CREATE TABLE harvest_jobs (id BIGINT PRIMARY KEY);
CREATE UNLOGGED TABLE IF NOT EXISTS harvest_archive (id BIGINT);"""


def self_test():
    tables = known_tables([("m/migrations/1/up.sql", MIGRATION)])
    assert tables == {"harvest_jobs", "harvest_archive"}, tables

    code, full = lex(FIXTURE)
    assert len(code) == len(full) == len(sql_view(code, full)) == len(FIXTURE)

    sites = scan_source(FIXTURE, tables)
    assert set(sites) == {"claim", "Store::depth", "count_rows!", "newest", "park", "purge"}, sites
    assert sites["newest"]["kind"] == {"read"}, sites["newest"]
    assert sites["park"]["kind"] == {"write"}, sites["park"]
    assert sites["park"]["tables"] == {"harvest_jobs"}, sites["park"]
    assert sites["purge"]["kind"] == {"read", "write"}, sites["purge"]
    assert sites["purge"]["tables"] == {"harvest_archive"}, sites["purge"]
    assert sites["claim"]["backend"] == {"postgres"}, sites["claim"]
    assert sites["claim"]["kind"] == {"write"}, sites["claim"]
    assert sites["claim"]["tables"] == {"harvest_jobs"}, sites["claim"]
    assert sites["Store::depth"]["kind"] == {"read"}, sites["Store::depth"]
    assert sites["count_rows!"]["kind"] == {"read"}, sites["count_rows!"]

    sites = scan_source(SQLITE_FIXTURE, tables)
    assert set(sites) == {"open"}, sites
    assert sites["open"]["backend"] == {"sqlite"}, sites["open"]
    assert sites["open"]["kind"] == {"ddl", "write"}, sites["open"]

    pages = {"docs/performance-claim.md": "Ledger measured `claim` here."}
    sources = {
        "c/src/queue.rs": FIXTURE,
        "c/src/lib.rs": "#[cfg(test)]\nmod fixtures;\n",
        "c/src/fixtures.rs": SQLITE_FIXTURE,
        "c/migrations/1/up.sql": MIGRATION,
    }
    good = {
        "claim": {"driver": "per_task", "backend": ["postgres"], "kind": ["write"],
                  "tables": ["harvest_jobs"], "measured": "docs/performance-claim.md#x"},
        "Store::depth": {"driver": "per_tick", "backend": ["postgres"], "kind": ["read"],
                         "tables": ["harvest_jobs"], "measured": "unmeasured"},
        "count_rows!": {"driver": "per_request", "backend": ["postgres"], "kind": ["read"],
                        "tables": ["harvest_jobs"], "measured": "unmeasured"},
        "newest": {"driver": "per_request", "backend": ["postgres"], "kind": ["read"],
                   "tables": ["harvest_jobs"], "measured": "unmeasured"},
        "park": {"driver": "per_task", "backend": ["postgres"], "kind": ["write"],
                 "tables": ["harvest_jobs"], "measured": "unmeasured"},
        "purge": {"driver": "setup", "backend": ["postgres"], "kind": ["read", "write"],
                  "tables": ["harvest_archive"], "measured": "unmeasured"},
    }
    catalogs = {"docs/ledger/c/queue.toml": good}
    assert findings(sources, catalogs, pages.get) == [], findings(sources, catalogs, pages.get)

    missing = {"docs/ledger/c/queue.toml": {k: v for k, v in good.items() if k != "claim"}}
    assert findings(sources, missing, pages.get) == [
        "docs/ledger/c/queue.toml: no profile for `claim` (c/src/queue.rs:9)"
    ], findings(sources, missing, pages.get)

    stale = {"docs/ledger/c/queue.toml": dict(good, old_name=good["claim"]),
             "docs/ledger/c/gone.toml": {}}
    assert findings(sources, stale, pages.get) == [
        "docs/ledger/c/queue.toml: profile [\"old_name\"] matches no query site in c/src/queue.rs",
        "docs/ledger/c/gone.toml: no source file has query sites for this catalog",
    ], findings(sources, stale, pages.get)

    wrong = dict(good)
    wrong["claim"] = dict(good["claim"], driver="often", kind=["read"], tables=[],
                          measured="docs/performance-other.md", extra=1)
    wrong["Store::depth"] = dict(good["Store::depth"], measured="docs/performance-claim.md")
    assert findings(sources, {"docs/ledger/c/queue.toml": wrong}, pages.get) == [
        "docs/ledger/c/queue.toml: [\"Store::depth\"] measured page docs/performance-claim.md "
        "does not name `depth`",
        "docs/ledger/c/queue.toml: [\"claim\"] has unknown field(s) ['extra']",
        f"docs/ledger/c/queue.toml: [\"claim\"] driver 'often' is not one of {sorted(DRIVERS)}",
        "docs/ledger/c/queue.toml: [\"claim\"] kind omits detected ['write']",
        "docs/ledger/c/queue.toml: [\"claim\"] tables omits detected ['harvest_jobs']",
        "docs/ledger/c/queue.toml: [\"claim\"] measured page docs/performance-other.md does not exist",
    ], findings(sources, {"docs/ledger/c/queue.toml": wrong}, pages.get)

    stub = scaffold({"c/src/queue.rs": FIXTURE}, {})
    assert '["claim"]\ndriver = "CHOOSE"\nbackend = ["postgres"]' in stub, stub

    print("ledger-profiles self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run(sys.argv[1:]))
