#!/usr/bin/env python3
"""Measure the access path of each Ledger-profiled query (issue #2366).

`docs/audits/ledger-profiles.py` keeps a profile for every function that
sends SQL. Most profiles say `measured = "unmeasured"`. This script measures
them in bulk, in two steps.

`attribute` reads the raw census that `autumn-harvest/src/ledger_census.rs`
writes while the DB suites run. Each line holds one statement and the source
frames of its call stack. The script maps the innermost frame that falls in a
profiled function to that profile. It writes the result to STATEMENTS, which
is committed, so the second step needs no test run.

`plan` runs `EXPLAIN (GENERIC_PLAN, FORMAT JSON)` for each statement on a
migrated database, with `enable_seqscan = off`. The setting makes the planner
take an index whenever one can serve the predicate. A `Seq Scan` that remains
means that no index can, whatever the size of the table. So the access path
does not depend on fixture data. The step writes one page for each source
file under `docs/ledger/plans/`, and it sets the `plan` field of each profile
that it measured.

`plan-sqlite` does the same for SQLite statements, with `EXPLAIN QUERY PLAN`
on an in-memory database built from the SQLite DDL that the census recorded
(issue #2429). A `SCAN` that names no index is the finding.

A plan is not a buffer count. `measured` stays for a Ledger page with real
buffers. A `plan` page shows which index each statement uses, and which
statements scan a table with no usable index.

Usage:
    HARVEST_LEDGER_CENSUS=/var/tmp/census.jsonl \\
        cargo test -p autumn-harvest --test integration -- --test-threads=6
    python3 docs/audits/ledger-census.py attribute /var/tmp/census.jsonl
    python3 docs/audits/ledger-census.py plan postgres://postgres@127.0.0.1/ledger_plan [--driver per_task]
    python3 docs/audits/ledger-census.py plan-sqlite [--driver per_task]
    python3 docs/audits/ledger-census.py --self-test

Leave `HARVEST_TEST_DATABASE_URL` unset, so that each suite boots a migrated
container. The plan database needs every migration in `autumn-harvest/migrations`.
"""
import importlib.util
import json
import re
import sqlite3
import subprocess
import sys
import tomllib
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATEMENTS = Path("docs/perf-artifacts/ledger-census/statements.json")
PLANS = Path("docs/ledger/plans")
CATALOG = Path("docs/ledger")

_spec = importlib.util.spec_from_file_location("ledger_profiles", ROOT / "docs/audits/ledger-profiles.py")
lp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lp)

# Leading `/* ... */` and `-- ...` comments are skipped, such as the claim
# ageing marker in `queue.rs`.
PLANNABLE = re.compile(
    r"^(?:\s|/\*.*?\*/|--[^\n]*\n)*(?:\(\s*)*(?:SELECT|INSERT|UPDATE|DELETE|WITH|VALUES|MERGE|TABLE)\b",
    re.IGNORECASE | re.DOTALL,
)
NO_INDEX = "no usable index"
FULL_INDEX = "full scan of index"
PARTIAL_INDEX = "full scan of partial index"
# A partial-index predicate names a state such as 'RUNNING'. Only such words
# are kept. A test key or payload looks like a secret to push protection.
STATE_WORD = re.compile(r"^[A-Za-z_]{1,32}$")
# A longer string is a payload, not a predicate value. It keeps the file small.
MAX_LITERAL = 64
# Literal sets kept for each statement.
MAX_LITERAL_SETS = 4
# A statement with more parameters is a bulk write. Its guessed types would
# fill most of the file, and a bulk write plans without them.
MAX_TYPED_PARAMS = 64


def line_starts(source):
    starts = [0]
    for i, ch in enumerate(source):
        if ch == "\n":
            starts.append(i + 1)
    return starts


class Resolver:
    """Map `crate/src/path.rs:line` to the profile key of its function."""

    def __init__(self, root, catalogs):
        self.root = root
        self.catalogs = catalogs
        self.cache = {}

    def _file(self, path):
        if path not in self.cache:
            full = self.root / path
            if not full.is_file():
                self.cache[path] = None
            else:
                source = full.read_text(encoding="utf-8")
                code, _ = lp.lex(source)
                frames, _, _ = lp.frames_of(code)
                self.cache[path] = (line_starts(source), frames)
        return self.cache[path]

    def key(self, frame):
        path, _, line = frame.rpartition(":")
        if not line.isdigit():
            return None
        entry = self._file(path)
        catalog = self.catalogs.get(lp.catalog_path(path))
        if entry is None or catalog is None:
            return None
        starts, frames = entry
        index = int(line) - 1
        if not 0 <= index < len(starts):
            return None
        inner = lp.innermost(frames, starts[index])
        owner = inner.owner() if inner else None
        while owner is not None:
            key = owner.chain()
            if key in catalog:
                return lp.catalog_path(path), key
            parent = owner.parent
            owner = parent.owner() if parent else None
        return None


def attribute(lines, resolver, skipped=None):
    """Return `{catalog: {key: {sql: [literals, ...]}}}` from raw census lines.

    `literals` maps a parameter number to the SQL literal of its bind value.
    A torn line is counted in `skipped`. Another sample of the same statement
    usually covers it.
    """
    found = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    sqlite = set()
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError:
            if skipped is not None:
                skipped.append(raw)
            continue
        for frame in record["frames"]:
            hit = resolver.key(frame)
            if hit:
                lits = literals(record.get("binds", ""))
                sql = normalize(record["sql"])
                found[hit[0]][hit[1]][sql].add(json.dumps(lits, sort_keys=True))
                if record.get("backend") == "sqlite":
                    sqlite.add(sql)
                break
    return {
        c: {k: {sql: tagged(shape([json.loads(m) for m in maps]), sql in sqlite)
                for sql, maps in sorted(sqls.items())}
            for k, sqls in sorted(keys.items())}
        for c, keys in sorted(found.items())
    }


def tagged(entry, sqlite):
    """Mark a SQLite statement, so each planner takes only its own backend."""
    return {**entry, "backend": "sqlite"} if sqlite else entry


def shape(maps):
    """Split literal maps into `{"types": ..., "literals": [...]}`.

    The types of a statement rarely change between calls, so one map holds
    them. Each distinct literal set is kept, up to MAX_LITERAL_SETS.
    """
    types, sets = {}, set()
    for lits in maps:
        for key, value in lits.items():
            if key.startswith("::"):
                types.setdefault(key[2:], value)
        sets.add(json.dumps({k: v for k, v in lits.items() if not k.startswith("::")}, sort_keys=True))
    literal_sets = [json.loads(m) for m in sorted(sets)][:MAX_LITERAL_SETS]
    if len(types) > MAX_TYPED_PARAMS:
        types = {}
    return {"types": dict(sorted(types.items(), key=lambda kv: int(kv[0]))), "literals": literal_sets}


def normalize(sql):
    """Collapse indentation, but keep each newline that ends a `--` comment."""
    sql = re.sub(r"[ \t]+", " ", sql)
    return re.sub(r" ?\n[ \n]*", "\n", sql).strip()


def merge(old, new):
    out = {c: {k: dict(v) for k, v in keys.items()} for c, keys in old.items()}
    for catalog, keys in new.items():
        for key, sqls in keys.items():
            slot = out.setdefault(catalog, {}).setdefault(key, {})
            for sql, entry in sqls.items():
                prior = slot.get(sql, {"types": {}, "literals": []})
                maps = [{**{f"::{n}": t for n, t in prior["types"].items()}, **m} for m in prior["literals"]]
                maps += [{**{f"::{n}": t for n, t in entry["types"].items()}, **m} for m in entry["literals"]]
                slot[sql] = tagged(shape(maps), "backend" in prior or "backend" in entry)
            out[catalog][key] = dict(sorted(slot.items()))
    return {c: dict(sorted(k.items())) for c, k in sorted(out.items())}


def split_top(text):
    """Split `a, [b, c], "d, e"` at its top-level commas."""
    parts, depth, quoted, escaped, start = [], 0, False, False, 0
    for i, ch in enumerate(text):
        if quoted:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                quoted = False
        elif ch == '"':
            quoted = True
        elif ch in "[({":
            depth += 1
        elif ch in "])}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i].strip())
            start = i + 1
    tail = text[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def sql_string(debug):
    """Return a quoted SQL literal for a Rust `Debug` string, or None."""
    try:
        value = json.loads(debug)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, str) or not STATE_WORD.match(value):
        return None
    return "'" + value + "'"


def literal(debug):
    """The SQL literal of one bind's `Debug` text, for a bool or a string."""
    if debug in ("true", "false"):
        return debug
    if debug.startswith("Some(") and debug.endswith(")"):
        return literal(debug[5:-1])
    if debug.startswith('"'):
        return sql_string(debug)
    if debug.startswith("[") and debug.endswith("]"):
        items = [sql_string(i) if i.startswith('"') else None for i in split_top(debug[1:-1])]
        if items and all(items):
            return "ARRAY[" + ", ".join(items) + "]"
    return None


UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
TIMESTAMPTZ = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z$")
JSON_DEBUG = ("Object {", "Array [", "String(", "Number(", "Bool(", "Null")


def bind_type(debug):
    """Guess the Postgres type of one bind from its `Debug` text, or None.

    A generic plan cannot infer the type of `$3[2]` or `unnest($1)`. The
    guess is used only after a plan fails, so a wrong guess costs nothing.
    """
    if debug in ("true", "false"):
        return "boolean"
    if debug.startswith("Some(") and debug.endswith(")"):
        return bind_type(debug[5:-1])
    if re.fullmatch(r"-?\d+", debug):
        return "bigint"
    if re.fullmatch(r"-?\d+\.\d+(?:e-?\d+)?", debug):
        return "float8"
    if UUID.match(debug):
        return "uuid"
    if TIMESTAMPTZ.match(debug):
        return "timestamptz"
    if debug.startswith('"'):
        return "text"
    if debug.startswith(JSON_DEBUG):
        return "jsonb"
    if debug.startswith("[") and debug.endswith("]"):
        for item in split_top(debug[1:-1]):
            inner = bind_type(item)
            if inner and not inner.endswith("[]"):
                return inner + "[]"
            if inner:
                return None
    return None


def literals(binds):
    """Map parameter numbers to the literals that a custom plan would see.

    Only bool values and state words are kept. They decide whether a partial index
    such as `WHERE NOT fired` applies. A generic plan cannot prove that.
    The key `::n` holds the guessed type of parameter `n`.
    """
    binds = binds.strip()
    if not (binds.startswith("[") and binds.endswith("]")):
        return {}
    out = {}
    for number, debug in enumerate(split_top(binds[1:-1]), 1):
        value = literal(debug)
        if value is not None and len(value) <= MAX_LITERAL:
            out[str(number)] = value
        kind = bind_type(debug)
        if kind:
            out[f"::{number}"] = kind
    return out


def substitute(sql, lits, cast=False):
    """Put each literal in place of its parameter, then renumber the rest.

    With `cast`, each remaining parameter with a guessed type gets a cast.
    """
    numbers = {}

    def swap(match):
        number = match.group(1)
        if number in lits:
            return lits[number]
        numbers.setdefault(number, len(numbers) + 1)
        kind = lits.get(f"::{number}") if cast else None
        return f"${numbers[number]}::{kind}" if kind else f"${numbers[number]}"

    return re.sub(r"\$(\d+)\b", swap, sql)


def bitmap_indexes(node):
    """Index names under a bitmap heap scan, without its subplans."""
    names = set()
    for child in node.get("Plans", []):
        if not child.get("Node Type", "").startswith("Bitmap"):
            continue
        if "Index Name" in child:
            names.add(child["Index Name"])
        names |= bitmap_indexes(child)
    return names


def unindexed(access):
    """Whether no index can serve the access, whatever the statistics."""
    return access == NO_INDEX


def walk(node, out, partial=frozenset()):
    relation = node.get("Relation Name")
    if relation:
        kind = node.get("Node Type", "")
        if kind == "Seq Scan":
            access = NO_INDEX
        elif "Index Name" in node and "Index Cond" not in node:
            label = PARTIAL_INDEX if node["Index Name"] in partial else FULL_INDEX
            access = f"{label} `{node['Index Name']}`"
        elif "Index Name" in node:
            access = f"{kind} on `{node['Index Name']}`"
        elif kind == "Bitmap Heap Scan":
            names = sorted(bitmap_indexes(node))
            access = f"{kind} on " + ", ".join(f"`{n}`" for n in names)
        else:
            access = kind
        out.add((relation, access))
    for child in node.get("Plans", []):
        walk(child, out, partial)
    return out


def access_paths(plan_json, partial=frozenset()):
    """Return the sorted `(table, access)` pairs of an EXPLAIN JSON plan."""
    plans = json.loads(plan_json)
    found = set()
    for entry in plans:
        walk(entry["Plan"], found, partial)
    return sorted(found)


def partial_indexes(url):
    """Names of the partial indexes, whose predicate bounds a full scan."""
    done = subprocess.run(
        ["psql", "-X", "-q", "-A", "-t", url, "-c",
         "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indpred IS NOT NULL"],
        capture_output=True, text=True, check=True,
    )
    return frozenset(done.stdout.split())


def explain(url, sql, partial=frozenset()):
    """Return `(paths, error)` for one statement."""
    script = (
        "BEGIN;\nSET LOCAL enable_seqscan = off;\n"
        f"EXPLAIN (GENERIC_PLAN, FORMAT JSON) {sql.rstrip().rstrip(';')};\nROLLBACK;\n"
    )
    done = subprocess.run(
        ["psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", url],
        input=script, capture_output=True, text=True, check=False,
    )
    if done.returncode != 0:
        error = done.stderr.strip().splitlines()
        return None, error[0] if error else "psql failed"
    text = done.stdout.strip()
    start = text.find("[")
    return access_paths(text[start:], partial), None


def plan_statement(url, sql, entry, partial):
    """Plan one statement once for each set of bind literals that it saw.

    The union of the access paths is the result. A literal set that does not
    plan is tried again with type casts, then without its literals.
    """
    paths, errors = set(), []
    types = {f"::{n}": t for n, t in entry["types"].items()}
    for literal_set in [m for m in entry["literals"] if m] or [{}]:
        lits = {**types, **literal_set}
        for text in (substitute(sql, lits), substitute(sql, lits, cast=True),
                     substitute(sql, {}), substitute(sql, types, cast=True)):
            found, error = explain(url, text, partial)
            if not error:
                break
        if error:
            errors.append(error)
        else:
            paths |= set(found)
    if not paths and errors:
        return None, errors[0]
    return sorted(paths), None


def first_line(sql, width=100):
    flat = re.sub(r"\s+", " ", sql).strip()
    return flat if len(flat) <= width else flat[: width - 3] + "..."


SQLITE_SECTION = "Backend: SQLite."


def is_sqlite(rows):
    """Whether a section, new or kept from an earlier run, plans SQLite."""
    if isinstance(rows, tuple):
        return SQLITE_SECTION in rows[1]
    return any(len(row) > 3 and row[3] for row in rows)


def render_page(source_path, results):
    """Render one generated page; `results` maps key -> [(sql, paths, error[, sqlite])]."""
    sqlite = any(is_sqlite(rows) for rows in results.values())
    postgres = any(not is_sqlite(rows) for rows in results.values())
    out = [
        f"# Ledger plans: `{source_path}`",
        "",
        "Generated by `docs/audits/ledger-census.py plan` (issue #2366). Do not edit.",
    ]
    each = "Each Postgres statement" if sqlite else "Each statement"
    if postgres:
        out += [
            f"{each} was planned with `EXPLAIN (GENERIC_PLAN)` and",
            "`enable_seqscan = off`. Each bool or string bind value that the census saw",
            "is put in place of its parameter, as in a custom plan, so a partial index",
            "on `NOT fired` applies.",
        ]
    if sqlite:
        each = "Each SQLite statement" if postgres else "Each statement"
        out += [
            f"{each} was planned with `EXPLAIN QUERY PLAN`, on the schema",
            "that the census recorded (issue #2429). SQLite has no `enable_seqscan`. With",
            "no statistics it assumes a large table, so it takes any index that can serve",
            "the predicate. A `SCAN` that names no index is the same finding.",
        ]
    out += [
        "",
        f"**{NO_INDEX}** is the finding. It means that no index can serve the",
        "predicate on that table, so the scan grows with the table. It does not",
        "depend on table statistics. The header of each section lists these tables.",
        "",
        "The other rows show the index that the planner chose on empty tables.",
        "Production statistics can change that choice. A",
        f"**{FULL_INDEX}** or **{PARTIAL_INDEX}** row is often such a choice,",
        "where a keyed index also exists. It is a hint, not a finding.",
        "A full scan of a partial index reads only the rows that match its predicate.",
        "",
        "The full statement text is in `docs/perf-artifacts/ledger-census/statements.json`.",
        "",
    ]
    for key, rows in results.items():
        if isinstance(rows, tuple):
            out.append("## " + rows[1].rstrip("\n"))
            out.append("")
            continue
        flags = sorted({t for _, paths, *_ in rows for t, a in (paths or []) if unindexed(a)})
        backend = f" {SQLITE_SECTION}" if is_sqlite(rows) else ""
        out.append(f"## `{key}`")
        out.append("")
        out.append(f"Statements: {len(rows)}.{backend} Tables with no usable index: "
                   + (", ".join(f"`{t}`" for t in flags) or "none") + ".")
        out.append("")
        out.append("| # | Statement | Access |")
        out.append("|---|---|---|")
        for number, (sql, paths, error, *_) in enumerate(rows, 1):
            head = first_line(sql).replace("|", "\\|").replace("`", "'")
            if error:
                access = "not planned: " + error.replace("|", "/")
            elif not paths:
                access = "no table"
            else:
                access = "<br>".join(f"`{t}`: {a}" for t, a in paths)
            out.append(f"| {number} | `{head}` | {access} |")
        out.append("")
    return "\n".join(out)


def set_plan_field(text, key, page):
    """Set `plan = "<page>"` in the `["key"]` table of a catalog file's text."""
    header = f'["{key}"]\n'
    start = text.index(header)
    end = text.find("\n[", start + len(header))
    end = len(text) if end < 0 else end + 1
    block = text[start:end]
    line = f'plan = "{page}"\n'
    if re.search(r"^plan = .*\n", block, re.MULTILINE):
        block = re.sub(r"^plan = .*\n", line, block, flags=re.MULTILINE)
    else:
        block = re.sub(r"^(measured = .*\n)", r"\1" + line.replace("\\", "\\\\"), block, flags=re.MULTILINE)
    return text[:start] + block + text[end:]


def load_catalogs():
    catalogs = {}
    for path in sorted((ROOT / CATALOG).rglob("*.toml")):
        with path.open("rb") as handle:
            catalogs[path.relative_to(ROOT).as_posix()] = tomllib.load(handle)
    return catalogs


def source_of(catalog):
    """`docs/ledger/autumn-harvest/queue.toml` -> `autumn-harvest/src/queue.rs`."""
    rel = Path(catalog).relative_to(CATALOG)
    crate, rest = rel.parts[0], Path(*rel.parts[1:])
    return f"{crate}/src/{rest.with_suffix('.rs').as_posix()}"


def run_plan(url, driver):
    statements = json.loads((ROOT / STATEMENTS).read_text())
    catalogs = load_catalogs()
    partial = partial_indexes(url)
    pages, missing = [], []
    for catalog, keys in sorted(catalogs.items()):
        wanted = {k: v for k, v in keys.items()
                  if "postgres" in v.get("backend", []) and (driver is None or v.get("driver") == driver)}
        if not wanted:
            continue
        seen = statements.get(catalog, {})
        results = {}
        for key in sorted(wanted):
            sqls = {s: m for s, m in seen.get(key, {}).items()
                    if PLANNABLE.match(s) and "backend" not in m}
            if not sqls:
                missing.append(f"{catalog} {key}")
                continue
            results[key] = [(sql, *plan_statement(url, sql, entry, partial)) for sql, entry in sqls.items()]
        if not results:
            continue
        pages.append(write_plan_page(catalog, results))
    write_index()
    print(f"ledger-census: planned {len(pages)} pages; {len(missing)} profiles not reached by the suites")
    for item in missing:
        print(f"  not reached: {item}")
    return 0


# DDL kinds in replay order. A table must exist before its column, index or trigger.
SQLITE_DDL = ("CREATE TABLE", "CREATE VIRTUAL TABLE", "ALTER TABLE", "CREATE INDEX",
              "CREATE UNIQUE INDEX", "CREATE VIEW", "CREATE TRIGGER")
SQLITE_SCAN = re.compile(r"^SCAN (\w+)(?: AS \w+)?(?: USING (?:COVERING )?INDEX (\w+))?")
SQLITE_SEARCH = re.compile(
    r"^SEARCH (\w+)(?: AS \w+)? USING (?:(?:COVERING )?INDEX (\w+)|INTEGER PRIMARY KEY|PRIMARY KEY)")


def ddl_kind(sql):
    """The replay rank of a DDL statement, or None for any other statement."""
    head = re.sub(r"\s+", " ", re.sub(r"^(?:\s|--[^\n]*\n|/\*.*?\*/)*", "", sql, flags=re.DOTALL)).upper()
    head = head.replace(" IF NOT EXISTS", "")
    for rank, kind in enumerate(SQLITE_DDL):
        if head.startswith(kind):
            return rank
    return None


def sqlite_schema(statements):
    """Build an in-memory database from every SQLite DDL statement the census saw.

    The suites run the schema set-up, so the census holds it. A statement
    that fails, such as an `ALTER TABLE` that adds a column the table has, is
    skipped. Each run starts from a fresh schema, so it changes nothing.
    """
    ddl = sorted({(rank, sql) for keys in statements.values() for sqls in keys.values()
                  for sql, entry in sqls.items()
                  if entry.get("backend") == "sqlite" and (rank := ddl_kind(sql)) is not None})
    db = sqlite3.connect(":memory:")
    for _, sql in ddl:
        try:
            db.executescript(sql)
        except sqlite3.Error:
            pass
    tables = {name for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    partial = {name for name, sql in db.execute("SELECT name, sql FROM sqlite_master WHERE type = 'index'")
               if sql and re.search(r"\bWHERE\b", sql, re.IGNORECASE)}
    return db, tables, frozenset(partial)


def sqlite_access(detail, tables, partial=frozenset()):
    """Map one `EXPLAIN QUERY PLAN` row to `(table, access)`, or None."""
    match = SQLITE_SEARCH.match(detail)
    if match and match.group(1) in tables:
        index = match.group(2)
        return match.group(1), (f"Search on `{index}`" if index else "Search on the primary key")
    match = SQLITE_SCAN.match(detail)
    if match and match.group(1) in tables:
        index = match.group(2)
        if not index:
            return match.group(1), NO_INDEX
        label = PARTIAL_INDEX if index in partial else FULL_INDEX
        return match.group(1), f"{label} `{index}`"
    return None


def sqlite_params(sql):
    """Bind NULL to each parameter, so that the planner sees an unbound value."""
    named = re.findall(r"[:@$][A-Za-z_]\w*", sql)
    if named:
        return {name[1:]: None for name in named}
    numbered = [int(n) for n in re.findall(r"\?(\d+)", sql)]
    return [None] * max(numbered + [len(re.findall(r"\?(?!\d)", sql))])


def explain_sqlite(db, sql, tables, partial=frozenset()):
    """Return `(paths, error)` for one SQLite statement."""
    text = f"EXPLAIN QUERY PLAN {sql.rstrip().rstrip(';')}"
    try:
        rows = db.execute(text, sqlite_params(sql)).fetchall()
    except sqlite3.ProgrammingError as error:
        # A `?` inside a string literal is not a parameter. SQLite names the true count.
        uses = re.search(r"uses (\d+)", str(error))
        if not uses:
            return None, str(error).splitlines()[0]
        try:
            rows = db.execute(text, [None] * int(uses.group(1))).fetchall()
        except sqlite3.Error as retry:
            return None, str(retry).splitlines()[0]
    except sqlite3.Error as error:
        return None, str(error).splitlines()[0]
    found = {hit for row in rows if (hit := sqlite_access(row[-1], tables, partial))}
    return sorted(found), None


def run_plan_sqlite(driver):
    statements = json.loads((ROOT / STATEMENTS).read_text())
    catalogs = load_catalogs()
    db, tables, partial = sqlite_schema(statements)
    pages, missing = [], []
    for catalog, keys in sorted(catalogs.items()):
        wanted = {k: v for k, v in keys.items()
                  if "sqlite" in v.get("backend", []) and (driver is None or v.get("driver") == driver)}
        if not wanted:
            continue
        seen = statements.get(catalog, {})
        results = {}
        for key in sorted(wanted):
            ours = [s for s, m in seen.get(key, {}).items() if m.get("backend") == "sqlite"]
            sqls = [s for s in ours if PLANNABLE.match(s)]
            if not sqls:
                missing.append(f"{catalog} {key}" + (" (only DDL or pragmas)" if ours else ""))
                continue
            results[key] = [(sql, *explain_sqlite(db, sql, tables, partial), True) for sql in sqls]
        if not results:
            continue
        pages.append(write_plan_page(catalog, results))
    write_index()
    print(f"ledger-census: planned {len(pages)} SQLite pages from {len(tables)} tables; "
          f"{len(missing)} profiles not reached by the suites")
    for item in missing:
        print(f"  not reached: {item}")
    return 0


def write_plan_page(catalog, results):
    """Merge `results` into the catalog's plan page, and link each profile to it."""
    source = source_of(catalog)
    page = (PLANS / Path(catalog).relative_to(CATALOG)).with_suffix(".md")
    existing = ROOT / page
    merged = parse_existing(existing) if existing.is_file() else {}
    merged.update(results)
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_text(render_page(source, dict(sorted(merged.items()))) + "\n")
    text = (ROOT / catalog).read_text()
    for key in results:
        text = set_plan_field(text, key, page.as_posix())
    (ROOT / catalog).write_text(text)
    return page


def parse_existing(page):
    """Keep sections of an earlier run, so a filtered run does not drop them."""
    text = page.read_text()
    sections = {}
    for block in re.split(r"^## ", text, flags=re.MULTILINE)[1:]:
        key = block.split("`", 2)[1]
        sections[key] = ("raw", block)
    return sections


def write_index():
    pages = sorted((ROOT / PLANS).rglob("*.md"))
    pages = [p for p in pages if p.name != "README.md"]
    lines = [
        "# Ledger plans (issue #2366)",
        "",
        "Generated by `docs/audits/ledger-census.py plan`. Do not edit.",
        "One page for each source file. See [the Ledger profiles](../README.md).",
        "",
    ]
    for page in pages:
        rel = page.relative_to(ROOT / PLANS).as_posix()
        lines.append(f"- [`{rel[:-3]}`]({rel})")
    (ROOT / PLANS / "README.md").write_text("\n".join(lines) + "\n")


def run_attribute(raw_path):
    catalogs = load_catalogs()
    resolver = Resolver(ROOT, catalogs)
    skipped = []
    with open(raw_path, encoding="utf-8") as handle:
        new = attribute(handle, resolver, skipped)
    if skipped:
        print(f"ledger-census: skipped {len(skipped)} torn lines")
    target = ROOT / STATEMENTS
    old = json.loads(target.read_text()) if target.is_file() else {}
    merged = merge(old, new)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(merged, indent=1, sort_keys=True) + "\n")
    count = sum(len(k) for k in merged.values())
    print(f"ledger-census: {count} profiles have statements in {STATEMENTS}")
    return 0


PLAN_JSON = """[{"Plan": {"Node Type": "Limit", "Plans": [
  {"Node Type": "Index Scan", "Relation Name": "harvest_task_queue", "Index Name": "idx_tq_poll",
   "Index Cond": "(id = $1)"},
  {"Node Type": "Seq Scan", "Relation Name": "harvest_workers"},
  {"Node Type": "Index Only Scan", "Relation Name": "harvest_timers", "Index Name": "harvest_timers_pkey"},
  {"Node Type": "Bitmap Heap Scan", "Relation Name": "harvest_events",
   "Plans": [{"Node Type": "Bitmap Index Scan", "Index Name": "idx_ev"}]}]}}]"""


def self_test():
    assert access_paths(PLAN_JSON) == [
        ("harvest_events", "Bitmap Heap Scan on `idx_ev`"),
        ("harvest_task_queue", "Index Scan on `idx_tq_poll`"),
        ("harvest_timers", f"{FULL_INDEX} `harvest_timers_pkey`"),
        ("harvest_workers", NO_INDEX),
    ], access_paths(PLAN_JSON)
    nested = json.dumps([{"Plan": {"Node Type": "Bitmap Heap Scan", "Relation Name": "q", "Plans": [
        {"Node Type": "Index Scan", "Relation Name": "w", "Index Name": "w_pkey", "Index Cond": "(id = $1)"},
        {"Node Type": "Bitmap Index Scan", "Index Name": "q_idx"}]}}])
    assert ("q", "Bitmap Heap Scan on `q_idx`") in access_paths(nested), access_paths(nested)
    full = json.dumps([{"Plan": {"Node Type": "Index Scan", "Relation Name": "q", "Index Name": "q_running"}}])
    assert access_paths(full, frozenset({"q_running"})) == [("q", f"{PARTIAL_INDEX} `q_running`")]
    assert not unindexed(f"{PARTIAL_INDEX} `q_running`")
    assert unindexed(NO_INDEX) and not unindexed(f"{FULL_INDEX} `x`") and not unindexed("Index Scan on `x`")

    assert PLANNABLE.match("  WITH c AS (SELECT 1) SELECT * FROM c")
    assert PLANNABLE.match("(SELECT 1) UNION (SELECT 2)")
    assert not PLANNABLE.match("SET LOCAL lock_timeout = '5s'")
    assert not PLANNABLE.match("BEGIN")
    assert PLANNABLE.match("/* claim: priority ageing */ WITH c AS (SELECT 1) SELECT * FROM c")
    assert PLANNABLE.match("-- note\n  SELECT 1")
    assert not PLANNABLE.match("/* x */ SET LOCAL a = 1")
    assert normalize("SELECT a -- why\n      FROM t\n\n  WHERE  b") == "SELECT a -- why\nFROM t\nWHERE b"

    text = '["a"]\ndriver = "per_task"\nmeasured = "unmeasured"\n\n["b"]\nmeasured = "unmeasured"\n'
    once = set_plan_field(text, "a", "docs/ledger/plans/x.md")
    assert once == ('["a"]\ndriver = "per_task"\nmeasured = "unmeasured"\nplan = "docs/ledger/plans/x.md"\n'
                    '\n["b"]\nmeasured = "unmeasured"\n'), once
    assert set_plan_field(once, "a", "docs/ledger/plans/y.md").count("plan = ") == 1
    assert set_plan_field(text, "b", "p.md").endswith('measured = "unmeasured"\nplan = "p.md"\n')

    root = Path("/fixture")
    source = "pub async fn claim(conn: &mut C) {\n    let x = 1;\n    helper(conn).await;\n}\n\nfn helper(c: &mut C) {\n    c.go();\n}\n"
    resolver = Resolver(root, {"docs/ledger/c/queue.toml": {"claim": {}}})
    resolver.cache["c/src/queue.rs"] = (line_starts(source), lp.frames_of(lp.lex(source)[0])[0])
    assert resolver.key("c/src/queue.rs:2") == ("docs/ledger/c/queue.toml", "claim")
    assert resolver.key("c/src/queue.rs:7") is None
    lines = [json.dumps({"sql": "SELECT\n    1", "frames": ["c/src/queue.rs:7", "c/src/queue.rs:3"]})]
    assert attribute(lines, resolver) == {"docs/ledger/c/queue.toml": {"claim": {"SELECT\n1": {"types": {}, "literals": [{}]}}}}
    torn = []
    assert attribute(lines + ['{"sql": "SEL{"sql"'], resolver, torn) == attribute(lines, resolver)
    assert len(torn) == 1

    assert source_of("docs/ledger/autumn-harvest/dst/mod.toml") == "autumn-harvest/src/dst/mod.rs"
    e = {"types": {}, "literals": [{}]}
    t = {"types": {"2": "uuid"}, "literals": [{"1": "true"}]}
    assert merge({"c": {"k": {"a": e}}}, {"c": {"k": {"b": e, "a": t}, "j": {"c": e}}}) == {
        "c": {"j": {"c": e}, "k": {"a": {"types": {"2": "uuid"}, "literals": [{"1": "true"}, {}]}, "b": e}}}
    assert shape([{"::2": "uuid", "1": "true"}, {"::2": "uuid"}]) == {"types": {"2": "uuid"}, "literals": [{"1": "true"}, {}]}

    binds = ('[0b1c-uuid, false, "RUNNING", Some(true), ["A", "B"], Object {"a": String("x, y")}, '
             '"it\'s", "test-' + "0a" * 16 + '"]')
    found = {k: v for k, v in literals(binds).items() if not k.startswith("::")}
    assert found == {"2": "false", "3": "'RUNNING'", "4": "true", "5": "ARRAY['A', 'B']"}, found
    typed = literals('[1f0c3a2e-0000-4000-8000-000000000001, 7, [2026-10-28T17:35:07.25Z], None, Object {}]')
    assert typed == {"::1": "uuid", "::2": "bigint", "::3": "timestamptz[]", "::5": "jsonb"}, typed
    assert substitute("SELECT ($2)[1] WHERE a = $1", {"1": "true", "::2": "timestamptz[]"}, cast=True) == \
        "SELECT ($1::timestamptz[])[1] WHERE a = true"
    assert literals("[]") == {} and literals("") == {}
    assert literals('["' + "x" * 80 + '"]') == {"::1": "text"}
    assert split_top('a, [b, c], "d, e"') == ["a", "[b, c]", '"d, e"']
    assert substitute("SELECT 1 WHERE a = $1 AND b = $2 AND c = $3 AND d = $10",
                      {"2": "false"}) == "SELECT 1 WHERE a = $1 AND b = false AND c = $2 AND d = $3"

    tables = {"harvest_tasks", "harvest_executions"}
    assert sqlite_access("SCAN harvest_tasks", tables) == ("harvest_tasks", NO_INDEX)
    assert sqlite_access("SCAN t", tables) is None
    assert sqlite_access("SCAN harvest_tasks AS t USING COVERING INDEX idx_seq", tables) == \
        ("harvest_tasks", f"{FULL_INDEX} `idx_seq`")
    assert sqlite_access("SCAN harvest_tasks USING INDEX idx_live", tables, frozenset({"idx_live"})) == \
        ("harvest_tasks", f"{PARTIAL_INDEX} `idx_live`")
    assert sqlite_access("SEARCH harvest_executions USING INDEX idx_key (exec_id=?)", tables) == \
        ("harvest_executions", "Search on `idx_key`")
    assert sqlite_access("SEARCH harvest_tasks USING INTEGER PRIMARY KEY (rowid=?)", tables) == \
        ("harvest_tasks", "Search on the primary key")
    assert sqlite_access("USE TEMP B-TREE FOR ORDER BY", tables) is None
    assert ddl_kind("-- why\nCREATE TABLE IF NOT EXISTS t (a)") < ddl_kind("CREATE INDEX IF NOT EXISTS i ON t (a)")
    assert ddl_kind("ALTER TABLE t ADD COLUMN b") == SQLITE_DDL.index("ALTER TABLE")
    assert ddl_kind("SELECT 1") is None
    assert sqlite_params("SELECT 1 WHERE a = ? AND b = ?") == [None, None]
    assert sqlite_params("SELECT 1 WHERE a = ?3") == [None, None, None]
    assert sqlite_params("SELECT 1 WHERE a = ?1 AND b = ?2") == [None, None]
    assert sqlite_params("SELECT 1 WHERE a = :id") == {"id": None}
    schema = {"c": {"k": {
        "CREATE TABLE IF NOT EXISTS q (id INTEGER PRIMARY KEY, s TEXT)": {"types": {}, "literals": [{}], "backend": "sqlite"},
        "CREATE INDEX IF NOT EXISTS q_s ON q (s) WHERE s IS NOT NULL": {"types": {}, "literals": [{}], "backend": "sqlite"},
        "CREATE TABLE pg_only (a int)": {"types": {}, "literals": [{}]}}}}
    db, found_tables, found_partial = sqlite_schema(schema)
    assert found_tables == {"q"} and found_partial == {"q_s"}, (found_tables, found_partial)
    assert explain_sqlite(db, "SELECT id FROM q WHERE s = ?", found_tables, found_partial) == \
        ([("q", "Search on `q_s`")], None)
    assert explain_sqlite(db, "SELECT id FROM q WHERE s LIKE ?", found_tables) == ([("q", NO_INDEX)], None)
    assert explain_sqlite(db, "SELECT id FROM q WHERE s = ? AND s <> '?'", found_tables, found_partial)[1] is None
    assert explain_sqlite(db, "SELECT nope FROM q", found_tables)[1].startswith("no such column")
    assert tagged(e, True) == {**e, "backend": "sqlite"} and tagged(e, False) == e
    sq = {"types": {}, "literals": [{}], "backend": "sqlite"}
    assert merge({"c": {"k": {"a": sq}}}, {"c": {"k": {"a": e}}}) == {"c": {"k": {"a": sq}}}
    page = render_page("x/src/y.rs", {"k": [("SELECT 1 FROM q", [("q", NO_INDEX)], None, True)]})
    assert SQLITE_SECTION in page and "EXPLAIN QUERY PLAN" in page and "GENERIC_PLAN" not in page
    assert "Each Postgres" not in render_page("x/src/y.rs", {"k": [("SELECT 1", [], None)]})

    print("ledger-census self-test: OK")
    return 0


def main(argv):
    if "--self-test" in argv:
        return self_test()
    if len(argv) >= 2 and argv[0] == "attribute":
        return run_attribute(argv[1])
    if argv and argv[0] == "plan-sqlite":
        driver = argv[argv.index("--driver") + 1] if "--driver" in argv else None
        return run_plan_sqlite(driver)
    if len(argv) >= 2 and argv[0] == "plan":
        driver = argv[argv.index("--driver") + 1] if "--driver" in argv else None
        return run_plan(argv[1], driver)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
