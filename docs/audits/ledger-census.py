#!/usr/bin/env python3
"""Measure the access path of each Ledger-profiled query (issue #2366).

`docs/audits/ledger-profiles.py` keeps a profile for every function that
sends SQL. Most profiles say `measured = "unmeasured"`. This script measures
them in bulk, in two steps.

`attribute` reads the raw census that `tests/integration/ledger_census.rs`
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

A plan is not a buffer count. `measured` stays for a Ledger page with real
buffers. A `plan` page shows which index each statement uses, and which
statements scan a table with no usable index.

Usage:
    HARVEST_LEDGER_CENSUS=/var/tmp/census.jsonl \\
        cargo test -p autumn-harvest --test integration -- --test-threads=6
    python3 docs/audits/ledger-census.py attribute /var/tmp/census.jsonl
    python3 docs/audits/ledger-census.py plan postgres://postgres@127.0.0.1/ledger_plan [--driver per_task]
    python3 docs/audits/ledger-census.py --self-test

Leave `HARVEST_TEST_DATABASE_URL` unset, so that each suite boots a migrated
container. The plan database needs every migration in `autumn-harvest/migrations`.
"""
import importlib.util
import json
import re
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

PLANNABLE = re.compile(r"^\s*(?:\(\s*)*(?:SELECT|INSERT|UPDATE|DELETE|WITH|VALUES|MERGE|TABLE)\b", re.IGNORECASE)
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
                sql = re.sub(r"\s+", " ", record["sql"]).strip()
                found[hit[0]][hit[1]][sql].add(json.dumps(lits, sort_keys=True))
                break
    return {
        c: {k: {sql: [json.loads(m) for m in sorted(maps)][:MAX_LITERAL_SETS] for sql, maps in sorted(sqls.items())}
            for k, sqls in sorted(keys.items())}
        for c, keys in sorted(found.items())
    }


def merge(old, new):
    out = {c: {k: dict(v) for k, v in keys.items()} for c, keys in old.items()}
    for catalog, keys in new.items():
        for key, sqls in keys.items():
            slot = out.setdefault(catalog, {}).setdefault(key, {})
            for sql, maps in sqls.items():
                known = {json.dumps(m, sort_keys=True) for m in slot.get(sql, [])}
                known |= {json.dumps(m, sort_keys=True) for m in maps}
                slot[sql] = [json.loads(m) for m in sorted(known)][:MAX_LITERAL_SETS]
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


def literals(binds):
    """Map parameter numbers to the literals that a custom plan would see.

    Only bool values and state words are kept. They decide whether a partial index
    such as `WHERE NOT fired` applies. A generic plan cannot prove that.
    """
    binds = binds.strip()
    if not (binds.startswith("[") and binds.endswith("]")):
        return {}
    out = {}
    for number, debug in enumerate(split_top(binds[1:-1]), 1):
        value = literal(debug)
        if value is not None and len(value) <= MAX_LITERAL:
            out[str(number)] = value
    return out


def substitute(sql, lits):
    """Put each literal in place of its parameter, then renumber the rest."""
    numbers = {}

    def swap(match):
        number = match.group(1)
        if number in lits:
            return lits[number]
        numbers.setdefault(number, len(numbers) + 1)
        return f"${numbers[number]}"

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


def plan_statement(url, sql, maps, partial):
    """Plan one statement once for each set of bind literals that it saw.

    The union of the access paths is the result. A statement with no literal
    gets one generic plan. A literal set that does not plan falls back to it.
    """
    paths, errors = set(), []
    for lits in [m for m in maps if m] or [{}]:
        found, error = explain(url, substitute(sql, lits), partial)
        if error and lits:
            found, error = explain(url, sql, partial)
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


def render_page(source_path, results):
    """Render one generated page; `results` maps key -> [(sql, paths, error)]."""
    out = [
        f"# Ledger plans: `{source_path}`",
        "",
        "Generated by `docs/audits/ledger-census.py plan` (issue #2366). Do not edit.",
        "Each statement was planned with `EXPLAIN (GENERIC_PLAN)` and",
        "`enable_seqscan = off`. Each bool or string bind value that the census saw",
        "is put in place of its parameter, as in a custom plan, so a partial index",
        "on `NOT fired` applies.",
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
        flags = sorted({t for _, paths, _ in rows for t, a in (paths or []) if unindexed(a)})
        out.append(f"## `{key}`")
        out.append("")
        out.append(f"Statements: {len(rows)}. Tables with no usable index: " + (", ".join(f"`{t}`" for t in flags) or "none") + ".")
        out.append("")
        out.append("| # | Statement | Access |")
        out.append("|---|---|---|")
        for number, (sql, paths, error) in enumerate(rows, 1):
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
            sqls = {s: m for s, m in seen.get(key, {}).items() if PLANNABLE.match(s)}
            if not sqls:
                missing.append(f"{catalog} {key}")
                continue
            results[key] = [(sql, *plan_statement(url, sql, maps, partial)) for sql, maps in sqls.items()]
        if not results:
            continue
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
        pages.append(page)
    write_index()
    print(f"ledger-census: planned {len(pages)} pages; {len(missing)} profiles not reached by the suites")
    for item in missing:
        print(f"  not reached: {item}")
    return 0


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
    assert attribute(lines, resolver) == {"docs/ledger/c/queue.toml": {"claim": {"SELECT 1": [{}]}}}
    torn = []
    assert attribute(lines + ['{"sql": "SEL{"sql"'], resolver, torn) == attribute(lines, resolver)
    assert len(torn) == 1

    assert source_of("docs/ledger/autumn-harvest/dst/mod.toml") == "autumn-harvest/src/dst/mod.rs"
    assert merge({"c": {"k": {"a": [{}]}}}, {"c": {"k": {"b": [{}], "a": [{"1": "true"}]}, "j": {"c": [{}]}}}) == {
        "c": {"j": {"c": [{}]}, "k": {"a": [{"1": "true"}, {}], "b": [{}]}}}

    binds = ('[0b1c-uuid, false, "RUNNING", Some(true), ["A", "B"], Object {"a": String("x, y")}, '
             '"it\'s", "test-' + "0a" * 16 + '"]')
    assert literals(binds) == {"2": "false", "3": "'RUNNING'", "4": "true", "5": "ARRAY['A', 'B']"}, literals(binds)
    assert literals("[]") == {} and literals("") == {}
    assert literals('["' + "x" * 80 + '"]') == {}
    assert split_top('a, [b, c], "d, e"') == ["a", "[b, c]", '"d, e"']
    assert substitute("SELECT 1 WHERE a = $1 AND b = $2 AND c = $3 AND d = $10",
                      {"2": "false"}) == "SELECT 1 WHERE a = $1 AND b = false AND c = $2 AND d = $3"

    print("ledger-census self-test: OK")
    return 0


def main(argv):
    if "--self-test" in argv:
        return self_test()
    if len(argv) >= 2 and argv[0] == "attribute":
        return run_attribute(argv[1])
    if len(argv) >= 2 and argv[0] == "plan":
        driver = argv[argv.index("--driver") + 1] if "--driver" in argv else None
        return run_plan(argv[1], driver)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
