#!/usr/bin/env python3
"""Keep every diesel `table!` narrow (issue #2492).

Diesel implements its traits for tuples up to a fixed size. The size comes
from a cargo feature: 32 by default, or `64-column-tables`, or
`128-column-tables`. A wider tier generates impls for every tuple size up to
its limit. That costs compile time and memory in every crate that builds
diesel. Cargo unifies features, so one wide table in this workspace makes
every embedder pay for the wide tier as well.

The script fails in five cases:
- A `table!` has more than 64 columns.
- A `table!` has more than 32 columns and is not in `WIDE_TABLES`.
- A table in `WIDE_TABLES` is wider than its ceiling.
- A table in `WIDE_TABLES` is narrower than its ceiling. Lower the ceiling,
  or remove the entry at 32 columns or fewer. That makes the list a ratchet.
- A manifest enables `128-column-tables`. When `WIDE_TABLES` is empty, a
  manifest that enables `64-column-tables` fails too.

Do not raise a ceiling to add a column. Move a column out of the `table!`
instead. `schema::harvest_workflow_executions_unmapped` shows the pattern.

Usage:
    python3 docs/audits/diesel-table-width.py
    python3 docs/audits/diesel-table-width.py --self-test
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HARD_LIMIT = 64
TARGET = 32
SKIP_DIRS = {"target", ".git", "node_modules"}

# Tables over TARGET columns, keyed by name, with their column count. The
# goal is an empty map, so that the workspace can drop `64-column-tables`.
WIDE_TABLES = {
    "harvest_workflow_executions": 61,
    "harvest_task_queue": 48,
    "harvest_schedules": 38,
}

TABLE_OPEN = re.compile(r"\btable!\s*\{")
# The table name and its primary key, then the opening brace of the body.
TABLE_HEAD = re.compile(r"([\w.]+)\s*(?:\([^)]*\))?\s*\{")
# One column: optional attributes, a name, then `->`.
COLUMN = re.compile(r"^\s*(?:#\[[^\]]*\]\s*)*(?:r#)?\w+\s*->", re.M)
DOC_LINE = re.compile(r"^\s*//[^\n]*$", re.M)
WIDE_FEATURE = re.compile(r'"(64|128)-column-tables"')


def tables_in(source):
    """Yield (name, column_count, line) for each `table!` in `source`."""
    for match in TABLE_OPEN.finditer(source):
        depth = 1
        pos = match.end()
        while depth and pos < len(source):
            if source[pos] == "{":
                depth += 1
            elif source[pos] == "}":
                depth -= 1
            pos += 1
        body = DOC_LINE.sub("", source[match.end() : pos - 1])
        head = TABLE_HEAD.search(body)
        if head is None:
            continue
        columns = len(COLUMN.findall(body[head.end() :]))
        line = source.count("\n", 0, match.start()) + 1
        yield head.group(1).split(".")[-1], columns, line


def walk(root, suffix):
    """Yield every file under `root` whose name ends with `suffix`."""
    for path in sorted(root.rglob("*" + suffix)):
        if not SKIP_DIRS.intersection(path.relative_to(root).parts):
            yield path


def check_tables(found, wide_tables):
    """Return a list of failures for (name, count, where) entries."""
    failures = []
    widest = {}
    for name, count, where in found:
        if count > HARD_LIMIT:
            failures.append(
                f"{where}: `{name}` has {count} columns. The limit is "
                f"{HARD_LIMIT}. Move columns out of the `table!`."
            )
        elif count > TARGET and name not in wide_tables:
            failures.append(
                f"{where}: `{name}` has {count} columns. A new table has "
                f"{TARGET} or fewer. Split it, or move columns out."
            )
        elif name in wide_tables and count > wide_tables[name]:
            failures.append(
                f"{where}: `{name}` has {count} columns, over its ceiling of "
                f"{wide_tables[name]}. Move a column out instead."
            )
        widest[name] = max(widest.get(name, 0), count)
    for name, ceiling in sorted(wide_tables.items()):
        count = widest.get(name)
        if count is None:
            failures.append(f"WIDE_TABLES: `{name}` has no `table!`. Remove it.")
        elif count <= TARGET:
            failures.append(
                f"WIDE_TABLES: `{name}` has {count} columns. Remove its entry."
            )
        elif count < ceiling:
            failures.append(
                f"WIDE_TABLES: `{name}` has {count} columns. Lower its "
                f"ceiling from {ceiling} to {count}."
            )
    return failures


def check_manifest(text, where, wide_tables):
    """Return a list of failures for one Cargo.toml."""
    failures = []
    for match in WIDE_FEATURE.finditer(text):
        tier = match.group(1)
        if tier == "128" or not wide_tables:
            failures.append(
                f"{where}: enables `{tier}-column-tables`. Every table fits "
                f"a narrower tier, so remove the feature."
            )
    return failures


def run(root):
    """Check the tree at `root`. Return the list of failures."""
    found = []
    for path in walk(root, ".rs"):
        source = path.read_text(encoding="utf-8", errors="replace")
        for name, count, line in tables_in(source):
            found.append((name, count, f"{path.relative_to(root)}:{line}"))
    failures = check_tables(found, WIDE_TABLES)
    for path in walk(root, "Cargo.toml"):
        text = path.read_text(encoding="utf-8")
        failures.extend(check_manifest(text, path.relative_to(root), WIDE_TABLES))
    return failures


def table_source(name, columns):
    """Return a `table!` fixture with `columns` columns."""
    body = "".join(f"        /// Doc.\n        c{i} -> Int4,\n" for i in range(columns))
    return f"diesel::table! {{\n    {name} (c0) {{\n{body}    }}\n}}\n"


def self_test():
    """Exercise each failure case on fixtures."""
    def count(source):
        return [(n, c) for n, c, _ in tables_in(source)]

    assert count(table_source("t", 3)) == [("t", 3)]
    attr = 'table! { s.t (id) {\n #[sql_name = "x"]\n id -> Int4,\n r#type -> Text,\n} }'
    assert count(attr) == [("t", 2)], count(attr)

    def fails(name, columns, wide):
        return check_tables([(name, columns, "f.rs:1")], wide)

    assert not fails("t", TARGET, {})
    assert fails("t", TARGET + 1, {})
    assert fails("t", HARD_LIMIT + 1, {"t": HARD_LIMIT + 1})
    assert not fails("t", 40, {"t": 40})
    assert fails("t", 41, {"t": 40})
    assert fails("t", 39, {"t": 40})
    assert fails("t", TARGET, {"t": 40})
    assert check_tables([], {"t": 40})

    feature = 'diesel = { features = ["64-column-tables"] }'
    assert not check_manifest(feature, "Cargo.toml", {"t": 40})
    assert check_manifest(feature, "Cargo.toml", {})
    assert check_manifest(feature.replace("64", "128"), "Cargo.toml", {"t": 40})
    print("diesel-table-width self-test passed")


def main():
    if "--self-test" in sys.argv[1:]:
        self_test()
        return 0
    failures = run(ROOT)
    for failure in failures:
        print(failure)
    if failures:
        print(f"diesel-table-width: {len(failures)} failure(s)")
        return 1
    print("diesel-table-width: every table! fits its ceiling")
    return 0


if __name__ == "__main__":
    sys.exit(main())
