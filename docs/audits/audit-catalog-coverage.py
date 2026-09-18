#!/usr/bin/env python3
"""Folio corpus harness: audit-catalog coverage scan for docs/audits/README.md.

Deterministic, reproducible on any checkout — no network access required.
`docs/audits/README.md` is itself a docs page: it is the table a contributor
reads to find out which audit scripts exist, what each one checks, and
whether it is wired into CI. `CLAUDE.md` and several audit scripts' own
docstrings point readers at it. Like any other page in the corpus, its table
can drift from the thing it describes — here, the drift is that a script
lands in `docs/audits/` and nobody adds its row.

This is a coverage check in miniature, over a public surface of one: every
`*.py` file in `docs/audits/` (this script included — it must catalog
itself, or a later removal of its own row would go uncaught) must have a
table row in `docs/audits/README.md`, its first cell naming the script as
inline code (`` `script-name.py` ``). A script present on disk but absent
from the table is invisible to a reader who only reads the catalog — the
same failure mode a coverage-matrix miss is anywhere else in the corpus.

Matching is scoped to the table's first column, not a substring search over
the whole file: the README's prose also names scripts inline (the Comment
Hygiene section, this docstring's own README references), and a script
named only in prose — never given a row — must still count as undocumented.

This does not check that a row's *content* (what it checks, its CI status)
stays accurate — that would need the check text to be a single source of
truth. It only checks that a row exists at all, which is the minimum bar
for the catalog to be honest about what it catalogs.

Usage:
    python3 docs/audits/audit-catalog-coverage.py

Exit code is 1 if any audit script is missing from the README table, 0
otherwise — safe to wire into CI as a gate, or run standalone as a report.
"""
import re
import sys
from pathlib import Path

AUDITS_DIR = Path(__file__).resolve().parent
README = AUDITS_DIR / "README.md"

# The table's first column only: a line starting with `| `name.py` |`. This
# deliberately does not match a script name appearing anywhere else in the
# file, so a script named only in prose still counts as undocumented.
TABLE_ROW_SCRIPT_RE = re.compile(r"^\|\s*`([\w.-]+\.py)`\s*\|", re.MULTILINE)


def main() -> int:
    readme_text = README.read_text(encoding="utf-8")

    scripts = sorted(p.name for p in AUDITS_DIR.glob("*.py"))
    documented = set(TABLE_ROW_SCRIPT_RE.findall(readme_text))

    missing = [name for name in scripts if name not in documented]

    print(
        f"Folio audit-catalog coverage — {len(scripts)} audit scripts on disk, "
        f"checked against docs/audits/README.md\n"
    )
    print(f"Undocumented audit scripts (fails CI): {len(missing)}")
    for name in missing:
        print(f"  docs/audits/{name} — no `{name}` reference in docs/audits/README.md")

    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
