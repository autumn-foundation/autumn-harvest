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
`*.py` file in `docs/audits/` (excluding this script and `__pycache__`) must
be named, as inline code (`` `script-name.py` ``), somewhere in
`docs/audits/README.md`. A script present on disk but absent from the table
is invisible to a reader who only reads the catalog — the same failure mode
a coverage-matrix miss is anywhere else in the corpus.

This does not check that a row's *content* (what it checks, its CI status)
stays accurate — that would need the check text to be a single source of
truth. It only checks that a row exists at all, which is the minimum bar
for the catalog to be honest about what it catalogs.

Usage:
    python3 docs/audits/audit-catalog-coverage.py

Exit code is 1 if any audit script is missing from the README table, 0
otherwise — safe to wire into CI as a gate, or run standalone as a report.
"""
import sys
from pathlib import Path

AUDITS_DIR = Path(__file__).resolve().parent
README = AUDITS_DIR / "README.md"
SELF_NAME = Path(__file__).name


def main() -> int:
    readme_text = README.read_text(encoding="utf-8")

    scripts = sorted(
        p.name
        for p in AUDITS_DIR.glob("*.py")
        if p.name != SELF_NAME
    )

    missing = [name for name in scripts if f"`{name}`" not in readme_text]

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
