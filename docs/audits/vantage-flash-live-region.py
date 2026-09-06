#!/usr/bin/env python3
"""WCAG 4.1.3 (Status Messages) audit for the Vantage dashboard's flash
messages (`autumn-harvest-plugin/src/ui.rs`).

Deterministic, reproducible on any checkout — no server, browser, or network
access required. Every mutating operator action in Vantage (cancel, pause,
resume, terminate, signal, reset, trigger-update, DAG retry, schedule
pause/resume/delete/run-now, dead-letter replay/discard, build-routing
retire/revoke, and more) redirects back to a page that renders its outcome
as a `div.flash` — this is the *only* place the operator learns whether the
action they just took succeeded, failed, or was a no-op. There is no
`<script>`-driven toast and no other notification path.

A `div.flash` with no `role="status"` (or `aria-live`) is announced to
nothing: sighted users see it because it renders inline where they're
already looking, but a screen-reader user gets no notification at all and
has to guess that something changed, then hunt the page for text. One
render site (the schedule run-history page) already carries `role="status"`
correctly — proving the fix is a one-line, no-visual-change addition, not a
design question.

Usage:
    python3 docs/audits/vantage-flash-live-region.py

Run from the repo root. Exits 1 if any render site is missing the live-
region role, 0 otherwise — safe to wire into CI as a gate, or run
standalone as a report.
"""
import re
import sys
from pathlib import Path

UI_RS = Path(__file__).resolve().parents[2] / "autumn-harvest-plugin" / "src" / "ui.rs"

# `div.flash { ... }` or `div class="flash" { ... }` (both maud spellings
# appear in the file), capturing whatever attributes sit between the class
# and the opening `{` of the block so we can check them for a live-region
# role without also matching unrelated `div`s.
FLASH_DIV = re.compile(r'div(?:\.flash|\s+class="flash")([^{]*)\{')

LIVE_REGION = re.compile(r'role="status"|role="alert"|aria-live=')


def line_of(src: str, index: int) -> int:
    return src.count("\n", 0, index) + 1


def main():
    src = UI_RS.read_text()

    sites = []
    for m in FLASH_DIV.finditer(src):
        attrs = m.group(1)
        sites.append((line_of(src, m.start()), bool(LIVE_REGION.search(attrs))))

    sites.sort()

    print(f"WCAG 4.1.3 status-message audit — {UI_RS}")
    print(f"{len(sites)} `div.flash` render sites checked.\n")

    missing = [(line, ok) for line, ok in sites if not ok]
    present = [(line, ok) for line, ok in sites if ok]

    if missing:
        print(f"MISSING live-region role — {len(missing)}:")
        for line, _ in missing:
            print(f"  ui.rs:{line}  div.flash has no role=\"status\" / aria-live")
    else:
        print("No `div.flash` render sites are missing a live-region role.")

    print(f"\nCorrect — {len(present)} of {len(sites)} render sites carry a live-region role.")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
