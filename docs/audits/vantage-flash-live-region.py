#!/usr/bin/env python3
"""Full-page-load status-message announcement audit for the Vantage
dashboard's flash messages (`autumn-harvest-plugin/src/ui.rs`).

Deterministic, reproducible on any checkout — no server, browser, or network
access required. Every mutating operator action in Vantage (cancel, pause,
resume, terminate, signal, reset, trigger-update, DAG retry, schedule
pause/resume/delete/run-now, dead-letter replay/discard, build-routing
retire/revoke, and more) redirects back to a page that renders its outcome
as a `div.flash` — this is the *only* place the operator learns whether the
action they just took succeeded, failed, or was a no-op. There is no
`<script>`-driven toast and no other notification path.

Two things have to both be true for that message to reach a screen-reader
user, and this audit checks both:

1. **A live-region role** (`role="status"` or `role="alert"`, matched only
   as a real attribute — not a substring of `data-role="status"` — and
   `aria-live` only counts when set to an active value, `polite` or
   `assertive`; `aria-live="off"` is explicitly not live).
2. **Programmatic focus on load** (`tabindex="-1"` *and* bare `autofocus`).
   Vantage is a traditional multi-page app: every flash is a full HTTP
   redirect to a fresh page load, not a DOM mutation on a page the
   assistive-tech tree has already registered. A live-region role by
   itself does not reliably announce content that is already present when
   the accessibility tree is first built — ARIA live regions fire on
   *changes after* registration, not on initial content (WAI-ARIA
   Authoring Practices Guide, "Live Region Perceivable Author Practices").
   The no-JS-required fix for a full page load is to move focus to the
   message: `tabindex="-1"` makes a non-interactive `<div>` programmatically
   focusable without adding it to the tab order, and `autofocus` makes the
   browser focus it the moment the page finishes loading — at which point
   assistive tech announces the focused element's content. `role="status"`
   is kept alongside it because it still gives the message the correct
   semantic (and would matter if this page ever grows client-side updates
   to the same node), but it is not, on its own, sufficient here.

Usage:
    python3 docs/audits/vantage-flash-live-region.py

Run from the repo root. Exits 1 if any render site is missing either half
of the mechanism, 0 otherwise — safe to wire into CI as a gate, or run
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

# `(?<![\w-])` requires the match not be preceded by a word character or a
# hyphen, so `role="status"` matches as a real attribute but the same text
# inside `data-role="status"` does not (the `e` of "role" would otherwise
# still start a substring match right after the hyphen). `aria-live` only
# counts with an active value — `aria-live="off"` is explicitly not live.
ROLE = re.compile(r'(?<![\w-])role="(?:status|alert)"|(?<![\w-])aria-live="(?:polite|assertive)"')
FOCUS_ON_LOAD = re.compile(r'(?<![\w-])tabindex="-1"')
AUTOFOCUS = re.compile(r'(?<![\w-])autofocus\b')


def line_of(src: str, index: int) -> int:
    return src.count("\n", 0, index) + 1


def main():
    src = UI_RS.read_text()

    sites = []
    for m in FLASH_DIV.finditer(src):
        attrs = m.group(1)
        has_role = bool(ROLE.search(attrs))
        has_focus = bool(FOCUS_ON_LOAD.search(attrs)) and bool(AUTOFOCUS.search(attrs))
        sites.append((line_of(src, m.start()), has_role, has_focus))

    sites.sort()

    print(f"Full-page-load status-message announcement audit — {UI_RS}")
    print(f"{len(sites)} `div.flash` render sites checked.\n")

    missing = [(line, role, focus) for line, role, focus in sites if not (role and focus)]
    ok_count = len(sites) - len(missing)

    if missing:
        print(f"INCOMPLETE — {len(missing)}:")
        for line, has_role, has_focus in missing:
            gaps = []
            if not has_role:
                gaps.append('no role="status"/"alert" (or active aria-live)')
            if not has_focus:
                gaps.append('no tabindex="-1"+autofocus (won\'t announce on a full page load)')
            print(f"  ui.rs:{line}  " + "; ".join(gaps))
    else:
        print("Every `div.flash` render site has both a live-region role and focus-on-load.")

    print(f"\nCorrect — {ok_count} of {len(sites)} render sites carry the full mechanism.")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
