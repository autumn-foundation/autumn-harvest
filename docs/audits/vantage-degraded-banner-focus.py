#!/usr/bin/env python3
"""Full-page-load status-message announcement audit for the Vantage
dashboard's degraded-state banners (`autumn-harvest-plugin/src/ui.rs`).

Deterministic, reproducible on any checkout — no server, browser, or network
access required. This is the `div.degraded-banner` counterpart to
`vantage-flash-live-region.py`, which audits `div.flash` for the same
mechanism. The two classes render the same kind of content — a status
message the operator must not miss — through the same full-page-load path,
so they need the same fix, and `vantage-flash-live-region.py`'s own scan is
scoped to `class` containing "flash" and does not see `degraded-banner` at
all.

`div.degraded-banner` is how Vantage tells an operator that what they are
looking at is incomplete or wrong: a rejected form submission ("Backfill not
started"), a paused or exhausted schedule that silently has no fire times to
show, or a cross-shard read that came back partial or unavailable ("this
history and its summary cover only the shards that answered; counts may be
understated" — the source comment at the `runs` page call site names this
explicitly: "a partial cross-shard answer is always visible, never silently
truncated data"). Every one of these is a full HTTP response to a GET or a
redirected POST, not a client-side update to a page already on screen.

Two things have to both be true for that message to reach a screen-reader
user, and this audit checks both:

1. **A live-region role** (`role="status"` or `role="alert"`, matched only
   as a real attribute — not a substring of `data-role="status"` — and
   `aria-live` only counts when set to an active value, `polite` or
   `assertive`; `aria-live="off"` is explicitly not live).
2. **Programmatic focus on load** (`tabindex="-1"` *and* bare `autofocus`).
   A live-region role by itself does not reliably announce content that is
   already present when the accessibility tree is first built — ARIA live
   regions fire on *changes after* registration, not on initial content
   (WAI-ARIA Authoring Practices Guide, "Live Region Perceivable Author
   Practices"). The no-JS-required fix for a full page load is to move
   focus to the message: `tabindex="-1"` makes a non-interactive `<div>`
   programmatically focusable without adding it to the tab order, and
   `autofocus` makes the browser focus it the moment the page finishes
   loading — at which point assistive tech announces the focused element's
   content. `role="status"` is kept alongside it for the correct semantic,
   but it is not, on its own, sufficient here. `div.flash` on this same
   file already carries both halves at every render site; this audit exists
   because `div.degraded-banner` did not.

Usage:
    python3 docs/audits/vantage-degraded-banner-focus.py

Run from the repo root. Exits 1 if any render site is missing either half
of the mechanism, 0 otherwise — safe to wire into CI as a gate, or run
standalone as a report.
"""
import re
import sys
from pathlib import Path

UI_RS = Path(__file__).resolve().parents[2] / "autumn-harvest-plugin" / "src" / "ui.rs"

# Every `div`'s full opening tag, from `div` to the block's `{`. Maud lets
# "degraded-banner" appear as any chained shorthand class
# (`div.notice.degraded-banner`, not just the first) or as one token in a
# space-separated `class="..."` value (`class="degraded-banner notice"`), so
# matching only `div.degraded-banner` or an exact `class="degraded-banner"`
# would miss those. Scanning the whole tag and checking for
# "degraded-banner" as a token, in either form, at any position, catches
# all of them.
DIV_OPEN = re.compile(r"\bdiv\b[^{]*\{")

# Maud's chained-class shorthand (`div.notice.degraded-banner`,
# `."quoted-name"`) can only appear contiguously right after `div`, before
# any space-separated attribute begins -- so anchoring at `^div` and
# consuming only that run means a `.degraded-banner`-shaped substring
# sitting inside some *other* attribute's quoted value (e.g.
# `div title=".degraded-banner"`) is never reached at all, with no need to
# reason about quoting. Maud reads a token stream, so whitespace around the
# `.` (`div .degraded-banner`, `div. degraded-banner`) is as insignificant
# there as it is around `=` elsewhere -- `\s*` tolerates it on both sides.
CHAINED_PREFIX = re.compile(r'^div((?:\s*\.\s*(?:[\w-]+|"[^"]*"))*)')
CHAINED_SEGMENT = re.compile(r'\.\s*([\w-]+)|\.\s*"([^"]*)"')
# `(?<![\w-])` requires a real attribute boundary before "class", so
# `data-class="degraded-banner"` (an unrelated attribute) doesn't match.
# Maud reads a Rust token stream, not raw text, so whitespace around `=`
# (`class = "degraded-banner"`) is as valid as the tight form and
# `cargo fmt` does not normalize it away inside a macro body -- `\s*=\s*`
# here and in ROLE/FOCUS_ON_LOAD tolerates both.
CLASS_ATTR = re.compile(r'(?<![\w-])class\s*=\s*"([^"]*)"')


def chained_class_tokens(tag_text: str) -> list[str]:
    m = CHAINED_PREFIX.match(tag_text)
    if not m:
        return []
    return [seg.group(1) or seg.group(2) for seg in CHAINED_SEGMENT.finditer(m.group(1))]


def is_degraded_banner_div(tag_text: str) -> bool:
    if "degraded-banner" in chained_class_tokens(tag_text):
        return True
    m = CLASS_ATTR.search(tag_text)
    return bool(m) and "degraded-banner" in m.group(1).split()


# `(?<![\w-])` requires the match not be preceded by a word character or a
# hyphen, so `role="status"` matches as a real attribute but the same text
# inside `data-role="status"` does not (the `e` of "role" would otherwise
# still start a substring match right after the hyphen). `aria-live` only
# counts with an active value — `aria-live="off"` is explicitly not live.
ROLE = re.compile(
    r'(?<![\w-])role\s*=\s*"(?:status|alert)"|(?<![\w-])aria-live\s*=\s*"(?:polite|assertive)"'
)
FOCUS_ON_LOAD = re.compile(r'(?<![\w-])tabindex\s*=\s*"-1"')
AUTOFOCUS = re.compile(r'(?<![\w-])autofocus\b')


def line_of(src: str, index: int) -> int:
    return src.count("\n", 0, index) + 1


def main():
    src = UI_RS.read_text()

    sites = []
    for m in DIV_OPEN.finditer(src):
        tag_text = m.group(0)
        if not is_degraded_banner_div(tag_text):
            continue
        has_role = bool(ROLE.search(tag_text))
        has_focus = bool(FOCUS_ON_LOAD.search(tag_text)) and bool(AUTOFOCUS.search(tag_text))
        sites.append((line_of(src, m.start()), has_role, has_focus))

    sites.sort()

    print(f"Full-page-load status-message announcement audit — {UI_RS}")
    print(f"{len(sites)} `div.degraded-banner` render sites checked.\n")

    if not sites:
        print("ERROR: found no `div.degraded-banner` render sites at all — no `div`")
        print("in ui.rs carries a \"degraded-banner\" class this audit recognizes.")
        print("Either the markup was rewritten in a spelling this audit doesn't")
        print("recognize (a silent pass would hide that), or degraded banners were")
        print("removed outright.")
        return 1

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
        print("Every `div.degraded-banner` render site has both a live-region role and focus-on-load.")

    print(f"\nCorrect — {ok_count} of {len(sites)} render sites carry the full mechanism.")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
