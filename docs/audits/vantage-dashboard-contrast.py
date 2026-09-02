#!/usr/bin/env python3
"""WCAG 1.4.3 contrast audit for the Vantage dashboard's shared stylesheet
(`STYLE` in autumn-harvest-plugin/src/ui.rs).

Deterministic, reproducible on any checkout — no server, browser, or network
access required. Extracts every CSS rule from the `STYLE` constant that
declares a text color (`color:` or SVG `fill:` on a `font:`-bearing rule)
together with a `background:`, or falls back to the page background
(`body{background:#0f172a}`) for rules that only set `color`. Computes the
WCAG relative-luminance contrast ratio for each pair and flags anything
below the AA thresholds (4.5:1 normal text, 3:1 large text/UI components).

Usage:
    python3 docs/audits/vantage-dashboard-contrast.py

Run from the repo root. Exits 0 always (report tool, not a CI gate) — treat
"FAILS" lines in the output as the finding.
"""
import re
import sys
from pathlib import Path

UI_RS = Path(__file__).resolve().parents[2] / "autumn-harvest-plugin" / "src" / "ui.rs"

# Rules whose text is only ever shown in a disabled/inactive UI-component
# state. WCAG SC 1.4.3 explicitly exempts "text ... that is part of an
# inactive user interface component" — recorded here so the report doesn't
# conflate an exempt disabled-state color with a real violation.
EXEMPT_DISABLED = {".bulk-actions button:disabled", ".pagination span.disabled"}

# Non-text graphical objects (SVG strokes on lines/edges, not glyphs).
# WCAG 1.4.11 (non-text contrast, 3:1) applies here, not 1.4.3 (4.5:1).
NON_TEXT_GRAPHICAL = {".dag-edge"}


def hex_to_rgb(h):
    h = h.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    return tuple(int(h[i : i + 2], 16) for i in (0, 2, 4))


def srgb_to_lin(c):
    c = c / 255.0
    return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4


def rel_lum(rgb):
    r, g, b = rgb
    return 0.2126 * srgb_to_lin(r) + 0.7152 * srgb_to_lin(g) + 0.0722 * srgb_to_lin(b)


def contrast(hex1, hex2):
    l1, l2 = rel_lum(hex_to_rgb(hex1)), rel_lum(hex_to_rgb(hex2))
    lighter, darker = max(l1, l2), min(l1, l2)
    return (lighter + 0.05) / (darker + 0.05)


def extract_style_block(src: str) -> str:
    m = re.search(r'const STYLE: &str = r#"\n(.*?)\n"#;', src, re.S)
    if not m:
        raise SystemExit("could not find `const STYLE` block in ui.rs")
    return m.group(1)


def extract_inline_style_colors(src: str):
    """Standalone `style="color: #rrggbb"` spans outside the STYLE block
    (maud markup literals), each reported against the page background."""
    out = []
    for m in re.finditer(r'style="color:\s*(#[0-9a-fA-F]{3,6})"', src):
        out.append(m.group(1))
    return out


def main():
    src = UI_RS.read_text()
    css = extract_style_block(src)
    body_bg = "#0f172a"

    rules = re.findall(r"([^{}]+)\{([^{}]*)\}", css)
    results = []
    for selector, decls in rules:
        selector = selector.strip()
        bg_m = re.search(r"background:\s*(#[0-9a-fA-F]{3,6})", decls)
        fg_m = re.search(r"(?:color|fill):\s*(#[0-9a-fA-F]{3,6})", decls)
        if not fg_m:
            continue
        # Only treat `fill:` as text color when paired with a `font:` decl
        # (an SVG text label) — a bare `fill:` on a shape isn't glyph color.
        if "fill:" in decls and "color:" not in decls and "font:" not in decls:
            continue
        fg = fg_m.group(1)
        bg = bg_m.group(1) if bg_m else body_bg
        results.append((selector, fg, bg))

    # Inline `style="color: #..."` spans in markup live outside STYLE.
    for fg in extract_inline_style_colors(src):
        results.append(("(inline style span, on #0f172a panel)", fg, body_bg))

    real_failures = []
    exempt = []
    passes = []
    for sel, fg, bg in results:
        ratio = contrast(fg, bg)
        row = (sel, fg, bg, ratio)
        if ratio >= 4.5:
            passes.append(row)
        elif sel in EXEMPT_DISABLED or sel in NON_TEXT_GRAPHICAL:
            exempt.append(row)
        else:
            real_failures.append(row)

    real_failures.sort(key=lambda r: r[3])
    print(f"WCAG 1.4.3 AA contrast audit — {UI_RS}")
    print(f"{len(results)} text/background pairs checked.\n")

    if real_failures:
        print(f"FAILING (below 4.5:1 for normal text) — {len(real_failures)}:")
        for sel, fg, bg, ratio in real_failures:
            print(f"  {ratio:5.2f}:1  {sel:40s} fg={fg} bg={bg}")
    else:
        print("No failing normal-text pairs.")

    if exempt:
        print(f"\nExempt (inactive UI component text, SC 1.4.3 carve-out; or non-text graphical stroke, SC 1.4.11) — {len(exempt)}:")
        for sel, fg, bg, ratio in exempt:
            print(f"  {ratio:5.2f}:1  {sel:40s} fg={fg} bg={bg}")

    print(f"\nPassing — {len(passes)} pairs at or above 4.5:1.")
    return 1 if real_failures else 0


if __name__ == "__main__":
    sys.exit(main())
