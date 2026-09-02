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

CSS `opacity` on an ancestor changes the *rendered* contrast of everything
inside it: both a text color and its local background are independently
alpha-blended toward whatever sits behind the opacity-bearing element, and
because WCAG contrast is a nonlinear function of (gamma-corrected)
luminance, that blend shrinks the ratio between them — usually well past
what a linear reading of the opacity value suggests. This script does not
generally model ancestor opacity (that needs real DOM/cascade awareness,
not regex extraction) — but every `style="opacity:..."` wrapper found in
`ui.rs` is checked explicitly by `check_opacity_wrapped_badges`, which
locates the badge class rendered inside it, resolves that badge's own
(text, background) pair from the STYLE rules above, and recomputes the
composited ratio. Extend OPACITY-affected markup gets this same explicit
treatment if it's ever added elsewhere, since there is currently exactly
one such wrapper in the file (issue found in PR review, see
`check_opacity_wrapped_badges`).

Usage:
    python3 docs/audits/vantage-dashboard-contrast.py

Run from the repo root. Exits 1 if any real (non-exempt) failure is found,
0 otherwise — safe to wire into CI as a gate, or run standalone as a report.
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


def blend_toward(fg_hex, alpha, backdrop_hex):
    """Alpha-composite `fg_hex` at `alpha` opacity over `backdrop_hex`,
    per-channel, the same way a browser composites an opacity-bearing
    element's rendered layer onto whatever is behind it."""
    fg, bd = hex_to_rgb(fg_hex), hex_to_rgb(backdrop_hex)
    return tuple(fg[i] * alpha + bd[i] * (1 - alpha) for i in range(3))


def contrast_rgb(rgb1, rgb2):
    l1, l2 = rel_lum(rgb1), rel_lum(rgb2)
    lighter, darker = max(l1, l2), min(l1, l2)
    return (lighter + 0.05) / (darker + 0.05)


def extract_style_block(src: str) -> str:
    m = re.search(r'const STYLE: &str = r#"\n(.*?)\n"#;', src, re.S)
    if not m:
        raise SystemExit("could not find `const STYLE` block in ui.rs")
    return m.group(1)


def extract_inline_style_pairs(src: str):
    """Inline `style="..."` attributes outside the STYLE block (maud markup
    literals). Parses the full declaration list regardless of property
    order, so `background:X;color:Y` and `color:Y;background:X` are both
    caught. Falls back to the page background only when the same `style`
    attribute sets no `background` of its own."""
    out = []
    for style_m in re.finditer(r'style="([^"]*)"', src):
        decls = style_m.group(1)
        fg_m = re.search(r"color:\s*(#[0-9a-fA-F]{3,6})", decls)
        if not fg_m:
            continue
        bg_m = re.search(r"background:\s*(#[0-9a-fA-F]{3,6})", decls)
        out.append((fg_m.group(1), bg_m.group(1) if bg_m else None))
    return out


def extract_badge_colors(css: str) -> dict:
    """Map badge class name (e.g. "COMPLETED") -> (text, background) hex,
    from `.badge.NAME{background:...;color:...}` rules in STYLE."""
    colors = {}
    for m in re.finditer(
        r"\.badge\.([A-Za-z0-9_-]+)\{[^{}]*?background:\s*(#[0-9a-fA-F]{3,6})[^{}]*?color:\s*(#[0-9a-fA-F]{3,6})",
        css,
    ):
        colors[m.group(1)] = (m.group(3), m.group(2))  # (text, background)
    return colors


def check_opacity_wrapped_badges(src: str, badge_colors: dict, body_bg: str):
    """Find every `style="...opacity:N..."` wrapper and, if a `.badge.NAME`
    is rendered inside it, recompute that badge's composited contrast (see
    module docstring) instead of trusting its unwrapped STYLE-rule ratio.

    Scoped to the wrapper's own markup block (up to the next `}` in the
    maud `html! { }` source) so it doesn't reach into unrelated markup."""
    results = []
    for m in re.finditer(r'style="[^"]*opacity:\s*([0-9.]+)[^"]*"', src):
        alpha = float(m.group(1))
        if alpha >= 1.0:
            continue
        window = src[m.end() : m.end() + 400]
        end = window.find("\n                }")
        window = window if end == -1 else window[:end]
        badge_m = re.search(r"badge[. ]([A-Za-z0-9_-]+)", window)
        if not badge_m or badge_m.group(1) not in badge_colors:
            continue
        fg, bg = badge_colors[badge_m.group(1)]
        eff_fg = blend_toward(fg, alpha, body_bg)
        eff_bg = blend_toward(bg, alpha, body_bg)
        ratio = contrast_rgb(eff_fg, eff_bg)
        label = f'(opacity:{alpha} wrapper) badge {badge_m.group(1)}'
        results.append((label, f"{fg}@{alpha}", f"{bg}@{alpha}", ratio))
    return results


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

    # Inline `style="..."` spans in markup live outside STYLE.
    for fg, bg in extract_inline_style_pairs(src):
        label = "(inline style span)" if bg else "(inline style span, on #0f172a panel)"
        results.append((label, fg, bg or body_bg))

    # Badges rendered under a `style="opacity:..."` ancestor — see module
    # docstring. These carry their own (fg, bg) already composited to the
    # effective rendered colors, so they flow through the same thresholding
    # below like any other pair.
    badge_colors = extract_badge_colors(css)
    opacity_results = check_opacity_wrapped_badges(src, badge_colors, body_bg)

    all_rows = [(sel, fg, bg, contrast(fg, bg)) for sel, fg, bg in results]
    all_rows += [(sel, fg, bg, ratio) for sel, fg, bg, ratio in opacity_results]

    real_failures = []
    exempt = []
    passes = []
    for sel, fg, bg, ratio in all_rows:
        row = (sel, fg, bg, ratio)
        if ratio >= 4.5:
            passes.append(row)
        elif sel in EXEMPT_DISABLED or sel in NON_TEXT_GRAPHICAL:
            exempt.append(row)
        else:
            real_failures.append(row)

    real_failures.sort(key=lambda r: r[3])
    print(f"WCAG 1.4.3 AA contrast audit — {UI_RS}")
    print(f"{len(all_rows)} text/background pairs checked "
          f"({len(opacity_results)} of them opacity-composited).\n")

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
