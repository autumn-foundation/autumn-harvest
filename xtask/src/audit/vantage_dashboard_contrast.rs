//! `cargo xtask audit vantage-dashboard-contrast`: the port of
//! `docs/audits/vantage-dashboard-contrast.py`.
//!
//! This is the WCAG 1.4.3 contrast audit for the shared stylesheet of the
//! Vantage dashboard (`STYLE` in `autumn-harvest-plugin/src/ui.rs`).
//!
//! The audit is deterministic and reproducible on any checkout. It needs no
//! server, no browser and no network access. It extracts each CSS rule from
//! the `STYLE` constant that declares a text color. A text color is `color:`,
//! or an SVG `fill:` on a rule that also has `font:`. The rule pairs it with
//! its own `background:`. A rule that only sets a color falls back to the page
//! background (`body{background:#0f172a}`). The audit computes the WCAG
//! relative-luminance contrast ratio of each pair. It flags each pair below
//! the AA thresholds: 4.5:1 for normal text, 3:1 for large text and UI
//! components.
//!
//! CSS `opacity` on an ancestor changes the *rendered* contrast of all its
//! content. The text color and its local background each blend toward the
//! backdrop of the opacity-bearing element. WCAG contrast is a nonlinear
//! function of gamma-corrected luminance. So that blend shrinks the ratio,
//! usually well past what a linear reading of the opacity value suggests.
//! This audit does not model ancestor opacity in general. That needs real
//! DOM and cascade awareness, not regex extraction. But `opacity_wrapped_badges`
//! checks each `style="opacity:..."` wrapper in `ui.rs` explicitly. It finds
//! the badge class rendered inside the wrapper. It resolves the text and
//! background pair of that badge from the `STYLE` rules. Then it recomputes
//! the composited ratio. Today the file holds exactly one such wrapper.
//! Opacity-affected markup added elsewhere needs the same explicit check.
//!
//! Run `cargo xtask audit vantage-dashboard-contrast` from anywhere in the
//! repository. The command exits 1 if it finds a real (non-exempt) failure,
//! and 0 otherwise. So it is safe as a CI gate, or as a standalone report.

use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;

use super::libpq_keyword_sync::py_read_text;
use super::vantage_flash_live_region::{compile, search_unpreceded, ui_rs};

/// Arguments of `cargo xtask audit vantage-dashboard-contrast`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// The page background, which a rule with no background of its own sits on.
const BODY_BG: &str = "#0f172a";

// Rules whose text shows only in a disabled or inactive UI-component state.
// WCAG SC 1.4.3 explicitly exempts "text ... that is part of an inactive
// user interface component". The list keeps the report from conflating an
// exempt disabled-state color with a real violation.
const EXEMPT_DISABLED: &[&str] = &[".bulk-actions button:disabled", ".pagination span.disabled"];

// Non-text graphical objects: SVG strokes on lines and edges, not glyphs.
// WCAG 1.4.11 (non-text contrast, 3:1) applies here, not 1.4.3 (4.5:1).
const NON_TEXT_GRAPHICAL: &[&str] = &[".dag-edge"];

static STYLE_BLOCK: LazyLock<Regex> =
    LazyLock::new(|| compile(r##"(?s)const STYLE: &str = r#"\n(.*?)\n"#;"##));
static INLINE_STYLE: LazyLock<Regex> = LazyLock::new(|| compile(r#"style="([^"]*)""#));
static BACKGROUND: LazyLock<Regex> =
    LazyLock::new(|| compile(r"background:\s*(#[0-9a-fA-F]{3,6})"));
// The Python pattern is `(?<!-)color:`. The port applies the lookbehind
// through `search_unpreceded`. See the comment in `rule_pairs` for why.
static INLINE_COLOR: LazyLock<Regex> = LazyLock::new(|| compile(r"color:\s*(#[0-9a-fA-F]{3,6})"));
static RULE_COLOR: LazyLock<Regex> =
    LazyLock::new(|| compile(r"(?:color|fill):\s*(#[0-9a-fA-F]{3,6})"));
static RULE: LazyLock<Regex> = LazyLock::new(|| compile(r"([^{}]+)\{([^{}]*)\}"));
static BADGE: LazyLock<Regex> = LazyLock::new(|| {
    compile(
        r"\.badge\.([A-Za-z0-9_-]+)\{[^{}]*?background:\s*(#[0-9a-fA-F]{3,6})[^{}]*?color:\s*(#[0-9a-fA-F]{3,6})",
    )
});
static OPACITY_WRAPPER: LazyLock<Regex> =
    LazyLock::new(|| compile(r#"style="[^"]*opacity:\s*([0-9.]+)[^"]*""#));
static BADGE_REF: LazyLock<Regex> = LazyLock::new(|| compile(r"badge[. ]([A-Za-z0-9_-]+)"));

/// Returns true for the hyphen that the Python lookbehind `(?<!-)` refuses.
const fn is_hyphen(c: char) -> bool {
    c == '-'
}

/// Parses a 3-digit or 6-digit hex color into its RGB channels.
///
/// The Python original slices the digits in pairs. A 4-digit color leaves an
/// empty third pair, so the parse fails there as well. A 5-digit color leaves
/// a 1-digit third pair, which both parse as one hex digit.
fn hex_to_rgb(hex: &str) -> Result<[f64; 3]> {
    let digits = hex.trim_start_matches('#');
    let digits: String = if digits.chars().count() == 3 {
        digits.chars().flat_map(|c| [c, c]).collect()
    } else {
        digits.to_string()
    };
    let chars: Vec<char> = digits.chars().collect();
    let mut rgb = [0.0; 3];
    for (slot, start) in rgb.iter_mut().zip([0, 2, 4]) {
        let pair: String = chars.iter().skip(start).take(2).collect();
        let value =
            u8::from_str_radix(&pair, 16).with_context(|| format!("invalid hex color {hex:?}"))?;
        *slot = f64::from(value);
    }
    Ok(rgb)
}

/// Converts one sRGB channel to linear light.
fn srgb_to_lin(c: f64) -> f64 {
    let c = c / 255.0;
    if c <= 0.039_28 {
        c / 12.92
    } else {
        ((c + 0.055) / 1.055).powf(2.4)
    }
}

/// Returns the WCAG relative luminance of a color.
///
/// The sum keeps the separate multiply and add of the Python original. A
/// fused `mul_add` rounds differently, so the report could drift by a digit.
#[allow(clippy::suboptimal_flops)]
fn rel_lum(rgb: [f64; 3]) -> f64 {
    let [r, g, b] = rgb;
    0.2126 * srgb_to_lin(r) + 0.7152 * srgb_to_lin(g) + 0.0722 * srgb_to_lin(b)
}

/// Returns the WCAG contrast ratio of two colors.
fn contrast_rgb(a: [f64; 3], b: [f64; 3]) -> f64 {
    let (l1, l2) = (rel_lum(a), rel_lum(b));
    let (lighter, darker) = (l1.max(l2), l1.min(l2));
    (lighter + 0.05) / (darker + 0.05)
}

/// Returns the WCAG contrast ratio of two hex colors.
fn contrast(a: &str, b: &str) -> Result<f64> {
    Ok(contrast_rgb(hex_to_rgb(a)?, hex_to_rgb(b)?))
}

/// Composites `fg` at `alpha` opacity over `backdrop`, channel by channel.
///
/// A browser composites the rendered layer of an opacity-bearing element
/// onto its backdrop the same way. The arithmetic is unfused, as in
/// `rel_lum`.
#[allow(clippy::suboptimal_flops)]
fn blend_toward(fg: &str, alpha: f64, backdrop: &str) -> Result<[f64; 3]> {
    let (fg, bd) = (hex_to_rgb(fg)?, hex_to_rgb(backdrop)?);
    Ok([0, 1, 2].map(|i| fg[i] * alpha + bd[i] * (1.0 - alpha)))
}

/// Formats a float the way Python's `str(float)` does.
///
/// Both languages print the shortest digits that round-trip. Python uses the
/// exponent form below 1e-4 and from 1e16 up, with at least two exponent
/// digits. It prints an integral value with a trailing `.0`.
fn py_float(value: f64) -> String {
    let magnitude = value.abs();
    if value.is_finite() && value != 0.0 && !(1e-4..1e16).contains(&magnitude) {
        let text = format!("{value:e}");
        let (mantissa, exponent) = text.split_once('e').unwrap_or((text.as_str(), "0"));
        let (sign, digits) = exponent
            .strip_prefix('-')
            .map_or(("+", exponent), |rest| ("-", rest));
        return format!("{mantissa}e{sign}{digits:0>2}");
    }
    if value.is_finite() && value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        format!("{value}")
    }
}

/// Returns the CSS text of the `STYLE` constant, or `None` when it is absent.
fn extract_style_block(src: &str) -> Option<&str> {
    STYLE_BLOCK
        .captures(src)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str())
}

/// Returns each inline `style="..."` pair as text color and background.
///
/// These are maud markup literals outside the `STYLE` block. The parse reads
/// the full declaration list in any property order. So it catches both
/// `background:X;color:Y` and `color:Y;background:X`. The pair falls back to
/// the page background only when the same attribute sets no `background`.
fn inline_style_pairs(src: &str) -> Vec<(String, Option<String>)> {
    let mut out = Vec::new();
    for style in INLINE_STYLE.captures_iter(src) {
        let decls = style.get(1).map_or("", |m| m.as_str());
        // This has the same `border-color:` hazard as the `STYLE` block parse.
        // See the comment in `rule_pairs`.
        let Some(fg) = search_unpreceded(&INLINE_COLOR, decls, is_hyphen) else {
            continue;
        };
        let bg = BACKGROUND.captures(decls).map(|c| c[1].to_string());
        out.push((fg[1].to_string(), bg));
    }
    out
}

/// Maps each badge class name to its text and background colors.
///
/// The colors come from the `.badge.NAME{background:...;color:...}` rules in
/// `STYLE`, for example the badge `COMPLETED`.
fn badge_colors(css: &str) -> Vec<(String, (String, String))> {
    let mut colors: Vec<(String, (String, String))> = Vec::new();
    for m in BADGE.captures_iter(css) {
        let entry = (m[3].to_string(), m[2].to_string());
        if let Some(slot) = colors.iter_mut().find(|(name, _)| *name == m[1]) {
            slot.1 = entry;
        } else {
            colors.push((m[1].to_string(), entry));
        }
    }
    colors
}

/// One checked pair: its label, its colors and its contrast ratio.
#[derive(Debug, Clone)]
struct Row {
    selector: String,
    fg: String,
    bg: String,
    ratio: f64,
}

/// Recomputes the composited contrast of each badge under an opacity wrapper.
///
/// The check finds each `style="...opacity:N..."` wrapper. If the wrapper
/// renders a `.badge.NAME`, the check composites the colors of that badge
/// (see the module docs). It does not trust the unwrapped `STYLE` ratio.
/// The scan stops at the end of the wrapper's own markup block. That is the
/// next `}` line in the maud `html! { }` source, so the scan does not reach
/// into unrelated markup.
fn opacity_wrapped_badges(
    src: &str,
    colors: &[(String, (String, String))],
    body_bg: &str,
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for m in OPACITY_WRAPPER.captures_iter(src) {
        let alpha: f64 = m[1]
            .parse()
            .with_context(|| format!("invalid opacity value {:?}", &m[1]))?;
        if alpha >= 1.0 {
            continue;
        }
        let tail = &src[m.get(0).map_or(src.len(), |w| w.end())..];
        let window: String = tail.chars().take(400).collect();
        let window = window
            .find("\n                }")
            .map_or(window.as_str(), |end| &window[..end]);
        let Some(badge) = BADGE_REF.captures(window) else {
            continue;
        };
        let Some((_, (fg, bg))) = colors.iter().find(|(name, _)| *name == badge[1]) else {
            continue;
        };
        let text_rgb = blend_toward(fg, alpha, body_bg)?;
        let back_rgb = blend_toward(bg, alpha, body_bg)?;
        let alpha_text = py_float(alpha);
        rows.push(Row {
            selector: format!("(opacity:{alpha_text} wrapper) badge {}", &badge[1]),
            fg: format!("{fg}@{alpha_text}"),
            bg: format!("{bg}@{alpha_text}"),
            ratio: contrast_rgb(text_rgb, back_rgb),
        });
    }
    Ok(rows)
}

/// Returns the text color and background of each `STYLE` rule with a color.
fn rule_pairs(css: &str) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for rule in RULE.captures_iter(css) {
        let selector = rule[1].trim().to_string();
        let decls = &rule[2];
        let bg = BACKGROUND.captures(decls).map(|c| c[1].to_string());
        // The Python `(?<!-)` lookbehind keeps this from matching the
        // "color:" tail of `border-color:`, or of any other `*-color:`
        // property. Without it, a rule that sets `border-color` before its
        // real `color:` would read its border color as the text color. That
        // happens because the search returns the leftmost match.
        let Some(fg) = search_unpreceded(&RULE_COLOR, decls, is_hyphen) else {
            continue;
        };
        // A `fill:` counts as a text color only with a `font:` declaration,
        // as on an SVG text label. A bare `fill:` on a shape is no glyph color.
        if decls.contains("fill:") && !decls.contains("color:") && !decls.contains("font:") {
            continue;
        }
        out.push((
            selector,
            fg[1].to_string(),
            bg.unwrap_or_else(|| BODY_BG.to_string()),
        ));
    }
    out
}

/// Builds the report over the source of `ui.rs`.
///
/// Returns the report lines and whether the audit passed. An absent `STYLE`
/// block is an `Err` whose text the caller prints, as the original did.
fn report(src: &str, path: &str) -> Result<std::result::Result<(Vec<String>, bool), String>> {
    let Some(css) = extract_style_block(src) else {
        return Ok(Err(
            "could not find `const STYLE` block in ui.rs".to_string()
        ));
    };

    let mut results = rule_pairs(css);
    // Inline `style="..."` spans in markup live outside `STYLE`.
    for (fg, bg) in inline_style_pairs(src) {
        let label = if bg.is_some() {
            "(inline style span)"
        } else {
            "(inline style span, on #0f172a panel)"
        };
        results.push((
            label.to_string(),
            fg,
            bg.unwrap_or_else(|| BODY_BG.to_string()),
        ));
    }

    // Badges under a `style="opacity:..."` ancestor: see the module docs.
    // Each carries a pair already composited to the rendered colors. So it
    // flows through the same thresholds below, like any other pair.
    let colors = badge_colors(css);
    let opacity_rows = opacity_wrapped_badges(src, &colors, BODY_BG)?;

    let mut rows = Vec::new();
    for (selector, fg, bg) in results {
        let ratio = contrast(&fg, &bg)?;
        rows.push(Row {
            selector,
            fg,
            bg,
            ratio,
        });
    }
    let opacity_count = opacity_rows.len();
    rows.extend(opacity_rows);

    let mut failures = Vec::new();
    let mut exempt = Vec::new();
    let mut passes = 0usize;
    for row in &rows {
        if row.ratio >= 4.5 {
            passes += 1;
        } else if EXEMPT_DISABLED.contains(&row.selector.as_str())
            || NON_TEXT_GRAPHICAL.contains(&row.selector.as_str())
        {
            exempt.push(row.clone());
        } else {
            failures.push(row.clone());
        }
    }
    failures.sort_by(|a, b| a.ratio.total_cmp(&b.ratio));

    let line = |r: &Row| {
        format!(
            "  {:5.2}:1  {:<40} fg={} bg={}",
            r.ratio, r.selector, r.fg, r.bg
        )
    };
    let mut out = vec![
        format!("WCAG 1.4.3 AA contrast audit — {path}"),
        format!(
            "{} text/background pairs checked ({opacity_count} of them opacity-composited).\n",
            rows.len()
        ),
    ];
    if failures.is_empty() {
        out.push("No failing normal-text pairs.".to_string());
    } else {
        out.push(format!(
            "FAILING (below 4.5:1 for normal text) — {}:",
            failures.len()
        ));
        out.extend(failures.iter().map(line));
    }
    if !exempt.is_empty() {
        out.push(format!(
            "\nExempt (inactive UI component text, SC 1.4.3 carve-out; or non-text graphical stroke, SC 1.4.11) — {}:",
            exempt.len()
        ));
        out.extend(exempt.iter().map(line));
    }
    out.push(format!("\nPassing — {passes} pairs at or above 4.5:1."));
    Ok(Ok((out, failures.is_empty())))
}

/// Runs `cargo xtask audit vantage-dashboard-contrast`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {} = args;
    let path = ui_rs();
    let src = py_read_text(&path)?;
    match report(&src, &path.display().to_string())? {
        Ok((lines, passed)) => {
            for line in lines {
                println!("{line}");
            }
            Ok(crate::util::gate_exit(passed))
        }
        Err(message) => {
            // Python's `SystemExit(message)` prints to stderr and exits 1.
            eprintln!("{message}");
            Ok(ExitCode::FAILURE)
        }
    }
}

#[cfg(test)]
#[allow(clippy::literal_string_with_formatting_args)]
mod tests {
    use super::*;

    fn fixture(css: &str, markup: &str) -> String {
        format!("const STYLE: &str = r#\"\n{css}\n\"#;\n{markup}")
    }

    #[test]
    fn contrast_matches_known_ratios() -> Result<()> {
        assert!((contrast("#000", "#fff")? - 21.0).abs() < 1e-9);
        assert!((contrast("#777777", "#777777")? - 1.0).abs() < 1e-9);
        assert!(hex_to_rgb("#abcd").is_err());
        Ok(())
    }

    #[test]
    fn a_border_color_is_not_the_text_color() {
        let pairs = rule_pairs(".x{border-color:#000;color:#fff;background:#000}");
        assert_eq!(pairs, vec![(".x".into(), "#fff".into(), "#000".into())]);
    }

    #[test]
    fn a_bare_fill_is_skipped_and_a_font_fill_counts() {
        assert_eq!(rule_pairs(".shape{fill:#fff}"), Vec::new());
        let pairs = rule_pairs(".lbl{fill:#fff;font:12px sans}");
        assert_eq!(pairs, vec![(".lbl".into(), "#fff".into(), BODY_BG.into())]);
    }

    #[test]
    fn a_low_contrast_rule_fails_and_an_exempt_one_does_not() -> Result<()> {
        let src = fixture(
            ".pagination span.disabled{color:#475569}\n.dim{color:#1e293b}",
            "",
        );
        let (lines, passed) = report(&src, "ui.rs")?.map_err(|e| anyhow!(e))?;
        assert!(!passed);
        assert_eq!(lines[2], "FAILING (below 4.5:1 for normal text) — 1:");
        assert!(lines[3].contains(".dim"), "{lines:?}");
        assert!(
            lines
                .iter()
                .any(|l| l.contains(".pagination span.disabled"))
        );
        Ok(())
    }

    #[test]
    fn an_opacity_wrapper_composites_its_badge() -> Result<()> {
        let markup = "html! {\n    span style=\"opacity:0.5\" { span.badge.DONE { \"x\" } }\n}\n";
        let src = fixture(".badge.DONE{background:#166534;color:#dcfce7}", markup);
        let (lines, _) = report(&src, "ui.rs")?.map_err(|e| anyhow!(e))?;
        assert_eq!(
            lines[1],
            "2 text/background pairs checked (1 of them opacity-composited).\n"
        );
        let colors = badge_colors(".badge.DONE{background:#166534;color:#dcfce7}");
        let rows = opacity_wrapped_badges(&src, &colors, BODY_BG)?;
        assert_eq!(rows[0].selector, "(opacity:0.5 wrapper) badge DONE");
        assert_eq!(rows[0].fg, "#dcfce7@0.5");
        Ok(())
    }

    #[test]
    fn floats_print_like_python() {
        assert_eq!(py_float(0.0), "0.0");
        assert_eq!(py_float(0.5), "0.5");
        assert_eq!(py_float(0.0001), "0.0001");
        assert_eq!(py_float(0.00001), "1e-05");
        assert_eq!(py_float(0.000_012_5), "1.25e-05");
        assert_eq!(py_float(1e-123), "1e-123");
    }

    #[test]
    fn a_five_digit_color_parses_like_python() -> Result<()> {
        assert_eq!(hex_to_rgb("#12345")?, [18.0, 52.0, 5.0]);
        Ok(())
    }

    #[test]
    fn an_absent_style_block_is_reported() -> Result<()> {
        assert!(report("fn main() {}", "ui.rs")?.is_err());
        Ok(())
    }
}
