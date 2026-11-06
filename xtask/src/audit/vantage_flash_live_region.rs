//! `cargo xtask audit vantage-flash-live-region`: the port of
//! `docs/audits/vantage-flash-live-region.py`.
//!
//! This is the full-page-load status-message announcement audit for the
//! Vantage dashboard's flash messages (`autumn-harvest-plugin/src/ui.rs`).
//!
//! The audit is deterministic and reproducible on any checkout. It needs no
//! server, no browser and no network access. Every mutating operator action
//! in Vantage redirects back to a page that renders its outcome as a
//! `div.flash`. Examples are cancel, pause, resume, terminate, signal, reset,
//! trigger-update, DAG retry, schedule actions, dead-letter replay or discard,
//! and build-routing retire or revoke. The flash is the *only* place where the
//! operator learns whether the action succeeded, failed, or was a no-op. There
//! is no `<script>`-driven toast and no other notification path.
//!
//! Two things must both be true for that message to reach a screen-reader
//! user, and this audit checks both:
//!
//! 1. **A live-region role.** That is `role="status"` or `role="alert"`,
//!    matched only as a real attribute and not as a substring of
//!    `data-role="status"`. An `aria-live` attribute counts only with an
//!    active value, `polite` or `assertive`. The value `aria-live="off"` is
//!    explicitly not live.
//! 2. **Programmatic focus on load.** That is `tabindex="-1"` *and* a bare
//!    `autofocus`. Vantage is a traditional multi-page app. Every flash is a
//!    full HTTP redirect to a fresh page load. It is not a DOM mutation on a
//!    page that the assistive-tech tree has already registered. A live-region
//!    role alone does not reliably announce content that is present when the
//!    accessibility tree is first built. ARIA live regions fire on *changes
//!    after* registration, not on initial content (WAI-ARIA Authoring
//!    Practices Guide, "Live Region Perceivable Author Practices"). The fix
//!    for a full page load needs no JavaScript: it moves focus to the
//!    message. The `tabindex="-1"` attribute makes a non-interactive `<div>`
//!    programmatically focusable without adding it to the tab order. The
//!    `autofocus` attribute makes the browser focus it when the page finishes
//!    loading. Assistive tech then announces the content of the focused
//!    element. The `role="status"` attribute stays alongside it, because it
//!    still gives the message the correct semantic. It also matters if the
//!    page ever grows client-side updates to the same node. On its own, it is
//!    not sufficient here.
//!
//! Run `cargo xtask audit vantage-flash-live-region`. The command exits 1 if
//! any render site lacks either half of the mechanism, and 0 otherwise. So it
//! is safe as a CI gate, or as a standalone report.
//!
//! This module also holds the scan that
//! `cargo xtask audit vantage-degraded-banner-focus` shares, and the
//! lookbehind helper that each Vantage audit uses.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::LazyLock;

use regex::{Captures, Regex};
use smop::prelude::*;

use super::libpq_keyword_sync::py_read_text;
use crate::util::repo_path;

/// Arguments of `cargo xtask audit vantage-flash-live-region`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask audit vantage-flash-live-region`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {} = args;
    run_status_audit(&FLASH)
}

/// The path of the Vantage dashboard source that each Vantage audit reads.
pub(super) fn ui_rs() -> PathBuf {
    repo_path("autumn-harvest-plugin/src/ui.rs")
}

/// Returns true for a character that the Python class `[\w-]` matches.
pub(super) fn is_word_or_hyphen(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-'
}

/// Returns true for a character that the Python class `[\w.-]` matches.
pub(super) fn is_word_dot_or_hyphen(c: char) -> bool {
    is_word_or_hyphen(c) || c == '.'
}

/// Finds each match of `re` in `text` whose preceding character fails `reject`.
///
/// The `regex` crate has no lookbehind. A Python pattern `(?<![\w-])x` becomes
/// the pattern `x` plus a `reject` test on the character before the match.
/// A rejected match moves the search one character on, as Python does. A
/// match at the start of `text` always passes, because nothing precedes it.
pub(super) fn captures_unpreceded<'t>(
    re: &Regex,
    text: &'t str,
    reject: fn(char) -> bool,
) -> Vec<Captures<'t>> {
    let mut found = Vec::new();
    let mut pos = 0;
    while pos <= text.len() {
        let Some(caps) = re.captures_at(text, pos) else {
            break;
        };
        let Some(whole) = caps.get(0) else {
            break;
        };
        let start = whole.start();
        let preceded = text[..start].chars().next_back().is_some_and(reject);
        if preceded {
            pos = next_boundary(text, start);
            continue;
        }
        pos = if whole.end() == start {
            next_boundary(text, start)
        } else {
            whole.end()
        };
        found.push(caps);
    }
    found
}

/// Returns the leftmost match of `re` in `text` that `reject` does not refuse.
pub(super) fn search_unpreceded<'t>(
    re: &Regex,
    text: &'t str,
    reject: fn(char) -> bool,
) -> Option<Captures<'t>> {
    let mut pos = 0;
    while pos <= text.len() {
        let caps = re.captures_at(text, pos)?;
        let start = caps.get(0)?.start();
        if !text[..start].chars().next_back().is_some_and(reject) {
            return Some(caps);
        }
        pos = next_boundary(text, start);
    }
    None
}

/// Returns the byte index of the character after the one at `index`.
fn next_boundary(text: &str, index: usize) -> usize {
    text[index..]
        .chars()
        .next()
        .map_or(text.len() + 1, |c| index + c.len_utf8())
}

/// Returns the 1-based line number of the byte at `index`.
pub(super) fn line_of(src: &str, index: usize) -> usize {
    src[..index].matches('\n').count() + 1
}

/// The class token and report wording of one status-message audit.
pub(super) struct StatusSpec {
    /// The class token that marks a render site, such as `flash`.
    pub class: &'static str,
    /// The lines that the report prints when it finds no render site.
    pub none_found: &'static [&'static str],
}

/// The spec of `cargo xtask audit vantage-flash-live-region`.
const FLASH: StatusSpec = StatusSpec {
    class: "flash",
    none_found: &[
        "ERROR: found no `div.flash` render sites at all — no `div` in",
        "ui.rs carries a \"flash\" class this audit recognizes. Either the",
        "flash markup was rewritten in a spelling this audit doesn't",
        "recognize (a silent pass would hide that), or flash messages",
        "were removed outright.",
    ],
};

// Every `div`'s full opening tag, from `div` to the block's `{`. Maud lets
// the class token appear as any chained shorthand class (`div.notice.flash`,
// not just the first). It can also be one token in a space-separated
// `class="..."` value (`class="flash notice"`). So matching only `div.flash`
// or an exact `class="flash"` missed those. The scan reads the whole tag and
// checks for the token in either form, at any position.
static DIV_OPEN: LazyLock<Regex> = LazyLock::new(|| compile(r"\bdiv\b[^{]*\{"));

// Maud's chained-class shorthand (`div.notice.flash`, `."quoted-name"`) can
// only appear contiguously right after `div`. It comes before any
// space-separated attribute begins. So the pattern anchors at `^div` and
// consumes only that run. A `.flash`-shaped substring inside some *other*
// attribute's quoted value (e.g. `div title=".flash"`) is then never reached.
// That needs no reasoning about quoting. Maud reads a token stream, so
// whitespace around the `.` (`div .flash`, `div. flash`) is insignificant
// there. It is as insignificant as it is around `=` elsewhere. The `\s*`
// tolerates it on both sides.
static CHAINED_PREFIX: LazyLock<Regex> =
    LazyLock::new(|| compile(r#"^div((?:\s*\.\s*(?:[\w-]+|"[^"]*"))*)"#));
static CHAINED_SEGMENT: LazyLock<Regex> =
    LazyLock::new(|| compile(r#"\.\s*([\w-]+)|\.\s*"([^"]*)""#));

// The Python pattern has `(?<![\w-])` before "class". It requires a real
// attribute boundary, so `data-class="flash"` (an unrelated attribute) does
// not match. The port applies that boundary through `search_unpreceded`.
// Maud reads a Rust token stream, not raw text. So whitespace around `=`
// (`class = "flash"`) is as valid as the tight form. And `cargo fmt` does not
// normalize it away inside a macro body. The `\s*=\s*` here and in `ROLE`
// and `FOCUS_ON_LOAD` tolerates both.
static CLASS_ATTR: LazyLock<Regex> = LazyLock::new(|| compile(r#"class\s*=\s*"([^"]*)""#));

// The Python pattern has `(?<![\w-])` before each branch. The match must not
// follow a word character or a hyphen. So `role="status"` matches as a real
// attribute, but the same text inside `data-role="status"` does not. The `e`
// of "role" would otherwise still start a substring match after the hyphen.
// The `aria-live` attribute counts only with an active value. The value
// `aria-live="off"` is explicitly not live.
static ROLE: LazyLock<Regex> = LazyLock::new(|| {
    compile(r#"role\s*=\s*"(?:status|alert)"|aria-live\s*=\s*"(?:polite|assertive)""#)
});
static FOCUS_ON_LOAD: LazyLock<Regex> = LazyLock::new(|| compile(r#"tabindex\s*=\s*"-1""#));
static AUTOFOCUS: LazyLock<Regex> = LazyLock::new(|| compile(r"autofocus\b"));

/// Compiles a pattern that is a constant of this crate.
pub(super) fn compile(pattern: &str) -> Regex {
    match Regex::new(pattern) {
        Ok(re) => re,
        Err(error) => panic!("invalid constant pattern {pattern:?}: {error}"),
    }
}

/// Returns the class tokens of the chained shorthand after `div`.
fn chained_class_tokens(tag_text: &str) -> Vec<String> {
    let Some(prefix) = CHAINED_PREFIX.captures(tag_text) else {
        return Vec::new();
    };
    let run = prefix.get(1).map_or("", |m| m.as_str());
    CHAINED_SEGMENT
        .captures_iter(run)
        .map(|seg| {
            seg.get(1)
                .filter(|m| !m.as_str().is_empty())
                .or_else(|| seg.get(2))
                .map_or_else(String::new, |m| m.as_str().to_string())
        })
        .collect()
}

/// Returns true when the opening tag carries `class` as a token.
fn is_class_div(tag_text: &str, class: &str) -> bool {
    if chained_class_tokens(tag_text).iter().any(|t| t == class) {
        return true;
    }
    search_unpreceded(&CLASS_ATTR, tag_text, is_word_or_hyphen).is_some_and(|m| {
        m.get(1)
            .is_some_and(|v| v.as_str().split_whitespace().any(|t| t == class))
    })
}

/// One render site: its line, and whether it has each half of the mechanism.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Site {
    line: usize,
    has_role: bool,
    has_focus: bool,
}

/// Returns every render site of `class` in `src`, sorted by line.
fn sites(src: &str, class: &str) -> Vec<Site> {
    let mut found: Vec<Site> = DIV_OPEN
        .find_iter(src)
        .filter(|m| is_class_div(m.as_str(), class))
        .map(|m| {
            let tag = m.as_str();
            Site {
                line: line_of(src, m.start()),
                has_role: search_unpreceded(&ROLE, tag, is_word_or_hyphen).is_some(),
                has_focus: search_unpreceded(&FOCUS_ON_LOAD, tag, is_word_or_hyphen).is_some()
                    && search_unpreceded(&AUTOFOCUS, tag, is_word_or_hyphen).is_some(),
            }
        })
        .collect();
    found.sort();
    found
}

/// Builds the report of one status-message audit over `src`.
///
/// Returns the report lines and whether the audit passed.
pub(super) fn report(src: &str, path: &str, spec: &StatusSpec) -> (Vec<String>, bool) {
    let found = sites(src, spec.class);
    let noun = format!("`div.{}`", spec.class);
    let mut out = vec![
        format!("Full-page-load status-message announcement audit — {path}"),
        format!("{} {noun} render sites checked.\n", found.len()),
    ];
    if found.is_empty() {
        out.extend(spec.none_found.iter().map(|line| (*line).to_string()));
        return (out, false);
    }
    let missing: Vec<&Site> = found
        .iter()
        .filter(|s| !(s.has_role && s.has_focus))
        .collect();
    let ok_count = found.len() - missing.len();
    if missing.is_empty() {
        out.push(format!(
            "Every {noun} render site has both a live-region role and focus-on-load."
        ));
    } else {
        out.push(format!("INCOMPLETE — {}:", missing.len()));
        for site in &missing {
            let mut gaps = Vec::new();
            if !site.has_role {
                gaps.push(r#"no role="status"/"alert" (or active aria-live)"#);
            }
            if !site.has_focus {
                gaps.push(r#"no tabindex="-1"+autofocus (won't announce on a full page load)"#);
            }
            out.push(format!("  ui.rs:{}  {}", site.line, gaps.join("; ")));
        }
    }
    out.push(format!(
        "\nCorrect — {ok_count} of {} render sites carry the full mechanism.",
        found.len()
    ));
    (out, missing.is_empty())
}

/// Runs one status-message audit over `ui.rs` and prints its report.
pub(super) fn run_status_audit(spec: &StatusSpec) -> Result<ExitCode> {
    let path = ui_rs();
    let src = py_read_text(&path)?;
    let (lines, passed) = report(&src, &path.display().to_string(), spec);
    for line in lines {
        println!("{line}");
    }
    Ok(crate::util::gate_exit(passed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_complete_flash_passes() {
        let src = "html! {\n    div.flash role=\"status\" tabindex=\"-1\" autofocus { (m) }\n}\n";
        let (lines, passed) = report(src, "ui.rs", &FLASH);
        assert!(passed, "{lines:?}");
        assert_eq!(lines[1], "1 `div.flash` render sites checked.\n");
    }

    #[test]
    fn every_class_spelling_is_a_site() {
        let src = concat!(
            "div.notice.flash {}\n",
            "div . flash {}\n",
            "div.\"flash\" {}\n",
            "div class=\"x flash\" {}\n",
            "div class = \"flash\" {}\n",
        );
        let found = sites(src, "flash");
        assert_eq!(found.len(), 5, "{found:?}");
        assert!(found.iter().all(|s| !s.has_role && !s.has_focus));
    }

    #[test]
    fn a_lookalike_attribute_is_not_a_site_or_a_role() {
        let src = concat!(
            "div title=\".flash\" {}\n",
            "div data-class=\"flash\" {}\n",
            "div.flash data-role=\"status\" aria-live=\"off\" tabindex=\"-1\" autofocus {}\n",
        );
        let found = sites(src, "flash");
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(
            found[0],
            Site {
                line: 3,
                has_role: false,
                has_focus: true
            }
        );
        let (lines, passed) = report(src, "ui.rs", &FLASH);
        assert!(!passed);
        assert_eq!(
            lines[3],
            "  ui.rs:3  no role=\"status\"/\"alert\" (or active aria-live)"
        );
    }

    #[test]
    fn no_site_at_all_fails() {
        let (lines, passed) = report("fn main() {}\n", "ui.rs", &FLASH);
        assert!(!passed);
        assert!(lines[2].starts_with("ERROR: found no `div.flash`"));
    }

    #[test]
    fn the_lookbehind_helper_skips_a_preceded_match() {
        let re = compile("role");
        let text = "data-role role";
        let hit = search_unpreceded(&re, text, is_word_or_hyphen);
        assert_eq!(hit.and_then(|c| c.get(0)).map(|m| m.start()), Some(10));
        assert_eq!(
            captures_unpreceded(&re, "rolerole", is_word_or_hyphen).len(),
            1
        );
        assert_eq!(
            captures_unpreceded(&re, "role role", is_word_or_hyphen).len(),
            2
        );
    }
}
