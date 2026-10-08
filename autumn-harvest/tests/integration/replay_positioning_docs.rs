//! Guards for the replay positioning page (issue #1993).
//!
//! `docs/why-deterministic-replay.md` tells an evaluator why Harvest keeps
//! deterministic replay. These guards check facts, not prose style. The page
//! must exist and keep its sections. It must name each asset that lowers the
//! cost of replay. It must cite only shipped issues and the current HVG range.
//! The comparison page must link it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const PAGE: &str = "docs/why-deterministic-replay.md";
const PAGE_LINK: &str = "why-deterministic-replay.md";
const COMPARISON: &str = "docs/comparison.md";
const HVG_SOURCE: &str = "autumn-harvest-macros/src/determinism_lint.rs";
const FILTER: &str = "--test integration replay_positioning_docs::";

/// The sections an evaluator needs, in reading order.
const SECTIONS: &[&str] = &[
    "## Two models",
    "## What replay buys",
    "## What replay costs",
    "## How Harvest lowers the cost",
    "## When checkpoint-only is the better choice",
    "## Related",
];

/// Each asset that issue #1993 tells the page to cite.
const ASSETS: &[&str] = &[
    "`det_check`",
    "`harvest-verify`",
    "replay canar",
    "drift gate",
    "parked",
];

/// The longest prose sentence the page may hold, per ASD-STE100.
const MAX_SENTENCE_WORDS: usize = 25;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

/// Read a repo file with CRLF folded to LF, so a Windows checkout matches.
fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("issue #1993: cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

#[test]
fn page_has_every_section_in_order() {
    let page = read(PAGE);
    assert!(
        page.starts_with("# Why Harvest keeps deterministic replay\n"),
        "{PAGE} must open with its title"
    );
    let mut from = 0;
    for section in SECTIONS {
        let at = page[from..]
            .find(&format!("\n{section}\n"))
            .unwrap_or_else(|| panic!("{PAGE} lacks `{section}`, or it is out of order"));
        from += at + section.len();
    }
}

#[test]
fn page_names_every_asset_from_the_issue() {
    let page = read(PAGE);
    for asset in ASSETS {
        assert!(page.contains(asset), "{PAGE} must cite {asset}");
    }
}

/// A new HVG rule must widen the range that the page cites.
#[test]
fn page_cites_the_current_hvg_range() {
    let source = read(HVG_SOURCE);
    let highest = hvg_codes(&source)
        .into_iter()
        .max()
        .unwrap_or_else(|| panic!("{HVG_SOURCE} defines no HVG rule"));
    let range = format!("HVG001–HVG{highest:03}");
    assert!(
        read(PAGE).contains(&range),
        "{PAGE} must cite the rule range `{range}`"
    );
}

/// A cited issue must be in the shipped-work record, so no claim is a plan.
#[test]
fn page_cites_only_shipped_issues() {
    let cited = issue_refs(&read(PAGE));
    assert!(!cited.is_empty(), "{PAGE} must cite the issues that shipped each asset");
    let shipped = issue_refs(&read("docs/shipped-work.md"));
    let unshipped: Vec<u32> = cited.difference(&shipped).copied().collect();
    assert!(
        unshipped.is_empty(),
        "{PAGE} cites issues absent from docs/shipped-work.md: {unshipped:?}"
    );
}

/// Issue #1993 is done when the comparison page links the new page.
#[test]
fn comparison_page_links_the_page() {
    let comparison = read(COMPARISON);
    let determinism_row = comparison
        .lines()
        .find(|line| line.starts_with("| **autumn-harvest** | Event-sourced deterministic replay"))
        .expect("the comparison page must keep its determinism row");
    assert!(
        determinism_row.contains(PAGE_LINK),
        "the determinism row in {COMPARISON} must link {PAGE_LINK}"
    );
    let related = comparison
        .split("\n## Related\n")
        .nth(1)
        .expect("the comparison page must keep its Related section");
    assert!(
        related.contains(PAGE_LINK),
        "the Related section in {COMPARISON} must link {PAGE_LINK}"
    );
}

#[test]
fn prose_sentences_stay_short() {
    let long: Vec<String> = prose_sentences(&read(PAGE))
        .into_iter()
        .filter(|sentence| sentence.split_whitespace().count() > MAX_SENTENCE_WORDS)
        .collect();
    assert!(
        long.is_empty(),
        "{PAGE} has prose sentences over {MAX_SENTENCE_WORDS} words:\n{}",
        long.join("\n")
    );
}

/// A docs-only change skips the `test` matrix, so `lint` must run these guards.
#[test]
fn guards_run_on_docs_only_changes() {
    let workflow = read(".github/workflows/ci.yml");
    let lint = workflow.find("\n  lint:").expect("ci.yml must define `lint`");
    let test = workflow.find("\n  test:").expect("ci.yml must define `test`");
    let step_at = workflow
        .find(FILTER)
        .expect("ci.yml must run the replay_positioning_docs guards");
    assert!(
        step_at > lint && step_at < test,
        "the replay_positioning_docs step must live in the ungated `lint` job"
    );
    let line_start = workflow[..step_at].rfind('\n').map_or(0, |i| i + 1);
    assert!(
        workflow[line_start..].trim_start().starts_with("run:"),
        "the guard invocation must be a step `run:` line"
    );
    let name_at = workflow[..step_at]
        .rfind("\n      - name:")
        .expect("the guard step must have a name");
    let next_step = workflow[step_at..]
        .find("\n      - ")
        .map_or(test, |i| step_at + i);
    assert!(
        !workflow[name_at..next_step].contains("\n        if:"),
        "the replay_positioning_docs step must not have an `if:` condition"
    );
}

#[test]
fn helpers_parse_their_inputs() {
    assert_eq!(
        hvg_codes(r#"add("HVG001"); add("HVG012"); // HVG099"#),
        BTreeSet::from([1, 12])
    );
    assert_eq!(
        issue_refs("(#603) and [x](https://github.com/o/r/issues/1817#top) #a"),
        BTreeSet::from([603, 1817])
    );
    let text = "# Title\n\nOne two. Three [four](x.md) five.\n\n```rust\nlet a = b. c;\n```\n\
                \n| a | b |\n|---|---|\n| Six seven. | x |\n\n- Eight nine.\n";
    assert_eq!(
        prose_sentences(text),
        ["One two.", "Three four five.", "a", "b", "Six seven.", "x", "Eight nine."]
    );
}

/// Rule codes that the lint emits, as quoted string literals.
///
/// A comment that names a code is not a rule, so only literals count.
fn hvg_codes(source: &str) -> BTreeSet<u32> {
    source
        .split("\"HVG")
        .skip(1)
        .filter_map(|rest| {
            let digits = rest.get(..3)?;
            let closed = rest[3..].starts_with('"');
            (closed && digits.bytes().all(|b| b.is_ascii_digit()))
                .then(|| digits.parse().ok())
                .flatten()
        })
        .collect()
}

/// Issue numbers cited as `#NNN` or as a GitHub `issues/NNN` link.
fn issue_refs(text: &str) -> BTreeSet<u32> {
    let mut refs = BTreeSet::new();
    for marker in ["#", "issues/"] {
        for rest in text.split(marker).skip(1) {
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            if (1..=6).contains(&digits.len())
                && let Ok(number) = digits.parse()
            {
                refs.insert(number);
            }
        }
    }
    refs
}

/// Prose sentences from paragraphs, list items and table cells.
///
/// Headings, code fences and table separator rows are not prose. A link keeps
/// its text and loses its target, so a URL does not count as words.
fn prose_sentences(text: &str) -> Vec<String> {
    let mut blocks: Vec<String> = Vec::new();
    let mut paragraph = String::new();
    let mut in_fence = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            in_fence = !in_fence;
            blocks.push(std::mem::take(&mut paragraph));
            continue;
        }
        if in_fence || trimmed.starts_with('#') || trimmed.starts_with("|-") {
            blocks.push(std::mem::take(&mut paragraph));
            continue;
        }
        if trimmed.starts_with('|') {
            blocks.push(std::mem::take(&mut paragraph));
            blocks.extend(trimmed.trim_matches('|').split('|').map(str::to_owned));
            continue;
        }
        let item = trimmed
            .strip_prefix("- ")
            .or_else(|| list_number_body(trimmed));
        if trimmed.is_empty() || item.is_some() {
            blocks.push(std::mem::take(&mut paragraph));
        }
        paragraph.push(' ');
        paragraph.push_str(item.unwrap_or(trimmed));
    }
    blocks.push(paragraph);

    blocks
        .iter()
        .map(|block| strip_link_targets(block))
        .flat_map(|block| split_sentences(&block))
        .collect()
}

/// The body of a `1. item` line, or `None`.
fn list_number_body(line: &str) -> Option<&str> {
    let (number, body) = line.split_once(". ")?;
    number.bytes().all(|b| b.is_ascii_digit()).then_some(body)
}

/// Replace `[text](target)` with `text`.
fn strip_link_targets(block: &str) -> String {
    let mut out = String::new();
    let mut rest = block;
    while let Some(open) = rest.find("](") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        rest = after.find(')').map_or("", |close| &after[close + 1..]);
    }
    out.push_str(rest);
    out.replace('[', "")
}

/// Split on `. `, `? ` and `! `, and drop empty pieces.
fn split_sentences(block: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut current = String::new();
    let words: Vec<&str> = block.split_whitespace().collect();
    for word in words {
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
        if word.ends_with(['.', '?', '!']) {
            sentences.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        sentences.push(current);
    }
    sentences
}
