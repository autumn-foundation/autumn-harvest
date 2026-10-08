//! Guards for the replay positioning page (issue #1993).
//!
//! `docs/why-deterministic-replay.md` tells an evaluator why Harvest keeps
//! deterministic replay. The page must exist and keep its sections. It must
//! name each asset that lowers the cost of replay. It must cite only shipped
//! issues and the current HVG range. The comparison page must link it.
//!
//! One guard also checks style: no prose sentence may exceed 25 words. That is
//! the ASD-STE100 limit that the repository uses for comments.

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

/// Words that end in a full stop but do not end a sentence.
const ABBREVIATIONS: &[&str] = &["e.g.", "i.e.", "etc.", "vs.", "cf."];

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

/// A new HVG rule must widen each range that the page cites.
///
/// The typed catalog is the source of truth. The macro keeps its own copy of
/// the rule codes, so the guard also checks that the two agree.
#[test]
fn page_cites_the_current_hvg_range() {
    let highest = autumn_harvest::guardrail::catalog()
        .iter()
        .filter_map(|rule| rule.id.strip_prefix("HVG")?.parse::<u32>().ok())
        .max()
        .expect("the guardrail catalog holds no HVG rule");
    assert_eq!(
        hvg_codes(&read(HVG_SOURCE)).last().copied(),
        Some(highest),
        "{HVG_SOURCE} and guardrail::catalog() disagree on the highest HVG rule"
    );

    let current = format!("HVG001–HVG{highest:03}");
    let cited = hvg_ranges(&read(PAGE));
    assert!(!cited.is_empty(), "{PAGE} must cite the range `{current}`");
    let stale: Vec<&String> = cited.iter().filter(|range| **range != current).collect();
    assert!(
        stale.is_empty(),
        "{PAGE} cites stale HVG ranges {stale:?}; the current range is `{current}`"
    );
}

/// A cited issue must have shipped, so no claim is a plan.
///
/// An issue has shipped when `docs/shipped-work.md` cites it, or when a
/// changelog fragment carries its number. Fragments wait in
/// `docs/changelog.d/` until a collation sweep folds them in.
#[test]
fn page_cites_only_shipped_issues() {
    let cited = issue_refs(&read(PAGE));
    assert!(
        !cited.is_empty(),
        "{PAGE} must cite the issues that shipped each asset"
    );
    let mut shipped = issue_refs(&read("docs/shipped-work.md"));
    shipped.extend(fragment_issues());
    let unshipped: Vec<u32> = cited.difference(&shipped).copied().collect();
    assert!(
        unshipped.is_empty(),
        "{PAGE} cites issues that have not shipped: {unshipped:?}. Cite only issues in \
         docs/shipped-work.md or docs/changelog.d/"
    );
}

/// Issue #1993 is done when the comparison page links the new page.
#[test]
fn comparison_page_links_the_page() {
    let comparison = read(COMPARISON);
    let determinism = section(&comparison, "\n### 5. Determinism", "\n### ")
        .expect("the comparison page must keep its determinism section");
    let row = determinism
        .lines()
        .find(|line| line.starts_with("| **autumn-harvest** |"))
        .expect("the determinism section must keep its autumn-harvest row");
    assert!(
        row.contains(PAGE_LINK),
        "the autumn-harvest determinism row in {COMPARISON} must link {PAGE_LINK}"
    );
    let related = section(&comparison, "\n## Related\n", "\n## ")
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
        "{PAGE} has prose sentences over {MAX_SENTENCE_WORDS} words. Split each one:\n{}",
        long.join("\n")
    );
}

/// A docs-only change skips the `test` matrix, so `lint` must run these guards.
#[test]
fn guards_run_on_docs_only_changes() {
    let workflow = read(".github/workflows/ci.yml");
    let lint = workflow
        .find("\n  lint:")
        .expect("ci.yml must define `lint`");
    let test = workflow
        .find("\n  test:")
        .expect("ci.yml must define `test`");
    let stanza = step_stanza(&workflow[lint..test], FILTER).unwrap_or_else(|| {
        panic!(
            "the `lint` job in ci.yml must run these guards. Add this step:\n\
             \x20     - name: Run docs/why-deterministic-replay.md guards (also on docs-only PRs)\n\
             \x20       run: \"cargo test -p autumn-harvest --no-default-features --features \
             testing {FILTER}\""
        )
    });
    check_stanza(stanza).unwrap_or_else(|reason| panic!("{reason}. Stanza:\n{stanza}"));
}

#[test]
fn stanza_checks_catch_each_bypass() {
    let good = "\n      - name: Guard\n        run: \"cargo test --test integration \
                replay_positioning_docs::\"\n";
    let block = format!("\n      - name: Before\n        run: echo a\n{good}\n  test:\n");
    let stanza = step_stanza(&block, FILTER).expect("the stanza is in the block");
    assert!(stanza.starts_with("\n      - name: Guard\n"), "{stanza}");
    assert!(
        !stanza.contains("Before") && !stanza.contains("test:"),
        "{stanza}"
    );
    assert_eq!(check_stanza(stanza), Ok(()));

    let after = format!("{good}        if: always()\n      - name: After\n        run: x\n");
    let stanza = step_stanza(&after, FILTER).expect("the stanza is in the block");
    assert!(
        stanza.ends_with("if: always()\n"),
        "an `if:` after `run:` is in the stanza"
    );
    assert!(check_stanza(stanza).is_err());

    for bad in [
        format!("{good}        continue-on-error: true\n"),
        good.replace("docs::\"", "docs::one_test\""),
    ] {
        let stanza = step_stanza(&bad, FILTER).expect("the stanza is in the block");
        assert!(check_stanza(stanza).is_err(), "{stanza}");
    }
}

#[test]
fn helpers_parse_their_inputs() {
    assert_eq!(
        hvg_codes(r#"add("HVG001"); add("HVG012"); // HVG099"#),
        [1, 12]
    );
    assert_eq!(
        hvg_ranges("HVG001–HVG009 and HVG001–HVG011."),
        ["HVG001–HVG009", "HVG001–HVG011"]
    );
    assert_eq!(
        issue_refs(
            "(#603) and [x](https://github.com/o/r/issues/1817#top) #a [y](c.md#5-five) \
             &#8212; (#12-anchor)"
        ),
        BTreeSet::from([603, 1817])
    );
    let text = "# Title\n\nOne two. Three [four](x.md) five.\n\n```rust\nlet a = b. c;\n```\n\
                \n| a | b |\n|---|---|\n| Six seven. | `x.y()` |\n\n- **Eight nine.** Ten (eleven.)\n\
                \n1. Twelve, e.g. thirteen.\n* Fourteen\n  #603 fifteen.\n\n> Sixteen. `a](b` c.\n\
                \n<!-- Not prose. -->\n";
    assert_eq!(
        prose_sentences(text),
        [
            "One two.",
            "Three four five.",
            "a",
            "b",
            "Six seven.",
            "x_y__",
            "**Eight nine.**",
            "Ten (eleven.)",
            "Twelve, e.g. thirteen.",
            "Fourteen #603 fifteen.",
            "Sixteen.",
            "a__b c."
        ]
    );
}

/// The text from `start` to the next `end` after it, or `None`.
fn section<'a>(doc: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let body = &doc[doc.find(start)? + start.len()..];
    Some(&body[..body.find(end).unwrap_or(body.len())])
}

/// The workflow step whose text contains `needle`.
///
/// The stanza runs from the step's `- ` line to the next step or job. A key
/// written below `run:` is part of the stanza.
fn step_stanza<'a>(block: &'a str, needle: &str) -> Option<&'a str> {
    let at = block.find(needle)?;
    let start = block[..at].rfind("\n      - ")?;
    let end = [
        block[at..].find("\n      - "),
        block[at..]
            .find("\n  ")
            .filter(|i| !block[at + i..].starts_with("\n   ")),
    ]
    .into_iter()
    .flatten()
    .min()
    .map_or(block.len(), |i| at + i + 1);
    Some(&block[start..end])
}

/// Each way a step can stop running the full guard set.
fn check_stanza(stanza: &str) -> Result<(), String> {
    if stanza.contains("\n        if:") {
        return Err("the guard step must not have an `if:` condition".into());
    }
    if stanza.contains("continue-on-error") {
        return Err("the guard step must not set `continue-on-error`".into());
    }
    let run = stanza
        .lines()
        .find(|line| line.trim_start().starts_with("run:"))
        .ok_or("the guard step must have a `run:` line")?;
    if !run.trim_end().ends_with(&format!("{FILTER}\"")) {
        return Err(format!("the `run:` line must end with `{FILTER}\"`"));
    }
    Ok(())
}

/// Rule codes that the lint emits, as quoted string literals, in order.
///
/// A comment that names a code is not a rule, so only literals count.
fn hvg_codes(source: &str) -> Vec<u32> {
    let codes: BTreeSet<u32> = source
        .split("\"HVG")
        .skip(1)
        .filter_map(|rest| {
            let digits = rest.get(..3)?;
            let closed = rest[3..].starts_with('"');
            (closed && digits.bytes().all(|b| b.is_ascii_digit()))
                .then(|| digits.parse().ok())
                .flatten()
        })
        .collect();
    codes.into_iter().collect()
}

/// Each `HVG001–HVGnnn` range in the text, with an en dash.
fn hvg_ranges(text: &str) -> Vec<String> {
    text.match_indices("HVG001–HVG")
        .filter_map(|(at, prefix)| {
            let digits = text.get(at + prefix.len()..at + prefix.len() + 3)?;
            digits
                .bytes()
                .all(|b| b.is_ascii_digit())
                .then(|| format!("{prefix}{digits}"))
        })
        .collect()
}

/// Issue numbers in `docs/changelog.d/` fragment names.
///
/// A fragment is named `issue-NNN-slug.md` or `pr-NNN-slug.md`.
fn fragment_issues() -> BTreeSet<u32> {
    let dir = repo_root().join("docs/changelog.d");
    std::fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("cannot list {}: {err}", dir.display()))
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            let rest = name
                .strip_prefix("issue-")
                .or_else(|| name.strip_prefix("pr-"))?;
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().ok()
        })
        .collect()
}

/// Issue numbers cited as `#NNN` or as a GitHub `issues/NNN` link.
///
/// An anchor such as `page.md#5-title` or `(#12-title)` is not a citation.
/// Neither is an HTML entity such as `&#8212;`.
fn issue_refs(text: &str) -> BTreeSet<u32> {
    let mut refs = BTreeSet::new();
    for (marker, after_marker_ok) in [("#", false), ("issues/", true)] {
        for (at, _) in text.match_indices(marker) {
            let before = text[..at].chars().next_back();
            let bad_before =
                before.is_some_and(|c| c.is_alphanumeric() || matches!(c, '&' | '#' | '/'));
            if bad_before && !after_marker_ok {
                continue;
            }
            let rest = &text[at + marker.len()..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            let next = rest[digits.len()..].chars().next();
            let anchor = next.is_some_and(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | ';'));
            if (1..=6).contains(&digits.len())
                && !anchor
                && let Ok(number) = digits.parse()
            {
                refs.insert(number);
            }
        }
    }
    refs
}

/// Prose sentences from paragraphs, list items, blockquotes and table cells.
///
/// Headings, code fences, HTML comments and table separator rows are not
/// prose. A link keeps its text and loses its target, so a URL does not count
/// as words. An inline code span counts as words but cannot end a sentence.
fn prose_sentences(text: &str) -> Vec<String> {
    let mut blocks: Vec<String> = Vec::new();
    let mut paragraph = String::new();
    let mut in_fence = false;
    let mut in_comment = false;
    for raw in text.lines() {
        let raw = raw.trim();
        if raw.starts_with("```") || raw.starts_with("~~~") {
            in_fence = !in_fence;
            blocks.push(std::mem::take(&mut paragraph));
            continue;
        }
        let line = mask_code_spans(raw);
        let mut trimmed = line.as_str();
        if trimmed.starts_with("<!--") {
            in_comment = true;
        }
        if in_fence || in_comment || is_heading(trimmed) || trimmed.starts_with("|-") {
            in_comment &= !trimmed.contains("-->");
            blocks.push(std::mem::take(&mut paragraph));
            continue;
        }
        if trimmed.starts_with('|') {
            blocks.push(std::mem::take(&mut paragraph));
            blocks.extend(trimmed.trim_matches('|').split('|').map(str::to_owned));
            continue;
        }
        while let Some(rest) = trimmed.strip_prefix('>') {
            trimmed = rest.trim_start();
        }
        let item = ["- ", "* ", "+ "]
            .iter()
            .find_map(|bullet| trimmed.strip_prefix(bullet))
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

/// A Markdown heading starts with one to six `#` and a space.
fn is_heading(line: &str) -> bool {
    let hashes = line.bytes().take_while(|&b| b == b'#').count();
    (1..=6).contains(&hashes) && matches!(line.as_bytes().get(hashes), None | Some(b' '))
}

/// Replace marks inside each inline code span with `_`.
///
/// A masked span keeps its word count. Its full stops, brackets and pipes no
/// longer end a sentence, start a link or split a table cell.
fn mask_code_spans(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut in_code = false;
    for c in line.chars() {
        if c == '`' {
            in_code = !in_code;
        } else if in_code && matches!(c, '.' | '?' | '!' | '[' | ']' | '(' | ')' | '|') {
            out.push('_');
        } else {
            out.push(c);
        }
    }
    out
}

/// The body of a `1. item` line, or `None`.
fn list_number_body(line: &str) -> Option<&str> {
    let (number, body) = line.split_once(". ")?;
    (!number.is_empty() && number.bytes().all(|b| b.is_ascii_digit())).then_some(body)
}

/// Replace `[text](target)` with `text`. An unclosed target stays as text.
fn strip_link_targets(block: &str) -> String {
    let mut out = String::new();
    let mut rest = block;
    while let Some(open) = rest.find("](") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let Some(close) = after.find(')') else {
            out.push(' ');
            rest = after;
            break;
        };
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    out.replace('[', "")
}

/// Split after a word that ends in `.`, `?` or `!`.
///
/// Closing emphasis, quote and bracket marks after the stop still end the
/// sentence, so `**Lead-in.**` is one sentence. An abbreviation does not end
/// a sentence.
fn split_sentences(block: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut current = String::new();
    for word in block.split_whitespace() {
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
        let bare = word.trim_end_matches(['*', '_', ')', '"', '\'']);
        let abbreviation = ABBREVIATIONS.contains(&bare.to_ascii_lowercase().as_str());
        if bare.ends_with(['.', '?', '!']) && !abbreviation {
            sentences.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        sentences.push(current);
    }
    sentences
}
