//! `cargo xtask audit corpus-link-check`: internal link check and orphan scan for `docs/`.
//!
//! This is the Folio corpus harness, ported from `docs/audits/corpus-link-check.py`.
//! It is deterministic and reproducible on any checkout. It needs no network access.
//! It runs two Tier-1 audits (see `docs/audits/README.md`).
//!
//! 1. Link check. Each relative markdown link in `docs/**/*.md` and in the
//!    top-level sources resolves against the filesystem. The check covers both
//!    link forms that this corpus uses. One form is inline: `[text](path#anchor)`.
//!    The other form is reference style: `[text][ref]`, `[text][]` or the bare
//!    shortcut `[ref]`. A reference resolves against the `[ref]: path#anchor`
//!    definitions of its own file, as in the `[replay canary]` link of
//!    `docs/replay-drift-gate.md`. A missing target file is broken. An
//!    undefined reference is broken. An `#anchor` that matches no heading in
//!    the target is broken. A same-file `#anchor` must match a heading of the
//!    source. Anchors use the GitHub heading-slug rule: lowercase, spaces to
//!    hyphens, punctuation other than `-` and `_` removed. Each internal link
//!    in this corpus was written against that rule.
//!
//! 2. Orphan scan. A page under `docs/` with no inbound link from another page
//!    of the corpus is an orphan. An unreachable page is a coverage defect in
//!    disguise. The answer can be correct and present, but nothing points a
//!    reader at it.
//!
//! External links (`http`, `https`, `mailto`) are not checked on purpose. That
//! check needs network access and is a different audit. This audit reads only
//! the filesystem, so it can run in CI on every PR.
//!
//! # Known limitations
//!
//! - A non-fenced code block with an indent of 4 spaces is not excluded from
//!   link and heading extraction. Only fenced blocks and inline code spans
//!   are. A correct fix needs container-relative indent tracking. An indent of
//!   4 spaces is a code block only when it is not a list-item continuation,
//!   and this corpus uses list continuations constantly. For example,
//!   `docs/performance.md` has a real link at a 4-space indent inside a `*`
//!   list item. A naive fix that blanks each 4-space line stops checking such
//!   links. No top-level indented code block in the corpus holds link-like or
//!   heading-like text (checked). So the risk of the naive fix is larger than
//!   the gap, which has no current impact.
//!
//! - `slugify` reads the raw Markdown source of a heading, not the text that
//!   GitHub renders. The markup that this corpus puts in headings does not
//!   show the difference. Bold, code and emoji pass through or are removed as
//!   punctuation either way. A heading that is itself a link, such as
//!   `## [Guide](guide.md)`, slugs as "Guide" on GitHub. Here it slugs from the
//!   raw link text and gives garbage. A correct fix renders inline Markdown to
//!   plain text first, which is a small renderer of its own. No heading in the
//!   corpus is a link (checked), so this stays unfixed.
//!
//! - A fenced code block inside a blockquote (each line starts with `>`) is not
//!   a fence here. The fence pattern allows up to 3 spaces before the
//!   delimiter, not a blockquote marker. The corpus has at least one such
//!   block, in `docs/runbooks/triage-pending-tasks-idle-workers.md`. Nothing
//!   inside it looks like a link or heading, so it gives no false result. A
//!   correct fix tracks the blockquote depth together with the fence state.
//!   That is real scope for a gap that the corpus does not trip over.
//!
//! # Usage
//!
//! ```sh
//! cargo xtask audit corpus-link-check [--json]
//! ```
//!
//! The exit code is 1 if a broken link is in the reader corpus. Orphans are
//! reported but do not fail the run. An orphan is a findability defect to
//! triage, not a hard error.
//!
//! A broken link inside a process-artifact subtree (see
//! `PROCESS_ARTIFACT_PREFIXES`) is reported but does not fail the run. The
//! exception is a page that a reader can reach by following links from the
//! real corpus, transitively. Such a page is graded as corpus, because a
//! reader really lands there. `docs/rnd/determinism-static-analysis.md`,
//! cited from `docs/harvest-verify.md`, is an example.

pub mod pycompat;

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use regex::{Captures, Regex};
use smop::prelude::*;

use self::pycompat::Json;
use crate::util;

/// Arguments of `cargo xtask audit corpus-link-check`.
#[derive(Debug, Args)]
pub struct Args {
    /// Print the result as JSON.
    #[arg(long)]
    pub json: bool,
}

/// Top-level files that are inbound-link sources but not graded corpus pages.
///
/// `README.md` points into `docs/`, so it takes part in the link graph. These
/// files are the front door (Onramp's territory) or process docs. They are
/// not reader-facing reference pages, so the orphan scan does not grade them.
pub const TOP_LEVEL_SOURCES: [&str; 3] = ["README.md", "CHANGELOG.md", "RELEASE_NOTES.md"];

/// Subtrees under `docs/` that hold process and working artifacts.
///
/// These are not indexed reader reference pages:
///
/// - `docs/plans/` holds engineering plans.
/// - `docs/design/` holds per-issue design records, which keep rejected options on purpose.
/// - `docs/changelog.d/` is the changelog fragment inbox, write-only until release.
/// - `docs/rnd/` holds R&D writeups.
/// - `docs/assays/` and `docs/perf-artifacts/` hold dated performance investigations.
///
/// A reader who debugs a live system does not land on these. So the orphan
/// scan does not grade them, and a broken link inside one does not fail CI.
/// They still take part as link sources, because a plan or an assay links out
/// to the reference page it is about. Their broken links are still reported,
/// for a later pruning pass.
pub const PROCESS_ARTIFACT_PREFIXES: [&str; 6] = [
    "docs/plans/",
    "docs/design/",
    "docs/changelog.d/",
    "docs/rnd/",
    "docs/assays/",
    "docs/perf-artifacts/",
];

/// The link label allows one level of nested brackets.
///
/// A linked image `[![alt](src)](dest)` is the badge idiom of this corpus, for
/// example the license badge in `README.md`. With this label it matches as one
/// link whose target is the outer `dest`. Without it, the inner image consumes
/// the match, so the outer target is never checked.
const LABEL: &str = r"(?:[^\[\]]|\[[^\[\]]*\])*";

/// The optional link title, in all three `CommonMark` delimiter forms.
///
/// A pattern that accepts only double quotes misses a whole link with a
/// single-quoted or parenthesized title. Such a link is then never reported
/// as broken.
const TITLE: &str = r#"(?:"[^"]*"|'[^']*'|\([^)]*\))"#;

/// The link destination, bare or in angle brackets.
///
/// `CommonMark` allows `[x](<a b.md>)` so that a path can hold a space.
/// `unwrap_angle_dest` removes the brackets afterwards. Without this form, the
/// brackets become part of the path, so a real file reads as missing.
///
/// The bare form allows one level of balanced parentheses, as in
/// `[x](guide(v2).md)`. A stop at the first `)` truncates that target and
/// reports a real file as missing.
const DEST: &str = r"(<[^<>]*>|(?:[^()\s]|\([^()]*\))+)";

/// The compiled patterns of the link check.
///
/// Python wrote several of them with a `(?<!\\)` look-behind before the
/// opening bracket. `CommonMark` renders `\[not a link](x)` as literal text. The
/// Rust regex crate has no look-around, so `find_matches` applies that guard
/// in code. The guard does not count runs of backslashes. An escaped
/// backslash before a real link is misread, and no such text is in the corpus.
pub struct Patterns {
    link: Regex,
    image: Regex,
    ref_use: Regex,
    shortcut: Regex,
    ref_def: Regex,
    heading: Regex,
    spaces: Regex,
    fence_open: Regex,
}

impl Patterns {
    /// Compiles each pattern.
    pub fn new() -> Result<Self> {
        Ok(Self {
            // An inline link. The label half of `[text](url)` never doubles as
            // a shortcut reference, because `shortcut_ok` refuses it.
            link: pycompat::regex(&format!(r"\[{LABEL}\]\({DEST}(?:\s+{TITLE})?\)"))?,
            // A bare image is checked on its own, so a local image source gets
            // a missing-file check even inside an outer link.
            image: pycompat::regex(&format!(r"!\[[^\]]*\]\({DEST}(?:\s+{TITLE})?\)"))?,
            // A reference-style link: `[text][ref]` or the collapsed `[text][]`.
            ref_use: pycompat::regex(r"\[([^\]]*)\]\[([^\]]*)\]")?,
            // A bare shortcut reference `[ref]`. It counts only when `ref` has
            // a definition. Otherwise it is prose, such as `[FAILED]` in a log.
            shortcut: pycompat::regex(r"\[([^\]]+)\]")?,
            // A reference definition, indented up to 3 spaces. The target is
            // bare or in angle brackets, the only way to put a space in it.
            ref_def: pycompat::regex(
                r#"(?m)^[ \t]{0,3}\[([^\]]+)\]:[ \t]*(<[^<>]*>|\S+)(?:[ \t]+(?:"[^"]*"|'[^']*'|\([^)]*\)))?[ \t]*$"#,
            )?,
            // CommonMark allows up to 3 leading spaces on an ATX heading, also
            // inside a list item. `docs/shipped-work.md` has many such headings.
            // A column-0 rule reports each link into one as a missing anchor.
            heading: pycompat::regex(r"(?m)^[ \t]{0,3}(#{1,6})\s+(.+?)\s*#*$")?,
            spaces: pycompat::regex(r"\s+")?,
            // A fence opens with 3 or more backticks or 3 or more tildes.
            fence_open: pycompat::regex(r"^[ \t]{0,3}(`{3,}|~{3,})")?,
        })
    }
}

/// Returns the length of the character at byte `at`, or 1 at the end.
fn char_len_at(text: &str, at: usize) -> usize {
    text[at..].chars().next().map_or(1, char::len_utf8)
}

/// Returns each match of `re` that no backslash escapes and that `accept` allows.
///
/// This is the Python `finditer` of a pattern that starts with `(?<!\\)`. A
/// refused match does not consume text, so the scan retries one character on.
fn find_matches<'h>(
    re: &Regex,
    text: &'h str,
    mut accept: impl FnMut(&Captures<'h>) -> bool,
) -> Vec<Captures<'h>> {
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
        if text[..start].ends_with('\\') || !accept(&caps) {
            pos = start + char_len_at(text, start);
            continue;
        }
        pos = if whole.end() > start {
            whole.end()
        } else {
            start + char_len_at(text, start)
        };
        found.push(caps);
    }
    found
}

/// Returns the text of capture group `i`, or an empty string.
fn group<'h>(caps: &Captures<'h>, i: usize) -> &'h str {
    caps.get(i).map_or("", |m| m.as_str())
}

/// Removes the angle brackets of a bracketed destination.
pub fn unwrap_angle_dest(target: &str) -> String {
    if target.len() >= 2 && target.starts_with('<') && target.ends_with('>') {
        target[1..target.len() - 1].to_owned()
    } else {
        target.to_owned()
    }
}

/// Normalizes a reference label for matching.
///
/// `CommonMark` label matching case-folds and collapses each run of inner
/// whitespace to one space. So `[x][foo bar]` matches a definition written
/// `[foo  bar]: ...`, also across a line wrap. A plain trim and lowercase
/// treats those as different keys and reports a defined reference as undefined.
fn normalize_ref_label(pats: &Patterns, label: &str) -> String {
    pats.spaces
        .replace_all(pycompat::strip(label), " ")
        .to_lowercase()
}

/// Returns the closing-fence test for a fence of `fence_len` characters `fence_char`.
///
/// A fence closes only on a run of the same character that is at least as
/// long as the opener. So a three-backtick example inside a four-backtick
/// fence does not close it early. Nor does a backtick fence inside a tilde one.
pub fn is_fence_close(line: &str, fence_char: char, fence_len: usize) -> bool {
    let indent = line.len() - line.trim_start_matches([' ', '\t']).len();
    if indent > 3 {
        return false;
    }
    let rest = &line[indent..];
    let run = rest.len() - rest.trim_start_matches(fence_char).len();
    run / fence_char.len_utf8() >= fence_len && rest[run..].chars().all(|c| c == ' ' || c == '\t')
}

/// Blanks every line of every fenced code block: open, body and close.
fn mask_fenced_lines(pats: &Patterns, lines: &[&str]) -> Vec<String> {
    let mut out = Vec::with_capacity(lines.len());
    let mut fence: Option<(char, usize)> = None;
    for line in lines {
        match fence {
            None => {
                if let Some(caps) = pats.fence_open.captures(line) {
                    let delim = group(&caps, 1);
                    fence = delim.chars().next().map(|c| (c, delim.len()));
                    out.push(String::new());
                } else {
                    out.push((*line).to_owned());
                }
            }
            Some((c, len)) => {
                if is_fence_close(line, c, len) {
                    fence = None;
                }
                out.push(String::new());
            }
        }
    }
    out
}

/// Returns the GitHub heading slug of a heading.
///
/// The rule lowercases the heading. It drops each character that is not a
/// word character, whitespace or a hyphen. Then it replaces each space with a
/// hyphen, one for one. Runs of spaces are not collapsed first. A dropped character, such as
/// the em dash in "runtime — file", leaves its spaces behind. So that heading
/// slugs to "runtime--file", with a double hyphen. A collapse before the
/// space pass gives single hyphens and reports each such anchor as broken.
/// This corpus has dozens of them, with em dashes or "/" as in "Signal /
/// approval gates".
pub fn slugify(heading: &str) -> String {
    pycompat::strip(heading)
        .to_lowercase()
        .chars()
        .filter(|&c| pycompat::is_word(c) || pycompat::is_space(c) || c == '-')
        .map(|c| if c == ' ' { '-' } else { c })
        .collect()
}

/// Reports whether `line` can be the text line of a Setext heading.
///
/// A list item, a table row or an ATX heading cannot be. That keeps an
/// ordinary `---` divider or table separator from reading as an underline for
/// unrelated prose above it.
fn setext_text(line: &str) -> bool {
    if line.is_empty() || line.chars().all(pycompat::is_space) || line.starts_with('#') {
        return false;
    }
    let body = pycompat::lstrip(line);
    let mut chars = body.chars();
    let first = chars.next();
    if matches!(first, Some('-' | '*' | '+')) {
        // The next line break also counts as whitespace after the marker.
        if chars.next().is_none_or(pycompat::is_space) {
            return false;
        }
    }
    first != Some('|')
}

/// Reports whether `line` is a Setext underline: `=` for H1, or 2 or more `-` for H2.
///
/// The underline can carry up to 3 leading spaces, the same allowance as an
/// ATX heading or a fence delimiter.
fn setext_underline(line: &str) -> bool {
    let indent = line.len() - line.trim_start_matches([' ', '\t']).len();
    if indent > 3 {
        return false;
    }
    let rest = &line[indent..];
    let Some(c) = rest.chars().next() else {
        return false;
    };
    if c != '=' && c != '-' {
        return false;
    }
    let run = rest.len() - rest.trim_start_matches(c).len();
    if c == '-' && run < 2 {
        return false;
    }
    rest[run..].chars().all(|ch| ch == ' ' || ch == '\t')
}

/// Returns the anchor slugs of each heading in `text`.
pub fn headings_in(pats: &Patterns, text: &str) -> HashSet<String> {
    // Fenced blocks are blanked, so a `#` comment or a divider in a snippet
    // is not a heading. The lines are blanked, not dropped. A dropped fence
    // pulls a paragraph and a later underline into false adjacency.
    let masked = mask_fenced_lines(pats, &pycompat::splitlines(text));
    let body = masked.join("\n");

    // ATX and Setext headings go in document order. GitHub numbers duplicate
    // slugs in the order the headings appear, whatever their syntax.
    let mut matches: Vec<(usize, String)> = pats
        .heading
        .captures_iter(&body)
        .filter_map(|caps| Some((caps.get(0)?.start(), group(&caps, 2).to_owned())))
        .collect();
    let mut offset = 0;
    let mut offsets = Vec::with_capacity(masked.len());
    for line in &masked {
        offsets.push(offset);
        offset += line.len() + 1;
    }
    let mut i = 0;
    while i + 1 < masked.len() {
        if setext_text(&masked[i]) && setext_underline(&masked[i + 1]) {
            matches.push((offsets[i], pycompat::strip(&masked[i]).to_owned()));
            i += 2;
        } else {
            i += 1;
        }
    }
    matches.sort_by_key(|pair| pair.0);

    let mut slugs = HashSet::new();
    let mut seen: HashMap<String, usize> = HashMap::new();
    for (_, title) in matches {
        let base = slugify(&title);
        let n = seen.get(&base).copied().unwrap_or(0);
        slugs.insert(if n == 0 {
            base.clone()
        } else {
            format!("{base}-{n}")
        });
        seen.insert(base, n + 1);
    }
    slugs
}

/// Removes each inline code span of one line.
///
/// A closer is a run of exactly as many backticks as the opener, not a run
/// that merely contains that many. Under `CommonMark`, one backtick and a later
/// two-backtick run make no code span at all. A naive back-reference finds
/// one backtick inside the longer run, so a real link turns into code and is
/// never checked. Here the opener takes its whole run, and the closer must be
/// a whole run of the same length.
fn strip_code_spans(line: &str) -> String {
    let chars: Vec<char> = line.chars().collect();
    let run_end = |from: usize| {
        let mut end = from;
        while end < chars.len() && chars[end] == '`' {
            end += 1;
        }
        end
    };
    let mut out = String::with_capacity(line.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '`' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        let open_end = run_end(i);
        let width = open_end - i;
        let mut j = open_end;
        let mut closer = None;
        while j < chars.len() {
            if chars[j] == '`' {
                let end = run_end(j);
                if end - j == width {
                    closer = Some(end);
                    break;
                }
                j = end;
            } else {
                j += 1;
            }
        }
        if let Some(end) = closer {
            i = end;
        } else {
            out.push('`');
            i += 1;
        }
    }
    out
}

/// Blanks fenced blocks and removes inline code spans.
///
/// Code spans often hold regex classes, JSON paths or Rust slices, such as
/// `[A-Za-z0-9_-]` or `["execution"]["id"]`. Each one looks like a
/// reference-style link to a regex without code-span rules. They go before the
/// link patterns run. Otherwise each such snippet reports as an undefined
/// reference, as several in `docs/shipped-work.md` once did.
pub fn strip_code_spans_and_fences(pats: &Patterns, text: &str) -> String {
    mask_fenced_lines(pats, &pycompat::splitlines(text))
        .iter()
        .map(|line| strip_code_spans(line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Reports whether a target is external or special, so the check skips it.
pub fn is_external_or_special(target: &str) -> bool {
    ["http://", "https://", "mailto:", "data:"]
        .iter()
        .any(|p| target.starts_with(p))
}

/// Returns the link targets and the undefined references of one stripped text.
///
/// Targets come from inline links, reference-style links and shortcut
/// references, each resolved to its URL or path. An undefined reference is
/// a `[text][ref]` whose `ref` has no definition in the file. It is a broken
/// link in its own right, the same as a dead path.
pub fn extract_link_targets(pats: &Patterns, text: &str) -> (Vec<String>, Vec<String>) {
    let mut defs: HashMap<String, String> = HashMap::new();
    let mut rest = String::with_capacity(text.len());
    let mut last = 0;
    for caps in pats.ref_def.captures_iter(text) {
        let Some(whole) = caps.get(0) else {
            continue;
        };
        rest.push_str(&text[last..whole.start()]);
        last = whole.end();
        // The definition line goes, so it cannot also match as prose.
        defs.insert(
            normalize_ref_label(pats, group(&caps, 1)),
            unwrap_angle_dest(group(&caps, 2)),
        );
    }
    rest.push_str(&text[last..]);
    let text = rest.as_str();

    let mut targets = Vec::new();
    let mut undefined = Vec::new();
    // Spans that an inline or reference-style match already claims.
    let mut consumed: Vec<(usize, usize)> = Vec::new();

    for caps in find_matches(&pats.link, text, |_| true) {
        targets.push(unwrap_angle_dest(group(&caps, 1)));
        if let Some(m) = caps.get(0) {
            consumed.push((m.start(), m.end()));
        }
    }
    // A local image source gets a missing-file check even inside the label of
    // an outer link. An image target has no anchor, so this adds only that check.
    for caps in find_matches(&pats.image, text, |_| true) {
        targets.push(unwrap_angle_dest(group(&caps, 1)));
    }
    for caps in find_matches(&pats.ref_use, text, |_| true) {
        let (label, reference) = (group(&caps, 1), group(&caps, 2));
        let key = normalize_ref_label(
            pats,
            if reference.is_empty() {
                label
            } else {
                reference
            },
        );
        if let Some(m) = caps.get(0) {
            consumed.push((m.start(), m.end()));
        }
        match defs.get(&key) {
            Some(target) => targets.push(target.clone()),
            None => undefined.push(group(&caps, 0).to_owned()),
        }
    }
    // The label half of `[text](url)` and `[text][ref]` is followed by `(` or
    // `[`. The shortcut pattern refuses that position, so it never doubles a match.
    let shortcut_ok = |caps: &Captures<'_>| {
        caps.get(0)
            .is_some_and(|m| !text[m.end()..].starts_with(['(', '[']))
    };
    for caps in find_matches(&pats.shortcut, text, shortcut_ok) {
        let Some(m) = caps.get(0) else {
            continue;
        };
        let (s, e) = (m.start(), m.end());
        if consumed.iter().any(|&(cs, ce)| cs < e && s < ce) {
            continue;
        }
        // An undefined shortcut is plain bracketed prose under CommonMark, so
        // it is not reported.
        if let Some(target) = defs.get(&normalize_ref_label(pats, group(&caps, 1))) {
            targets.push(target.clone());
            consumed.push((s, e));
        }
    }
    (targets, undefined)
}

/// Returns `docs/**/*.md` under `root`, sorted as Python sorts paths.
pub fn docs_markdown(root: &Path) -> Vec<PathBuf> {
    let docs = root.join("docs");
    if !docs.is_dir() {
        return Vec::new();
    }
    let mut files: Vec<PathBuf> = walkdir::WalkDir::new(&docs)
        .min_depth(1)
        .into_iter()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".md"))
        .map(walkdir::DirEntry::into_path)
        .collect();
    files.sort();
    files
}

/// Returns each corpus file: `docs/**/*.md`, then each present top-level source.
pub fn corpus_files(root: &Path) -> Vec<PathBuf> {
    let mut files = docs_markdown(root);
    for name in TOP_LEVEL_SOURCES {
        let p = root.join(name);
        if p.exists() {
            files.push(p);
        }
    }
    files
}

/// One broken link: the source page, the raw target and the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broken {
    /// The page that holds the link.
    pub source: PathBuf,
    /// The raw link target.
    pub target: String,
    /// Why the link is broken.
    pub reason: &'static str,
}

/// The outcome of one pass over the corpus links.
struct LinkScan {
    broken: Vec<Broken>,
    inbound: HashMap<PathBuf, HashSet<PathBuf>>,
}

/// Follows each link of each corpus file.
///
/// With `check` false, the pass only builds the inbound-link graph, as the
/// Python `compute_corpus_reachable` did. It then reads no anchor target.
fn scan_links(pats: &Patterns, root: &Path, files: &[PathBuf], check: bool) -> Result<LinkScan> {
    let file_set: HashSet<&PathBuf> = files.iter().collect();
    let mut headings: HashMap<PathBuf, HashSet<String>> = HashMap::new();
    let mut broken = Vec::new();
    let mut inbound: HashMap<PathBuf, HashSet<PathBuf>> =
        files.iter().map(|p| (p.clone(), HashSet::new())).collect();
    let mut report = |source: &Path, target: &str, reason: &'static str| {
        broken.push(Broken {
            source: source.to_path_buf(),
            target: target.to_owned(),
            reason,
        });
    };

    for src in files {
        let text = strip_code_spans_and_fences(pats, &pycompat::read_text_lossy(src)?);
        let (targets, undefined) = extract_link_targets(pats, &text);
        if check {
            for raw in &undefined {
                report(src, raw, "undefined reference");
            }
        }
        let parent = src.parent().unwrap_or(root);
        for target in &targets {
            if is_external_or_special(target) {
                continue;
            }
            let (path_part, anchor) = target.split_once('#').unwrap_or((target.as_str(), ""));
            let mut resolved = if path_part.is_empty() {
                // A same-file anchor, such as `[x](#some-heading)`.
                src.clone()
            } else {
                pycompat::resolve(&parent.join(path_part))
            };
            if !path_part.is_empty() {
                if !resolved.starts_with(root) {
                    // A target full of `../` can walk out of the checkout, as
                    // `../../../../etc/passwd` does. It can exist on the machine
                    // by coincidence, but GitHub never resolves it. It is not a
                    // path in this repository, so it is flagged before `exists`.
                    if check {
                        report(src, target, "escapes repository root");
                    }
                    continue;
                }
                if !resolved.exists() {
                    if check {
                        report(src, target, "missing file");
                    }
                    continue;
                }
                if resolved.is_dir() {
                    // GitHub renders a directory link, such as `dashboards/`,
                    // as the README of that directory. So the README gets the
                    // inbound link. Otherwise a page that only a directory
                    // link reaches reads as an orphan.
                    let readme = resolved.join("README.md");
                    if file_set.contains(&readme) {
                        resolved = readme;
                    }
                }
                if file_set.contains(&resolved) {
                    inbound
                        .entry(resolved.clone())
                        .or_default()
                        .insert(src.clone());
                }
            }
            if check && !anchor.is_empty() {
                if !headings.contains_key(&resolved) {
                    let found = headings_in(pats, &pycompat::read_text_lossy(&resolved)?);
                    headings.insert(resolved.clone(), found);
                }
                if !headings.get(&resolved).is_some_and(|s| s.contains(anchor)) {
                    report(src, target, "missing anchor");
                }
            }
        }
    }
    Ok(LinkScan { broken, inbound })
}

/// Reports whether `path` sits under a process-artifact prefix.
fn under_process_prefix(path: &Path, root: &Path) -> bool {
    let rel = pycompat::rel(path, root);
    PROCESS_ARTIFACT_PREFIXES.iter().any(|p| rel.starts_with(p))
}

/// Returns the pages a reader can reach from the real corpus.
///
/// A page under a process-artifact prefix counts as process only when no real
/// markdown link from the corpus reaches it. The prefix alone is not enough.
/// `docs/rnd/determinism-static-analysis.md` is required reading, linked with
/// anchors from `docs/harvest-verify.md` and
/// `docs/workflow-determinism-guide.md`. The same holds for
/// `docs/rnd/sqlite-feasibility.md`, linked from `docs/sqlite-backend.md`.
/// Corpus pages send readers there on purpose, so those targets are corpus.
/// So is each page that they link to in turn, because a reader keeps
/// following links. That is why `docs/rnd/wasm-activities-spike.md` counts,
/// one hop further out, through `docs/shipped-work.md`.
///
/// Some process pages are only cited as backtick-quoted paths in prose, never
/// as a link that a reader can click. Examples are
/// `docs/assays/0001-redis-adapter-throughput-ceiling.md` and
/// `docs/plans/2026-09-01-e2e-benchmark-suite.md`. They correctly stay exempt.
/// A citation without a link is a findability defect too, but a different one.
///
/// The set is a reachability closure seeded from each non-process page. A
/// hand-kept exception list goes stale the next time a link changes.
fn reachable_from(
    root: &Path,
    files: &[PathBuf],
    inbound: &HashMap<PathBuf, HashSet<PathBuf>>,
) -> HashSet<PathBuf> {
    let docs = root.join("docs");
    let top_level: HashSet<PathBuf> = TOP_LEVEL_SOURCES
        .iter()
        .map(|n| root.join(n))
        .filter(|p| p.exists())
        .collect();
    let mut reachable: HashSet<PathBuf> = files
        .iter()
        .filter(|p| {
            (p.starts_with(&docs) || top_level.contains(*p)) && !under_process_prefix(p, root)
        })
        .cloned()
        .collect();
    let mut worklist: Vec<PathBuf> = reachable.iter().cloned().collect();
    while let Some(cur) = worklist.pop() {
        for (target, sources) in inbound {
            if sources.contains(&cur) && !reachable.contains(target) {
                reachable.insert(target.clone());
                worklist.push(target.clone());
            }
        }
    }
    reachable
}

/// Returns the subset of `files` that a reader can reach.
///
/// That is each non-process-artifact page, plus each process-artifact page
/// that one of them links to, transitively. The other docs audits share this
/// one definition of "process artifact". A copy of only the prefix half drifts
/// from the closure that this audit applies. `config-cli-drift` once had such
/// a copy (PR #1373).
pub fn compute_corpus_reachable(root: &Path, files: &[PathBuf]) -> Result<HashSet<PathBuf>> {
    let pats = Patterns::new()?;
    let scan = scan_links(&pats, root, files, false)?;
    Ok(reachable_from(root, files, &scan.inbound))
}

/// The findings of one full run.
pub struct Report {
    /// The number of files scanned.
    pub files_scanned: usize,
    /// Broken links in the reader corpus. These fail the run.
    pub broken_corpus: Vec<Broken>,
    /// Broken links in process artifacts. These are reported only.
    pub broken_process: Vec<Broken>,
    /// Pages under `docs/` with no inbound link.
    pub orphans: Vec<PathBuf>,
}

/// Runs the link check and the orphan scan over the corpus under `root`.
pub fn check(root: &Path) -> Result<Report> {
    let pats = Patterns::new()?;
    let files = corpus_files(root);
    let scan = scan_links(&pats, root, &files, true)?;
    let reachable = reachable_from(root, &files, &scan.inbound);
    let is_process = |p: &Path| under_process_prefix(p, root) && !reachable.contains(p);
    let docs = root.join("docs");
    let orphans = files
        .iter()
        .filter(|p| {
            p.starts_with(&docs)
                && scan.inbound.get(*p).is_none_or(HashSet::is_empty)
                && !is_process(p)
        })
        .cloned()
        .collect();
    let (broken_process, broken_corpus): (Vec<Broken>, Vec<Broken>) =
        scan.broken.into_iter().partition(|b| is_process(&b.source));
    Ok(Report {
        files_scanned: files.len(),
        broken_corpus,
        broken_process,
        orphans,
    })
}

/// Renders a report as the Python script printed it.
fn render(report: &Report, root: &Path, json: bool) -> String {
    let rel = |p: &Path| pycompat::rel(p, root);
    let entries = |list: &[Broken]| {
        Json::Arr(
            list.iter()
                .map(|b| {
                    Json::Obj(vec![
                        ("source".into(), Json::Str(rel(&b.source))),
                        ("target".into(), Json::Str(b.target.clone())),
                        ("reason".into(), Json::Str(b.reason.into())),
                    ])
                })
                .collect(),
        )
    };
    if json {
        let value = Json::Obj(vec![
            ("files_scanned".into(), Json::Int(report.files_scanned)),
            ("broken_links_corpus".into(), entries(&report.broken_corpus)),
            (
                "broken_links_process_artifacts".into(),
                entries(&report.broken_process),
            ),
            (
                "orphans".into(),
                Json::strings(report.orphans.iter().map(|p| rel(p))),
            ),
        ]);
        return value.render() + "\n";
    }
    let mut out = format!(
        "Folio corpus link check — {} files scanned\n\n",
        report.files_scanned
    );
    let _ = writeln!(
        out,
        "Broken links (corpus, fails CI): {}",
        report.broken_corpus.len()
    );
    for b in &report.broken_corpus {
        let _ = writeln!(out, "  {}: ({}) — {}", rel(&b.source), b.target, b.reason);
    }
    let _ = writeln!(
        out,
        "\nBroken links (process artifacts — plans/design/changelog.d/rnd/assays/perf-artifacts, reported only): {}",
        report.broken_process.len()
    );
    for b in &report.broken_process {
        let _ = writeln!(out, "  {}: ({}) — {}", rel(&b.source), b.target, b.reason);
    }
    let _ = writeln!(
        out,
        "\nOrphan pages (0 inbound links): {}",
        report.orphans.len()
    );
    for p in &report.orphans {
        let _ = writeln!(out, "  {}", rel(p));
    }
    out
}

/// Runs `cargo xtask audit corpus-link-check`.
#[expect(
    clippy::needless_pass_by_value,
    reason = "every command takes its `Args` by value, so each dispatch arm has one shape"
)]
pub fn run(args: Args) -> Result<ExitCode> {
    let root = pycompat::resolve(&util::repo_root());
    let report = check(&root)?;
    print!("{}", render(&report, &root, args.json));
    Ok(util::gate_exit(report.broken_corpus.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pats() -> Patterns {
        Patterns::new().unwrap()
    }

    fn targets(text: &str) -> (Vec<String>, Vec<String>) {
        let p = pats();
        extract_link_targets(&p, &strip_code_spans_and_fences(&p, text))
    }

    #[test]
    fn slugs_follow_the_github_rule() {
        assert_eq!(slugify("Runtime — file"), "runtime--file");
        assert_eq!(slugify("Signal / approval gates"), "signal--approval-gates");
        assert_eq!(slugify("  `code` and **bold** "), "code-and-bold");
        assert_eq!(slugify("Ünïcode Straße 😀 ok"), "ünïcode-straße--ok");
        assert_eq!(slugify("snake_case-name"), "snake_case-name");
        assert_eq!(slugify("Tab\there"), "tab\there");
    }

    #[test]
    fn duplicate_headings_get_suffixes_in_document_order() {
        let text = "# Intro\n\nIntro\n=====\n\n## Intro\n```\n# Intro\n```\n";
        let slugs = headings_in(&pats(), text);
        let mut got: Vec<_> = slugs.into_iter().collect();
        got.sort();
        assert_eq!(got, ["intro", "intro-1", "intro-2"]);
    }

    #[test]
    fn headings_allow_indent_and_closing_hashes() {
        let slugs = headings_in(&pats(), "   ### What shipped ###\n    #### Code\n");
        assert!(slugs.contains("what-shipped"));
        assert!(!slugs.contains("code"));
    }

    #[test]
    fn setext_ignores_list_items_and_tables() {
        let slugs = headings_in(&pats(), "- item\n---\n| a |\n---\nReal\n--\n");
        let got: Vec<_> = slugs.into_iter().collect();
        assert_eq!(got, ["real"]);
    }

    #[test]
    fn link_forms_are_extracted() {
        let text = "[a](x.md) [b](<y z.md> 'T') [![i](img.png)](dest.md)\n\
                    [c][ref] [ref] [d][] \\[e](skip.md) [x](guide(v2).md)\n\
                    [ref]: target.md#h\n[d]: <d d.md>\n";
        let (t, undefined) = targets(text);
        assert_eq!(
            t,
            [
                "x.md",
                "y z.md",
                "dest.md",
                "guide(v2).md",
                "img.png",
                "target.md#h",
                "d d.md",
                "target.md#h"
            ]
        );
        assert_eq!(undefined, [] as [std::string::String; 0]);
    }

    #[test]
    fn undefined_references_and_code_spans() {
        let (t, undefined) = targets("[x][nope] `[a][b]` [FAILED]\n```\n[c](d.md)\n```\n");
        assert_eq!(t, [] as [std::string::String; 0]);
        assert_eq!(undefined, ["[x][nope]"]);
        // One backtick and a later two-backtick run make no code span.
        let (t, _) = targets("`[x](y.md)`` tail\n");
        assert_eq!(t, ["y.md"]);
    }

    #[test]
    fn reference_labels_collapse_whitespace() {
        let (t, undefined) = targets("[x][Foo  Bar]\n\n[foo bar]: a.md\n");
        assert_eq!(t, ["a.md"]);
        assert_eq!(undefined, [] as [std::string::String; 0]);
    }

    #[test]
    fn fence_close_needs_the_same_character_and_length() {
        assert!(is_fence_close("````  ", '`', 4));
        assert!(!is_fence_close("```", '`', 4));
        assert!(!is_fence_close("~~~~", '`', 3));
        assert!(!is_fence_close("    ```", '`', 3));
        assert!(!is_fence_close("``` x", '`', 3));
    }

    fn fixture(files: &[(&str, &str)]) -> smop::fs::TempDir {
        let dir = smop::fs::temp_dir().unwrap();
        for (path, body) in files {
            let full = dir.path().join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(full, body).unwrap();
        }
        dir
    }

    #[test]
    fn a_fixture_corpus_reports_broken_links_and_orphans() {
        let dir = fixture(&[
            ("README.md", "[a](docs/a.md) [p](docs/plans/p.md)\n"),
            (
                "docs/a.md",
                "# A\n[b](b.md#b-title) [gone](gone.md) [h](#nope)\n[up](../../x.md)\n",
            ),
            ("docs/b.md", "# B title\n"),
            ("docs/orphan.md", "# O\n"),
            ("docs/plans/p.md", "[bad](missing.md)\n"),
            ("docs/plans/q.md", "[bad](missing.md)\n"),
            ("docs/dash/README.md", "[a](../a.md)\n"),
            ("docs/c.md", "[d](dash/)\n"),
        ]);
        let root = pycompat::resolve(dir.path());
        let report = check(&root).unwrap();
        let text = render(&report, &root, false);
        assert_eq!(report.files_scanned, 8);
        let reasons: Vec<_> = report
            .broken_corpus
            .iter()
            .map(|b| (b.target.as_str(), b.reason))
            .collect();
        assert_eq!(
            reasons,
            [
                ("gone.md", "missing file"),
                ("#nope", "missing anchor"),
                ("../../x.md", "escapes repository root"),
                ("missing.md", "missing file"),
            ]
        );
        assert_eq!(report.broken_process.len(), 1);
        let orphans: Vec<_> = report
            .orphans
            .iter()
            .map(|p| pycompat::rel(p, &root))
            .collect();
        assert_eq!(orphans, ["docs/c.md", "docs/orphan.md"]);
        assert!(text.starts_with("Folio corpus link check — 8 files scanned\n\n"));
        assert!(text.contains("  docs/a.md: (gone.md) — missing file\n"));
        let json = render(&report, &root, true);
        assert!(json.contains("\"orphans\": [\n    \"docs/c.md\",\n    \"docs/orphan.md\"\n  ]"));
    }
}
