#!/usr/bin/env python3
"""Folio corpus harness: Rust syntax check for getting-started code blocks.

Deterministic, reproducible on any checkout — no network access required.
Uses only `rustfmt`, which ships with the toolchain already installed for
this workspace; it never invokes `cargo` and never touches the network or
`target/`.

## What this checks, and why

`docs/getting-started/*.md` and `README.md` are the front door: Chapter 1's
own promise is "no database, no Docker, and nothing to configure" for the
fastest path, and every later chapter's ```rust fences are the snippets a
reader on that path copies into their editor. Before this script, nothing
in the corpus verified that those fences are even valid Rust — the audits
in this directory check links, config/CLI drift, and comment hygiene, but
none of them look inside a ```rust fence (see docs/audits/README.md's
table). A snippet that does not parse breaks the very first thing a reader
tries: pasting it.

This is a SYNTAX check, not a compile check. It does not resolve `use`
imports, does not know whether a type exists, and does not run the borrow
checker — doing that would require a real `cargo build` against a
scratch crate for every combination of chapter-local context (imports
established two chapters earlier, `autumn-web` vs. connector-feature
dependencies for chapters 1/12/13, etc.), which is exactly the kind of
network- and compile-time-heavy check this directory's audits are
deliberately not (see docs/audits/README.md's "no network access
required"). A bare parse failure is still a real, mechanical defect class:
`rustc`'s lexer/parser rejects the snippet outright, before any question of
imports or types arises, so it is a strict subset of "would fail to
compile" — zero false positives from missing `use` statements or
undefined example types, at the cost of not catching type errors.

## How a block is checked

Extraction is line-oriented, not a single multi-line regex: a fence is any
line whose stripped content opens with "```rust", however far it is
indented under a list item, and the matching close is the next line whose
stripped content is exactly "```". An opener with no close before EOF is a
hard error (a silently unmatched fence would drop a block from the
"corpus" count without saying so). An `ignore` info-string annotation (for
example ```` ```rust,ignore ````) does NOT exempt a block here — only the
named entries in KNOWN_FRAGMENT_EXCEPTIONS below do, since `ignore` marks a
block skipped by `rustdoc`, not a block whose Rust syntax stopped mattering.

Each ```rust fenced block is wrapped as the body of its own function,
`fn __snippet() { <block content> }`, and handed to `rustfmt --emit
stdout` **on its own** — one subprocess call per block, not one call per
file for the whole concatenated corpus. A single shared buffer was tried
first and dropped: `rustfmt` reports a parse error at whatever line its
recovery lands on, which for an unclosed brace or an unterminated string
can be past the block that caused it — even past EOF of the buffer — so
attributing an error back to "the block whose header line is the closest
one at or before the error line" silently credited the fault to a later
block, including an excepted one, and dropped it. Checking one block per
process is the simplest fix that cannot misattribute: there is only ever
one block in scope, so any diagnostic is unambiguously its own. Wrapping
in a function body — rather than parsing as a sequence of top-level items
— is deliberate: a fn body accepts local item declarations (`#[workflow]
async fn ...`, `struct ...`, `use ...`) AND bare statements/expressions in
the same scope, so one wrapper handles both a complete workflow definition
and a short expression fragment without needing to first classify which
kind of block it is. `rustfmt` fails on a genuine parse error (`error:
expected expression, found ...`) and exits 0 on anything syntactically
valid, whether or not it was already formatted — this script only cares
about the former.

## Known, documented exceptions

A handful of blocks are deliberately not complete, parseable Rust: prose
like "add this line to your existing builder chain" shows a bare
`.method(...)` continuation with no receiver, which is correct technical
writing and not a defect. Each is listed in KNOWN_FRAGMENT_EXCEPTIONS below
with the reason, matched by a stable prefix of the block's own first
content line (not a line number, so a later edit elsewhere in the file
does not silently stop covering it). A block matched here is still counted
and printed in the summary as excluded, never silently dropped.

Usage:
    python3 docs/audits/doc-snippet-syntax.py

Exit code is 1 if any non-excepted block fails to parse, 0 otherwise —
safe to wire into CI as a gate, or run standalone as a report.
"""
import glob
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

TARGET_GLOBS = [
    "docs/getting-started/*.md",
    "README.md",
]

# LEAD_INDENT is the fence's own plain indentation, consumed exactly ONCE
# regardless of whether a list marker follows — not once before an optional
# marker AND once again after, which would let two separate 3-character
# budgets stack into 6 characters of indent for a plain (markerless) line,
# well past CommonMark's actual per-container cap.
#
# BQ is one level of Markdown blockquote marker (">") plus its own trailing
# whitespace, repeated for however many "> " a nested blockquote carries —
# captured (with LEAD_INDENT) so the same text can be stripped from the
# block's content lines too, since a blockquote's "> " is Markdown syntax,
# not part of the Rust source, and left in place would break every
# blockquoted snippet's parse regardless of whether its actual code is
# valid.
#
# BQ's own trailing run is capped at 4 characters, not 3: CommonMark
# consumes at most ONE character right after ">" as the marker's own
# optional padding (deterministically, if present — not a choice), and
# only what follows THAT is the blockquote's inner content, subject to the
# usual 3-column fence-indent budget fresh from there. So up to 1 (padding)
# + 3 (budget) = 4 total characters/columns after ">" can still be a valid
# fence — capping the whole run at 3 like a bare LEAD_INDENT would reject a
# legitimate "> " (with padding) followed by a further 3 columns of indent.
# _valid_bq_padding below does the actual budget check (1 mandatory-if-
# present padding char peeled off, then the usual 3-column check on the
# rest); the regex here only needs to admit enough characters for that
# check to have something to work with.
#
# LEAD_INDENT itself — and, after peeling BQ's own padding, what
# _valid_bq_padding checks — is capped at 3 COLUMNS, not 3 characters: a
# single leading tab expands to a 4-column stop and so already exceeds the
# budget by itself, which `{0,3}` alone (a character count) would not
# catch. `docs/audits/comment-hygiene.py` already implements this exact
# CommonMark rule (see its fence_delimiter, which validates with the
# equivalent of `text.expandtabs(4)`, and its own fixture tests around
# "four spaces is an indented code line, not a fence opener" / "an
# over-indented \`\`\` inside a fence is content, not a closer") for the
# same reason — indent past the container's budget makes a
# delimiter-looking line indented CONTENT, not a fence marker, opener or
# closer alike. The regexes below still only bound each run to a character
# count (a plain, tab-free run within that count is always within budget);
# _valid_indent/_valid_bq_padding additionally reject a run whose
# tab-expanded width exceeds budget, since regex alone cannot do that
# arithmetic.
LEAD_INDENT = r"[ \t]{0,3}"
BQ = r">[ \t]{0,4}"
#
# LIST_MARKER additionally allows a fence to be the first block of a list
# item, opening on the marker's own line ("- ```rust", "1. ```rust") rather
# than on a line under it — valid CommonMark, and unlike the marker's own
# text there is no per-line repeat of it to strip back out of content lines,
# so those are left to the plain indentation-stripping case above. It is
# never legal on a CLOSING fence line, which must consist of nothing but
# its container's indentation/blockquote markers and the delimiter itself —
# a line like "- ```" inside a still-open block is literal fenced content,
# not a closer. An ordered marker is 1-9 ASCII digits (CommonMark's own
# limit — a ten-digit run like "1234567890." is not a list marker at all,
# just prose that happens to start with digits) followed by "." or ")";
# `[0-9]`, not `\d`, so a Unicode digit character does not also qualify.
#
# This is deliberately not a full CommonMark container parser — this
# script trades that for staying dependency-free: docs/audits/*.py runs
# with no network access, so no markdown-parsing package can be installed
# to do this properly, and a hand-rolled line scanner is what stays within
# that. Three specific, known gaps from that trade:
#
# 1. A fence on a list item's own CONTINUATION line, not the marker's own
#    line, needs that item's content column carried over from wherever the
#    marker actually was — this script has no cross-line container stack,
#    so it only recognizes a marker's width when the marker is on the
#    fence's own opening line (LIST_MARKER above). Needs state spanning
#    lines OUTSIDE the fence's own span.
# 2. Whether an ordered marker (other than "1.") may open a list at all
#    depends on whether it interrupts an open paragraph — this script has
#    no paragraph-state tracking, so every marker that otherwise matches
#    LIST_MARKER is accepted regardless of what precedes it. Also needs
#    state spanning lines outside the fence's own span.
# 3. Containers can nest in ANY order — "> - \`\`\`rust" (list inside
#    blockquote) is as valid as "- > \`\`\`rust" (blockquote inside list,
#    which this script does handle). FENCE_OPEN_RE hard-codes list markers
#    before blockquote markers (group 2 then group 3): fully local to the
#    opener's own line, unlike 1 and 2 above, but supporting arbitrary
#    interleaving means replacing "all list markers, then all blockquote
#    markers" with an ordered token sequence — and every downstream piece
#    that currently assumes that fixed shape (_blockquote_depth,
#    _fence_indent_valid, the content-line stripping in find_rust_blocks)
#    would need to walk that same sequence instead of two flat groups.
#    That is a rewrite of this script's core model, not a local fix.
# All three are confirmed absent from the current corpus (grepped for a
# multi-digit/wide marker followed by an indented fence, for a non-"1."
# ordered marker anywhere near a fence, and for a blockquote marker
# followed by a list marker before a fence — all three come up empty) —
# if any shows up for real, this is where to extend the scanner, most
# likely by adopting the container-stack approach
# `docs/audits/comment-hygiene.py`'s own fence_delimiter/strip_containers
# already use for the same problem in a different context (Rust doc
# comments rather than raw Markdown).
#
# The list marker is captured separately (group 2) from the indentation/
# blockquote text around it (groups 1 and 3) because only the latter repeats
# on every content line of the container — "- " never appears again after
# the fence's own opening line, but "> " does, on every line of a
# blockquote. Stripping the marker back out of content lines would be
# wrong; not stripping the indent/blockquote text would leave Markdown
# syntax inside the Rust source handed to `rustfmt`.
#
# The marker's own trailing `\s+` is unbounded in the regex, but CommonMark
# only lets 1-4 columns of it count as padding (absorbed into the list
# item's content column) — 5 or more means just ONE column separates marker
# from content, per comment-hygiene.py's own list_content. This can only
# ever make a same-line fence invalid, never valid: whatever is left after
# that single column is the delimiter's own indent relative to the list
# item's real content start, and 5 padding columns minus the 1 that counts
# is already 4 — past the fence's own 3-column budget by construction, so
# _list_marker_padding_valid below simply requires the whole run to be 1-4
# columns; nothing past 4 could pass the indent budget anyway.
#
# The delimiter (group 4) is 3+ backticks or 3+ tildes, matching CommonMark:
# a closing fence must use the same character and be at least as long as
# the opener's, so a run of 4 is also captured and compared, not just
# matched literally against exactly 3. Group 5 is the raw info string,
# checked in Python (not baked into the regex as a literal "rust") so a
# non-Rust fence can still be recognized AS a fence — its span tracked and
# skipped whole — rather than left invisible to the scanner: an invisible
# non-Rust fence containing a line that merely looks like a Rust fence
# opener (a Markdown-about-Markdown example, say) would otherwise be misread
# as a real one.
# A fence can open inside SEVERAL list containers stacked on one line, not
# just one — "- 1. ```rust" is a bullet item containing an ordered item
# containing the fence, both containers opening together (comment-
# hygiene.py's own fixture: "two list markers on one line both open
# containers for the fence"). MARKER_GLYPH/LIST_MARKER are split so the
# repeated group below can match any number of them, and so each one's own
# padding can still be checked independently afterward. Unlike the
# cross-line gaps noted above, this needs no state beyond the current
# line, so it stays in scope.
MARKER_GLYPH = r"(?:[-*+]|[0-9]{1,9}[.)])"
LIST_MARKER = rf"{MARKER_GLYPH}\s+"
FENCE_DELIM = r"(`{3,}|~{3,})"
FENCE_OPEN_RE = re.compile(
    rf"^({LEAD_INDENT})((?:{LIST_MARKER})*)((?:{BQ})*){FENCE_DELIM}(.*)$"
)
FENCE_CLOSE_RE = re.compile(rf"^({LEAD_INDENT}(?:{BQ})*){FENCE_DELIM}\s*$")


def _expand_column(text: str, start_col: int) -> int:
    """The absolute column reached after `text`, starting at `start_col`,
    expanding tabs to 4-column stops. `str.expandtabs` always measures as
    though its input starts at column 0; a tab's width depends on the
    column it actually starts at (a tab at column 3 reaches column 4, one
    at column 4 reaches column 8), so expanding an isolated substring on
    its own under- or over-counts unless it truly begins the line.
    """
    col = start_col
    for ch in text:
        col = col + 4 - (col % 4) if ch == "\t" else col + 1
    return col


def _valid_indent(text: str) -> bool:
    """True if pure-indentation `text` is within the 3-column fence budget
    once tabs are expanded to 4-column stops. A literal character count
    (what `{0,3}` alone gives the regex) under-counts a tab, which reaches
    the next stop, and so can exceed the budget in a single character.
    """
    return len(text.expandtabs(4)) <= 3


def _valid_bq_padding(text: str) -> bool:
    """True if the whitespace right after a single ">" is within budget.
    Unlike a bare lead indent, up to 4 characters can be valid here: the
    first one (if present) is the blockquote marker's own optional padding
    — CommonMark consumes it as part of the marker itself, not as indent —
    and only what remains after it counts against the usual 3-column fence
    budget.
    """
    return not text or _valid_indent(text[1:])


def _fence_indent_valid(prefix: str) -> bool:
    """True if every indentation run in a captured fence prefix is within
    budget: the lead indent before any blockquote marker (a flat 3-column
    budget), and each ">"'s own trailing indent (up to 4, one of which may
    be the marker's own padding). Splitting on ">" recovers each run
    whether `prefix` is an opener's lead+trail or a closer's single
    combined group.
    """
    chunks = prefix.split(">")
    return _valid_indent(chunks[0]) and all(_valid_bq_padding(c) for c in chunks[1:])


def _list_marker_padding_valid(marker: str, start_col: int) -> bool:
    """True if EVERY list marker glyph in a captured marker run — possibly
    several stacked on one line, as in "- 1. " — is followed by 1-4 COLUMNS
    of padding, CommonMark's allowance checked independently per container
    since each one opens on its own. `start_col` is the true column the
    marker text itself begins at (after any lead indent); a glyph is never
    a tab, so it always advances the column by its own character length,
    but the padding after it is measured with _expand_column from that
    running position — measuring the padding substring on its own (as a
    naive `expandtabs(4)` would) silently resets tab math to column 0 and
    can under-count a tab's true width. Empty `marker` (no marker at all)
    is trivially fine.
    """
    col = start_col
    for m in re.finditer(rf"({MARKER_GLYPH})(\s+)", marker):
        col += len(m.group(1))
        end_col = _expand_column(m.group(2), col)
        if not (1 <= end_col - col <= 4):
            return False
        col = end_col
    return True


def _blockquote_depth(prefix: str) -> int:
    """Counts ">" markers in a captured fence prefix, to tell a closing fence
    at the opener's own blockquote depth from an unrelated one deeper or
    shallower — for example a plain, unquoted "```" that happens to follow a
    "> ```rust" opener, which is not that opener's close. A list marker does
    not similarly need tracking: unlike ">", it is not repeated on every line
    of the container, so it never appears on a closing fence line at all.
    """
    return prefix.count(">")


def _quote_prefix_present(line: str, depth: int) -> bool:
    """True if `line` still carries at least `depth` levels of blockquote
    marker. CommonMark has no lazy continuation inside a fenced code block
    (unlike a paragraph, which can continue without repeating the marker):
    a line that drops the required ">" ends the blockquote there, and any
    fence still open inside it along with it — not merely leaves that
    fence's own closer unfound many lines later. `depth == 0` (no
    blockquote at all — the overwhelming common case) is trivially always
    present.
    """
    if depth == 0:
        return True
    return re.match(rf"^{LEAD_INDENT}(?:{BQ}){{{depth},}}", line) is not None


def _list_prefix_present(line: str, marker_width: int) -> bool:
    """True if `line` is still indented to at least the list item's own
    content column (`marker_width`, 0 when the fence didn't open on a
    marker's own line — trivially always present then). Unlike the sibling
    blockquote check above (a blank line there DOES end it — comment-
    hygiene.py's own fixtures give each container its own answer: "an
    unclosed fence ends when its block quote does" right next to "a blank
    line does not end the list item a fence sits in"), a list item
    tolerates an unindented blank line without ending — CommonMark lets a
    list item's blocks be separated by blank lines the same way top-level
    blocks are, so one appearing (still indented or not) inside an open
    fence is just blank fence content, not a dedent out of the item.
    """
    if line.strip() == "":
        return True
    col = 0
    for ch in line:
        if not ch.isspace():
            break
        col = _expand_column(ch, col)
    return col >= marker_width


# The only edition this corpus ever tells a reader to use: chapter 1's
# Cargo.toml block pins `edition = "2021"` for the tutorial project every
# later chapter (and README.md) builds on. Checking snippets against a
# different edition risks both false positives (valid 2021 code rejected
# for using an edition-2024-only reserved word like `gen`) and false
# negatives (a snippet only valid in 2024 waved through as fine for readers
# on the documented 2021 setup).
RUST_EDITION = "2021"

# (file, first-content-line-prefix) -> reason. The prefix is matched against
# the block's first non-blank, non-comment line, stripped. Verified against
# this file's own snippet_syntax baseline run before being added here.
KNOWN_FRAGMENT_EXCEPTIONS = {
    (
        "docs/getting-started/05-child-workflows.md",
        ".workflows(workflows![checkout, issue_invoice])",
    ): "deliberate partial excerpt: \"add this line to your existing "
    "builder chain\", not a standalone statement",
    (
        "docs/getting-started/10-operations.md",
        ".workflows(vec![",
    ): "deliberate partial excerpt: \"the same builder is available "
    "without the macro\", a continuation line, not a standalone statement",
    (
        "docs/getting-started/13-broker-connectors.md",
        ".map_json(|ctx, event: Reading| {",
    ): "deliberate partial excerpt: \"the key is on the mapping "
    "context\", a continuation line, not a standalone statement",
}


def first_content_line(code: str) -> str:
    for line in code.split("\n"):
        s = line.strip()
        if s and not s.startswith("//"):
            return s
    return ""


def collect_files() -> list[str]:
    files: set[str] = set()
    for pattern in TARGET_GLOBS:
        files.update(glob.glob(pattern, root_dir=REPO_ROOT))
    return sorted(files)


def _strip_blockquote(line: str, depth: int) -> str:
    """Strips `depth` levels of blockquote marker from the start of `line`.

    Structural, not textual: each level is arbitrary leading whitespace, a
    ">", and at most one following space/tab — CommonMark's own grammar for
    a blockquote marker, and permissive about how one is spelled. A literal-
    prefix comparison against the opener's exact spelling would miss this:
    "> > ```rust" opens the same depth-2 blockquote as ">> ```rust" (no
    space between the markers), but a body line spelled the other way from
    its opener shares no common leading substring with it at all.
    """
    for _ in range(depth):
        stripped = line.lstrip(" \t")
        if not stripped.startswith(">"):
            break
        stripped = stripped[1:]
        if stripped[:1] in (" ", "\t"):
            stripped = stripped[1:]
        line = stripped
    return line


def _strip_marker_width(line: str, width: int) -> str:
    """Strips leading whitespace from `line` up to `width` COLUMNS, not
    characters.

    A fence opened on its list marker's own line ("- ```rust") leaves no
    marker text on later content lines, but CommonMark still indents those
    lines by the marker's own display width ("- " is 2 columns) to keep them
    inside the list item — plain alignment spaces, not part of the Rust
    source. A character count under-strips when the marker itself contained
    a tab (its width in columns exceeds its length in characters), leaving
    genuine alignment whitespace sitting in front of otherwise-correct
    content. Stops at the first non-whitespace character, or once `width`
    columns are consumed, rather than always removing exactly `width`
    characters, so a shorter or blank line is not corrupted.
    """
    col = 0
    n = 0
    for ch in line:
        if col >= width or not ch.isspace():
            break
        col = _expand_column(ch, col)
        n += 1
    return line[n:]


def find_rust_blocks(text: str, relpath: str) -> list[str]:
    """Returns the code of every ```rust fenced block in `text`, in order.

    Line-oriented so an indented or blockquoted fence (a code block nested
    under a list item or a "> " note) is still found — a single-regex scan
    anchored on an unindented close silently drops those. A list marker's
    own display width (its "10. " is 4 columns, not the usual "- "'s 2) is
    stripped from every line of its container, closer included, before that
    line is measured against the 3-column indent budget or matched for
    blockquote depth — the budget is relative to the list item's own
    content column, not to column 0, per CommonMark. `depth` levels of
    blockquote marker (">", with at most one following space, however many
    columns of whitespace lead into it) are then stripped structurally from
    each content line, not by comparing it against the opener's own exact
    spelling — a nested "> > ```rust" and a body written as ">> let x = 1;"
    (no space between the markers) are the same depth-2 blockquote, but
    share no literal common prefix to fall back on. A closing fence only
    ends the block if it is at the same blockquote depth AND uses the same
    delimiter character with a run at least as long as the opener's, per
    CommonMark — otherwise an unrelated fence (a different depth, or a bare
    "```" closing some other ```` ```` ````-delimited block) could get
    mistaken for this one's close and swallow everything up to it as this
    block's content.

    Every fence is tracked this way, Rust-tagged or not: once a fence of any
    language is open, the only thing that can end it is ITS OWN matching
    closer — no other fence can open inside it, so a line that merely looks
    like a nested ```rust opener, inside some outer fence used to show
    Markdown-about-Markdown as a literal example, is correctly read as inert
    content of the outer fence rather than a real block of its own. A
    backtick-delimited opener whose info string itself contains a backtick
    is not a fence at all, per CommonMark (it would be ambiguous with an
    inline code span) — skipped over as plain content rather than opening
    anything, so a later, real ```rust a few lines down is not mistaken for
    that non-opener's content and left unchecked.

    Exits with an error if an opening fence has no matching close before
    EOF: a block dropped that way would shrink the "corpus" count with no
    signal that it happened, and so would one whose real close was skipped
    over for a depth or delimiter mismatch with nothing compatible after it.
    """
    lines = text.split("\n")
    if lines and lines[-1] == "":
        # A file ending in "\n" (virtually all of them) splits into a
        # phantom empty final element that is not a real line. Left in,
        # the container-end check below reads it as a genuine blank line
        # dropping out of an open blockquote — implicitly, wrongly,
        # "closing" a fence that was never actually closed, right at true
        # EOF, defeating the unmatched-fence error this function exists to
        # raise.
        lines.pop()
    blocks: list[str] = []
    i = 0
    n = len(lines)
    while i < n:
        m = FENCE_OPEN_RE.match(lines[i])
        if (
            not m
            or not _fence_indent_valid(m.group(1) + m.group(3))
            or not _list_marker_padding_valid(
                m.group(2), _expand_column(m.group(1), 0)
            )
        ):
            i += 1
            continue
        lead, marker, trail, delim, info = m.groups()
        delim_char, delim_len = delim[0], len(delim)
        if delim_char == "`" and "`" in info:
            i += 1
            continue
        # The info string's language is its first comma-separated word
        # (rustdoc's own convention for annotations like "rust,ignore" or
        # "rust,no_run") — an exact match, not a prefix: "rustfmt" or
        # "rustic" both start with "rust" but are not the Rust language tag,
        # and a block genuinely written in one of those (if this corpus ever
        # gets one) is not Rust source this script should be checking.
        is_rust = info.strip().split(",", 1)[0].strip() == "rust"
        depth = _blockquote_depth(lead + trail)
        marker_start_col = _expand_column(lead, 0)
        # The absolute column marker's content starts at, not just the
        # marker text's own width: an indented marker ("   - ```rust") has
        # already used marker_start_col columns before its own text even
        # begins, and a later line must reach lead's indent PLUS the
        # marker's width to still be inside the item, not just the
        # marker's width alone measured from column 0. 0 when there is no
        # marker at all (not merely when it measures to 0 some other way),
        # since this also gates whether the list-specific checks below
        # apply at all.
        marker_width = _expand_column(marker, marker_start_col) if marker else 0
        start_line = i + 1
        i += 1
        code_lines: list[str] = []
        closed = False
        while i < n:
            raw = lines[i]
            if marker_width > 0 and not _list_prefix_present(raw, marker_width):
                # Same idea as the blockquote check below, checked first and
                # on the RAW line: a line dedented below the list item's own
                # content column ends the item (and any fence open inside
                # it) before the marker-width stripping below would even
                # make sense to apply.
                closed = True
                break
            line = _strip_marker_width(raw, marker_width)
            cm = FENCE_CLOSE_RE.match(line)
            if (
                cm
                and _fence_indent_valid(cm.group(1))
                and _blockquote_depth(cm.group(1)) == depth
                and cm.group(2)[0] == delim_char
                and len(cm.group(2)) >= delim_len
            ):
                closed = True
                i += 1
                break
            if depth > 0 and not _quote_prefix_present(line, depth):
                # No lazy continuation inside a fenced code block: a line
                # that drops out of the blockquote ends it, and the fence
                # open inside it, right here -- not merely fails to close
                # it. Leave `i` where it is so this same line is still
                # considered as a fresh opener candidate on the next pass.
                closed = True
                break
            if is_rust:
                code_lines.append(_strip_blockquote(line, depth))
            i += 1
        if not closed:
            sys.exit(
                f"{relpath}: fence opened at line {start_line} has no "
                "closing delimiter before end of file"
            )
        if is_rust:
            blocks.append("\n".join(code_lines))
    return blocks


def check_block(code: str) -> tuple[str, str] | None:
    """Runs `rustfmt` on one wrapped block. None if it parses cleanly,
    else (rustfmt_location, rustfmt_message) for its first diagnostic.
    """
    wrapped = "fn __snippet() {\n" + code + "\n}\n"
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".rs", delete=False, encoding="utf-8"
    ) as tf:
        tf.write(wrapped)
        tmp_path = tf.name

    try:
        proc = subprocess.run(
            ["rustfmt", "--edition", RUST_EDITION, "--emit", "stdout", tmp_path],
            capture_output=True,
            text=True,
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    if proc.returncode == 0:
        return None
    msg, loc = "?", "?"
    for el in proc.stderr.splitlines():
        s = el.strip()
        if s.startswith("error"):
            msg = s
        elif s.startswith("-->"):
            loc = s
            break
    return loc, msg


def check_file(relpath: str) -> tuple[int, int, list[tuple[str, str, str]]]:
    """Returns (blocks_checked, blocks_excepted, failures).

    failures is a list of (first_line, rustfmt_location, rustfmt_message).
    Each block is checked in its own `rustfmt` invocation (see the module
    docstring's "How a block is checked" for why a shared buffer is wrong):
    that is what guarantees a diagnostic can never be credited to a
    different block than the one that produced it.
    """
    text = (REPO_ROOT / relpath).read_text(encoding="utf-8")
    blocks = find_rust_blocks(text, relpath)
    if not blocks:
        return 0, 0, []

    excepted_count = 0
    failures: list[tuple[str, str, str]] = []
    for code in blocks:
        first_line = first_content_line(code)
        if (relpath, first_line) in KNOWN_FRAGMENT_EXCEPTIONS:
            excepted_count += 1
            continue
        result = check_block(code)
        if result is not None:
            loc, msg = result
            failures.append((first_line, loc, msg))

    return len(blocks), excepted_count, failures


def main() -> int:
    files = collect_files()
    total_blocks = 0
    total_excepted = 0
    all_failures: list[tuple[str, str, str, str]] = []

    for relpath in files:
        checked, excepted, failures = check_file(relpath)
        total_blocks += checked
        total_excepted += excepted
        for first_line, loc, msg in failures:
            all_failures.append((relpath, first_line, loc, msg))

    print(
        f"Folio doc-snippet syntax check — {total_blocks} ```rust blocks "
        f"across {len(files)} files ({total_excepted} documented fragment "
        f"exceptions, not parse-checked)\n"
    )
    if all_failures:
        print(f"{len(all_failures)} block(s) fail to parse as valid Rust syntax:\n")
        for relpath, first_line, loc, msg in all_failures:
            print(f"  {relpath}  ({first_line!r})")
            print(f"    {msg}")
            print(f"    {loc}")
        return 1

    print("All non-excepted blocks parse cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
