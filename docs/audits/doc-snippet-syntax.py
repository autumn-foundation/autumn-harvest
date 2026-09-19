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

# CONTAINER absorbs indentation and Markdown blockquote markers ("> "),
# including a blockquote nested inside a list item, ahead of the fence
# itself — captured so the same text can be stripped from the block's
# content lines too, since a blockquote's "> " is Markdown syntax, not part
# of the Rust source, and left in place would break every blockquoted
# snippet's parse regardless of whether its actual code is valid.
#
# Each run of plain indentation, whether at the start or straight after a
# ">", is capped at 3 columns: `docs/audits/comment-hygiene.py` already
# implements this exact CommonMark rule (see its fence_delimiter, and its
# own fixture tests around "four spaces is an indented code line, not a
# fence opener" / "an over-indented \`\`\` inside a fence is content, not a
# closer") for the same reason — 4+ columns of indent relative to the
# container makes a delimiter-looking line indented CONTENT, not a fence
# marker, opener or closer alike.
CONTAINER = r"[ \t]{0,3}(?:>[ \t]{0,3})*"
#
# LIST_MARKER additionally allows a fence to be the first block of a list
# item, opening on the marker's own line ("- ```rust", "1. ```rust") rather
# than on a line under it — valid CommonMark, and unlike the marker's own
# text there is no per-line repeat of it to strip back out of content lines,
# so those are left to the plain indentation-stripping case above. It is
# never legal on a CLOSING fence line, which must consist of nothing but
# its container's indentation/blockquote markers and the delimiter itself —
# a line like "- ```" inside a still-open block is literal fenced content,
# not a closer.
#
# This is deliberately not a full CommonMark container parser (no nesting
# beyond one list level, no lazy continuation lines) — this script trades
# that for staying dependency-free: docs/audits/*.py runs with no network
# access, so no markdown-parsing package can be installed to do this
# properly, and a hand-rolled line scanner is what stays within that.
#
# The list marker is captured separately (group 2) from the indentation/
# blockquote text around it (groups 1 and 3) because only the latter repeats
# on every content line of the container — "- " never appears again after
# the fence's own opening line, but "> " does, on every line of a
# blockquote. Stripping the marker back out of content lines would be
# wrong; not stripping the indent/blockquote text would leave Markdown
# syntax inside the Rust source handed to `rustfmt`.
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
LIST_MARKER = r"(?:[-*+]|\d+[.)])\s+"
FENCE_DELIM = r"(`{3,}|~{3,})"
FENCE_OPEN_RE = re.compile(rf"^({CONTAINER})((?:{LIST_MARKER})?)({CONTAINER}){FENCE_DELIM}(.*)$")
FENCE_CLOSE_RE = re.compile(rf"^({CONTAINER}){FENCE_DELIM}\s*$")


def _blockquote_depth(prefix: str) -> int:
    """Counts ">" markers in a captured fence prefix, to tell a closing fence
    at the opener's own blockquote depth from an unrelated one deeper or
    shallower — for example a plain, unquoted "```" that happens to follow a
    "> ```rust" opener, which is not that opener's close. A list marker does
    not similarly need tracking: unlike ">", it is not repeated on every line
    of the container, so it never appears on a closing fence line at all.
    """
    return prefix.count(">")

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
    """Strips up to `width` leading whitespace characters from `line`.

    A fence opened on its list marker's own line ("- ```rust") leaves no
    marker text on later content lines, but CommonMark still indents those
    lines by the marker's own display width ("- " is 2 columns) to keep them
    inside the list item — plain alignment spaces, not part of the Rust
    source. Stops at the first non-whitespace character rather than always
    removing exactly `width`, so a shorter or blank line is not corrupted.
    """
    n = 0
    for ch in line[:width]:
        if not ch.isspace():
            break
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
    blocks: list[str] = []
    i = 0
    n = len(lines)
    while i < n:
        m = FENCE_OPEN_RE.match(lines[i])
        if not m:
            i += 1
            continue
        lead, marker, trail, delim, info = m.groups()
        delim_char, delim_len = delim[0], len(delim)
        if delim_char == "`" and "`" in info:
            i += 1
            continue
        is_rust = info.strip().startswith("rust")
        depth = _blockquote_depth(lead + trail)
        marker_width = len(marker)
        start_line = i + 1
        i += 1
        code_lines: list[str] = []
        closed = False
        while i < n:
            line = _strip_marker_width(lines[i], marker_width)
            cm = FENCE_CLOSE_RE.match(line)
            if (
                cm
                and _blockquote_depth(cm.group(1)) == depth
                and cm.group(2)[0] == delim_char
                and len(cm.group(2)) >= delim_len
            ):
                closed = True
                i += 1
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
