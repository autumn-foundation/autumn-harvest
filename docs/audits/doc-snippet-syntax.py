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

# [\s>]* absorbs both list indentation and Markdown blockquote markers ("> "),
# including a blockquote nested inside a list item, ahead of the fence itself.
# Captured so the same prefix can be stripped from the block's content lines
# too — a blockquoted block's lines all carry "> ", which is Markdown syntax,
# not part of the Rust source, and left in place would break every blockquoted
# snippet's parse regardless of whether its actual code is valid.
FENCE_OPEN_RE = re.compile(r"^([\s>]*)```rust(.*)$")
FENCE_CLOSE_RE = re.compile(r"^[\s>]*```\s*$")

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


def _strip_prefix(line: str, prefix: str) -> str:
    """Strips `prefix` from the start of `line`. A blank blockquoted line is
    often just ">" with no trailing space its sibling lines have, so an exact
    `startswith` match is not guaranteed; fall back to stripping whatever
    leading run of characters `line` and `prefix` have in common.
    """
    if line.startswith(prefix):
        return line[len(prefix) :]
    n = 0
    for a, b in zip(line, prefix):
        if a != b:
            break
        n += 1
    return line[n:]


def find_rust_blocks(text: str, relpath: str) -> list[str]:
    """Returns the code of every ```rust fenced block in `text`, in order.

    Line-oriented so an indented or blockquoted fence (a code block nested
    under a list item or a "> " note) is still found — a single-regex scan
    anchored on an unindented close silently drops those. The same leading
    indentation/">" prefix the opening fence carried is stripped from every
    content line too, so a blockquote's "> " does not end up inside the Rust
    source handed to `rustfmt`. Exits with an error if an opening fence has
    no matching close before EOF: a block dropped that way would shrink the
    "corpus" count with no signal that it happened.
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
        prefix = m.group(1)
        start_line = i + 1
        i += 1
        code_lines: list[str] = []
        closed = False
        while i < n:
            if FENCE_CLOSE_RE.match(lines[i]):
                closed = True
                i += 1
                break
            code_lines.append(_strip_prefix(lines[i], prefix))
            i += 1
        if not closed:
            sys.exit(
                f"{relpath}: ```rust fence opened at line {start_line} has no "
                "closing ``` before end of file"
            )
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
