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

Each ```rust fenced block is wrapped as the body of its own function,
`fn __snippet_N() { <block content> }`, and every block from one source
file is concatenated into a single generated file, which is handed to
`rustfmt --emit stdout`. Wrapping in a function body — rather than parsing
as a sequence of top-level items — is deliberate: a fn body accepts local
item declarations (`#[workflow] async fn ...`, `struct ...`, `use ...`)
AND bare statements/expressions in the same scope, so one wrapper handles
both a complete workflow definition and a short expression fragment
without needing to first classify which kind of block it is. `rustfmt`
fails on a genuine parse error (`error: expected expression, found ...`)
and exits 0 on anything syntactically valid, whether or not it was already
formatted — this script only cares about the former.

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

FENCE_RE = re.compile(r"```rust([^\n]*)\n(.*?)\n```", re.DOTALL)

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


def check_file(relpath: str) -> tuple[int, int, list[tuple[str, str, str]]]:
    """Returns (blocks_checked, blocks_excepted, failures).

    failures is a list of (first_line, rustfmt_location, rustfmt_message).
    """
    text = (REPO_ROOT / relpath).read_text(encoding="utf-8")
    blocks = list(FENCE_RE.finditer(text))
    if not blocks:
        return 0, 0, []

    buf_lines: list[str] = []
    # block_meta[i] = (fn_header_line_in_buf, first_line, excepted)
    block_meta = []
    excepted_count = 0
    for i, m in enumerate(blocks):
        info = m.group(1).strip()
        code = m.group(2)
        first_line = first_content_line(code)
        excepted = (relpath, first_line) in KNOWN_FRAGMENT_EXCEPTIONS or "ignore" in info
        if excepted:
            excepted_count += 1
        fn_name = f"__snippet_{i}"
        buf_lines.append(f"fn {fn_name}() {{")
        fn_header_line = len(buf_lines)
        buf_lines.extend(code.split("\n"))
        buf_lines.append("}")
        buf_lines.append("")
        block_meta.append((fn_header_line, first_line, excepted))

    wrapped = "\n".join(buf_lines) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".rs", delete=False, encoding="utf-8"
    ) as tf:
        tf.write(wrapped)
        tmp_path = tf.name

    try:
        proc = subprocess.run(
            ["rustfmt", "--edition", "2024", "--emit", "stdout", tmp_path],
            capture_output=True,
            text=True,
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    failures: list[tuple[str, str, str]] = []
    if proc.returncode != 0:
        err_lines = proc.stderr.splitlines()
        for j, el in enumerate(err_lines):
            loc_match = re.search(r":(\d+):\d+$", el.strip())
            if not (el.strip().startswith("-->") and loc_match):
                continue
            err_line_no = int(loc_match.group(1))
            owner = None
            for hdr, first_line, excepted in block_meta:
                if err_line_no >= hdr:
                    owner = (first_line, excepted)
                else:
                    break
            if owner is None:
                continue
            first_line, excepted = owner
            if excepted:
                continue
            msg = err_lines[j - 1].strip() if j > 0 else "?"
            failures.append((first_line, el.strip(), msg))

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
