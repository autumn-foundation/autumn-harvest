#!/usr/bin/env python3
"""Standalone chapter sync check (issue #1614).

Deterministic. No network, no build.

The standalone getting-started chapter shows a real crate,
`examples/standalone-quickstart`. CI compiles that crate and runs it. This
audit makes sure that the chapter shows the same bytes that CI runs.

It checks these items:

1. Each `<!-- sync: PATH -->` marker has a fenced block after it. The block
   body is identical to the file at PATH.
2. Each file in `REQUIRED_SYNC` has a marker. A deleted marker cannot hide
   a stale block.
3. The chapter has the `<!-- chapter-run: KIND -->` markers that
   `scripts/run-standalone-chapter.sh` needs.
4. The first Rust block of Chapter 2 appears verbatim in the crate. The
   fork then really runs "the same first workflow".
5. `docs/embedding.md` has no Rust fence that rustdoc skips. The
   `EmbeddingDocSnippets` doctest then compiles every Rust block in it.
6. The doctest harness and the CI steps that run these guards exist.

Usage:
    python3 docs/audits/standalone-chapter-sync.py
    python3 docs/audits/standalone-chapter-sync.py --self-test
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CHAPTER = "docs/getting-started/standalone-axum.md"
EMBEDDING = "docs/embedding.md"
CRATE = "examples/standalone-quickstart"

REQUIRED_SYNC = [
    f"{CRATE}/Cargo.toml",
    f"{CRATE}/compose.yaml",
    f"{CRATE}/src/workflows.rs",
    f"{CRATE}/src/main.rs",
]
CHAPTER_2 = "docs/getting-started/02-first-workflow.md"
CHAPTER_2_FILE = f"{CRATE}/src/workflows.rs"
REQUIRED_RUN_KINDS = ["serve", "expect", "preflight"]

# Each pair is (file, text). The text must appear in the file.
REQUIRED_WIRING = [
    (
        "autumn-harvest-plugin/src/lib.rs",
        '#[doc = include_str!("../../docs/embedding.md")]',
    ),
    (".github/workflows/ci.yml", "python3 docs/audits/standalone-chapter-sync.py"),
    (".github/workflows/ci.yml", "--doc EmbeddingDocSnippets"),
    (".github/workflows/ci.yml", "./scripts/run-standalone-chapter.sh"),
]

SYNC_RE = re.compile(r"^<!-- sync: (\S+) -->$")
RUN_RE = re.compile(r"^<!-- chapter-run: (\w+)(?: .*)? -->$")
FENCE_OPEN_RE = re.compile(r"^```(\S*)$")
RUST_SKIP_ATTRS = {"ignore", "text", "compile_fail"}


def next_block(lines: list[str], start: int) -> tuple[str, str] | None:
    """Return (info string, body) of the first fence at or after `start`.

    Only blank lines can stand between the marker and the fence.
    """
    index = start
    while index < len(lines) and not lines[index].strip():
        index += 1
    if index >= len(lines):
        return None
    opener = FENCE_OPEN_RE.match(lines[index])
    if not opener:
        return None
    body: list[str] = []
    for line in lines[index + 1 :]:
        if line == "```":
            return opener.group(1), "".join(f"{entry}\n" for entry in body)
        body.append(line)
    return None


def check_sync(chapter_text: str, read_file) -> tuple[list[str], set[str]]:
    """Compare each synced block with its file. Return errors and paths."""
    errors: list[str] = []
    seen: set[str] = set()
    lines = chapter_text.splitlines()
    for number, line in enumerate(lines, start=1):
        marker = SYNC_RE.match(line)
        if not marker:
            continue
        path = marker.group(1)
        seen.add(path)
        block = next_block(lines, number)
        if block is None:
            errors.append(f"{CHAPTER}:{number}: no fenced block after sync marker for {path}")
            continue
        expected = read_file(path)
        if expected is None:
            errors.append(f"{CHAPTER}:{number}: {path} does not exist")
            continue
        if block[1] != expected:
            errors.append(
                f"{CHAPTER}:{number}: the block differs from {path}. "
                "Copy the file into the chapter, or the chapter into the file."
            )
    return errors, seen


def check_run_markers(chapter_text: str) -> list[str]:
    """Each run marker has a bash block. Each required kind is present."""
    errors: list[str] = []
    kinds: set[str] = set()
    lines = chapter_text.splitlines()
    for number, line in enumerate(lines, start=1):
        marker = RUN_RE.match(line)
        if not marker:
            continue
        kinds.add(marker.group(1))
        block = next_block(lines, number)
        if block is None or block[0] != "bash":
            errors.append(f"{CHAPTER}:{number}: a chapter-run marker needs a bash block after it")
    for kind in REQUIRED_RUN_KINDS:
        if kind not in kinds:
            errors.append(f"{CHAPTER}: no <!-- chapter-run: {kind} --> marker")
    return errors


def check_embedding_fences(text: str) -> list[str]:
    """Every Rust fence in embedding.md is one that rustdoc compiles."""
    errors: list[str] = []
    for number, line in enumerate(text.splitlines(), start=1):
        opener = FENCE_OPEN_RE.match(line)
        if not opener or not opener.group(1).startswith("rust"):
            continue
        attrs = set(opener.group(1).split(",")[1:])
        skipped = attrs & RUST_SKIP_ATTRS
        if skipped:
            errors.append(
                f"{EMBEDDING}:{number}: a rust fence marked {sorted(skipped)} is not compiled. "
                "Make the block compile."
            )
    return errors


def first_rust_block(text: str) -> str | None:
    """Return the body of the first `rust` fence in `text`."""
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line == "```rust":
            block = next_block(lines, index)
            return block[1] if block else None
    return None


def check_same_workflow(chapter_2: str | None, crate_file: str | None) -> list[str]:
    """The Chapter 2 workflow is a verbatim part of the crate file."""
    block = first_rust_block(chapter_2 or "")
    if block is None:
        return [f"{CHAPTER_2}: no rust block found"]
    if crate_file is None or block not in crate_file:
        return [
            f"{CHAPTER_2_FILE}: does not contain the first rust block of {CHAPTER_2} verbatim"
        ]
    return []


def read_repo_file(path: str) -> str | None:
    target = REPO_ROOT / path
    return target.read_text(encoding="utf-8") if target.is_file() else None


def run() -> int:
    errors: list[str] = []
    chapter = read_repo_file(CHAPTER)
    if chapter is None:
        errors.append(f"{CHAPTER}: not found")
    else:
        sync_errors, seen = check_sync(chapter, read_repo_file)
        errors.extend(sync_errors)
        for path in REQUIRED_SYNC:
            if path not in seen:
                errors.append(f"{CHAPTER}: no <!-- sync: {path} --> marker")
        errors.extend(check_run_markers(chapter))

    errors.extend(
        check_same_workflow(read_repo_file(CHAPTER_2), read_repo_file(CHAPTER_2_FILE))
    )

    embedding = read_repo_file(EMBEDDING)
    if embedding is None:
        errors.append(f"{EMBEDDING}: not found")
    else:
        errors.extend(check_embedding_fences(embedding))

    for path, needle in REQUIRED_WIRING:
        text = read_repo_file(path) or ""
        if needle not in text:
            errors.append(f"{path}: missing {needle!r}")

    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        print(f"standalone-chapter-sync: {len(errors)} finding(s)", file=sys.stderr)
        return 1
    print("standalone-chapter-sync: OK")
    return 0


def self_test() -> int:
    files = {"a.rs": "fn a() {}\n"}
    good = "<!-- sync: a.rs -->\n\n```rust\nfn a() {}\n```\n"
    errors, seen = check_sync(good, files.get)
    assert errors == [] and seen == {"a.rs"}, errors

    stale = "<!-- sync: a.rs -->\n```rust\nfn b() {}\n```\n"
    errors, _ = check_sync(stale, files.get)
    assert len(errors) == 1 and "differs" in errors[0], errors

    no_block = "<!-- sync: a.rs -->\nSome prose.\n```rust\nfn a() {}\n```\n"
    errors, _ = check_sync(no_block, files.get)
    assert len(errors) == 1 and "no fenced block" in errors[0], errors

    unclosed = "<!-- sync: a.rs -->\n```rust\nfn a() {}\n"
    errors, _ = check_sync(unclosed, files.get)
    assert len(errors) == 1, errors

    missing = "<!-- sync: b.rs -->\n```rust\n```\n"
    errors, _ = check_sync(missing, files.get)
    assert len(errors) == 1 and "does not exist" in errors[0], errors

    runs = (
        "<!-- chapter-run: serve -->\n```bash\ncargo run\n```\n"
        "<!-- chapter-run: expect sent -->\n```bash\ncurl x\n```\n"
        "<!-- chapter-run: preflight -->\n```bash\nharvest preflight\n```\n"
    )
    assert check_run_markers(runs) == []
    wrong_lang = "<!-- chapter-run: serve -->\n```sh\ncargo run\n```\n"
    assert len(check_run_markers(wrong_lang)) == 3

    chapter_2 = "Text.\n\n```rust\nfn a() {}\n```\n\n```rust\nfn b() {}\n```\n"
    assert check_same_workflow(chapter_2, "use x;\nfn a() {}\nfn c() {}\n") == []
    assert len(check_same_workflow(chapter_2, "fn b() {}\n")) == 1
    assert len(check_same_workflow("no code", "fn a() {}\n")) == 1

    assert check_embedding_fences("```rust\nfn a() {}\n```\n") == []
    assert check_embedding_fences("```rust,no_run\nfn a() {}\n```\n") == []
    assert len(check_embedding_fences("```rust,ignore\nfn a() {}\n```\n")) == 1
    print("standalone-chapter-sync self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
