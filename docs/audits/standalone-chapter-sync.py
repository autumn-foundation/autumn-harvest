#!/usr/bin/env python3
"""Standalone chapter sync check (issue #1614).

Deterministic. No network, no build.

The standalone getting-started chapter shows a real crate,
`examples/standalone-quickstart`. CI compiles that crate and runs the
chapter's shell blocks with `scripts/run-standalone-chapter.sh`. This audit
makes sure that the chapter shows the same bytes that CI runs, and that no
shell block escapes the run.

It checks these items:

1. The chapter has no CR byte. The runner reads lines with awk, so a CR
   would hide a marker from it.
2. Each `<!-- sync: PATH -->` marker has a fenced block after it. The block
   body is identical to the file at PATH. Each file in `REQUIRED_SYNC` has
   a marker, so a deleted marker cannot hide a stale block.
3. Each shell fence has a `<!-- chapter-run: KIND -->` marker before it.
   Each marker parses, and the chapter has each kind that the runner needs.
4. The first Rust block of Chapter 2 appears verbatim in the crate. The
   fork then really runs "the same first workflow".
5. `docs/embedding.md` has no Rust fence that rustdoc skips. The
   `EmbeddingDocSnippets` doctest then compiles every Rust block in it.
6. The doctest harness and the CI steps that run these guards exist, and
   are not commented out.
7. The CI Postgres service matches `compose.yaml`, so CI tests the database
   that the chapter starts.

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
CHAPTER_2 = "docs/getting-started/02-first-workflow.md"
CHAPTER_2_FILE = f"{CRATE}/src/workflows.rs"
COMPOSE = f"{CRATE}/compose.yaml"
CI = ".github/workflows/ci.yml"
CI_JOB = "standalone-chapter"

REQUIRED_SYNC = [
    f"{CRATE}/Cargo.toml",
    COMPOSE,
    f"{CRATE}/src/workflows.rs",
    f"{CRATE}/src/main.rs",
]
REQUIRED_RUN_KINDS = ["serve", "expect", "preflight"]

# Each pair is (file, line). The line must appear in the file, uncommented.
REQUIRED_WIRING = [
    (
        "autumn-harvest-plugin/src/lib.rs",
        '#[cfg(all(doctest, feature = "metrics", feature = "webhooks"))]',
    ),
    (
        "autumn-harvest-plugin/src/lib.rs",
        '#[doc = include_str!("../../docs/embedding.md")]',
    ),
    (CI, "python3 docs/audits/standalone-chapter-sync.py"),
    (
        CI,
        "cargo test -p autumn-harvest-plugin --features metrics,webhooks "
        "--doc EmbeddingDocSnippets 2>&1 | tee embedding-doctests.log",
    ),
    (CI, "run: ./scripts/run-standalone-chapter.sh"),
]

SYNC_RE = re.compile(r"^<!-- sync: (\S+) -->$")
RUN_RE = re.compile(r"^<!-- chapter-run: (serve|preflight|expect \S.*|skip \S.*) -->$")
FENCE_OPEN_RE = re.compile(r"^ {0,3}```(.*)$")
SHELL_LANGS = {"bash", "sh", "shell", "console", "zsh"}
RUST_SKIP_ATTRS = {"text", "compile_fail"}


def fence_info(line: str) -> str | None:
    """Return the info string of an opening fence, or None."""
    opener = FENCE_OPEN_RE.match(line)
    return opener.group(1).strip() if opener else None


def next_block(lines: list[str], start: int) -> tuple[str, str] | None:
    """Return (info string, body) of the first fence at or after `start`.

    Only blank lines can stand between the marker and the fence.
    """
    index = start
    while index < len(lines) and not lines[index].strip():
        index += 1
    if index >= len(lines):
        return None
    info = fence_info(lines[index])
    if info is None:
        return None
    body: list[str] = []
    for line in lines[index + 1 :]:
        if line.strip() == "```":
            return info, "".join(f"{entry}\n" for entry in body)
        body.append(line)
    return None


def check_line_endings(text: str) -> list[str]:
    """The chapter has no CR byte."""
    if "\r" in text:
        return [f"{CHAPTER}: contains a CR byte. Save it with LF line endings."]
    return []


def check_sync(chapter_text: str, read_file) -> tuple[list[str], set[str]]:
    """Compare each synced block with its file. Return errors and paths."""
    errors: list[str] = []
    seen: set[str] = set()
    lines = chapter_text.split("\n")
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


def is_shell_fence(info: str) -> bool:
    words = re.split(r"[,\s]+", info)
    return bool(words) and words[0] in SHELL_LANGS


def check_run_markers(chapter_text: str) -> list[str]:
    """Each marker parses and has a bash block. Each shell fence has a marker."""
    errors: list[str] = []
    kinds: set[str] = set()
    marked_fences: set[int] = set()
    lines = chapter_text.split("\n")
    for number, line in enumerate(lines, start=1):
        if "chapter-run" not in line:
            continue
        marker = RUN_RE.match(line)
        if not marker:
            errors.append(f"{CHAPTER}:{number}: a chapter-run marker does not parse: {line!r}")
            continue
        kinds.add(marker.group(1).split(" ", 1)[0])
        index = number
        while index < len(lines) and not lines[index].strip():
            index += 1
        marked_fences.add(index)
        block = next_block(lines, number)
        if block is None or block[0] != "bash":
            errors.append(f"{CHAPTER}:{number}: a chapter-run marker needs a bash block after it")

    in_fence = False
    for index, line in enumerate(lines):
        if in_fence:
            if line.strip() == "```":
                in_fence = False
            continue
        info = fence_info(line)
        if info is None:
            continue
        in_fence = True
        if is_shell_fence(info) and index not in marked_fences:
            errors.append(
                f"{CHAPTER}:{index + 1}: a shell block has no chapter-run marker. "
                "Mark it, or mark it `skip REASON`."
            )

    for kind in REQUIRED_RUN_KINDS:
        if kind not in kinds:
            errors.append(f"{CHAPTER}: no <!-- chapter-run: {kind} --> marker")
    return errors


def check_embedding_fences(text: str) -> list[str]:
    """Every Rust fence in embedding.md is one that rustdoc compiles."""
    errors: list[str] = []
    for number, line in enumerate(text.split("\n"), start=1):
        info = fence_info(line)
        if not info:
            continue
        words = [word for word in re.split(r"[,\s]+", info) if word]
        if not words or words[0] != "rust":
            continue
        skipped = [
            word
            for word in words[1:]
            if word.startswith("ignore") or word in RUST_SKIP_ATTRS
        ]
        if skipped:
            errors.append(
                f"{EMBEDDING}:{number}: a rust fence marked {skipped} is not compiled. "
                "Make the block compile."
            )
    return errors


def first_rust_block(text: str) -> str | None:
    """Return the body of the first `rust` fence in `text`."""
    lines = text.split("\n")
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


def compose_value(compose: str, key: str) -> str | None:
    match = re.search(rf"^\s*{key}:\s*(\S+)\s*$", compose, re.M)
    return match.group(1) if match else None


def check_service_matches_compose(compose: str, ci_job: str) -> list[str]:
    """The CI service uses the image, the credentials and the port of compose.yaml."""
    errors: list[str] = []
    needles = []
    for key in ["image", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"]:
        value = compose_value(compose, key)
        if value is None:
            errors.append(f"{COMPOSE}: no {key}")
            continue
        needles.append(f"{key}: {value}")
    port = re.search(r"(\d+):5432", compose)
    if port is None:
        errors.append(f"{COMPOSE}: no published Postgres port")
    else:
        needles.append(f"{port.group(1)}:5432")
    for needle in needles:
        if needle not in ci_job:
            errors.append(f"{CI}: job {CI_JOB} does not have {needle!r} from {COMPOSE}")
    return errors


def ci_job_text(ci: str, job: str) -> str:
    """Return the lines of one top-level job in ci.yml."""
    match = re.search(rf"^  {re.escape(job)}:\n(.*?)(?=^  [\w-]+:\n|\Z)", ci, re.M | re.S)
    return match.group(1) if match else ""


def wiring_present(text: str, needle: str) -> bool:
    """`needle` is on a line that is not a YAML or Rust comment."""
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped.startswith(("#", "//")) and not stripped.startswith("#["):
            continue
        if needle in line:
            return True
    return False


def read_repo_file(path: str) -> str | None:
    """Read a file with its line endings unchanged."""
    target = REPO_ROOT / path
    if not target.is_file():
        return None
    return target.read_bytes().decode("utf-8")


def run() -> int:
    errors: list[str] = []
    chapter = read_repo_file(CHAPTER)
    if chapter is None:
        errors.append(f"{CHAPTER}: not found")
    else:
        errors.extend(check_line_endings(chapter))
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
        if not wiring_present(read_repo_file(path) or "", needle):
            errors.append(f"{path}: missing {needle!r}")

    errors.extend(
        check_service_matches_compose(
            read_repo_file(COMPOSE) or "", ci_job_text(read_repo_file(CI) or "", CI_JOB)
        )
    )

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
    # A CR anywhere in the chapter is a finding, because awk sees it.
    assert len(check_line_endings("a\r\nb\n")) == 1
    # A line that mentions chapter-run but does not parse is a finding.
    assert len(check_run_markers("<!--  chapter-run: serve -->\n```bash\nx\n```\n")) >= 1
    assert len(check_run_markers("<!-- chapter-run: expect  -->\n```bash\nx\n```\n")) >= 1
    assert len(check_run_markers("<!-- chapter-run: serving -->\n```bash\nx\n```\n")) >= 1
    # Every shell fence needs a marker. `skip` with a reason is a marker.
    unmarked = runs + "```bash\ndocker compose up\n```\n"
    assert len(check_run_markers(unmarked)) == 1
    assert len(check_run_markers(runs + "   ```sh\nls\n   ```\n")) == 1
    skipped = runs + "<!-- chapter-run: skip CI uses a service. -->\n```bash\nup\n```\n"
    assert check_run_markers(skipped) == []
    # rustdoc also skips these forms.
    assert len(check_embedding_fences("```rust, ignore\nfn a() {}\n```\n")) == 1
    assert len(check_embedding_fences("```rust ignore\nfn a() {}\n```\n")) == 1
    assert len(check_embedding_fences("  ```rust,ignore-wasm32\nfn a() {}\n  ```\n")) == 1
    # The CI service matches compose.yaml.
    compose = (
        "    image: postgres:16\n      POSTGRES_USER: u\n"
        "      POSTGRES_PASSWORD: p\n      POSTGRES_DB: d\n      - \"127.0.0.1:5435:5432\"\n"
    )
    job = "image: postgres:16\nPOSTGRES_USER: u\nPOSTGRES_PASSWORD: p\nPOSTGRES_DB: d\n- 5435:5432\n"
    assert check_service_matches_compose(compose, job) == []
    assert len(check_service_matches_compose(compose, job.replace("16", "17"))) == 1
    # A commented-out CI line does not count as wiring.
    assert not wiring_present("# run: cargo test x\n", "run: cargo test x")
    assert wiring_present("  run: cargo test x\n", "run: cargo test x")
    print("standalone-chapter-sync self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
