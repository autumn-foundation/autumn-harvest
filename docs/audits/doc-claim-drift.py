#!/usr/bin/env python3
"""Fail when a doc claim contradicts the shipped code (issue #1831).

Four rules:

1. variant-count. A doc that states a `WorkflowEvent` variant count must
   state the count in `autumn-harvest/src/event.rs`.
2. duplicate-row. A table in `docs/architecture.md` names each row once.
3. release-notes. If `RELEASE_NOTES.md` has a version heading, its newest
   version is the workspace version.
4. stale-claim. Each STALE_CLAIMS pattern is absent from its file. Each one
   is a claim that a shipped change made false.

`docs/shipped-work.md` and the process records are out of scope. They
record the state at the time of each change.

Usage:
    python3 docs/audits/doc-claim-drift.py
    python3 docs/audits/doc-claim-drift.py --self-test
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Historical records, as in corpus-link-check.py, plus the shipped-work log.
SKIP_DIRS = ("plans", "changelog.d", "rnd", "assays", "perf-artifacts")
SKIP_FILES = ("docs/shipped-work.md",)

VARIANT_COUNT_RE = re.compile(r"WorkflowEvent\b[^.\n|]{0,60}?\b(\d+) variants")
VERSION_HEADING_RE = re.compile(r"^## \[(\d+\.\d+\.\d+[^\]]*)\]", re.MULTILINE)
WORKSPACE_VERSION_RE = re.compile(
    r"^\[workspace\.package\][^\[]*?^version\s*=\s*\"([^\"]+)\"", re.MULTILINE | re.DOTALL
)

# (path, pattern, the fact that makes the claim false)
STALE_CLAIMS = [
    (
        "docs/comparison.md",
        r"(?is)planned\b.{0,120}?#954\b",
        "cross-region DR shipped (issue #954), see docs/cross-region-dr.md",
    ),
    (
        "docs/comparison.md",
        r"(?i)no (built-in )?(multi|cross)-region (DR|replication)",
        "cross-region DR shipped (issue #954), see docs/cross-region-dr.md",
    ),
    (
        "docs/comparison.md",
        r"(?i)no cross-shard workflows|cross-shard workflows (are )?(explicitly )?out of scope",
        "cross-shard child placement shipped (issue #956)",
    ),
    (
        "docs/architecture.md",
        r"(?i)rebalancing of existing workflows is out of scope",
        "shard rebalancing shipped (issue #964), see shard_rebalance.rs",
    ),
    (
        "docs/sharding.md",
        r"(?i)out of scope[^\n]*cross-region failover",
        "cross-region DR shipped (issue #954), see docs/cross-region-dr.md",
    ),
    (
        "docs/autumn-workflow-architecture.md",
        r"(?i)harvest_task_queue`?[^\n.]*list[- ]partition",
        "harvest_task_queue is one table, not partitioned (issue #1811)",
    ),
    (
        "autumn-harvest-redis/src/lib.rs",
        r"\A//! Redis Streams task queue adapter",
        "the worker uses the crate as a dispatch channel for task references",
    ),
    (
        "autumn-harvest/src/worker.rs",
        r"TODO\(#606 step 9\)",
        "#606 step 9 is wired: build_activity_enqueue_plan pins session tasks",
    ),
]


def strip_comments_and_strings(source):
    """Blank out comments and string literals. Keep the line breaks."""
    pattern = re.compile(r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"', re.DOTALL)
    return pattern.sub(lambda m: re.sub(r"[^\n]", " ", m.group()), source)


def count_variants(source, enum="WorkflowEvent"):
    """Return the variant count of `pub enum <enum>`, or None if absent."""
    code = strip_comments_and_strings(source)
    code = re.sub(r"#\[[^\]]*\]", " ", code)
    head = re.search(rf"\bpub enum {enum}\s*\{{", code)
    if head is None:
        return None
    count, expect_variant = 0, True
    depth = {"{": 1, "(": 0, "[": 0}
    closer = {"}": "{", ")": "(", "]": "["}
    i = head.end()
    while depth["{"] > 0 and i < len(code):
        c = code[i]
        top = depth["{"] == 1 and depth["("] == 0 and depth["["] == 0
        if c in depth:
            depth[c] += 1
        elif c in closer:
            depth[closer[c]] -= 1
        elif top and c == ",":
            expect_variant = True
        elif top and expect_variant and (c.isalpha() or c == "_"):
            count += 1
            expect_variant = False
        i += 1
    return count


def variant_count_findings(docs, actual):
    """docs: {path: text}. Return a message for each wrong count."""
    found = []
    for path, text in docs.items():
        for n, line in enumerate(text.splitlines(), 1):
            for m in VARIANT_COUNT_RE.finditer(line):
                if int(m.group(1)) != actual:
                    found.append(
                        f"{path}:{n}: says {m.group(1)} WorkflowEvent variants, "
                        f"event.rs has {actual}. Fix the count or drop it."
                    )
    return found


def duplicate_row_findings(path, text):
    """Return a message for each repeated first cell within one table."""
    found, seen = [], None
    for n, line in enumerate(text.splitlines(), 1):
        if not line.startswith("|"):
            seen = None
            continue
        if seen is None:
            seen = {}
        cell = line.split("|")[1].strip()
        if not cell or set(cell) <= set("-: "):
            continue
        if cell in seen:
            found.append(f"{path}:{n}: row {cell} repeats line {seen[cell]}")
        else:
            seen[cell] = n
    return found


def release_notes_findings(notes, cargo_toml):
    headings = VERSION_HEADING_RE.findall(notes)
    if not headings:
        return []
    m = WORKSPACE_VERSION_RE.search(cargo_toml)
    if m is None:
        return ["Cargo.toml: no [workspace.package] version"]
    if headings[0] != m.group(1):
        return [
            f"RELEASE_NOTES.md: newest heading is {headings[0]}, the workspace "
            f"is {m.group(1)}. CHANGELOG.md holds the release history."
        ]
    return []


def stale_claim_findings(read, claims):
    """read(path) returns the text, or None for a missing file."""
    found = []
    for path, pattern, fact in claims:
        text = read(path)
        if text is None:
            found.append(f"{path}: missing. Move or drop its STALE_CLAIMS entry.")
            continue
        for m in re.finditer(pattern, text):
            n = text.count("\n", 0, m.start()) + 1
            found.append(f"{path}:{n}: stale claim. Fact: {fact}.")
    return found


def reader_docs():
    paths = [ROOT / "README.md"]
    for path in sorted((ROOT / "docs").rglob("*.md")):
        rel = path.relative_to(ROOT).as_posix()
        if rel in SKIP_FILES or any(f"docs/{d}/" in rel for d in SKIP_DIRS):
            continue
        paths.append(path)
    return {p.relative_to(ROOT).as_posix(): p.read_text(encoding="utf-8") for p in paths}


def read(rel):
    path = ROOT / rel
    return path.read_text(encoding="utf-8") if path.is_file() else None


def run():
    event_rs = read("autumn-harvest/src/event.rs")
    actual = count_variants(event_rs)
    found = []
    if actual is None:
        found.append("autumn-harvest/src/event.rs: no `pub enum WorkflowEvent`")
    else:
        found += variant_count_findings(reader_docs(), actual)
    found += duplicate_row_findings("docs/architecture.md", read("docs/architecture.md"))
    found += release_notes_findings(read("RELEASE_NOTES.md") or "", read("Cargo.toml"))
    found += stale_claim_findings(read, STALE_CLAIMS)
    for message in found:
        print(message)
    print(f"doc-claim-drift: {len(found)} findings")
    return 1 if found else 0


def self_test():
    enum_src = """
    // pub enum WorkflowEvent { Fake }
    #[derive(Debug)]
    #[serde(tag = "type", content = "data")]
    pub enum WorkflowEvent {
        /// A doc comment with { braces } and Words, Like, These.
        Started {
            input: Value,
            nested: Option<Map<String, Value>>,
        },
        #[serde(rename = "X")]
        Unit,
        Tuple(
            Box<Inner>,
            Other,
        ),
        Discriminant = 3,
        /* Block, Comment { } */
        Last { s: String },
    }
    pub enum Other { A, B }
    """
    assert count_variants(enum_src) == 5, count_variants(enum_src)
    assert count_variants("pub enum Else { A }") is None

    docs = {
        "a.md": "| `event.rs` | `WorkflowEvent` enum (5 variants, tagged) |",
        "b.md": "The `WorkflowEvent` enum has 41 variants.",
        "c.md": "Event enum now has 41 variants.",
    }
    found = variant_count_findings(docs, 5)
    assert len(found) == 1 and found[0].startswith("b.md:1"), found

    table = "| Module | P |\n|---|---|\n| `a.rs` | 1 |\n| `a.rs` | 2 |\n\n| `a.rs` | 3 |\n"
    found = duplicate_row_findings("x.md", table)
    assert len(found) == 1 and "x.md:4" in found[0], found

    cargo = '[workspace]\nmembers = []\n\n[workspace.package]\nedition = "2024"\nversion = "0.7.0"\n'
    assert release_notes_findings("", cargo) == []
    assert release_notes_findings("Pointer to CHANGELOG.md.\n", cargo) == []
    assert release_notes_findings("## [0.7.0] - x\n## [0.6.0] - y\n", cargo) == []
    found = release_notes_findings("<!-- -->\n## [0.4.0] - x\n", cargo)
    assert len(found) == 1 and "0.4.0" in found[0], found

    files = {"d.md": "Planned: cross-region DR ([#954](u)).", "e.md": "Shipped (#954)."}
    claims = [
        ("d.md", r"(?is)planned\b.{0,120}?#954\b", "shipped"),
        ("e.md", r"(?is)planned\b.{0,120}?#954\b", "shipped"),
        ("gone.md", r"x", "y"),
    ]
    found = stale_claim_findings(files.get, claims)
    assert len(found) == 2, found
    assert found[0].startswith("d.md:1") and "shipped" in found[0], found
    assert "gone.md" in found[1] and "missing" in found[1], found

    # Every real pattern compiles.
    for _, pattern, _ in STALE_CLAIMS:
        re.compile(pattern)

    print("doc-claim-drift self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
