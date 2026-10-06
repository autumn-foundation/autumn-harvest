#!/usr/bin/env python3
"""Fail when a doc claim contradicts the shipped code (issue #1831).

Four rules:

1. variant-count. A doc that states a `WorkflowEvent` variant count must
   match the variant count in `autumn-harvest/src/event.rs`.
2. duplicate-row. No two rows of one table in `docs/architecture.md` share
   a first cell.
3. release-notes. If `RELEASE_NOTES.md` has a version heading, its newest
   version is the workspace version.
4. stale-claim. Each STALE_CLAIMS pattern stays absent from its file. Each
   one is a claim that a shipped change made false.

Rule 1 skips `docs/shipped-work.md` and the historical records in SKIP_DIRS.
They record the state at the time of each change.

Usage:
    python3 docs/audits/doc-claim-drift.py
    python3 docs/audits/doc-claim-drift.py --self-test
"""
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Historical records, as in corpus-link-check.py, plus ADRs and the
# shipped-work log.
SKIP_DIRS = ("plans", "changelog.d", "rnd", "assays", "perf-artifacts", "adr")
SKIP_FILES = ("docs/shipped-work.md",)

# A total count only: "enum (41 variants" or "has 41 variants", not "gains 2
# variants". The pattern can cross a line wrap.
VARIANT_COUNT_RE = re.compile(
    r"WorkflowEvent`?\s+(?:enum\s+\(|(?:enum\s+)?(?:now\s+)?has\s+)(\d[\d,]*)\s+variants\b"
)
VERSION_HEADING_RE = re.compile(r"^##\s+\[?v?(\d+\.\d+\.\d+[^\]\s]*)\]?", re.MULTILINE)
FENCE_RE = re.compile(r"^(```|~~~).*?^\1", re.MULTILINE | re.DOTALL)
CELL_SPLIT_RE = re.compile(r"(?<!\\)\|")

# A quote of the old claim, as history, starts with a quotation mark.
NOT_QUOTED = r"(?<![\"“])"
REBALANCING = (
    r"(?i)" + NOT_QUOTED + r"\bcross-shard\s+rebalancing(\s+of\s+existing\s+workflows)?"
    r"\s+is\s+(out\s+of\s+scope|not\s+supported)"
)

# (path, pattern, the fact that makes the claim false)
STALE_CLAIMS = [
    (
        "docs/comparison.md",
        r"(?i)\bplanned\b[^\n]{0,120}?#954\b",
        "cross-region DR shipped (issue #954), see docs/cross-region-dr.md",
    ),
    (
        "docs/comparison.md",
        r"(?i)\bno\s+(built-in\s+)?(multi|cross)-region\s+(DR|replication)"
        r"\s+or\s+(replication|failover)\b",
        "cross-region DR shipped (issue #954), see docs/cross-region-dr.md",
    ),
    (
        "docs/comparison.md",
        r"(?i)\bno\s+cross-shard\s+workflows\b"
        r"|\bcross-shard\s+workflows\s+(are\s+)?(explicitly\s+)?out\s+of\s+scope",
        "cross-shard child placement shipped (issue #956)",
    ),
    (
        "docs/architecture.md",
        REBALANCING,
        "shard rebalancing shipped (issue #964), see shard_rebalance.rs",
    ),
    (
        "docs/sharding.md",
        REBALANCING,
        "shard rebalancing shipped (issue #964), see shard_rebalance.rs",
    ),
    (
        "docs/architecture.md",
        r"(?i)\bcross-shard\s+child\s+fan-out\s+is\s+out\s+of\s+scope",
        "the `_placed` fan-out variants take a ChildPlacement (issue #956)",
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

RAW_STRING_RE = re.compile(r'b?r(#*)"')
CHAR_RE = re.compile(r"'(\\.[^']*|[^\\'])'")


def strip_rust(source):
    """Blank out comments, strings and char literals. Keep the line breaks."""
    out, i, n = [], 0, len(source)

    def blank(end):
        out.append(re.sub(r"[^\n]", " ", source[i:end]))
        return end

    while i < n:
        prev = source[i - 1] if i else " "
        raw = RAW_STRING_RE.match(source, i)
        char = CHAR_RE.match(source, i)
        if source.startswith("//", i):
            end = source.find("\n", i)
            i = blank(n if end < 0 else end)
        elif source.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if source.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif source.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            i = blank(j)
        elif raw and not (prev.isalnum() or prev == "_"):
            end = source.find('"' + raw.group(1), raw.end())
            i = blank(n if end < 0 else end + 1 + len(raw.group(1)))
        elif source[i] == '"':
            j = i + 1
            while j < n and source[j] != '"':
                j += 2 if source[j] == "\\" else 1
            i = blank(j + 1)
        elif char:
            i = blank(char.end())
        else:
            out.append(source[i])
            i += 1
    return "".join(out)


def count_variants(source, enum="WorkflowEvent"):
    """Return the variant count of `pub enum <enum>`, or None if absent."""
    code = strip_rust(source)
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
        if c == "#" and code[i + 1 : i + 2] == "[":
            # Skip a whole attribute, so a bracket inside it cannot count.
            level, i = 1, i + 2
            while i < len(code) and level:
                level += {"[": 1, "]": -1}.get(code[i], 0)
                i += 1
            continue
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


def line_of(text, offset):
    return text.count("\n", 0, offset) + 1


def variant_count_findings(docs, actual):
    """docs: {path: text}. Return a message for each wrong count."""
    found = []
    for path, text in docs.items():
        for m in VARIANT_COUNT_RE.finditer(text):
            stated = int(m.group(1).replace(",", ""))
            if stated != actual:
                found.append(
                    f"{path}:{line_of(text, m.start())}: says {stated} WorkflowEvent "
                    f"variants, event.rs has {actual}. Fix the count or drop it."
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
        cell = CELL_SPLIT_RE.split(line)[1].strip()
        if not cell or set(cell) <= set("-: "):
            continue
        if cell in seen:
            found.append(f"{path}:{n}: row {cell} repeats line {seen[cell]}")
        else:
            seen[cell] = n
    return found


def release_notes_findings(notes, cargo_toml):
    headings = VERSION_HEADING_RE.findall(FENCE_RE.sub("", notes))
    if not headings:
        return []
    try:
        version = tomllib.loads(cargo_toml)["workspace"]["package"]["version"]
    except (tomllib.TOMLDecodeError, KeyError) as err:
        return [f"Cargo.toml: no [workspace.package] version ({err})"]
    if headings[0] != version:
        return [
            f"RELEASE_NOTES.md: newest heading is {headings[0]}, the workspace "
            f"is {version}. CHANGELOG.md holds the release history."
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
            found.append(f"{path}:{line_of(text, m.start())}: stale claim. Fact: {fact}.")
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
    found = []
    event_rs, architecture, cargo = (
        read("autumn-harvest/src/event.rs"),
        read("docs/architecture.md"),
        read("Cargo.toml"),
    )
    for rel, text in (
        ("autumn-harvest/src/event.rs", event_rs),
        ("docs/architecture.md", architecture),
        ("Cargo.toml", cargo),
    ):
        if text is None:
            found.append(f"{rel}: missing. Update docs/audits/doc-claim-drift.py.")
    if event_rs is not None:
        actual = count_variants(event_rs)
        if actual is None:
            found.append("autumn-harvest/src/event.rs: no `pub enum WorkflowEvent`")
        else:
            found += variant_count_findings(reader_docs(), actual)
    if architecture is not None:
        found += duplicate_row_findings("docs/architecture.md", architecture)
    if cargo is not None:
        found += release_notes_findings(read("RELEASE_NOTES.md") or "", cargo)
    found += stale_claim_findings(read, STALE_CLAIMS)
    for message in found:
        print(message)
    print(f"doc-claim-drift: {len(found)} findings")
    return 1 if found else 0


def self_test():
    enum_src = r"""
    const Q: char = '"';
    const L: &str = r#"pub enum WorkflowEvent { Fake }"#;
    // pub enum WorkflowEvent { Fake }
    #[derive(Debug)]
    #[serde(tag = "type", content = "data")]
    pub enum WorkflowEvent {
        /// A doc comment with { braces } and Words, Like, These.
        Started {
            input: Value,
            nested: Option<Map<String, Value>>,
            tag: &'static str,
        },
        #[serde(rename = "X]")]
        #[doc = concat!["a"]]
        Unit,
        Tuple(
            Box<Inner>,
            Other,
        ),
        Discriminant = 3,
        /* Block, /* Nested, */ Comment { } */
        Last { s: String, c: char },
    }
    pub enum Other { A, B }
    """
    assert count_variants(enum_src) == 5, count_variants(enum_src)
    assert count_variants("pub enum Else { A }") is None

    docs = {
        "a.md": "| `event.rs` | `WorkflowEvent` enum (5 variants, tagged) |",
        "b.md": "Intro.\nThe `WorkflowEvent` enum has\n41 variants.",
        "c.md": "Event enum now has 41 variants.",
        "d.md": "`WorkflowEvent` gains 2 variants in issue #140.",
        "e.md": "`WorkflowEvent` has 1,050 variants.",
    }
    found = variant_count_findings(docs, 5)
    assert len(found) == 2, found
    assert found[0].startswith("b.md:2"), found
    assert "says 1050" in found[1], found

    table = (
        "| Module | P |\n|---|---|\n| `a.rs` | 1 |\n| `a.rs` | 2 |\n"
        "| `x \\| y` | 1 |\n| `x \\| z` | 1 |\n\n| `a.rs` | 3 |\n"
    )
    found = duplicate_row_findings("x.md", table)
    assert len(found) == 1 and "x.md:4" in found[0], found

    cargo = (
        '[workspace]\nmembers = []\n\n[workspace.package]\n'
        'authors = ["A"]\nversion = "0.7.0"\n'
    )
    assert release_notes_findings("", cargo) == []
    assert release_notes_findings("Pointer to CHANGELOG.md.\n", cargo) == []
    assert release_notes_findings("## [0.7.0] - x\n## [0.6.0] - y\n", cargo) == []
    assert release_notes_findings("```\n## [0.1.0]\n```\n## 0.7.0\n", cargo) == []
    found = release_notes_findings("<!-- -->\n## [0.4.0] - x\n", cargo)
    assert len(found) == 1 and "0.4.0" in found[0], found

    planned, region = STALE_CLAIMS[0][1], STALE_CLAIMS[1][1]
    files = {
        "d.md": "**Planned:** cross-region DR ([#954](u)).",
        "e.md": "Unplanned failover is in [#954](u). Planned windows.\n\nSee [#954](u).",
        "f.md": "There is no built-in cross-region replication\n  or failover.",
        "g.md": "Watch for no cross-region replication lag.",
        "h.md": 'It ended at *"cross-shard rebalancing of existing\nworkflows is out of scope"*.',
        "i.md": "Cross-shard rebalancing of existing\nworkflows is out of scope.",
        "j.md": "Online rebalancing of a running workflow is not supported.",
    }
    claims = [
        ("d.md", planned, "shipped"),
        ("e.md", planned, "shipped"),
        ("f.md", region, "shipped"),
        ("g.md", region, "shipped"),
        ("h.md", REBALANCING, "shipped"),
        ("i.md", REBALANCING, "shipped"),
        ("j.md", REBALANCING, "shipped"),
        ("gone.md", r"x", "y"),
    ]
    found = stale_claim_findings(files.get, claims)
    assert [m.split(":")[0] for m in found] == ["d.md", "f.md", "i.md", "gone.md"], found
    assert "shipped" in found[0] and "missing" in found[3], found

    # Every real pattern compiles.
    for _, pattern, _ in STALE_CLAIMS:
        re.compile(pattern)

    print("doc-claim-drift self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
