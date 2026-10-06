#!/usr/bin/env python3
"""Fail when a doc claim contradicts the shipped code (issue #1831).

Four rules:

1. event-row-count. The `event.rs` row in `docs/architecture.md` states no
   `WorkflowEvent` variant count. A hard-coded count drifts: two rows once
   said 41 and 35 while the enum had 50.
2. duplicate-row. No two rows of one table in `docs/architecture.md` share
   a first cell.
3. release-notes. If `RELEASE_NOTES.md` has a version heading, its newest
   version is the workspace version.
4. stale-claim. Each STALE_CLAIMS pin stays absent from its file. A pin is
   the exact wording that a shipped change made false.

Each rule checks a fixed place or an exact phrase. None of them judges free
prose, because a regex cannot tell a total from a subset, or harvest from a
competitor, with certainty.

Usage:
    python3 docs/audits/doc-claim-drift.py
    python3 docs/audits/doc-claim-drift.py --self-test
"""
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

EVENT_ROW_PREFIX = "| `event.rs` |"
# A count beside "variant", in either order, with only separators between:
# "41 variants", "49 Variants", "Variants: 49", "variants — 1,050".
COUNT_RE = re.compile(
    r"(?i)\b\d[\d,]*[\s:=—-]*variants?\b|\bvariants?\b[\s:=(—-]*\d"
)
# A semver core with an optional prerelease and build part. Heading
# punctuation after it, such as "## 0.7.0: notes", is not part of it.
SEMVER_ID = r"[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*"
VERSION_HEADING_RE = re.compile(
    rf"^##\s+\[?v?(\d+\.\d+\.\d+(?:-{SEMVER_ID})?(?:\+{SEMVER_ID})?)", re.MULTILINE
)
FENCE_RE = re.compile(r"^(```|~~~).*?^\1", re.MULTILINE | re.DOTALL)
CELL_SPLIT_RE = re.compile(r"(?<!\\)\|")

# (path, pattern, the fact that makes the claim false)
#
# Each pattern pins the exact wording that a shipped change made false, as a
# regression check. A pattern does not try to judge new prose: a general
# phrase such as "no cross-region failover" is true of some competitors and
# of running workflows. `\s+` lets a pin match across a line wrap.
STALE_CLAIMS = [
    (
        "docs/comparison.md",
        r"\*\*Planned:\*\*\s+cross-region\s+DR|Planned\s+R&D:\s+\[#954\]",
        "cross-region DR shipped (issue #954), see docs/cross-region-dr.md",
    ),
    (
        "docs/comparison.md",
        r"no\s+built-in\s+multi-region\s+replication\s+or\s+failover\s+today"
        r"|Single-region\s+—\s+no\s+multi-region\s+DR\s+or\s+replication",
        "cross-region DR shipped (issue #954), see docs/cross-region-dr.md",
    ),
    (
        "docs/comparison.md",
        r"\*\*No\s+cross-shard\s+workflows\.\*\*"
        r"|cross-shard\s+workflows\s+are\s+(explicitly\s+)?out\s+of\s+scope",
        "cross-shard child placement shipped (issue #956)",
    ),
    (
        "docs/architecture.md",
        r"Cross-shard\s+rebalancing\s+of\s+existing\s+workflows\s+is\s+out\s+of\s+scope\.",
        "shard rebalancing shipped (issue #964), see shard_rebalance.rs",
    ),
    (
        "docs/sharding.md",
        r"\(cross-shard\s+rebalancing\s+is\s+not\s+supported\)",
        "shard rebalancing shipped (issue #964), see shard_rebalance.rs",
    ),
    (
        "docs/architecture.md",
        r"cross-shard\s+child\s+fan-out\s+is\s+out\s+of\s+scope",
        "the `_placed` fan-out variants take a ChildPlacement (issue #956)",
    ),
    (
        "docs/sharding.md",
        r"geo-replication\s*/\s*cross-region\s+failover",
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


def line_of(text, offset):
    return text.count("\n", 0, offset) + 1


def markdown_lines(text):
    """Yield (line number, line) outside fenced blocks."""
    fence = None
    for n, line in enumerate(text.splitlines(), 1):
        marker = re.match(r"\s*(```|~~~)", line)
        if marker:
            fence = None if fence == marker.group(1) else fence or marker.group(1)
            continue
        if fence is None:
            yield n, line


def event_row_count_findings(path, text):
    """Return a message for each variant count in an `event.rs` row."""
    found = []
    for n, line in markdown_lines(text):
        if line.startswith(EVENT_ROW_PREFIX):
            for m in COUNT_RE.finditer(line):
                found.append(
                    f"{path}:{n}: the `event.rs` row states \"{m.group()}\". "
                    "Drop the count: it drifts from event.rs."
                )
    return found


def duplicate_row_findings(path, text):
    """Return a message for each repeated first cell within one table."""
    found, seen, last = [], None, 0
    for n, line in markdown_lines(text):
        if n != last + 1:
            # A fenced block between two lines ends the table.
            seen = None
        last = n
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


def read(rel):
    path = ROOT / rel
    return path.read_text(encoding="utf-8") if path.is_file() else None


def run():
    found = []
    architecture, cargo = read("docs/architecture.md"), read("Cargo.toml")
    for rel, text in (("docs/architecture.md", architecture), ("Cargo.toml", cargo)):
        if text is None:
            found.append(f"{rel}: missing. Update docs/audits/doc-claim-drift.py.")
    if architecture is not None:
        found += event_row_count_findings("docs/architecture.md", architecture)
        found += duplicate_row_findings("docs/architecture.md", architecture)
    if cargo is not None:
        found += release_notes_findings(read("RELEASE_NOTES.md") or "", cargo)
    found += stale_claim_findings(read, STALE_CLAIMS)
    for message in found:
        print(message)
    print(f"doc-claim-drift: {len(found)} findings")
    return 1 if found else 0


def self_test():
    rows = (
        "| `event.rs` | 1 | `WorkflowEvent` enum (41 variants, tagged). |\n"
        "| `event.rs` | 1 | `WorkflowEvent` enum — 1,050 variants. |\n"
        "| `event.rs` | 1 | `WorkflowEvent` enum. Variants: 49. |\n"
        "| `event.rs` | 1 | `WorkflowEvent` enum (49 Variants). |\n"
        "| `event.rs` | 1 | `WorkflowEvent` enum. Variants added in issue #140. |\n"
        "```\n| `event.rs` | 1 | 7 variants, in a sample. |\n```\n"
        "| `queue.rs` | 2 | Handles 3 variants of `TaskType`. |\n"
        "Prose: `WorkflowEvent` has 2 variants for sessions.\n"
    )
    found = event_row_count_findings("x.md", rows)
    assert [m.split(":")[1] for m in found] == ["1", "2", "3", "4"], found
    assert "41 variants" in found[0] and "1,050 variants" in found[1], found

    table = (
        "| Module | P |\n|---|---|\n| `a.rs` | 1 |\n| `a.rs` | 2 |\n"
        "| `x \\| y` | 1 |\n| `x \\| z` | 1 |\n\n| `a.rs` | 3 |\n"
    )
    found = duplicate_row_findings("x.md", table)
    assert len(found) == 1 and "x.md:4" in found[0], found
    sample = "```text\n| state |\n| state |\n```\n| `b.rs` | 1 |\n"
    assert duplicate_row_findings("x.md", sample) == []
    split = "| `c.rs` | 1 |\n```\nx\n```\n| `c.rs` | 2 |\n"
    assert duplicate_row_findings("x.md", split) == []

    cargo = (
        '[workspace]\nmembers = []\n\n[workspace.package]\n'
        'authors = ["A"]\nversion = "0.7.0"\n'
    )
    assert release_notes_findings("", cargo) == []
    assert release_notes_findings("Pointer to CHANGELOG.md.\n", cargo) == []
    assert release_notes_findings("## [0.7.0] - x\n## [0.6.0] - y\n", cargo) == []
    assert release_notes_findings("```\n## [0.1.0]\n```\n## 0.7.0\n", cargo) == []
    assert release_notes_findings("## 0.7.0: Release notes\n", cargo) == []
    assert release_notes_findings("## v0.7.0.\n", cargo) == []
    found = release_notes_findings("## [0.7.0-rc.1+b.5] - x\n", cargo)
    assert len(found) == 1 and "0.7.0-rc.1+b.5" in found[0], found
    found = release_notes_findings("<!-- -->\n## [0.4.0] - x\n", cargo)
    assert len(found) == 1 and "0.4.0" in found[0], found

    old_claims = {
        "docs/comparison.md": (
            "**Planned:** cross-region DR via logical replication.\n"
            "  Planned R&D: [#954](u)\n"
            "There is no built-in multi-region replication or failover today.\n"
            "- **Single-region — no multi-region DR or replication.** Text.\n"
            "cross-shard workflows are explicitly out of scope per the contract.\n"
            "and cross-shard workflows are out of\n  scope by design.\n"
            "- **No cross-shard workflows.** A single workflow's state.\n"
        ),
        "docs/architecture.md": (
            "Cross-shard rebalancing of existing workflows is out of scope.\n"
            "(cross-shard child fan-out is out of scope, consistent with it).\n"
        ),
        "docs/sharding.md": (
            "out of scope (cross-shard rebalancing is not supported).\n"
            "per-shard worker assignment, geo-replication / cross-region failover, and\n"
        ),
    }
    claims = [c for c in STALE_CLAIMS if c[0] in old_claims]
    found = stale_claim_findings(old_claims.get, claims)
    assert len(found) == 11, found

    # True statements that a pin must not match.
    current = {
        "docs/comparison.md": (
            "| DBOS | DBOS has no cross-region replication or failover. |\n"
            "Unlike Harvest, DBOS has no cross-region replication or failover.\n"
            "Unplanned failover is covered by [#954](u).\n"
            "There is no automatic regional failover.\n"
            "Cross-shard composition is limited.\n"
        ),
        "docs/architecture.md": (
            "Cross-shard rebalancing is not supported for running workflows.\n"
            'It ended at *"cross-shard rebalancing of existing\nworkflows is out of scope"*.\n'
        ),
        "docs/sharding.md": "A running workflow cannot move. Cross-region failover is in cross-region-dr.md.\n",
    }
    assert stale_claim_findings(current.get, claims) == []

    found = stale_claim_findings({}.get, [("gone.md", r"x", "y")])
    assert len(found) == 1 and "missing" in found[0], found

    # Every real pattern compiles.
    for _, pattern, _ in STALE_CLAIMS:
        re.compile(pattern)

    print("doc-claim-drift self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
