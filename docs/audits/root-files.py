#!/usr/bin/env python3
"""Fail on a tracked file at the repository root that is not in ALLOWED.

The root holds build config, licences and the few documents that GitHub and
tooling expect there. Everything else has a home under a directory. A design
record goes in `docs/design/`, a plan in `docs/plans/`, an R&D note in
`docs/rnd/`, and an ADR in `docs/adr/`.

Agents copy the layout that they see. One design record at the root became
seventy-five, so this gate keeps the root to a fixed list.

A root file that the repository needs goes in ALLOWED, with a reason. An
ALLOWED entry that is not tracked is also a finding, so the list stays exact.

Usage:
    python3 docs/audits/root-files.py
    python3 docs/audits/root-files.py --self-test
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

ALLOWED = {
    ".dockerignore": "Docker build context filter",
    ".gitattributes": "git attributes",
    ".gitignore": "git ignore rules",
    "AGENTS.md": "agent instructions; agents read it from the root",
    "CHANGELOG.md": "release changelog; git-cliff writes it here",
    "CLAUDE.md": "agent instructions; agents read it from the root",
    "CONTRIBUTING.md": "GitHub links it from the root",
    "Cargo.lock": "workspace lockfile",
    "Cargo.toml": "workspace manifest",
    "Dockerfile": "worker image build",
    "LICENSE-APACHE": "licence",
    "LICENSE-MIT": "licence",
    "README.md": "GitHub renders it from the root",
    "RELEASE_NOTES.md": "release notes; doc-claim-drift.py reads it here",
    "SECURITY.md": "GitHub links it from the root",
    "cliff.toml": "git-cliff config",
    "clippy.toml": "clippy config",
    "deny.toml": "cargo-deny config",
    "harvest-verify.allow.toml": "harvest-verify allow list",
    "rust-toolchain.toml": "toolchain pin",
    "rustfmt.toml": "rustfmt config",
}

HOMES = {
    "DESIGN-": "docs/design/",
    "PLAN": "docs/plans/",
    "ADR": "docs/adr/",
}


def hint(path):
    """Return the directory where a stray root file most likely belongs."""
    upper = path.upper()
    for prefix, home in HOMES.items():
        if upper.startswith(prefix):
            return home
    return "a directory under docs/" if path.endswith(".md") else "a subdirectory"


def findings(paths, allowed):
    """Return one message for each root path that breaks the rule."""
    root = sorted(p for p in paths if "/" not in p)
    found = [
        f"{p}: not allowed at the root; move it to {hint(p)}"
        for p in root
        if p not in allowed
    ]
    tracked = set(root)
    for path in sorted(allowed):
        if path not in tracked:
            found.append(f"{path}: in ALLOWED, but not tracked")
    return found


def tracked_paths():
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout
    # Git stores path bytes. A path that is not UTF-8 must still reach a report.
    return [p for p in out.decode("utf-8", "surrogateescape").split("\0") if p]


def run():
    paths = tracked_paths()
    found = findings(paths, ALLOWED)
    for message in found:
        print(message.encode("utf-8", "backslashreplace").decode("utf-8"))
    print(f"root-files: {len(ALLOWED)} allowed, {len(found)} findings")
    if found:
        print(
            "Fix: `git mv` the file into its home. If the root must hold it, "
            "add it to ALLOWED in docs/audits/root-files.py with a reason."
        )
    return 1 if found else 0


def self_test():
    allow = {"README.md": "x", "Cargo.toml": "x"}

    assert findings(["README.md", "Cargo.toml", "docs/design/DESIGN-1.md"], allow) == []

    found = findings(["README.md", "Cargo.toml", "DESIGN-2308.md"], allow)
    assert found == [
        "DESIGN-2308.md: not allowed at the root; move it to docs/design/"
    ], found

    found = findings(["README.md", "Cargo.toml", "PLAN-x.md", "notes.md", "a.sh"], allow)
    assert found == [
        "PLAN-x.md: not allowed at the root; move it to docs/plans/",
        "a.sh: not allowed at the root; move it to a subdirectory",
        "notes.md: not allowed at the root; move it to a directory under docs/",
    ], found

    found = findings(["README.md"], allow)
    assert found == ["Cargo.toml: in ALLOWED, but not tracked"], found

    print("root-files self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
