#!/usr/bin/env python3
"""Fail on a tracked build artifact or patch leftover (issue #1831).

A tracked file fails when one of these rules matches:

1. Its name ends in a patch leftover suffix, `.orig` or `.rej`.
2. Its content is binary. The rule is the one git uses: a NUL byte in the
   first 8000 bytes.

A binary file that the repository needs goes in ALLOWED, with a reason. An
ALLOWED entry that is not tracked is also a finding, so the list stays exact.

Usage:
    python3 docs/audits/tracked-artifacts.py
    python3 docs/audits/tracked-artifacts.py --self-test
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

LEFTOVER_SUFFIXES = (".orig", ".rej")

# Git reads this many bytes to decide if a file is binary.
SNIFF_BYTES = 8000

ALLOWED = {
    "autumn-harvest/examples/wasm-guests/echo.wasm": (
        "a compiled guest that a test runs; see its README"
    ),
}


def findings(paths, read_head, allowed):
    """Return one message for each rule that a tracked path breaks."""
    found = []
    for path in paths:
        if path.endswith(LEFTOVER_SUFFIXES):
            found.append(f"{path}: patch leftover")
            continue
        if path in allowed:
            continue
        head = read_head(path)
        if head is not None and b"\0" in head[:SNIFF_BYTES]:
            found.append(f"{path}: binary file")
    tracked = set(paths)
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


def read_head(path):
    """Return the first bytes of a regular file, or None to skip it."""
    full = ROOT / path
    # A symlink holds a path, and a gitlink is a directory.
    if os.path.islink(full) or not full.is_file():
        return None
    with open(full, "rb") as f:
        return f.read(SNIFF_BYTES)


def run():
    paths = tracked_paths()
    found = findings(paths, read_head, ALLOWED)
    for message in found:
        print(message.encode("utf-8", "backslashreplace").decode("utf-8"))
    print(f"tracked-artifacts: {len(paths)} files, {len(found)} findings")
    if found:
        print(
            "Fix: `git rm` the file. If the repository needs a binary, add it "
            "to ALLOWED in docs/audits/tracked-artifacts.py with a reason."
        )
    return 1 if found else 0


def self_test():
    files = {
        "src/lib.rs": b"fn main() {}\n",
        "empty.txt": b"",
        "late-nul.txt": b"a" * SNIFF_BYTES + b"\0",
        "src/lib.rs.orig": b"fn main() {}\n",
        "fix.rej": b"@@ -1 +1 @@\n",
        "test_debug": b"\x7fELF\x02\x01\x01\0\0\0",
        "guest.wasm": b"\0asm\x01\0\0\0",
        "link": None,
    }

    def check(names, allow=None):
        return findings(names, files.get, allow or {})

    assert check(["src/lib.rs", "empty.txt", "late-nul.txt", "link"]) == []
    assert check(["guest.wasm"], allow={"guest.wasm": "fixture"}) == []
    assert "binary" in check(["guest.wasm"])[0]

    found = check(["src/lib.rs.orig", "fix.rej"])
    assert len(found) == 2, found
    assert all("patch leftover" in m for m in found), found

    found = check(["test_debug"])
    assert len(found) == 1 and "binary" in found[0], found

    found = check(["src/lib.rs"], allow={"gone.bin": "fixture"})
    assert len(found) == 1 and "gone.bin" in found[0], found

    # ALLOWED exempts a binary, never a patch leftover.
    found = check(["src/lib.rs.orig"], allow={"src/lib.rs.orig": "x"})
    assert len(found) == 1 and "patch leftover" in found[0], found

    print("tracked-artifacts self-test: OK")
    return 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
