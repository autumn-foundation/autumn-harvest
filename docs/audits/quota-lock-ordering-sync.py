#!/usr/bin/env python3
"""Quota-lock-ordering clone-class sync check for debounce.rs / throttle.rs.

Deterministic, reproducible on any checkout — no network access, no build.

`autumn-harvest/src/debounce.rs` and `autumn-harvest/src/throttle.rs` each
carry a byte-identical copy of the batch quota-lock-ordering algorithm that
closes the ABBA deadlock issue #1230 Finding 2 describes (two concurrent
scanner transactions claiming disjoint due-row batches that need the same
quota locks in opposite order). PR #1480's review history shows the two
copies co-edited in lockstep across roughly a dozen review rounds — every
fix to the algorithm (the sort-instead-of-prelock redesign, the
hashtext-collision fix, the pure-function split) landed in both files in
the same commit.

An Echo duplication survey (this repo's scheduled clone-detection agent,
issue #1695) confirmed the two copies are still byte-identical and
evaluated merging them into one shared helper. It did not: the clone
class has exactly 2 instances and no missed-fix defect is on record (a
fix landing in one copy and staying stale in the other), so it falls
short of the project's 2-instances-needs-a-missed-fix merge bar.

Falling short of that bar does not make the risk go away — a future fix
applied to one copy and not its sibling would be exactly the missed-fix
defect the bar is designed to catch, and it would fail silently: nothing
else in this crate reads or calls across the two files to notice. This
script is the substitute for the abstraction: it fails CI the moment the
two copies diverge, so the sync work review already had to do by hand
across PR #1480 stays enforced automatically instead of by convention.

Scope is the four functions confirmed byte-identical by direct diff:
`resolve_quota_lock_ids`, `order_rows_by_quota_lock_id`,
`snapshot_quota_policies`, and `order_due_rows_for_deadlock_free_firing`
(the wrapper composing the other three — Codex review on PR #1696 found
this fourth function belonged in the tracked set too, since a change to
how it composes them would drift the two copies while leaving the other
three individually unchanged). `resolve_row_quota_lock_key` is
intentionally excluded — it destructures each file's own `FireDueRow`
shape, so it can never be identical text and is not part of this clone
class.

Usage:
    python3 docs/audits/quota-lock-ordering-sync.py

Exit code is 1 if either copy is missing a tracked function, or if any
tracked function's text has diverged between the two files; 0 otherwise.
"""
import difflib
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEBOUNCE = REPO_ROOT / "autumn-harvest" / "src" / "debounce.rs"
THROTTLE = REPO_ROOT / "autumn-harvest" / "src" / "throttle.rs"

# The clone class confirmed byte-identical between debounce.rs and
# throttle.rs (issue #1230 Finding 2 / PR #1480). Deliberately excludes
# resolve_row_quota_lock_key, which is per-file by construction.
TRACKED_FUNCTIONS = [
    "resolve_quota_lock_ids",
    "order_rows_by_quota_lock_id",
    "snapshot_quota_policies",
    "order_due_rows_for_deadlock_free_firing",
]

FN_SIGNATURE_RE_TEMPLATE = r"\n(?:async )?fn {name}\s*\("


def extract_function(text: str, name: str) -> str | None:
    """Return `name`'s full source (signature through closing brace).

    Finds the signature line, then the first `{`, then matches braces to
    that function's own closing `}`. Doc comments and attributes above the
    signature are not included — deliberately, so the two copies' shared
    line-number-dependent `#[cfg(...)]` placement never causes a false
    divergence unrelated to the algorithm itself.
    """
    sig_re = re.compile(FN_SIGNATURE_RE_TEMPLATE.format(name=re.escape(name)))
    m = sig_re.search(text)
    if not m:
        return None
    start = m.start() + 1  # skip the leading newline
    open_brace = text.index("{", m.end())
    depth = 1
    i = open_brace + 1
    while depth > 0:
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
        i += 1
    return text[start:i]


def main() -> int:
    debounce_text = DEBOUNCE.read_text(encoding="utf-8")
    throttle_text = THROTTLE.read_text(encoding="utf-8")

    print(
        "Quota-lock-ordering clone-class sync check — "
        f"{len(TRACKED_FUNCTIONS)} tracked functions, "
        f"{DEBOUNCE.relative_to(REPO_ROOT)} vs {THROTTLE.relative_to(REPO_ROOT)}\n"
    )

    failures = 0
    for name in TRACKED_FUNCTIONS:
        a = extract_function(debounce_text, name)
        b = extract_function(throttle_text, name)

        if a is None or b is None:
            failures += 1
            missing_from = []
            if a is None:
                missing_from.append(str(DEBOUNCE.relative_to(REPO_ROOT)))
            if b is None:
                missing_from.append(str(THROTTLE.relative_to(REPO_ROOT)))
            print(f"FAIL {name}: not found in {', '.join(missing_from)}")
            continue

        if a == b:
            print(f"OK   {name}: byte-identical ({len(a)} chars)")
            continue

        failures += 1
        print(f"FAIL {name}: copies have diverged")
        diff = difflib.unified_diff(
            a.splitlines(),
            b.splitlines(),
            fromfile=f"debounce.rs::{name}",
            tofile=f"throttle.rs::{name}",
            lineterm="",
        )
        for line in diff:
            print(f"  {line}")

    print()
    if failures:
        print(
            f"{failures} of {len(TRACKED_FUNCTIONS)} tracked functions failed "
            "(fails CI). A fix to the quota-lock-ordering algorithm (issue "
            "#1230 Finding 2) must be applied identically to both "
            "debounce.rs and throttle.rs, or a tracked function was "
            "renamed/removed — update TRACKED_FUNCTIONS in this script to "
            "match."
        )
    else:
        print("All tracked functions are byte-identical between the two copies.")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
