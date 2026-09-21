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

Scope is five functions confirmed byte-identical (four outright, one after
normalizing one known field-name difference — see `FIELD_NORMALIZATIONS`
below) by direct diff: `resolve_quota_lock_ids`,
`order_rows_by_quota_lock_id`, `snapshot_quota_policies`,
`order_due_rows_for_deadlock_free_firing` (the wrapper composing the first
three), and `resolve_row_quota_lock_key`. Codex review on PR #1696 found
both the wrapper and the resolver belonged in the tracked set: a change to
how the wrapper composes the other functions, or to the resolver's
policy/key logic, would drift the two copies while every other tracked
function stayed individually unchanged.

`resolve_row_quota_lock_key` reads its row's input from a field each
file's own `FireDueRow` names differently (`last_input` in debounce.rs,
`input` in throttle.rs) — the one structural difference the two scanners'
row shapes actually require. `FIELD_NORMALIZATIONS` substitutes a common
placeholder for that one field access before comparing, so the rest of
the function (the policy lookup, the `has_any_cap` guard, the resolved-key
construction) is still checked byte-for-byte.

Text identity is not the whole invariant: `order_due_rows_for_deadlock_free_firing`
being correct and unchanged does not help if a fire path stops calling it.
Codex review on PR #1696 also found that gap: this script did not check
that either scanner's claim loop still calls the wrapper, so removing or
bypassing that one call site would restore claim-order firing (and its
ABBA deadlock risk) while every tracked function stayed identical.
`CALL_SITE_GUARDS` closes it: for each scanner's `fire_due_on_conn`, it
asserts the wrapper call appears, and appears before the per-row firing
loop starts.

Usage:
    python3 docs/audits/quota-lock-ordering-sync.py

Exit code is 1 if either copy is missing a tracked function, if any
tracked function's text has diverged between the two files, or if a
guarded call site no longer calls the ordering wrapper before its firing
loop; 0 otherwise.
"""
import difflib
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEBOUNCE = REPO_ROOT / "autumn-harvest" / "src" / "debounce.rs"
THROTTLE = REPO_ROOT / "autumn-harvest" / "src" / "throttle.rs"

# The clone class confirmed byte-identical (outright, or after
# FIELD_NORMALIZATIONS) between debounce.rs and throttle.rs (issue #1230
# Finding 2 / PR #1480).
TRACKED_FUNCTIONS = [
    "resolve_quota_lock_ids",
    "order_rows_by_quota_lock_id",
    "snapshot_quota_policies",
    "order_due_rows_for_deadlock_free_firing",
    "resolve_row_quota_lock_key",
]

# Per-function field-name normalization: a tracked function whose two
# copies read one field under a name the row type itself forces to
# differ. Each entry replaces a debounce.rs-side substring and a
# throttle.rs-side substring with the same placeholder before comparing,
# so the rest of the function is still held to a byte-identical bar. A
# function not listed here is compared with no normalization at all.
FIELD_NORMALIZATIONS: dict[str, tuple[str, str]] = {
    "resolve_row_quota_lock_key": ("row.last_input", "row.input"),
}
NORMALIZED_PLACEHOLDER = "row.__normalized_input_field__"

# Each entry names an enclosing function, in both files, that must call
# `wrapper_call` before `loop_pattern`'s first match — the invariant that
# a claimed batch is reordered before any row fires. This is a structural
# check on the CALLER, not a text comparison of the wrapper itself.
CALL_SITE_GUARDS = [
    {
        "enclosing_fn": "fire_due_on_conn",
        "wrapper_call": "order_due_rows_for_deadlock_free_firing",
        "loop_pattern": re.compile(r"for\s+\w+\s+in\s+due_rows\b"),
    },
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


def check_call_site_guard(
    text: str, file_label: str, enclosing_fn: str, wrapper_call: str, loop_pattern: re.Pattern
) -> str | None:
    """Return a failure message, or `None` if the guard holds.

    Finds `enclosing_fn`'s body, then requires a call to `wrapper_call`
    that appears strictly before `loop_pattern`'s first match inside that
    same body. Either match missing, or the call appearing at or after
    the loop, is a failure: the ordering wrapper must run before any row
    in the claimed batch fires.
    """
    body = extract_function(text, enclosing_fn)
    if body is None:
        return f"{file_label}: enclosing function `{enclosing_fn}` not found"

    call_match = re.search(re.escape(wrapper_call) + r"\s*\(", body)
    if call_match is None:
        return f"{file_label}::{enclosing_fn}: no call to `{wrapper_call}` found"

    loop_match = loop_pattern.search(body)
    if loop_match is None:
        return f"{file_label}::{enclosing_fn}: no `{loop_pattern.pattern}` firing loop found"

    if call_match.start() >= loop_match.start():
        return (
            f"{file_label}::{enclosing_fn}: `{wrapper_call}` is called at or after "
            "the firing loop starts, not before it"
        )
    return None


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

        a_compared, b_compared = a, b
        normalized_note = ""
        if name in FIELD_NORMALIZATIONS:
            debounce_pattern, throttle_pattern = FIELD_NORMALIZATIONS[name]
            a_compared = a.replace(debounce_pattern, NORMALIZED_PLACEHOLDER)
            b_compared = b.replace(throttle_pattern, NORMALIZED_PLACEHOLDER)
            normalized_note = (
                f" (after normalizing `{debounce_pattern}` / `{throttle_pattern}`)"
            )

        if a_compared == b_compared:
            print(f"OK   {name}: byte-identical{normalized_note} ({len(a)} chars)")
            continue

        failures += 1
        print(f"FAIL {name}: copies have diverged{normalized_note}")
        diff = difflib.unified_diff(
            a_compared.splitlines(),
            b_compared.splitlines(),
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

    print()
    guard_failures = 0
    for guard in CALL_SITE_GUARDS:
        for label, text in ((str(DEBOUNCE.relative_to(REPO_ROOT)), debounce_text),
                            (str(THROTTLE.relative_to(REPO_ROOT)), throttle_text)):
            error = check_call_site_guard(
                text, label, guard["enclosing_fn"], guard["wrapper_call"], guard["loop_pattern"]
            )
            if error is None:
                print(
                    f"OK   {label}::{guard['enclosing_fn']} calls "
                    f"`{guard['wrapper_call']}` before its firing loop"
                )
            else:
                guard_failures += 1
                print(f"FAIL {error}")

    print()
    if guard_failures:
        print(
            f"{guard_failures} call-site guard(s) failed (fails CI). A fire path "
            "must call its ordering wrapper before iterating the claimed batch — "
            "see CALL_SITE_GUARDS in this script."
        )
    else:
        print("All guarded call sites invoke their ordering wrapper before firing.")

    return 1 if (failures or guard_failures) else 0


if __name__ == "__main__":
    sys.exit(main())
