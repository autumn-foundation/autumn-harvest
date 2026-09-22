#!/usr/bin/env python3
"""Folio corpus harness: `test-db-linux` per-shard weight drift report.

Deterministic, reproducible on any checkout — no network access required,
no cargo build. Report-only (always exits 0): see "Why report-only" below.

`.github/ci/integration-suites.txt` assigns each `linux`-osclass row to a
shard by `row_ordinal % SEMAPHORE_SHARD_COUNT`, where `row_ordinal` is the
row's position counted ONLY among other `linux`-osclass rows, in manifest
order (see `run-suites.sh`'s `do_run`, the actual implementation this
script mirrors). Two rows land on the same shard whenever their ordinals
are exactly `SEMAPHORE_SHARD_COUNT` apart. The manifest grows
alphabetically as suites are added, so that gap silently drifts — a
shard-10 collision between `integration_e2e` and `quota_enforcement_tests`
was found and fixed once (issue #1267, moving `SEMAPHORE_SHARD_COUNT` from
10 to 11); by 2026-09-21 the same two suites' gap had drifted from 10 rows
back onto 11, recreating the exact collision the fix targeted
(docs/rnd/2026-09-21-ci-health-semaphore-quota-outbox-recurrence.md).

That report, and the four before it in the same series, computed
`row_ordinal` from a plain line count over the WHOLE manifest file
(comments, blank lines, and non-`linux` rows included) rather than the
`linux`-only count `do_run` actually uses. The two numbering schemes agree
on whether two rows are the same distance apart (a fixed offset survives
either way), which is why those reports' headline finding still held, but
they do not agree on which shard a given row lands in — this script
counts the way `do_run` counts, so its shard numbers are the ones that
match a real CI run's job names.

That same 2026-09-21 report closed by asking for "a full per-shard test-
weight accounting... so the invariant cannot silently drift again". This
script is that accounting, run automatically instead of by hand:

1. Read the manifest exactly as `run-suites.sh`'s `records()` does (strip
   comment and blank lines), keep `linux`-osclass rows in order, and
   number them the way `do_run`'s `row_ordinal` does.
2. Resolve each row to the test file it runs: an `autumn-harvest`/
   `integration` row with a filter names a module under
   `autumn-harvest/tests/integration/<filter>.rs`; every other row names
   `<crate>/tests/<target>.rs` directly.
3. Weigh each row by its file's runnable-test attribute count: `#[test]`
   and `#[tokio::test]` (matching every argument variant, e.g.
   `#[tokio::test(flavor = "multi_thread")]`, any indentation, since a
   nested-module test is indented, and excluding any test also carrying
   `#[ignore]` — see `test_weight()`'s own docstring for two rounds of
   Codex-review corrections to this counting: an indentation/plain-`#[test]`
   undercount, a multi-line-attribute undercount, and an `#[ignore]`
   overcount, in that order. Every number in this docstring reflects all
   three fixes.
4. Read `SEMAPHORE_SHARD_COUNT` out of the `test-db-linux` job block in
   `ci.yml` (not hand-copied), and report each shard's total weight plus
   any shard carrying more than one row at or above HEAVY_THRESHOLD.

A run against today's manifest (2026-09-22) finds **seven** shards, not one,
carrying 2+ suites at or above HEAVY_THRESHOLD under today's
`SEMAPHORE_SHARD_COUNT = 11` — every one of them is a genuine
`SEMAPHORE_SHARD_COUNT`-apart-ordinal collision, not just the two this
docstring's earlier drafts singled out for narrative reasons. **Correction
(Codex review):** naming only two here read as if they were the only ones
the script finds, which is not what a run of the script itself shows.  The
full set, from this script's own output:

| Shard | Colliding rows (ordinal, weight) |
|---|---|
| 0  | `integration_e2e` (33, 120), `quota_enforcement_tests` (44, 47), `backup_verify_tests` (77, 57), `api_scheduler_integration` (88, 80), `interface_schema_integration` (110, 31) |
| 1  | `transactional_start_tests` (67, 36), `codec_rotation_db_tests` (78, 56) |
| 2  | `capability_miss_tests` (13, 46), `cross_region_dr_tests` (79, 31) |
| 3  | `pacing_override_integration` (113, 46), `workflow_rerun_integration` (146, 68) |
| 6  | `admission_gate_authoritative` (6, 34), `shard_rebalance_db_tests` (83, 111) |
| 7  | `audit_export_tests` (7, 88), `event_partitioning_tests` (29, 155) |
| 10 | `queue_pause_tests` (43, 44), `hot_code_swap_tests` (76, 79), `stall_diagnosis_integration` (131, 76) |

Shard 7 also runs `claim_budget_tests` (ordinal 18) — the same
`SEMAPHORE_SHARD_COUNT`-apart pattern as its two listed neighbors — but at a
corrected weight of 29 (7 of its 36 matched attributes are `#[ignore]`d
one-shot evidence generators that never run in CI) it falls just under
HEAVY_THRESHOLD, so shard 7 is a genuine two-suite heavy collision, not
three. **Correction (Codex review, this harness's own second PR round):**
an earlier version of this table listed `claim_budget_tests` as shard 7's
third heavy member at its uncorrected weight of 36; fixed once the
`#[ignore]` exclusion above landed.

Shards 0 and 7 remain the two worst by both row count and total weight, and
shard 0's `integration_e2e`/`quota_enforcement_tests` pair is the only one a
prior report in this series (09-21) had already found — the other six are
new to this script. (That same 09-21 report's own manifest-wide check placed
`event_partitioning_tests` on "shard 6"; that placement used the whole-file
line count described above, not the `linux`-only one, which is the same
discrepancy this docstring's third paragraph describes.)

A sweep of `SEMAPHORE_SHARD_COUNT` from 9 through 24 finds NO value with
zero heavy-suite collisions: today's manifest carries roughly twenty rows
at or above HEAVY_THRESHOLD, round-robin assignment across single-digit or
low-double-digit shard counts pigeonholes several of them together
whichever count is chosen, and which suites collide shifts unpredictably
as the count changes. Bumping `SEMAPHORE_SHARD_COUNT` again would likely
just relocate today's collision rather than remove the failure mode —
this script's job is to keep that visible, not to pick the next number.

Why report-only: gating this now would redden every open PR on a
pre-existing structural property nobody caused and this script cannot fix
(the real fix is either a weight-aware assignment in `run-suites.sh`,
replacing the modulo scheme, or a periodic re-simulation before each
`SEMAPHORE_SHARD_COUNT` change — a bigger decision than one CI-health
session's single, unreviewed pass should gate on unilaterally). `main()`
below always `return`s 0; once the underlying assignment is fixed, change
that to fail when `heavy_collisions` is non-empty, so the invariant cannot
silently drift again.

Usage:
    python3 docs/audits/shard-weight-drift.py [--threshold N] [--sweep]

Exit code is always 0 (see "Why report-only" above); `--sweep` additionally
prints the N=9..24 collision sweep this docstring's findings came from.
"""
import argparse
import re
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MANIFEST = REPO_ROOT / ".github" / "ci" / "integration-suites.txt"
CI_YAML = REPO_ROOT / ".github" / "workflows" / "ci.yml"

HEAVY_THRESHOLD_DEFAULT = 30
# Optional leading whitespace (nested-module tests are indented) and either
# `#[test]` or `#[tokio::test...]` (any argument variant). An earlier version
# anchored to column 0 and matched only `#[tokio::test`, which silently
# undercounted every indented test and every plain `#[test]` — caught by
# Codex review on this harness's own first PR (see the module docstring's
# "correction" note below for the concrete files this changed).
TEST_ATTR_RE = re.compile(r"^\s*#\[(?:tokio::test|test)\b")
IGNORE_ATTR_RE = re.compile(r"^\s*#\[ignore\b")
ATTR_LINE_RE = re.compile(r"^\s*#\[")
FN_LINE_RE = re.compile(r"^\s*(?:pub\s+)?(?:async\s+)?fn\s")
COMMENT_OR_BLANK_RE = re.compile(r"^\s*(#|$)")


def parse_manifest_records(text):
    """Mirror run-suites.sh's `records()`: strip comment/blank lines."""
    records = []
    for line in text.splitlines():
        if COMMENT_OR_BLANK_RE.match(line):
            continue
        parts = line.split()
        if len(parts) != 5:
            continue
        records.append(parts)
    return records


def linux_rows_in_order(records):
    """Mirror do_run's row_ordinal: position among `linux`-osclass rows only."""
    return [r for r in records if r[0] == "linux"]


def resolve_test_file(crate, target, filt):
    if crate == "autumn-harvest" and target == "integration" and filt != "-":
        integration_dir = REPO_ROOT / "autumn-harvest" / "tests" / "integration"
        direct = integration_dir / f"{filt}.rs"
        if direct.exists():
            return direct
        # A handful of manifest filters are a cargo-test substring filter
        # against test NAMES, shorter than the module's own file stem (e.g.
        # filter "admission_gate_authoritative" selects module file
        # "admission_gate_authoritative_tests.rs"), rather than the module
        # name itself. Fall back to the "_tests" spelling before giving up.
        suffixed = integration_dir / f"{filt}_tests.rs"
        if suffixed.exists():
            return suffixed
        return direct
    if crate == "autumn-harvest" and target == "integration" and filt == "-":
        return REPO_ROOT / "autumn-harvest" / "tests" / "integration.rs"
    return REPO_ROOT / crate / "tests" / f"{target}.rs"


def row_label(crate, target, filt):
    if crate == "autumn-harvest" and target == "integration" and filt != "-":
        return f"{crate}/{target} -- {filt}"
    return f"{crate}/{target}"


def test_weight(path):
    """Count runnable `#[test]`/`#[tokio::test]` functions in `path`,
    excluding any carrying `#[ignore]` — `run-suites.sh` invokes plain
    `cargo test` with no `--ignored`/`--include-ignored`, so an `#[ignore]`d
    test never actually executes in CI and should not count toward a shard's
    real workload. Caught by Codex review on this harness's own first PR:
    `claim_budget_tests.rs` carries 7 `#[ignore]`d one-shot evidence
    generators (documented in the file itself as "not a repeatable CI
    assertion"), inflating its counted weight from 29 (what actually runs)
    to 36 and fabricating a heavy-suite collision that direct log evidence
    does not support.

    Attributes stack directly above their `fn`/`async fn` line with no
    blank line between (a leading `///` doc-comment block may sit above the
    attribute stack, but never inside it), so a small forward scan — collect
    contiguous `#[...]` lines, decide on the next `fn` line, then reset — is
    accurate without a full Rust parser.

    An attribute can span multiple physical lines two different ways in this
    corpus: a bracketed argument list broken across lines
    (`#[allow(\n    clippy::too_many_lines,\n)]`), and a string literal
    continued with a trailing `\` (`#[ignore = "...\` /
    `            ...script.sh"]`). Both are tracked the same way, by a
    running count of unmatched `[`/`(` vs `]`/`)` on the attribute's own
    text: the first case is unbalanced until its closing `)]`; the second is
    unbalanced from the opening `#[`'s `[` until the closing `]` on the
    string's continuation line (the backslash itself needs no special
    handling — it is just a character to the bracket count). **Correction
    (Codex review, this harness's own second PR round):** an earlier version
    tracked only the trailing-backslash form, which mis-closed a bracketed
    `#[allow(...)]` spanning multiple lines at its first line and lost the
    pending test attribute before reaching `fn` — undercounting
    `hot_code_swap_tests.rs` by 2 real tests (79 vs. the correct 77, once
    also corrected for `#[ignore]` per this function's other fix).
    """
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None
    count = 0
    pending_test = False
    pending_ignore = False
    bracket_depth = 0
    for line in lines:
        if bracket_depth > 0:
            bracket_depth += line.count("[") + line.count("(")
            bracket_depth -= line.count("]") + line.count(")")
            continue
        if ATTR_LINE_RE.match(line):
            if TEST_ATTR_RE.match(line):
                pending_test = True
            if IGNORE_ATTR_RE.match(line):
                pending_ignore = True
            bracket_depth = line.count("[") + line.count("(")
            bracket_depth -= line.count("]") + line.count(")")
            continue
        if FN_LINE_RE.match(line):
            if pending_test and not pending_ignore:
                count += 1
            pending_test = False
            pending_ignore = False
            continue
        stripped = line.strip()
        if stripped == "" or stripped.startswith("//"):
            continue
        # Any other code line between attributes and a fn would be unusual;
        # treat it as ending the pending attribute stack rather than
        # misattributing it to a later, unrelated fn.
        pending_test = False
        pending_ignore = False
    return count


def read_shard_count():
    """Extract SEMAPHORE_SHARD_COUNT from the test-db-linux job block only —
    test-nodb declares the same env var name with a different value (4), so
    a whole-file search would be ambiguous."""
    text = CI_YAML.read_text(encoding="utf-8")
    match = re.search(r"^  test-db-linux:.*?(?=^  \S)", text, re.DOTALL | re.MULTILINE)
    if not match:
        return None
    count_match = re.search(r'SEMAPHORE_SHARD_COUNT:\s*"(\d+)"', match.group(0))
    return int(count_match.group(1)) if count_match else None


def weigh_rows(linux_rows):
    weights = []
    unresolved = []
    for ordinal, (_os, crate, target, _feats, filt) in enumerate(linux_rows):
        path = resolve_test_file(crate, target, filt)
        label = row_label(crate, target, filt)
        weight = test_weight(path)
        if weight is None:
            unresolved.append((ordinal, label, path))
            weight = 0
        weights.append((ordinal, label, weight))
    return weights, unresolved


def shard_report(weights, shard_count, heavy_threshold):
    totals = [0] * shard_count
    members = [[] for _ in range(shard_count)]
    for ordinal, label, weight in weights:
        shard = ordinal % shard_count
        totals[shard] += weight
        members[shard].append((ordinal, label, weight))
    heavy_collisions = []
    for shard in range(shard_count):
        heavy = [m for m in members[shard] if m[2] >= heavy_threshold]
        if len(heavy) > 1:
            heavy_collisions.append((shard, heavy))
    return totals, members, heavy_collisions


def sweep(weights, heavy_threshold, lo=9, hi=24):
    print(f"\n--- Shard-count sweep N={lo}..{hi} (heavy >= {heavy_threshold}) ---")
    for n in range(lo, hi + 1):
        totals, _members, collisions = shard_report(weights, n, heavy_threshold)
        spread = max(totals) - min(totals)
        print(
            f"N={n:2d} spread={spread:4d} max={max(totals):4d} "
            f"min={min(totals):4d} heavy_collisions={len(collisions)}"
        )


def self_test():
    """The harness is only as good as its parser: check it against a small,
    known-shape fixture matching the real manifest's five-column format."""
    fixture = """\
# comment line, skipped
   # indented comment, skipped

linux        crate-a  integration  -  mod_a
linux        crate-a  integration  -  mod_b
allos        crate-a  integration  -  mod_c
linux        crate-b  suite_x      -  -
"""
    records = parse_manifest_records(fixture)
    assert len(records) == 4, f"expected 4 non-comment records, got {len(records)}"
    linux_rows = linux_rows_in_order(records)
    assert len(linux_rows) == 3, f"expected 3 linux rows, got {len(linux_rows)}"
    assert linux_rows[0][4] == "mod_a"
    assert linux_rows[1][4] == "mod_b"
    assert linux_rows[2][2] == "suite_x"
    assert row_label("autumn-harvest", "integration", "quota_enforcement_tests") == (
        "autumn-harvest/integration -- quota_enforcement_tests"
    )
    assert row_label("autumn-harvest-plugin", "ui_integration", "-") == (
        "autumn-harvest-plugin/ui_integration"
    )

    # test_weight: plain #[test], indented #[tokio::test], an #[ignore]d one
    # (with a backslash-continued reason string, matching the real corpus's
    # shape), and a doc comment sitting above an attribute stack — none of
    # which should confuse the scan.
    fixture_rs = """\
/// Some doc comment.
#[test]
fn plain_test() {}

mod nested {
    #[tokio::test(flavor = "multi_thread")]
    async fn nested_test() {}
}

/// `#[ignore]`d on purpose: a one-shot evidence generator.
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- run via \\
            some/script.sh"]
#[allow(clippy::too_many_lines)]
async fn ignored_test() {}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    clippy::needless_return
)]
async fn multiline_bracketed_attr_test() {}

fn not_a_test() {}
"""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".rs", delete=False
    ) as tmp:
        tmp.write(fixture_rs)
        tmp_path = Path(tmp.name)
    try:
        weight = test_weight(tmp_path)
    finally:
        tmp_path.unlink()
    assert weight == 3, f"expected 3 runnable tests (excluding the ignored one), got {weight}"
    assert test_weight(Path("/nonexistent/path/does/not/exist.rs")) is None

    print("self-test: ok")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=int, default=HEAVY_THRESHOLD_DEFAULT)
    parser.add_argument("--sweep", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0

    self_test()

    shard_count = read_shard_count()
    if shard_count is None:
        print(
            "::warning::shard-weight-drift.py could not find "
            "SEMAPHORE_SHARD_COUNT in the test-db-linux job block of "
            f"{CI_YAML} — skipping the report",
            file=sys.stderr,
        )
        return 0

    records = parse_manifest_records(MANIFEST.read_text(encoding="utf-8"))
    linux_rows = linux_rows_in_order(records)
    weights, unresolved = weigh_rows(linux_rows)

    for ordinal, label, path in unresolved:
        print(
            f"::warning::shard-weight-drift.py could not resolve a test "
            f"file for row {ordinal} ({label}) at {path} — weighed as 0",
            file=sys.stderr,
        )

    totals, members, heavy_collisions = shard_report(
        weights, shard_count, args.threshold
    )

    print(
        f"test-db-linux: {len(linux_rows)} linux rows, "
        f"SEMAPHORE_SHARD_COUNT={shard_count}, heavy threshold={args.threshold}"
    )
    print(f"shard totals: min={min(totals)} max={max(totals)} spread={max(totals) - min(totals)}")
    for shard in range(shard_count):
        top = sorted(members[shard], key=lambda m: -m[2])[:3]
        top_str = ", ".join(f"{label}({weight})" for _o, label, weight in top)
        print(f"  shard {shard}: total={totals[shard]:4d}  n_rows={len(members[shard]):3d}  top={top_str}")

    if heavy_collisions:
        print(
            f"\n{len(heavy_collisions)} shard(s) carry 2+ suites at or above "
            f"the heavy threshold ({args.threshold}) — each pair/triple below "
            "runs sequentially on one shard, so its members' durations add:"
        )
        for shard, heavy in heavy_collisions:
            names = ", ".join(f"{label} (ordinal {o}, {w} tests)" for o, label, w in heavy)
            print(f"::warning::shard {shard} carries multiple heavy suites: {names}")
    else:
        print(f"\nno shard carries 2+ suites at or above the heavy threshold ({args.threshold})")

    if args.sweep:
        sweep(weights, args.threshold)

    # Report-only — see the module docstring's "Why report-only" section.
    return 0


if __name__ == "__main__":
    sys.exit(main())
