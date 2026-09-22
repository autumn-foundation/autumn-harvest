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
2. Resolve each row to what it actually runs. Every crate/target other than
   `autumn-harvest`/`integration` has its own dedicated binary, so a row
   there names `<crate>/tests/<target>.rs` directly. `autumn-harvest`/
   `integration` rows all share ONE binary (`tests/integration/mod.rs`
   declares every module), and `run-suites.sh` passes each row's filter to
   libtest with no `--exact` — a SUBSTRING match against every test in
   that whole binary, not just "the file the filter looks like it names".
   `crate_integration_test_index()` builds the qualified-name index this
   needs once; `integration_row_weight()` substring-matches each row
   against it. **Correction (Codex review, this harness's own fourth PR
   round):** an earlier version resolved a row to a single guessed file and
   counted only that file's own tests, undercounting any row whose filter
   also matches a test elsewhere — e.g. `nd_block` was reported as 10 (only
   `nd_block_tests.rs`), but 9 more tests elsewhere (`retry_clock_skew_tests`,
   `dag_compensation_tests`, `event_partitioning_tests`, `mutex_tests`,
   `claim_budget_tests`, `replayer_tests`) also match the substring, for a
   real weight of 19. See `enumerate_qualified_tests()`'s docstring for the
   full mechanism. Checked against every one of the 91
   `autumn-harvest`/`integration` rows in today's manifest: `nd_block` is
   the ONLY one this changes — none of the seven heavy collisions below
   shifted.
3. Weigh each row by counting only the `#[test]`/`#[tokio::test]` functions
   that actually compile and run for THAT row's enabled feature set (the
   crate's own defaults, since `run-suites.sh` never passes
   `--no-default-features` for `linux` rows, plus whatever the manifest's
   `feats` column adds) and are not `#[ignore]`d (never run — `run-suites.sh`
   passes no `--ignored`/`--include-ignored`). Feature-gating on an
   `autumn-harvest`/`integration` test can live in three places, all
   honored: `tests/integration/mod.rs`'s own `#[cfg(...)] mod X;`
   declaration (`parse_mod_rs_gates()` — where MOST of it actually is, 199
   declarations, most gated on `db`), a file's own `#![cfg(...)]` inner
   attribute gating the whole file, and a `#[cfg(...)]` on an individual
   test or the `mod { ... }` block containing several. See `test_weight()`'s
   own docstring for three further rounds of Codex-review corrections to
   this counting: an indentation/plain-`#[test]` undercount, a
   multi-line-attribute undercount, and an `#[ignore]` overcount. Every
   number in this docstring reflects all fixes, this section's included.
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
FN_LINE_RE = re.compile(r"^\s*(?:pub\s+)?(?:async\s+)?fn\s+(\w+)")
COMMENT_OR_BLANK_RE = re.compile(r"^\s*(#|$)")
# `#[cfg(...)]` gates one item (typically the next `mod` or `fn`); `#![cfg(...)]`
# is an inner attribute gating the item it appears INSIDE (here, always the
# whole file, since every occurrence in this corpus is the first line of a
# `tests/integration/*.rs` module file). Both are single-line in every
# instance this corpus has today (checked against the 5 "linux"-reachable
# files that use `cfg(feature`).
CFG_ATTR_RE = re.compile(r"^\s*#\[cfg\((.*)\)\]\s*$")
CFG_INNER_ATTR_RE = re.compile(r"^\s*#!\[cfg\((.*)\)\]\s*$")
MOD_OPEN_RE = re.compile(r"^\s*(?:pub\s+)?mod\s+(\w+)\s*\{")
FEATURE_RE = re.compile(r'feature\s*=\s*"([^"]+)"')


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


def resolve_test_file(crate, target):
    """The dedicated test binary source for a `linux` row's `crate`/`target`
    — every crate/target other than `autumn-harvest`/`integration`, which
    `weigh_rows()` never routes here: that target's tests are compiled into
    ONE shared binary (`tests/integration/mod.rs`), so a manifest row there
    is weighed by substring-matching `crate_integration_test_index()`
    instead (see `integration_row_weight()`), not by resolving one file."""
    return REPO_ROOT / crate / "tests" / f"{target}.rs"


_DEFAULT_FEATURES_CACHE = {}


def crate_default_features(crate):
    """Read `default = [...]` out of `<crate>/Cargo.toml`'s `[features]`
    table (not hand-copied — a crate's defaults can change independently of
    this script). No `default` key, or no `[features]` table at all, means
    an empty default set, matching Cargo's own behavior."""
    if crate in _DEFAULT_FEATURES_CACHE:
        return _DEFAULT_FEATURES_CACHE[crate]
    path = REPO_ROOT / crate / "Cargo.toml"
    features = set()
    try:
        text = path.read_text(encoding="utf-8")
        match = re.search(r'^default\s*=\s*\[([^\]]*)\]', text, re.MULTILINE)
        if match:
            features = {
                f.strip().strip('"') for f in match.group(1).split(",") if f.strip()
            }
    except FileNotFoundError:
        pass
    _DEFAULT_FEATURES_CACHE[crate] = features
    return features


def enabled_features_for_row(crate, feats):
    """The feature set active for a `linux`-osclass row's `cargo test`
    invocation: `run-suites.sh`'s `do_run` never passes
    `--no-default-features` for `linux` rows (only a specific `allos` case
    does), so a row's enabled set is always the crate's own defaults, PLUS
    whatever the manifest's `feats` column adds via `--features`.

    KNOWN LIMITATION: this is a plain set union, not a full dependency-graph
    walk of `[features]` — if some feature X the manifest row requests
    itself implies a different feature Y transitively (`X = ["Y", ...]` in
    `Cargo.toml`) without Y being named directly in either the row's
    `feats` column or the crate's `default` list, Y is not detected as
    enabled here. None of the features actually gating a test in a
    `linux`-reachable file today (`db`, `testing`) hit that gap — `db` is
    always in `autumn-harvest`'s own default set, and every row gating on
    `testing` either lists it directly in `feats` or doesn't have it
    enabled at all (checked by hand against today's 5 affected files)."""
    features = set(crate_default_features(crate))
    if feats != "-":
        features |= {f.strip() for f in feats.split(",") if f.strip()}
    return features


def cfg_is_enabled(condition, enabled_features):
    """Evaluate a `cfg(...)`/`cfg!(...)`'s inner condition text against a
    row's enabled feature set. Handles only the forms actually present in
    this corpus today (checked by hand): a bare `feature = "X"`, an
    `all(feature = "A", feature = "B", ...)` (AND — no `any(feature = ...)`
    combination appears anywhere in the corpus), and `test`/`unix`/
    `target_os = "linux"` (always true — CI's Docker-backed shard runs on
    `ubuntu-latest`) / `not(unix)` (always false there). Anything else is
    treated as enabled (fail open): an unrecognized condition should not
    silently make this script UNDER-count a test that really does run,
    which was the failure mode of every correction so far in this file's
    history — over-counting is the newly-introduced risk this leaves open,
    and is why this function's coverage is documented rather than silent.
    """
    condition = condition.strip()
    if condition in ("test", "unix") or condition == 'target_os = "linux"':
        return True
    if condition == "not(unix)":
        return False
    if condition.startswith("all(") and condition.endswith(")"):
        inner = condition[len("all(") : -1]
        return all(feat in enabled_features for feat in FEATURE_RE.findall(inner))
    single = FEATURE_RE.fullmatch(condition)
    if single:
        return single.group(1) in enabled_features
    return True


def row_label(crate, target, filt):
    if crate == "autumn-harvest" and target == "integration" and filt != "-":
        return f"{crate}/{target} -- {filt}"
    return f"{crate}/{target}"


def test_weight(path, enabled_features=frozenset()):
    """Count runnable `#[test]`/`#[tokio::test]` functions in `path` that
    actually run under `enabled_features` — `run-suites.sh` invokes plain
    `cargo test` with no `--ignored`/`--include-ignored`, so an `#[ignore]`d
    test never executes, and a test gated behind a `cfg(feature = "X")` not
    in this row's enabled set never even compiles in. Both are excluded.

    **Three corrections, all from Codex review on this harness's PRs, each
    the exact opposite failure mode of undercounting the module docstring
    already covers — this function counts too MUCH unless corrected:**

    1. `#[ignore]`: `claim_budget_tests.rs` carries 7 `#[ignore]`d one-shot
       evidence generators (documented in the file itself as "not a
       repeatable CI assertion"), inflating its weight from 29 (what
       actually runs) to 36 and fabricating a heavy-suite collision direct
       log evidence does not support.
    2. `cfg(feature = ...)`: `retry_after_tests.rs`'s `replay_tests` module
       is `#[cfg(feature = "testing")]`, but its manifest row requests no
       extra features (`feats` column is `-`) and `autumn-harvest`'s own
       defaults don't include `testing` — so its 1 test never compiles for
       that row, and the file's real weight is 6, not the 7 a plain
       attribute count finds.
    3. `#![cfg(...)]` (an inner attribute, gating the enclosing item — here,
       always the whole file, since every occurrence in this corpus is a
       file's first line): `claim_budget_tests.rs` and
       `quota_history_bytes_perf_tests.rs` both open with
       `#![cfg(feature = "db")]`. `db` is one of `autumn-harvest`'s own
       default features, so this is always true for every `linux` row today
       (`run-suites.sh` never passes `--no-default-features` there) — a
       currently-inert case, kept correct anyway rather than assumed.

    Attributes stack directly above their `fn`/`async fn` line with no
    blank line between (a leading `///` doc-comment block may sit above the
    attribute stack, but never inside it), so a small forward scan — collect
    contiguous `#[...]` lines, decide on the next `fn` line, then reset — is
    accurate without a full Rust parser. A `#[cfg(...)]` directly above a
    `mod name {` gates every test inside that module until its closing
    brace: tracked with a running `{`/`}` depth count (checked, not
    assumed, against every "linux"-reachable file using `cfg(feature` in
    today's corpus — none nests a cfg'd `mod` inside another, so a single
    stack level suffices, but the code stacks correctly regardless).

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
    for line in lines[:5]:
        inner = CFG_INNER_ATTR_RE.match(line)
        if inner and not cfg_is_enabled(inner.group(1), enabled_features):
            return 0
    count = 0
    pending_test = False
    pending_ignore = False
    pending_cfgs = []
    bracket_depth = 0
    brace_depth = 0
    scope_stack = []  # list of (depth_to_pop_at, enabled)
    for line in lines:
        delta = line.count("{") - line.count("}")
        if bracket_depth > 0:
            bracket_depth += line.count("[") + line.count("(")
            bracket_depth -= line.count("]") + line.count(")")
            brace_depth += delta
            while scope_stack and brace_depth <= scope_stack[-1][0]:
                scope_stack.pop()
            continue
        if ATTR_LINE_RE.match(line):
            if TEST_ATTR_RE.match(line):
                pending_test = True
            if IGNORE_ATTR_RE.match(line):
                pending_ignore = True
            cfg_match = CFG_ATTR_RE.match(line)
            if cfg_match:
                pending_cfgs.append(cfg_match.group(1))
            bracket_depth = line.count("[") + line.count("(")
            bracket_depth -= line.count("]") + line.count(")")
            brace_depth += delta
            while scope_stack and brace_depth <= scope_stack[-1][0]:
                scope_stack.pop()
            continue
        if MOD_OPEN_RE.match(line):
            enabled = all(cfg_is_enabled(c, enabled_features) for c in pending_cfgs)
            scope_stack.append((brace_depth, enabled))
            pending_test = False
            pending_ignore = False
            pending_cfgs = []
            brace_depth += delta
            continue
        if FN_LINE_RE.match(line):
            scope_enabled = all(enabled for _depth, enabled in scope_stack)
            item_enabled = all(cfg_is_enabled(c, enabled_features) for c in pending_cfgs)
            if pending_test and not pending_ignore and scope_enabled and item_enabled:
                count += 1
            pending_test = False
            pending_ignore = False
            pending_cfgs = []
            brace_depth += delta
            while scope_stack and brace_depth <= scope_stack[-1][0]:
                scope_stack.pop()
            continue
        brace_depth += delta
        while scope_stack and brace_depth <= scope_stack[-1][0]:
            scope_stack.pop()
        stripped = line.strip()
        if stripped == "" or stripped.startswith("//"):
            continue
        # Any other code line between attributes and a fn would be unusual;
        # treat it as ending the pending attribute stack rather than
        # misattributing it to a later, unrelated fn.
        pending_test = False
        pending_ignore = False
        pending_cfgs = []
    return count


MOD_RS_PATH = REPO_ROOT / "autumn-harvest" / "tests" / "integration" / "mod.rs"
MOD_DECL_RE = re.compile(r"^\s*(?:pub\s+)?mod\s+(\w+)\s*;")


def enumerate_qualified_tests(path):
    """Like `test_weight()`, but returns every non-`#[ignore]`d test's
    libtest-style qualified name (`<mod>::<nested_mod>::<fn_name>`, WITHOUT
    the file stem — the caller prepends that, since `mod.rs`'s own `mod X;`
    name is what libtest actually uses as the top path segment, and it can
    differ from the file's stem in principle even though it never does in
    today's corpus) alongside the tuple of `cfg(...)` conditions that must
    ALL hold for it to exist (the file's own `#![cfg(...)]`, if any, plus
    every enclosing `#[cfg(...)] mod { ... }`, plus its own item-level
    `#[cfg(...)]`).

    **Why this exists, separately from `test_weight()` (Codex review, this
    harness's own fourth PR round):** `.github/ci/run-suites.sh:121-129`
    passes a manifest row's `filter` column to libtest as a plain positional
    arg, with no `--exact` — cargo/libtest's default is a SUBSTRING match
    against every test's qualified name in the WHOLE `integration` binary,
    not just the one file `resolve_test_file()` guesses from the filter
    text. A row's real weight is however many qualified names in the ENTIRE
    binary contain its filter as a substring, which can be more than the
    "file it looks like it names" carries. Concretely: the manifest's
    `nd_block` row was reported as 10 (the ordinary per-file count of
    `nd_block_tests.rs`), but `retry_clock_skew_tests.rs`,
    `dag_compensation_tests.rs`, and `event_partitioning_tests.rs` each
    carry a test whose name also contains the substring `nd_block`
    (`requeue_workflow_task_nd_blocked_...`, `..._never_nd_blocks`,
    `..._dropped_and_blocked`), so the row's real weight is higher.
    `crate_integration_test_index()` below builds the qualified-name index
    this substring match needs; `resolve_test_file()`'s single-file guess
    is no longer used to WEIGH an `autumn-harvest`/`integration` row (see
    `weigh_rows()`), only to label it in output.

    Structurally this is the same forward scan as `test_weight()` — see
    that function's docstring for the attribute/bracket/brace mechanics,
    which are identical here — extended to also track the STACK of
    enclosing `mod name { ... }` names (for the qualified path) and to
    collect results instead of only counting them.
    """
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None
    file_gate = []
    for line in lines[:5]:
        inner = CFG_INNER_ATTR_RE.match(line)
        if inner:
            file_gate.append(inner.group(1))
    results = []
    pending_test = False
    pending_ignore = False
    pending_cfgs = []
    bracket_depth = 0
    brace_depth = 0
    scope_stack = []  # list of (depth_to_pop_at, mod_name, conditions_tuple)

    def active_conditions():
        conds = list(file_gate)
        for _depth, _name, mod_conds in scope_stack:
            conds.extend(mod_conds)
        return conds

    for line in lines:
        delta = line.count("{") - line.count("}")
        if bracket_depth > 0:
            bracket_depth += line.count("[") + line.count("(")
            bracket_depth -= line.count("]") + line.count(")")
            brace_depth += delta
            while scope_stack and brace_depth <= scope_stack[-1][0]:
                scope_stack.pop()
            continue
        if ATTR_LINE_RE.match(line):
            if TEST_ATTR_RE.match(line):
                pending_test = True
            if IGNORE_ATTR_RE.match(line):
                pending_ignore = True
            cfg_match = CFG_ATTR_RE.match(line)
            if cfg_match:
                pending_cfgs.append(cfg_match.group(1))
            bracket_depth = line.count("[") + line.count("(")
            bracket_depth -= line.count("]") + line.count(")")
            brace_depth += delta
            while scope_stack and brace_depth <= scope_stack[-1][0]:
                scope_stack.pop()
            continue
        mod_match = MOD_OPEN_RE.match(line)
        if mod_match:
            scope_stack.append((brace_depth, mod_match.group(1), tuple(pending_cfgs)))
            pending_test = False
            pending_ignore = False
            pending_cfgs = []
            brace_depth += delta
            continue
        fn_match = FN_LINE_RE.match(line)
        if fn_match:
            if pending_test and not pending_ignore:
                conditions = tuple(active_conditions()) + tuple(pending_cfgs)
                mod_path = [name for _depth, name, _c in scope_stack]
                qualified = "::".join(mod_path + [fn_match.group(1)])
                results.append((qualified, conditions))
            pending_test = False
            pending_ignore = False
            pending_cfgs = []
            brace_depth += delta
            while scope_stack and brace_depth <= scope_stack[-1][0]:
                scope_stack.pop()
            continue
        brace_depth += delta
        while scope_stack and brace_depth <= scope_stack[-1][0]:
            scope_stack.pop()
        stripped = line.strip()
        if stripped == "" or stripped.startswith("//"):
            continue
        pending_test = False
        pending_ignore = False
        pending_cfgs = []
    return results


def parse_mod_rs_gates():
    """Every `mod X;` declaration in `tests/integration/mod.rs`, with the
    `cfg(...)` condition(s) (if any) immediately preceding it — this is
    WHERE most feature-gating on an integration test file actually lives
    (199 `mod` declarations, the large majority individually
    `#[cfg(feature = "db")]`-gated; a handful gate on `testing`, `chaos`,
    `debugger`, or `wasm-activities` instead), not inside the files
    themselves. A file can ALSO carry its own internal
    `#![cfg(...)]`/`#[cfg(...)]` gates (see `enumerate_qualified_tests()`);
    both apply, ANDed together, in `crate_integration_test_index()`.
    """
    gates = {}
    pending_cfgs = []
    for line in MOD_RS_PATH.read_text(encoding="utf-8").splitlines():
        cfg_match = CFG_ATTR_RE.match(line)
        if cfg_match:
            pending_cfgs.append(cfg_match.group(1))
            continue
        mod_match = MOD_DECL_RE.match(line)
        if mod_match:
            gates[mod_match.group(1)] = tuple(pending_cfgs)
            pending_cfgs = []
            continue
        stripped = line.strip()
        if stripped == "" or stripped.startswith("//"):
            continue
        pending_cfgs = []
    return gates


_INTEGRATION_TEST_INDEX_CACHE = None


def crate_integration_test_index():
    """The full `(qualified_name, conditions)` list for EVERY test in the
    `autumn-harvest`/`integration` binary, across every file `mod.rs`
    declares — computed once (module-level cache; this function has no
    arguments, since the raw structural scan does not depend on which
    features end up enabled, only the later per-row filter does).
    `qualified_name` includes the `mod.rs`-declared module name as its
    first segment (matching `enumerate_qualified_tests()`'s file-stem-less
    output), and `conditions` prepends `mod.rs`'s own gate on that
    `mod X;` line to the file's internal ones.
    """
    global _INTEGRATION_TEST_INDEX_CACHE
    if _INTEGRATION_TEST_INDEX_CACHE is not None:
        return _INTEGRATION_TEST_INDEX_CACHE
    mod_gates = parse_mod_rs_gates()
    index = []
    for mod_name, mod_conditions in mod_gates.items():
        path = MOD_RS_PATH.parent / f"{mod_name}.rs"
        tests = enumerate_qualified_tests(path)
        if tests is None:
            continue
        for qualified, conditions in tests:
            index.append(
                (f"{mod_name}::{qualified}", mod_conditions + conditions)
            )
    _INTEGRATION_TEST_INDEX_CACHE = index
    return index


def integration_row_weight(filt, enabled_features):
    """An `autumn-harvest`/`integration` row's real weight: the count of
    qualified names in `crate_integration_test_index()` that (a) are
    enabled under `enabled_features` and (b) contain `filt` as a substring
    — matching `run-suites.sh`'s un-`--exact`'d libtest invocation exactly
    (see `enumerate_qualified_tests()`'s docstring for why a per-file guess
    undercounts this)."""
    return sum(
        1
        for qualified, conditions in crate_integration_test_index()
        if filt in qualified
        and all(cfg_is_enabled(c, enabled_features) for c in conditions)
    )


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
    for ordinal, (_os, crate, target, feats, filt) in enumerate(linux_rows):
        label = row_label(crate, target, filt)
        enabled = enabled_features_for_row(crate, feats)
        # autumn-harvest/integration rows share ONE binary, and
        # run-suites.sh's filter is a libtest SUBSTRING match against every
        # test in it (no --exact) -- weighing by "the one file the filter
        # looks like it names" undercounts whenever the substring also
        # matches tests elsewhere (see integration_row_weight()'s
        # docstring). Every other crate/target has its own dedicated
        # binary per row and, in today's manifest, always filt == "-" (no
        # filter at all), so the simpler per-file count remains correct
        # and cheaper there.
        if crate == "autumn-harvest" and target == "integration":
            weight = integration_row_weight(filt, enabled)
        else:
            path = resolve_test_file(crate, target)
            weight = test_weight(path, enabled)
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

    # cfg(feature = ...) gating: a plain test, a #[cfg]'d mod block (gates
    # every test inside until its closing brace), and an item-level
    # #[cfg] directly on one test — each checked both with and without the
    # gating feature enabled, matching retry_after_tests.rs's real shape.
    cfg_fixture_rs = """\
#[tokio::test]
async fn always_runs() {}

#[cfg(feature = "testing")]
mod gated_mod {
    #[tokio::test]
    async fn inside_gated_mod() {}
}

#[cfg(feature = "chaos")]
#[tokio::test]
async fn item_level_gated() {}

#[cfg(all(feature = "testing", feature = "db"))]
#[tokio::test]
async fn needs_both_features() {}
"""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".rs", delete=False
    ) as tmp:
        tmp.write(cfg_fixture_rs)
        cfg_path = Path(tmp.name)
    try:
        assert test_weight(cfg_path, frozenset()) == 1, "only always_runs, nothing gated on"
        assert test_weight(cfg_path, frozenset({"testing"})) == 2, (
            "always_runs + inside_gated_mod, needs_both_features still needs db too"
        )
        assert test_weight(cfg_path, frozenset({"testing", "db"})) == 3, (
            "always_runs + inside_gated_mod + needs_both_features"
        )
        assert test_weight(cfg_path, frozenset({"chaos"})) == 2, (
            "always_runs + item_level_gated"
        )
    finally:
        cfg_path.unlink()

    # #![cfg(...)] inner attribute: gates the WHOLE file (every occurrence in
    # the real corpus is a file's first line), unlike the outer `#[cfg(...)]`
    # forms above which gate only the next item.
    inner_cfg_fixture_rs = """\
#![cfg(feature = "db")]

#[tokio::test]
async fn only_if_db_enabled() {}
"""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".rs", delete=False
    ) as tmp:
        tmp.write(inner_cfg_fixture_rs)
        inner_path = Path(tmp.name)
    try:
        assert test_weight(inner_path, frozenset()) == 0, "whole file excluded, db not enabled"
        assert test_weight(inner_path, frozenset({"db"})) == 1
    finally:
        inner_path.unlink()

    assert cfg_is_enabled('feature = "testing"', {"testing"}) is True
    assert cfg_is_enabled('feature = "testing"', set()) is False
    assert cfg_is_enabled('all(feature = "a", feature = "b")', {"a", "b"}) is True
    assert cfg_is_enabled('all(feature = "a", feature = "b")', {"a"}) is False
    assert cfg_is_enabled("test", set()) is True
    assert cfg_is_enabled("unix", set()) is True
    assert cfg_is_enabled("not(unix)", set()) is False

    assert enabled_features_for_row("autumn-harvest", "-") == {
        "db",
        "unified-dag-execution",
    }
    assert enabled_features_for_row("autumn-harvest", "testing,debugger") == {
        "db",
        "unified-dag-execution",
        "testing",
        "debugger",
    }
    assert enabled_features_for_row("autumn-harvest-plugin", "-") == set()

    # enumerate_qualified_tests: qualified paths (mod-name-prefixed, no file
    # stem — the caller adds that) and their cfg conditions, for a nested
    # cfg'd mod plus a plain top-level test.
    enum_fixture_rs = """\
#[tokio::test]
async fn top_level() {}

#[cfg(feature = "testing")]
mod nested {
    #[tokio::test]
    async fn inner() {}
}

#[cfg(feature = "chaos")]
#[tokio::test]
async fn item_gated() {}

#[ignore = "not a CI assertion"]
#[tokio::test]
async fn skipped() {}
"""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".rs", delete=False
    ) as tmp:
        tmp.write(enum_fixture_rs)
        enum_path = Path(tmp.name)
    try:
        results = dict(enumerate_qualified_tests(enum_path))
    finally:
        enum_path.unlink()
    assert set(results) == {"top_level", "nested::inner", "item_gated"}, results
    assert results["top_level"] == ()
    assert results["nested::inner"] == ('feature = "testing"',)
    assert results["item_gated"] == ('feature = "chaos"',)
    assert enumerate_qualified_tests(Path("/nonexistent/x.rs")) is None

    # parse_mod_rs_gates: a fixture matching mod.rs's real shape (a bare
    # mod, a gated one, an all(...)-gated one).
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".rs", delete=False
    ) as tmp:
        tmp.write(
            "mod plain_mod;\n"
            '#[cfg(feature = "db")]\n'
            "mod db_mod;\n"
            '#[cfg(all(feature = "db", feature = "testing"))]\n'
            "mod both_mod;\n"
        )
        mod_rs_fixture = Path(tmp.name)
    global MOD_RS_PATH
    orig_mod_rs = MOD_RS_PATH
    MOD_RS_PATH = mod_rs_fixture
    try:
        gates = parse_mod_rs_gates()
    finally:
        MOD_RS_PATH = orig_mod_rs
        mod_rs_fixture.unlink()
    assert gates == {
        "plain_mod": (),
        "db_mod": ('feature = "db"',),
        "both_mod": ('all(feature = "db", feature = "testing")',),
    }, gates

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
