#!/usr/bin/env python3
"""Folio corpus harness: CLI-flag coverage matrix for docs/.

Deterministic, reproducible on any checkout — no network access required.
This is the inverse of `config-cli-drift.py`. That script catches a doc
citing a `--flag` that no longer exists (drift); this one catches the other
direction: a real `harvest` CLI flag that no page in the corpus ever
mentions at all (a coverage gap — see docs/audits/README.md's Tier-1 list,
"Coverage matrix").

Ground truth is the same mechanical extraction `config-cli-drift.py` already
performs over `autumn-harvest-cli/src/lib.rs` (every `#[arg(long...)]`
field), imported by path rather than re-implemented, so the two scripts can
never disagree about what a real flag is.

**This is report-only by design and never fails CI.** A large fraction of a
mature CLI's flags are narrow, internal, or self-explanatory
(`--limit-groups`, `--conf-file`, `--refill-per-sec`) and gating every one of
them at zero would be exactly the "comprehensiveness worship" this corpus's
own doctrine rejects — undocumented-and-stable beats documented-and-wrong,
and a blind coverage gate pressures someone into padding a page just to turn
a number green. The number this script prints is triage input for a human
(or Folio) pass, not a build gate: a flag is worth a fix only when an
existing page already walks a reader through the exact command that flag
belongs to and stops short of it — that judgment call needs a person
reading the surrounding prose, not a script.

Usage:
    python3 docs/audits/cli-flag-coverage.py [--json]

Always exits 0.
"""
import argparse
import importlib.util
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Loaded by path (both scripts have hyphenated filenames, so a normal
# `import` can't name them) rather than re-implemented, for the same reason
# config-cli-drift.py itself loads corpus-link-check.py: one definition of
# "what is a real CLI flag" / "what files make up the corpus", so this
# script can't quietly drift from config-cli-drift.py's own ground truth.
_drift_spec = importlib.util.spec_from_file_location(
    "config_cli_drift", Path(__file__).with_name("config-cli-drift.py")
)
_drift = importlib.util.module_from_spec(_drift_spec)
_drift_spec.loader.exec_module(_drift)
extract_cli_ground_truth = _drift.extract_cli_ground_truth
mask_fenced = _drift.mask_fenced
FLAG_TOKEN_RE = _drift.FLAG_TOKEN_RE

_link_check_spec = importlib.util.spec_from_file_location(
    "corpus_link_check", Path(__file__).with_name("corpus-link-check.py")
)
_link_check = importlib.util.module_from_spec(_link_check_spec)
_link_check_spec.loader.exec_module(_link_check)
corpus_files = _link_check.corpus_files
compute_corpus_reachable = _link_check.compute_corpus_reachable

DOCS_ROOT = REPO_ROOT / "docs"


def graded_corpus_files():
    """The same file set corpus-link-check.py's orphan scan grades: pages
    under docs/, reachable from the real corpus (a process-artifact page —
    docs/plans/, docs/rnd/, etc. — counts only if something actually links
    to it). Deliberately narrower than `corpus_files()` itself, which also
    returns README.md/CHANGELOG.md/RELEASE_NOTES.md as link SOURCES — those
    are the front door (Onramp's territory, per corpus-link-check.py's own
    docstring), not part of the reference corpus this script's "coverage"
    question is about. A flag documented only in README's CLI walkthrough
    (`--to-event`, found in review) is a real answer for a reader who opens
    README, but is invisible to anyone who searches docs/ itself — treating
    it as "documented" here would hide that gap instead of surfacing it, so
    that mention doesn't count."""
    all_files = corpus_files()
    reachable = compute_corpus_reachable(all_files)
    return [p for p in all_files if p.is_relative_to(DOCS_ROOT) and p in reachable]


def flags_documented_in(path: Path) -> set:
    """Real `--flag` tokens this page credits as documented.

    Outside fenced code — prose and reference tables — a bare `--flag`
    mention counts on its own: this corpus's runbooks routinely document a
    flag in an explanatory sentence or a table row ("`--dry-run` previews
    without writing") without repeating a full runnable example every time,
    and that answers a reader's question just as well.

    Inside a fenced code block, a `--flag` mention counts only if the block
    also contains the word `harvest` somewhere in it (not necessarily the
    same line — a multi-line invocation may put `harvest workflow reset` on
    one line and `--to-event` on a continuation). Found in review:
    `docs/upgrading/0.5.0.md` shows a fenced ` ```bash ` block containing
    only `diesel migration generate --version` — no mention of `harvest`
    anywhere in that block — and an earlier version of this scan, which
    matched `--flag` against raw page text with no fence-aware distinction,
    credited that as documentation of harvest's own `--version`
    (`#[command(version)]`): a false match on a different tool's homonymous
    flag. A block that never says `harvest` is never a `harvest` example,
    whatever flags happen to appear in it."""
    text = path.read_text(encoding="utf-8", errors="replace")
    masked_lines, blocks = mask_fenced(text.splitlines())
    found = set()

    prose_text = "\n".join(masked_lines)
    for m in FLAG_TOKEN_RE.finditer(prose_text):
        found.add(m.group(1))

    for _lang, block_lines in blocks:
        block_text = "\n".join(block_lines)
        if "harvest" not in block_text:
            continue
        for m in FLAG_TOKEN_RE.finditer(block_text):
            found.add(m.group(1))

    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    cli_flags, _cli_env_vars = extract_cli_ground_truth()

    files = graded_corpus_files()
    documented = set()
    for p in files:
        documented |= flags_documented_in(p)

    undocumented = sorted(cli_flags - documented)

    if args.json:
        print(
            json.dumps(
                {
                    "files_scanned": len(files),
                    "cli_flags_known": sorted(cli_flags),
                    "undocumented_flags": undocumented,
                },
                indent=2,
            )
        )
    else:
        print(
            f"Folio CLI-flag coverage matrix — {len(files)} files scanned, "
            f"{len(cli_flags)} CLI flags known\n"
        )
        print(
            f"Undocumented CLI flags (reported only, never fails CI): "
            f"{len(undocumented)}"
        )
        for f in undocumented:
            print(f"  --{f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
