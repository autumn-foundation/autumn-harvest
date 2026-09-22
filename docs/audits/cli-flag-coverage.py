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
import re
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


def flag_mention_re(flag: str) -> re.Pattern:
    # Word-boundary on both sides of the flag name (not just `\b`, which
    # treats `-` as a boundary already and would let `--to-event-time` count
    # as a mention of `--to-event`). Matched against raw file text, not just
    # fenced code — a flag named in prose ("pass `--dry-run` to preview")
    # answers the coverage question just as well as a runnable example.
    return re.compile(rf"(?<![\w-])--{re.escape(flag)}(?![\w-])")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    cli_flags, _cli_env_vars = extract_cli_ground_truth()

    files = graded_corpus_files()
    corpus_text = "\n".join(
        p.read_text(encoding="utf-8", errors="replace") for p in files
    )

    undocumented = sorted(
        f for f in cli_flags if not flag_mention_re(f).search(corpus_text)
    )

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
