#!/usr/bin/env python3
"""Grade the decision-owner baseline sweep from its raw output (issue #2248).

Usage: grade.py <raw directory>

Prints one Markdown table per measure, over valid runs only:

- workflows/sec per depth and claim cap;
- the claim split: count per workflow and mean time of each claim kind;
- the share of claim time that workflow claims take;
- the persist phases: mean per decision persist and share of the persist.
"""

import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

CELL = re.compile(r"^cell (.*)$")
KINDS = ["workflow", "activity", "empty", "error"]
PHASES = ["fence", "events", "tasks", "notify", "commit", "other"]


def parse_kv(text):
    return dict(part.split("=", 1) for part in text.split() if "=" in part)


def load(directory):
    cells = defaultdict(list)
    for path in sorted(Path(directory).glob("r*.txt")):
        for line in path.read_text().splitlines():
            m = CELL.match(line)
            if not m:
                continue
            kv = parse_kv(m.group(1))
            if kv.get("valid") != "PASS" or kv.get("truncated") != "false":
                continue
            cells[(kv["tree"], int(kv["depth"]))].append(kv)
    return cells


def num(kv, key):
    return float(kv.get(key, "0"))


def time_ms(kv, name):
    """The summed time of one signal in milliseconds."""
    return num(kv, f"{name}_n") * num(kv, f"{name}_mean_ms")


def mean_of(runs, fn):
    return statistics.mean(fn(kv) for kv in runs)


def main(argv):
    cells = load(argv[1])
    trees = sorted({t for t, _ in cells}, key=lambda t: int(t.replace("claims", "")))
    depths = sorted({d for _, d in cells})

    print("### Workflows/sec (mean of valid runs; run count in brackets)\n")
    print("| depth | " + " | ".join(trees) + " |")
    print("|--:|" + "--:|" * len(trees))
    for depth in depths:
        row = [str(depth)]
        for tree in trees:
            runs = cells.get((tree, depth), [])
            row.append(f"{mean_of(runs, lambda kv: num(kv, 'wfps')):.2f} ({len(runs)})" if runs else "—")
        print("| " + " | ".join(row) + " |")

    print("\n### Claim split (mean over depths and runs)\n")
    print("| claim loops | workflow claims per workflow | workflow ms | activity claims per workflow "
          "| activity ms | empty claims per workflow | empty ms | workflow share of claim time |")
    print("|:--|--:|--:|--:|--:|--:|--:|--:|")
    for tree in trees:
        runs = [kv for (t, _), rs in cells.items() if t == tree for kv in rs]
        if not runs:
            continue

        def per_wf(kind):
            return mean_of(runs, lambda kv: num(kv, f"claim_{kind}_n") / num(kv, "completed"))

        def mean_ms(kind):
            weighted = sum(time_ms(kv, f"claim_{kind}") for kv in runs)
            count = sum(num(kv, f"claim_{kind}_n") for kv in runs)
            return weighted / count if count else 0.0

        def wf_share(kv):
            total = sum(time_ms(kv, f"claim_{k}") for k in KINDS)
            return time_ms(kv, "claim_workflow") / total if total else 0.0

        print(f"| {tree.replace('claims', '')} | {per_wf('workflow'):.2f} | {mean_ms('workflow'):.2f} "
              f"| {per_wf('activity'):.2f} | {mean_ms('activity'):.2f} | {per_wf('empty'):.2f} "
              f"| {mean_ms('empty'):.2f} | {100 * mean_of(runs, wf_share):.1f}% |")

    print("\n### Claim time by kind and depth (ms, workflow / activity)\n")
    print("| depth | " + " | ".join(trees) + " |")
    print("|--:|" + "--:|" * len(trees))
    for depth in depths:
        row = [str(depth)]
        for tree in trees:
            runs = cells.get((tree, depth), [])
            if not runs:
                row.append("—")
                continue
            wf = mean_of(runs, lambda kv: num(kv, "claim_workflow_mean_ms"))
            act = mean_of(runs, lambda kv: num(kv, "claim_activity_mean_ms"))
            row.append(f"{wf:.2f} / {act:.2f}")
        print("| " + " | ".join(row) + " |")

    print("\n### Decision persist phases (mean ms per persist; share of the persist)\n")
    print("| claim loops | persists per workflow | persist ms | " + " | ".join(PHASES) + " |")
    print("|:--|--:|--:|" + "--:|" * len(PHASES))
    for tree in trees:
        runs = [kv for (t, _), rs in cells.items() if t == tree for kv in rs]
        if not runs:
            continue
        persists = sum(num(kv, "phase_commit_n") for kv in runs)
        sums = {p: sum(time_ms(kv, f"phase_{p}") for kv in runs) for p in PHASES}
        total = sum(sums.values())
        per_wf = mean_of(runs, lambda kv: num(kv, "phase_commit_n") / num(kv, "completed"))
        parts = [f"{sums[p] / persists:.2f} ({100 * sums[p] / total:.0f}%)" for p in PHASES]
        print(f"| {tree.replace('claims', '')} | {per_wf:.2f} | {total / persists:.2f} | "
              + " | ".join(parts) + " |")


if __name__ == "__main__":
    main(sys.argv)
