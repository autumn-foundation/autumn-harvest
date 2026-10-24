#!/usr/bin/env python3
"""Grade the owner decider sweep from its raw output (issue #2255).

Usage: grade.py <raw directory>

Prints Markdown tables over valid runs only:

- workflows/sec per depth, mode and claim cap, and the owner/legacy ratio;
- claims and owner takes per workflow, by kind, with their mean times.
"""

import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

CELL = re.compile(r"^cell (.*)$")
KINDS = ["workflow", "activity", "empty", "error"]


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
            mode, claims = kv["tree"].split("-claims")
            cells[(mode, int(claims), int(kv["depth"]))].append(kv)
    return cells


def num(kv, key):
    return float(kv.get(key, "0"))


def mean(values):
    values = list(values)
    return statistics.mean(values) if values else float("nan")


def main(argv):
    cells = load(argv[1])
    caps = sorted({c for _, c, _ in cells})
    depths = sorted({d for _, _, d in cells})

    print("### Workflows/sec (mean of valid runs; run count in brackets)\n")
    print("| depth | claim loops | legacy | owner | owner / legacy |")
    print("|--:|--:|--:|--:|--:|")
    pooled = defaultdict(list)
    for cap in caps:
        for depth in depths:
            legacy = cells.get(("legacy", cap, depth), [])
            owner = cells.get(("owner", cap, depth), [])
            lw = mean(num(kv, "wfps") for kv in legacy)
            ow = mean(num(kv, "wfps") for kv in owner)
            pooled[cap].append((lw, ow))
            print(f"| {depth} | {cap} | {lw:.2f} ({len(legacy)}) | {ow:.2f} ({len(owner)}) "
                  f"| {ow / lw:.2f}x |")
    print("\n### Owner / legacy over all depths\n")
    print("| claim loops | legacy wf/s | owner wf/s | ratio |")
    print("|--:|--:|--:|--:|")
    for cap in caps:
        lw = mean(l for l, _ in pooled[cap])
        ow = mean(o for _, o in pooled[cap])
        print(f"| {cap} | {lw:.2f} | {ow:.2f} | {ow / lw:.2f}x |")

    print("\n### Claims and takes per workflow, over all depths\n")
    print("| mode | claim loops | workflow claims | activity claims | empty claims "
          "| task claims | workflow takes | empty takes | claim ms | take ms |")
    print("|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
    for mode in ["legacy", "owner"]:
        for cap in caps:
            runs = [kv for d in depths for kv in cells.get((mode, cap, d), [])]
            if not runs:
                continue

            def per(key):
                return mean(num(kv, key) / num(kv, "completed") for kv in runs)

            def mean_ms(prefix):
                total = sum(num(kv, f"{prefix}_{k}_n") * num(kv, f"{prefix}_{k}_mean_ms")
                            for kv in runs for k in KINDS)
                count = sum(num(kv, f"{prefix}_{k}_n") for kv in runs for k in KINDS)
                return total / count if count else 0.0

            wf = per("claim_workflow_n")
            act = per("claim_activity_n")
            print(f"| {mode} | {cap} | {wf:.2f} | {act:.2f} | {per('claim_empty_n'):.2f} "
                  f"| {wf + act:.2f} | {per('take_workflow_n'):.2f} | {per('take_empty_n'):.2f} "
                  f"| {mean_ms('claim'):.2f} | {mean_ms('take'):.2f} |")


if __name__ == "__main__":
    main(sys.argv)
