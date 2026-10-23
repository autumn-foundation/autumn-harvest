#!/usr/bin/env python3
"""Grade the fence bench runs of the owner decider (issue #2255).

Usage: grade_fence.py <raw file> [<raw file> ...]

Each file is the output of one one-writer `decision_fence_bench` run. The
script takes the criterion point estimate of each mode. It prints the
estimates of each run and the delta of the fused fence against `generation`,
`generation_owner_split` and `owner` in the same run, with the medians.
"""

import re
import statistics
import sys

CASE = re.compile(r"^decision_persist/(\S+)/writers/(\S+)\s*$")
TIME = re.compile(r"time:\s+\[\S+ \S+ (\S+) (\S+) \S+ \S+\]")
UNITS = {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1e3}
MODES = ["none", "generation", "owner", "generation_owner", "generation_owner_split"]


def load_run(path):
    run = {}
    case = None
    for line in open(path, encoding="utf-8"):
        m = CASE.match(line.strip())
        if m:
            case = m.group(1)
            continue
        m = TIME.search(line)
        if m and case:
            run[case] = float(m.group(1)) * UNITS[m.group(2)]
            case = None
    return run


def pct(a, b):
    return (a / b - 1.0) * 100.0


def main(paths):
    runs = [(p.split("/")[-1].removesuffix(".txt"), load_run(p)) for p in paths]
    print("### One writer, criterion point estimates, ms\n")
    print("| run | " + " | ".join(f"`{m}`" for m in MODES)
          + " | fused vs `generation` | fused vs split | fused vs `owner` |")
    print("|:--|" + "--:|" * (len(MODES) + 3))
    deltas = {"generation": [], "generation_owner_split": [], "owner": []}
    for name, run in runs:
        cells = [f"{run[m]:.2f}" if m in run else "—" for m in MODES]
        row = []
        for base in deltas:
            if "generation_owner" in run and base in run:
                d = pct(run["generation_owner"], run[base])
                deltas[base].append(d)
                row.append(f"{d:+.1f}%")
            else:
                row.append("—")
        print(f"| {name} | " + " | ".join(cells) + " | " + " | ".join(row) + " |")
    med = [f"**{statistics.median(v):+.1f}%**" if v else "—" for v in deltas.values()]
    print("| **median** | " + " | ".join("" for _ in MODES) + " | " + " | ".join(med) + " |")


if __name__ == "__main__":
    main(sys.argv[1:])
