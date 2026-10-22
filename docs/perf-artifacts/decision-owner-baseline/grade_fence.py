#!/usr/bin/env python3
"""Grade the decision fence bench from its raw output (issue #2248).

Usage: grade_fence.py <raw file> [<raw file> ...]

Each file is the output of one `decision_fence_bench` run. The script takes
the criterion point estimate of each case, then prints the mean over the
runs and each delta that the stage 1b pass condition reads.
"""

import re
import statistics
import sys
from collections import defaultdict

CASE = re.compile(r"^decision_persist/(\S+)/writers/(\S+)\s*$")
TIME = re.compile(r"time:\s+\[\S+ \S+ (\S+) (\S+) \S+ \S+\]")
UNITS = {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1e3}


def load_run(path):
    """The point estimate of each case in one run, in ms."""
    run = {}
    case = None
    for line in open(path, encoding="utf-8"):
        m = CASE.match(line.strip())
        if m:
            case = (m.group(1), m.group(2))
            continue
        m = TIME.search(line)
        if m and case:
            run[case] = float(m.group(1)) * UNITS[m.group(2)]
            case = None
    return run


def load(paths):
    cases = defaultdict(list)
    for path in paths:
        case = None
        for line in open(path, encoding="utf-8"):
            m = CASE.match(line.strip())
            if m:
                case = (m.group(1), m.group(2))
                continue
            m = TIME.search(line)
            if m and case:
                cases[case].append(float(m.group(1)) * UNITS[m.group(2)])
                case = None
    return cases


def main(paths):
    cases = load(paths)
    layouts = sorted({w for _, w in cases}, key=lambda w: (int(w.split("-")[0]), w))
    modes = ["none", "generation", "owner", "generation_owner",
             "generation_plain_owner_row", "generation_owner_refence"]
    print("### Mean decision persist latency, ms (runs in brackets)\n")
    print("| mode | " + " | ".join(f"writers {w}" for w in layouts) + " |")
    print("|:--|" + "--:|" * len(layouts))
    for mode in modes:
        row = [f"`{mode}`"]
        for w in layouts:
            runs = cases.get((mode, w), [])
            mark = ""
            if not runs and "-spread" in w:
                runs = cases.get((mode, w.replace("spread", "shared")), [])
                mark = "*"
            row.append(f"{statistics.mean(runs):.2f} ({len(runs)}){mark}" if runs else "—")
        print("| " + " | ".join(row) + " |")

    def mean(mode, w):
        runs = cases.get((mode, w)) or cases.get((mode, w.replace("spread", "shared")))
        return statistics.mean(runs) if runs else None

    print("\n_A cell marked * reuses the shared case: the mode locks no owner row._")

    print("\n### Deltas of the pooled means\n")
    print("| delta | " + " | ".join(f"writers {w}" for w in layouts) + " |")
    print("|:--|" + "--:|" * len(layouts))
    for name, top, base in [
        ("second row: `generation_owner` over `generation`", "generation_owner", "generation"),
        ("plain row lock: `generation_plain_owner_row` over `generation`",
         "generation_plain_owner_row", "generation"),
        ("first row: `generation` over `none`", "generation", "none"),
        ("owner row alone: `owner` over `none`", "owner", "none"),
        ("the fix: `generation_owner` over `generation_owner_refence`",
         "generation_owner", "generation_owner_refence"),
    ]:
        row = [name]
        for w in layouts:
            a, b = mean(top, w), mean(base, w)
            row.append(f"{100 * (a - b) / b:+.1f}%" if a and b else "—")
        print("| " + " | ".join(row) + " |")

    print("\n### Deltas within each run, 1 writer\n")
    print("| run | `generation` ms | `generation_owner` ms | `generation_plain_owner_row` ms "
          "| second row | plain row lock |")
    print("|:--|--:|--:|--:|--:|--:|")
    second, plain = [], []
    for path in paths:
        run = load_run(path)
        g = run.get(("generation", "1"))
        go = run.get(("generation_owner", "1"))
        pl = run.get(("generation_plain_owner_row", "1"))
        if not (g and go and pl):
            continue
        second.append(100 * (go - g) / g)
        plain.append(100 * (pl - g) / g)
        name = path.rsplit("/", 1)[-1]
        print(f"| {name} | {g:.2f} | {go:.2f} | {pl:.2f} | {second[-1]:+.1f}% | {plain[-1]:+.1f}% |")
    if second:
        print(f"| median | | | | {statistics.median(second):+.1f}% | {statistics.median(plain):+.1f}% |")
        over = sum(1 for d in second if d >= 10)
        print(f"\nThe second-row delta is 10% or more in {over} of {len(second)} runs.")


if __name__ == "__main__":
    main(sys.argv[1:])
