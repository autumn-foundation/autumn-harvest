#!/usr/bin/env python3
"""Grade assay ledger #14 from the raw run output.

Committed with the pre-registration, before any run. It reads every file in
one directory, prints the cell table, the #1815 signal table and the five
registered lines as Markdown.

    python3 grade.py results/raw

Harvest runs print one `cell key=value ...` line each. Temporal runs print
assay #11's own format, which carries two decimals.
"""

import re
import sys
from pathlib import Path

TREES = ["0aeb887", "513b7aa", "9f444b7"]
HARVEST_ARMS = ["postgres", "redis_pg"]
TEMPORAL_TREE = "1.25.2"
TEMPORAL_ARM = "temporal_go"
DEPTHS = [250, 500, 1000, 2000]
BASE, FIX, TRUNK = TREES
# Recorded runs per cell, valid or not. A cell with any other count comes
# from an interrupted sweep or stale files, so it has no mean.
EXPECTED_RUNS = 3
SIGNALS = [
    "claim_mean_ms",
    "persist_mean_ms",
    "scan_mean_ms",
    "heartbeat_mean_ms",
    "wait_p99_ms",
    "in_use_mean",
]

BACKLOG_RE = re.compile(r"^Backlog (\d+) workflows", re.MULTILINE)
TEMPORAL_REP_RE = re.compile(
    r"^rep \d+: ([0-9.]+) workflows/sec \(.*correctness (PASS|FAIL)(, TRUNCATED)?\)$",
    re.MULTILINE,
)


def parse_cell_line(line):
    """Return the key=value pairs of one harvest `cell` line."""
    return dict(part.split("=", 1) for part in line.split()[1:] if "=" in part)


def load(directory):
    """Return {(tree, arm, depth): [run dict]}, every run kept, valid or not."""
    runs = {}
    for path in sorted(Path(directory).glob("*.txt")):
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            if line.startswith("cell "):
                run = parse_cell_line(line)
                key = (run["tree"], run["arm"], int(run["depth"]))
                run["valid"] = run.get("valid") == "PASS"
                run["wfps"] = float(run["wfps"])
                runs.setdefault(key, []).append(run)
        backlog = BACKLOG_RE.search(text)
        if backlog and "Temporal arm" in text:
            depth = int(backlog.group(1))
            for match in TEMPORAL_REP_RE.finditer(text):
                valid = match.group(2) == "PASS" and not match.group(3)
                run = {"wfps": float(match.group(1)), "valid": valid, "file": path.name}
                runs.setdefault((TEMPORAL_TREE, TEMPORAL_ARM, depth), []).append(run)
    return runs


def valid_rates(runs, key):
    return [r["wfps"] for r in runs.get(key, []) if r["valid"]]


def mean(values):
    return sum(values) / len(values) if values else None


def complete(runs, key):
    return len(runs.get(key, [])) == EXPECTED_RUNS


def cell_mean(runs, tree, arm, depth):
    key = (tree, arm, depth)
    return mean(valid_rates(runs, key)) if complete(runs, key) else None


def span(runs, key):
    rates = valid_rates(runs, key)
    return (min(rates), max(rates)) if rates else None


def overlap(runs, a, b):
    if not (complete(runs, a) and complete(runs, b)):
        return None
    sa, sb = span(runs, a), span(runs, b)
    if sa is None or sb is None:
        return None
    return sa[0] <= sb[1] and sb[0] <= sa[1]


def signal_mean(runs, key, signal):
    values = [float(r[signal]) for r in runs.get(key, []) if r["valid"] and signal in r]
    return mean(values)


def fmt(value):
    return "n/a" if value is None else f"{value:.2f}"


def cell_table(runs):
    out = [
        "| tree | arm | depth | mean workflows/sec | per rep | valid reps |",
        "|:--|:--|--:|--:|:--|--:|",
    ]
    keys = [(t, a, d) for d in DEPTHS for t in TREES for a in HARVEST_ARMS]
    keys += [(TEMPORAL_TREE, TEMPORAL_ARM, d) for d in DEPTHS]
    for tree, arm, depth in sorted(keys, key=lambda k: (k[2], k[0] == TEMPORAL_TREE, k)):
        reps = runs.get((tree, arm, depth), [])
        per_rep = " / ".join(
            f"{r['wfps']:.2f}" + ("" if r["valid"] else " (invalid)") for r in reps
        )
        valid = len(valid_rates(runs, (tree, arm, depth)))
        if not complete(runs, (tree, arm, depth)):
            per_rep = f"{per_rep or 'none'} (incomplete: {len(reps)} of {EXPECTED_RUNS} runs)"
        out.append(
            f"| `{tree}` | `{arm}` | {depth} | {fmt(cell_mean(runs, tree, arm, depth))} "
            f"| {per_rep or 'none'} | {valid} |"
        )
    return "\n".join(out)


# Every per-run signal the harness prints, in table order.
SIGNAL_COLUMNS = [
    ("claim_n", "claims"),
    ("claim_mean_ms", "claim mean ms"),
    ("claim_p99_ms", "claim p99 ms"),
    ("persist_n", "persists"),
    ("persist_mean_ms", "persist mean ms"),
    ("persist_p99_ms", "persist p99 ms"),
    ("scan_n", "scans"),
    ("scan_mean_ms", "scan mean ms"),
    ("scan_p99_ms", "scan p99 ms"),
    ("heartbeat_n", "heartbeats"),
    ("heartbeat_mean_ms", "heartbeat mean ms"),
    ("heartbeat_p99_ms", "heartbeat p99 ms"),
    ("wait_n", "pool waits"),
    ("wait_p99_ms", "pool wait p99 ms"),
    ("wait_max_ms", "pool wait max ms"),
    ("in_use_mean", "in use, mean"),
    ("in_use_max", "in use, max"),
]


def signal_table(runs):
    """Every registered #1815 field, as a mean over the valid reps."""
    out = [
        "| tree | arm | depth | " + " | ".join(label for _, label in SIGNAL_COLUMNS) + " |",
        "|:--|:--|--:|" + "--:|" * len(SIGNAL_COLUMNS),
    ]
    for tree in TREES:
        for arm in HARVEST_ARMS:
            for depth in DEPTHS:
                key = (tree, arm, depth)
                cells = [signal_mean(runs, key, field) for field, _ in SIGNAL_COLUMNS]
                out.append(
                    f"| `{tree}` | `{arm}` | {depth} | " + " | ".join(fmt(c) for c in cells) + " |"
                )
    return "\n".join(out)


def grade(passed):
    if passed is None:
        return "**INDETERMINATE**"
    return "**PASS**" if passed else "**KILL**"


def overlap_note(runs, a, b):
    o = overlap(runs, a, b)
    if o is None:
        return ""
    return " Ranges overlap." if o else " Ranges do not overlap."


def lines(runs):
    out = []
    t2000 = cell_mean(runs, TEMPORAL_TREE, TEMPORAL_ARM, 2000)
    temporal_2000 = (TEMPORAL_TREE, TEMPORAL_ARM, 2000)

    # L1.
    h = cell_mean(runs, TRUNK, "postgres", 2000)
    ok = None if h is None or t2000 is None else h >= t2000
    out.append(
        f"* **L1** `postgres` on `{TRUNK}` at 2000: {fmt(h)} against `temporal_go` "
        f"{fmt(t2000)}: {grade(ok)}."
        + overlap_note(runs, (TRUNK, "postgres", 2000), temporal_2000)
    )

    # L2.
    verdicts, parts = [], []
    for depth in DEPTHS:
        best = [cell_mean(runs, TRUNK, a, depth) for a in HARVEST_ARMS]
        best = None if None in best else max(best)
        t = cell_mean(runs, TEMPORAL_TREE, TEMPORAL_ARM, depth)
        verdicts.append(None if best is None or t is None else best >= t)
        parts.append(f"{depth}: {fmt(best)} against {fmt(t)}")
    ok = None if None in verdicts else all(verdicts)
    out.append(f"* **L2** best mode on `{TRUNK}` at every depth ({'; '.join(parts)}): {grade(ok)}.")

    # L3.
    deep = cell_mean(runs, FIX, "postgres", 2000)
    shallow = cell_mean(runs, FIX, "postgres", 250)
    ok = None if deep is None or shallow is None else deep >= 0.80 * shallow
    ratio = None if ok is None or shallow == 0 else deep / shallow
    out.append(
        f"* **L3** `postgres` on `{FIX}`, depth 2000 over depth 250: {fmt(deep)} / "
        f"{fmt(shallow)} = {fmt(ratio)} against a 0.80 line: {grade(ok)}."
        + overlap_note(runs, (FIX, "postgres", 2000), (FIX, "postgres", 250))
    )

    # L4.
    before = cell_mean(runs, BASE, "postgres", 2000)
    ok = None if deep is None or before is None else deep >= 2.0 * before
    ratio = None if ok is None or before == 0 else deep / before
    out.append(
        f"* **L4** `postgres` at 2000, `{FIX}` over `{BASE}`: {fmt(deep)} / {fmt(before)} "
        f"= {fmt(ratio)}x against a 2.0x line: {grade(ok)}."
        + overlap_note(runs, (FIX, "postgres", 2000), (BASE, "postgres", 2000))
    )

    # L5.
    ok = None if deep is None or t2000 is None or deep == 0 else t2000 / deep <= 2.5
    ratio = None if ok is None else t2000 / deep
    out.append(
        f"* **L5** `temporal_go` over `postgres` on `{FIX}` at 2000: {fmt(t2000)} / "
        f"{fmt(deep)} = {fmt(ratio)}x against a 2.5x line: {grade(ok)}."
    )
    return "\n".join(out)


def attribution(runs):
    out = []
    for tree in TREES:
        shallow = cell_mean(runs, tree, "postgres", 250)
        deep = cell_mean(runs, tree, "postgres", 2000)
        if shallow is None or deep is None or deep >= 0.75 * shallow:
            out.append(f"* `{tree}`: `postgres` does not fall by more than 25%. No attribution.")
            continue
        rises = []
        for signal in SIGNALS:
            a = signal_mean(runs, (tree, "postgres", 250), signal)
            b = signal_mean(runs, (tree, "postgres", 2000), signal)
            if a and b is not None:
                rises.append((b / a, signal, a, b))
        if not rises:
            out.append(f"* `{tree}`: no signal recorded.")
            continue
        top = max(rises)
        out.append(
            f"* `{tree}`: `postgres` falls {fmt(shallow)} to {fmt(deep)}. "
            f"`{top[1]}` rises most: {top[2]:.2f} to {top[3]:.2f} ({top[0]:.1f}x)."
        )
    return "\n".join(out)


def main(argv):
    if len(argv) != 2:
        print(__doc__)
        return 2
    runs = load(argv[1])
    print("## Cells\n")
    print(cell_table(runs))
    print("\n## #1815 signals (means over valid reps)\n")
    print(signal_table(runs))
    print("\n## Lines\n")
    print(lines(runs))
    print("\n## Attribution\n")
    print(attribution(runs))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
