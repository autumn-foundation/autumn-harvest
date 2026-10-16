#!/usr/bin/env python3
"""Merge, summarize and ratchet LCOV coverage (issue #1818).

Usage:
    coverage.py convert IN.json --root DIR --output OUT
    coverage.py merge [--root DIR] --output OUT IN...
    coverage.py summary IN --json OUT --markdown OUT [--baseline B]
    coverage.py ratchet --baseline B --summary S [--update]
    coverage.py --self-test
"""
import argparse
import json
import math
import sys
from pathlib import Path


def _relative(path, root):
    """Return `path` relative to `root`, with `/` separators."""
    path = path.replace("\\", "/")
    if not root:
        return path
    prefix = root.replace("\\", "/").rstrip("/") + "/"
    return path[len(prefix):] if path.startswith(prefix) else path


def _new():
    return {"lines": {}, "branches": {}}


def parse_lcov(text, root):
    """Parse an LCOV tracefile into per-file line and branch hit counts.

    Each file maps to `lines` (line number to hits) and `branches`
    ((line, block, branch) to hits). LCOV writes `-` for a branch whose
    block never ran. That counts as zero hits. The parser drops function
    records, because the report does not use them.
    """
    records = {}
    current = None
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("SF:"):
            current = records.setdefault(_relative(line[3:], root), _new())
        elif line == "end_of_record":
            current = None
        elif current is None:
            continue
        elif line.startswith("DA:"):
            number, hits = line[3:].split(",")[:2]
            lines = current["lines"]
            lines[int(number)] = lines.get(int(number), 0) + int(hits)
        elif line.startswith("BRDA:"):
            number, block, branch, taken = line[5:].split(",")
            key = (int(number), block, branch)
            hits = 0 if taken == "-" else int(taken)
            current["branches"][key] = current["branches"].get(key, 0) + hits
    return records


def _line_counts(segments):
    """Return the hit count of each mapped line, as `llvm-cov` computes it.

    This is the rule of `LineCoverageStats` in LLVM. A line is mapped when a
    counted region starts on it, or when a counted segment wraps into it. A
    line that starts a skipped region is not mapped. A gap region does not
    count as a start. The count is the largest count that applies.
    """
    out = {}
    if not segments:
        return out
    index, wrapped, current = 0, None, []
    for line in range(segments[0][0], segments[-1][0] + 1):
        if current:
            wrapped = current[-1]
        current = []
        while index < len(segments) and segments[index][0] == line:
            current.append(segments[index])
            index += 1
        starts = [s[2] for s in current if s[3] and s[4] and not s[5]]
        skipped = bool(current) and not current[0][3] and current[0][4]
        if skipped or not (starts or (wrapped and wrapped[3])):
            continue
        out[line] = max(starts + ([wrapped[2]] if wrapped else []))
    return out


def from_export(doc, root):
    """Convert `llvm-cov export` JSON into the records of `parse_lcov`.

    The LCOV exporter of LLVM 23 crashes on this crate, so the jobs export
    JSON. A branch key is its line, its column and its side. Two
    instantiations of one generic branch share a key, so their hits add up.
    The branches of each macro expansion count too.

    The JSON does not mark a folded branch side. LLVM drops a folded side
    from its own totals, and this script counts it as not taken. So a branch
    percent here can be lower than `cargo llvm-cov report` shows. The
    difference is the same each night, so the ratchet is not affected.
    """
    if doc.get("type") != "llvm.coverage.json.export":
        raise ValueError("not an llvm-cov export JSON file")
    records = {}
    for data in doc["data"]:
        for item in data["files"]:
            into = records.setdefault(_relative(item["filename"], root), _new())
            for number, hits in _line_counts(item["segments"]).items():
                into["lines"][number] = into["lines"].get(number, 0) + hits
            for branch in item["branches"]:
                _add_branch(into, branch[0], str(branch[1]), branch)
            # A branch inside a macro expansion counts at the line of the
            # call, as in the LCOV exporter. Its key also holds the call
            # column and its own place, so two branches stay apart.
            for expansion in item.get("expansions", []):
                line, column = expansion["source_region"][:2]
                for branch in expansion.get("branches", []):
                    _add_branch(into, line, f"{column}@{branch[0]}:{branch[1]}", branch)
    return records


def _add_branch(into, line, block, branch):
    for side, hits in (("0", branch[4]), ("1", branch[5])):
        key = (line, block, side)
        into["branches"][key] = into["branches"].get(key, 0) + hits


def merge(records):
    """Sum the hits of several parsed tracefiles.

    The key of a line is its file and number, so a line that two shards run
    counts once, with the hits of both.
    """
    out = {}
    for tracefile in records:
        for path, data in tracefile.items():
            into = out.setdefault(path, _new())
            for kind in ("lines", "branches"):
                for key, hits in data[kind].items():
                    into[kind][key] = into[kind].get(key, 0) + hits
    return out


def render_lcov(records):
    """Write parsed records back as an LCOV tracefile."""
    out = []
    for path in sorted(records):
        data = records[path]
        out.append(f"SF:{path}")
        for number in sorted(data["lines"]):
            out.append(f"DA:{number},{data['lines'][number]}")
        for key in sorted(data["branches"]):
            number, block, branch = key
            out.append(f"BRDA:{number},{block},{branch},{data['branches'][key]}")
        branches = data["branches"].values()
        out.append(f"BRF:{len(branches)}")
        out.append(f"BRH:{sum(1 for h in branches if h > 0)}")
        out.append(f"LF:{len(data['lines'])}")
        out.append(f"LH:{sum(1 for h in data['lines'].values() if h > 0)}")
        out.append("end_of_record")
    return "\n".join(out) + "\n"


def _pct(hit, total):
    return round(100.0 * hit / total, 2) if total else None


def summarize(records):
    """Return line and branch counts and percentages for each file."""
    files = {}
    for path in sorted(records):
        data = records[path]
        row = {}
        for kind in ("lines", "branches"):
            counts = data[kind].values()
            hit = sum(1 for h in counts if h > 0)
            row[f"{kind}_hit"] = hit
            row[f"{kind}_total"] = len(counts)
            row[f"{kind}_pct"] = _pct(hit, len(counts))
        files[path] = row
    return files


def _module_floors(baseline):
    return baseline["modules"].items()


def check(baseline, summary):
    """Return one message for each critical module below its floor.

    A module fails when its coverage is lower than its floor minus the
    tolerance. A module `tolerance` overrides the global one. A small
    module needs it, because one branch there is worth more than a point. A module with no data fails too. Otherwise a moved or renamed
    module would leave the gate in silence. A `null` floor is not recorded
    yet, so it passes. The guard forbids a `null` floor in the committed
    baseline.
    """
    found = []
    for path, floors in _module_floors(baseline):
        tolerance = floors.get("tolerance", baseline["tolerance"])
        row = summary["files"].get(path)
        if not row or not row["lines_total"]:
            found.append(f"{path}: no coverage data")
            continue
        for kind in ("lines", "branches"):
            floor = floors.get(kind)
            current = row[f"{kind}_pct"]
            if floor is None:
                continue
            if current is None:
                found.append(f"{path}: no branch data, but the {kind} floor is {floor}")
            elif current < round(floor - tolerance, 2):
                found.append(
                    f"{path}: {kind} coverage {current}% is below the floor {floor}% "
                    f"minus the tolerance {tolerance}"
                )
    return found


def _round_down(pct):
    # The epsilon stops float error from taking 37.80 down to 37.79.
    return math.floor(pct * 100 + 1e-6) / 100


def raised(baseline, summary):
    """Return a copy of `baseline` with each floor raised to the current value.

    A floor never drops. A `null` floor takes the current value.
    """
    out = json.loads(json.dumps(baseline))
    for path, floors in _module_floors(out):
        row = summary["files"].get(path) or {}
        for kind in ("lines", "branches"):
            current = row.get(f"{kind}_pct")
            if current is None:
                continue
            current = _round_down(current)
            if floors.get(kind) is None or current > floors[kind]:
                floors[kind] = current
    return out


def _fmt(pct):
    return "n/a" if pct is None else f"{pct:.2f}%"


def files_markdown(files):
    """Render the per-crate totals and the per-file table."""
    crates = {}
    for path, row in files.items():
        if "/src/" in path:
            crate = path.split("/src/")[0]
        else:
            crate = "." if path.startswith("src/") else path.split("/")[0]
        total = crates.setdefault(crate, dict.fromkeys(row, 0))
        for key in ("lines_hit", "lines_total", "branches_hit", "branches_total"):
            total[key] += row[key]
    out = ["## Coverage by crate", "", "| Crate | Lines | Branches |", "|---|---:|---:|"]
    for crate in sorted(crates):
        t = crates[crate]
        lines = _pct(t["lines_hit"], t["lines_total"])
        branches = _pct(t["branches_hit"], t["branches_total"])
        out.append(f"| `{crate}` | {_fmt(lines)} | {_fmt(branches)} |")
    out += ["", "<details><summary>Coverage by module</summary>", ""]
    out += ["| Module | Lines | Branches |", "|---|---:|---:|"]
    for path, row in files.items():
        lines = f"{_fmt(row['lines_pct'])} ({row['lines_hit']}/{row['lines_total']})"
        branches = f"{_fmt(row['branches_pct'])} ({row['branches_hit']}/{row['branches_total']})"
        out.append(f"| `{path}` | {lines} | {branches} |")
    out += ["", "</details>", ""]
    return "\n".join(out)


def ratchet_markdown(baseline, summary, found, partial=False):
    """Render each critical module against its floor.

    A `partial` run had a failed test job. Its coverage is short, so the
    summary shows no verdict and suggests no new floor.
    """
    out = ["## Critical modules (issue #1818)", ""]
    out.append(f"Tolerance: {baseline['tolerance']} points, unless a module sets its own.")
    out += ["", "| Module | Lines | Floor | Branches | Floor |", "|---|---:|---:|---:|---:|"]
    for path, floors in _module_floors(baseline):
        row = summary["files"].get(path) or {}
        out.append(
            f"| `{path}` | {_fmt(row.get('lines_pct'))} | {_fmt(floors.get('lines'))} "
            f"| {_fmt(row.get('branches_pct'))} | {_fmt(floors.get('branches'))} |"
        )
    out.append("")
    if partial:
        out += ["Ratchet skipped: a test job failed, so the coverage of this run is partial.", ""]
        return "\n".join(out)
    floors_set = [f.get(k) for _, f in _module_floors(baseline) for k in ("lines", "branches")]
    if all(v is None for v in floors_set):
        out += ["No floor is recorded yet. The ratchet checks only that each module has data.", ""]
    out += [f"- FAIL: {m}" for m in found] or ["No critical module is below its floor."]
    if raised(baseline, summary) != baseline:
        out += ["", "A floor can rise. Download `summary.json`, run `python3 .github/ci/coverage.py "
                "ratchet --update --baseline .github/ci/coverage-baseline.json --summary summary.json`, "
                "and commit the baseline."]
    out.append("")
    return "\n".join(out)


def _write(path, text):
    Path(path).write_text(text, encoding="utf-8")


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def cmd_convert(args):
    doc = _read_json(args.input)
    _write(args.output, render_lcov(from_export(doc, args.root)))
    return 0


def cmd_merge(args):
    tracefiles = [parse_lcov(Path(p).read_text(encoding="utf-8"), args.root) for p in args.inputs]
    _write(args.output, render_lcov(merge(tracefiles)))
    return 0


def cmd_summary(args):
    files = summarize(merge([parse_lcov(Path(args.input).read_text(encoding="utf-8"), "")]))
    summary = {"files": files}
    _write(args.json, json.dumps(summary, indent=2) + "\n")
    text = files_markdown(files)
    if args.baseline:
        baseline = _read_json(args.baseline)
        found = check(baseline, summary)
        text = ratchet_markdown(baseline, summary, found, args.partial) + "\n" + text
    _write(args.markdown, text)
    return 0


def cmd_ratchet(args):
    baseline = _read_json(args.baseline)
    summary = _read_json(args.summary)
    if args.update:
        _write(args.baseline, json.dumps(raised(baseline, summary), indent=2) + "\n")
        print(f"updated {args.baseline}")
        return 0
    found = check(baseline, summary)
    for message in found:
        print(f"FAIL: {message}")
    if not found:
        print("No critical module is below its floor.")
    return 1 if found else 0


def _parser():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("convert", help="convert llvm-cov export JSON into LCOV")
    p.add_argument("input")
    p.add_argument("--root", required=True, help="workspace root to strip from paths")
    p.add_argument("--output", required=True)
    p.set_defaults(func=cmd_convert)
    p = sub.add_parser("merge", help="merge LCOV tracefiles")
    p.add_argument("--root", default="", help="workspace root to strip from paths")
    p.add_argument("--output", required=True)
    p.add_argument("inputs", nargs="+")
    p.set_defaults(func=cmd_merge)
    p = sub.add_parser("summary", help="write per-module JSON and Markdown")
    p.add_argument("input")
    p.add_argument("--json", required=True)
    p.add_argument("--markdown", required=True)
    p.add_argument("--baseline", help="also render the critical modules against their floors")
    p.add_argument("--partial", action="store_true", help="a test job failed; show no verdict")
    p.set_defaults(func=cmd_summary)
    p = sub.add_parser("ratchet", help="compare critical modules with their floors")
    p.add_argument("--baseline", required=True)
    p.add_argument("--summary", required=True)
    p.add_argument("--update", action="store_true", help="raise each floor; never lower one")
    p.set_defaults(func=cmd_ratchet)
    return parser


SHARD_A = """TN:
SF:/w/r/autumn-harvest/src/queue.rs
FN:3,f
FNDA:1,f
DA:3,1
DA:4,0
BRDA:4,0,0,1
BRDA:4,0,1,-
BRF:2
BRH:1
LF:2
LH:1
end_of_record
SF:/w/r/autumn-harvest/src/erase.rs
DA:1,0
LF:1
LH:0
end_of_record
"""

SHARD_B = """SF:/w/r/autumn-harvest/src/queue.rs
DA:4,2
DA:5,0
BRDA:4,0,0,-
BRDA:4,0,1,3
end_of_record
"""


# An `llvm-cov export` JSON file. A segment is [line, column, count,
# has_count, is_region_entry, is_gap_region]. A branch is [line, column,
# end_line, end_column, true_count, false_count, file_id, expanded_id, kind].
EXPORT = {
    "type": "llvm.coverage.json.export",
    "data": [{"files": [{
        "filename": "/w/r/autumn-harvest/src/timeout.rs",
        "segments": [
            [1, 1, 5, True, True, False], [1, 10, 0, False, False, False],
            [3, 1, 2, True, True, False], [3, 5, 7, True, True, False],
            [3, 9, 0, False, False, False],
            [5, 1, 0, True, True, False], [6, 3, 0, False, False, False],
            [8, 1, 0, False, True, False],
            [10, 1, 4, True, True, True], [10, 20, 3, True, True, False],
        ],
        "branches": [[3, 5, 3, 8, 2, 0, 0, 0, 4], [3, 5, 3, 8, 1, 1, 0, 0, 4]],
        # A macro call on line 6, column 3. Its branch is at line 40 of the
        # macro definition.
        "expansions": [{
            "source_region": [6, 3, 6, 20, 1, 0, 1, 1],
            "branches": [[40, 9, 40, 14, 1, 0, 1, 0, 4]],
        }],
    }]}],
}


def self_test():
    timeout = from_export(EXPORT, "/w/r")["autumn-harvest/src/timeout.rs"]
    # Lines 2 and 4 follow a segment with no count. Line 6 follows a counted
    # region. Line 8 starts a skipped region. Line 10 starts with a gap, so
    # only its second region counts.
    assert timeout["lines"] == {1: 5, 3: 7, 5: 0, 6: 0, 10: 3}, timeout
    # Two instantiations of one branch add up. A branch inside a macro
    # expansion counts at the line of the call, as the LCOV exporter does.
    assert timeout["branches"] == {
        (3, "5", "0"): 3, (3, "5", "1"): 1,
        (6, "3@40:9", "0"): 1, (6, "3@40:9", "1"): 0,
    }, timeout
    try:
        from_export({"type": "other"}, "")
        raise AssertionError("a non-export file must fail")
    except ValueError:
        pass

    a = parse_lcov(SHARD_A, "/w/r")
    b = parse_lcov(SHARD_B, "/w/r/")
    assert set(a) == {"autumn-harvest/src/queue.rs", "autumn-harvest/src/erase.rs"}, a
    assert set(parse_lcov(SHARD_B, "")) == {"/w/r/autumn-harvest/src/queue.rs"}
    assert set(parse_lcov(SHARD_B, "/other")) == {"/w/r/autumn-harvest/src/queue.rs"}

    merged = merge([a, b])
    queue = merged["autumn-harvest/src/queue.rs"]
    assert queue["lines"] == {3: 1, 4: 2, 5: 0}, queue
    assert queue["branches"] == {(4, "0", "0"): 1, (4, "0", "1"): 3}, queue
    assert merge([a, a])["autumn-harvest/src/queue.rs"]["lines"] == {3: 2, 4: 0}

    again = merge([parse_lcov(render_lcov(merged), "/elsewhere")])
    assert again == merged, again

    files = summarize(merged)
    assert files["autumn-harvest/src/queue.rs"] == {
        "lines_hit": 2, "lines_total": 3, "lines_pct": 66.67,
        "branches_hit": 2, "branches_total": 2, "branches_pct": 100.0,
    }, files
    erase = files["autumn-harvest/src/erase.rs"]
    assert erase["lines_pct"] == 0.0 and erase["branches_pct"] is None, erase

    summary = {"files": files}
    queue_floor = {"lines": 66.0, "branches": 100.0}

    def base(modules, tolerance=0.5):
        return {"tolerance": tolerance, "modules": modules}

    assert check(base({"autumn-harvest/src/queue.rs": queue_floor}), summary) == []
    within = {"lines": 67.1, "branches": 100.0}
    assert check(base({"autumn-harvest/src/queue.rs": within}), summary) == []
    drop = {"lines": 67.2, "branches": 100.0}
    found = check(base({"autumn-harvest/src/queue.rs": drop}), summary)
    assert len(found) == 1 and "lines" in found[0] and "67.2" in found[0], found
    found = check(base({"autumn-harvest/src/gone.rs": queue_floor}), summary)
    assert len(found) == 1 and "no coverage data" in found[0], found
    found = check(base({"autumn-harvest/src/erase.rs": {"lines": 0.0, "branches": 10.0}}), summary)
    assert len(found) == 1 and "no branch data" in found[0], found
    pending = {"lines": None, "branches": None}
    assert check(base({"autumn-harvest/src/queue.rs": pending}), summary) == []

    low = base({"autumn-harvest/src/queue.rs": {"lines": 50.0, "branches": 100.0}})
    new = raised(low, summary)
    assert new["modules"]["autumn-harvest/src/queue.rs"] == {"lines": 66.67, "branches": 100.0}, new
    assert low["modules"]["autumn-harvest/src/queue.rs"]["lines"] == 50.0, "raised must copy"
    high = base({"autumn-harvest/src/queue.rs": {"lines": 90.0, "branches": 100.0}})
    assert raised(high, summary) == high, "a floor never drops"
    new = raised(base({"autumn-harvest/src/queue.rs": pending}), summary)
    assert new["modules"]["autumn-harvest/src/queue.rs"] == {"lines": 66.67, "branches": 100.0}, new
    text = ratchet_markdown(low, summary, check(low, summary))
    assert "| `autumn-harvest/src/queue.rs` | 66.67% | 50.00% | 100.00% | 100.00% |" in text, text
    assert "A floor can rise" in text and "FAIL" not in text, text
    text = ratchet_markdown(high, summary, check(high, summary))
    assert "- FAIL: autumn-harvest/src/queue.rs: lines" in text and "can rise" not in text, text
    text = files_markdown(files)
    assert "| `autumn-harvest` | 50.00% | 100.00% |" in text, text
    assert "| `autumn-harvest/src/erase.rs` | 0.00% (0/1) | n/a (0/0) |" in text, text
    assert "| `.` |" in files_markdown({"src/lib.rs": erase}), "a root crate"

    # A module tolerance overrides the global one, and `raised` keeps it.
    small = base({"autumn-harvest/src/queue.rs": {"lines": 66.67, "branches": 100.0}})
    one_branch = {"files": {"autumn-harvest/src/queue.rs": dict(files["autumn-harvest/src/queue.rs"], branches_pct=98.6)}}
    assert len(check(small, one_branch)) == 1
    small["modules"]["autumn-harvest/src/queue.rs"]["tolerance"] = 1.5
    assert check(small, one_branch) == []
    assert raised(small, summary)["modules"]["autumn-harvest/src/queue.rs"]["tolerance"] == 1.5

    # Float error must not move a floor or the tolerance edge.
    assert _round_down(37.8) == 37.8 and _round_down(19.49) == 19.49
    edge = {"files": {"m.rs": {"lines_total": 1, "lines_pct": 0.01, "branches_pct": None}}}
    assert check(base({"m.rs": {"lines": 0.51, "branches": None}}), edge) == []
    assert len(check(base({"m.rs": {"lines": 0.52, "branches": None}}), edge)) == 1
    # A run with a failed test job shows no verdict.
    text = ratchet_markdown(high, summary, check(high, summary), partial=True)
    assert "Ratchet skipped" in text and "FAIL" not in text and "can rise" not in text, text
    # With no floor recorded, the summary says what the gate still checks.
    unarmed = base({"autumn-harvest/src/queue.rs": pending})
    assert "No floor is recorded yet" in ratchet_markdown(unarmed, summary, [])
    assert "No floor is recorded yet" not in ratchet_markdown(low, summary, [])
    print("coverage.py self-test: ok")


def main(argv):
    if argv == ["--self-test"]:
        self_test()
        return 0
    args = _parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
