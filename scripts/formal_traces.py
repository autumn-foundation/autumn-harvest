#!/usr/bin/env python3
"""Check NDJSON engine traces against the TLA+ trace specs (issue #2003).

scripts/check-formal-traces.sh runs this file. Each trace file holds one
task row's history. Line 1 is a header:

    {"spec": "ActivityClaim", "case": "...", "checks": {"fixed": "accept"}}

Each other line is one committed transaction on the row. The first of them
is the row's insert, with op "init".

For each check, the script writes a TLA+ module that holds the trace as a
sequence. It then runs TLC on formal/tla/trace/<spec>Trace.tla with the
guard constants of that check. "accept" means that a behavior of the spec
matches every line. "reject" means that no behavior does.

The script fails when a result differs from its expectation, when TLC
fails for another reason, or when a directory has no trace for a spec.
"""

import argparse
import concurrent.futures
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys

SPECS = ("ActivityClaim", "WorkflowTaskClaim")

# The guard constants of each check. "fixed" is the code today.
GUARDS = {
    "ActivityClaim": {
        "fixed": {"Fenced": "TRUE"},
        "pre-fix": {"Fenced": "FALSE"},
    },
    "WorkflowTaskClaim": {
        "fixed": {"ChecksAttempt": "TRUE", "CapMissGuard": '"epoch"'},
        "pre-fix": {"ChecksAttempt": "FALSE", "CapMissGuard": '"strikes"'},
    },
}

OPS = ("init", "write", "start", "heartbeat")
STATE_RE = re.compile(r"^[A-Z_]+$")
DEPTH_RE = re.compile(r"The depth of the complete state graph search is (\d+)\.")


class TraceError(Exception):
    """A trace file that the script cannot check."""


def load(path):
    """Return the header and the step lines of one trace file."""
    rows = []
    for number, text in enumerate(path.read_text().splitlines(), start=1):
        if not text.strip():
            continue
        try:
            rows.append(json.loads(text))
        except json.JSONDecodeError as err:
            raise TraceError(f"{path}:{number}: {err}") from err
    if len(rows) < 2:
        raise TraceError(f"{path}: want a header and at least one line")
    header, lines = rows[0], rows[1:]
    if header.get("spec") not in SPECS:
        raise TraceError(f"{path}: unknown spec {header.get('spec')!r}")
    checks = header.get("checks")
    if not isinstance(checks, dict) or not checks:
        raise TraceError(f"{path}: the header has no checks")
    for guard, expect in checks.items():
        if guard not in GUARDS[header["spec"]] or expect not in ("accept", "reject"):
            raise TraceError(f"{path}: bad check {guard!r}: {expect!r}")
    for index, line in enumerate(lines):
        where = f"{path}:{index + 2}"
        if line.get("op") not in OPS or (line["op"] == "init") != (index == 0):
            raise TraceError(f"{where}: bad op {line.get('op')!r}")
        if not STATE_RE.match(str(line.get("state", ""))):
            raise TraceError(f"{where}: bad state {line.get('state')!r}")
        for key in ("attempt", "strikes", "terminal"):
            if not isinstance(line.get(key), int) or line[key] < 0:
                raise TraceError(f"{where}: bad {key} {line.get(key)!r}")
    return header, lines


def normalize(lines):
    """Rename the workers w1, w2, ... in order of first use.

    The specs are symmetric in their workers, so the rename keeps the
    verdict. Equal traces then share one TLC run.
    """
    names = {}

    def name(worker):
        if worker is None:
            return ""
        return names.setdefault(worker, f"w{len(names) + 1}")

    out = []
    for line in lines:
        actor = line.get("actor") or {}
        out.append(
            (
                line["op"],
                line["state"],
                name(line.get("worker")),
                line["attempt"],
                line["strikes"],
                line["terminal"],
                name(actor.get("worker")) if actor else "",
                actor.get("attempt", 0) if actor else 0,
                name(line.get("by")),
            )
        )
    return tuple(out), sorted(names.values())


def tla_record(step):
    op, state, worker, attempt, strikes, terminal, actor, actor_attempt, by = step
    return (
        f'[op |-> "{op}", state |-> "{state}", worker |-> "{worker}", '
        f"attempt |-> {attempt}, strikes |-> {strikes}, terminal |-> {terminal}, "
        f'actor |-> "{actor}", actorAttempt |-> {actor_attempt}, by |-> "{by}"]'
    )


def write_model(run_dir, tla_dir, spec, steps, workers, guard):
    """Write TraceRun.tla and TraceRun.cfg for one check into run_dir."""
    run_dir.mkdir(parents=True)
    for source in list(tla_dir.glob("*.tla")) + list((tla_dir / "trace").glob("*.tla")):
        shutil.copy(source, run_dir / source.name)
    records = ",\n    ".join(tla_record(s) for s in steps)
    worker_set = ", ".join(f'"{w}"' for w in workers)
    (run_dir / "TraceRun.tla").write_text(
        "---- MODULE TraceRun ----\n"
        f"EXTENDS {spec}Trace\n"
        f"TraceLog == <<\n    {records}\n>>\n"
        f"TraceWorkers == {{{worker_set}}}\n"
        "====\n"
    )
    max_claims = max([len(steps)] + [s[3] for s in steps])
    constants = {"NoWorker": "NoWorker", "MaxClaims": str(max_claims)}
    if spec == "WorkflowTaskClaim":
        constants["MaxStrikes"] = str(max(s[4] for s in steps))
    constants.update(GUARDS[spec][guard])
    body = "".join(f"    {k} = {v}\n" for k, v in constants.items())
    (run_dir / "TraceRun.cfg").write_text(
        "CONSTANTS\n"
        "    Log <- TraceLog\n"
        "    Workers <- TraceWorkers\n"
        f"{body}"
        "INIT TraceInit\n"
        "NEXT TraceNext\n"
        "INVARIANT LogNotConsumed\n"
        "CHECK_DEADLOCK FALSE\n"
    )


def run_tlc(jar, run_dir):
    """Run TLC in run_dir. Return ("accept" | "reject" | "error", detail)."""
    # Each JVM gets its own temp dir. Parallel TLC runs that share one
    # sometimes fail to parse a module.
    tmp = run_dir / "tmp"
    tmp.mkdir()
    proc = subprocess.run(
        [
            "java",
            "-XX:+UseParallelGC",
            f"-Djava.io.tmpdir={tmp}",
            "-cp",
            str(jar),
            "tlc2.TLC",
            "-config",
            "TraceRun.cfg",
            "-metadir",
            str(run_dir / "meta"),
            "-workers",
            "1",
            "-cleanup",
            "TraceRun.tla",
        ],
        cwd=run_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    log = proc.stdout + proc.stderr
    if proc.returncode == 12 and "Invariant LogNotConsumed is violated" in log:
        return "accept", None
    if proc.returncode == 0 and "No error has been found" in log:
        match = DEPTH_RE.search(log)
        depth = int(match.group(1)) if match else 0
        return "reject", depth
    return "error", log


def describe_reject(depth, steps):
    """Name the first line that no behavior of the spec matches."""
    if depth == 0:
        return "the init line is not an initial state of the spec"
    # TraceInit matches Log[1] at depth 1. Depth d leaves cursor = d + 1.
    index = depth + 1
    if index > len(steps):
        return "every line matched, but TLC did not report the end"
    return f"line {index + 1} is not a step of the spec: {tla_record(steps[index - 1])}"


def collect(dirs):
    """Group the trace files by normalized content."""
    groups = {}
    for directory in dirs:
        files = sorted(pathlib.Path(directory).glob("*.ndjson"))
        if not files:
            raise TraceError(f"{directory}: no traces")
        seen = set()
        for path in files:
            header, lines = load(path)
            seen.add(header["spec"])
            steps, workers = normalize(lines)
            for guard, expect in header["checks"].items():
                key = (header["spec"], steps, guard)
                groups.setdefault(key, (workers, []))[1].append((path, expect))
        missing = [s for s in SPECS if s not in seen]
        if missing:
            raise TraceError(f"{directory}: no trace for {', '.join(missing)}")
    return groups


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--jar", required=True)
    parser.add_argument("--tla", required=True, type=pathlib.Path)
    parser.add_argument("--work", required=True, type=pathlib.Path)
    parser.add_argument("dirs", nargs="+")
    args = parser.parse_args()

    try:
        groups = collect(args.dirs)
    except TraceError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1

    runs = list(groups.items())

    def check(numbered):
        number, ((spec, steps, guard), (workers, _)) = numbered
        run_dir = args.work / f"{number:05d}"
        write_model(run_dir, args.tla, spec, steps, workers, guard)
        result = run_tlc(args.jar, run_dir)
        shutil.rmtree(run_dir, ignore_errors=True)
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() or 2) as pool:
        results = list(pool.map(check, enumerate(runs)))

    failures = 0
    files = 0
    for ((spec, steps, guard), (_, uses)), (verdict, detail) in zip(runs, results):
        note = f" ({describe_reject(detail, steps)})" if verdict == "reject" else ""
        for path, expect in uses:
            files += 1
            mark = "" if verdict == expect else "  <-- FAIL"
            print(f"{path} [{spec}, {guard}]: want {expect}, got {verdict}{note}{mark}")
            if verdict != expect:
                failures += 1
                if verdict == "error":
                    print(detail)
    print(f"{len(groups)} TLC runs for {files} checks")
    if failures:
        print(f"{failures} of {files} trace checks failed")
        return 1
    print(f"all {files} trace checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
