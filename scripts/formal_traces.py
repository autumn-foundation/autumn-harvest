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
# "reject@N" also names the file line that no behavior may match.
EXPECT_RE = re.compile(r"^(accept|reject|reject@[1-9][0-9]*)$")
STATES_RE = re.compile(r"(\d+) distinct states? found")
# A trace check takes seconds. The bound stops a hung JVM.
TLC_TIMEOUT_SECS = 900
DEPTH_RE = re.compile(r"The depth of the complete state graph search is (\d+)\.")


class TraceError(Exception):
    """A trace file that the script cannot check."""


# TLC integers are 32-bit.
MAX_INT = 2**31 - 1


def is_count(value, low=0):
    """True for an int in [low, MAX_INT]. A bool is not an int here."""
    return type(value) is int and low <= value <= MAX_INT


def is_name(value):
    """True for a worker name: a string or null."""
    return value is None or isinstance(value, str)


def check_line(where, index, line):
    """Raise TraceError when one step line is malformed.

    Every field that reaches the TLA+ text is checked here, so a trace
    cannot inject TLA+ code.
    """
    if not isinstance(line, dict):
        raise TraceError(f"{where}: a line must be a JSON object")
    if line.get("op") not in OPS or (line["op"] == "init") != (index == 0):
        raise TraceError(f"{where}: bad op {line.get('op')!r}")
    if not isinstance(line.get("state"), str) or not STATE_RE.match(line["state"]):
        raise TraceError(f"{where}: bad state {line.get('state')!r}")
    for key in ("attempt", "strikes", "terminal"):
        if not is_count(line.get(key)):
            raise TraceError(f"{where}: bad {key} {line.get(key)!r}")
    for key in ("worker", "by"):
        if not is_name(line.get(key)):
            raise TraceError(f"{where}: bad {key} {line.get(key)!r}")
    actor = line.get("actor")
    if actor is not None and not (
        isinstance(actor, dict)
        and isinstance(actor.get("worker"), str)
        and is_count(actor.get("attempt"), low=1)
    ):
        raise TraceError(f"{where}: bad actor {actor!r}")


def load(path):
    """Return the header and the step lines of one trace file.

    Only the last line may be blank, so a reported line number is the
    line number in the file.
    """
    rows = []
    texts = path.read_text().splitlines()
    for number, text in enumerate(texts, start=1):
        if not text.strip():
            raise TraceError(f"{path}:{number}: blank line")
        try:
            rows.append(json.loads(text))
        except json.JSONDecodeError as err:
            raise TraceError(f"{path}:{number}: {err}") from err
    if len(rows) < 2:
        raise TraceError(f"{path}: want a header and at least one line")
    header, lines = rows[0], rows[1:]
    if not isinstance(header, dict):
        raise TraceError(f"{path}:1: the header must be a JSON object")
    if header.get("spec") not in SPECS:
        raise TraceError(f"{path}: unknown spec {header.get('spec')!r}")
    checks = header.get("checks")
    if not isinstance(checks, dict) or not checks:
        raise TraceError(f"{path}: the header has no checks")
    for guard, expect in checks.items():
        if guard not in GUARDS[header["spec"]] or not EXPECT_RE.match(str(expect)):
            raise TraceError(f"{path}: bad check {guard!r}: {expect!r}")
    for index, line in enumerate(lines):
        check_line(f"{path}:{index + 2}", index, line)
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
    try:
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
            timeout=TLC_TIMEOUT_SECS,
        )
    except (OSError, subprocess.TimeoutExpired) as err:
        return "error", f"TLC did not run: {err}"
    log = proc.stdout + proc.stderr
    if proc.returncode == 12 and "Invariant LogNotConsumed is violated" in log:
        return "accept", None
    if proc.returncode == 0 and "No error has been found" in log:
        # No initial state means that the init line does not match Init.
        states = STATES_RE.search(log)
        if states and int(states.group(1)) == 0:
            return "reject", 0
        depth = DEPTH_RE.search(log)
        if depth:
            return "reject", int(depth.group(1))
    return "error", log


def reject_line(depth):
    """The file line that TLC could not match, from the search depth.

    TraceInit matches line 2 at depth 1. Depth d leaves cursor = d + 1, so
    line d + 2 is the first line that no behavior matches.
    """
    return depth + 2


def meets(expect, verdict):
    """True when a verdict such as "reject@7" meets an expectation."""
    if expect == "reject":
        return verdict.startswith("reject")
    return verdict == expect


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
    # TLC runs in a temp dir, so a relative jar path must be made absolute.
    args.jar = pathlib.Path(args.jar).resolve()

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
        note = ""
        if verdict == "reject":
            note = f" ({describe_reject(detail, steps)})"
            verdict = f"reject@{reject_line(detail)}"
        for path, expect in uses:
            files += 1
            ok = meets(expect, verdict)
            mark = "" if ok else "  <-- FAIL"
            print(f"{path} [{spec}, {guard}]: want {expect}, got {verdict}{note}{mark}")
            if not ok:
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
