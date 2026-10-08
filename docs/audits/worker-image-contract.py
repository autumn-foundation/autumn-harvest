#!/usr/bin/env python3
"""Worker image and chart contract guard (issue #1989).

The release publishes one container image, and the Helm chart deploys it.
This guard fails when the image, the chart or the decision note drifts from
the contract that `docs/adr/0006-worker-container-image.md` states.

Checks:
1. The decision note has its sections and names the image and each binary.
2. The `Dockerfile` pins each base image by digest. The build image uses the
   toolchain of `rust-toolchain.toml`. The build is auditable and locked.
   The final stage runs as a non-root user and holds each binary and the
   plugin migrations.
3. `Chart.yaml` has the workspace version as `appVersion`, and a
   `kubeVersion` that allows the `preStop` sleep action.
4. The chart default image is the image that the release publishes.
5. The rendered chart has the probe paths, the `preStop` sleep, a grace
   period that covers the drain, a non-root security context, and a
   migration Job that runs as a pre-install and pre-upgrade hook.
6. The chart refuses to render without a database secret.

Check 5 and check 6 run `helm template`. `HELM` names the binary, else
`helm` on `PATH`. A missing `helm` or PyYAML is a hard failure, so the
check never skips silently.

`--self-test` runs the checks against fixtures. It needs PyYAML only.

Usage:
    python3 docs/audits/worker-image-contract.py [--self-test]
"""
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("worker-image-contract: PyYAML is required", file=sys.stderr)
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[2]
NOTE = "docs/adr/0006-worker-container-image.md"
DOCKERFILE = "Dockerfile"
TOOLCHAIN = "rust-toolchain.toml"
WORKSPACE = "Cargo.toml"
RELEASE = ".github/workflows/release.yml"
CHART = "charts/autumn-harvest-worker"
LINT_VALUES = f"{CHART}/ci/lint-values.yaml"

IMAGE = "ghcr.io/autumn-foundation/autumn-harvest"
BINARIES = ("harvest", "harvest-replay", "standalone-runner")
MIGRATIONS_SRC = "autumn-harvest-plugin/migrations/harvest"
MIGRATIONS_DIR = "/usr/share/autumn-harvest/migrations/harvest"
NONROOT = "65532:65532"
LIVE = "/api/harvest/health/live"
READY = "/api/harvest/health/ready"
SECTIONS = ("## Status", "## Context", "## Decision", "## Consequences")
# The `preStop` sleep action is on by default from Kubernetes 1.30.
KUBE_MIN = (1, 30)
# The runner drain: a 10 s response grace and the 25 s worker
# `shutdown_timeout`. The margin is the one that the probe guide uses.
DRAIN_SECONDS = 35
MARGIN_SECONDS = 10

DIGEST = re.compile(r"@sha256:[0-9a-f]{64}(?:\s|$)")


# ── Decision note ───────────────────────────────────────────────────────────


def check_note(text):
    """Return the findings for the decision note."""
    if text is None:
        return [f"{NOTE}: missing"]
    found = []
    lines = {line.rstrip() for line in text.splitlines()}
    for heading in SECTIONS:
        if heading not in lines:
            found.append(f"{NOTE}: no `{heading}` heading")
    for name in (IMAGE,) + tuple(f"`{b}`" for b in BINARIES):
        if name not in text:
            found.append(f"{NOTE}: does not name {name}")
    return found


# ── Dockerfile ──────────────────────────────────────────────────────────────


def stages(text):
    """Return (from line, body lines) for each stage, with continuations joined."""
    joined = re.sub(r"\\\n", " ", text)
    result = []
    for line in joined.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if re.match(r"(?i)^FROM\s", stripped):
            result.append((stripped, []))
        elif result:
            result[-1][1].append(stripped)
    return result


def check_dockerfile(text, channel):
    """Return the findings for the Dockerfile."""
    if text is None:
        return [f"{DOCKERFILE}: missing"]
    found = []
    parts = stages(text)
    if len(parts) < 2:
        return [f"{DOCKERFILE}: needs a build stage and a runtime stage"]
    names = set()
    for line, _ in parts:
        image = line.split()[1]
        alias = re.search(r"(?i)\sAS\s+(\S+)", line)
        if image not in names and not DIGEST.search(line):
            found.append(f"{DOCKERFILE}: `{line}` does not pin a digest")
        if alias:
            names.add(alias.group(1))
    build_line = parts[0][0]
    if f"rust:{channel}-" not in build_line:
        found.append(
            f"{DOCKERFILE}: the build image is not `rust:{channel}-*` from {TOOLCHAIN}"
        )
    build = " ".join(parts[0][1])
    for needle in ("cargo auditable build --release --locked",) + tuple(
        f"-p {p}" for p in ("autumn-harvest-cli", "standalone-runner")
    ):
        if needle not in build:
            found.append(f"{DOCKERFILE}: the build stage does not run `{needle}`")
    final = parts[-1][1]
    if f"USER {NONROOT}" not in final:
        found.append(f"{DOCKERFILE}: the runtime stage is not `USER {NONROOT}`")
    copies = " ".join(line for line in final if line.upper().startswith("COPY"))
    for binary in BINARIES:
        if not re.search(rf"/{re.escape(binary)}(?:\s|$)", copies):
            found.append(f"{DOCKERFILE}: the runtime stage does not copy `{binary}`")
    if MIGRATIONS_SRC not in copies or MIGRATIONS_DIR not in copies:
        found.append(
            f"{DOCKERFILE}: the runtime stage does not copy {MIGRATIONS_SRC} "
            f"to {MIGRATIONS_DIR}"
        )
    return found


def toolchain_channel(text):
    """Return the pinned channel of `rust-toolchain.toml`, or None."""
    match = re.search(r'^channel\s*=\s*"([^"]+)"', text or "", re.MULTILINE)
    return match.group(1) if match else None


# ── Chart metadata ──────────────────────────────────────────────────────────


def workspace_version(text):
    """Return `[workspace.package] version`, or None."""
    match = re.search(
        r'^\[workspace\.package\][^\[]*?^version\s*=\s*"([^"]+)"',
        text or "",
        re.MULTILINE | re.DOTALL,
    )
    return match.group(1) if match else None


def kube_floor(constraint):
    """Return the (major, minor) of a `>=X.Y` constraint, or None."""
    match = re.search(r">=\s*v?(\d+)\.(\d+)", constraint or "")
    return (int(match.group(1)), int(match.group(2))) if match else None


def check_chart(chart, version):
    """Return the findings for the parsed `Chart.yaml`."""
    if chart is None:
        return [f"{CHART}/Chart.yaml: missing"]
    found = []
    if str(chart.get("appVersion")) != version:
        found.append(
            f"{CHART}/Chart.yaml: appVersion {chart.get('appVersion')!r} "
            f"is not the workspace version {version!r}"
        )
    floor = kube_floor(chart.get("kubeVersion"))
    if floor is None or floor < KUBE_MIN:
        found.append(
            f"{CHART}/Chart.yaml: kubeVersion must be at least "
            f">={KUBE_MIN[0]}.{KUBE_MIN[1]} for the preStop sleep"
        )
    return found


def check_values(values, release_text):
    """Return the findings for the chart image against the release image."""
    found = []
    repo = ((values or {}).get("image") or {}).get("repository")
    if repo != IMAGE:
        found.append(f"{CHART}/values.yaml: image.repository is {repo!r}, not {IMAGE}")
    if f"IMAGE: {IMAGE}" not in (release_text or ""):
        found.append(f"{RELEASE}: does not publish `IMAGE: {IMAGE}`")
    return found


# ── Rendered chart ──────────────────────────────────────────────────────────


def by_kind(docs, kind):
    return [d for d in docs if isinstance(d, dict) and d.get("kind") == kind]


def http_path(probe):
    return ((probe or {}).get("httpGet") or {}).get("path")


def check_deployment(docs):
    """Return the findings for the rendered worker Deployment."""
    deployments = by_kind(docs, "Deployment")
    if len(deployments) != 1:
        return [f"{CHART}: renders {len(deployments)} Deployments, not 1"]
    pod = deployments[0]["spec"]["template"]["spec"]
    containers = pod.get("containers") or []
    if not containers:
        return [f"{CHART}: the Deployment has no container"]
    worker = containers[0]
    found = []
    if http_path(worker.get("livenessProbe")) != LIVE:
        found.append(f"{CHART}: the liveness probe does not use {LIVE}")
    if http_path(worker.get("readinessProbe")) != READY:
        found.append(f"{CHART}: the readiness probe does not use {READY}")
    sleep = (((worker.get("lifecycle") or {}).get("preStop") or {}).get("sleep") or {})
    seconds = sleep.get("seconds")
    if not isinstance(seconds, int) or seconds <= 0:
        found.append(f"{CHART}: the worker has no preStop sleep")
        seconds = 0
    grace = pod.get("terminationGracePeriodSeconds")
    floor = seconds + DRAIN_SECONDS + MARGIN_SECONDS
    if not isinstance(grace, int) or grace < floor:
        found.append(
            f"{CHART}: terminationGracePeriodSeconds {grace} is below {floor} "
            f"(preStop {seconds} + drain {DRAIN_SECONDS} + margin {MARGIN_SECONDS})"
        )
    pod_ctx = pod.get("securityContext") or {}
    ctx = worker.get("securityContext") or {}
    if not (pod_ctx.get("runAsNonRoot") or ctx.get("runAsNonRoot")):
        found.append(f"{CHART}: the worker does not set runAsNonRoot")
    if not ctx.get("readOnlyRootFilesystem"):
        found.append(f"{CHART}: the worker root filesystem is writable")
    return found


def check_migration_job(docs):
    """Return the findings for the rendered migration Job."""
    jobs = by_kind(docs, "Job")
    hooked = [
        j
        for j in jobs
        if {"pre-install", "pre-upgrade"}
        <= set(
            ((j.get("metadata") or {}).get("annotations") or {})
            .get("helm.sh/hook", "")
            .split(",")
        )
    ]
    if len(hooked) != 1:
        return [f"{CHART}: renders {len(hooked)} pre-install,pre-upgrade Jobs, not 1"]
    container = hooked[0]["spec"]["template"]["spec"]["containers"][0]
    argv = list(container.get("command") or []) + list(container.get("args") or [])
    found = []
    if not argv or not argv[0].endswith("harvest"):
        found.append(f"{CHART}: the migration Job does not run `harvest`")
    if argv[1:3] != ["migrate", "run"]:
        found.append(f"{CHART}: the migration Job does not run `migrate run`")
    if MIGRATIONS_DIR not in argv:
        found.append(f"{CHART}: the migration Job does not include {MIGRATIONS_DIR}")
    return found


def helm_binary():
    return os.environ.get("HELM") or shutil.which("helm")


def render(helm, *extra):
    """Return (exit code, stdout, stderr) of `helm template` on the chart."""
    proc = subprocess.run(
        [helm, "template", "contract", str(ROOT / CHART), *extra],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def check_render(helm):
    """Return the findings that need `helm template`."""
    code, out, err = render(helm, "-f", str(ROOT / LINT_VALUES))
    if code != 0:
        return [f"{CHART}: `helm template` failed: {err.strip()}"]
    docs = [d for d in yaml.safe_load_all(out) if d]
    found = check_deployment(docs) + check_migration_job(docs)
    code, _, err = render(helm)
    if code == 0 or "existingSecret" not in err:
        found.append(f"{CHART}: renders with no database.existingSecret")
    return found


# ── Runner ──────────────────────────────────────────────────────────────────


def read(rel):
    path = ROOT / rel
    return path.read_text(encoding="utf-8") if path.is_file() else None


def load(rel):
    text = read(rel)
    return yaml.safe_load(text) if text is not None else None


def run():
    helm = helm_binary()
    if helm is None:
        print("worker-image-contract: helm is required (set HELM)", file=sys.stderr)
        return 2
    channel = toolchain_channel(read(TOOLCHAIN))
    findings = (
        check_note(read(NOTE))
        + check_dockerfile(read(DOCKERFILE), channel)
        + check_chart(load(f"{CHART}/Chart.yaml"), workspace_version(read(WORKSPACE)))
        + check_values(load(f"{CHART}/values.yaml"), read(RELEASE))
    )
    if (ROOT / CHART).is_dir():
        findings += check_render(helm)
    for finding in findings:
        print(finding)
    print(f"worker-image-contract: {len(findings)} findings")
    return 1 if findings else 0


# ── Self-test ───────────────────────────────────────────────────────────────

PIN = "@sha256:" + "a" * 64

GOOD_DOCKERFILE = f"""\
FROM rust:1.99.0-bookworm{PIN} AS build
RUN cargo auditable build --release --locked \\
    -p autumn-harvest-cli -p standalone-runner
FROM gcr.io/distroless/cc-debian12:nonroot{PIN}
COPY --from=build /out/harvest /out/harvest-replay /out/standalone-runner /usr/local/bin/
COPY {MIGRATIONS_SRC} {MIGRATIONS_DIR}
USER {NONROOT}
"""

GOOD_NOTE = "\n".join(SECTIONS) + f"\n{IMAGE} " + " ".join(f"`{b}`" for b in BINARIES)


def pod(probe_live=LIVE, probe_ready=READY, sleep=10, grace=60, ro=True):
    return {
        "kind": "Deployment",
        "spec": {
            "template": {
                "spec": {
                    "terminationGracePeriodSeconds": grace,
                    "securityContext": {"runAsNonRoot": True},
                    "containers": [
                        {
                            "livenessProbe": {"httpGet": {"path": probe_live}},
                            "readinessProbe": {"httpGet": {"path": probe_ready}},
                            "lifecycle": {"preStop": {"sleep": {"seconds": sleep}}},
                            "securityContext": {"readOnlyRootFilesystem": ro},
                        }
                    ],
                }
            }
        },
    }


def job(hook="pre-install,pre-upgrade", argv=None):
    argv = argv or ["/usr/local/bin/harvest", "migrate", "run", "--include-dir", MIGRATIONS_DIR]
    return {
        "kind": "Job",
        "metadata": {"annotations": {"helm.sh/hook": hook}},
        "spec": {"template": {"spec": {"containers": [{"command": argv}]}}},
    }


def self_test():
    release = f"env:\n  IMAGE: {IMAGE}\n"
    chart = {"appVersion": "0.7.0", "kubeVersion": ">=1.30.0-0"}
    cases = [
        (check_note(GOOD_NOTE), 0),
        (check_note(None), 1),
        (check_note(GOOD_NOTE.replace("## Decision", "## Choice")), 1),
        (check_note(GOOD_NOTE.replace("`harvest-replay`", "")), 1),
        (check_dockerfile(GOOD_DOCKERFILE, "1.99.0"), 0),
        (check_dockerfile(None, "1.99.0"), 1),
        # A toolchain bump without a builder bump.
        (check_dockerfile(GOOD_DOCKERFILE, "1.100.0"), 1),
        # A tag with no digest.
        (check_dockerfile(GOOD_DOCKERFILE.replace(PIN, "", 1), "1.99.0"), 1),
        (check_dockerfile(GOOD_DOCKERFILE.replace(f"USER {NONROOT}", "USER root"), "1.99.0"), 1),
        (check_dockerfile(GOOD_DOCKERFILE.replace(" /out/harvest-replay", ""), "1.99.0"), 1),
        (check_dockerfile(GOOD_DOCKERFILE.replace("auditable ", ""), "1.99.0"), 1),
        (check_dockerfile(GOOD_DOCKERFILE.replace(f"COPY {MIGRATIONS_SRC}", "COPY x"), "1.99.0"), 1),
        # A stage built from an earlier stage needs no digest.
        (check_dockerfile(GOOD_DOCKERFILE + "FROM build AS extra\nUSER 65532:65532\n"
                          + "COPY /out/harvest /out/harvest-replay /out/standalone-runner "
                          + f"{MIGRATIONS_SRC} {MIGRATIONS_DIR} /x/\n", "1.99.0"), 0),
        (check_chart(chart, "0.7.0"), 0),
        (check_chart(None, "0.7.0"), 1),
        (check_chart(chart, "0.8.0"), 1),
        (check_chart({**chart, "kubeVersion": ">=1.29.0-0"}, "0.7.0"), 1),
        (check_chart({"appVersion": "0.7.0"}, "0.7.0"), 1),
        (check_values({"image": {"repository": IMAGE}}, release), 0),
        (check_values({"image": {"repository": "x/y"}}, release), 1),
        (check_values({"image": {"repository": IMAGE}}, "env: {}\n"), 1),
        (check_deployment([pod()]), 0),
        (check_deployment([]), 1),
        (check_deployment([pod(probe_live=READY)]), 1),
        (check_deployment([pod(probe_ready="/api/harvest/health")]), 1),
        (check_deployment([pod(sleep=0)]), 1),
        (check_deployment([pod(grace=30)]), 1),
        (check_deployment([pod(ro=False)]), 1),
        (check_migration_job([job()]), 0),
        (check_migration_job([]), 1),
        (check_migration_job([job(hook="post-install")]), 1),
        (check_migration_job([job(argv=["/usr/local/bin/harvest", "migrate", "status"])]), 2),
        (workspace_version('[workspace]\n[workspace.package]\nedition = "2024"\nversion = "1.2.3"\n'), "1.2.3"),
        (toolchain_channel('[toolchain]\nchannel = "1.99.0"\n'), "1.99.0"),
    ]
    bad = []
    for i, (got, want) in enumerate(cases):
        ok = got == want if isinstance(want, str) else len(got) == want
        if not ok:
            bad.append(i)
            print(f"self-test case {i}: got {got}")
    print(f"worker-image-contract self-test: {len(cases)} cases, {len(bad)} failed")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(self_test() if "--self-test" in sys.argv[1:] else run())
