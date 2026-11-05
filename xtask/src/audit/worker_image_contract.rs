//! `cargo xtask audit worker-image-contract`: the worker image and chart
//! contract guard (issue #1989).
//!
//! This is the port of `docs/audits/worker-image-contract.py`.
//!
//! The release publishes one container image, and the Helm chart deploys it.
//! This guard fails when the image, the chart or the decision note drifts from
//! the contract that `docs/adr/0006-worker-container-image.md` states.
//!
//! Checks:
//!
//! 1. The decision note has its sections and names the image and each binary.
//! 2. The `Dockerfile` pins each base image by digest. The build image and
//!    `RUSTUP_TOOLCHAIN` use the toolchain of `rust-toolchain.toml`. The build
//!    is auditable and locked. The last `USER` of the final stage is non-root.
//!    The final stage holds each binary and the plugin migrations.
//! 3. `Chart.yaml` has the workspace version as `appVersion`, and a
//!    `kubeVersion` that allows the `preStop` sleep action.
//! 4. The chart default image is the `IMAGE` that the release publishes.
//! 5. The rendered chart has the probe paths and the `preStop` sleep. Its
//!    grace period covers the drain, and its security context is non-root.
//!    Its migration Job runs as a pre-install and pre-upgrade hook.
//! 6. The chart refuses to render without a database secret.
//!
//! Check 5 and check 6 run `helm template`. `HELM` names the binary, else
//! `helm` on `PATH`. A missing `helm` is a hard failure with exit 2, so the
//! check never skips silently. The YAML reader is compiled in, so the `PyYAML`
//! requirement of the script is gone.
//!
//! `--self-test` runs the checks against fixtures. It needs no `helm`.
//!
//! Usage: `cargo xtask audit worker-image-contract [--self-test]`.

use std::path::Path;
use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;

use super::workflow_yaml_parse::pytext::{self, repr_str};
use super::workflow_yaml_parse::pyyaml::{self, Value};
use crate::util;

/// Arguments of `cargo xtask audit worker-image-contract`.
#[derive(Debug, Args)]
pub struct Args {
    /// Run the checks against fixtures instead of the repository.
    #[arg(long)]
    pub self_test: bool,
}

const NOTE: &str = "docs/adr/0006-worker-container-image.md";
const DOCKERFILE: &str = "Dockerfile";
const TOOLCHAIN: &str = "rust-toolchain.toml";
const WORKSPACE: &str = "Cargo.toml";
const RELEASE: &str = ".github/workflows/release.yml";
const CHART: &str = "charts/autumn-harvest-worker";
const LINT_VALUES: &str = "charts/autumn-harvest-worker/ci/lint-values.yaml";

const IMAGE: &str = "ghcr.io/autumn-foundation/autumn-harvest";
const BINARIES: [&str; 3] = ["harvest", "harvest-replay", "standalone-runner"];
const MIGRATIONS_SRC: &str = "autumn-harvest-plugin/migrations/harvest";
const MIGRATIONS_DIR: &str = "/usr/share/autumn-harvest/migrations/harvest";
const NONROOT: &str = "65532:65532";
const LIVE: &str = "/api/harvest/health/live";
const READY: &str = "/api/harvest/health/ready";
const SECTIONS: [&str; 4] = ["## Status", "## Context", "## Decision", "## Consequences"];
/// The `preStop` sleep action is on by default from Kubernetes 1.30.
const KUBE_MIN: (u64, u64) = (1, 30);
/// The runner drain: a 10 s response grace and the 25 s worker
/// `shutdown_timeout`. The margin is the one that the probe guide uses.
const DRAIN_SECONDS: i128 = 35;
const MARGIN_SECONDS: i128 = 10;

/// Compiles one of the fixed patterns of this module.
fn pattern(source: &str) -> Regex {
    Regex::new(source).unwrap_or_else(|error| unreachable!("pattern {source}: {error}"))
}

static DIGEST: LazyLock<Regex> = LazyLock::new(|| pattern(r"@sha256:[0-9a-f]{64}(?:\s|$)"));
static FROM_RE: LazyLock<Regex> = LazyLock::new(|| pattern(r"(?i)^FROM\s"));
static ALIAS_RE: LazyLock<Regex> = LazyLock::new(|| pattern(r"(?i)\sAS\s+(\S+)"));
static USER_RE: LazyLock<Regex> = LazyLock::new(|| pattern(r"(?i)^USER\s"));
static KUBE_RE: LazyLock<Regex> = LazyLock::new(|| pattern(r">=\s*v?(\d+)\.(\d+)"));

// ── Decision note ───────────────────────────────────────────────────────────

/// Returns the findings for the decision note.
fn check_note(text: Option<&str>) -> Vec<String> {
    let Some(text) = text else {
        return vec![format!("{NOTE}: missing")];
    };
    let mut found = Vec::new();
    let lines: Vec<&str> = pytext::splitlines(text)
        .into_iter()
        .map(pytext::rstrip)
        .collect();
    for heading in SECTIONS {
        if !lines.contains(&heading) {
            found.push(format!("{NOTE}: no `{heading}` heading"));
        }
    }
    let names = std::iter::once(IMAGE.to_owned()).chain(BINARIES.iter().map(|b| format!("`{b}`")));
    for name in names {
        if !text.contains(&name) {
            found.push(format!("{NOTE}: does not name {name}"));
        }
    }
    found
}

// ── Dockerfile ──────────────────────────────────────────────────────────────

/// Returns (from line, body lines) for each stage, with continuations joined.
fn stages(text: &str) -> Vec<(String, Vec<String>)> {
    let joined = text.replace("\\\n", " ");
    let mut result: Vec<(String, Vec<String>)> = Vec::new();
    for line in pytext::splitlines(&joined) {
        let stripped = pytext::strip(line);
        if stripped.is_empty() || stripped.starts_with('#') {
            continue;
        }
        if FROM_RE.is_match(stripped) {
            result.push((stripped.to_owned(), Vec::new()));
        } else if let Some((_, body)) = result.last_mut() {
            body.push(stripped.to_owned());
        }
    }
    result
}

/// Formats an optional string as an f-string formats it: `None` for none.
fn or_none(value: Option<&str>) -> &str {
    value.unwrap_or("None")
}

/// Returns the findings for the `Dockerfile`.
fn check_dockerfile(text: Option<&str>, channel: Option<&str>) -> Vec<String> {
    let Some(text) = text else {
        return vec![format!("{DOCKERFILE}: missing")];
    };
    let mut found = Vec::new();
    let parts = stages(text);
    if parts.len() < 2 {
        return vec![format!(
            "{DOCKERFILE}: needs a build stage and a runtime stage"
        )];
    }
    let mut names: Vec<String> = Vec::new();
    for (line, _) in &parts {
        let image = line.split_whitespace().nth(1).unwrap_or_default();
        if !names.iter().any(|name| name == image) && !DIGEST.is_match(line) {
            found.push(format!("{DOCKERFILE}: `{line}` does not pin a digest"));
        }
        if let Some(alias) = ALIAS_RE.captures(line) {
            names.push(alias[1].to_owned());
        }
    }
    let shown = or_none(channel);
    let build_line = &parts[0].0;
    if !build_line.contains(&format!("rust:{shown}-")) {
        found.push(format!(
            "{DOCKERFILE}: the build image is not `rust:{shown}-*` from {TOOLCHAIN}"
        ));
    }
    let build = parts[0].1.join(" ");
    // The env var picks the compiler, so it must move with the image tag.
    let toolchain_re = Regex::new(&format!(
        r"\bRUSTUP_TOOLCHAIN={}(?:\s|$)",
        regex::escape(channel.unwrap_or_default())
    ));
    if !toolchain_re.is_ok_and(|re| re.is_match(&build)) {
        found.push(format!(
            "{DOCKERFILE}: the build stage does not set RUSTUP_TOOLCHAIN={shown}"
        ));
    }
    let needles = std::iter::once("cargo auditable build --release --locked".to_owned())
        .chain(["autumn-harvest-cli", "standalone-runner"].map(|p| format!("-p {p}")));
    for needle in needles {
        if !build.contains(&needle) {
            found.push(format!(
                "{DOCKERFILE}: the build stage does not run `{needle}`"
            ));
        }
    }
    let last = &parts[parts.len() - 1].1;
    // Docker applies the last `USER`.
    let user = last
        .iter()
        .rfind(|line| USER_RE.is_match(line))
        .map(|line| {
            let rest = line.trim_start_matches(|c: char| !c.is_whitespace());
            pytext::strip(rest).to_owned()
        });
    if user.as_deref() != Some(NONROOT) {
        found.push(format!(
            "{DOCKERFILE}: the runtime stage does not end as `USER {NONROOT}`"
        ));
    }
    let copies = last
        .iter()
        .filter(|line| line.to_uppercase().starts_with("COPY"))
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    for binary in BINARIES {
        let in_copies = Regex::new(&format!(r"/{}(?:\s|$)", regex::escape(binary)))
            .is_ok_and(|re| re.is_match(&copies));
        if !in_copies {
            found.push(format!(
                "{DOCKERFILE}: the runtime stage does not copy `{binary}`"
            ));
        }
    }
    if !copies.contains(MIGRATIONS_SRC) || !copies.contains(MIGRATIONS_DIR) {
        found.push(format!(
            "{DOCKERFILE}: the runtime stage does not copy {MIGRATIONS_SRC} to {MIGRATIONS_DIR}"
        ));
    }
    found
}

/// Returns the parsed TOML, or an empty table.
fn parse_toml(text: Option<&str>) -> toml::Table {
    text.unwrap_or_default().parse().unwrap_or_default()
}

/// Returns the pinned channel of `rust-toolchain.toml`, or `None`.
fn toolchain_channel(text: Option<&str>) -> Option<String> {
    parse_toml(text)
        .get("toolchain")?
        .get("channel")?
        .as_str()
        .map(str::to_owned)
}

// ── Chart metadata ──────────────────────────────────────────────────────────

/// Returns `[workspace.package] version`, or `None`.
fn workspace_version(text: Option<&str>) -> Option<String> {
    parse_toml(text)
        .get("workspace")?
        .get("package")?
        .get("version")?
        .as_str()
        .map(str::to_owned)
}

/// Returns the (major, minor) of a `>=X.Y` constraint, or `None`.
fn kube_floor(constraint: Option<&str>) -> Option<(u64, u64)> {
    let found = KUBE_RE.captures(constraint.unwrap_or_default())?;
    Some((found[1].parse().ok()?, found[2].parse().ok()?))
}

/// Returns the findings for the parsed `Chart.yaml`.
fn check_chart(chart: Option<&Value>, version: Option<&str>) -> Vec<String> {
    let Some(chart) = chart.filter(|chart| **chart != Value::None) else {
        return vec![format!("{CHART}/Chart.yaml: missing")];
    };
    let Some(version) = version else {
        return vec![format!("{WORKSPACE}: no [workspace.package] version")];
    };
    let mut found = Vec::new();
    let app_version = chart.get("appVersion").unwrap_or(&Value::None);
    if app_version.py_str() != version {
        found.push(format!(
            "{CHART}/Chart.yaml: appVersion {} is not the workspace version {}",
            app_version.py_repr(),
            repr_str(version)
        ));
    }
    let floor = kube_floor(chart.get("kubeVersion").and_then(Value::as_str));
    if floor.is_none_or(|floor| floor < KUBE_MIN) {
        found.push(format!(
            "{CHART}/Chart.yaml: kubeVersion must be at least >={}.{} for the preStop sleep",
            KUBE_MIN.0, KUBE_MIN.1
        ));
    }
    found
}

/// Returns the findings for the chart image against the release image.
fn check_values(values: Option<&Value>, release: Option<&Value>) -> Vec<String> {
    let mut found = Vec::new();
    let repo = Value::or_get(Value::or_get(values, "image"), "repository");
    if repo.and_then(Value::as_str) != Some(IMAGE) {
        found.push(format!(
            "{CHART}/values.yaml: image.repository is {}, not {IMAGE}",
            repo.unwrap_or(&Value::None).py_repr()
        ));
    }
    // The publish job and the release notes both read the workflow `env`.
    let published = Value::or_get(Value::or_get(release, "env"), "IMAGE");
    if published.and_then(Value::as_str) != Some(IMAGE) {
        found.push(format!(
            "{RELEASE}: env.IMAGE is {}, not {IMAGE}",
            published.unwrap_or(&Value::None).py_repr()
        ));
    }
    found
}

// ── Rendered chart ──────────────────────────────────────────────────────────

fn by_kind<'a>(docs: &'a [Value], kind: &str) -> Vec<&'a Value> {
    docs.iter()
        .filter(|doc| doc.get("kind").and_then(Value::as_str) == Some(kind))
        .collect()
}

fn http_path(probe: Option<&Value>) -> Option<&str> {
    Value::or_get(Value::or_get(probe, "httpGet"), "path").and_then(Value::as_str)
}

/// A Python exception that stopped the script, such as a `KeyError` on a
/// rendered manifest or a `YAMLError` from a loaded file.
///
/// Python exits 1 after an uncaught exception, so the port exits 1 for it as
/// well. Exit 2 stays the code for a missing `helm`.
#[derive(Debug)]
struct Crash(String);

impl std::fmt::Display for Crash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Crash {}

/// Ports `value[key]`. A missing key is an error, as the `KeyError` was.
fn index<'a>(value: &'a Value, key: &str) -> Result<&'a Value> {
    value
        .get(key)
        .ok_or_else(|| Crash(format!("{CHART}: the rendered manifest has no `{key}` key")).into())
}

/// Ports `list(value or [])`: the items of a list, or the characters of a
/// string, or the keys of a dict.
fn py_list(value: Option<&Value>) -> Result<Vec<Value>> {
    let Some(value) = value.filter(|value| value.truthy()) else {
        return Ok(Vec::new());
    };
    Ok(match value {
        Value::List(items) | Value::Set(items) => items.clone(),
        Value::Str(text) => text.chars().map(|c| Value::Str(c.to_string())).collect(),
        Value::Dict(pairs) => pairs.iter().map(|(key, _)| key.clone()).collect(),
        other => {
            return Err(Crash(format!("{CHART}: `{}` is not iterable", other.py_repr())).into());
        }
    })
}

/// Ports `values[0]` on a list.
fn first(value: Option<&Value>, what: &str) -> Result<Value> {
    py_list(value)?
        .into_iter()
        .next()
        .ok_or_else(|| Crash(format!("{CHART}: the rendered {what} list is empty")).into())
}

/// Returns the findings for the rendered worker Deployment.
fn check_deployment(docs: &[Value]) -> Result<Vec<String>> {
    let deployments = by_kind(docs, "Deployment");
    if deployments.len() != 1 {
        return Ok(vec![format!(
            "{CHART}: renders {} Deployments, not 1",
            deployments.len()
        )]);
    }
    let pod = index(index(index(deployments[0], "spec")?, "template")?, "spec")?;
    let containers = py_list(pod.get("containers"))?;
    let Some(worker) = containers.first() else {
        return Ok(vec![format!("{CHART}: the Deployment has no container")]);
    };
    let mut found = Vec::new();
    if http_path(worker.get("livenessProbe")) != Some(LIVE) {
        found.push(format!("{CHART}: the liveness probe does not use {LIVE}"));
    }
    if http_path(worker.get("readinessProbe")) != Some(READY) {
        found.push(format!("{CHART}: the readiness probe does not use {READY}"));
    }
    let pre_stop = Value::or_get(worker.get("lifecycle"), "preStop");
    let seconds = Value::or_get(Value::or_get(pre_stop, "sleep"), "seconds");
    let (seconds_shown, seconds) =
        match seconds.and_then(|value| value.as_int().map(|n| (value, n))) {
            Some((value, number)) if number > 0 => (value.py_str(), number),
            _ => {
                found.push(format!("{CHART}: the worker has no preStop sleep"));
                ("0".to_owned(), 0)
            }
        };
    let grace = pod.get("terminationGracePeriodSeconds");
    // A huge `seconds` saturates instead of overflowing. The message still
    // shows the exact sum, as the Python `int` does.
    let floor = seconds
        .saturating_add(DRAIN_SECONDS)
        .saturating_add(MARGIN_SECONDS);
    let floor_shown = if seconds == i128::MAX {
        add_decimal(&seconds_shown, DRAIN_SECONDS + MARGIN_SECONDS)
    } else {
        floor.to_string()
    };
    if grace
        .and_then(Value::as_int)
        .is_none_or(|grace| grace < floor)
    {
        found.push(format!(
            "{CHART}: terminationGracePeriodSeconds {} is below {floor_shown} (preStop {seconds_shown} + drain {DRAIN_SECONDS} + margin {MARGIN_SECONDS})",
            grace.unwrap_or(&Value::None).py_str()
        ));
    }
    let pod_ctx = pod.get("securityContext");
    let ctx = worker.get("securityContext");
    let truthy = |value: Option<&Value>| value.is_some_and(Value::truthy);
    if !(truthy(Value::or_get(pod_ctx, "runAsNonRoot"))
        || truthy(Value::or_get(ctx, "runAsNonRoot")))
    {
        found.push(format!("{CHART}: the worker does not set runAsNonRoot"));
    }
    if !truthy(Value::or_get(ctx, "readOnlyRootFilesystem")) {
        found.push(format!("{CHART}: the worker root filesystem is writable"));
    }
    Ok(found)
}

/// Adds a small `addend` to a non-negative decimal string of any length.
fn add_decimal(digits: &str, addend: i128) -> String {
    let mut carry = u32::try_from(addend).unwrap_or(0);
    let mut out: Vec<char> = Vec::with_capacity(digits.len() + 1);
    for ch in digits.chars().rev() {
        let total = ch.to_digit(10).unwrap_or(0) + carry;
        out.push(char::from_digit(total % 10, 10).unwrap_or('0'));
        carry = total / 10;
    }
    while carry > 0 {
        out.push(char::from_digit(carry % 10, 10).unwrap_or('0'));
        carry /= 10;
    }
    out.iter().rev().collect()
}

/// Returns true when a Job runs as a pre-install and pre-upgrade hook.
fn is_install_hook(job: &Value) -> bool {
    let annotations = Value::or_get(job.get("metadata"), "annotations");
    let hooks = annotations
        .and_then(|annotations| annotations.get("helm.sh/hook"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let hooks: Vec<&str> = hooks.split(',').map(pytext::strip).collect();
    hooks.contains(&"pre-install") && hooks.contains(&"pre-upgrade")
}

/// Returns the findings for the rendered migration Job.
fn check_migration_job(docs: &[Value]) -> Result<Vec<String>> {
    let hooked: Vec<&Value> = by_kind(docs, "Job")
        .into_iter()
        .filter(|job| is_install_hook(job))
        .collect();
    if hooked.len() != 1 {
        return Ok(vec![format!(
            "{CHART}: renders {} pre-install,pre-upgrade Jobs, not 1",
            hooked.len()
        )]);
    }
    let pod = index(index(index(hooked[0], "spec")?, "template")?, "spec")?;
    let container = first(Some(index(pod, "containers")?), "containers")?;
    let mut argv = py_list(container.get("command"))?;
    argv.extend(py_list(container.get("args"))?);
    let mut found = Vec::new();
    let runs_harvest = argv
        .first()
        .and_then(Value::as_str)
        .is_some_and(|program| program.ends_with("harvest"));
    if !runs_harvest {
        found.push(format!("{CHART}: the migration Job does not run `harvest`"));
    }
    let migrate_run = [Value::Str("migrate".into()), Value::Str("run".into())];
    if argv.get(1..3) != Some(&migrate_run[..]) {
        found.push(format!(
            "{CHART}: the migration Job does not run `migrate run`"
        ));
    }
    let include = argv.windows(2).any(|pair| {
        pair[0].as_str() == Some("--include-dir") && pair[1].as_str() == Some(MIGRATIONS_DIR)
    });
    if !include {
        found.push(format!(
            "{CHART}: the migration Job does not pass `--include-dir {MIGRATIONS_DIR}`"
        ));
    }
    Ok(found)
}

/// Returns the `helm` path, or `None`. A bad `HELM` path also gives `None`.
fn helm_binary() -> Option<std::path::PathBuf> {
    let name = std::env::var("HELM")
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "helm".to_owned());
    path::which(name).ok()
}

/// Returns (exit code, stdout, stderr) of `helm template` on the chart.
fn render(helm: &Path, root: &Path, extra: &[&str]) -> Result<(i32, String, String)> {
    let output = sh::cmd(helm)
        .arg("template")
        .arg("contract")
        .arg(root.join(CHART))
        .args(extra)
        .output_raw()?;
    // Python read both streams in text mode, which translates each line end
    // to `\n`.
    let text = |bytes: &[u8]| pytext::universal_newlines(&String::from_utf8_lossy(bytes));
    Ok((
        output.status.code().unwrap_or(-1),
        text(&output.stdout),
        text(&output.stderr),
    ))
}

/// Returns the findings that need `helm template`.
fn check_render(helm: &Path, root: &Path) -> Result<Vec<String>> {
    let lint_values = root.join(LINT_VALUES);
    let lint_values = lint_values.to_string_lossy();
    let (code, out, err) = render(helm, root, &["-f", &lint_values])?;
    if code != 0 {
        return Ok(vec![format!(
            "{CHART}: `helm template` failed: {}",
            pytext::strip(&err)
        )]);
    }
    let docs: Vec<Value> = pyyaml::safe_load_all(&out)
        .map_err(|error| {
            Crash(format!(
                "{CHART}: the rendered chart: {}",
                error.render(&out)
            ))
        })?
        .into_iter()
        .filter(Value::truthy)
        .collect();
    let mut found = check_deployment(&docs)?;
    found.extend(check_migration_job(&docs)?);
    let (code, _, err) = render(helm, root, &[])?;
    if code == 0 || !err.contains("existingSecret") {
        found.push(format!("{CHART}: renders with no database.existingSecret"));
    }
    Ok(found)
}

// ── Runner ──────────────────────────────────────────────────────────────────

/// Ports `read`: the text of a repository file, or `None`.
fn read(root: &Path, rel: &str) -> Result<Option<String>> {
    pytext::read_text(&root.join(rel))
}

/// Ports `load`: the parsed YAML of a repository file, or `None`.
fn load(root: &Path, rel: &str) -> Result<Option<Value>> {
    let Some(text) = read(root, rel)? else {
        return Ok(None);
    };
    pyyaml::safe_load(&text)
        .map(Some)
        .map_err(|error| Crash(format!("{rel}: {}", error.render(&text))).into())
}

/// Runs every check under `root` with the `helm` at `helm`. Returns the
/// findings.
fn audit(root: &Path, helm: &Path) -> Result<Vec<String>> {
    let channel = toolchain_channel(read(root, TOOLCHAIN)?.as_deref());
    let mut findings = check_note(read(root, NOTE)?.as_deref());
    findings.extend(check_dockerfile(
        read(root, DOCKERFILE)?.as_deref(),
        channel.as_deref(),
    ));
    findings.extend(check_chart(
        load(root, &format!("{CHART}/Chart.yaml"))?.as_ref(),
        workspace_version(read(root, WORKSPACE)?.as_deref()).as_deref(),
    ));
    findings.extend(check_values(
        load(root, &format!("{CHART}/values.yaml"))?.as_ref(),
        load(root, RELEASE)?.as_ref(),
    ));
    if root.join(CHART).is_dir() {
        findings.extend(check_render(helm, root)?);
    }
    Ok(findings)
}

// ── Self-test ───────────────────────────────────────────────────────────────

fn pin() -> String {
    format!("@sha256:{}", "a".repeat(64))
}

fn good_dockerfile() -> String {
    let pin = pin();
    format!(
        "FROM rust:1.99.0-bookworm{pin} AS build\n\
         ENV RUSTUP_TOOLCHAIN=1.99.0\n\
         RUN cargo auditable build --release --locked \\\n    -p autumn-harvest-cli -p standalone-runner\n\
         FROM gcr.io/distroless/cc-debian12:nonroot{pin}\n\
         COPY --from=build /out/harvest /out/harvest-replay /out/standalone-runner /usr/local/bin/\n\
         COPY {MIGRATIONS_SRC} {MIGRATIONS_DIR}\n\
         USER {NONROOT}\n"
    )
}

fn good_note() -> String {
    let binaries: Vec<String> = BINARIES.iter().map(|b| format!("`{b}`")).collect();
    format!("{}\n{IMAGE} {}", SECTIONS.join("\n"), binaries.join(" "))
}

/// The options of one self-test Deployment.
struct Pod {
    live: &'static str,
    ready: &'static str,
    sleep: i64,
    grace: i64,
    read_only: bool,
    nonroot: bool,
}

impl Default for Pod {
    fn default() -> Self {
        Self {
            live: LIVE,
            ready: READY,
            sleep: 10,
            grace: 60,
            read_only: true,
            nonroot: true,
        }
    }
}

fn pod(options: &Pod) -> Value {
    serde_json::json!({
        "kind": "Deployment",
        "spec": {"template": {"spec": {
            "terminationGracePeriodSeconds": options.grace,
            "securityContext": {"runAsNonRoot": options.nonroot},
            "containers": [{
                "livenessProbe": {"httpGet": {"path": options.live}},
                "readinessProbe": {"httpGet": {"path": options.ready}},
                "lifecycle": {"preStop": {"sleep": {"seconds": options.sleep}}},
                "securityContext": {"readOnlyRootFilesystem": options.read_only},
            }],
        }}},
    })
    .into()
}

fn job(hook: &str, argv: Option<&[&str]>) -> Value {
    let default = [
        "/usr/local/bin/harvest",
        "migrate",
        "run",
        "--include-dir",
        MIGRATIONS_DIR,
    ];
    let argv = argv.unwrap_or(&default);
    serde_json::json!({
        "kind": "Job",
        "metadata": {"annotations": {"helm.sh/hook": hook}},
        "spec": {"template": {"spec": {"containers": [{"command": argv}]}}},
    })
    .into()
}

fn value(json: serde_json::Value) -> Value {
    json.into()
}

/// One self-test outcome: a finding list that wants a count, or a value that
/// wants an exact match.
enum Case {
    Count(Vec<String>, usize),
    Exact(Option<String>, Option<&'static str>),
}

impl Case {
    fn ok(&self) -> bool {
        match self {
            Self::Count(got, want) => got.len() == *want,
            Self::Exact(got, want) => got.as_deref() == *want,
        }
    }

    /// Renders the result as Python `repr` renders `got`.
    fn got(&self) -> String {
        match self {
            Self::Count(got, _) => {
                let items: Vec<String> = got.iter().map(|item| repr_str(item)).collect();
                format!("[{}]", items.join(", "))
            }
            Self::Exact(got, _) => got.as_deref().map_or_else(|| "None".to_owned(), repr_str),
        }
    }
}

#[allow(clippy::too_many_lines)]
fn self_test_cases() -> Result<Vec<Case>> {
    use Case::{Count, Exact};
    let release = value(serde_json::json!({"env": {"IMAGE": IMAGE}}));
    let df = good_dockerfile();
    let df = Some(df.as_str());
    let good = good_dockerfile();
    let note = good_note();
    let chart = value(serde_json::json!({"appVersion": "0.7.0", "kubeVersion": ">=1.30.0-0"}));
    let image = value(serde_json::json!({"image": {"repository": IMAGE}}));
    let user_line = format!("USER {NONROOT}");
    let extra_stage = format!(
        "{good}FROM build AS extra\nUSER 65532:65532\nCOPY /out/harvest /out/harvest-replay /out/standalone-runner {MIGRATIONS_SRC} {MIGRATIONS_DIR} /x/\n"
    );
    let harvest = "/usr/local/bin/harvest";
    Ok(vec![
        Count(check_note(Some(&note)), 0),
        Count(check_note(None), 1),
        Count(
            check_note(Some(&note.replace("## Decision", "## Choice"))),
            1,
        ),
        Count(check_note(Some(&note.replace("`harvest-replay`", ""))), 1),
        Count(check_note(Some(&note.replace(IMAGE, ""))), 1),
        Count(check_dockerfile(df, Some("1.99.0")), 0),
        Count(check_dockerfile(None, Some("1.99.0")), 1),
        // A toolchain bump without a builder bump: the tag and the env var.
        Count(check_dockerfile(df, Some("1.100.0")), 2),
        // A tag with no digest.
        Count(
            check_dockerfile(Some(&good.replacen(&pin(), "", 1)), Some("1.99.0")),
            1,
        ),
        Count(
            check_dockerfile(Some(&good.replace(&user_line, "USER root")), Some("1.99.0")),
            1,
        ),
        // Docker applies the last `USER`.
        Count(
            check_dockerfile(
                Some(&good.replace(&user_line, &format!("{user_line}\nUSER root"))),
                Some("1.99.0"),
            ),
            1,
        ),
        Count(
            check_dockerfile(Some(&good.replace(&user_line, "")), Some("1.99.0")),
            1,
        ),
        // The image tag moved, the env var did not.
        Count(
            check_dockerfile(
                Some(&good.replace("rust:1.99.0", "rust:1.100.0")),
                Some("1.100.0"),
            ),
            1,
        ),
        Count(
            check_dockerfile(
                Some(&good.replace("ENV RUSTUP_TOOLCHAIN=1.99.0\n", "")),
                Some("1.99.0"),
            ),
            1,
        ),
        Count(
            check_dockerfile(good.split("FROM gcr").next(), Some("1.99.0")),
            1,
        ),
        Count(
            check_dockerfile(
                Some(&good.replace(" /out/harvest-replay", "")),
                Some("1.99.0"),
            ),
            1,
        ),
        Count(
            check_dockerfile(Some(&good.replace("auditable ", "")), Some("1.99.0")),
            1,
        ),
        Count(
            check_dockerfile(
                Some(&good.replace(&format!("COPY {MIGRATIONS_SRC}"), "COPY x")),
                Some("1.99.0"),
            ),
            1,
        ),
        // A stage built from an earlier stage needs no digest.
        Count(check_dockerfile(Some(&extra_stage), Some("1.99.0")), 0),
        Count(check_chart(Some(&chart), Some("0.7.0")), 0),
        Count(check_chart(None, Some("0.7.0")), 1),
        Count(check_chart(Some(&chart), Some("0.8.0")), 1),
        Count(
            check_chart(
                Some(&value(
                    serde_json::json!({"appVersion": "0.7.0", "kubeVersion": ">=1.29.0-0"}),
                )),
                Some("0.7.0"),
            ),
            1,
        ),
        Count(
            check_chart(
                Some(&value(serde_json::json!({"appVersion": "0.7.0"}))),
                Some("0.7.0"),
            ),
            1,
        ),
        Count(check_chart(Some(&chart), None), 1),
        Count(check_values(Some(&image), Some(&release)), 0),
        Count(
            check_values(
                Some(&value(serde_json::json!({"image": {"repository": "x/y"}}))),
                Some(&release),
            ),
            1,
        ),
        Count(
            check_values(Some(&image), Some(&value(serde_json::json!({"env": {}})))),
            1,
        ),
        // A job-level comment or value does not count, only the workflow env.
        Count(
            check_values(
                Some(&image),
                Some(&value(
                    serde_json::json!({"jobs": {"x": {"env": {"IMAGE": IMAGE}}}}),
                )),
            ),
            1,
        ),
        Count(check_deployment(&[pod(&Pod::default())])?, 0),
        Count(check_deployment(&[])?, 1),
        Count(
            check_deployment(&[pod(&Pod {
                live: READY,
                ..Pod::default()
            })])?,
            1,
        ),
        Count(
            check_deployment(&[pod(&Pod {
                ready: "/api/harvest/health",
                ..Pod::default()
            })])?,
            1,
        ),
        Count(
            check_deployment(&[pod(&Pod {
                sleep: 0,
                ..Pod::default()
            })])?,
            1,
        ),
        Count(
            check_deployment(&[pod(&Pod {
                grace: 30,
                ..Pod::default()
            })])?,
            1,
        ),
        Count(
            check_deployment(&[pod(&Pod {
                read_only: false,
                ..Pod::default()
            })])?,
            1,
        ),
        Count(
            check_deployment(&[pod(&Pod {
                nonroot: false,
                ..Pod::default()
            })])?,
            1,
        ),
        Count(
            check_deployment(&[value(serde_json::json!(
                {"kind": "Deployment", "spec": {"template": {"spec": {"containers": []}}}}
            ))])?,
            1,
        ),
        Count(
            check_migration_job(&[job("pre-install,pre-upgrade", None)])?,
            0,
        ),
        Count(check_migration_job(&[])?, 1),
        Count(check_migration_job(&[job("post-install", None)])?, 1),
        Count(
            check_migration_job(&[job(
                "pre-install,pre-upgrade",
                Some(&[harvest, "migrate", "status"]),
            )])?,
            2,
        ),
        // Helm accepts a space after the comma.
        Count(
            check_migration_job(&[job("pre-install, pre-upgrade", None)])?,
            0,
        ),
        Count(
            check_migration_job(&[job(
                "pre-install,pre-upgrade",
                Some(&[
                    "/bin/migrate",
                    "migrate",
                    "run",
                    "--include-dir",
                    MIGRATIONS_DIR,
                ]),
            )])?,
            1,
        ),
        Count(
            check_migration_job(&[job(
                "pre-install,pre-upgrade",
                Some(&[harvest, "migrate", "run", "--other", MIGRATIONS_DIR]),
            )])?,
            1,
        ),
        Exact(
            workspace_version(Some(
                "[workspace]\n[workspace.package]\nkeywords = [\"a\"]\nversion = \"1.2.3\"\n",
            )),
            Some("1.2.3"),
        ),
        Exact(workspace_version(Some("[workspace]\n")), None),
        Exact(workspace_version(Some("not toml [")), None),
        Exact(
            toolchain_channel(Some("[toolchain]\nchannel = \"1.99.0\"\n")),
            Some("1.99.0"),
        ),
        Exact(toolchain_channel(None), None),
    ])
}

/// Runs the fixtures. Returns the output lines and the count of failed cases.
fn self_test() -> Result<(Vec<String>, usize)> {
    let cases = self_test_cases()?;
    let mut lines = Vec::new();
    let mut bad = 0;
    for (index, case) in cases.iter().enumerate() {
        if !case.ok() {
            bad += 1;
            lines.push(format!("self-test case {index}: got {}", case.got()));
        }
    }
    lines.push(format!(
        "worker-image-contract self-test: {} cases, {bad} failed",
        cases.len()
    ));
    Ok((lines, bad))
}

/// Runs `cargo xtask audit worker-image-contract`.
#[expect(
    clippy::needless_pass_by_value,
    reason = "every command takes its `Args` by value, so each dispatch arm has one shape"
)]
pub fn run(args: Args) -> Result<ExitCode> {
    let (lines, bad) = if args.self_test {
        self_test()?
    } else {
        let Some(helm) = helm_binary() else {
            eprintln!("worker-image-contract: helm is required (set HELM)");
            return Ok(ExitCode::from(2));
        };
        let findings = match audit(&util::repo_root(), &helm) {
            Ok(findings) => findings,
            Err(error) => {
                let Some(crash) = error.downcast_ref::<Crash>() else {
                    return Err(error);
                };
                eprintln!("worker-image-contract: {crash}");
                return Ok(ExitCode::FAILURE);
            }
        };
        let count = findings.len();
        let mut lines = findings;
        lines.push(format!("worker-image-contract: {count} findings"));
        (lines, count)
    };
    for line in lines {
        println!("{line}");
    }
    Ok(util::gate_exit(bad == 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_self_test_case_passes() {
        let (lines, bad) = self_test().unwrap_or_default();
        assert_eq!(bad, 0, "{lines:#?}");
        assert_eq!(
            lines,
            ["worker-image-contract self-test: 50 cases, 0 failed"]
        );
    }

    #[test]
    fn a_failed_case_renders_as_python_repr() {
        assert_eq!(Case::Exact(None, Some("x")).got(), "None");
        assert_eq!(Case::Exact(Some("1.0".into()), None).got(), "'1.0'");
        assert_eq!(Case::Count(vec!["a".into()], 0).got(), "['a']");
    }

    #[test]
    fn an_unquoted_app_version_compares_as_python_str() {
        // YAML 1.1 reads `1.0` as a float, and `str(1.0)` is `1.0`.
        let chart =
            pyyaml::safe_load("appVersion: 1.0\nkubeVersion: '>=1.30'\n").unwrap_or(Value::None);
        assert_eq!(check_chart(Some(&chart), Some("1.0")), Vec::<String>::new());
        let chart = pyyaml::safe_load("kubeVersion: '>=1.30'\n").unwrap_or(Value::None);
        assert_eq!(
            check_chart(Some(&chart), Some("1.0")),
            [
                "charts/autumn-harvest-worker/Chart.yaml: appVersion None is not the workspace version '1.0'"
            ]
        );
    }

    #[test]
    fn a_bool_counts_as_an_int_as_in_python() {
        let doc = pyyaml::safe_load(concat!(
            "kind: Deployment\nspec:\n  template:\n    spec:\n",
            "      terminationGracePeriodSeconds: true\n",
            "      containers:\n      - lifecycle: {preStop: {sleep: {seconds: yes}}}\n",
        ))
        .unwrap_or(Value::None);
        let found = check_deployment(&[doc]).unwrap_or_default();
        assert!(
            found.contains(&"charts/autumn-harvest-worker: terminationGracePeriodSeconds True is below 46 (preStop True + drain 35 + margin 10)".to_owned()),
            "{found:#?}"
        );
    }

    #[test]
    fn the_repository_passes_every_check_that_needs_no_helm() {
        let root = util::repo_root();
        let read = |rel: &str| read(&root, rel).ok().flatten();
        let channel = toolchain_channel(read(TOOLCHAIN).as_deref());
        assert_eq!(check_note(read(NOTE).as_deref()), Vec::<String>::new());
        assert_eq!(
            check_dockerfile(read(DOCKERFILE).as_deref(), channel.as_deref()),
            Vec::<String>::new()
        );
        let chart = load(&root, &format!("{CHART}/Chart.yaml")).ok().flatten();
        let version = workspace_version(read(WORKSPACE).as_deref());
        assert_eq!(
            check_chart(chart.as_ref(), version.as_deref()),
            Vec::<String>::new()
        );
        let values = load(&root, &format!("{CHART}/values.yaml")).ok().flatten();
        let release = load(&root, RELEASE).ok().flatten();
        assert_eq!(
            check_values(values.as_ref(), release.as_ref()),
            Vec::<String>::new()
        );
    }

    /// A stub `helm` renders a canned chart with `-f`, and fails without it.
    #[cfg(unix)]
    #[test]
    fn check_render_reads_the_helm_output() {
        use std::os::unix::fs::PermissionsExt;
        let dir = fs::temp_dir().unwrap_or_else(|error| panic!("{error:#}"));
        let rendered = dir.path().join("rendered.yaml");
        let manifest = format!(
            "---\n# Source: x\nkind: Deployment\nspec:\n  template:\n    spec:\n      terminationGracePeriodSeconds: 60\n      securityContext: {{runAsNonRoot: true}}\n      containers:\n      - livenessProbe: {{httpGet: {{path: {LIVE}}}}}\n        readinessProbe: {{httpGet: {{path: {READY}}}}}\n        lifecycle: {{preStop: {{sleep: {{seconds: 10}}}}}}\n        securityContext: {{readOnlyRootFilesystem: true}}\n---\n---\nkind: Job\nmetadata:\n  annotations: {{helm.sh/hook: 'pre-install,pre-upgrade'}}\nspec:\n  template:\n    spec:\n      containers:\n      - command: [/usr/local/bin/harvest, migrate, run]\n        args: [--include-dir, {MIGRATIONS_DIR}]\n"
        );
        std::fs::write(&rendered, manifest).unwrap_or_else(|error| panic!("{error}"));
        let helm = dir.path().join("helm");
        let script = format!(
            "#!/bin/sh\nif [ \"$4\" = \"-f\" ]; then cat '{}'; exit 0; fi\necho 'database.existingSecret is required' >&2\nexit 1\n",
            rendered.display()
        );
        std::fs::write(&helm, script).unwrap_or_else(|error| panic!("{error}"));
        std::fs::set_permissions(&helm, std::fs::Permissions::from_mode(0o755))
            .unwrap_or_else(|error| panic!("{error}"));
        let found =
            check_render(&helm, &util::repo_root()).unwrap_or_else(|error| panic!("{error:#}"));
        assert_eq!(found, Vec::<String>::new());
    }
}
