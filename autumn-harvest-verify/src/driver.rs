//! Cargo driver: emits MIR for the requested targets into an owned target dir and
//! returns the `.mir` files (located from cargo's JSON artifact messages).
//!
//! The whole substrate decision of D1 lives here. `rustc --emit=mir` is stable,
//! but *finding* the file it wrote is not: the name carries cargo's metadata
//! hash, so the only reliable route is cargo's own `--message-format=json`
//! `compiler-artifact` messages, whose `filenames` name the rlib/binary the
//! `.mir` sits beside.
//!
//! Three invariants the driver enforces rather than assumes:
//!
//! * **opt-level 0.** At `-C opt-level=1` and above, rustc inlines the very
//!   helper calls the interprocedural analysis exists to follow, so a laundered
//!   source disappears into its caller and the tool would report
//!   `proven-deterministic` for a workflow that is not. An optimized profile is
//!   refused ([`crate::Error::Cargo`]), never silently accepted. The guard reads
//!   `CARGO_ENCODED_RUSTFLAGS` with its `\x1f` separator and the plain-text
//!   flag variables on whitespace, so a multi-flag `-O` cannot slip past it.
//! * **this run's artifacts only.** A `compiler-artifact` message is accepted
//!   only when its `package_id` is one of the ids `cargo metadata` resolved for
//!   the packages this invocation asked for *and* its `target.kind` is the kind
//!   requested. Cargo reports every unit in the dependency graph, so without the
//!   package filter a `lib` request would accept any dependency's artifact —
//!   and, through it, a `.mir` this invocation never emitted.
//! * **no stale MIR, and no guessed MIR.** The `.mir` path is derived from the
//!   artifact filename that carries cargo's metadata hash
//!   (`deps/libNAME-HASH.rmeta` → `deps/NAME-HASH.mir`), never by scanning a
//!   directory for a `<crate>-*.mir` and taking the newest: two toolchains, a
//!   restored cache or a `cp -a` all defeat mtime, and the file picked would be
//!   analyzed as if it described the current source. When the derived file is
//!   absent — a `fresh` unit whose target dir predates `--emit=mir` — the unit's
//!   fingerprint is deleted and the build re-run exactly once; a second miss is
//!   an error naming the target, not a silent gap in coverage.
//!
//! A relative `--target-dir` resolves against the **workspace root**, not the
//! process's working directory, so the same command run from a subdirectory
//! analyzes the same tree. (Cargo itself resolves it against the cwd; the
//! asymmetry with the *default* — `<workspace root>/target/harvest-verify` —
//! is worse than the asymmetry with cargo, because it silently splits one
//! logical emit directory into several.)

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

use crate::Error;

/// What to build.
#[derive(Debug, Clone, Default)]
pub struct BuildRequest {
    pub manifest_path: Option<PathBuf>,
    pub packages: Vec<String>,
    pub lib: bool,
    pub examples: Vec<String>,
    pub all_examples: bool,
    pub bins: Vec<String>,
    /// Integration-test targets (`--test NAME`): `tests/NAME.rs`.
    pub tests: Vec<String>,
    pub features: Vec<String>,
    pub no_default_features: bool,
    pub target_dir: Option<PathBuf>,
}

impl BuildRequest {
    /// True when the request selects no target at all (nothing to build).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        !self.lib
            && !self.all_examples
            && self.examples.is_empty()
            && self.bins.is_empty()
            && self.tests.is_empty()
            && self.packages.is_empty()
    }
}

/// One emitted MIR file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedMir {
    pub crate_name: String,
    pub target_kind: String,
    pub path: PathBuf,
}

/// Which target of a package to compile. `cargo rustc` takes exactly one target
/// per invocation, so a multi-target request becomes a loop.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TargetSel {
    Lib,
    Example(String),
    Bin(String),
    Test(String),
}

impl TargetSel {
    fn cargo_flags(&self) -> Vec<String> {
        match self {
            Self::Lib => vec!["--lib".to_string()],
            Self::Example(name) => vec!["--example".to_string(), name.clone()],
            Self::Bin(name) => vec!["--bin".to_string(), name.clone()],
            Self::Test(name) => vec!["--test".to_string(), name.clone()],
        }
    }

    const fn kind(&self) -> &'static str {
        match self {
            Self::Lib => "lib",
            Self::Example(_) => "example",
            Self::Bin(_) => "bin",
            Self::Test(_) => "test",
        }
    }

    fn name(&self) -> Option<&str> {
        match self {
            Self::Lib => None,
            Self::Example(name) | Self::Bin(name) | Self::Test(name) => Some(name),
        }
    }

    /// How the target reads in an error message.
    fn describe(&self) -> String {
        self.name().map_or_else(
            || "lib".to_string(),
            |name| format!("{} {name}", self.kind()),
        )
    }
}

/// Emit MIR. Refuses optimized builds (`-C opt-level` ≠ 0 would inline helper calls away).
///
/// # Errors
/// When cargo fails, when the profile is optimized, or when no `.mir` file is produced.
pub fn emit_mir(req: &BuildRequest) -> crate::Result<Vec<EmittedMir>> {
    emit_mir_with_warnings(req).map(|(mirs, _)| mirs)
}

/// [`emit_mir`] plus the non-fatal notes worth surfacing in the report header
/// (skipped examples, targets that produced no MIR).
///
/// # Errors
/// See [`emit_mir`].
pub fn emit_mir_with_warnings(req: &BuildRequest) -> crate::Result<(Vec<EmittedMir>, Vec<String>)> {
    refuse_optimized_profile()?;
    let mut warnings: Vec<String> = Vec::new();
    let root = workspace_root(req.manifest_path.as_deref())?;
    // A relative `--target-dir` is resolved against the workspace root, not the
    // cwd: the default lives there, and a run from a subdirectory that quietly
    // emitted into a *second* directory would re-pay the whole build and could
    // mix two feature sets in one tree. Documented on `Cli::target_dir`.
    let target_dir = match &req.target_dir {
        Some(dir) if dir.is_absolute() => dir.clone(),
        Some(dir) => root.join(dir),
        None => root.join("target").join("harvest-verify"),
    };

    // One `cargo metadata` for the whole run: it resolves the package ids that
    // gate artifact acceptance, and `--all-examples` reads the same document.
    let metadata = workspace_metadata(req)?;

    let packages: Vec<Option<String>> = if req.packages.is_empty() {
        vec![None]
    } else {
        req.packages.iter().map(|p| Some(p.clone())).collect()
    };

    let mut emitted: Vec<EmittedMir> = Vec::new();
    for package in &packages {
        let accept = package_ids(&metadata, package.as_deref())?;
        let selections = targets_for(req, package.as_deref(), &metadata, &mut warnings);
        for selection in selections {
            let mut mirs =
                run_cargo_rustc(req, package.as_deref(), &selection, &target_dir, &accept)?;
            emitted.append(&mut mirs);
        }
    }
    Ok((emitted, warnings))
}

/// `cargo metadata --no-deps` for the requested workspace, as raw JSON.
fn workspace_metadata(req: &BuildRequest) -> crate::Result<serde_json::Value> {
    let mut cmd = Command::new(cargo_bin());
    cmd.arg("metadata")
        .arg("--no-deps")
        .arg("--format-version")
        .arg("1");
    if let Some(path) = &req.manifest_path {
        cmd.arg("--manifest-path").arg(path);
    }
    let out = cmd
        .output()
        .map_err(|e| Error::Cargo(format!("cannot run `cargo metadata`: {e}")))?;
    if !out.status.success() {
        return Err(Error::Cargo(format!(
            "`cargo metadata` failed:\n{}",
            stderr_tail(&out.stderr)
        )));
    }
    serde_json::from_slice(&out.stdout)
        .map_err(|e| Error::Cargo(format!("`cargo metadata` produced unreadable JSON: {e}")))
}

/// The `package_id`s a `compiler-artifact` message may carry to be accepted as
/// this invocation's output.
///
/// `None` means the request named no package, so cargo picks the workspace
/// default; every workspace member is then a legitimate producer. Dependencies
/// never are — and cargo reports one `compiler-artifact` per unit in the graph,
/// so this is the filter that keeps a dependency's `.mir` out of the analysis.
fn package_ids(
    metadata: &serde_json::Value,
    package: Option<&str>,
) -> crate::Result<std::collections::BTreeSet<String>> {
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut ids = std::collections::BTreeSet::new();
    for pkg in packages {
        let name = pkg.get("name").and_then(serde_json::Value::as_str);
        if package.is_some() && name != package {
            continue;
        }
        if let Some(id) = pkg.get("id").and_then(serde_json::Value::as_str) {
            ids.insert(id.to_string());
        }
    }
    if ids.is_empty() {
        return Err(Error::Cargo(package.map_or_else(
            || "`cargo metadata` reported no workspace packages".to_string(),
            |name| format!("`cargo metadata` knows no workspace package named `{name}`"),
        )));
    }
    Ok(ids)
}

/// The workspace root for `manifest_path` (via `cargo locate-project --workspace`).
///
/// # Errors
/// When cargo cannot locate the workspace.
pub fn workspace_root(manifest_path: Option<&Path>) -> crate::Result<PathBuf> {
    let mut cmd = Command::new(cargo_bin());
    cmd.arg("locate-project")
        .arg("--workspace")
        .arg("--message-format")
        .arg("plain");
    if let Some(path) = manifest_path {
        cmd.arg("--manifest-path").arg(path);
    }
    let out = cmd
        .output()
        .map_err(|e| Error::Cargo(format!("cannot run `cargo locate-project`: {e}")))?;
    if !out.status.success() {
        return Err(Error::Cargo(format!(
            "`cargo locate-project --workspace` failed:\n{}",
            stderr_tail(&out.stderr)
        )));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let manifest = Path::new(text.trim());
    manifest.parent().map(Path::to_path_buf).ok_or_else(|| {
        Error::Cargo(format!(
            "`cargo locate-project` returned an unusable path: {}",
            text.trim()
        ))
    })
}

/// `rustc -V`, or `"unknown"` when rustc cannot be run.
///
/// The report prints it because the MIR text format is not a stable API: the
/// parser is validated on one toolchain and a different one is a warning, not a
/// refusal (D1).
#[must_use]
pub fn rustc_version() -> String {
    Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
        .arg("-V")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map_or_else(
            || "unknown".to_string(),
            |out| String::from_utf8_lossy(&out.stdout).trim().to_string(),
        )
}

/// Collect pre-emitted `.mir` inputs (`--mir`), each a file or a directory.
///
/// The crate name is recovered from the file stem by dropping a trailing
/// `-<hash>` — rustc names its output `<crate>-<metadata hash>.mir`, and the
/// parser and the analysis key workflow paths on that crate name. A stem with
/// no such suffix (a hand-trimmed fixture like `format_and_outparams.mir`) is
/// used whole. Dashes become underscores either way, because that is how the
/// crate name appears inside the MIR itself.
#[must_use]
pub fn collect_mir_paths(paths: &[PathBuf]) -> Vec<EmittedMir> {
    let mut out: Vec<EmittedMir> = Vec::new();
    for path in paths {
        if path.is_dir() {
            collect_dir(path, &mut out);
        } else {
            push_mir(path, &mut out);
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out.dedup_by(|a, b| a.path == b.path);
    out
}

/// Walk `dir` for `.mir` files, **without following symlinks**.
///
/// `symlink_metadata` rather than `is_dir`/`is_file`: a linked directory turns
/// the walk into someone else's tree, and a linked file makes the report name a
/// path whose content lives somewhere the caller never pointed at. Only regular
/// files are accepted, so a fifo or device node named `x.mir` cannot block the
/// run either.
fn collect_dir(dir: &Path, out: &mut Vec<EmittedMir>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries.filter_map(Result::ok).map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_dir() {
            collect_dir(&path, out);
        } else if meta.is_file() && path.extension().is_some_and(|e| e == "mir") {
            push_mir(&path, out);
        }
    }
}

fn push_mir(path: &Path, out: &mut Vec<EmittedMir>) {
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    out.push(EmittedMir {
        crate_name: crate_name_from_stem(&stem),
        target_kind: "mir".to_string(),
        path: path.to_path_buf(),
    });
}

/// `wf_corpus-3f2a91c0d4e5b678` → `wf_corpus`; `spike` → `spike`.
fn crate_name_from_stem(stem: &str) -> String {
    let base = match stem.rsplit_once('-') {
        Some((head, hash))
            if !head.is_empty()
                && hash.len() >= 8
                && hash.chars().all(|c| c.is_ascii_alphanumeric()) =>
        {
            head
        }
        _ => stem,
    };
    base.replace('-', "_")
}

/// Split a flag variable into tokens the way cargo itself reads it.
///
/// `CARGO_ENCODED_RUSTFLAGS` is **`\x1f`-separated**, precisely so a flag may
/// contain spaces; splitting it on whitespace (as this used to) reads
/// `--cfg\x1fx\x1f-O` as one token that equals none of the patterns, and an
/// optimized build then produces MIR with the helper calls inlined away — i.e.
/// a false `proven-deterministic`, the one outcome the tool must never print.
fn flag_tokens(var: &str, raw: &str) -> Vec<String> {
    if var == "CARGO_ENCODED_RUSTFLAGS" {
        raw.split('\x1f')
            .filter(|t| !t.is_empty())
            .map(str::to_string)
            .collect()
    } else {
        raw.split_whitespace().map(str::to_string).collect()
    }
}

/// The first token in `tokens` that asks for optimization, if any.
///
/// Recognises `-O`, `-Copt-level=N` / `-C opt-level=N` (both spellings, and the
/// two-token form cargo passes through verbatim) for any `N` other than `0`,
/// and a release profile requested inline (`--release`, `--profile release`).
fn optimization_request(tokens: &[String]) -> Option<String> {
    let mut index = 0;
    while index < tokens.len() {
        let token = tokens[index].as_str();
        let next = tokens.get(index + 1).map(String::as_str);
        index += 1;

        if token == "-O" || token == "--release" {
            return Some(token.to_string());
        }
        if token == "--profile" && next == Some("release") {
            return Some("--profile release".to_string());
        }
        // `-C opt-level=2`, `-Copt-level=2`, `--codegen opt-level=2`.
        let setting = if token == "-C" || token == "--codegen" {
            next.map(|value| (value.to_string(), format!("{token} {value}")))
        } else {
            token
                .strip_prefix("-C")
                .or_else(|| token.strip_prefix("--codegen="))
                .map(|rest| (rest.to_string(), token.to_string()))
        };
        let Some((setting, printed)) = setting else {
            continue;
        };
        let Some(level) = setting.trim().strip_prefix("opt-level") else {
            continue;
        };
        let level = level.trim_start().strip_prefix('=').unwrap_or(level).trim();
        // `-C opt-level` with no value is not a setting; anything but 0 is.
        if !level.is_empty() && level.trim_matches('"') != "0" {
            return Some(printed);
        }
    }
    None
}

/// Refuse a profile that would inline the calls the analysis follows.
fn refuse_optimized_profile() -> crate::Result<()> {
    if let Some(level) = std::env::var_os("CARGO_PROFILE_DEV_OPT_LEVEL") {
        let level = level.to_string_lossy().trim().to_string();
        if level.trim_matches('"') != "0" {
            return Err(Error::Cargo(format!(
                "CARGO_PROFILE_DEV_OPT_LEVEL={level}: harvest-verify needs opt-level 0, \
                 because inlining erases the helper calls its traces are made of"
            )));
        }
    }
    for var in [
        "RUSTFLAGS",
        "CARGO_BUILD_RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
    ] {
        let Some(flags) = std::env::var_os(var) else {
            continue;
        };
        let flags = flags.to_string_lossy().into_owned();
        if let Some(offender) = optimization_request(&flag_tokens(var, &flags)) {
            return Err(Error::Cargo(format!(
                "{var} requests optimization ({offender}); harvest-verify needs opt-level 0, \
                 because inlining erases the helper calls its traces are made of"
            )));
        }
    }
    if std::env::var_os("CARGO_PROFILE").is_some_and(|p| p.to_string_lossy() == "release") {
        return Err(Error::Cargo(
            "a release profile is requested; harvest-verify needs opt-level 0".to_string(),
        ));
    }
    Ok(())
}

/// Which targets of one package to compile.
fn targets_for(
    req: &BuildRequest,
    package: Option<&str>,
    metadata: &serde_json::Value,
    warnings: &mut Vec<String>,
) -> Vec<TargetSel> {
    let mut out: Vec<TargetSel> = Vec::new();
    if req.lib {
        out.push(TargetSel::Lib);
    }
    for name in &req.examples {
        out.push(TargetSel::Example(name.clone()));
    }
    for name in &req.bins {
        out.push(TargetSel::Bin(name.clone()));
    }
    for name in &req.tests {
        out.push(TargetSel::Test(name.clone()));
    }
    if req.all_examples {
        for name in enumerate_examples(req, package, metadata, warnings) {
            let selection = TargetSel::Example(name);
            if !out.contains(&selection) {
                out.push(selection);
            }
        }
    }
    if out.is_empty() {
        out.push(TargetSel::Lib);
    }
    out
}

/// Examples of `package`, skipping those whose `required-features` are not enabled.
fn enumerate_examples(
    req: &BuildRequest,
    package: Option<&str>,
    metadata: &serde_json::Value,
    warnings: &mut Vec<String>,
) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    for pkg in packages {
        let pkg_name = pkg.get("name").and_then(serde_json::Value::as_str);
        if package.is_some() && pkg_name != package {
            continue;
        }
        let enabled = enabled_features(req, pkg);
        let targets = pkg
            .get("targets")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for target in targets {
            let is_example = target
                .get("kind")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|kinds| kinds.iter().any(|k| k.as_str() == Some("example")));
            if !is_example {
                continue;
            }
            let Some(name) = target.get("name").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let required: Vec<&str> = target
                .get("required-features")
                .and_then(serde_json::Value::as_array)
                .map(|f| f.iter().filter_map(serde_json::Value::as_str).collect())
                .unwrap_or_default();
            let missing: Vec<&str> = required
                .iter()
                .copied()
                .filter(|f| !enabled.contains(*f))
                .collect();
            if missing.is_empty() {
                names.push(name.to_string());
            } else {
                warnings.push(format!(
                    "skipping example {name}: required feature(s) not enabled: {}",
                    missing.join(", ")
                ));
            }
        }
    }
    names.sort();
    names.dedup();
    names
}

/// The feature set the request enables for `pkg`, closed over feature-to-feature edges.
fn enabled_features(
    req: &BuildRequest,
    pkg: &serde_json::Value,
) -> std::collections::BTreeSet<String> {
    let table = pkg.get("features").and_then(serde_json::Value::as_object);
    let mut wanted: Vec<String> = Vec::new();
    for group in &req.features {
        for name in group.split([',', ' ']) {
            if !name.trim().is_empty() {
                wanted.push(name.trim().to_string());
            }
        }
    }
    if !req.no_default_features {
        wanted.push("default".to_string());
    }

    let mut enabled: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    while let Some(feature) = wanted.pop() {
        if !enabled.insert(feature.clone()) {
            continue;
        }
        let Some(children) = table.and_then(|t| t.get(&feature)) else {
            continue;
        };
        let Some(children) = children.as_array() else {
            continue;
        };
        for child in children.iter().filter_map(serde_json::Value::as_str) {
            // `dep:foo` / `foo/bar` edges do not enable a feature of this package.
            if !child.contains(':') && !child.contains('/') {
                wanted.push(child.to_string());
            }
        }
    }
    enabled
}

/// One `cargo rustc` invocation for one target.
///
/// Runs the build, accepts only this run's artifacts, and derives the `.mir`
/// from each accepted artifact's hashed filename. A target that produced no
/// `.mir` gets exactly one retry with its fingerprint deleted (the `fresh` unit
/// whose target dir predates `--emit=mir`); a second miss is an error.
fn run_cargo_rustc(
    req: &BuildRequest,
    package: Option<&str>,
    selection: &TargetSel,
    target_dir: &Path,
    accept: &std::collections::BTreeSet<String>,
) -> crate::Result<Vec<EmittedMir>> {
    let (stdout, started) = invoke_cargo_rustc(req, package, selection, target_dir)?;
    let artifacts = accepted_artifacts(&stdout, selection, accept);
    let mirs = mirs_of(&artifacts, selection, started)?;
    if !mirs.is_empty() {
        return Ok(mirs);
    }

    // Nothing on disk. Either cargo built nothing for this target (no accepted
    // artifact at all), or it reported the unit `fresh` from a target dir that
    // was populated without `--emit=mir`. Both are fixed the same way: drop the
    // fingerprints so cargo must recompile, and run once more.
    purge_fingerprints(&artifacts, target_dir, package, selection);
    let (stdout, started) = invoke_cargo_rustc(req, package, selection, target_dir)?;
    let artifacts = accepted_artifacts(&stdout, selection, accept);
    let mirs = mirs_of(&artifacts, selection, started)?;
    if mirs.is_empty() {
        return Err(Error::Cargo(format!(
            "no .mir was emitted for {}{} — cargo reported {} artifact(s) for it and \
             none carried a `deps/<name>-<hash>` filename whose `.mir` exists, even \
             after the fingerprint was cleared. Delete {} and retry.",
            selection.describe(),
            package
                .map(|p| format!(" of package {p}"))
                .unwrap_or_default(),
            artifacts.len(),
            target_dir.display()
        )));
    }
    Ok(mirs)
}

/// One artifact message this invocation is willing to own.
struct Artifact {
    crate_name: String,
    fresh: bool,
    filenames: Vec<PathBuf>,
}

/// Run `cargo rustc`, returning its stdout and the instant the build started.
fn invoke_cargo_rustc(
    req: &BuildRequest,
    package: Option<&str>,
    selection: &TargetSel,
    target_dir: &Path,
) -> crate::Result<(String, SystemTime)> {
    let started = SystemTime::now();
    let mut cmd = Command::new(cargo_bin());
    cmd.arg("rustc");
    if let Some(path) = &req.manifest_path {
        cmd.arg("--manifest-path").arg(path);
    }
    if let Some(name) = package {
        cmd.arg("-p").arg(name);
    }
    for flag in selection.cargo_flags() {
        cmd.arg(flag);
    }
    if !req.features.is_empty() {
        cmd.arg("--features").arg(req.features.join(","));
    }
    if req.no_default_features {
        cmd.arg("--no-default-features");
    }
    cmd.arg("--target-dir").arg(target_dir);
    cmd.arg("--message-format=json");
    cmd.arg("--").arg("--emit=mir").arg("-C").arg("opt-level=0");

    let out = cmd.output().map_err(|e| {
        Error::Cargo(format!(
            "cannot run `cargo rustc` for {}: {e}",
            selection.describe()
        ))
    })?;
    if !out.status.success() {
        return Err(Error::Cargo(format!(
            "`cargo rustc` failed for {} (exit {}):\n{}",
            selection.describe(),
            out.status.code().unwrap_or(-1),
            stderr_tail(&out.stderr)
        )));
    }
    Ok((String::from_utf8_lossy(&out.stdout).into_owned(), started))
}

/// The `compiler-artifact` messages that belong to this invocation.
///
/// Both filters are load-bearing. `target.kind` alone is not enough: cargo
/// reports a `compiler-artifact` for **every** unit in the dependency graph, so
/// a `--lib` request would accept every dependency's lib — and `TargetSel::Lib`
/// has no target name to fall back on, so nothing else would exclude them.
fn accepted_artifacts(
    stdout: &str,
    selection: &TargetSel,
    accept: &std::collections::BTreeSet<String>,
) -> Vec<Artifact> {
    let mut out = Vec::new();
    for line in stdout.lines() {
        let Ok(message) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if message.get("reason").and_then(serde_json::Value::as_str) != Some("compiler-artifact") {
            continue;
        }
        let package_id = message
            .get("package_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if !accept.contains(package_id) {
            continue;
        }
        let target = message.get("target");
        let name = target
            .and_then(|t| t.get("name"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let kinds: Vec<&str> = target
            .and_then(|t| t.get("kind"))
            .and_then(serde_json::Value::as_array)
            .map(|k| k.iter().filter_map(serde_json::Value::as_str).collect())
            .unwrap_or_default();
        if !kinds.contains(&selection.kind()) {
            continue;
        }
        // Cargo reports the *target* name (dashes preserved for a bin/example/
        // test, underscored for a lib); the request names it the same way.
        if selection.name().is_some_and(|wanted| {
            name != wanted && name.replace('-', "_") != wanted.replace('-', "_")
        }) {
            continue;
        }
        out.push(Artifact {
            crate_name: name.replace('-', "_"),
            fresh: message
                .get("fresh")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            filenames: message
                .get("filenames")
                .and_then(serde_json::Value::as_array)
                .map(|f| {
                    f.iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(PathBuf::from)
                        .collect()
                })
                .unwrap_or_default(),
        });
    }
    out
}

/// The `.mir` files of `artifacts`, in artifact order, deduplicated.
fn mirs_of(
    artifacts: &[Artifact],
    selection: &TargetSel,
    started: SystemTime,
) -> crate::Result<Vec<EmittedMir>> {
    let mut emitted: Vec<EmittedMir> = Vec::new();
    for artifact in artifacts {
        let Some(path) = mir_for_artifact(&artifact.filenames)? else {
            continue;
        };
        check_freshness(&path, started, artifact.fresh)?;
        let mir = EmittedMir {
            crate_name: artifact.crate_name.clone(),
            target_kind: selection.kind().to_string(),
            path,
        };
        if !emitted.contains(&mir) {
            emitted.push(mir);
        }
    }
    Ok(emitted)
}

/// Derive the `.mir` path from an artifact's `filenames`, exactly.
///
/// rustc writes `<out-dir>/<crate>-<metadata hash>.mir`, so the whole problem is
/// learning that hash without guessing. Three rules, tried in order, and every
/// one of them is an *identity*, never a heuristic:
///
/// 1. **A filename that carries the hash.** `deps/libNAME-HASH.rlib` and
///    `deps/libNAME-HASH.rmeta` → `deps/NAME-HASH.mir`; `deps/NAME-HASH` (bins
///    and `--test` binaries) and `examples/NAME-HASH[.exe|.d]` → the same name
///    with a `.mir` extension. The `.rmeta` matters: a `fresh` lib reports the
///    *uplifted* `debug/libX.rlib`, whose name has no hash at all, beside the
///    hashed `deps/libX-HASH.rmeta`.
/// 2. **The uplifted copy, resolved by file identity.** For an example or a bin
///    cargo reports *only* `examples/NAME` — no hash anywhere in the message —
///    and uplifts it by hard-linking `examples/NAME-HASH`. So the hashed sibling
///    is found by asking the filesystem which file in that directory *is* the
///    same file (same device and inode), which is exactly the relation "cargo
///    uplifted this from that unit's output".
/// 3. **A single unambiguous candidate.** Only reachable where the uplift was a
///    copy rather than a hard link. Exactly one `<name>-<hash>.mir` in the
///    directory is accepted; **two or more is an error**, not a choice.
///
/// What is deliberately absent is the previous mtime-ordered scan for
/// `<crate>-*.mir`. After a toolchain switch, a restored build cache, a `cp -a`
/// or a second feature-set build, `deps/` holds several and the newest is not
/// necessarily this run's — and the verdict computed from the wrong one looks
/// exactly like a real verdict.
fn mir_for_artifact(filenames: &[PathBuf]) -> crate::Result<Option<PathBuf>> {
    // 1. A filename that carries the hash.
    for path in filenames {
        if let Some(candidate) = mir_beside(path)
            && candidate.is_file()
        {
            return Ok(Some(candidate));
        }
    }
    // 2. An uplifted filename, resolved to its hashed original by identity.
    for path in filenames {
        if mir_beside(path).is_some() {
            continue; // already hashed; rule 1 handled it
        }
        let Some(sibling) = hashed_original(path) else {
            continue;
        };
        if let Some(candidate) = mir_beside(&sibling)
            && candidate.is_file()
        {
            return Ok(Some(candidate));
        }
    }
    // 3. Exactly one candidate, or an error naming the ambiguity.
    for path in filenames {
        if mir_beside(path).is_some() {
            continue; // a hashed filename is never resolved by counting
        }
        let candidates = hashed_mir_candidates(path);
        match candidates.len() {
            0 => {}
            1 => return Ok(candidates.into_iter().next()),
            _ => {
                return Err(Error::Cargo(format!(
                    "{} .mir files could be {}'s: {}. Cargo reported no hashed \
                     filename for this target and the uplifted copy is not a hard \
                     link to any of them, so there is no way to tell which one this \
                     build wrote — delete the target dir and retry rather than \
                     analyzing a guess.",
                    candidates.len(),
                    path.display(),
                    candidates
                        .iter()
                        .map(|c| c.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }
    }
    Ok(None)
}

/// The hashed file `path` was uplifted from: the sibling that *is* the same
/// file on disk.
fn hashed_original(path: &Path) -> Option<PathBuf> {
    let dir = path.parent()?;
    let name = path.file_name()?.to_str()?;
    for entry in std::fs::read_dir(dir).ok()?.filter_map(Result::ok) {
        let candidate = entry.path();
        if candidate.file_name().and_then(|n| n.to_str()) == Some(name) {
            continue;
        }
        if mir_beside(&candidate).is_none() {
            continue;
        }
        if same_file(path, &candidate) {
            return Some(candidate);
        }
    }
    None
}

/// True when `a` and `b` are the same file on disk (hard links to one inode).
///
/// On platforms without inode identity this is always false, and the caller
/// falls through to the unique-candidate rule.
fn same_file(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        match (std::fs::metadata(a), std::fs::metadata(b)) {
            (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
            _ => false,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        false
    }
}

/// Every `<name>-<hash>.mir` beside `path` whose `<name>` is `path`'s own,
/// sorted. Used only by rule 3, and only its *count* is trusted.
fn hashed_mir_candidates(path: &Path) -> Vec<PathBuf> {
    let Some(dir) = path.parent() else {
        return Vec::new();
    };
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return Vec::new();
    };
    let base = name.split('.').next().unwrap_or(name);
    let base = base.strip_prefix("lib").unwrap_or(base);
    if base.is_empty() {
        return Vec::new();
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "mir"))
        .filter(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|stem| stem.strip_prefix(base))
                .and_then(|rest| rest.strip_prefix('-'))
                .is_some_and(|hash| hash.len() >= 8 && hash.chars().all(|c| c.is_ascii_hexdigit()))
        })
        .collect();
    out.sort();
    out
}

/// The `.mir` name the artifact filename `path` implies, if it implies one.
fn mir_beside(path: &Path) -> Option<PathBuf> {
    let dir = path.parent()?;
    let full = path.file_name()?.to_str()?;
    let stem = match path.extension().and_then(|e| e.to_str()) {
        // A library: drop the extension *and* the `lib` prefix rustc gives its
        // own outputs. The `.mir` beside it has neither.
        Some(ext @ ("rlib" | "rmeta" | "so" | "dylib" | "a")) => {
            let bare = full.strip_suffix(ext)?.strip_suffix('.')?;
            bare.strip_prefix("lib").unwrap_or(bare)
        }
        // A binary, example or test binary, and the dep-info file beside it.
        Some(ext @ ("exe" | "d")) => full.strip_suffix(ext)?.strip_suffix('.')?,
        // Anything else (`.mir` itself, `.pdb`, …) is not a witness.
        Some(_) => return None,
        None => full,
    };
    has_metadata_hash(stem).then(|| dir.join(format!("{stem}.mir")))
}

/// True when `stem` ends in `-<hash>`, cargo's metadata hash (16 hex digits
/// today; the length is not a stable promise, so ≥ 8 hex digits is the test).
fn has_metadata_hash(stem: &str) -> bool {
    stem.rsplit_once('-').is_some_and(|(head, hash)| {
        !head.is_empty() && hash.len() >= 8 && hash.chars().all(|c| c.is_ascii_hexdigit())
    })
}

/// Delete the fingerprints of the unit that failed to produce a `.mir`, so the
/// retry recompiles it instead of reporting it `fresh` again.
///
/// Best effort by design: a fingerprint that cannot be removed simply means the
/// retry finds nothing and the caller raises a named error, which is strictly
/// better than a removal failure masking the real diagnosis.
fn purge_fingerprints(
    artifacts: &[Artifact],
    target_dir: &Path,
    package: Option<&str>,
    selection: &TargetSel,
) {
    // The profile directory is `<target-dir>/debug` for our profile, but the
    // artifacts name it exactly, so take it from them when we have one.
    let mut roots: Vec<PathBuf> = Vec::new();
    for artifact in artifacts {
        for filename in &artifact.filenames {
            let profile = if filename.parent().is_some_and(|d| {
                d.file_name()
                    .is_some_and(|n| n == "deps" || n == "examples")
            }) {
                filename.parent().and_then(Path::parent)
            } else {
                filename.parent()
            };
            if let Some(profile) = profile {
                let root = profile.join(".fingerprint");
                if !roots.contains(&root) {
                    roots.push(root);
                }
            }
        }
    }
    if roots.is_empty() {
        roots.push(target_dir.join("debug").join(".fingerprint"));
    }

    // Fingerprint directories are named `<package name>-<hash>`. Match the hash
    // shape too: without it, `foo-` would also claim `foo-deep-<hash>`.
    let prefixes: Vec<String> = package.map_or_else(
        || {
            artifacts
                .iter()
                .map(|a| format!("{}-", a.crate_name))
                .collect()
        },
        |p| vec![format!("{p}-")],
    );
    let _ = selection;
    for root in &roots {
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        for entry in entries.filter_map(Result::ok) {
            let name = entry.file_name().to_string_lossy().into_owned();
            let matches = prefixes.iter().any(|prefix| {
                name.strip_prefix(prefix.as_str()).is_some_and(|hash| {
                    hash.len() >= 8 && hash.chars().all(|c| c.is_ascii_hexdigit())
                })
            });
            if matches {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }
}

/// The stale-MIR guard: a rebuilt target must have rewritten its `.mir`.
fn check_freshness(path: &Path, started: SystemTime, fresh: bool) -> crate::Result<()> {
    if fresh {
        // Cargo's fingerprint says this unit is up to date, and `path` was
        // derived from *that unit's* hashed artifact filename — so the file on
        // disk was written by a previous run of this same compilation, with
        // this crate, these features and this toolchain. (Before the derivation
        // was exact this reasoning did not hold: a directory scan could hand
        // back another unit's file and `fresh` would wave it through.)
        return Ok(());
    }
    let modified = std::fs::metadata(path)
        .and_then(|m| m.modified())
        .map_err(|e| Error::Io {
            path: path.display().to_string(),
            source: e,
        })?;
    // One second of slack: file mtimes are coarser than `SystemTime::now` on
    // some filesystems, and a rebuild that lands in the same second is current.
    let cutoff = started
        .checked_sub(std::time::Duration::from_secs(1))
        .unwrap_or(started);
    if modified < cutoff {
        return Err(Error::Cargo(format!(
            "{} is older than this invocation; cargo rebuilt the target but did not \
             re-emit its MIR — delete the target dir and retry",
            path.display()
        )));
    }
    Ok(())
}

fn cargo_bin() -> std::ffi::OsString {
    std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into())
}

/// The last few lines of a child's stderr, for an error message that explains itself.
fn stderr_tail(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(40);
    lines.get(start..).unwrap_or(&lines).join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_names_drop_the_metadata_hash_and_underscore_dashes() {
        assert_eq!(
            crate_name_from_stem("wf_corpus-3f2a91c0d4e5b678"),
            "wf_corpus"
        );
        assert_eq!(
            crate_name_from_stem("harvest-verify-corpus-3f2a91c0d4e5b678"),
            "harvest_verify_corpus"
        );
        assert_eq!(crate_name_from_stem("spike"), "spike");
        assert_eq!(
            crate_name_from_stem("format_and_outparams"),
            "format_and_outparams"
        );
        assert_eq!(
            crate_name_from_stem("a-b"),
            "a_b",
            "a short suffix is not a metadata hash"
        );
    }

    /// A metadata document with no packages: enough for every selection test
    /// that does not use `--all-examples`.
    fn no_metadata() -> serde_json::Value {
        serde_json::json!({ "packages": [] })
    }

    #[test]
    fn target_selection_defaults_to_the_lib() {
        let mut warnings = Vec::new();
        let selections = targets_for(
            &BuildRequest::default(),
            None,
            &no_metadata(),
            &mut warnings,
        );
        assert_eq!(selections, vec![TargetSel::Lib]);
        assert!(warnings.is_empty());
    }

    #[test]
    fn target_selection_keeps_every_requested_target() {
        let req = BuildRequest {
            lib: true,
            examples: vec!["one".to_string(), "two".to_string()],
            bins: vec!["cli".to_string()],
            tests: vec!["integration".to_string()],
            ..BuildRequest::default()
        };
        let mut warnings = Vec::new();
        let selections = targets_for(&req, None, &no_metadata(), &mut warnings);
        assert_eq!(
            selections,
            vec![
                TargetSel::Lib,
                TargetSel::Example("one".to_string()),
                TargetSel::Example("two".to_string()),
                TargetSel::Bin("cli".to_string()),
                TargetSel::Test("integration".to_string()),
            ]
        );
    }

    #[test]
    fn a_test_target_asks_cargo_for_the_test_kind() {
        let selection = TargetSel::Test("integration".to_string());
        assert_eq!(
            selection.cargo_flags(),
            vec!["--test".to_string(), "integration".to_string()]
        );
        assert_eq!(selection.kind(), "test");
        assert_eq!(selection.name(), Some("integration"));
        assert_eq!(selection.describe(), "test integration");
        assert_eq!(TargetSel::Lib.describe(), "lib");
        assert!(
            !BuildRequest {
                tests: vec!["integration".to_string()],
                ..BuildRequest::default()
            }
            .is_empty(),
            "`--test NAME` alone selects a target"
        );
    }

    #[test]
    fn feature_closure_follows_feature_to_feature_edges() {
        let pkg = serde_json::json!({
            "features": {
                "default": ["a"],
                "a": ["b"],
                "b": [],
                "c": ["dep:serde", "tokio/rt"],
            }
        });
        let enabled = enabled_features(&BuildRequest::default(), &pkg);
        assert!(enabled.contains("default") && enabled.contains("a") && enabled.contains("b"));
        assert!(!enabled.contains("c"));

        let req = BuildRequest {
            features: vec!["c,b".to_string()],
            no_default_features: true,
            ..BuildRequest::default()
        };
        let enabled = enabled_features(&req, &pkg);
        assert!(enabled.contains("c") && enabled.contains("b"));
        assert!(!enabled.contains("default") && !enabled.contains("a"));
        assert!(
            !enabled.contains("dep:serde") && !enabled.contains("tokio/rt"),
            "dependency edges are not features of this package"
        );
    }

    #[test]
    fn an_optimized_profile_is_refused() {
        // Guarded on the ambient environment so the test is honest about what
        // it can observe; the CI job runs with neither variable set.
        if std::env::var_os("RUSTFLAGS").is_none()
            && std::env::var_os("CARGO_PROFILE_DEV_OPT_LEVEL").is_none()
            && std::env::var_os("CARGO_ENCODED_RUSTFLAGS").is_none()
        {
            assert!(refuse_optimized_profile().is_ok());
        }
    }

    #[test]
    fn collecting_mir_paths_ignores_everything_else() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mir = dir.path().join("seeded-1234567890abcdef.mir");
        std::fs::write(&mir, "// mir").expect("write");
        std::fs::write(dir.path().join("seeded.rlib"), "").expect("write");
        let found = collect_mir_paths(&[dir.path().to_path_buf()]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].crate_name, "seeded");
        assert_eq!(found[0].path, mir);
        assert_eq!(found[0].target_kind, "mir");
    }

    #[test]
    fn a_named_mir_file_is_taken_as_given() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mir = dir.path().join("fixture.mir");
        std::fs::write(&mir, "// mir").expect("write");
        let found = collect_mir_paths(std::slice::from_ref(&mir));
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].crate_name, "fixture");
    }

    #[test]
    fn the_stale_guard_rejects_a_file_older_than_the_run() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mir = dir.path().join("old.mir");
        std::fs::write(&mir, "// mir").expect("write");
        let future = SystemTime::now() + std::time::Duration::from_secs(3600);
        assert!(check_freshness(&mir, future, false).is_err());
        assert!(
            check_freshness(&mir, future, true).is_ok(),
            "a `fresh` artifact is up to date by cargo's own fingerprint"
        );
        assert!(check_freshness(&mir, SystemTime::now(), false).is_ok());
    }

    #[test]
    fn the_workspace_root_contains_this_crate() {
        let root = workspace_root(None).expect("cargo locate-project");
        assert!(root.join("Cargo.toml").is_file(), "{}", root.display());
    }

    #[test]
    fn the_rustc_version_is_reported() {
        let version = rustc_version();
        assert!(version.starts_with("rustc "), "{version}");
    }

    // ── the `.mir` name is derived, never guessed ────────────────────────────

    #[test]
    fn a_mir_name_is_derived_from_the_hashed_artifact_filename() {
        let cases: [(&str, Option<&str>); 8] = [
            // A library: the hash lives on the `deps/` rmeta, and a `fresh`
            // lib reports only that plus the unhashed uplifted rlib.
            (
                "/t/debug/deps/libwf_corpus-15a511a7aac6c30a.rmeta",
                Some("/t/debug/deps/wf_corpus-15a511a7aac6c30a.mir"),
            ),
            (
                "/t/debug/deps/libwf_corpus-15a511a7aac6c30a.rlib",
                Some("/t/debug/deps/wf_corpus-15a511a7aac6c30a.mir"),
            ),
            // The uplifted copy carries no hash: it must be ignored, not turned
            // into `debug/wf_corpus.mir`, a file that never exists.
            ("/t/debug/libwf_corpus.rlib", None),
            // Examples, binaries and `--test` binaries.
            (
                "/t/debug/examples/spike-3f2a91c0d4e5b678",
                Some("/t/debug/examples/spike-3f2a91c0d4e5b678.mir"),
            ),
            (
                "/t/debug/examples/spike-3f2a91c0d4e5b678.d",
                Some("/t/debug/examples/spike-3f2a91c0d4e5b678.mir"),
            ),
            (
                "/t/debug/examples/spike-3f2a91c0d4e5b678.exe",
                Some("/t/debug/examples/spike-3f2a91c0d4e5b678.mir"),
            ),
            (
                "/t/debug/deps/integration-cf4f080f7ff034f3",
                Some("/t/debug/deps/integration-cf4f080f7ff034f3.mir"),
            ),
            // A suffix that is not a metadata hash is not a hash.
            ("/t/debug/deps/libwf-corpus.rmeta", None),
        ];
        for (artifact, expected) in cases {
            assert_eq!(
                mir_beside(Path::new(artifact)),
                expected.map(PathBuf::from),
                "artifact {artifact}"
            );
        }
    }

    #[test]
    fn a_metadata_hash_is_hex_and_long_enough() {
        assert!(has_metadata_hash("wf_corpus-15a511a7aac6c30a"));
        assert!(has_metadata_hash("integration-deadbeef"));
        assert!(!has_metadata_hash("wf_corpus"));
        assert!(!has_metadata_hash("harvest-verify"), "`verify` is not hex");
        assert!(!has_metadata_hash("a-1234567"), "seven digits is too short");
        assert!(!has_metadata_hash("-15a511a7aac6c30a"), "no crate name");
    }

    #[test]
    fn only_an_existing_file_is_accepted_and_no_directory_is_scanned() {
        let dir = tempfile::tempdir().expect("tempdir");
        let deps = dir.path().join("debug/deps");
        std::fs::create_dir_all(&deps).expect("mkdir");
        // A decoy with the right crate name and the newest mtime, exactly what
        // the old `<crate>-*.mir` mtime scan would have picked.
        std::fs::write(deps.join("wf_corpus-deadbeefdeadbeef.mir"), "// decoy").expect("write");
        let wanted = deps.join("wf_corpus-15a511a7aac6c30a.rmeta");

        assert_eq!(
            mir_for_artifact(std::slice::from_ref(&wanted)).expect("no ambiguity"),
            None,
            "the derived name does not exist, so nothing is returned — the decoy \
             beside it must not be substituted"
        );

        std::fs::write(deps.join("wf_corpus-15a511a7aac6c30a.mir"), "// real").expect("write");
        assert_eq!(
            mir_for_artifact(&[wanted]).expect("no ambiguity"),
            Some(deps.join("wf_corpus-15a511a7aac6c30a.mir"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_uplifted_example_is_resolved_through_its_hard_link() {
        // Cargo reports ONLY `examples/NAME` for an example — no hash anywhere
        // in the message — and uplifts it by hard-linking `examples/NAME-HASH`.
        let dir = tempfile::tempdir().expect("tempdir");
        let examples = dir.path().join("debug/examples");
        std::fs::create_dir_all(&examples).expect("mkdir");
        let hashed = examples.join("spike-46fe075bef8c6793");
        std::fs::write(&hashed, "ELF").expect("write");
        let uplifted = examples.join("spike");
        std::fs::hard_link(&hashed, &uplifted).expect("hard link");
        std::fs::write(examples.join("spike-46fe075bef8c6793.mir"), "// mir").expect("write");
        // A decoy from another toolchain, written later. The old mtime-ordered
        // scan picked exactly this file.
        std::fs::write(examples.join("spike-deadbeefdeadbeef"), "ELF").expect("write");
        std::fs::write(examples.join("spike-deadbeefdeadbeef.mir"), "// decoy").expect("write");

        assert_eq!(
            mir_for_artifact(std::slice::from_ref(&uplifted)).expect("identity is unambiguous"),
            Some(examples.join("spike-46fe075bef8c6793.mir")),
            "the hashed original is the file the uplifted copy IS, not the \
             newest file whose name looks similar"
        );
    }

    #[test]
    fn two_possible_mir_files_are_an_error_rather_than_a_choice() {
        // No hash in the message and no hard link (an uplift that copied): the
        // only honest answer is that the driver cannot tell them apart.
        let dir = tempfile::tempdir().expect("tempdir");
        let examples = dir.path().join("debug/examples");
        std::fs::create_dir_all(&examples).expect("mkdir");
        let uplifted = examples.join("spike");
        std::fs::write(&uplifted, "ELF").expect("write");
        std::fs::write(examples.join("spike-46fe075bef8c6793.mir"), "// a").expect("write");
        std::fs::write(examples.join("spike-deadbeefdeadbeef.mir"), "// b").expect("write");

        let err = mir_for_artifact(std::slice::from_ref(&uplifted))
            .expect_err("two candidates must not be silently resolved");
        let message = format!("{err}");
        assert!(message.contains("2 .mir files"), "{message}");

        // One candidate is unambiguous and is accepted.
        std::fs::remove_file(examples.join("spike-deadbeefdeadbeef.mir")).expect("rm");
        assert_eq!(
            mir_for_artifact(std::slice::from_ref(&uplifted)).expect("one candidate"),
            Some(examples.join("spike-46fe075bef8c6793.mir"))
        );
    }

    #[test]
    fn artifact_acceptance_is_scoped_to_the_requested_package_and_kind() {
        let ours = "path+file:///w/corpus/clean#harvest-verify-corpus-clean@0.6.0";
        let theirs = "path+file:///w/autumn-harvest#0.6.0";
        let line = |package_id: &str, name: &str, kind: &str| {
            serde_json::json!({
                "reason": "compiler-artifact",
                "package_id": package_id,
                "fresh": true,
                "target": { "name": name, "kind": [kind] },
                "filenames": [format!("/t/debug/deps/lib{name}-15a511a7aac6c30a.rmeta")],
            })
            .to_string()
        };
        let stdout = [
            line(theirs, "autumn_harvest", "lib"),
            line(ours, "harvest_verify_corpus_clean", "lib"),
            line(ours, "harvest_verify_corpus_clean", "test"),
            "not json at all".to_string(),
        ]
        .join("\n");
        let accept: std::collections::BTreeSet<String> =
            std::iter::once(ours.to_string()).collect();

        let accepted = accepted_artifacts(&stdout, &TargetSel::Lib, &accept);
        assert_eq!(accepted.len(), 1, "one package, one kind");
        assert_eq!(accepted[0].crate_name, "harvest_verify_corpus_clean");
        assert!(
            accepted_artifacts(&stdout, &TargetSel::Lib, &std::collections::BTreeSet::new())
                .is_empty(),
            "an empty accept set accepts nothing"
        );
    }

    #[test]
    fn package_ids_come_from_metadata_and_a_typo_is_an_error() {
        let metadata = serde_json::json!({
            "packages": [
                { "name": "a", "id": "path+file:///w/a#a@0.1.0" },
                { "name": "b", "id": "path+file:///w/b#b@0.1.0" },
            ]
        });
        assert_eq!(
            package_ids(&metadata, Some("a")).expect("a is a member"),
            std::iter::once("path+file:///w/a#a@0.1.0".to_string()).collect()
        );
        assert_eq!(
            package_ids(&metadata, None)
                .expect("no -p means every workspace member")
                .len(),
            2
        );
        let err = package_ids(&metadata, Some("nope")).expect_err("unknown package");
        assert!(format!("{err}").contains("nope"), "{err}");
    }

    // ── the opt-level guard ──────────────────────────────────────────────────

    #[test]
    fn the_encoded_rustflags_separator_is_not_whitespace() {
        // The regression: `--cfg\x1fx\x1f-O` split on whitespace is one token
        // that matches nothing, and the run proceeds with inlining on.
        let encoded = "--cfg\x1fx\x1f-O";
        assert_eq!(
            flag_tokens("CARGO_ENCODED_RUSTFLAGS", encoded),
            vec!["--cfg".to_string(), "x".to_string(), "-O".to_string()]
        );
        assert_eq!(
            optimization_request(&flag_tokens("CARGO_ENCODED_RUSTFLAGS", encoded)),
            Some("-O".to_string())
        );
        assert_eq!(
            flag_tokens("RUSTFLAGS", "--cfg x -O"),
            vec!["--cfg".to_string(), "x".to_string(), "-O".to_string()]
        );
    }

    #[test]
    fn every_spelling_of_a_non_zero_opt_level_is_refused() {
        let refused = [
            "-O",
            "-Copt-level=1",
            "-C opt-level=2",
            "-Copt-level=s",
            "-Copt-level=z",
            "--codegen opt-level=3",
            "--codegen=opt-level=3",
            "--release",
            "--profile release",
            "--cfg foo -Copt-level=2 --cfg bar",
        ];
        for flags in refused {
            assert!(
                optimization_request(&flag_tokens("RUSTFLAGS", flags)).is_some(),
                "`{flags}` must be refused: at opt-level > 0 rustc inlines the \
                 helper calls the traces are made of"
            );
        }
        let allowed = [
            "",
            "-Copt-level=0",
            "-C opt-level=0",
            "--cfg opt-level=2",
            "-D warnings",
            "-Ctarget-cpu=native",
            "--profile dev",
            "-Cdebuginfo=2",
        ];
        for flags in allowed {
            assert_eq!(
                optimization_request(&flag_tokens("RUSTFLAGS", flags)),
                None,
                "`{flags}` is not an optimization request"
            );
        }
    }

    // ── `--mir` directory scans ──────────────────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn a_mir_directory_scan_does_not_follow_symlinks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let scanned = dir.path().join("scanned");
        let elsewhere = dir.path().join("elsewhere");
        std::fs::create_dir_all(&scanned).expect("mkdir");
        std::fs::create_dir_all(&elsewhere).expect("mkdir");
        std::fs::write(scanned.join("real-1234567890abcdef.mir"), "// mir").expect("write");
        std::fs::write(elsewhere.join("linked-1234567890abcdef.mir"), "// mir").expect("write");
        std::os::unix::fs::symlink(
            elsewhere.join("linked-1234567890abcdef.mir"),
            scanned.join("link-1234567890abcdef.mir"),
        )
        .expect("symlink");
        std::os::unix::fs::symlink(&elsewhere, scanned.join("subdir")).expect("symlink");

        let found = collect_mir_paths(std::slice::from_ref(&scanned));
        assert_eq!(
            found
                .iter()
                .map(|m| m.path.clone())
                .collect::<Vec<PathBuf>>(),
            vec![scanned.join("real-1234567890abcdef.mir")],
            "a linked file and a linked directory are both outside what the \
             caller pointed at"
        );
    }

    /// Smoke test for the real cargo driver: opt in with `HARVEST_VERIFY_SMOKE=1`
    /// (it compiles a corpus crate, so it is far too slow for the default run).
    #[test]
    #[ignore = "slow: compiles a corpus crate; set HARVEST_VERIFY_SMOKE=1 and run with --ignored"]
    fn smoke_emits_mir_for_a_corpus_crate() {
        if std::env::var("HARVEST_VERIFY_SMOKE").ok().as_deref() != Some("1") {
            return;
        }
        let root = workspace_root(None).expect("workspace root");
        let req = BuildRequest {
            manifest_path: Some(root.join("Cargo.toml")),
            packages: vec!["harvest-verify-corpus-helpers-deep".to_string()],
            lib: true,
            target_dir: Some(root.join("target/harvest-verify")),
            ..BuildRequest::default()
        };
        let (mirs, warnings) = emit_mir_with_warnings(&req).expect("emit_mir");
        assert!(!mirs.is_empty(), "warnings: {warnings:?}");
        for mir in &mirs {
            let size = std::fs::metadata(&mir.path).map_or(0, |m| m.len());
            println!(
                "{} {} {} ({size} bytes)",
                mir.crate_name,
                mir.target_kind,
                mir.path.display()
            );
            assert!(size > 0);
        }
    }

    /// Example enumeration against the real workspace: `--all-examples` must
    /// list every example whose `required-features` are enabled and warn about
    /// (not silently drop) the rest. Same opt-in as the driver smoke test.
    #[test]
    #[ignore = "runs `cargo metadata`; set HARVEST_VERIFY_SMOKE=1 and run with --ignored"]
    fn smoke_enumerates_examples_and_respects_required_features() {
        if std::env::var("HARVEST_VERIFY_SMOKE").ok().as_deref() != Some("1") {
            return;
        }
        let root = workspace_root(None).expect("workspace root");
        let bare = BuildRequest {
            manifest_path: Some(root.join("Cargo.toml")),
            ..BuildRequest::default()
        };
        let metadata = workspace_metadata(&bare).expect("cargo metadata");
        let mut skipped = Vec::new();
        let without = enumerate_examples(&bare, Some("autumn-harvest"), &metadata, &mut skipped);

        let with_testing = BuildRequest {
            features: vec!["testing".to_string()],
            ..bare
        };
        let mut warnings = Vec::new();
        let with = enumerate_examples(
            &with_testing,
            Some("autumn-harvest"),
            &metadata,
            &mut warnings,
        );

        println!(
            "examples: {} without `testing` ({} skipped), {} with it ({} skipped)",
            without.len(),
            skipped.len(),
            with.len(),
            warnings.len()
        );
        assert!(with.len() > without.len(), "`testing` unlocks examples");
        assert!(warnings.len() < skipped.len());
        assert!(
            skipped.iter().any(|w| w.contains("testing")),
            "a skipped example must say which feature it needs: {skipped:?}"
        );
    }
}
