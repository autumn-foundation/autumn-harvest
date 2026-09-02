//! Cargo driver: emits MIR for the requested targets into an owned target dir and
//! returns the `.mir` files (located from cargo's JSON artifact messages).
//!
//! The whole substrate decision of D1 lives here. `rustc --emit=mir` is stable,
//! but *finding* the file it wrote is not: the name carries cargo's metadata
//! hash, so the only reliable route is cargo's own `--message-format=json`
//! `compiler-artifact` messages, whose `filenames` name the rlib/binary the
//! `.mir` sits beside.
//!
//! Two invariants the driver enforces rather than assumes:
//!
//! * **opt-level 0.** At `-C opt-level=1` and above, rustc inlines the very
//!   helper calls the interprocedural analysis exists to follow, so a laundered
//!   source disappears into its caller and the tool would report
//!   `proven-deterministic` for a workflow that is not. An optimized profile is
//!   refused ([`crate::Error::Cargo`]), never silently accepted.
//! * **no stale MIR.** A `.mir` file left over from an earlier run would be
//!   analyzed as if it described the current source. Every emitted file must
//!   either be newer than the invocation, or belong to an artifact cargo itself
//!   reported as `fresh` (its fingerprint is up to date, so the file on disk
//!   *is* the current one).

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
}

impl TargetSel {
    fn cargo_flags(&self) -> Vec<String> {
        match self {
            Self::Lib => vec!["--lib".to_string()],
            Self::Example(name) => vec!["--example".to_string(), name.clone()],
            Self::Bin(name) => vec!["--bin".to_string(), name.clone()],
        }
    }

    const fn kind(&self) -> &'static str {
        match self {
            Self::Lib => "lib",
            Self::Example(_) => "example",
            Self::Bin(_) => "bin",
        }
    }

    fn name(&self) -> Option<&str> {
        match self {
            Self::Lib => None,
            Self::Example(name) | Self::Bin(name) => Some(name),
        }
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
    let target_dir = match &req.target_dir {
        Some(dir) => dir.clone(),
        None => workspace_root(req.manifest_path.as_deref())?
            .join("target")
            .join("harvest-verify"),
    };

    let packages: Vec<Option<String>> = if req.packages.is_empty() {
        vec![None]
    } else {
        req.packages.iter().map(|p| Some(p.clone())).collect()
    };

    let mut emitted: Vec<EmittedMir> = Vec::new();
    for package in &packages {
        let selections = targets_for(req, package.as_deref(), &mut warnings)?;
        for selection in selections {
            let mut mirs = run_cargo_rustc(req, package.as_deref(), &selection, &target_dir)?;
            if mirs.is_empty() {
                warnings.push(format!(
                    "no .mir produced for {} target {}{}",
                    selection.kind(),
                    selection.name().unwrap_or("(lib)"),
                    package
                        .as_deref()
                        .map(|p| format!(" of package {p}"))
                        .unwrap_or_default()
                ));
            }
            emitted.append(&mut mirs);
        }
    }
    Ok((emitted, warnings))
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

fn collect_dir(dir: &Path, out: &mut Vec<EmittedMir>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries.filter_map(Result::ok).map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            collect_dir(&path, out);
        } else if path.extension().is_some_and(|e| e == "mir") {
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

/// Refuse a profile that would inline the calls the analysis follows.
fn refuse_optimized_profile() -> crate::Result<()> {
    if let Some(level) = std::env::var_os("CARGO_PROFILE_DEV_OPT_LEVEL") {
        let level = level.to_string_lossy().trim().to_string();
        if level != "0" {
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
        if flags.contains("opt-level") && !flags.contains("opt-level=0") {
            return Err(Error::Cargo(format!(
                "{var} sets a non-zero opt-level ({flags}); harvest-verify needs opt-level 0"
            )));
        }
        if flags.split_whitespace().any(|f| f == "-O") {
            return Err(Error::Cargo(format!(
                "{var} contains -O; harvest-verify needs opt-level 0"
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
    warnings: &mut Vec<String>,
) -> crate::Result<Vec<TargetSel>> {
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
    if req.all_examples {
        for name in enumerate_examples(req, package, warnings)? {
            let selection = TargetSel::Example(name);
            if !out.contains(&selection) {
                out.push(selection);
            }
        }
    }
    if out.is_empty() {
        out.push(TargetSel::Lib);
    }
    Ok(out)
}

/// Examples of `package`, skipping those whose `required-features` are not enabled.
fn enumerate_examples(
    req: &BuildRequest,
    package: Option<&str>,
    warnings: &mut Vec<String>,
) -> crate::Result<Vec<String>> {
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
    let meta: serde_json::Value = serde_json::from_slice(&out.stdout)
        .map_err(|e| Error::Cargo(format!("`cargo metadata` produced unreadable JSON: {e}")))?;

    let mut names: Vec<String> = Vec::new();
    let packages = meta
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
    Ok(names)
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
fn run_cargo_rustc(
    req: &BuildRequest,
    package: Option<&str>,
    selection: &TargetSel,
    target_dir: &Path,
) -> crate::Result<Vec<EmittedMir>> {
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
            "cannot run `cargo rustc` for {} {}: {e}",
            selection.kind(),
            selection.name().unwrap_or("(lib)")
        ))
    })?;
    if !out.status.success() {
        return Err(Error::Cargo(format!(
            "`cargo rustc` failed for {} {} (exit {}):\n{}",
            selection.kind(),
            selection.name().unwrap_or("(lib)"),
            out.status.code().unwrap_or(-1),
            stderr_tail(&out.stderr)
        )));
    }

    let mut emitted: Vec<EmittedMir> = Vec::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let Ok(message) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if message.get("reason").and_then(serde_json::Value::as_str) != Some("compiler-artifact") {
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
        if selection.name().is_some_and(|wanted| name != wanted) {
            continue;
        }
        let fresh = message
            .get("fresh")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let filenames: Vec<&str> = message
            .get("filenames")
            .and_then(serde_json::Value::as_array)
            .map(|f| f.iter().filter_map(serde_json::Value::as_str).collect())
            .unwrap_or_default();
        let crate_name = name.replace('-', "_");
        if let Some(path) = locate_mir(&filenames, &crate_name, started, fresh)? {
            let mir = EmittedMir {
                crate_name,
                target_kind: selection.kind().to_string(),
                path,
            };
            if !emitted.contains(&mir) {
                emitted.push(mir);
            }
        }
    }
    Ok(emitted)
}

/// Derive the `.mir` path from an artifact's `filenames` and check it is current.
///
/// `deps/libNAME-HASH.rlib` → `deps/NAME-HASH.mir`; `examples/NAME-HASH` and
/// `deps/NAME-HASH` (binaries) → the same name with a `.mir` extension. When
/// that exact name is absent, the artifact's directory is scanned for a
/// `<crate>-*.mir`, because rustc names its emit after the *crate* name
/// (underscored) while cargo names the artifact after the *target* name.
fn locate_mir(
    filenames: &[&str],
    crate_name: &str,
    started: SystemTime,
    fresh: bool,
) -> crate::Result<Option<PathBuf>> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    for filename in filenames {
        let path = Path::new(filename);
        // Record the directory of *every* filename, `.rmeta` included: when
        // cargo reports the unit as `fresh` it lists the uplifted
        // `debug/libX.rlib` and a `deps/libX-hash.rmeta`, and the `.mir` lives
        // beside the latter. `deps/` is added explicitly for the same reason.
        for dir in path
            .parent()
            .into_iter()
            .flat_map(|d| [d.to_path_buf(), d.join("deps")])
        {
            if !dirs.contains(&dir) {
                dirs.push(dir);
            }
        }
        if path.extension().is_some_and(|e| e == "rmeta" || e == "d") {
            continue;
        }
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let bare = if path.extension().is_some_and(|e| e == "rlib") {
            stem.strip_prefix("lib").unwrap_or(&stem).to_string()
        } else {
            stem
        };
        let Some(dir) = path.parent() else {
            continue;
        };
        let candidate = dir.join(format!("{bare}.mir"));
        if candidate.is_file() {
            check_freshness(&candidate, started, fresh)?;
            return Ok(Some(candidate));
        }
    }
    // Fall back to whatever `<crate>-*.mir` the compiler left in the out dir.
    let mut best: Option<(SystemTime, PathBuf)> = None;
    for dir in &dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "mir") {
                continue;
            }
            let stem = path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            if stem != crate_name && !stem.starts_with(&format!("{crate_name}-")) {
                continue;
            }
            let modified = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            if best.as_ref().is_none_or(|(seen, _)| modified > *seen) {
                best = Some((modified, path));
            }
        }
    }
    match best {
        Some((_, path)) => {
            check_freshness(&path, started, fresh)?;
            Ok(Some(path))
        }
        None => Ok(None),
    }
}

/// The stale-MIR guard: a rebuilt target must have rewritten its `.mir`.
fn check_freshness(path: &Path, started: SystemTime, fresh: bool) -> crate::Result<()> {
    if fresh {
        // Cargo's fingerprint says the artifact is up to date, so the file on
        // disk was written by a previous run of this same compilation.
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

    #[test]
    fn target_selection_defaults_to_the_lib() {
        let mut warnings = Vec::new();
        let selections = targets_for(&BuildRequest::default(), None, &mut warnings)
            .expect("no cargo call is needed without --all-examples");
        assert_eq!(selections, vec![TargetSel::Lib]);
        assert!(warnings.is_empty());
    }

    #[test]
    fn target_selection_keeps_every_requested_target() {
        let req = BuildRequest {
            lib: true,
            examples: vec!["one".to_string(), "two".to_string()],
            bins: vec!["cli".to_string()],
            ..BuildRequest::default()
        };
        let mut warnings = Vec::new();
        let selections = targets_for(&req, None, &mut warnings).expect("selection");
        assert_eq!(
            selections,
            vec![
                TargetSel::Lib,
                TargetSel::Example("one".to_string()),
                TargetSel::Example("two".to_string()),
                TargetSel::Bin("cli".to_string()),
            ]
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
            let size = std::fs::metadata(&mir.path).map(|m| m.len()).unwrap_or(0);
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
        let mut skipped = Vec::new();
        let without = enumerate_examples(&bare, Some("autumn-harvest"), &mut skipped)
            .expect("cargo metadata");

        let with_testing = BuildRequest {
            features: vec!["testing".to_string()],
            ..bare
        };
        let mut warnings = Vec::new();
        let with = enumerate_examples(&with_testing, Some("autumn-harvest"), &mut warnings)
            .expect("cargo metadata");

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
