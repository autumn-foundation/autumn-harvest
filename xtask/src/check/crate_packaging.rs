//! `cargo xtask check crate-packaging`: checks that each publishable crate
//! packages for crates.io.
//!
//! Publishing stays manual. The maintainer runs `cargo publish` for each
//! crate, in the order of `docs/releasing.md`. This command finds a problem
//! before the tag, with no registry token.
//!
//! The plan check reads `cargo metadata` and the publish order. It fails in
//! these cases:
//!
//! - A publishable crate is not in the order. Add it, or set
//!   `publish = false`.
//! - The order lists a crate that is not a publishable workspace member.
//! - The order lists a crate twice.
//! - The order lists a crate before a workspace crate that it depends on.
//! - A publishable crate depends on a workspace crate that is not published.
//! - A publishable crate has no `description` or no `license`. Cargo only
//!   warns, but crates.io refuses the upload.
//!
//! A dependency is a normal or build dependency, or a dev dependency with a
//! version. Cargo removes a path-only dev dependency from the package.
//!
//! The full check then runs `cargo package --locked` for each crate in one
//! run. Cargo verifies each crate against a local registry that holds its
//! siblings. Last, the check reads each `.crate` file. A file over 10 MiB
//! fails, because crates.io refuses it. A file at 8 MiB or more gives a
//! warning.
//!
//! The `lint` job runs `--plan-only` on each pull request. `release-pr.yml`
//! runs the full check on each release pull request.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde_json::Value;
use smop::prelude::*;

use super::support::Report;
use crate::util;

/// The page that holds the publish order.
const ORDER_DOC: &str = "docs/releasing.md";
/// The line before the publish order.
const ORDER_START: &str = "<!-- publish-order:start -->";
/// The line after the publish order.
const ORDER_END: &str = "<!-- publish-order:end -->";
/// The largest `.crate` file that crates.io accepts.
const MAX_CRATE_BYTES: u64 = 10 * 1024 * 1024;
/// The size at which a `.crate` file gives a warning.
const WARN_CRATE_BYTES: u64 = 8 * 1024 * 1024;
/// The registry name that `cargo metadata` uses for crates.io.
const CRATES_IO: &str = "crates-io";

/// Arguments of `cargo xtask check crate-packaging`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {
    /// Run the plan check only. Do not run `cargo package`.
    #[arg(long)]
    pub plan_only: bool,
    /// Pass `--no-verify` to `cargo package`, for a fast local run.
    #[arg(long)]
    pub no_verify: bool,
    /// Pass `--allow-dirty` to `cargo package`, for a local run.
    #[arg(long)]
    pub allow_dirty: bool,
    /// The form of each warning line. `run` sets it from `GITHUB_ACTIONS`.
    #[arg(skip)]
    pub warnings: WarningStyle,
}

/// The form of a warning line.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WarningStyle {
    /// A plain `warning: ` line.
    #[default]
    Plain,
    /// A GitHub Actions `::warning::` annotation.
    Annotation,
}

/// One dependency of a workspace member on another workspace member.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Edge {
    /// The package name of the dependency.
    name: String,
    /// True when the package on crates.io keeps this dependency.
    kept: bool,
}

/// One workspace member, as the check needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Member {
    name: String,
    version: String,
    publishable: bool,
    has_description: bool,
    has_license: bool,
    edges: Vec<Edge>,
}

/// The workspace members and the target directory of `cargo metadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Workspace {
    members: Vec<Member>,
    target_dir: PathBuf,
}

/// Runs `cargo xtask check crate-packaging`.
pub fn run(mut args: Args) -> Result<ExitCode> {
    if std::env::var("GITHUB_ACTIONS").is_ok_and(|value| value == "true") {
        args.warnings = WarningStyle::Annotation;
    }
    let root = util::repo_root();
    let metadata = sh::cargo()
        .args(["metadata", "--no-deps", "--format-version=1"])
        .dir(&root)
        .output_raw()?;
    let workspace = if metadata.status.success() {
        serde_json::from_slice::<Value>(&metadata.stdout)
            .ok()
            .as_ref()
            .and_then(parse_metadata)
    } else {
        eprintln!("{}", String::from_utf8_lossy(&metadata.stderr).trim_end());
        None
    };
    let doc = std::fs::read_to_string(root.join(ORDER_DOC)).ok();
    let report = evaluate(
        workspace.as_ref(),
        doc.as_deref(),
        args,
        |cargo_args| {
            let status = sh::cargo().args(cargo_args).dir(&root).status()?;
            Ok(status.success())
        },
        |path| std::fs::metadata(path).ok().map(|meta| meta.len()),
    )?;
    Ok(report.emit())
}

/// Reads the workspace members from `cargo metadata --no-deps` output.
///
/// `None` means that the output has no package list.
fn parse_metadata(metadata: &Value) -> Option<Workspace> {
    let ids: BTreeSet<&str> = metadata
        .get("workspace_members")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let packages: Vec<&Value> = metadata
        .get("packages")?
        .as_array()?
        .iter()
        .filter(|package| {
            package
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| ids.contains(id))
        })
        .collect();
    let names: BTreeSet<&str> = packages
        .iter()
        .filter_map(|package| package.get("name").and_then(Value::as_str))
        .collect();
    let text = |package: &Value, key: &str| {
        package
            .get(key)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
    };
    let mut members = Vec::new();
    for package in packages {
        // Null means any registry. An empty list is `publish = false`.
        let publishable = match package.get("publish") {
            None | Some(Value::Null) => true,
            Some(registries) => registries
                .as_array()?
                .iter()
                .any(|registry| registry.as_str() == Some(CRATES_IO)),
        };
        let mut edges = Vec::new();
        for dependency in package.get("dependencies")?.as_array()? {
            let name = dependency.get("name")?.as_str()?;
            if dependency.get("path").is_none_or(Value::is_null) || !names.contains(name) {
                continue;
            }
            // Cargo removes a dev dependency with no version from the package.
            let dev = dependency.get("kind").and_then(Value::as_str) == Some("dev");
            let versioned = dependency.get("req").and_then(Value::as_str) != Some("*");
            edges.push(Edge {
                name: name.to_owned(),
                kept: !dev || versioned,
            });
        }
        members.push(Member {
            name: package.get("name")?.as_str()?.to_owned(),
            version: package.get("version")?.as_str()?.to_owned(),
            publishable,
            has_description: text(package, "description"),
            has_license: text(package, "license") || text(package, "license_file"),
            edges,
        });
    }
    Some(Workspace {
        members,
        target_dir: PathBuf::from(metadata.get("target_directory")?.as_str()?),
    })
}

/// Reads the publish order between the two markers of `doc`.
///
/// An error names the defect of the page.
fn parse_order(doc: &str) -> Result<Vec<String>, String> {
    let lines: Vec<&str> = doc.lines().map(str::trim).collect();
    let section = lines
        .iter()
        .position(|line| *line == ORDER_START)
        .and_then(|start| {
            let rest = &lines[start + 1..];
            rest.iter()
                .position(|line| *line == ORDER_END)
                .map(|end| &rest[..end])
        })
        .ok_or_else(|| {
            format!("{ORDER_DOC}: no line `{ORDER_START}` with a later line `{ORDER_END}`.")
        })?;
    let mut order = Vec::new();
    for line in section.iter().filter(|line| !line.is_empty()) {
        let name = line
            .split_once(". `")
            .filter(|(number, _)| {
                !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
            })
            .and_then(|(_, rest)| rest.split_once('`'))
            .map(|(name, _)| name)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                format!(
                    "{ORDER_DOC}: cannot read `{line}` in the publish order. \
                     Write each item as `N. `crate``."
                )
            })?;
        order.push(name.to_owned());
    }
    if order.is_empty() {
        return Err(format!("{ORDER_DOC}: the publish order is empty."));
    }
    Ok(order)
}

/// Returns one finding for each defect of the plan.
fn plan_findings(members: &[Member], order: &[String]) -> Vec<String> {
    let mut findings = Vec::new();
    let by_name: BTreeMap<&str, &Member> = members
        .iter()
        .map(|member| (member.name.as_str(), member))
        .collect();
    let mut position: BTreeMap<&str, usize> = BTreeMap::new();
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for (index, name) in order.iter().enumerate() {
        position.entry(name).or_insert(index);
        *counts.entry(name).or_default() += 1;
    }

    for member in members.iter().filter(|member| member.publishable) {
        if !position.contains_key(member.name.as_str()) {
            findings.push(format!(
                "{}: publishable, but not in the publish order of {ORDER_DOC}. \
                 Add it, or set `publish = false`.",
                member.name
            ));
        }
    }
    let mut reported = BTreeSet::new();
    for name in order {
        if !reported.insert(name.as_str()) {
            continue;
        }
        if !by_name
            .get(name.as_str())
            .is_some_and(|member| member.publishable)
        {
            findings.push(format!(
                "{name}: in the publish order, but not a publishable workspace crate."
            ));
        }
        let count = counts.get(name.as_str()).copied().unwrap_or_default();
        if count > 1 {
            findings.push(format!("{name}: in the publish order {count} times."));
        }
    }

    for member in members.iter().filter(|member| member.publishable) {
        let name = member.name.as_str();
        let mut seen = BTreeSet::new();
        for edge in member.edges.iter().filter(|edge| edge.kept) {
            let dep = edge.name.as_str();
            if dep == name || !seen.insert(dep) {
                continue;
            }
            match by_name.get(dep) {
                Some(target) if !target.publishable => findings.push(format!(
                    "{name}: depends on {dep}, which sets `publish = false`."
                )),
                _ => {
                    if let (Some(at), Some(dep_at)) = (position.get(name), position.get(dep))
                        && dep_at > at
                    {
                        findings.push(format!(
                            "{name}: in the publish order before {dep}, which it depends on."
                        ));
                    }
                }
            }
        }
        if !member.has_description {
            findings.push(format!(
                "{name}: no `description`. crates.io refuses the upload."
            ));
        }
        if !member.has_license {
            findings.push(format!(
                "{name}: no `license` or `license-file`. crates.io refuses the upload."
            ));
        }
    }
    findings
}

/// Formats a byte count in MiB with two decimals.
fn mib(bytes: u64) -> String {
    let hundredths = bytes.saturating_mul(100) / (1024 * 1024);
    format!("{}.{:02} MiB", hundredths / 100, hundredths % 100)
}

/// Returns the size failures and the size warnings of each `.crate` file.
///
/// Each entry holds a crate name and its file size. `None` means that the
/// file is missing.
fn size_findings(sizes: &[(String, Option<u64>)]) -> (Vec<String>, Vec<String>) {
    let mut failures = Vec::new();
    let mut warnings = Vec::new();
    for (name, size) in sizes {
        match *size {
            None => failures.push(format!("{name}: cargo package wrote no .crate file.")),
            Some(bytes) if bytes > MAX_CRATE_BYTES => failures.push(format!(
                "{name}: the .crate file is {bytes} bytes. \
                 crates.io refuses a file over {MAX_CRATE_BYTES} bytes."
            )),
            Some(bytes) if bytes >= WARN_CRATE_BYTES => warnings.push(format!(
                "{name}: the .crate file is {}, near the crates.io limit of {}.",
                mib(bytes),
                mib(MAX_CRATE_BYTES)
            )),
            Some(_) => {}
        }
    }
    (failures, warnings)
}

/// Returns the arguments of the `cargo package` run.
fn package_args(order: &[String], args: Args) -> Vec<String> {
    let mut out = vec!["package".to_owned(), "--locked".to_owned()];
    if args.no_verify {
        out.push("--no-verify".to_owned());
    }
    if args.allow_dirty {
        out.push("--allow-dirty".to_owned());
    }
    for name in order {
        out.push("-p".to_owned());
        out.push(name.clone());
    }
    out
}

/// Returns the path of the `.crate` file of one crate.
fn crate_file(target_dir: &Path, name: &str, version: &str) -> PathBuf {
    target_dir
        .join("package")
        .join(format!("{name}-{version}.crate"))
}

/// Adds the findings to `report` as a failure, with a fix hint.
fn fail(report: &mut Report, title: &str, findings: &[String], hint: &str) {
    report.err(title);
    for finding in findings {
        report.err(format!("  - {finding}"));
    }
    report.err("");
    report.fail(hint);
}

/// Runs the check on the parsed inputs.
///
/// `package` runs `cargo package` and returns true on success. `size`
/// returns the size of a file, or `None` when it is missing.
fn evaluate(
    workspace: Option<&Workspace>,
    doc: Option<&str>,
    args: Args,
    package: impl FnOnce(&[String]) -> Result<bool>,
    size: impl Fn(&Path) -> Option<u64>,
) -> Result<Report> {
    let mut report = Report::new();
    // A gate that read no crate list must not print OK.
    let Some(workspace) =
        workspace.filter(|found| found.members.iter().any(|member| member.publishable))
    else {
        report.fail(
            "cargo xtask check crate-packaging: cargo metadata or the parser failed, or the \
             workspace has no publishable crate. The check fails closed.",
        );
        return Ok(report);
    };
    let Some(doc) = doc else {
        report.fail(format!(
            "cargo xtask check crate-packaging: cannot read {ORDER_DOC}. The check fails closed."
        ));
        return Ok(report);
    };
    let order = match parse_order(doc) {
        Ok(order) => order,
        Err(defect) => {
            report.fail(defect);
            return Ok(report);
        }
    };

    let findings = plan_findings(&workspace.members, &order);
    if !findings.is_empty() {
        fail(
            &mut report,
            "The publish plan is wrong:",
            &findings,
            &format!(
                "Fix: list each publishable crate once in {ORDER_DOC}, after its dependencies. \
                 Set `publish = false` on each crate that crates.io must not get."
            ),
        );
        return Ok(report);
    }
    if args.plan_only {
        report.out(format!(
            "OK: the publish order lists each of the {} publishable crates after its \
             dependencies.",
            order.len()
        ));
        return Ok(report);
    }

    let versions: BTreeMap<&str, &str> = workspace
        .members
        .iter()
        .map(|member| (member.name.as_str(), member.version.as_str()))
        .collect();
    let files: Vec<(String, PathBuf)> = order
        .iter()
        .map(|name| {
            let version = versions.get(name.as_str()).copied().unwrap_or_default();
            (
                name.clone(),
                crate_file(&workspace.target_dir, name, version),
            )
        })
        .collect();
    // An old file from an earlier run must not pass the size check.
    for (_, path) in &files {
        if path.exists() {
            std::fs::remove_file(path)
                .with_context(|| format!("removing the old {}", path.display()))?;
        }
    }
    if !package(&package_args(&order, args))? {
        report.fail("cargo package failed. Read the cargo output above.");
        return Ok(report);
    }

    let sizes: Vec<(String, Option<u64>)> = files
        .iter()
        .map(|(name, path)| (name.clone(), size(path)))
        .collect();
    let (failures, warnings) = size_findings(&sizes);
    let prefix = match args.warnings {
        WarningStyle::Plain => "warning: ",
        WarningStyle::Annotation => "::warning::",
    };
    for warning in &warnings {
        report.err(format!("{prefix}{warning}"));
    }
    if !failures.is_empty() {
        fail(
            &mut report,
            "A .crate file is wrong:",
            &failures,
            "Fix: exclude large files with `exclude` or `include` in the crate manifest.",
        );
        return Ok(report);
    }
    report.out(format!(
        "OK: {} crates package for crates.io, in the documented order: {}.",
        order.len(),
        order.join(", ")
    ));
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;

    const FULL: Args = Args {
        plan_only: false,
        no_verify: false,
        allow_dirty: false,
        warnings: WarningStyle::Plain,
    };
    const PLAN: Args = Args {
        plan_only: true,
        ..FULL
    };

    fn member(name: &str, edges: &[(&str, bool)]) -> Member {
        Member {
            name: name.into(),
            version: "1.0.0".into(),
            publishable: true,
            has_description: true,
            has_license: true,
            edges: edges
                .iter()
                .map(|(dep, kept)| Edge {
                    name: (*dep).into(),
                    kept: *kept,
                })
                .collect(),
        }
    }

    fn unpublished(name: &str) -> Member {
        Member {
            publishable: false,
            ..member(name, &[])
        }
    }

    fn order(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    fn doc(names: &[&str]) -> String {
        let mut text = format!("# Releasing\n\n{ORDER_START}\n");
        for (index, name) in names.iter().enumerate() {
            let _ = writeln!(text, "{}. `{name}`", index + 1);
        }
        text.push_str(ORDER_END);
        text.push('\n');
        text
    }

    fn workspace(members: Vec<Member>) -> Workspace {
        Workspace {
            members,
            target_dir: PathBuf::from("/t"),
        }
    }

    fn two_crates() -> Workspace {
        workspace(vec![
            member("core", &[]),
            member("app", &[("core", true)]),
            unpublished("tool"),
        ])
    }

    #[test]
    fn a_complete_plan_has_no_finding() {
        let members = two_crates().members;
        assert_eq!(
            plan_findings(&members, &order(&["core", "app"])),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_publishable_crate_that_the_order_omits_fails() {
        let members = two_crates().members;
        let findings = plan_findings(&members, &order(&["core"]));
        assert_eq!(
            findings,
            [
                "app: publishable, but not in the publish order of docs/releasing.md. \
                 Add it, or set `publish = false`."
            ]
        );
    }

    #[test]
    fn a_listed_crate_that_is_not_publishable_fails() {
        let members = two_crates().members;
        let findings = plan_findings(&members, &order(&["core", "app", "tool", "gone"]));
        assert_eq!(
            findings,
            [
                "tool: in the publish order, but not a publishable workspace crate.",
                "gone: in the publish order, but not a publishable workspace crate.",
            ]
        );
    }

    #[test]
    fn a_crate_listed_twice_fails_once() {
        let members = two_crates().members;
        let findings = plan_findings(&members, &order(&["core", "app", "core"]));
        assert_eq!(findings, ["core: in the publish order 2 times."]);
    }

    #[test]
    fn a_crate_before_its_dependency_fails() {
        let members = two_crates().members;
        let findings = plan_findings(&members, &order(&["app", "core"]));
        assert_eq!(
            findings,
            ["app: in the publish order before core, which it depends on."]
        );
    }

    #[test]
    fn a_stripped_dev_dependency_sets_no_order() {
        let members = vec![
            member("core", &[("app", false)]),
            member("app", &[("core", true)]),
        ];
        assert_eq!(
            plan_findings(&members, &order(&["core", "app"])),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_kept_dependency_on_an_unpublished_crate_fails() {
        let members = vec![
            member("core", &[("tool", true), ("fixture", false)]),
            unpublished("tool"),
            unpublished("fixture"),
        ];
        let findings = plan_findings(&members, &order(&["core"]));
        assert_eq!(
            findings,
            ["core: depends on tool, which sets `publish = false`."]
        );
    }

    #[test]
    fn missing_metadata_fails() {
        let mut core = member("core", &[]);
        core.has_description = false;
        core.has_license = false;
        let findings = plan_findings(&[core], &order(&["core"]));
        assert_eq!(
            findings,
            [
                "core: no `description`. crates.io refuses the upload.",
                "core: no `license` or `license-file`. crates.io refuses the upload.",
            ]
        );
    }

    #[test]
    fn the_order_reads_the_numbered_items_between_the_markers() {
        let text = format!(
            "1. `outside`\n{ORDER_START}\n\n1. `core`: the base crate.\n2. `app`\n{ORDER_END}\n3. `after`\n"
        );
        assert_eq!(parse_order(&text), Ok(order(&["core", "app"])));
    }

    #[test]
    fn a_defective_order_section_is_an_error() {
        assert_eq!(
            parse_order("no markers"),
            Err(format!(
                "{ORDER_DOC}: no line `{ORDER_START}` with a later line `{ORDER_END}`."
            ))
        );
        assert_eq!(
            parse_order(&format!("{ORDER_START}\n{ORDER_END}\n")),
            Err(format!("{ORDER_DOC}: the publish order is empty."))
        );
        assert_eq!(
            parse_order(&format!("{ORDER_START}\n1. `core`\n- app\n{ORDER_END}\n")),
            Err(format!(
                "{ORDER_DOC}: cannot read `- app` in the publish order. \
                 Write each item as `N. `crate``."
            ))
        );
    }

    #[test]
    fn the_real_page_has_a_publish_order() {
        let text = util::read_repo_file(ORDER_DOC).expect("read the release page");
        let found = parse_order(&text).expect("parse the publish order");
        assert!(found.contains(&"autumn-harvest".to_owned()), "{found:?}");
    }

    #[test]
    fn metadata_gives_members_edges_and_flags() {
        let metadata = serde_json::json!({
            "workspace_members": ["core 1", "app 1", "tool 1"],
            "target_directory": "/r/target",
            "packages": [
                {
                    "id": "core 1", "name": "core", "version": "1.0.0",
                    "description": "Core.", "license": "MIT", "license_file": null,
                    "publish": null,
                    "dependencies": [
                        { "name": "app", "kind": "dev", "req": "*", "path": "/r/app" },
                        { "name": "serde", "kind": null, "req": "^1" }
                    ]
                },
                {
                    "id": "app 1", "name": "app", "version": "1.0.0",
                    "description": "", "license": null, "license_file": "LICENSE",
                    "publish": ["crates-io"],
                    "dependencies": [
                        { "name": "core", "kind": null, "req": "^1.0.0", "path": "/r/core" },
                        { "name": "tool", "kind": "dev", "req": "^1.0.0", "path": "/r/tool" },
                        { "name": "core", "kind": "build", "req": "^1.0.0", "path": "/r/core" }
                    ]
                },
                {
                    "id": "tool 1", "name": "tool", "version": "1.0.0",
                    "description": null, "license": null, "license_file": null,
                    "publish": [], "dependencies": []
                },
                {
                    "id": "other 1", "name": "other", "version": "1.0.0",
                    "publish": null, "dependencies": []
                }
            ]
        });
        let parsed = parse_metadata(&metadata).expect("workspace");
        assert_eq!(parsed.target_dir, PathBuf::from("/r/target"));
        let names: Vec<&str> = parsed.members.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, ["core", "app", "tool"]);
        let core = &parsed.members[0];
        assert!(core.publishable && core.has_description && core.has_license);
        assert_eq!(
            core.edges,
            [Edge {
                name: "app".into(),
                kept: false
            }]
        );
        let app = &parsed.members[1];
        assert!(app.publishable && !app.has_description && app.has_license);
        assert_eq!(
            app.edges,
            [
                Edge {
                    name: "core".into(),
                    kept: true
                },
                Edge {
                    name: "tool".into(),
                    kept: true
                },
                Edge {
                    name: "core".into(),
                    kept: true
                },
            ]
        );
        assert!(!parsed.members[2].publishable);
    }

    #[test]
    fn another_registry_is_not_crates_io() {
        let metadata = serde_json::json!({
            "workspace_members": ["a 1"],
            "target_directory": "/r/target",
            "packages": [{
                "id": "a 1", "name": "a", "version": "1.0.0",
                "publish": ["private"], "dependencies": []
            }]
        });
        let parsed = parse_metadata(&metadata).expect("workspace");
        assert!(!parsed.members[0].publishable);
    }

    #[test]
    fn metadata_with_no_package_list_is_none() {
        assert_eq!(parse_metadata(&serde_json::json!({})), None);
    }

    #[test]
    fn sizes_fail_over_the_limit_and_warn_near_it() {
        let sizes = vec![
            ("small".to_owned(), Some(1024)),
            ("near".to_owned(), Some(WARN_CRATE_BYTES)),
            ("edge".to_owned(), Some(MAX_CRATE_BYTES)),
            ("big".to_owned(), Some(MAX_CRATE_BYTES + 1)),
            ("lost".to_owned(), None),
        ];
        let (failures, warnings) = size_findings(&sizes);
        assert_eq!(
            failures,
            [
                "big: the .crate file is 10485761 bytes. crates.io refuses a file over 10485760 bytes.",
                "lost: cargo package wrote no .crate file.",
            ]
        );
        assert_eq!(
            warnings,
            [
                "near: the .crate file is 8.00 MiB, near the crates.io limit of 10.00 MiB.",
                "edge: the .crate file is 10.00 MiB, near the crates.io limit of 10.00 MiB.",
            ]
        );
    }

    #[test]
    fn package_args_name_each_crate_and_pass_the_local_flags() {
        let crates = order(&["core", "app"]);
        assert_eq!(
            package_args(&crates, FULL),
            ["package", "--locked", "-p", "core", "-p", "app"]
        );
        let local = Args {
            no_verify: true,
            allow_dirty: true,
            ..FULL
        };
        assert_eq!(
            package_args(&crates, local),
            [
                "package",
                "--locked",
                "--no-verify",
                "--allow-dirty",
                "-p",
                "core",
                "-p",
                "app"
            ]
        );
    }

    #[test]
    fn the_crate_file_is_under_target_package() {
        assert_eq!(
            crate_file(Path::new("/t"), "core", "1.2.3"),
            PathBuf::from("/t/package/core-1.2.3.crate")
        );
    }

    #[test]
    fn a_full_run_packages_in_order_and_checks_each_size() {
        let ws = two_crates();
        let text = doc(&["core", "app"]);
        let mut seen = Vec::new();
        let report = evaluate(
            Some(&ws),
            Some(&text),
            FULL,
            |args| {
                seen.extend_from_slice(args);
                Ok(true)
            },
            |path| (path == Path::new("/t/package/core-1.0.0.crate")).then_some(WARN_CRATE_BYTES),
        )
        .expect("evaluate");
        assert_eq!(seen, ["package", "--locked", "-p", "core", "-p", "app"]);
        assert_eq!(report.code(), 1);
        let stderr = report.stderr();
        assert!(
            stderr.contains("  - app: cargo package wrote no .crate file.\n"),
            "{stderr}"
        );
        assert!(
            stderr.contains("warning: core: the .crate file is 8.00 MiB"),
            "{stderr}"
        );
    }

    #[test]
    fn a_warning_is_an_annotation_on_github_actions() {
        let ws = two_crates();
        let text = doc(&["core", "app"]);
        let annotate = Args {
            warnings: WarningStyle::Annotation,
            ..FULL
        };
        let report = evaluate(
            Some(&ws),
            Some(&text),
            annotate,
            |_| Ok(true),
            |_| Some(WARN_CRATE_BYTES),
        )
        .expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
        assert!(
            report
                .stderr()
                .starts_with("::warning::core: the .crate file is 8.00 MiB"),
            "{}",
            report.stderr()
        );
    }

    #[test]
    fn a_clean_full_run_passes() {
        let ws = two_crates();
        let text = doc(&["core", "app"]);
        let report =
            evaluate(Some(&ws), Some(&text), FULL, |_| Ok(true), |_| Some(1024)).expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
        assert_eq!(
            report.stdout(),
            "OK: 2 crates package for crates.io, in the documented order: core, app.\n"
        );
    }

    #[test]
    fn plan_only_never_packages() {
        let ws = two_crates();
        let text = doc(&["core", "app"]);
        let report = evaluate(
            Some(&ws),
            Some(&text),
            PLAN,
            |_| panic!("no package run"),
            |_| panic!("no size read"),
        )
        .expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
        assert_eq!(
            report.stdout(),
            "OK: the publish order lists each of the 2 publishable crates after its dependencies.\n"
        );
    }

    #[test]
    fn a_plan_finding_stops_before_the_package_run() {
        let ws = two_crates();
        let text = doc(&["app", "core"]);
        let report = evaluate(
            Some(&ws),
            Some(&text),
            FULL,
            |_| panic!("no package run"),
            |_| panic!("no size read"),
        )
        .expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .contains("  - app: in the publish order before core, which it depends on.\n")
        );
    }

    #[test]
    fn a_failed_package_run_fails_without_a_size_read() {
        let ws = two_crates();
        let text = doc(&["core", "app"]);
        let report = evaluate(
            Some(&ws),
            Some(&text),
            FULL,
            |_| Ok(false),
            |_| panic!("no size read"),
        )
        .expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(report.stderr().contains("cargo package failed."));
    }

    #[test]
    fn no_metadata_no_crate_or_no_page_fails_closed() {
        let text = doc(&["core"]);
        let empty = workspace(vec![unpublished("tool")]);
        let ws = two_crates();
        for (workspace, page) in [
            (None, Some(text.as_str())),
            (Some(&empty), Some(text.as_str())),
            (Some(&ws), None),
        ] {
            let report =
                evaluate(workspace, page, PLAN, |_| panic!("no run"), |_| None).expect("evaluate");
            assert_eq!(report.code(), 1);
            assert!(
                report.stderr().contains("The check fails closed."),
                "{}",
                report.stderr()
            );
        }
    }

    #[test]
    fn a_defective_page_fails() {
        let ws = two_crates();
        let report = evaluate(
            Some(&ws),
            Some("no markers"),
            PLAN,
            |_| panic!("no run"),
            |_| None,
        )
        .expect("evaluate");
        assert_eq!(report.code(), 1);
        assert!(
            report
                .stderr()
                .contains("no line `<!-- publish-order:start -->`")
        );
    }
}
