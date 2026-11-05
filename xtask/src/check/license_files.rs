//! `cargo xtask check license-files`: the port of
//! `scripts/check-license-files.sh`.
//!
//! The guard fails if a license or policy file is missing from the repository
//! root. It also fails if a license file is missing from a publishable package
//! (issue #1990).
//!
//! The workspace declares `license = "MIT OR Apache-2.0"`. Corporate review
//! rejects a crate that does not ship the license text. Cargo does not copy a
//! root `LICENSE-*` file into a crate, so each crate directory holds a
//! symlink to it.
//!
//! The guard checks these things:
//!
//! - The root holds `LICENSE-MIT`, `LICENSE-APACHE`, `SECURITY.md` and
//!   `CONTRIBUTING.md`.
//! - Each publishable workspace crate declares `MIT OR Apache-2.0`.
//! - `cargo package --list` for that crate lists both license files.
//! - The license files in the crate directory match the root, byte for byte.
//!   A copy with different bytes fails. A symlink that a checkout turned into
//!   a text file also fails.
//! - The TypeScript client holds copies of both license files with the root
//!   bytes. npm does not pack a symlink.
//!   `cargo xtask dev build-typescript-client` checks that the tarball holds
//!   them.
//!
//! The guard reads the crate list from `cargo metadata`. It checks a new
//! crate with no edit. It skips a crate with `publish = false`.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde_json::Value;
use smop::prelude::*;

use super::support::{self, Report};
use crate::util;

const LICENSE_FILES: [&str; 2] = ["LICENSE-MIT", "LICENSE-APACHE"];
const POLICY_FILES: [&str; 2] = ["SECURITY.md", "CONTRIBUTING.md"];
const EXPECTED_LICENSE: &str = "MIT OR Apache-2.0";
const NPM_PACKAGE_DIR: &str = "clients/typescript";

/// Arguments of `cargo xtask check license-files`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// One publishable workspace crate.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Package {
    name: String,
    license: String,
    dir: PathBuf,
}

/// Runs `cargo xtask check license-files`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let root = util::repo_root();
    let metadata = sh::cargo()
        .args(["metadata", "--no-deps", "--format-version=1"])
        .dir(&root)
        .output_raw()?;
    let parsed = if metadata.status.success() {
        serde_json::from_slice::<Value>(&metadata.stdout).ok()
    } else {
        None
    };
    let packages = parsed.as_ref().and_then(publishable);
    let report = evaluate(&root, packages.as_deref(), |name| {
        support::cargo_merged(&root, &["package", "--list", "--allow-dirty", "-p", name])
    })?;
    Ok(report.emit())
}

/// Lists the publishable workspace members of `cargo metadata`.
///
/// `publish = false` shows as an empty list, and null means any registry.
/// `None` means that the metadata has no package list.
fn publishable(metadata: &Value) -> Option<Vec<Package>> {
    let members: Vec<&str> = metadata
        .get("workspace_members")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let mut packages = Vec::new();
    for package in metadata.get("packages")?.as_array()? {
        let id = package.get("id")?.as_str()?;
        if !members.contains(&id) {
            continue;
        }
        if package
            .get("publish")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            continue;
        }
        let manifest = PathBuf::from(package.get("manifest_path")?.as_str()?);
        packages.push(Package {
            name: package.get("name")?.as_str()?.to_owned(),
            license: package
                .get("license")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            dir: manifest.parent().map(Path::to_path_buf).unwrap_or_default(),
        });
    }
    Some(packages)
}

/// Reports whether two files hold the same bytes, like `cmp -s`.
///
/// A file that cannot be read never matches.
fn same_bytes(left: &Path, right: &Path) -> bool {
    match (std::fs::read(left), std::fs::read(right)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// Checks the files under `root`. `list` runs `cargo package --list`.
fn evaluate(
    root: &Path,
    packages: Option<&[Package]>,
    mut list: impl FnMut(&str) -> Result<(bool, String)>,
) -> Result<Report> {
    let mut failures: Vec<String> = Vec::new();

    for file in LICENSE_FILES.into_iter().chain(POLICY_FILES) {
        let present = std::fs::metadata(root.join(file)).is_ok_and(|meta| meta.len() > 0);
        if !present {
            failures.push(format!("repository root: {file} is missing or empty"));
        }
    }

    for file in LICENSE_FILES {
        if !same_bytes(&root.join(file), &root.join(NPM_PACKAGE_DIR).join(file)) {
            failures.push(format!(
                "{NPM_PACKAGE_DIR}: {file} is missing or does not match the root {file}"
            ));
        }
    }

    let mut report = Report::new();
    // A failed `cargo metadata` must not print OK for a workspace that the
    // guard never read. So the guard fails closed here.
    let Some(packages) = packages.filter(|found| !found.is_empty()) else {
        report.fail(
            "cargo xtask check license-files: cargo metadata or the parser failed, or the workspace has no publishable crate. The check fails closed.",
        );
        return Ok(report);
    };

    for package in packages {
        let name = &package.name;
        if package.license != EXPECTED_LICENSE {
            failures.push(format!(
                "{name}: license is \"{}\", expected \"{EXPECTED_LICENSE}\"",
                package.license
            ));
        }

        let (listed, listing) = list(name)?;
        if !listed {
            failures.push(format!("{name}: cargo package --list failed: {listing}"));
            continue;
        }

        for file in LICENSE_FILES {
            if !support::lines(&listing).contains(&file) {
                failures.push(format!("{name}: cargo package --list does not show {file}"));
            } else if !same_bytes(&root.join(file), &package.dir.join(file)) {
                failures.push(format!(
                    "{name}: {}/{file} does not match the root {file}",
                    package.dir.display()
                ));
            }
        }
    }

    if !failures.is_empty() {
        report.err("Legal or policy files are missing or wrong:");
        for failure in &failures {
            report.err(format!("  - {failure}"));
        }
        report.err("");
        report.err("Fix: add the missing root file. In a crate directory, add a relative");
        report.err("symlink to each root license file, for example:");
        report.err("  ln -s ../LICENSE-MIT autumn-harvest/LICENSE-MIT");
        report.err(format!(
            "In {NPM_PACKAGE_DIR}, copy the root license files. Set the crate"
        ));
        report.fail(format!("license to \"{EXPECTED_LICENSE}\"."));
        return Ok(report);
    }

    report.out(
        "OK: the root holds the license and policy files. Each publishable crate and the TypeScript client ship both license texts.",
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LISTING: &str = "Cargo.toml\nLICENSE-APACHE\nLICENSE-MIT\nsrc/lib.rs";

    fn fixture() -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        let root = dir.path();
        for dir in [NPM_PACKAGE_DIR, "core"] {
            std::fs::create_dir_all(root.join(dir)).expect("mkdir");
        }
        for file in LICENSE_FILES {
            for at in [".", NPM_PACKAGE_DIR, "core"] {
                std::fs::write(root.join(at).join(file), format!("{file} text\n"))
                    .expect("license");
            }
        }
        for file in POLICY_FILES {
            std::fs::write(root.join(file), "policy\n").expect("policy");
        }
        dir
    }

    fn core(root: &Path, license: &str) -> Vec<Package> {
        vec![Package {
            name: "core".into(),
            license: license.into(),
            dir: root.join("core"),
        }]
    }

    #[test]
    fn a_complete_tree_passes() {
        let dir = fixture();
        let packages = core(dir.path(), EXPECTED_LICENSE);
        let report = evaluate(dir.path(), Some(&packages), |_| Ok((true, LISTING.into())))
            .expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
    }

    #[test]
    fn each_defect_is_one_bullet() {
        let dir = fixture();
        std::fs::write(dir.path().join("SECURITY.md"), "").expect("empty");
        std::fs::write(
            dir.path().join(NPM_PACKAGE_DIR).join("LICENSE-MIT"),
            "other",
        )
        .expect("npm");
        std::fs::write(dir.path().join("core/LICENSE-APACHE"), "other").expect("crate");
        let packages = core(dir.path(), "MIT");
        let report = evaluate(dir.path(), Some(&packages), |_| {
            Ok((true, "Cargo.toml\nLICENSE-APACHE".into()))
        })
        .expect("evaluate");
        assert_eq!(report.code(), 1);
        let stderr = report.stderr();
        assert!(stderr.contains("  - repository root: SECURITY.md is missing or empty\n"));
        assert!(stderr.contains(
            "  - clients/typescript: LICENSE-MIT is missing or does not match the root LICENSE-MIT\n"
        ));
        assert!(stderr.contains("  - core: license is \"MIT\", expected \"MIT OR Apache-2.0\"\n"));
        assert!(stderr.contains("  - core: cargo package --list does not show LICENSE-MIT\n"));
        assert!(stderr.contains("/core/LICENSE-APACHE does not match the root LICENSE-APACHE\n"));
        assert!(stderr.ends_with("license to \"MIT OR Apache-2.0\".\n"));
    }

    #[test]
    fn a_failed_listing_skips_the_file_checks() {
        let dir = fixture();
        let packages = core(dir.path(), EXPECTED_LICENSE);
        let report = evaluate(dir.path(), Some(&packages), |_| Ok((false, "boom".into())))
            .expect("evaluate");
        assert!(
            report
                .stderr()
                .contains("  - core: cargo package --list failed: boom\n\n")
        );
    }

    #[test]
    fn no_metadata_or_no_crate_fails_closed() {
        let dir = fixture();
        for packages in [None, Some(&[][..])] {
            let report =
                evaluate(dir.path(), packages, |_| panic!("no listing")).expect("evaluate");
            assert_eq!(report.code(), 1);
            assert!(report.stderr().contains("The check fails closed."));
        }
    }

    #[test]
    fn publishable_skips_unpublished_and_foreign_packages() {
        let metadata = serde_json::json!({
            "workspace_members": ["a 1", "b 1"],
            "packages": [
                { "id": "a 1", "name": "a", "license": null, "publish": null, "manifest_path": "/r/a/Cargo.toml" },
                { "id": "b 1", "name": "b", "license": "MIT", "publish": [], "manifest_path": "/r/b/Cargo.toml" },
                { "id": "c 1", "name": "c", "license": "MIT", "publish": null, "manifest_path": "/r/c/Cargo.toml" }
            ]
        });
        assert_eq!(
            publishable(&metadata).expect("packages"),
            vec![Package {
                name: "a".into(),
                license: String::new(),
                dir: PathBuf::from("/r/a")
            }]
        );
    }
}
