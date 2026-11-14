//! `cargo xtask dev build-typescript-client`: build and pack the TypeScript
//! client from `docs/openapi.json` (issue #1616).
//!
//! This is the port of the old `scripts/build-typescript-client.sh`.
//!
//! No running app is necessary. CI and the release job both run this
//! command. Progress goes to stderr. The only line on stdout is the tarball
//! path, so a caller can capture it and still see every error in the log.
//! Each child process writes its stdout to stderr for the same reason.

use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, ExitStatus, Stdio};

use serde_json::Value;
use smop::prelude::*;

use crate::util;

/// The files that the package must hold.
///
/// It must hold the build output. It must also ship both license texts
/// (issue #1990). npm adds a file named `LICENSE-MIT` only when `files` lists
/// it.
const REQUIRED: [&str; 5] = [
    "package/dist/index.js",
    "package/dist/index.d.ts",
    "package/dist/harvest-api.d.ts",
    "package/LICENSE-MIT",
    "package/LICENSE-APACHE",
];

/// The `package.json` of the clean consumer.
const CONSUMER_PACKAGE: &str =
    "{ \"name\": \"consumer-smoke\", \"private\": true, \"type\": \"module\" }\n";

/// The import check that the clean consumer runs.
const IMPORT_CHECK: &str = "const m = await import(\"autumn-harvest-client\"); \
     if (typeof m.createHarvestClient !== \"function\") process.exit(1);";

/// The seconds to wait before the one retry of a registry call.
const RETRY_DELAY_SECS: u64 = 10;

/// Arguments of `cargo xtask dev build-typescript-client`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// A step that failed. Its exit code becomes the exit code of this command,
/// as `set -e` did for the shell script.
#[derive(Debug)]
struct Failed(u8);

impl fmt::Display for Failed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a step failed with exit code {}", self.0)
    }
}

impl std::error::Error for Failed {}

/// Runs `cargo xtask dev build-typescript-client`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {} = args;
    match build() {
        Ok(tarball) => {
            println!("{}", tarball.display());
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => match error.downcast_ref::<Failed>() {
            Some(Failed(code)) => Ok(ExitCode::from(*code)),
            None => Err(error),
        },
    }
}

/// Builds, checks and packs the client, and returns the tarball path.
fn build() -> Result<PathBuf> {
    let client = util::repo_path("clients/typescript");

    retry(
        &client,
        &["ci", "--no-audit", "--no-fund", "--loglevel=error"],
    )?;
    npm(&client, &["run", "typecheck"])?;
    npm(&client, &["test"])?;
    npm(&client, &["run", "build"])?;

    remove_stale_tarballs(&client)?;
    let pack = capture(
        Command::new("npm")
            .args(["pack", "--json", "--loglevel=error"])
            .current_dir(&client),
    )?;
    let tarball = packed_filename(&pack)?;

    let listing = capture(
        Command::new("tar")
            .arg("-tzf")
            .arg(&tarball)
            .current_dir(&client),
    )?;
    if let Some(problem) = listing_problem(&tarball, &listing) {
        eprintln!("error: {problem}");
        return Err(Failed(1).into());
    }

    // Install the tarball into a clean consumer, then type-check and import
    // it.
    let consumer = fs::temp_dir()?;
    let dir = consumer.path();
    for file in ["index.ts", "tsconfig.json"] {
        fs::copy(client.join("consumer-smoke").join(file), dir.join(file))?;
    }
    fs::write_string(dir.join("package.json"), CONSUMER_PACKAGE)?;
    let packed = client.join(&tarball);
    let prefix = dir.to_string_lossy();
    let packed_arg = packed.to_string_lossy();
    retry(
        &client,
        &[
            "install",
            "--prefix",
            &prefix,
            "--no-audit",
            "--no-fund",
            "--loglevel=error",
            &packed_arg,
        ],
    )?;
    step(
        Command::new(client.join("node_modules/.bin/tsc"))
            .arg("-p")
            .arg(dir.join("tsconfig.json"))
            .current_dir(&client),
    )?;
    step(
        Command::new("node")
            .args(["--input-type=module", "-e", IMPORT_CHECK])
            .current_dir(dir),
    )?;

    Ok(packed)
}

/// Runs `npm` with `args` in `dir`, and once more after a failure.
///
/// The registry is the one step here that fails for unrelated reasons. So
/// each npm network call gets one retry. A type or test error gets none.
fn retry(dir: &Path, args: &[&str]) -> Result<()> {
    if npm(dir, args).is_ok() {
        return Ok(());
    }
    eprintln!("retrying after a failure: {}", retry_line(args));
    time::sleep_secs(RETRY_DELAY_SECS);
    npm(dir, args)
}

/// The command line that the retry message names, as `$*` printed it.
fn retry_line(args: &[&str]) -> String {
    std::iter::once("npm")
        .chain(args.iter().copied())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Runs `npm` with `args` in `dir` as one step.
fn npm(dir: &Path, args: &[&str]) -> Result<()> {
    step(Command::new("npm").args(args).current_dir(dir))
}

/// Runs `command` with its stdout on stderr. A failure is a `Failed` error.
fn step(command: &mut Command) -> Result<()> {
    let status = command
        .stdin(Stdio::inherit())
        .stdout(std::io::stderr())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("starting {command:?}"))?;
    check(status)
}

/// Runs `command` and returns its stdout. Its stderr passes through.
fn capture(command: &mut Command) -> Result<String> {
    let output = command
        .stdin(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .with_context(|| format!("starting {command:?}"))?;
    check(output.status)?;
    String::from_utf8(output.stdout).with_context(|| format!("reading the output of {command:?}"))
}

/// Maps a failed exit status to a `Failed` error, as a POSIX shell does.
fn check(status: ExitStatus) -> Result<()> {
    if status.success() {
        return Ok(());
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return Err(Failed(u8::try_from(128 + signal).unwrap_or(u8::MAX)).into());
        }
    }
    let code = status.code().map_or(1, |code| {
        u8::try_from(code.rem_euclid(256)).unwrap_or(u8::MAX)
    });
    Err(Failed(code).into())
}

/// Removes each `autumn-harvest-client-*.tgz` in `dir`, as `rm -f` did.
fn remove_stale_tarballs(dir: &Path) -> Result<()> {
    let entries = std::fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| format!("listing {}", dir.display()))?;
        if is_stale_tarball(&entry.file_name().to_string_lossy()) {
            fs::remove(entry.path())?;
        }
    }
    Ok(())
}

/// True for a file name that `autumn-harvest-client-*.tgz` matches.
///
/// The match is case-sensitive, as the shell glob was.
fn is_stale_tarball(name: &str) -> bool {
    name.strip_prefix("autumn-harvest-client-")
        .and_then(|rest| rest.strip_suffix(".tgz"))
        .is_some()
}

/// Reads the tarball name from the JSON that `npm pack --json` prints.
fn packed_filename(pack: &str) -> Result<String> {
    let packed: Value = serde_json::from_str(pack).context("`npm pack --json` printed no JSON")?;
    packed
        .get(0)
        .and_then(|entry| entry.get("filename"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("`npm pack --json` named no filename")
}

/// Checks a `tar -tzf` listing of the package.
///
/// The package must hold the build output and the license texts, and nothing
/// from `src` or `test`. It returns the first problem, if any.
fn listing_problem(tarball: &str, listing: &str) -> Option<String> {
    for required in REQUIRED {
        if !listing.lines().any(|line| line == required) {
            return Some(format!("{tarball} has no {required}"));
        }
    }
    let holds_sources = listing
        .lines()
        .any(|line| line.starts_with("package/src/") || line.starts_with("package/test/"));
    holds_sources.then(|| format!("{tarball} holds source or test files"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = "package/package.json\npackage/dist/index.js\npackage/dist/index.d.ts\n\
                        package/dist/harvest-api.d.ts\npackage/LICENSE-MIT\npackage/LICENSE-APACHE\n";

    #[test]
    fn a_complete_package_passes() {
        assert_eq!(listing_problem("c.tgz", GOOD), None);
    }

    #[test]
    fn a_missing_license_fails() {
        let listing = GOOD.replace("package/LICENSE-MIT\n", "");
        assert_eq!(
            listing_problem("c.tgz", &listing).as_deref(),
            Some("c.tgz has no package/LICENSE-MIT")
        );
    }

    #[test]
    fn a_partial_name_is_not_a_match() {
        let listing = GOOD.replace("package/dist/index.js\n", "package/dist/index.js.map\n");
        assert_eq!(
            listing_problem("c.tgz", &listing).as_deref(),
            Some("c.tgz has no package/dist/index.js")
        );
    }

    #[test]
    fn source_or_test_files_fail() {
        for extra in ["package/src/index.ts", "package/test/a.test.ts"] {
            let listing = format!("{GOOD}{extra}\n");
            assert_eq!(
                listing_problem("c.tgz", &listing).as_deref(),
                Some("c.tgz holds source or test files")
            );
        }
    }

    #[test]
    fn the_pack_json_names_the_tarball() {
        let pack = r#"[{"id":"autumn-harvest-client@0.9.0","filename":"autumn-harvest-client-0.9.0.tgz"}]"#;
        assert_eq!(
            packed_filename(pack).unwrap(),
            "autumn-harvest-client-0.9.0.tgz"
        );
        assert!(packed_filename("[]").is_err());
        assert!(packed_filename("npm ERR!").is_err());
    }

    #[test]
    fn only_client_tarballs_are_stale() {
        assert!(is_stale_tarball("autumn-harvest-client-0.9.0.tgz"));
        assert!(is_stale_tarball("autumn-harvest-client-.tgz"));
        assert!(!is_stale_tarball("autumn-harvest-client.tgz"));
        assert!(!is_stale_tarball("autumn-harvest-client-0.9.0.tgz.bak"));
        assert!(!is_stale_tarball("package.json"));
    }

    #[test]
    fn the_retry_message_names_the_npm_call() {
        assert_eq!(retry_line(&["ci", "--no-audit"]), "npm ci --no-audit");
    }
}
