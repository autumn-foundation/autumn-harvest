//! `cargo xtask dev regenerate-openapi`: write both copies of the `OpenAPI`
//! document from `docs/api-contract.json`.
//!
//! This is the port of the old `scripts/regenerate-openapi.sh`.
//!
//! - `autumn-harvest-plugin/openapi.json` is compact. The crate compiles it
//!   in and serves it.
//! - `docs/openapi.json` is pretty-printed, for reading.
//!
//! `cargo test -p autumn-harvest-plugin --test openapi_spec` fails on drift.

use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, ExitStatus, Stdio};

use smop::prelude::*;

use crate::util;

/// The source of both documents, relative to the repository root.
const CONTRACT: &str = "docs/api-contract.json";

/// The pretty-printed copy, relative to the repository root.
const PRETTY: &str = "docs/openapi.json";

/// The compact copy that the crate compiles in.
const COMPACT: &str = "autumn-harvest-plugin/openapi.json";

/// Arguments of `cargo xtask dev regenerate-openapi`.
///
/// The command takes no argument, so the type is `Copy`.
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

/// Runs `cargo xtask dev regenerate-openapi`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {} = args;
    match regenerate(&util::repo_root()) {
        Ok(()) => {
            println!("wrote {PRETTY} and {COMPACT}");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => match error.downcast_ref::<Failed>() {
            Some(Failed(code)) => Ok(ExitCode::from(*code)),
            None => Err(error),
        },
    }
}

/// Builds the emitter and writes both documents under `root`.
fn regenerate(root: &Path) -> Result<()> {
    let compact = root.join(COMPACT);
    // The crate compiles in the compact copy, so the example cannot build
    // without it. Seed it on a first run. The real content lands below.
    if !compact.is_file() {
        fs::write_string(&compact, "{}\n")?;
    }

    let status = sh::cargo()
        .args([
            "build",
            "--quiet",
            "-p",
            "autumn-harvest-plugin",
            "--example",
            "emit_openapi",
        ])
        .dir(root)
        .status()?;
    check(status)?;
    let emit = emitter_path(root, std::env::var_os("CARGO_TARGET_DIR").as_deref());

    let pretty = root.join(PRETTY);
    let pretty_tmp = with_tmp_suffix(&pretty);
    let compact_tmp = with_tmp_suffix(&compact);
    emit_to(&emit, root, &[], &pretty_tmp)?;
    emit_to(&emit, root, &["--compact"], &compact_tmp)?;
    fs::rename(&pretty_tmp, &pretty)?;
    fs::rename(&compact_tmp, &compact)?;
    Ok(())
}

/// The path of the built emitter, as `${CARGO_TARGET_DIR:-target}` gave it.
///
/// A relative target directory is relative to `root`, because the shell
/// script ran from there.
fn emitter_path(root: &Path, target_dir: Option<&std::ffi::OsStr>) -> PathBuf {
    let target = target_dir
        .filter(|dir| !dir.is_empty())
        .map_or_else(|| PathBuf::from("target"), PathBuf::from);
    root.join(target).join("debug/examples/emit_openapi")
}

/// Returns `path` with `.tmp` appended to its file name.
fn with_tmp_suffix(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

/// Runs the emitter on the contract with `extra` and writes its stdout to
/// `out`.
fn emit_to(emit: &Path, root: &Path, extra: &[&str], out: &Path) -> Result<()> {
    let file = std::fs::File::create(out).with_context(|| format!("creating {}", out.display()))?;
    let status = Command::new(emit)
        .arg(CONTRACT)
        .args(extra)
        .current_dir(root)
        .stdin(Stdio::inherit())
        .stdout(Stdio::from(file))
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("starting {}", emit.display()))?;
    check(status)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_emitter_defaults_to_the_target_directory() {
        let root = Path::new("/repo");
        assert_eq!(
            emitter_path(root, None),
            PathBuf::from("/repo/target/debug/examples/emit_openapi")
        );
        assert_eq!(
            emitter_path(root, Some(std::ffi::OsStr::new(""))),
            PathBuf::from("/repo/target/debug/examples/emit_openapi")
        );
    }

    #[test]
    fn the_emitter_follows_cargo_target_dir() {
        let root = Path::new("/repo");
        assert_eq!(
            emitter_path(root, Some(std::ffi::OsStr::new("/cache/t"))),
            PathBuf::from("/cache/t/debug/examples/emit_openapi")
        );
        assert_eq!(
            emitter_path(root, Some(std::ffi::OsStr::new("out"))),
            PathBuf::from("/repo/out/debug/examples/emit_openapi")
        );
    }

    #[test]
    fn the_temporary_copy_sits_next_to_the_document() {
        assert_eq!(
            with_tmp_suffix(Path::new("/repo/docs/openapi.json")),
            PathBuf::from("/repo/docs/openapi.json.tmp")
        );
    }

    #[test]
    fn a_failed_emitter_keeps_both_documents() {
        let dir = fs::temp_dir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("docs")).unwrap();
        fs::create_dir_all(root.join("autumn-harvest-plugin")).unwrap();
        fs::write_string(root.join(PRETTY), "pretty\n").unwrap();
        let out = with_tmp_suffix(&root.join(PRETTY));
        let error = emit_to(Path::new("false"), root, &[], &out).unwrap_err();
        assert!(matches!(error.downcast_ref::<Failed>(), Some(Failed(1))));
        assert_eq!(fs::read_string(root.join(PRETTY)).unwrap(), "pretty\n");
    }
}
