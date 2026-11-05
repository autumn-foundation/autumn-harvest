//! `cargo xtask audit tracked-artifacts`: the port of
//! `docs/audits/tracked-artifacts.py`.
//!
//! It fails on a tracked build artifact or patch leftover (issue #1831).
//!
//! A tracked file fails when one of these rules matches:
//!
//! 1. Its name ends in a patch leftover suffix, `.orig` or `.rej`.
//! 2. Its content is binary. The rule is the one git uses: a NUL byte in the
//!    first 8000 bytes.
//!
//! A binary file that the repository needs goes in `ALLOWED`, with a reason.
//! An `ALLOWED` entry that is not tracked is also a finding, so the list
//! stays exact.
//!
//! Usage: `cargo xtask audit tracked-artifacts [--self-test]`.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::process::ExitCode;

use smop::prelude::*;

use super::root_files::{TrackedPath, tracked_paths};
use crate::util::repo_root;

/// Arguments of `cargo xtask audit tracked-artifacts`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {
    /// Run the fixtures instead of the real tree.
    #[arg(long)]
    pub self_test: bool,
}

const LEFTOVER_SUFFIXES: &[&str] = &[".orig", ".rej"];

// Git reads this many bytes to decide if a file is binary.
const SNIFF_BYTES: usize = 8000;

/// Each binary file that the repository needs, with the reason.
const ALLOWED: &[(&str, &str)] = &[(
    "autumn-harvest/examples/wasm-guests/echo.wasm",
    "a compiled guest that a test runs; see its README",
)];

/// Returns one message for each rule that a tracked path breaks.
///
/// `read_head` returns the first bytes of a path, or `None` to skip it. An
/// error from `read_head` stops the scan, as the Python traceback did.
fn findings(
    paths: &[TrackedPath],
    read_head: &dyn Fn(&TrackedPath) -> Result<Option<Vec<u8>>>,
    allowed: &[&str],
) -> Result<Vec<String>> {
    let mut found = Vec::new();
    for path in paths {
        if LEFTOVER_SUFFIXES.iter().any(|s| path.text.ends_with(s)) {
            found.push(format!("{}: patch leftover", path.text));
            continue;
        }
        if allowed.contains(&path.text.as_str()) {
            continue;
        }
        let binary =
            read_head(path)?.is_some_and(|head| head.iter().take(SNIFF_BYTES).any(|b| *b == 0));
        if binary {
            found.push(format!("{}: binary file", path.text));
        }
    }
    let tracked: BTreeSet<&str> = paths.iter().map(|p| p.text.as_str()).collect();
    let sorted: BTreeSet<&str> = allowed.iter().copied().collect();
    for path in sorted {
        if !tracked.contains(path) {
            found.push(format!("{path}: in ALLOWED, but not tracked"));
        }
    }
    Ok(found)
}

/// Turns the raw path bytes of `git ls-files -z` into a path.
///
/// On Unix a path is bytes, so each byte is kept. Git for Windows prints
/// paths as UTF-8, so the bytes decode as UTF-8 there.
#[cfg(unix)]
fn os_path(raw: &[u8]) -> std::path::PathBuf {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;
    OsStr::from_bytes(raw).into()
}

/// Turns the raw path bytes of `git ls-files -z` into a path.
///
/// Git for Windows prints paths as UTF-8, so the bytes decode as UTF-8.
#[cfg(not(unix))]
fn os_path(raw: &[u8]) -> std::path::PathBuf {
    String::from_utf8_lossy(raw).into_owned().into()
}

/// Returns the first bytes of a regular file, or `None` to skip it.
///
/// A path that cannot be examined, such as a file that the work tree lacks,
/// is skipped, as Python's `islink` and `is_file` skip it. A regular file
/// that cannot be read is an error. Python raised there, so the gate failed
/// closed. A silent skip would pass a binary file that it never read.
fn read_head(path: &TrackedPath) -> Result<Option<Vec<u8>>> {
    let full = repo_root().join(os_path(&path.raw));
    // A symlink holds a path, and a gitlink is a directory.
    let Ok(meta) = std::fs::symlink_metadata(&full) else {
        return Ok(None);
    };
    if meta.file_type().is_symlink() || !meta.is_file() {
        return Ok(None);
    }
    let file = std::fs::File::open(&full).with_context(|| format!("opening {}", full.display()))?;
    let mut head = Vec::with_capacity(SNIFF_BYTES);
    file.take(SNIFF_BYTES as u64)
        .read_to_end(&mut head)
        .with_context(|| format!("reading {}", full.display()))?;
    Ok(Some(head))
}

/// Checks the real tree and prints the findings.
fn check() -> Result<ExitCode> {
    let paths = tracked_paths()?;
    let allowed: Vec<&str> = ALLOWED.iter().map(|(path, _)| *path).collect();
    let found = findings(&paths, &read_head, &allowed)?;
    for message in &found {
        println!("{message}");
    }
    println!(
        "tracked-artifacts: {} files, {} findings",
        paths.len(),
        found.len()
    );
    if !found.is_empty() {
        println!(
            "Fix: `git rm` the file. If the repository needs a binary, add it \
             to ALLOWED in xtask/src/audit/tracked_artifacts.rs with a reason."
        );
    }
    Ok(crate::util::gate_exit(found.is_empty()))
}

/// Exercises each failure case on fixtures.
fn self_test() -> Result<()> {
    let mut late_nul = vec![b'a'; SNIFF_BYTES];
    late_nul.push(0);
    let files: BTreeMap<&str, Option<Vec<u8>>> = BTreeMap::from([
        ("src/lib.rs", Some(b"fn main() {}\n".to_vec())),
        ("empty.txt", Some(Vec::new())),
        ("late-nul.txt", Some(late_nul)),
        ("src/lib.rs.orig", Some(b"fn main() {}\n".to_vec())),
        ("fix.rej", Some(b"@@ -1 +1 @@\n".to_vec())),
        ("test_debug", Some(b"\x7fELF\x02\x01\x01\0\0\0".to_vec())),
        ("guest.wasm", Some(b"\0asm\x01\0\0\0".to_vec())),
        ("link", None),
    ]);
    let lookup = |p: &TrackedPath| Ok(files.get(p.text.as_str()).cloned().flatten());
    let check = |names: &[&str], allow: &[&str]| -> Result<Vec<String>> {
        let paths: Vec<TrackedPath> = names
            .iter()
            .map(|n| TrackedPath::from_bytes(n.as_bytes()))
            .collect();
        findings(&paths, &lookup, allow)
    };

    let found = check(&["src/lib.rs", "empty.txt", "late-nul.txt", "link"], &[])?;
    ensure!(found.is_empty(), "{found:?}");
    let found = check(&["guest.wasm"], &["guest.wasm"])?;
    ensure!(found.is_empty(), "{found:?}");
    let found = check(&["guest.wasm"], &[])?;
    ensure!(
        found.first().is_some_and(|m| m.contains("binary")),
        "{found:?}"
    );

    let found = check(&["src/lib.rs.orig", "fix.rej"], &[])?;
    ensure!(found.len() == 2, "{found:?}");
    ensure!(
        found.iter().all(|m| m.contains("patch leftover")),
        "{found:?}"
    );

    let found = check(&["test_debug"], &[])?;
    ensure!(found.len() == 1 && found[0].contains("binary"), "{found:?}");

    let found = check(&["src/lib.rs"], &["gone.bin"])?;
    ensure!(
        found.len() == 1 && found[0].contains("gone.bin"),
        "{found:?}"
    );

    // ALLOWED exempts a binary, never a patch leftover.
    let found = check(&["src/lib.rs.orig"], &["src/lib.rs.orig"])?;
    ensure!(
        found.len() == 1 && found[0].contains("patch leftover"),
        "{found:?}"
    );
    Ok(())
}

/// Runs `cargo xtask audit tracked-artifacts`.
pub fn run(args: Args) -> Result<ExitCode> {
    if !args.self_test {
        return check();
    }
    match self_test() {
        Ok(()) => {
            println!("tracked-artifacts self-test: OK");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            eprintln!("tracked-artifacts self-test failed: {error:#}");
            Ok(ExitCode::FAILURE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_test_passes() -> Result<()> {
        self_test()
    }

    #[test]
    fn a_read_error_fails_the_scan() {
        let paths = [TrackedPath::from_text("unreadable.bin")];
        let failing = |_: &TrackedPath| -> Result<Option<Vec<u8>>> { bail!("denied") };
        assert!(findings(&paths, &failing, &[]).is_err());
    }

    #[test]
    fn the_head_reader_skips_a_missing_file() {
        let path = TrackedPath::from_text("no/such/file.bin");
        assert_eq!(read_head(&path).unwrap(), None);
    }

    #[test]
    fn the_head_reader_reads_a_real_file() {
        let head = read_head(&TrackedPath::from_text("Cargo.toml"))
            .unwrap()
            .unwrap();
        assert!(!head.is_empty() && !head.contains(&0));
    }
}
