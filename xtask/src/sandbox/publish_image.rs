//! `cargo xtask sandbox publish-image`: the port of `scripts/sandbox/publish-image.sh`.
//!
//! Move a built guest image from `STAGE_DIR` into `OUT_DIR`.
//!
//! Each file of the stage replaces its namesake in `OUT_DIR` by a rename.
//! A hard link keeps each old file first. When a rename fails, each file
//! that already moved gets its old version back, so `OUT_DIR` keeps a whole
//! image. `STAGE_DIR` must be on the file system of `OUT_DIR`.
//!
//! A jailed launcher pins the image under a shared flock(1) of `OUT_DIR`.
//! The publish holds the lock exclusively, so a launcher never pins half an
//! image.
//!
//! Usage:
//!
//! ```sh
//! cargo xtask sandbox publish-image STAGE_DIR OUT_DIR
//! ```
//!
//! The exit code is 0 on a publish and 1 on any failure.

use std::ffi::OsString;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use smop::prelude::*;

/// The files of a guest image, in the order that the publish moves them.
pub const FILES: [&str; 3] = ["rootfs.ext4", "vmlinux", "image.sha256"];

/// How long the publish waits for the pins of the launchers to end.
const LOCK_WAIT: Duration = Duration::from_secs(60);

/// Arguments of `cargo xtask sandbox publish-image`.
#[derive(Debug, Args)]
pub struct Args {
    /// The directory that holds the new image files.
    pub stage: Option<OsString>,
    /// The directory that holds the published image.
    pub out: Option<OsString>,
}

/// Returns a path argument. An empty value counts as missing, as in
/// `${1:?}` of the old script.
pub fn path_arg(arg: Option<OsString>) -> Option<PathBuf> {
    arg.filter(|value| !value.is_empty()).map(PathBuf::from)
}

/// Runs `cargo xtask sandbox publish-image`.
///
/// The group dispatcher fixes the `Result` return type.
#[allow(clippy::unnecessary_wraps)]
pub fn run(args: Args) -> Result<ExitCode> {
    let (Some(stage), Some(out)) = (path_arg(args.stage), path_arg(args.out)) else {
        eprintln!("usage: cargo xtask sandbox publish-image STAGE_DIR OUT_DIR");
        return Ok(ExitCode::FAILURE);
    };
    Ok(match publish(&stage, &out) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(err) => {
            eprintln!("publish-image: {err:#}");
            ExitCode::FAILURE
        }
    })
}

/// Moves the image files of `stage` into `out`.
///
/// Returns `false` after a failure that the function reports itself. The old
/// image then stays in `out`, or its files stay in a named backup.
///
/// # Errors
///
/// Returns an error when a step before the first rename fails. No file of
/// `out` has changed then.
pub fn publish(stage: &Path, out: &Path) -> Result<bool> {
    // The jailed VMM reads each file as the jail uid. A build under a strict
    // umask, such as 077, makes files that only the owner can read. So each
    // file gets mode 0644 before it moves.
    for file in FILES {
        make_readable(&stage.join(file))?;
    }

    let dir = File::open(out).with_context(|| format!("cannot open {}", out.display()))?;
    if !lock_exclusive(&dir, out)? {
        eprintln!(
            "publish-image: {} stayed locked; nothing is published",
            out.display()
        );
        return Ok(false);
    }

    let backup = smop::fs::TempDir::with_prefix_in(".old.", out)
        .with_context(|| format!("cannot make a backup directory in {}", out.display()))?;
    // A target that cannot be linked, such as a directory, stops the publish
    // before any rename.
    for file in FILES {
        let target = out.join(file);
        if target.exists() {
            std::fs::hard_link(&target, backup.path().join(file))
                .with_context(|| format!("cannot link {} into the backup", target.display()))?;
        }
    }

    let mut moved: Vec<&str> = Vec::new();
    for file in FILES {
        let (from, to) = (stage.join(file), out.join(file));
        let Err(err) = std::fs::rename(&from, &to) else {
            moved.push(file);
            continue;
        };
        eprintln!(
            "publish-image: cannot move {} to {}: {err}",
            from.display(),
            to.display()
        );
        // A failed restore keeps the backup, so the old files are not lost.
        let restored = restore(&moved, backup.path(), out);
        if !restored {
            let kept = backup.keep();
            eprintln!(
                "publish-image: could not put back the old image; its files are in {}",
                kept.display()
            );
            return Ok(false);
        }
        eprintln!(
            "publish-image: could not publish {file}; {} keeps the old image",
            out.display()
        );
        return Ok(false);
    }
    drop(backup);
    drop(dir);
    Ok(true)
}

/// Puts back the old version of each file in `moved`. A file with no old
/// version is removed. Returns `false` when one of these steps fails.
fn restore(moved: &[&str], backup: &Path, out: &Path) -> bool {
    let mut restored = true;
    for back in moved {
        let (old, target) = (backup.join(back), out.join(back));
        let step = if old.exists() {
            std::fs::rename(&old, &target)
        } else {
            match std::fs::remove_file(&target) {
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                other => other,
            }
        };
        if let Err(err) = step {
            eprintln!("publish-image: cannot put back {}: {err}", target.display());
            restored = false;
        }
    }
    restored
}

/// Takes the exclusive lock of `dir`, as `flock -w 60 -x` does.
///
/// Returns `false` when the lock stays held for the whole wait.
fn lock_exclusive(dir: &File, out: &Path) -> Result<bool> {
    let deadline = Instant::now() + LOCK_WAIT;
    loop {
        match dir.try_lock() {
            Ok(()) => return Ok(true),
            Err(std::fs::TryLockError::WouldBlock) => {
                if Instant::now() >= deadline {
                    return Ok(false);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(std::fs::TryLockError::Error(err)) => {
                return Err(err).with_context(|| format!("cannot lock {}", out.display()));
            }
        }
    }
}

/// Sets the mode of `path` to 0644, as `chmod 0644` does.
#[cfg(unix)]
fn make_readable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644))
        .with_context(|| format!("cannot change the mode of {}", path.display()))
}

/// Checks that `path` exists. Other systems have no Unix modes.
#[cfg(not(unix))]
fn make_readable(path: &Path) -> Result<()> {
    std::fs::metadata(path)
        .map(|_| ())
        .with_context(|| format!("cannot change the mode of {}", path.display()))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    fn setup(staged: &[&str]) -> (smop::fs::TempDir, PathBuf, PathBuf) {
        let dir = smop::fs::temp_dir().unwrap();
        let (stage, out) = (dir.path().join("stage"), dir.path().join("out"));
        std::fs::create_dir_all(&stage).unwrap();
        std::fs::create_dir_all(&out).unwrap();
        for file in FILES {
            std::fs::write(out.join(file), format!("old {file}")).unwrap();
        }
        for file in staged {
            let path = stage.join(file);
            std::fs::write(&path, format!("new {file}")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        (dir, stage, out)
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    fn content(out: &Path, file: &str) -> String {
        std::fs::read_to_string(out.join(file)).unwrap()
    }

    #[test]
    fn a_complete_stage_replaces_each_file_with_mode_0644() {
        let (_dir, stage, out) = setup(&FILES);
        assert!(publish(&stage, &out).unwrap());
        for file in FILES {
            assert_eq!(content(&out, file), format!("new {file}"));
            let mode = std::fs::metadata(out.join(file))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o644, "{file}");
        }
        assert_eq!(names(&out), ["image.sha256", "rootfs.ext4", "vmlinux"]);
    }

    #[test]
    fn a_missing_staged_file_changes_nothing() {
        let (_dir, stage, out) = setup(&FILES[..2]);
        assert!(publish(&stage, &out).is_err());
        for file in FILES {
            assert_eq!(content(&out, file), format!("old {file}"));
        }
        assert_eq!(names(&out), ["image.sha256", "rootfs.ext4", "vmlinux"]);
    }

    #[test]
    fn a_failed_rename_puts_back_the_moved_files() {
        let (_dir, stage, out) = setup(&FILES);
        // A directory in the stage cannot replace a file of the image.
        std::fs::remove_file(stage.join("image.sha256")).unwrap();
        std::fs::create_dir(stage.join("image.sha256")).unwrap();
        assert!(!publish(&stage, &out).unwrap());
        for file in FILES {
            assert_eq!(content(&out, file), format!("old {file}"));
        }
        assert_eq!(names(&out), ["image.sha256", "rootfs.ext4", "vmlinux"]);
    }

    #[test]
    fn an_empty_path_argument_counts_as_missing() {
        assert_eq!(path_arg(None), None);
        assert_eq!(path_arg(Some(OsString::new())), None);
        assert_eq!(path_arg(Some("out".into())), Some(PathBuf::from("out")));
    }

    #[test]
    fn a_first_publish_needs_no_old_image() {
        let (_dir, stage, out) = setup(&FILES);
        for file in FILES {
            std::fs::remove_file(out.join(file)).unwrap();
        }
        assert!(publish(&stage, &out).unwrap());
        assert_eq!(names(&out), ["image.sha256", "rootfs.ext4", "vmlinux"]);
    }
}
