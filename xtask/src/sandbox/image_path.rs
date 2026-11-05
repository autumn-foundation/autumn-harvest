//! `cargo xtask sandbox image-path`: the port of `scripts/sandbox/image-path.sh`.
//!
//! Resolve a path inside a guest root file system, as a chroot would.
//!
//! The image controls its symbolic links. An absolute link starts again at
//! the image root, and `..` stops there. So no link of the image can take a
//! root build step to a host path. A path with more than 40 links fails.
//!
//! Usage:
//!
//! ```sh
//! cargo xtask sandbox image-path ROOT PATH
//! ```
//!
//! The command prints `ROOT` joined with the resolved path. The old script
//! was a shell function that the build sourced. The root file system step of
//! `cargo xtask sandbox build-guest-image` now runs this command instead.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

/// The most links that one resolution follows.
const MAX_LINKS: u32 = 40;

/// Arguments of `cargo xtask sandbox image-path`.
#[derive(Debug, Args)]
pub struct Args {
    /// The root directory of the guest file system.
    pub root: String,
    /// The path inside the guest file system.
    pub path: String,
}

/// Runs `cargo xtask sandbox image-path`.
///
/// The group dispatcher fixes the `Result` return type.
#[allow(clippy::unnecessary_wraps)]
pub fn run(args: Args) -> Result<ExitCode> {
    match resolve(&args.root, &args.path) {
        Ok(resolved) => {
            println!("{resolved}");
            Ok(ExitCode::SUCCESS)
        }
        Err(message) => {
            eprintln!("{message}");
            Ok(ExitCode::FAILURE)
        }
    }
}

/// Resolves `path` inside the image at `root`, and returns `root` joined with
/// the result.
///
/// Each name is checked in turn. A link puts its target in front of the rest
/// of the path. The result has no link inside the image, so a later write
/// cannot leave the image through one.
///
/// # Errors
///
/// Returns the message of the old script when the path has more than 40
/// links. Returns a message when a link cannot be read.
pub fn resolve(root: &str, path: &str) -> Result<String, String> {
    let mut todo = path.strip_prefix('/').unwrap_or(path).to_owned();
    let mut done = String::new();
    let mut hops = 0;
    while !todo.is_empty() {
        let (name, rest) = match todo.split_once('/') {
            Some((name, rest)) => (name.to_owned(), rest.to_owned()),
            None => (todo.clone(), String::new()),
        };
        todo = rest;
        match name.as_str() {
            "" | "." => continue,
            ".." => {
                // The root has no parent, so `..` stops there.
                if let Some(cut) = done.rfind('/') {
                    done.truncate(cut);
                }
                continue;
            }
            _ => {}
        }
        let candidate = format!("{root}{done}/{name}");
        if is_link(Path::new(&candidate)) {
            hops += 1;
            if hops > MAX_LINKS {
                return Err(format!("image-path: too many links in {path}"));
            }
            let target = std::fs::read_link(&candidate)
                .map_err(|err| format!("image-path: cannot read the link {candidate}: {err}"))?
                .into_os_string()
                .into_string()
                .map_err(|_| format!("image-path: the link {candidate} is not UTF-8"))?;
            // An absolute link starts again at the image root.
            if target.starts_with('/') {
                done.clear();
            }
            let target = target.strip_prefix('/').unwrap_or(&target);
            todo = if todo.is_empty() {
                target.to_owned()
            } else {
                format!("{target}/{todo}")
            };
        } else {
            done.push('/');
            done.push_str(&name);
        }
    }
    Ok(format!("{root}{done}"))
}

/// Reports whether `path` is a symbolic link, as `[ -L path ]` does.
fn is_link(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink())
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::symlink;

    use super::*;

    fn image() -> (smop::fs::TempDir, String) {
        let dir = smop::fs::temp_dir().unwrap();
        std::fs::create_dir_all(dir.path().join("usr/sbin")).unwrap();
        std::fs::create_dir_all(dir.path().join("etc")).unwrap();
        let root = dir.path().display().to_string();
        (dir, root)
    }

    #[test]
    fn a_plain_path_joins_the_root() {
        let (_dir, root) = image();
        assert_eq!(resolve(&root, "/work").unwrap(), format!("{root}/work"));
        assert_eq!(resolve(&root, "//a/./b/").unwrap(), format!("{root}/a/b"));
        assert_eq!(resolve(&root, "/").unwrap(), root);
    }

    #[test]
    fn links_stay_inside_the_image() {
        let (dir, root) = image();
        symlink("/etc", dir.path().join("abs")).unwrap();
        assert_eq!(resolve(&root, "/abs/x").unwrap(), format!("{root}/etc/x"));
        symlink("../../../../../../etc", dir.path().join("up")).unwrap();
        assert_eq!(resolve(&root, "/up").unwrap(), format!("{root}/etc"));
        symlink("usr/sbin", dir.path().join("sbin")).unwrap();
        assert_eq!(resolve(&root, "/sbin").unwrap(), format!("{root}/usr/sbin"));
        assert_eq!(resolve(&root, "/a/../../b").unwrap(), format!("{root}/b"));
    }

    #[test]
    fn a_link_loop_fails() {
        let (dir, root) = image();
        symlink("loop", dir.path().join("loop")).unwrap();
        assert_eq!(
            resolve(&root, "/loop").unwrap_err(),
            "image-path: too many links in /loop"
        );
    }
}
