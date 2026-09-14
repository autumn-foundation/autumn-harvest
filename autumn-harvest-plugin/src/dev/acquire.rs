//! Acquiring `PostgreSQL` binaries on a machine that has none (issue #525, AC2).
//!
//! Only compiled under `dev-runtime-managed`. Discovery
//! ([`super::discovery`]) is always tried first, so a developer who already has
//! Postgres installed never downloads anything; this is the path that makes
//! "a clean machine with only the Rust toolchain" literally true.
//!
//! The archive is fetched and verified by `postgresql_archive` (the same crate
//! `autumn-web`'s own managed-Postgres provider builds on) and extracted into a
//! per-user cache, so the download happens once per machine rather than once per
//! run. **Only the binaries come from there.** The cluster is still initialised,
//! started, and destroyed by our own lifecycle code, which is what keeps
//! ephemerality and teardown under this crate's control.

use std::path::PathBuf;

use super::DevError;
use super::discovery::{Platform, PostgresBinaries, REQUIRED_TOOLS};

/// Major version acquired when none is installed.
///
/// Pinned to a major rather than left open: two developers on the same commit
/// should get the same *server*, and Postgres patch releases within a major are
/// compatible, so letting the patch float costs nothing and avoids depending on
/// one exact upstream artifact continuing to exist.
const MANAGED_VERSION_REQ: &str = "^16";

/// Cache-directory segment for [`MANAGED_VERSION_REQ`].
///
/// The requirement is a range, so the resolved patch version is not known until
/// after a download — and checking the cache must not require the network. The
/// cache is therefore keyed on the *major*, and a populated cache is reused
/// as-is; clearing `HARVEST_DEV_CACHE_DIR` is how you take a newer patch.
const MANAGED_CACHE_KEY: &str = "16";

/// Fetch (or reuse) a cached `PostgreSQL` install and return its binaries.
///
/// # Errors
///
/// [`DevError::Acquire`] if the archive cannot be fetched or extracted — most
/// often no network on a machine that also has no Postgres, which the message
/// says explicitly.
pub async fn acquire_postgres_binaries() -> Result<PostgresBinaries, DevError> {
    let cache_root = cache_root()?;
    let version_req =
        postgresql_archive::VersionReq::parse(MANAGED_VERSION_REQ).map_err(|source| {
            DevError::Acquire {
                detail: format!("invalid pinned version requirement: {source}"),
            }
        })?;

    // Refuse a cache another local user could write to BEFORE downloading 30 MB
    // into it — and say why, rather than reporting the download as corrupt,
    // which is what a bare `cached_install` miss looks like.
    if cache_root.exists() && !directory_is_private(&cache_root) {
        return Err(DevError::Acquire {
            detail: format!(
                "the cache directory {} is not private, and its contents would be executed. \
                 Point HARVEST_DEV_CACHE_DIR somewhere private, or install PostgreSQL yourself",
                cache_root.display()
            ),
        });
    }

    // A previous run already extracted this version: use it and touch nothing.
    if let Some(binaries) = cached_install(&cache_root) {
        return Ok(binaries);
    }

    tracing::info!(
        "no PostgreSQL server found on this machine; downloading one (about 30 MB, once)"
    );
    eprintln!("harvest dev: no PostgreSQL server installed — downloading one (once, ~30 MB)…");

    let (_version, archive) = postgresql_archive::get_archive(
        postgresql_archive::configuration::theseus::URL,
        &version_req,
    )
    .await
    .map_err(|source| DevError::Acquire {
        detail: format!(
            "could not download a PostgreSQL build ({source}). This machine has no Postgres \
             installed and no reachable network; install Postgres, or point \
             HARVEST_DEV_PG_BIN at an existing install"
        ),
    })?;

    // Extract to a private staging directory and rename it into place, so two
    // `cargo dev`s racing on a cold cache cannot interleave their writes into a
    // half-populated install that then fails in confusing ways. The loser's
    // rename fails because the destination exists — which is success, not an
    // error: the winner's install is already there.
    let parent = cache_root.parent().unwrap_or(&cache_root).to_path_buf();
    std::fs::create_dir_all(&parent).map_err(|source| DevError::Acquire {
        detail: format!("could not create {}: {source}", parent.display()),
    })?;
    // The staging directory must NOT exist yet: `postgresql_archive`'s theseus
    // extractor treats an existing output directory as "another process already
    // extracted this" and returns success having written nothing — which would
    // then be renamed into the cache as a permanently empty install, and every
    // later run would re-download 30 MB and fail identically.
    let staging = parent.join(format!(".staging-{}", uuid::Uuid::new_v4().simple()));

    let extracted = postgresql_archive::extract(
        postgresql_archive::configuration::theseus::URL,
        &archive,
        &staging,
    )
    .await;
    if let Err(source) = extracted {
        let _ = std::fs::remove_dir_all(&staging);
        return Err(DevError::Acquire {
            detail: format!("could not extract the PostgreSQL archive: {source}"),
        });
    }

    let this_call_created_cache_root = std::fs::rename(&staging, &cache_root).is_ok();
    if !this_call_created_cache_root {
        // Either another process got there first (fine) or the rename genuinely
        // failed (the completeness check below is the arbiter either way).
        let _ = std::fs::remove_dir_all(&staging);
    }

    // The preflight above only ran if `cache_root` already existed. A fresh
    // acquire into a not-yet-existing versioned subdirectory skips it. An
    // untrusted `HARVEST_DEV_CACHE_DIR` is caught only here, after the
    // rename — but only when THIS call is the one that just created
    // `cache_root`. A lost race means some other process owns it now.
    // This call must not delete a directory it does not own, whatever
    // `directory_is_private` says about it.
    if this_call_created_cache_root && !directory_is_private(&cache_root) {
        // Remove what was just downloaded, rather than leaving an install
        // in a shared location. Report the real reason, not the misleading
        // "archive did not contain the expected binaries" a bare
        // `cached_install` miss would otherwise produce.
        let _ = std::fs::remove_dir_all(&cache_root);
        return Err(DevError::Acquire {
            detail: format!(
                "the cache directory {} is not private, and its contents would be executed. \
                 Point HARVEST_DEV_CACHE_DIR somewhere private, or install PostgreSQL yourself",
                cache_root.display()
            ),
        });
    }

    cached_install(&cache_root).ok_or_else(|| DevError::Acquire {
        detail: format!(
            "the downloaded archive did not contain the expected server binaries under {}",
            cache_root.display()
        ),
    })
}

/// The extracted install under `cache_root`, if it is complete and ours.
///
/// These files get executed, so a cache directory that another local user could
/// write to is not usable. The default cache is per-user, but a container or CI
/// image that points `XDG_CACHE_HOME` at a shared volume would otherwise turn
/// "plant three files" into code execution in the developer's session.
fn cached_install(cache_root: &std::path::Path) -> Option<PostgresBinaries> {
    if !directory_is_private(cache_root) {
        tracing::warn!(
            path = %cache_root.display(),
            "dev runtime: ignoring a cached PostgreSQL install in a directory that is not \
             private — it would be executed"
        );
        return None;
    }
    let bin_dir = cache_root.join("bin");
    let platform = Platform::host();
    let complete = REQUIRED_TOOLS.iter().all(|tool| {
        let name = match platform {
            Platform::Windows => format!("{tool}.exe"),
            Platform::Linux | Platform::MacOs => (*tool).to_owned(),
        };
        bin_dir.join(name).is_file()
    });
    complete.then(|| PostgresBinaries::at(bin_dir))
}

/// Whether `dir` is safe to execute binaries from.
///
/// A leaf-only check is not enough (issue #1292). The check and the run
/// happen at two separate moments. A local user who can write an ancestor
/// of `dir` can act in between. That user can rename the validated
/// directory aside. That user can then put a symlink, or a directory of
/// their own, in its place. Checking every ancestor removes the directory
/// such a user would need to replace.
///
/// This still checks, then trusts the path for later use. It closes the
/// race against another local user, but it does not remove the gap
/// between the check and the run. Only opening a handle and executing
/// from that handle would remove the gap.
fn directory_is_private(dir: &std::path::Path) -> bool {
    leaf_is_private(dir) && dir.parent().is_none_or(ancestors_are_private)
}

/// Whether `dir` exists, is a real directory (not a symlink), and is ours to
/// trust — see [`super::reaper::directory_is_ours`]. On Unix it must also not
/// be writable by group or other.
fn leaf_is_private(dir: &std::path::Path) -> bool {
    let Ok(metadata) = std::fs::symlink_metadata(dir) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return false;
    }
    if super::reaper::directory_is_ours(dir, &metadata).is_err() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o022 != 0 {
            return false;
        }
    }
    true
}

/// Whether `dir` and every directory above it, up to the filesystem root,
/// resist a rename-and-replace race.
///
/// The ownership rule is looser than in [`leaf_is_private`]. An ancestor
/// may be owned by the current user or by root. This lets root-owned
/// system directories, such as `/` and `/home`, pass. An ancestor must
/// still not be a symlink.
///
/// A group- or other-writable ancestor is refused, unless it has the
/// sticky bit. The kernel applies the same rule to `/tmp`. The sticky bit
/// blocks the actual attack: another user renaming or deleting an entry
/// they do not own. A plain shared temp directory does not need refusal
/// to close that attack.
///
/// [`leaf_is_private`] fails open on ownership when the current uid
/// cannot be determined. That is by design for one directory: a real
/// permission problem still surfaces as `initdb`'s own error. This walk
/// checks many directories. The same fail-open choice would widen, not
/// narrow, what an unidentified process trusts. An ancestor with an
/// indeterminate uid is refused here, unless the ancestor is root-owned.
#[cfg(unix)]
fn ancestors_are_private(dir: &std::path::Path) -> bool {
    use std::os::unix::fs::MetadataExt as _;
    use std::os::unix::fs::PermissionsExt as _;

    let mut current = dir;
    loop {
        let Ok(metadata) = std::fs::symlink_metadata(current) else {
            return false;
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return false;
        }
        let untrusted_owner =
            metadata.uid() != 0 && super::reaper::unix_uid() != Some(metadata.uid());
        let mode = metadata.permissions().mode();
        let sticky = mode & 0o1000 != 0;
        let writable_by_others = mode & 0o022 != 0;
        if untrusted_owner || (writable_by_others && !sticky) {
            return false;
        }
        current = match current.parent() {
            Some(parent) => parent,
            None => return true,
        };
    }
}

#[cfg(not(unix))]
fn ancestors_are_private(_dir: &std::path::Path) -> bool {
    true
}

/// Where downloaded binaries are cached, so the download is once per machine.
fn cache_root() -> Result<PathBuf, DevError> {
    let base = std::env::var_os("HARVEST_DEV_CACHE_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("XDG_CACHE_HOME").map(PathBuf::from))
        .or_else(|| {
            std::env::var_os("LOCALAPPDATA")
                .map(PathBuf::from)
                .filter(|_| cfg!(windows))
        })
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".cache"))
        })
        .ok_or_else(|| DevError::Acquire {
            detail: "no cache directory is available (set HARVEST_DEV_CACHE_DIR)".to_owned(),
        })?;
    // The ancestor walk in `directory_is_private` needs a real chain of
    // directories to check. A relative path has no such chain above the
    // current directory, and would fail that walk with a confusing error.
    // Reject it here, with a message that names the actual problem.
    if !base.is_absolute() {
        return Err(DevError::Acquire {
            detail: format!(
                "the cache directory {} is not absolute. Set HARVEST_DEV_CACHE_DIR to an \
                 absolute path",
                base.display()
            ),
        });
    }
    Ok(base
        .join("autumn-harvest")
        .join("postgresql")
        .join(MANAGED_CACHE_KEY))
}

#[cfg(test)]
mod tests {
    use super::directory_is_private;

    /// Issue #1287 regression: the Unix answer must not change. A directory
    /// we own, with default `tempfile` permissions, is still private.
    ///
    /// Canonicalised first: the ancestor walk (issue #1292) refuses a
    /// symlinked ancestor. On macOS `tempfile`'s own base directory
    /// resolves through one (`/var` -> `/private/var`). That symlink is
    /// root-owned. No attacker can replace it, so it is not the case
    /// this test means to pin. Canonicalising resolves it away, leaving
    /// only real directories in the chain.
    #[cfg(unix)]
    #[test]
    fn an_owner_owned_directory_is_still_private_on_unix() {
        let dir = tempfile::tempdir().expect("temp dir");
        let real_dir = dir.path().canonicalize().expect("canonicalize temp dir");
        assert!(directory_is_private(&real_dir));
    }

    /// Issue #1287 regression: group/other-writable is still rejected on
    /// Unix, independent of the new `directory_is_ours` delegation above.
    #[cfg(unix)]
    #[test]
    fn a_group_writable_directory_is_still_rejected_on_unix() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o770))
            .expect("chmod");
        assert!(!directory_is_private(dir.path()));
    }

    /// Issue #1287's actual regression, for the cache-directory site: before
    /// this fix, any non-symlink directory read as "private" on Windows.
    /// `C:\Windows\Temp` is the machine-wide location the issue names as the
    /// realistic exposure.
    #[cfg(windows)]
    #[test]
    fn a_directory_outside_any_per_user_root_is_not_private_on_windows() {
        let dir = std::path::PathBuf::from(r"C:\Windows\Temp");
        // `directory_is_private` reads `false` both for "correctly rejected"
        // and for "could not even be stat'd". Stat it here first, so a
        // broken runner fails loudly instead of passing this test vacuously.
        std::fs::symlink_metadata(&dir).expect("C:\\Windows\\Temp must be stat-able");
        assert!(
            !directory_is_private(&dir),
            "a shared directory must not be treated as a private cache"
        );
    }

    /// The other half: a cache directory under `%LOCALAPPDATA%` is still
    /// accepted.
    #[cfg(windows)]
    #[test]
    fn a_directory_under_localappdata_is_private_on_windows() {
        let base = std::env::var_os("LOCALAPPDATA").expect("LOCALAPPDATA must be set on Windows");
        // A unique temp dir, not a fixed name: a fixed name could already
        // exist with real contents, which dropping a `TempDir` would delete.
        let dir = tempfile::tempdir_in(base).expect("temp dir under LOCALAPPDATA");
        assert!(directory_is_private(dir.path()));
    }

    /// The ancestor walk (issue #1292) has its own dedicated suite. It
    /// needs real chmod/chown control over a multi-level directory tree.
    /// Only Unix gives this file that control.
    #[cfg(unix)]
    mod ancestor_walk {
        use std::fs;
        use std::os::unix::fs::PermissionsExt as _;

        use super::directory_is_private;

        /// A cache root nested three levels under a fresh, owner-only temp
        /// directory: the shape a real per-user cache has.
        ///
        /// Every level gets an explicit `chmod`. `create_dir_all` alone would
        /// leave each level's mode to the process umask, and a permissive
        /// umask would then make this "accepted" fixture fail its own test.
        ///
        /// The root is canonicalised before use. On macOS, `tempfile`'s own
        /// base resolves through a root-owned symlink (`/var` ->
        /// `/private/var`). No attacker can replace that symlink, but the
        /// ancestor walk refuses any symlinked ancestor regardless of who
        /// owns it. Canonicalising leaves only real directories for it to
        /// walk, so this "accepted" fixture does not fail on that platform.
        fn private_cache_root() -> (tempfile::TempDir, std::path::PathBuf) {
            let root = tempfile::tempdir().expect("temp dir");
            let root_path = root.path().canonicalize().expect("canonicalize temp dir");
            let a = root_path.join("a");
            let b = a.join("b");
            let cache = b.join("cache");
            fs::create_dir_all(&cache).expect("mkdir cache");
            for dir in [
                root_path.as_path(),
                a.as_path(),
                b.as_path(),
                cache.as_path(),
            ] {
                fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).expect("chmod");
            }
            (root, cache)
        }

        #[test]
        fn a_cache_root_under_private_ancestors_is_accepted() {
            let (_root, cache) = private_cache_root();
            assert!(directory_is_private(&cache));
        }

        #[test]
        fn a_world_writable_ancestor_is_refused() {
            let (root, cache) = private_cache_root();
            let ancestor = root.path().join("a");
            fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o777)).expect("chmod");
            assert!(!directory_is_private(&cache));
        }

        #[test]
        fn a_group_writable_ancestor_is_refused() {
            let (root, cache) = private_cache_root();
            let ancestor = root.path().join("a");
            fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o770)).expect("chmod");
            assert!(!directory_is_private(&cache));
        }

        #[test]
        fn a_symlinked_ancestor_is_refused() {
            let (root, _cache) = private_cache_root();
            let real_a = root.path().join("a");
            let link_a = root.path().join("link-a");
            std::os::unix::fs::symlink(&real_a, &link_a).expect("symlink");
            let cache_via_link = link_a.join("b").join("cache");
            assert!(!directory_is_private(&cache_via_link));
        }

        #[test]
        fn the_immediate_parent_writable_by_others_is_refused() {
            let (_root, cache) = private_cache_root();
            let parent = cache.parent().expect("parent");
            fs::set_permissions(parent, fs::Permissions::from_mode(0o777)).expect("chmod");
            assert!(!directory_is_private(&cache));
        }

        /// A world-writable ancestor with the sticky bit — `/tmp` itself, in
        /// shape — must still be accepted. Sticky already blocks the rename
        /// this check exists to catch, and every temp-backed cache root has one.
        #[test]
        fn a_sticky_world_writable_ancestor_is_accepted() {
            let (root, cache) = private_cache_root();
            let ancestor = root.path().join("a");
            fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o1777)).expect("chmod");
            assert!(directory_is_private(&cache));
        }

        /// An ancestor owned by neither root nor the current user is refused,
        /// even with a strict, no-write-bit mode. Mode alone is not the whole
        /// rule: that owner still controls the directory's contents.
        ///
        /// Changing an ancestor's owner needs root, so this test runs only as
        /// root — the same account this sandbox already runs test suites as.
        #[test]
        fn an_ancestor_owned_by_someone_else_is_refused() {
            if super::super::super::reaper::unix_uid() != Some(0) {
                eprintln!("SKIP: chowning an ancestor to another user needs root");
                return;
            }
            let (root, cache) = private_cache_root();
            let ancestor = root.path().join("a");
            let other_uid = 65534; // "nobody" on most systems; the raw uid is enough.
            std::os::unix::fs::chown(&ancestor, Some(other_uid), None).expect("chown");
            assert!(!directory_is_private(&cache));
        }
    }
}
