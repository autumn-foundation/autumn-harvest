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
                "the cache directory {} is not owner-only, and its contents would be executed. \
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

    if std::fs::rename(&staging, &cache_root).is_err() {
        // Either another process got there first (fine) or the rename genuinely
        // failed (the completeness check below is the arbiter either way).
        let _ = std::fs::remove_dir_all(&staging);
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
             owner-only — it would be executed"
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

/// Whether `dir` exists, is a real directory (not a symlink), is owned by this
/// user, and is not writable by group or other.
fn directory_is_private(dir: &std::path::Path) -> bool {
    let Ok(metadata) = std::fs::symlink_metadata(dir) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        use std::os::unix::fs::PermissionsExt as _;
        if super::reaper::unix_uid().is_some_and(|uid| uid != metadata.uid()) {
            return false;
        }
        if metadata.permissions().mode() & 0o022 != 0 {
            return false;
        }
    }
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
    Ok(base
        .join("autumn-harvest")
        .join("postgresql")
        .join(MANAGED_CACHE_KEY))
}
