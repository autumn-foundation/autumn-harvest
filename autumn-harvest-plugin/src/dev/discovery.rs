//! Locating the `PostgreSQL` server binaries the dev runtime drives (issue #525).
//!
//! The dev runtime runs a *real* Postgres, so it needs `initdb`, `pg_ctl` and
//! `postgres`. It looks for them in this order:
//!
//! 1. `HARVEST_DEV_PG_BIN` — an explicit override, always first.
//! 2. `PGBIN` — the conventional spelling other Postgres tooling honours.
//! 3. every entry on `PATH`.
//! 4. the well-known install layouts for the platform, newest version first.
//!
//! Only when all of those come up empty does the `dev-runtime-managed` tier
//! download a build (see [`super::acquire`]).
//!
//! The ordering and the "does this directory hold a usable toolset" decision are
//! pure functions over an injected environment and an injected probe, so the
//! whole policy — including the macOS and Windows layouts — is unit-testable on
//! any host.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The host families with distinct Postgres install conventions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Platform {
    /// Linux and the other Unixes that follow the same layouts.
    Linux,
    /// macOS: Homebrew and Postgres.app.
    MacOs,
    /// Windows: the `EnterpriseDB` installer.
    Windows,
}

impl Platform {
    /// The platform this binary was compiled for.
    #[must_use]
    pub const fn host() -> Self {
        if cfg!(target_os = "windows") {
            Self::Windows
        } else if cfg!(target_os = "macos") {
            Self::MacOs
        } else {
            Self::Linux
        }
    }

    /// The `PATH` separator this platform uses.
    const fn path_separator(self) -> char {
        match self {
            Self::Windows => ';',
            Self::Linux | Self::MacOs => ':',
        }
    }

    /// The on-disk file name for `tool` on this platform.
    fn executable_name(self, tool: &str) -> String {
        match self {
            Self::Windows => format!("{tool}.exe"),
            Self::Linux | Self::MacOs => tool.to_owned(),
        }
    }
}

/// The environment variables discovery reads, captured so tests can supply
/// their own without touching the process environment.
#[derive(Debug, Clone, Default)]
pub struct DiscoveryEnv {
    vars: BTreeMap<String, String>,
}

impl DiscoveryEnv {
    /// An environment with nothing in it.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// The current process environment, restricted to the variables discovery
    /// actually consults.
    #[must_use]
    pub fn from_process() -> Self {
        let mut env = Self::empty();
        for key in ["HARVEST_DEV_PG_BIN", "PGBIN", "PATH"] {
            if let Ok(value) = std::env::var(key) {
                env.set(key, &value);
            }
        }
        env
    }

    /// Set one variable.
    pub fn set(&mut self, key: &str, value: &str) {
        self.vars.insert(key.to_owned(), value.to_owned());
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.vars.get(key).map(String::as_str)
    }
}

/// `PostgreSQL` major versions probed in the well-known layouts, newest first.
///
/// The engine's migrations target modern Postgres; anything below 13 is out of
/// support upstream and is not worth probing for.
const PROBED_MAJOR_VERSIONS: &[u32] = &[19, 18, 17, 16, 15, 14, 13];

/// The three executables a directory must hold to be able to provision and run
/// a cluster. `psql` alone (a client-only install) is not enough.
pub const REQUIRED_TOOLS: &[&str] = &["initdb", "pg_ctl", "postgres"];

/// Every directory worth probing, in preference order and without duplicates.
#[must_use]
pub fn candidate_bin_dirs(platform: Platform, env: &DiscoveryEnv) -> Vec<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    let push = |dir: PathBuf, candidates: &mut Vec<PathBuf>| {
        if !candidates.contains(&dir) {
            candidates.push(dir);
        }
    };

    for key in ["HARVEST_DEV_PG_BIN", "PGBIN"] {
        if let Some(value) = env.get(key).map(str::trim).filter(|v| !v.is_empty()) {
            push(PathBuf::from(value), &mut candidates);
        }
    }

    if let Some(path) = env.get("PATH") {
        for entry in path.split(platform.path_separator()) {
            let entry = entry.trim();
            if !entry.is_empty() {
                push(PathBuf::from(entry), &mut candidates);
            }
        }
    }

    for dir in well_known_bin_dirs(platform) {
        push(dir, &mut candidates);
    }

    candidates
}

/// The platform's conventional Postgres install locations, newest version first.
fn well_known_bin_dirs(platform: Platform) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    match platform {
        Platform::Linux => {
            for version in PROBED_MAJOR_VERSIONS {
                // Debian/Ubuntu.
                dirs.push(PathBuf::from(format!("/usr/lib/postgresql/{version}/bin")));
            }
            for version in PROBED_MAJOR_VERSIONS {
                // RedHat/Fedora (PGDG packages).
                dirs.push(PathBuf::from(format!("/usr/pgsql-{version}/bin")));
            }
            dirs.push(PathBuf::from("/usr/local/pgsql/bin"));
            dirs.push(PathBuf::from("/usr/local/bin"));
            dirs.push(PathBuf::from("/usr/bin"));
        }
        Platform::MacOs => {
            for version in PROBED_MAJOR_VERSIONS {
                // Homebrew, Apple silicon then Intel.
                dirs.push(PathBuf::from(format!(
                    "/opt/homebrew/opt/postgresql@{version}/bin"
                )));
                dirs.push(PathBuf::from(format!(
                    "/usr/local/opt/postgresql@{version}/bin"
                )));
            }
            for version in PROBED_MAJOR_VERSIONS {
                dirs.push(PathBuf::from(format!(
                    "/Applications/Postgres.app/Contents/Versions/{version}/bin"
                )));
            }
            dirs.push(PathBuf::from(
                "/Applications/Postgres.app/Contents/Versions/latest/bin",
            ));
            dirs.push(PathBuf::from("/opt/homebrew/bin"));
            dirs.push(PathBuf::from("/usr/local/bin"));
        }
        Platform::Windows => {
            for version in PROBED_MAJOR_VERSIONS {
                dirs.push(PathBuf::from(format!(
                    r"C:\Program Files\PostgreSQL\{version}\bin"
                )));
            }
        }
    }
    dirs
}

/// The first candidate directory holding the whole toolset.
///
/// `probe` answers "does this directory contain this executable"; it takes the
/// platform-correct file name (`initdb.exe` on Windows), so a caller only has
/// to test for existence.
#[must_use]
pub fn resolve_bin_dir(
    candidates: &[PathBuf],
    platform: Platform,
    probe: &dyn Fn(&Path, &str) -> bool,
) -> Option<PathBuf> {
    candidates
        .iter()
        .find(|dir| {
            REQUIRED_TOOLS
                .iter()
                .all(|tool| probe(dir, &platform.executable_name(tool)))
        })
        .cloned()
}

/// A resolved, usable set of `PostgreSQL` server binaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresBinaries {
    bin_dir: PathBuf,
    platform: Platform,
}

impl PostgresBinaries {
    /// Adopt an already-known binary directory without re-probing.
    #[must_use]
    pub const fn at(bin_dir: PathBuf) -> Self {
        Self {
            bin_dir,
            platform: Platform::host(),
        }
    }

    /// Find server binaries on this machine.
    ///
    /// # Errors
    ///
    /// [`super::DevError::NoPostgresBinaries`] when nothing usable is
    /// installed, carrying the directories that were probed and a
    /// platform-specific remedy.
    pub fn discover() -> Result<Self, super::DevError> {
        let platform = Platform::host();
        let env = DiscoveryEnv::from_process();
        let candidates = candidate_bin_dirs(platform, &env);
        resolve_bin_dir(&candidates, platform, &|dir, tool| dir.join(tool).is_file())
            .map(|bin_dir| Self { bin_dir, platform })
            .ok_or_else(|| super::DevError::NoPostgresBinaries {
                probed: candidates.len(),
                remedy: install_remedy(platform).to_owned(),
            })
    }

    /// The directory the binaries live in.
    #[must_use]
    pub fn bin_dir(&self) -> &Path {
        &self.bin_dir
    }

    /// The full path of one tool, with the platform's executable suffix.
    #[must_use]
    pub fn tool(&self, name: &str) -> PathBuf {
        self.bin_dir.join(self.platform.executable_name(name))
    }
}

/// What to tell a developer who has no Postgres binaries at all.
#[must_use]
pub const fn install_remedy(platform: Platform) -> &'static str {
    match platform {
        Platform::Linux => {
            "install a Postgres server (`sudo apt install postgresql` or \
             `sudo dnf install postgresql-server`), point `HARVEST_DEV_PG_BIN` at an existing \
             install, or rebuild with `--features dev-runtime-managed` to have one downloaded \
             for you"
        }
        Platform::MacOs => {
            "install a Postgres server (`brew install postgresql@16`, or Postgres.app), point \
             `HARVEST_DEV_PG_BIN` at an existing install, or rebuild with \
             `--features dev-runtime-managed` to have one downloaded for you"
        }
        Platform::Windows => {
            "install PostgreSQL (the EnterpriseDB installer puts it under \
             `C:\\Program Files\\PostgreSQL\\<version>\\bin`), point `HARVEST_DEV_PG_BIN` at an \
             existing install, or rebuild with `--features dev-runtime-managed` to have one \
             downloaded for you"
        }
    }
}
