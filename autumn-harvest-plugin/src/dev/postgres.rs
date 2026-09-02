//! The ephemeral `PostgreSQL` cluster the dev runtime provisions (issue #525).
//!
//! `initdb` into a throwaway directory, start a postmaster bound to loopback on
//! a kernel-assigned port, create the database, hand back a DSN — then stop the
//! server and delete every byte of it on the way out.
//!
//! **The storage engine is unchanged.** This is a real `PostgreSQL` server running
//! the engine's real migrations; what is automated is its *lifecycle*. A
//! workflow that runs here is byte-for-byte the workflow it will be in
//! production, which is the whole reason a dev-only storage backend was
//! rejected.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use uuid::Uuid;

use super::discovery::PostgresBinaries;
use super::reaper::{process_start_token, session_root};
use super::session::{
    SESSION_DIR_PREFIX, SESSION_RECORD_FILE, SessionRecord, parse_postmaster_pid,
};
use super::{DevError, DevRuntimeConfig};

/// The role and database the ephemeral cluster is created with.
const DEV_ROLE: &str = "harvest";
/// Database name. Deliberately boring: it also has to pass the safety gate's
/// production-shaped-name check.
const DEV_DATABASE: &str = "harvest_dev";

/// Characters escaped when a generated password becomes part of a URI.
const PASSWORD_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'/')
    .add(b':')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// How long to wait for `initdb` / `pg_ctl start` before giving up with a
/// diagnostic that includes the server's own log.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(90);

/// Build the DSN for a provisioned cluster.
///
/// The password is percent-encoded: it is machine-generated, so it can and does
/// contain characters that would otherwise terminate the userinfo segment.
#[must_use]
pub fn ephemeral_dsn(user: &str, password: &str, port: u16, database: &str) -> String {
    let encoded = utf8_percent_encode(password, PASSWORD_ENCODE_SET);
    format!("postgres://{user}:{encoded}@127.0.0.1:{port}/{database}")
}

/// The `postgresql.conf` lines appended to the generated config.
///
/// Three independent jobs.
///
/// **Reachability**: `listen_addresses` is pinned to loopback, so a dev cluster
/// on a laptop on a café network is not a service.
///
/// **Confinement**: `unix_socket_directories` is pinned *inside the session
/// directory*. This is not cosmetic. Debian and Ubuntu build Postgres with
/// `--with-system-tsearch`-style packaging defaults that put the socket in
/// `/var/run/postgresql`, which an ordinary developer cannot write to — so on
/// the single most common Linux dev platform the postmaster starts, fails with
/// `could not create lock file … Permission denied`, and shuts itself down.
/// Putting the socket under the session directory also means it is reclaimed
/// with everything else rather than left behind.
///
/// **Throughput**: this cluster is deleted when the process exits, so durability
/// settings that exist to survive a crash are pure cost — turning them off is
/// what keeps first-run start-up inside the time budget.
#[must_use]
pub fn postgres_conf_lines(port: u16, socket_dir: &Path) -> Vec<String> {
    vec![
        "listen_addresses = '127.0.0.1'".to_owned(),
        format!("port = {port}"),
        format!(
            "unix_socket_directories = '{}'",
            escape_conf_string(&socket_dir.to_string_lossy())
        ),
        // The data directory is deleted on exit; there is nothing to recover.
        "fsync = off".to_owned(),
        "synchronous_commit = off".to_owned(),
        "full_page_writes = off".to_owned(),
        // A single developer, a worker and a handful of API calls.
        "max_connections = 60".to_owned(),
        "shared_buffers = 64MB".to_owned(),
        // No archiving, no replication: this cluster has no future.
        "wal_level = minimal".to_owned(),
        "max_wal_senders = 0".to_owned(),
        // Log to stderr, which pg_ctl redirects into the session log file.
        "logging_collector = off".to_owned(),
    ]
}

/// A running ephemeral cluster.
///
/// Holds the only handle to its own teardown. Prefer
/// [`shutdown`](Self::shutdown), which reports failure; [`Drop`] is the
/// best-effort backstop for panics and early returns.
#[derive(Debug)]
pub struct EphemeralPostgres {
    binaries: PostgresBinaries,
    session_dir: PathBuf,
    data_dir: PathBuf,
    port: u16,
    postmaster_pid: u32,
    database_url: String,
    version: String,
    /// Cleared by `shutdown`/`leak_for_reaper_test` so `Drop` does not repeat
    /// the work or fight a deliberate leak.
    teardown_pending: bool,
}

impl EphemeralPostgres {
    /// Provision and start a throwaway cluster.
    ///
    /// # Errors
    ///
    /// [`DevError`] if the session directory cannot be created, `initdb` or
    /// `pg_ctl` fails (the server's own stderr is included), or the cluster does
    /// not accept connections within [`STARTUP_TIMEOUT`].
    ///
    /// # Panics
    ///
    /// Never in practice: the only `expect` takes the first four bytes of a
    /// freshly generated UUID, which is always sixteen bytes long.
    pub async fn start(
        binaries: &PostgresBinaries,
        config: &DevRuntimeConfig,
    ) -> Result<Self, DevError> {
        // Never directly in the system temp directory: `session_root` creates
        // and verifies an owner-only per-user root, because a session record is
        // an instruction to stop a process and delete a tree.
        let root = session_root(
            &config
                .session_root
                .clone()
                .unwrap_or_else(std::env::temp_dir),
        )?;
        // Short by necessity, not by taste: a Unix socket path is capped at 107
        // bytes by `sockaddr_un`, and the socket lives inside this directory.
        // A full 32-hex UUID here pushed a default `/tmp` layout over the limit,
        // and Postgres's failure for that is `could not create any Unix-domain
        // sockets` — which reads like anything but "your path is too long".
        // The process id plus 32 bits of randomness is ample for a directory
        // that lives for minutes.
        let session_dir = root.join(format!(
            "{SESSION_DIR_PREFIX}{}-{:08x}",
            std::process::id(),
            u32::from_be_bytes(
                Uuid::new_v4().as_bytes()[..4]
                    .try_into()
                    .expect("4 bytes of a UUID")
            )
        ));
        let data_dir = session_dir.join("data");
        std::fs::create_dir_all(&session_dir).map_err(|source| DevError::SessionDir {
            path: session_dir.clone(),
            source,
        })?;
        // Written IMMEDIATELY, before `initdb` — the longest single step and the
        // likeliest place to be killed. A directory without a record is
        // invisible to the reaper forever, and this one already holds the
        // generated superuser password.
        write_session_record(&session_dir, &data_dir, None, binaries)?;

        match Self::provision(binaries, &session_dir, &data_dir).await {
            Ok(started) => Ok(started),
            Err(error) => {
                // Never delete a directory whose cluster might still be up: a
                // `pg_ctl start` that timed out, or a `postmaster.pid` we could
                // not read, both leave a live postmaster behind. Removing the
                // data directory then orphans it permanently, because the
                // reaper only ever acts on records. Leave it for the next run.
                if cluster_is_confirmed_down(binaries, &data_dir) {
                    let _ = std::fs::remove_dir_all(&session_dir);
                } else {
                    tracing::warn!(
                        path = %session_dir.display(),
                        "dev runtime: start failed with a cluster that may still be running; \
                         leaving the session for the reaper rather than deleting it"
                    );
                }
                Err(error)
            }
        }
    }

    async fn provision(
        binaries: &PostgresBinaries,
        session_dir: &Path,
        data_dir: &Path,
    ) -> Result<Self, DevError> {
        refuse_to_run_as_root()?;

        let password = generate_password();
        let password_file = session_dir.join("pw");
        write_private(&password_file, &password)?;

        // `--auth-host=scram-sha-256` rather than `trust`: the cluster is on
        // loopback, but every other local user is also on loopback.
        run_tool(
            &binaries.tool("initdb"),
            &[
                "--pgdata".as_ref(),
                data_dir.as_os_str(),
                "--username".as_ref(),
                DEV_ROLE.as_ref(),
                "--pwfile".as_ref(),
                password_file.as_os_str(),
                "--auth-host=scram-sha-256".as_ref(),
                "--auth-local=peer".as_ref(),
                "--encoding=UTF8".as_ref(),
                "--locale=C".as_ref(),
                "--no-sync".as_ref(),
            ],
            "initdb",
        )
        .await?;
        // The password reached `initdb`; it never needs to exist on disk again.
        let _ = std::fs::remove_file(&password_file);

        let port = reserve_local_port()?;
        // Inside the session directory, so it is both writable and reclaimed.
        let socket_dir = session_dir.join("socket");
        // Checked before `pg_ctl` so the diagnostic names the real problem.
        check_socket_path_fits(&socket_dir)?;
        std::fs::create_dir_all(&socket_dir).map_err(|source| DevError::SessionDir {
            path: socket_dir.clone(),
            source,
        })?;
        append_conf(data_dir, &postgres_conf_lines(port, &socket_dir))?;

        let log_file = session_dir.join("postgres.log");
        run_tool(
            &binaries.tool("pg_ctl"),
            &[
                "--pgdata".as_ref(),
                data_dir.as_os_str(),
                "--log".as_ref(),
                log_file.as_os_str(),
                "--wait".as_ref(),
                "--timeout".as_ref(),
                STARTUP_TIMEOUT.as_secs().to_string().as_ref(),
                "start".as_ref(),
            ],
            "pg_ctl start",
        )
        .await
        .map_err(|error| attach_log(error, &log_file))?;

        let postmaster_pid = read_postmaster_pid(data_dir)?;

        // `initdb -U harvest` creates the `postgres` maintenance database owned
        // by that role; create the application database through it.
        let admin_url = ephemeral_dsn(DEV_ROLE, &password, port, "postgres");
        let mut started = Self {
            binaries: binaries.clone(),
            session_dir: session_dir.to_path_buf(),
            data_dir: data_dir.to_path_buf(),
            port,
            postmaster_pid,
            database_url: ephemeral_dsn(DEV_ROLE, &password, port, DEV_DATABASE),
            version: String::new(),
            teardown_pending: true,
        };

        // Any failure past this point owns a running postmaster, so it must go
        // through `shutdown` rather than propagating over a live server.
        match started.finish_provisioning(&admin_url, session_dir).await {
            Ok(version) => {
                started.version = version;
                Ok(started)
            }
            Err(error) => {
                started.shutdown().await.ok();
                Err(error)
            }
        }
    }

    async fn finish_provisioning(
        &self,
        admin_url: &str,
        session_dir: &Path,
    ) -> Result<String, DevError> {
        write_session_record(
            session_dir,
            &self.data_dir,
            Some(self.postmaster_pid),
            &self.binaries,
        )?;

        execute_sql(admin_url, &format!("CREATE DATABASE {DEV_DATABASE}")).await?;
        let version = query_scalar_on(&self.database_url, "SELECT version() AS value").await?;
        Ok(short_version(&version))
    }

    /// The DSN for the provisioned application database.
    #[must_use]
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// The session directory holding the cluster and its bookkeeping.
    #[must_use]
    pub fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    /// The TCP port the postmaster is listening on.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// The postmaster's process id.
    #[must_use]
    pub const fn postmaster_pid(&self) -> u32 {
        self.postmaster_pid
    }

    /// The server version, e.g. `"16.4"`.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Run a query returning one text column, which must be aliased `value`.
    ///
    /// # Errors
    ///
    /// [`DevError::Sql`] if the connection or the query fails.
    pub async fn query_scalar(&self, sql: &str) -> Result<String, DevError> {
        query_scalar_on(&self.database_url, sql).await
    }

    /// Stop the server and remove every byte of ephemeral state.
    ///
    /// # Errors
    ///
    /// [`DevError`] if the server could not be stopped or the directory could
    /// not be removed — the two conditions that would leave state behind, so
    /// they are reported rather than swallowed.
    pub async fn shutdown(mut self) -> Result<(), DevError> {
        self.teardown_pending = false;
        teardown(
            &self.binaries,
            &self.data_dir,
            &self.session_dir,
            self.postmaster_pid,
        )
        .await
    }

    /// Abandon this handle *without* tearing down, leaving exactly what a
    /// `SIGKILL`ed run leaves behind.
    ///
    /// Only for exercising the reaper: nothing else has a legitimate reason to
    /// orphan a cluster.
    #[doc(hidden)] // exposed for the #525 reaper test; not a stable API
    pub const fn leak_for_reaper_test(mut self) {
        self.teardown_pending = false;
        std::mem::forget(self);
    }
}

impl Drop for EphemeralPostgres {
    /// Best-effort teardown for the panic / early-return path.
    ///
    /// `Drop` cannot await, so this runs the same stop-then-remove sequence
    /// synchronously. A failure here is logged rather than propagated — the
    /// reaper is the backstop, and the sequence leaves the session directory in
    /// place when the cluster could not be confirmed stopped, which is exactly
    /// what the reaper needs to find it.
    fn drop(&mut self) {
        if !self.teardown_pending {
            return;
        }
        let run = || {
            teardown_blocking(
                &self.binaries,
                &self.data_dir,
                &self.session_dir,
                self.postmaster_pid,
            )
        };
        // `pg_ctl stop --wait` blocks for as long as the shutdown takes. On a
        // multi-thread runtime `block_in_place` moves that off the scheduler
        // rather than stalling it; elsewhere there is nothing to do but run it.
        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(run)
            }
            _ => run(),
        };
        if let Err(error) = result {
            tracing::warn!(
                error = %error,
                path = %self.session_dir.display(),
                "dev runtime: ephemeral storage was not fully reclaimed"
            );
        }
    }
}

/// Stop the cluster and remove its session directory.
async fn teardown(
    binaries: &PostgresBinaries,
    data_dir: &Path,
    session_dir: &Path,
    postmaster_pid: u32,
) -> Result<(), DevError> {
    let binaries = binaries.clone();
    let data_dir = data_dir.to_path_buf();
    let session_dir = session_dir.to_path_buf();
    // `pg_ctl stop --wait` genuinely blocks; keep it off an async worker thread.
    tokio::task::spawn_blocking(move || {
        teardown_blocking(&binaries, &data_dir, &session_dir, postmaster_pid)
    })
    .await
    .map_err(|source| DevError::Join {
        what: "postgres teardown",
        source,
    })?
}

/// Stop the cluster, then — and only then — remove its directory.
///
/// The ordering is the whole point: deleting the data directory of a postmaster
/// that is still running corrupts a live cluster *and* orphans it, because the
/// record the reaper needs goes with it.
fn teardown_blocking(
    binaries: &PostgresBinaries,
    data_dir: &Path,
    session_dir: &Path,
    postmaster_pid: u32,
) -> Result<(), DevError> {
    stop_cluster_blocking(binaries, data_dir, Some(postmaster_pid))?;
    std::fs::remove_dir_all(session_dir).map_err(|source| DevError::SessionDir {
        path: session_dir.to_path_buf(),
        source,
    })
}

/// Whether no cluster is running in `data_dir` — used on the start-failure path,
/// where the pid may never have been read.
fn cluster_is_confirmed_down(binaries: &PostgresBinaries, data_dir: &Path) -> bool {
    let recorded = std::fs::read_to_string(data_dir.join("postmaster.pid"))
        .ok()
        .as_deref()
        .and_then(parse_postmaster_pid);
    recorded.map_or_else(
        // No pid file at all: `pg_ctl start` never got far enough to write one.
        || !data_dir.join("postmaster.pid").exists(),
        |pid| stop_cluster_blocking(binaries, data_dir, Some(pid)).is_ok(),
    )
}

/// `pg_ctl stop -m fast --wait`, synchronously.
///
/// `fast` (not `immediate`): it rolls back open transactions and shuts down
/// cleanly, and there is no crash-recovery cost to avoid on a cluster we are
/// about to delete.
///
/// **Returning `Ok` means the postmaster is gone, not that `pg_ctl` exited 0.**
/// Three states used to be reported as success without being it: a missing
/// `postmaster.pid` (so we did nothing at all), `pg_ctl` exiting 0 for a
/// cluster it did not own, and a "not running" message that was merely English
/// text. Every one of them ends in the caller deleting the data directory of a
/// *live* postmaster, so the pid — when we know it — is checked directly.
pub(super) fn stop_cluster_blocking(
    binaries: &PostgresBinaries,
    data_dir: &Path,
    expected_pid: Option<u32>,
) -> Result<(), DevError> {
    let already_gone =
        |pid: Option<u32>| pid.is_some_and(|pid| !super::reaper::process_is_alive(pid));

    if !data_dir.join("postmaster.pid").exists() {
        // No pid file. If we know the pid we can still answer truthfully; if we
        // do not, we cannot claim anything was stopped.
        return match expected_pid {
            Some(pid) if !super::reaper::process_is_alive(pid) => Ok(()),
            Some(pid) => Err(DevError::StopUnconfirmed { pid }),
            None => Ok(()),
        };
    }
    let output = std::process::Command::new(binaries.tool("pg_ctl"))
        .arg("--pgdata")
        .arg(data_dir)
        .arg("--mode=fast")
        .arg("--wait")
        .arg("--timeout")
        .arg(STARTUP_TIMEOUT.as_secs().to_string())
        .arg("stop")
        .stdin(Stdio::null())
        // `pg_ctl`'s "server is not running" is the one thing we read out of its
        // stderr, so pin the locale rather than letting a translated message
        // turn a clean stop into a reported failure.
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|source| DevError::Spawn {
            tool: "pg_ctl stop",
            source,
        })?;

    if output.status.success() || already_gone(expected_pid) {
        return match expected_pid {
            Some(pid) if super::reaper::process_is_alive(pid) => {
                Err(DevError::StopUnconfirmed { pid })
            }
            _ => Ok(()),
        };
    }
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if stderr.contains("not running") || stderr.contains("No such file") {
        return match expected_pid {
            Some(pid) if super::reaper::process_is_alive(pid) => {
                Err(DevError::StopUnconfirmed { pid })
            }
            _ => Ok(()),
        };
    }
    Err(DevError::Tool {
        tool: "pg_ctl stop",
        stderr,
    })
}

/// Run one Postgres tool to completion, failing with its own stderr.
async fn run_tool(
    program: &Path,
    args: &[&std::ffi::OsStr],
    label: &'static str,
) -> Result<(), DevError> {
    let output = tokio::process::Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|source| DevError::Spawn {
            tool: label,
            source,
        })?;
    if output.status.success() {
        return Ok(());
    }
    Err(DevError::Tool {
        tool: label,
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
    })
}

/// Attach the postmaster's own log tail to a start failure.
///
/// A bare `pg_ctl: could not start server` is unactionable; the reason is
/// always in the log the server wrote just before dying.
fn attach_log(error: DevError, log_file: &Path) -> DevError {
    let DevError::Tool { tool, stderr } = error else {
        return error;
    };
    let tail = std::fs::read_to_string(log_file)
        .map(|log| {
            log.lines()
                .rev()
                .take(20)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    if tail.is_empty() {
        return DevError::Tool { tool, stderr };
    }
    DevError::Tool {
        tool,
        stderr: format!("{stderr}\n--- postgres log ---\n{tail}"),
    }
}

/// Ask the kernel for a free loopback port.
///
/// Postgres has no "port 0" mode, so the port has to be concrete before the
/// server starts. Binding and immediately releasing leaves a small race with
/// anything else on the machine doing the same; a collision surfaces as a
/// `pg_ctl start` failure naming the port rather than as silent misbehaviour.
fn reserve_local_port() -> Result<u16, DevError> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))
        .map_err(|source| DevError::ReservePort { source })?;
    let port = listener
        .local_addr()
        .map_err(|source| DevError::ReservePort { source })?
        .port();
    drop(listener);
    Ok(port)
}

/// Append our settings to the generated `postgresql.conf`.
///
/// Appending (rather than replacing) keeps `initdb`'s own platform-tuned
/// defaults, so this only overrides what it names.
fn append_conf(data_dir: &Path, lines: &[String]) -> Result<(), DevError> {
    use std::io::Write as _;
    let path = data_dir.join("postgresql.conf");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .map_err(|source| DevError::SessionDir {
            path: path.clone(),
            source,
        })?;
    let body = format!(
        "\n# --- autumn-harvest dev runtime (ephemeral, issue #525) ---\n{}\n",
        lines.join("\n")
    );
    file.write_all(body.as_bytes())
        .map_err(|source| DevError::SessionDir { path, source })
}

/// The longest Unix socket path the kernel accepts.
///
/// `sockaddr_un::sun_path` is 108 bytes including the terminating NUL, which is
/// the 107 Postgres reports.
pub const MAX_UNIX_SOCKET_PATH_LEN: usize = 107;

/// The socket file Postgres will create in `socket_dir` for `port`.
///
/// Split out so the length check is a pure function over a path — the failure it
/// prevents is otherwise reported by Postgres as `could not create any
/// Unix-domain sockets`, which names neither the path nor the limit.
#[must_use]
pub fn unix_socket_path_len(socket_dir: &Path, port: u16) -> usize {
    socket_dir.as_os_str().len() + format!("/.s.PGSQL.{port}").len()
}

/// Refuse a session directory whose socket path cannot fit.
///
/// Checked at the widest possible port number so the answer does not depend on
/// which ephemeral port the kernel happened to hand out.
fn check_socket_path_fits(socket_dir: &Path) -> Result<(), DevError> {
    let len = unix_socket_path_len(socket_dir, u16::MAX);
    if len <= MAX_UNIX_SOCKET_PATH_LEN {
        return Ok(());
    }
    Err(DevError::SocketPathTooLong {
        path: socket_dir.to_path_buf(),
        len,
        limit: MAX_UNIX_SOCKET_PATH_LEN,
    })
}

/// Escape a value for a single-quoted `postgresql.conf` string.
///
/// Postgres's own rule: a literal single quote is written twice. Session paths
/// live under the system temp directory and realistically never contain one,
/// but a config file we generate should not depend on that.
fn escape_conf_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// Write (or rewrite) the session record the reaper reads.
fn write_session_record(
    session_dir: &Path,
    data_dir: &Path,
    postmaster_pid: Option<u32>,
    binaries: &PostgresBinaries,
) -> Result<(), DevError> {
    write_private(
        &session_dir.join(SESSION_RECORD_FILE),
        &SessionRecord {
            owner_pid: std::process::id(),
            owner_start_token: process_start_token(std::process::id()),
            postmaster_pid,
            // So the reaper can stop this cluster with the very binaries that
            // started it, including a downloaded install discovery never sees.
            bin_dir: Some(binaries.bin_dir().to_path_buf()),
            // Recorded together with the pid, so the reaper can later prove the
            // process at that pid is still the one we started rather than a
            // reuse of the number.
            postmaster_start_token: postmaster_pid.and_then(process_start_token),
            data_dir: data_dir.to_path_buf(),
            created_at: chrono::Utc::now(),
        }
        .to_json()
        .map_err(DevError::SessionRecord)?,
    )
}

/// Refuse early, and legibly, when the process is `root`.
///
/// `PostgreSQL` itself refuses to run as `root` — `initdb` exits with a security
/// message before doing anything. Left to surface as a raw tool failure that
/// reads like a bug in the dev runtime, so it is checked up front and answered
/// with what to actually do about it. This is a real situation, not a
/// hypothetical: plenty of container-based dev environments run as `root`.
fn refuse_to_run_as_root() -> Result<(), DevError> {
    if running_as_root() {
        return Err(DevError::RunningAsRoot);
    }
    Ok(())
}

/// Whether this process is running as `root`, and so cannot provision a
/// cluster.
///
/// Dependency-free: the effective uid is reachable from `std` only through
/// `libc`, and adding `libc` to this crate for one predicate would be a poor
/// trade — so this asks `id`, which every Unix ships. Always `false` on
/// Windows, which has no equivalent restriction.
#[must_use]
pub fn running_as_root() -> bool {
    #[cfg(unix)]
    {
        super::reaper::unix_uid() == Some(0)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// Read `postmaster.pid`, which `pg_ctl start --wait` has by now written.
fn read_postmaster_pid(data_dir: &Path) -> Result<u32, DevError> {
    let path = data_dir.join("postmaster.pid");
    let contents = std::fs::read_to_string(&path).map_err(|source| DevError::SessionDir {
        path: path.clone(),
        source,
    })?;
    parse_postmaster_pid(&contents).ok_or(DevError::PostmasterPid { path })
}

/// Write a file only this user can read. The generated superuser password and
/// the DSN that embeds it both go through here.
fn write_private(path: &Path, contents: &str) -> Result<(), DevError> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|source| DevError::SessionDir {
        path: path.to_path_buf(),
        source,
    })?;
    file.write_all(contents.as_bytes())
        .map_err(|source| DevError::SessionDir {
            path: path.to_path_buf(),
            source,
        })
}

/// A 32-character alphanumeric password for the ephemeral superuser.
///
/// Alphanumeric only so it survives a `postgresql.conf`/URI round trip without
/// depending on escaping being right; 32 characters of it is ample for a
/// loopback-only cluster that exists for minutes.
fn generate_password() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    // Two v4 UUIDs are 256 bits from the OS RNG.
    let entropy: Vec<u8> = Uuid::new_v4()
        .as_bytes()
        .iter()
        .chain(Uuid::new_v4().as_bytes().iter())
        .copied()
        .collect();
    entropy
        .iter()
        .take(32)
        .map(|byte| ALPHABET[usize::from(*byte) % ALPHABET.len()] as char)
        .collect()
}

/// Turns `PostgreSQL 16.4 on x86_64...` into `16.4`.
fn short_version(full: &str) -> String {
    full.split_whitespace()
        .nth(1)
        .unwrap_or("unknown")
        .to_owned()
}

// ---------------------------------------------------------------------------
// Small SQL helpers
//
// The dev runtime needs exactly two things before the engine's own pool exists:
// create a database, and read one scalar. Both go through the same diesel-async
// connection the rest of the crate uses, so there is no second Postgres client
// in the graph.
// ---------------------------------------------------------------------------

/// One text column, aliased `value`.
#[derive(diesel::QueryableByName)]
struct ScalarRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    value: String,
}

async fn connect(url: &str) -> Result<diesel_async::AsyncPgConnection, DevError> {
    use diesel_async::AsyncConnection as _;
    diesel_async::AsyncPgConnection::establish(url)
        .await
        .map_err(|error| DevError::Sql {
            what: "connect",
            detail: error.to_string(),
        })
}

pub(super) async fn execute_sql(url: &str, sql: &str) -> Result<(), DevError> {
    use diesel_async::RunQueryDsl as _;
    let mut conn = connect(url).await?;
    diesel::sql_query(sql)
        .execute(&mut conn)
        .await
        .map_err(|error| DevError::Sql {
            what: "execute",
            detail: error.to_string(),
        })?;
    Ok(())
}

pub(super) async fn query_scalar_on(url: &str, sql: &str) -> Result<String, DevError> {
    use diesel_async::RunQueryDsl as _;
    let mut conn = connect(url).await?;
    let rows: Vec<ScalarRow> = diesel::sql_query(sql)
        .load(&mut conn)
        .await
        .map_err(|error| DevError::Sql {
            what: "query",
            detail: error.to_string(),
        })?;
    rows.into_iter()
        .next()
        .map(|row| row.value)
        .ok_or(DevError::Sql {
            what: "query",
            detail: "the query returned no rows".to_owned(),
        })
}
