//! Zero-setup local dev runtime (issue #525).
//!
//! One command brings up a **fully working** local Harvest: an ephemeral
//! Postgres started for you, the ordinary embedded migrations applied, a worker
//! polling, and the management API + Vantage UI served — with no Docker, no
//! hand-authored `compose.yaml`, no `DATABASE_URL` to set and no
//! `diesel migration run` to remember.
//!
//! ```text
//! cargo dev
//! ```
//!
//! # What this is not
//!
//! It is **not** a second storage backend. The cluster it starts is real
//! `PostgreSQL` running the engine's real schema and real migrations, so a
//! workflow that runs here is byte-for-byte the workflow it will be in
//! production. What is automated is the Postgres *lifecycle* — nothing about
//! the engine, the event contract or replay changes. There is no "dev mode lies
//! to you" gap, which is exactly why a SQLite or in-memory backend was rejected.
//!
//! It is also **not** for production, and says so on every start. It refuses to
//! talk to anything it cannot show to be a local development database
//! ([`classify_database_url`]), and everything it creates is reclaimed on exit
//! — including after a `SIGKILL`, via [`reap_stale_sessions`].
//!
//! # Layout
//!
//! | Module | Responsibility |
//! |--------|----------------|
//! | [`safety`] | Refuse anything that is not a local development database |
//! | [`discovery`] | Find `PostgreSQL` server binaries already on the machine |
//! | [`acquire`] | Download one when the machine has none (`dev-runtime-managed`) |
//! | [`postgres`] | `initdb` → start → create database → stop → delete |
//! | [`session`] | On-disk session record and the pure reap decision |
//! | [`reaper`] | Reclaim what a killed run left behind |
//! | [`banner`] | The start banner and the copy-pasteable trigger command |
//! | [`sample`] | The built-in `dev_greeting` sample workflow |

pub mod banner;
pub mod discovery;
pub mod postgres;
pub mod reaper;
pub mod safety;
pub mod sample;
pub mod session;

#[cfg(feature = "dev-runtime-managed")]
pub mod acquire;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::Mutex;

pub use banner::{BannerInputs, StorageDescription, redact_dsn, render_banner};
pub use discovery::{
    DiscoveryEnv, Platform, PostgresBinaries, REQUIRED_TOOLS, candidate_bin_dirs, install_remedy,
    resolve_bin_dir,
};
use postgres::refuse_to_run_as_root;
pub use postgres::{
    EphemeralPostgres, MAX_UNIX_SOCKET_PATH_LEN, ephemeral_dsn, postgres_conf_lines,
    running_as_root, unix_socket_path_len, write_private_atomic,
};
pub use reaper::{
    proc_stat_is_live, proc_stat_start_time, process_is_alive, process_start_token,
    reap_stale_sessions, rewrite_owner_pid_for_test, session_root,
};
pub use safety::{DatabaseSafety, RefusalReason, SuspicionReason, classify_database_url};
pub use sample::SAMPLE_WORKFLOW;
pub use session::{
    ReapDecision, SESSION_DIR_PREFIX, SESSION_RECORD_FILE, SESSION_ROOT_PREFIX, SessionRecord,
    SkipReason, decide_reap, effective_postmaster_pid, is_session_dir, parse_postmaster_pid,
    record_is_self_consistent,
};

/// Environment variable naming a database to use instead of provisioning one.
pub const BYO_DATABASE_URL_ENV: &str = "HARVEST_DEV_DATABASE_URL";

/// Everything wrong that can happen bringing the dev runtime up.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DevError {
    /// No `PostgreSQL` server binaries anywhere, and no managed tier compiled in.
    #[error(
        "no PostgreSQL server binaries found (probed {probed} locations). \
         The dev runtime needs a real Postgres to run the engine's real schema — {remedy}"
    )]
    NoPostgresBinaries {
        /// How many directories were probed.
        probed: usize,
        /// The platform-specific remedy.
        remedy: String,
    },

    /// The managed tier could not obtain a `PostgreSQL` build.
    #[error("could not acquire a PostgreSQL build: {detail}")]
    Acquire {
        /// What went wrong, and what to do about it.
        detail: String,
    },

    /// A session file or directory could not be created, read or removed.
    #[error("dev runtime storage at {path}: {source}")]
    SessionDir {
        /// The path involved.
        path: PathBuf,
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// A Postgres tool could not be executed at all.
    #[error("could not run `{tool}`: {source}")]
    Spawn {
        /// Which tool.
        tool: &'static str,
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// A Postgres tool ran and failed.
    #[error("`{tool}` failed: {stderr}")]
    Tool {
        /// Which tool.
        tool: &'static str,
        /// Its own stderr, plus the server log where one exists.
        stderr: String,
    },

    /// No loopback port could be reserved for the cluster.
    #[error("could not reserve a local port for the ephemeral Postgres: {source}")]
    ReservePort {
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// `postmaster.pid` was missing or unreadable after a successful start.
    #[error("the server started but {path} did not contain a process id")]
    PostmasterPid {
        /// The pid file.
        path: PathBuf,
    },

    /// The session record could not be serialised.
    #[error("could not record the dev session: {0}")]
    SessionRecord(#[source] serde_json::Error),

    /// A statement against the ephemeral cluster failed.
    #[error("dev runtime {what} failed: {detail}")]
    Sql {
        /// Which operation.
        what: &'static str,
        /// The rendered database error.
        detail: String,
    },

    /// A blocking task did not join.
    #[error("dev runtime {what} did not complete: {source}")]
    Join {
        /// Which operation.
        what: &'static str,
        /// The join error.
        source: tokio::task::JoinError,
    },

    /// The supplied database is not one the dev runtime will ever touch.
    #[error(
        "refusing to start against this database: {reason}.\n\
         The dev runtime applies migrations and runs a worker against whatever it is \
         pointed at, so it only ever talks to an ephemeral local database. Unset \
         {BYO_DATABASE_URL_ENV} to have one provisioned for you."
    )]
    RefusedDatabase {
        /// Why it was refused.
        reason: RefusalReason,
    },

    /// The supplied database is local but looks production-shaped.
    #[error(
        "refusing to start against this database: {reason}.\n\
         It is on this machine, so if that name really is just a local database, re-run with \
         `--allow-suspicious-database-name`."
    )]
    SuspiciousDatabase {
        /// Why it needs an opt-in.
        reason: SuspicionReason,
    },

    /// The app did not become reachable.
    #[error("the dev runtime's HTTP server did not become ready at {url} within {seconds}s")]
    ServerNotReady {
        /// The URL that was polled.
        url: String,
        /// How long we waited.
        seconds: u64,
    },

    /// The process is `root`, which `PostgreSQL` refuses to run as.
    #[error(
        "refusing to provision a cluster as root — PostgreSQL will not run as root, by design.\n\
         Run as an ordinary user, or point the dev runtime at a database you already have \
         with {BYO_DATABASE_URL_ENV}."
    )]
    RunningAsRoot,

    /// The per-user session root is not one we can trust.
    #[error(
        "refusing to use the dev-runtime session directory {path}: {reason}. \
         It holds records that say which processes to stop and which directories to delete, \
         so it must be private to you"
    )]
    UntrustedSessionRoot {
        /// The offending directory.
        path: PathBuf,
        /// Why it is not trusted.
        reason: &'static str,
    },

    /// The configured HTTP bind address is not loopback.
    #[error(
        "refusing to serve the dev runtime on {host}, which is not loopback. The management \
         API is mounted without authentication because it is only ever reachable from this \
         machine — binding it anywhere else would expose starting and mutating workflows to \
         the network. Use 127.0.0.1 or ::1."
    )]
    NonLoopbackHttpHost {
        /// The host as configured.
        host: String,
    },

    /// The requested HTTP port cannot be bound.
    #[error(
        "cannot bind http://127.0.0.1:{port} ({source}). Something else is already listening \
         there — stop it, or pass `--port <PORT>` (or `--port 0` to have one chosen for you)"
    )]
    HttpPortUnavailable {
        /// The port that was asked for.
        port: u16,
        /// The underlying bind error.
        source: std::io::Error,
    },

    /// The generated Unix socket path is longer than the kernel allows.
    #[error(
        "the ephemeral cluster's socket path would be {len} bytes ({path}), over the {limit}-byte \
         limit a Unix socket address has. Pass a shorter `--session-root` (or set a shorter \
         TMPDIR)"
    )]
    SocketPathTooLong {
        /// The socket directory that does not fit.
        path: PathBuf,
        /// How long the resulting socket path would be.
        len: usize,
        /// The kernel's limit.
        limit: usize,
    },

    /// A cluster could not be shown to have stopped.
    #[error(
        "the ephemeral PostgreSQL (pid {pid}) could not be confirmed stopped, so its data \
         directory was left in place rather than deleted out from under a live server. \
         The next run will reclaim it"
    )]
    StopUnconfirmed {
        /// The postmaster still believed to be running.
        pid: u32,
    },

    /// The thread the HTTP server runs on could not be started.
    #[error("could not start the dev runtime's server thread: {source}")]
    ServerThread {
        /// The underlying I/O error.
        source: std::io::Error,
    },
}

/// How the dev runtime should come up.
#[derive(Debug, Clone)]
pub struct DevRuntimeConfig {
    /// Use this database instead of provisioning one. Still goes through the
    /// safety gate.
    pub database_url: Option<String>,
    /// Accept a loopback database whose *name* looks production-shaped.
    ///
    /// Note there is deliberately no equivalent for a remote host: "my local
    /// database is called `myapp_production`" is ordinary, "my dev runtime is
    /// pointed at `db.prod.internal`" never is.
    pub allow_suspicious_database_name: bool,
    /// HTTP port. `0` asks the kernel for a free one.
    pub http_port: u16,
    /// HTTP bind address. Loopback, and there is no reason to change it.
    pub http_host: String,
    /// Where session directories are created. Defaults to the system temp dir.
    pub session_root: Option<PathBuf>,
    /// Where the management API and UI are mounted.
    pub api_path: String,
}

impl Default for DevRuntimeConfig {
    fn default() -> Self {
        Self {
            database_url: None,
            allow_suspicious_database_name: false,
            http_port: 3000,
            http_host: "127.0.0.1".to_owned(),
            session_root: None,
            api_path: "/api/harvest".to_owned(),
        }
    }
}

impl DevRuntimeConfig {
    /// Layer the `HARVEST_DEV_*` environment variables over these settings.
    #[must_use]
    pub fn with_env_overrides(mut self) -> Self {
        if let Ok(url) = std::env::var(BYO_DATABASE_URL_ENV)
            && !url.trim().is_empty()
        {
            self.database_url = Some(url);
        }
        if let Ok(raw) = std::env::var("HARVEST_DEV_PORT") {
            let raw = raw.trim();
            if !raw.is_empty() {
                match raw.parse() {
                    Ok(port) => self.http_port = port,
                    // Silently falling back to 3000 makes `HARVEST_DEV_PORT=abc`
                    // look like it worked and then collide with whatever is on
                    // 3000. `--port abc` already errors; match it.
                    Err(_) => eprintln!(
                        "harvest-dev: ignoring HARVEST_DEV_PORT={raw:?} — not a port number"
                    ),
                }
            }
        }
        self
    }
}

/// A running dev runtime.
///
/// Owns the ephemeral cluster and the HTTP server; [`shutdown`](Self::shutdown)
/// stops both and reclaims the storage.
pub struct DevRuntime {
    api_url: String,
    ui_url: String,
    banner: String,
    /// `None` once teardown has taken it, so a double shutdown is a no-op and
    /// the two teardown paths (this one and Autumn's `on_shutdown` hook) can
    /// race harmlessly — exactly one of them wins.
    postgres: Arc<Mutex<Option<EphemeralPostgres>>>,
    server: ServerThread,
}

impl DevRuntime {
    /// Bring the whole runtime up: storage, migrations, worker, API and UI.
    ///
    /// # Errors
    ///
    /// [`DevError`] for a refused database, unavailable Postgres binaries, a
    /// cluster that will not start, or an HTTP server that never becomes ready.
    pub async fn start(config: DevRuntimeConfig) -> Result<Self, DevError> {
        // Before anything binds: `http_host` is a public field documented as
        // loopback-only, and until now nothing enforced it.
        require_loopback_http_host(&config.http_host)?;

        // The port is settled FIRST, before any cluster exists. autumn-web
        // `process::exit(1)`s on a bind failure, which skips every destructor
        // and both teardown paths — so a busy port used to leak a postmaster
        // and its data directory outright. Failing here costs nothing.
        let http_port = reserve_http_port(&config.http_host, config.http_port)?;

        let (database_url, storage, postgres) = provision_storage(&config).await?;
        let postgres = Arc::new(Mutex::new(postgres));

        // The reservation above could not be *held* across provisioning —
        // autumn-web binds this same port itself — and provisioning is the long
        // step. Two `cargo dev` runs that both found 3000 free will therefore
        // both arrive here, and the loser would reach autumn-web's bind-failure
        // `process::exit(1)`, which skips every destructor and strands the
        // cluster it just built. Re-prove the port now, while teardown is still
        // ours to run: the window narrows from seconds of provisioning to the
        // microseconds before the server binds.
        if let Err(error) = reserve_http_port(&config.http_host, http_port) {
            return Err(abandon_cluster(&postgres, error).await);
        }

        let base = format!("http://{}", http_authority(&config.http_host, http_port));
        let api_url = format!("{base}{}", config.api_path);
        let ui_url = format!("{api_url}/ui");

        let server = match spawn_server(&config, &database_url, http_port, Arc::clone(&postgres)) {
            Ok(server) => server,
            Err(error) => return Err(abandon_cluster(&postgres, error).await),
        };

        let banner = render_banner(&BannerInputs {
            ui_url: ui_url.clone(),
            api_url: api_url.clone(),
            sample_workflow: SAMPLE_WORKFLOW.to_owned(),
            storage,
        });

        let runtime = Self {
            api_url,
            ui_url,
            banner,
            postgres,
            server,
        };
        if let Err(error) = runtime.wait_until_ready().await {
            // Same rule: a runtime that never became usable must still take its
            // cluster with it — and if it could not, say so rather than
            // reporting only the readiness failure.
            if let Err(teardown) = runtime.shutdown().await {
                tracing::error!(%teardown, "dev runtime: storage was left behind");
                eprintln!("harvest-dev: storage was left behind: {teardown}");
            }
            return Err(error);
        }
        Ok(runtime)
    }

    /// Base URL of the management API.
    #[must_use]
    pub fn api_url(&self) -> &str {
        &self.api_url
    }

    /// URL of the Vantage dashboard.
    #[must_use]
    pub fn ui_url(&self) -> &str {
        &self.ui_url
    }

    /// The start banner, already rendered.
    #[must_use]
    pub fn banner(&self) -> &str {
        &self.banner
    }

    /// Block until the process is asked to stop.
    ///
    /// `SIGTERM` as well as `Ctrl-C`: an IDE's stop button, `timeout`, and every
    /// process supervisor send the former. Waiting only on `Ctrl-C` left the
    /// binary blocked forever after autumn-web's own handler had already torn
    /// the server down.
    pub async fn wait_for_shutdown_signal() {
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
            if let Some(term) = term.as_mut() {
                tokio::select! {
                    () = async { let _ = tokio::signal::ctrl_c().await; } => {}
                    _ = term.recv() => {}
                }
            } else {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }

    /// Stop the server and reclaim every byte of ephemeral state.
    ///
    /// # Errors
    ///
    /// [`DevError`] if the cluster could not be stopped or its directory could
    /// not be removed — the two failures that would actually leave state
    /// behind, so they are reported rather than swallowed.
    pub async fn shutdown(mut self) -> Result<(), DevError> {
        self.server.stop().await;
        let taken = self.postgres.lock().await.take();
        match taken {
            Some(postgres) => postgres.shutdown().await,
            None => Ok(()),
        }
    }

    /// Poll the health endpoint until the app answers.
    async fn wait_until_ready(&self) -> Result<(), DevError> {
        const READY_TIMEOUT_SECS: u64 = 180;
        let url = format!("{}/health", self.api_url);
        let client = reqwest::Client::new();
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(READY_TIMEOUT_SECS);
        while std::time::Instant::now() < deadline {
            // A server that has already exited will never answer, and waiting
            // out the full budget for it only buries the real failure.
            if self.server.has_exited() {
                break;
            }
            if let Ok(response) = client.get(&url).send().await
                && response.status().is_success()
            {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        Err(DevError::ServerNotReady {
            url,
            seconds: READY_TIMEOUT_SECS,
        })
    }
}

impl std::fmt::Debug for DevRuntime {
    /// Deliberately hand-written: a derived `Debug` would print the
    /// `EphemeralPostgres` behind the mutex, and its `database_url` carries the
    /// generated superuser password.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevRuntime")
            .field("api_url", &self.api_url)
            .field("ui_url", &self.ui_url)
            .finish_non_exhaustive()
    }
}

impl Drop for DevRuntime {
    /// Backstop for a `DevRuntime` dropped without `shutdown()` — a panicking
    /// test, an early `?`.
    ///
    /// Without this, dropping detaches the server thread and the last `Arc` to
    /// the cluster lives inside that thread's task, so `EphemeralPostgres`'s own
    /// `Drop` may never run before the process exits. Every failing run of the
    /// end-to-end test leaked a postmaster.
    fn drop(&mut self) {
        if let Some(stop) = self.server.stop.take() {
            let _ = stop.send(());
        }
        // `try_lock`: nothing else can hold this by the time we are dropped, and
        // blocking here would be worse than skipping the teardown.
        if let Ok(mut guard) = self.postgres.try_lock()
            && let Some(postgres) = guard.take()
        {
            // Dropping it runs `EphemeralPostgres`'s own synchronous teardown.
            drop(postgres);
        }
        if let Some(handle) = self.server.handle.take() {
            let _ = handle.join();
        }
    }
}

/// The OS thread the Autumn app runs on, plus the channel that stops it.
///
/// A dedicated thread with its own Tokio runtime, rather than a `tokio::spawn`:
/// `AppBuilder::run`'s future is not provably `Send` — rustc cannot infer the
/// higher-ranked lifetime through one of autumn-web's internal closures and
/// rejects the spawn outright — and `Runtime::block_on` carries no such bound.
/// It also isolates the dev server's runtime from the caller's, which is what
/// lets a test start and stop one from inside its own `#[tokio::test]` runtime.
struct ServerThread {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<std::thread::JoinHandle<()>>,
    exited: Arc<std::sync::atomic::AtomicBool>,
}

impl ServerThread {
    fn has_exited(&self) -> bool {
        self.exited.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Ask the server to stop, then wait for its thread.
    ///
    /// Best-effort by design: a server thread that will not come back must not
    /// stop us from reclaiming the database, which is the part that actually
    /// leaks.
    async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let Some(handle) = self.handle.take() else {
            return;
        };
        if let Err(error) = tokio::task::spawn_blocking(move || handle.join()).await {
            tracing::warn!(%error, "dev runtime: the server thread did not join");
        }
    }
}

/// Resolve storage: either the developer's database (gated) or a fresh
/// ephemeral cluster.
async fn provision_storage(
    config: &DevRuntimeConfig,
) -> Result<(String, StorageDescription, Option<EphemeralPostgres>), DevError> {
    if let Some(url) = config.database_url.as_deref() {
        match classify_database_url(url) {
            DatabaseSafety::Allowed => {}
            DatabaseSafety::Suspicious(reason) => {
                if !config.allow_suspicious_database_name {
                    return Err(DevError::SuspiciousDatabase { reason });
                }
                // `eprintln!` as well as `tracing`: at this point in boot no
                // subscriber has been installed (autumn-web installs its own
                // later, on the server thread), so a tracing-only warning is
                // silently dropped — and AC4 calls for a loud, unmissable one.
                eprintln!(
                    "harvest-dev: WARNING — proceeding against a production-shaped database \
                     name: {reason}"
                );
                tracing::warn!(
                    %reason,
                    "dev runtime: proceeding against a production-shaped local database name"
                );
            }
            DatabaseSafety::Refused(reason) => {
                return Err(DevError::RefusedDatabase { reason });
            }
        }
        return Ok((
            url.to_owned(),
            StorageDescription::BringYourOwn {
                redacted_dsn: redact_dsn(url),
            },
            None,
        ));
    }

    // Before the reaper, not after. A stale session record names a `bin_dir`
    // whose `pg_ctl` `stop_orphan` executes, and as `root` this process would
    // happily read one out of a `harvest-dev-0` some unprivileged local user
    // pre-created. Provisioning as `root` cannot work regardless, so refuse it
    // here — while the only thing we have done is decide to.
    refuse_to_run_as_root()?;

    // Reclaim anything a killed predecessor left behind before adding to it.
    let root = reaper::session_root(
        &config
            .session_root
            .clone()
            .unwrap_or_else(std::env::temp_dir),
    )?;
    match reap_stale_sessions(&root) {
        Ok(0) => {}
        Ok(reclaimed) => tracing::info!(reclaimed, "dev runtime: reclaimed abandoned sessions"),
        Err(error) => tracing::warn!(%error, "dev runtime: could not scan for abandoned sessions"),
    }

    let binaries = resolve_binaries().await?;
    let postgres = EphemeralPostgres::start(&binaries, config).await?;
    let url = postgres.database_url().to_owned();
    let storage = StorageDescription::Provisioned {
        version: postgres.version().to_owned(),
        data_dir: postgres.session_dir().join("data"),
    };
    Ok((url, storage, Some(postgres)))
}

/// An installed Postgres if there is one, otherwise the managed tier.
// Only the `dev-runtime-managed` arm awaits; the signature stays uniform so the
// call site does not need its own `cfg`.
#[cfg_attr(not(feature = "dev-runtime-managed"), allow(clippy::unused_async))]
async fn resolve_binaries() -> Result<PostgresBinaries, DevError> {
    match PostgresBinaries::discover() {
        Ok(binaries) => {
            tracing::debug!(
                bin_dir = %binaries.bin_dir().display(),
                "dev runtime: using an installed PostgreSQL"
            );
            Ok(binaries)
        }
        #[cfg(feature = "dev-runtime-managed")]
        Err(_) => acquire::acquire_postgres_binaries().await,
        #[cfg(not(feature = "dev-runtime-managed"))]
        Err(error) => Err(error),
    }
}

/// Give up on a start that has already provisioned a cluster, taking the
/// cluster with it.
///
/// The cluster is up; never leak it behind a failure that happened afterwards.
/// `take()` into a local first: holding the guard across the awaited shutdown is
/// the classic deadlock shape, and the `on_shutdown` hook contends for this very
/// lock. The error is passed through so callers read as `return Err(...)`.
async fn abandon_cluster(
    postgres: &Arc<Mutex<Option<EphemeralPostgres>>>,
    error: DevError,
) -> DevError {
    let taken = postgres.lock().await.take();
    if let Some(postgres) = taken {
        postgres.shutdown().await.ok();
    }
    error
}

/// Refuse to serve anywhere but loopback.
///
/// `DevRuntimeConfig::http_host` is public and documented as loopback-only, but
/// a doc comment is not an enforcement: a library caller could set `0.0.0.0`,
/// `::` or a LAN interface. That matters more here than in most servers because
/// `run_app` mounts the management router with `.api(...)`, **not**
/// `api_with_auth` — the dev runtime is unauthenticated precisely because it is
/// unreachable, so the two facts have to be kept true together.
///
/// Every address the host resolves to must be loopback, and a host that cannot
/// be resolved is refused rather than assumed: proving loopback is the point,
/// and an unprovable host is not a proof.
fn require_loopback_http_host(host: &str) -> Result<(), DevError> {
    use std::net::ToSocketAddrs as _;

    let refuse = || DevError::NonLoopbackHttpHost {
        host: host.to_owned(),
    };
    // Port 0 only to satisfy the resolver; nothing is bound here.
    let mut resolved = (host, 0u16)
        .to_socket_addrs()
        .map_err(|_| refuse())?
        .peekable();
    if resolved.peek().is_none() {
        return Err(refuse());
    }
    if resolved.any(|address| !address.ip().is_loopback()) {
        return Err(refuse());
    }
    Ok(())
}

/// The `host:port` authority for a URL, bracketing an IPv6 literal.
///
/// `::1` is a perfectly valid loopback host, and
/// `format!("http://{host}:{port}")` turns it into `http://::1:3000`, which no
/// URL parser accepts — so the readiness poll could never succeed and the
/// runtime would report `ServerNotReady` after its full budget, with the real
/// cause nowhere in the message.
#[must_use]
pub fn http_authority(host: &str, port: u16) -> String {
    // A name or an IPv4 literal needs no brackets; only an IPv6 literal does,
    // and it is identified by parsing, not by counting colons.
    if host.starts_with('[') || host.parse::<std::net::Ipv6Addr>().is_err() {
        return format!("{host}:{port}");
    }
    format!("[{host}]:{port}")
}

/// Settle the HTTP port, proving it is bindable.
///
/// `0` asks the kernel to choose. A concrete port is bound and released, which
/// turns "something else is already on 3000" into a legible error here instead
/// of a `process::exit(1)` deep inside autumn-web's boot — which skips every
/// destructor and would strand the cluster we are about to create.
fn reserve_http_port(host: &str, requested: u16) -> Result<u16, DevError> {
    // Bind the host we will actually serve on, not a hard-coded `127.0.0.1`:
    // otherwise `--http-host ::1` proves a port free on the wrong interface.
    let listener = std::net::TcpListener::bind((host, requested)).map_err(|source| {
        DevError::HttpPortUnavailable {
            port: requested,
            source,
        }
    })?;
    let port = listener
        .local_addr()
        .map_err(|source| DevError::ReservePort { source })?
        .port();
    drop(listener);
    Ok(port)
}

/// Build and run the Autumn app carrying `HarvestPlugin` and the sample.
///
/// Configuration is injected through Autumn's own [`ConfigLoader`] seam rather
/// than by mutating the process environment: `std::env::set_var` is `unsafe` and
/// unsound once other threads exist, and this runs inside a Tokio runtime that
/// already has them.
///
/// [`ConfigLoader`]: autumn_web::config::ConfigLoader
fn spawn_server(
    config: &DevRuntimeConfig,
    database_url: &str,
    http_port: u16,
    postgres: Arc<Mutex<Option<EphemeralPostgres>>>,
) -> Result<ServerThread, DevError> {
    let loader = DevConfigLoader {
        database_url: database_url.to_owned(),
        host: config.http_host.clone(),
        port: http_port,
    };
    let api_path = config.api_path.clone();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let exited = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let exited_in_thread = Arc::clone(&exited);

    let handle = std::thread::Builder::new()
        .name("harvest-dev-server".to_owned())
        .spawn(move || {
            // A guard, not a trailing store: a panicking server thread must
            // still mark itself finished, or `wait_until_ready` burns its whole
            // 180-second budget waiting for a thread that is already dead.
            let _exit_guard = ExitedOnDrop(exited_in_thread);
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::error!(%error, "dev runtime: could not build the server runtime");
                    return;
                }
            };
            runtime.block_on(async move {
                tokio::select! {
                    () = run_app(loader, api_path, postgres) => {}
                    _ = stop_rx => {}
                }
            });
            // Bounded, but generously: `pg_ctl stop --wait` is allowed 90
            // seconds, so a 5-second budget here could abandon a teardown
            // half-done and then report success.
            runtime.shutdown_timeout(std::time::Duration::from_secs(120));
        })
        .map_err(|source| DevError::ServerThread { source })?;

    Ok(ServerThread {
        stop: Some(stop_tx),
        handle: Some(handle),
        exited,
    })
}

/// The app's own boot-and-serve body.
async fn run_app(
    loader: DevConfigLoader,
    api_path: String,
    postgres: Arc<Mutex<Option<EphemeralPostgres>>>,
) {
    let dashboard_path = sample::DevDashboardPath(format!("{api_path}/ui"));
    autumn_web::app()
        .with_config_loader(loader)
        // `AppBuilder::run` asserts at least one route is registered, and the
        // plugin's management API mounts through `nest`, which does not count.
        // This is a landing redirect to the dashboard AND the thing that makes
        // the app boot at all.
        .routes(sample::routes())
        .with_extension(dashboard_path)
        .plugin(
            crate::HarvestPlugin::new()
                .workflows(sample::workflows())
                .activities(sample::activities())
                .worker(autumn_harvest::WorkerConfig::default())
                .api(api_path),
        )
        .run()
        .await;
    // Deliberately NO `on_shutdown` teardown hook. It looked like belt and
    // braces and was actually a race: on Ctrl-C both this thread's hook and
    // `DevRuntime::shutdown` reach for the same `Option`, and whichever loses
    // reports success for work the winner may have had truncated — the server
    // thread is cancelled and its runtime given a 5 s budget, while
    // `pg_ctl stop --wait` is allowed 90. Teardown has exactly one owner:
    // `DevRuntime::shutdown`, plus `Drop` as the panic-path backstop.
    let _ = postgres;
}

/// Marks the server thread finished on the way out, panic or not.
struct ExitedOnDrop(Arc<std::sync::atomic::AtomicBool>);

impl Drop for ExitedOnDrop {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Feeds the dev runtime's resolved settings to Autumn.
#[derive(Debug, Clone)]
struct DevConfigLoader {
    database_url: String,
    host: String,
    port: u16,
}

impl autumn_web::config::ConfigLoader for DevConfigLoader {
    // Not an `async fn`: the body never awaits, so it is a ready value dressed
    // up as a future. `unused_async_trait_impl` (clippy 1.98) says so.
    fn load(
        &self,
    ) -> impl std::future::Future<
        Output = Result<autumn_web::config::AutumnConfig, autumn_web::config::ConfigError>,
    > + Send {
        let mut config = autumn_web::config::AutumnConfig {
            // `dev` is what makes Autumn *apply* pending migrations rather than
            // merely report them — the same switch the Docker quickstart sets
            // by hand, and the reason no `diesel migration run` is needed.
            profile: Some("dev".to_owned()),
            ..autumn_web::config::AutumnConfig::default()
        };
        config.server.host.clone_from(&self.host);
        config.server.port = self.port;
        config.database.url = Some(self.database_url.clone());
        config.database.auto_migrate = Some(true);
        std::future::ready(Ok(config))
    }
}
