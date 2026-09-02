//! Applying Harvest's schema migrations to a **dedicated** Harvest database
//! (issue #1240).
//!
//! # Why this exists
//!
//! Since 0.6.0 `HarvestPlugin` *registers* its migration sets with Autumn
//! instead of applying them, so `autumn migrate` covers everything that lives
//! in the application's primary database. Under `harvest.mode = "embedded"`
//! that is the whole story — the application database *is* the Harvest
//! database.
//!
//! Under `harvest.mode = "split"` / `"external"` it is not. Harvest storage is
//! a separate database that Autumn has no handle on, and `plugin_migrations`
//! can only ever target the primary one. Outside the `dev` profile the plugin
//! only *warns* about pending migrations there, so an operator has to migrate
//! that database themselves before rolling replicas — which until now meant a
//! checked-out source tree and the `diesel` CLI. This module is that procedure
//! as library code, driven by `harvest migrate` in the CLI.
//!
//! # Compatibility with Diesel's bookkeeping
//!
//! Deliberately bug-for-bug compatible with `diesel_migrations`, because the
//! same database is also migrated by Autumn (in `embedded` mode) and by the
//! plugin's own startup path:
//!
//! * the ledger is `__diesel_schema_migrations (version VARCHAR(50) PRIMARY
//!   KEY, run_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`;
//! * a migration's **version** is the part of its directory name before the
//!   first `_` (`20260409000000_harvest_initial` → `20260409000000`);
//! * migrations are applied in ascending version order, each inside one
//!   transaction together with its ledger row, so a failure leaves neither the
//!   schema change nor the record of it;
//! * a migration whose `metadata.toml` says `run_in_transaction = false` — what
//!   a `CREATE INDEX CONCURRENTLY` migration needs, since Postgres rejects that
//!   statement inside a transaction block — is applied without one, as Diesel
//!   applies it (see [`apply_without_transaction`]).
//!
//! A version already present in the ledger is skipped, whoever wrote it. Rows
//! this binary does not recognise are *reported*, never removed: they normally
//! mean the database was migrated by a newer build than the one running here
//! (see [`MigrationPlan::unrecognized`]).
//!
//! # What it does not do
//!
//! * **No `down.sql`.** Rolling a schema back under a running fleet is an
//!   operator decision with data loss attached; it is not automated here.
//! * **No collision resolution.** Autumn can rewrite a colliding version to a
//!   substitute so both migrations still apply, because it sees every plugin's
//!   set at once. This module sees only what it is handed, so a duplicate
//!   version across sets is a hard error ([`validate_versions`]) rather than a
//!   silently skipped migration.
//! * **No transport policy.** [`connect`] is `AsyncPgConnection::establish`,
//!   which is `NoTls`. A caller that must reach a TLS-requiring database — the
//!   `harvest migrate` CLI does, since a managed Harvest database usually is
//!   one — builds its own connection and calls [`plan_on_connection`] /
//!   [`apply_to_connection`] instead. Keeping the connector out of here is what
//!   keeps a rustls stack out of the engine core.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel::sql_types::Text;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde::Serialize;

use crate::error::{HarvestError, HarvestResult, database_error};

include!(concat!(env!("OUT_DIR"), "/migration_scripts.rs"));

/// Diesel's migration ledger. Named here rather than derived so the coupling to
/// `diesel_migrations` is greppable.
const MIGRATION_LEDGER: &str = "__diesel_schema_migrations";

/// The lock mode each migration's transaction takes on `MIGRATION_LEDGER`.
///
/// Public so a test can pin the two non-conflicts this mode is chosen for
/// against a real server rather than against this string. See
/// `apply_in_transaction` for why a mode that blocked `ROW EXCLUSIVE` would
/// deadlock against Diesel's own body-then-insert lock order.
pub const LEDGER_LOCK_MODE: &str = "SHARE UPDATE EXCLUSIVE";

/// `CREATE TABLE IF NOT EXISTS` for the ledger, byte-compatible with the table
/// `diesel_migrations` creates on a fresh database.
const CREATE_LEDGER_SQL: &str = "CREATE TABLE IF NOT EXISTS __diesel_schema_migrations (\
     version VARCHAR(50) PRIMARY KEY NOT NULL, \
     run_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)";

/// Diesel's ledger column is `VARCHAR(50)`; a longer version cannot be recorded
/// at all, so it is rejected when the set is read rather than mid-apply.
const MAX_VERSION_LEN: usize = 50;

/// Diesel's per-migration metadata file, sitting beside `up.sql`.
const METADATA_FILE: &str = "metadata.toml";

/// One migration ready to apply: the directory name, the version Diesel keys
/// the ledger by, the `up.sql` body, and whether Diesel would wrap it in a
/// transaction.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct MigrationScript {
    /// Migration directory name, e.g. `20260409000000_harvest_initial`.
    pub name: String,
    /// Ledger key: everything before the first `_` in [`name`](Self::name).
    pub version: String,
    /// The `up.sql` body. Skipped when serialised — a report names migrations,
    /// it does not reprint the schema.
    #[serde(skip)]
    pub sql: String,
    /// Whether to wrap the body in a transaction, from the migration's
    /// `metadata.toml` (`run_in_transaction`, Diesel's key and default).
    ///
    /// `false` is what a `CREATE INDEX CONCURRENTLY` migration needs: Postgres
    /// rejects that statement inside a transaction block, so a migration
    /// Diesel applies happily would otherwise fail here.
    pub run_in_transaction: bool,
}

impl MigrationScript {
    /// Build a script from a migration directory name and its `up.sql` body,
    /// with Diesel's default of running inside a transaction.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`] when the name carries no version Diesel could
    /// record: an empty version, or one longer than the ledger's `VARCHAR(50)`.
    pub fn new(name: impl Into<String>, sql: impl Into<String>) -> HarvestResult<Self> {
        let name = name.into();
        // Diesel's own rule: the version is the leading component, and a name
        // with no `_` at all *is* its version.
        let version = name.split('_').next().unwrap_or_default().to_string();
        if version.is_empty() {
            return Err(HarvestError::Config(format!(
                "migration `{name}`: no version prefix (expected `<version>_<description>`)"
            )));
        }
        // Characters, not bytes: Postgres counts `VARCHAR(50)` in characters,
        // so a 26-character prefix of two-byte characters is 52 bytes and
        // records perfectly well. Measuring in bytes would refuse a migration
        // Diesel accepts.
        let version_chars = version.chars().count();
        if version_chars > MAX_VERSION_LEN {
            return Err(HarvestError::Config(format!(
                "migration `{name}`: version `{version}` is {version_chars} characters, but \
                 {MIGRATION_LEDGER}.version is VARCHAR({MAX_VERSION_LEN})"
            )));
        }
        Ok(Self {
            name,
            version,
            sql: sql.into(),
            run_in_transaction: true,
        })
    }

    /// Build a script and apply the migration's `metadata.toml` to it.
    ///
    /// `metadata` is the file's contents, or `""` when the migration has none
    /// (Diesel's default: run in a transaction).
    ///
    /// # Errors
    ///
    /// As [`new`](Self::new), plus [`HarvestError::Config`] when the metadata
    /// cannot be read unambiguously — see [`parse_run_in_transaction`].
    pub fn with_metadata(
        name: impl Into<String>,
        sql: impl Into<String>,
        metadata: &str,
    ) -> HarvestResult<Self> {
        let mut script = Self::new(name, sql)?;
        script.run_in_transaction = parse_run_in_transaction(&script.name, metadata)?;
        Ok(script)
    }
}

/// Read Diesel's `run_in_transaction` out of a `metadata.toml`.
///
/// # Why the real TOML parser
///
/// This began as a hand-rolled reader of the one key Diesel defines, on the
/// theory that a strict subset of TOML was cheaper than a dependency. Review
/// found five separate ways that subset was wrong — an unknown key carrying an
/// invalid value, a duplicate key, a line that is not `key = value`, an
/// unbalanced quoted key, and non-ASCII whitespace — each of them a file
/// `diesel migration run` refuses and this accepted, applying and recording a
/// migration on metadata nobody agreed about.
///
/// Parsing with the crate Diesel parses with removes the class rather than the
/// instances: what this reads and what Diesel reads are identical by
/// construction, rejections included. Unknown keys are *ignored* (serde's
/// default, and Diesel's behaviour) — but only once the file parses as TOML,
/// which is exactly the distinction the hand parser could not make.
///
/// # Errors
///
/// [`HarvestError::Config`] naming the migration when the metadata is not valid
/// TOML, or when `run_in_transaction` is present but not a boolean.
pub fn parse_run_in_transaction(name: &str, metadata: &str) -> HarvestResult<bool> {
    toml::from_str::<Metadata>(metadata)
        .map(|parsed| parsed.run_in_transaction)
        .map_err(|error| {
            HarvestError::Config(format!(
                "migration `{name}`: {METADATA_FILE} is not readable as Diesel \
                 reads it: {error}"
            ))
        })
}

/// Diesel's `metadata.toml` shape: one optional key, defaulting to `true`.
///
/// `#[serde(default)]` and no `deny_unknown_fields`, mirroring Diesel's own
/// struct — a key this does not know is ignored rather than refused, so a
/// future Diesel option does not make such a migration unapplyable here.
#[derive(Debug, serde::Deserialize)]
#[serde(default)]
struct Metadata {
    run_in_transaction: bool,
}

impl Default for Metadata {
    fn default() -> Self {
        // Diesel's default, and the one that matters: a migration that says
        // nothing about it runs inside a transaction.
        Self {
            run_in_transaction: true,
        }
    }
}

/// Harvest's own migration set, embedded in the binary at build time.
///
/// This is the same `migrations/` directory `autumn_harvest::MIGRATIONS`
/// embeds for Diesel — regenerated by `build.rs` on every build, so it cannot
/// drift from what Autumn and the plugin apply.
///
/// # Panics
///
/// If a migration directory in this crate is named such that it carries no
/// usable version — an authoring error in this repository, caught by the
/// `embedded_set_is_well_formed_and_matches_the_build_manifest` unit test
/// rather than left to fail against a production database.
#[must_use]
pub fn embedded() -> Vec<MigrationScript> {
    let mut scripts: Vec<MigrationScript> = EMBEDDED_MIGRATION_SCRIPTS
        .iter()
        .map(|(name, sql, metadata)| {
            // A name that cannot produce a version, or a `metadata.toml` this
            // cannot read, is a build-time authoring error in this repository --
            // caught by `embedded_set_is_well_formed_and_matches_the_build_manifest`
            // rather than left to fail against a production database.
            MigrationScript::with_metadata(*name, *sql, metadata)
                .expect("embedded migration is not well formed")
        })
        .collect();
    scripts.sort_by(|a, b| a.version.cmp(&b.version));
    scripts
}

/// Read a migration set from a directory laid out the way Diesel expects:
/// one `<version>_<description>/up.sql` per migration.
///
/// The escape hatch for sets this binary does not embed — the plugin's own
/// Harvest-database migrations (connector dead-letters), or an application's.
/// Hidden entries and plain files are skipped, exactly as Diesel skips them.
///
/// A migration's optional `metadata.toml` is honoured
/// ([`parse_run_in_transaction`]), so an application's `CREATE INDEX
/// CONCURRENTLY` migration applies here exactly as `diesel migration run`
/// applies it.
///
/// # Errors
///
/// [`HarvestError::Config`] when the directory cannot be read, when a
/// migration directory has no readable `up.sql` (a silently short set is the
/// failure mode this exists to prevent), when a name carries no usable
/// version, or when a `metadata.toml` cannot be read unambiguously.
pub fn from_directory(dir: &Path) -> HarvestResult<Vec<MigrationScript>> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        HarvestError::Config(format!(
            "cannot read migration directory `{}`: {e}",
            dir.display()
        ))
    })?;

    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| {
            HarvestError::Config(format!(
                "cannot read migration directory `{}`: {e}",
                dir.display()
            ))
        })?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || !entry.path().is_dir() {
            continue;
        }
        names.push(name);
    }
    names.sort();

    let mut scripts = Vec::with_capacity(names.len());
    for name in names {
        let up = dir.join(&name).join("up.sql");
        let sql = std::fs::read_to_string(&up).map_err(|e| {
            HarvestError::Config(format!(
                "migration `{name}`: missing/unreadable up.sql ({}): {e}",
                up.display()
            ))
        })?;
        // Diesel's optional per-migration metadata. Absent is the common case
        // and means its default (run in a transaction); present and unreadable
        // is an error rather than a silent default, because the one thing it
        // says decides whether the body may contain `CONCURRENTLY`.
        let metadata_path = dir.join(&name).join(METADATA_FILE);
        let metadata = match std::fs::read_to_string(&metadata_path) {
            Ok(metadata) => metadata,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => {
                return Err(HarvestError::Config(format!(
                    "migration `{name}`: unreadable {METADATA_FILE} ({}): {e}",
                    metadata_path.display()
                )));
            }
        };
        scripts.push(MigrationScript::with_metadata(name, sql, &metadata)?);
    }
    scripts.sort_by(|a, b| a.version.cmp(&b.version));
    Ok(scripts)
}

/// Reject two migrations that would key the same ledger row.
///
/// Diesel's ledger is keyed by version *alone*, so two sets that independently
/// picked the same version would leave one of them applied and the other
/// recorded-but-never-run — invisibly, forever. Autumn resolves that by
/// assigning the loser a substitute version, which it can only do because it
/// sees every registered set at once. Here the honest answer is to refuse.
///
/// # Errors
///
/// [`HarvestError::Config`] naming both migrations that share a version.
pub fn validate_versions(scripts: &[MigrationScript]) -> HarvestResult<()> {
    let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
    for script in scripts {
        if let Some(previous) = seen.insert(&script.version, &script.name) {
            return Err(HarvestError::Config(format!(
                "migration version `{}` is claimed by both `{previous}` and `{}`; \
                 rename one -- Diesel's ledger is keyed by version alone, so one \
                 of them would never be applied",
                script.version, script.name
            )));
        }
    }
    Ok(())
}

/// What a migration run would do against one database.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct MigrationPlan {
    /// Supplied migrations the ledger already records, in version order.
    pub already_applied: Vec<String>,
    /// Supplied migrations not yet in the ledger, in the order they would be
    /// applied.
    pub pending: Vec<MigrationScript>,
    /// Ledger versions no supplied set accounts for, in version order.
    ///
    /// Ordinarily this means the database was migrated by a **newer** build
    /// than the one reporting — the deploy that owns those versions has already
    /// shipped. It is not an error (Diesel ignores such rows too), but it is
    /// worth an operator's attention: it can equally mean this binary is
    /// pointed at the wrong database.
    pub unrecognized: Vec<String>,
    /// Whether the ledger table exists yet. `false` on a database that has
    /// never been migrated; [`plan`] does not create it.
    pub ledger_exists: bool,
}

impl MigrationPlan {
    /// Whether anything remains to apply.
    #[must_use]
    pub const fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}

/// A run that failed part-way, carrying **what it did before it failed**.
///
/// A run applies migrations one at a time and each commits on its own, so a
/// failure at the fourth of six leaves three applied. Dropping that on the
/// error path would leave an operator — or the deploy tooling reading the JSON
/// report — unable to say which of the six the database now has, which is the
/// question they need answered before deciding what to do next.
///
/// [`Display`](std::fmt::Display) delegates to the error, so a caller that only
/// prints it sees exactly the failure message.
///
/// The `apply*` signatures return this **boxed**. It is the `Err` half of a
/// `Result` whose `Ok` half is one `MigrationReport`, and an `Err` that dwarfs
/// the `Ok` is paid on the success path too — `clippy::result_large_err`, which
/// CI denies. Field access is unaffected: `partial.report` and `partial.error`
/// read through the box.
#[derive(Debug)]
pub struct PartialMigration {
    /// Everything applied before the failure, in the order applied.
    pub report: MigrationReport,
    /// Why the run stopped.
    pub error: HarvestError,
}

impl std::fmt::Display for PartialMigration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for PartialMigration {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl From<HarvestError> for Box<PartialMigration> {
    /// So `?` still lifts a plain [`HarvestError`] into the boxed error the
    /// `apply*` signatures use.
    fn from(error: HarvestError) -> Self {
        Self::new(PartialMigration::from(error))
    }
}

impl From<HarvestError> for PartialMigration {
    /// For the failures that happen before any migration is applied — a
    /// duplicate version, a connection that never opened, an unreadable ledger.
    fn from(error: HarvestError) -> Self {
        Self {
            report: MigrationReport {
                applied: Vec::new(),
                already_applied: Vec::new(),
                applied_concurrently: Vec::new(),
                unrecognized: Vec::new(),
                failed: None,
                // These failures happen before the privilege is probed, and
                // nothing was applied, so there is no unserialized apply to
                // warn about.
                ledger_lock_available: true,
                applied_unserialized: Vec::new(),
            },
            error,
        }
    }
}

/// The migration a run stopped on.
///
/// Named separately from the error message so a report — JSON included — says
/// *structurally* what the database may now contain. That matters most for a
/// `run_in_transaction = false` migration: nothing rolls it back, so an early
/// statement can stand while a later one fails, and the run has applied
/// something it cannot list.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct FailedMigration {
    /// The migration that failed.
    pub name: String,
    /// `true` when its transaction rolled the whole body back, so the database
    /// is exactly as it was before this migration started. `false` when the
    /// migration declared `run_in_transaction = false`: any statement that
    /// already succeeded still stands, and a re-run replays the whole body from
    /// the start. Whether that is safe depends on the body being idempotent,
    /// which nothing here can verify — so `false` means "inspect before
    /// re-running", not "re-run freely".
    pub rolled_back: bool,
}

/// What a migration run actually did against one database.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct MigrationReport {
    /// Migration names applied by this run, in the order they were applied.
    pub applied: Vec<String>,
    /// Migrations that were already recorded before this run started.
    pub already_applied: Vec<String>,
    /// Migrations another run applied *while this one was working* — recorded
    /// by the concurrent writer, skipped here. See [`apply_to_connection`].
    pub applied_concurrently: Vec<String>,
    /// Ledger versions no supplied set accounts for. See
    /// [`MigrationPlan::unrecognized`].
    pub unrecognized: Vec<String>,
    /// The migration the run stopped on, when it stopped. `None` on a run that
    /// finished.
    pub failed: Option<FailedMigration>,
    /// Whether this role may take the ledger lock at all.
    ///
    /// `false` means it lacks `UPDATE`/`DELETE`/`TRUNCATE` on the ledger, so
    /// *every* migration in this run was applied unserialized. This is the
    /// cause a grant can fix; see [`applied_unserialized`](Self::applied_unserialized)
    /// for what actually ran that way.
    pub ledger_lock_available: bool,
    /// Migrations this run applied **without** holding the ledger lock, in the
    /// order applied.
    ///
    /// Two causes, and they take different remedies, which is why this is a
    /// list rather than a second flag:
    ///
    /// * the role cannot lock the ledger at all
    ///   ([`ledger_lock_available`](Self::ledger_lock_available) is `false`) —
    ///   fixable with a grant, or by running migrators one at a time;
    /// * the migration declares `run_in_transaction = false`, so there is no
    ///   transaction for a lock to be held in. Inherent, and no grant helps:
    ///   run migrators one at a time.
    ///
    /// Either way a concurrent migrator can run the same body, so this is
    /// reported rather than only logged — a caller may be a CLI with no
    /// tracing subscriber installed, and a degradation nobody sees is not one
    /// anyone can act on. See [`apply_to_connection`].
    pub applied_unserialized: Vec<String>,
}

/// Connect to `database_url`.
///
/// # Errors
///
/// [`HarvestError::Database`] when the connection cannot be established. The
/// message is Diesel's; the DSN is **not** interpolated into it here, so a
/// caller is free to redact before printing.
pub async fn connect(database_url: &str) -> HarvestResult<AsyncPgConnection> {
    AsyncPgConnection::establish(database_url)
        .await
        .map_err(database_error)
}

/// Report what [`apply`] would do, without writing anything at all — the
/// ledger table is not created, so this is safe against a database an operator
/// is only inspecting.
///
/// # Errors
///
/// [`HarvestError::Config`] when the supplied set has a duplicate version;
/// [`HarvestError::Database`] when the connection or a query fails.
pub async fn plan(database_url: &str, scripts: &[MigrationScript]) -> HarvestResult<MigrationPlan> {
    validate_versions(scripts)?;
    let mut conn = connect(database_url).await?;
    plan_on_connection(&mut conn, scripts).await
}

/// [`plan`] against a connection the caller already holds.
///
/// # Errors
///
/// As [`plan`].
pub async fn plan_on_connection(
    conn: &mut AsyncPgConnection,
    scripts: &[MigrationScript],
) -> HarvestResult<MigrationPlan> {
    validate_versions(scripts)?;
    let ledger_exists = ledger_exists(conn).await?;
    let recorded = if ledger_exists {
        recorded_versions(conn).await?
    } else {
        BTreeSet::new()
    };
    Ok(build_plan(scripts, &recorded, ledger_exists))
}

/// Apply every pending migration in version order.
///
/// # Errors
///
/// A [`PartialMigration`], carrying the failure *and* everything applied before
/// it: [`HarvestError::Config`] when the supplied set has a duplicate version,
/// [`HarvestError::Database`] when the connection fails or a migration does —
/// naming the migration. That migration and its ledger row are rolled back
/// together; migrations applied earlier in the run stay applied, which is
/// exactly Diesel's behaviour and the reason migrations are ordered, so
/// [`PartialMigration::report`] is what says which ones those were.
pub async fn apply(
    database_url: &str,
    scripts: &[MigrationScript],
) -> Result<MigrationReport, Box<PartialMigration>> {
    validate_versions(scripts)?;
    let mut conn = connect(database_url).await?;
    apply_to_connection(&mut conn, scripts).await
}

/// [`apply`] against a connection the caller already holds.
///
/// # Concurrency
///
/// Each migration's transaction takes a `SHARE UPDATE EXCLUSIVE` lock on the
/// ledger table before running anything. A second migrator racing on the same
/// database therefore blocks there rather than half-way through the schema
/// change, and on acquiring the lock re-reads the ledger: finding the version
/// already committed, it skips the migration as already applied
/// ([`MigrationReport::applied_concurrently`]) instead of replaying DDL that
/// has already happened. The lock does not conflict with plain readers, so
/// `status` is unaffected, nor with `ROW EXCLUSIVE`, so it never blocks another
/// migrator's own ledger insert — see `apply_in_transaction` for why that
/// second property is what keeps this deadlock-free.
///
/// That serialization is best-effort by design, because it costs a privilege
/// Diesel's own bookkeeping never needs: Postgres requires `UPDATE`, `DELETE`
/// or `TRUNCATE` on a table to lock it in any mode above `ROW EXCLUSIVE`,
/// while reading the ledger and inserting into it need only `SELECT` and
/// `INSERT`. A least-privilege role granted exactly those two on a ledger it
/// does not own would be denied the lock before any body ran. Rather than
/// refuse a migration set `diesel migration run` would apply for that role,
/// the run probes the privilege once (`ledger_lock_is_permitted`), warns, and
/// proceeds unlocked — Diesel's behaviour exactly. Run migrators one at a time
/// against such a database.
///
/// The ledger row itself is **not** the contention point, because it is written
/// after the body — Diesel's order, which a body that consults the ledger can
/// depend on. See `apply_in_transaction`.
///
/// A `run_in_transaction = false` migration has no transaction to contend on,
/// so that protection does not apply to it: see `apply_without_transaction`
/// for what a race costs there.
///
/// # Errors
///
/// As [`apply`].
pub async fn apply_to_connection(
    conn: &mut AsyncPgConnection,
    scripts: &[MigrationScript],
) -> Result<MigrationReport, Box<PartialMigration>> {
    validate_versions(scripts)?;

    create_ledger(conn).await?;

    // Decided once for the run: every migration's transaction takes the same
    // lock, and the answer cannot change mid-run without someone revoking a
    // grant underneath a deploy.
    let lock_ledger = ledger_lock_is_permitted(conn).await?;
    if !lock_ledger {
        tracing::warn!(
            ledger = MIGRATION_LEDGER,
            mode = LEDGER_LOCK_MODE,
            "this role may not lock the migration ledger (needs UPDATE, DELETE or TRUNCATE on \
             it); applying without it, exactly as `diesel migration run` does. Concurrent \
             migrators against this database are not serialized — run one at a time."
        );
    }

    let recorded = recorded_versions(conn).await?;
    let plan = build_plan(scripts, &recorded, true);

    let mut report = MigrationReport {
        applied: Vec::new(),
        already_applied: plan.already_applied,
        applied_concurrently: Vec::new(),
        unrecognized: plan.unrecognized,
        failed: None,
        ledger_lock_available: lock_ledger,
        applied_unserialized: Vec::new(),
    };

    for script in &plan.pending {
        match apply_one(conn, script, lock_ledger).await {
            Ok(Applied::Yes) => {
                report.applied.push(script.name.clone());
                // Not `lock_ledger` alone: a `run_in_transaction = false`
                // migration has no transaction to hold a lock in, so it runs
                // unserialized however privileged the role is.
                if !lock_ledger || !script.run_in_transaction {
                    report.applied_unserialized.push(script.name.clone());
                }
            }
            Ok(Applied::Concurrently) => report.applied_concurrently.push(script.name.clone()),
            Err(error) => {
                // Say which it was: a rolled-back migration left nothing
                // behind, a non-transactional one may have applied part of
                // its body and needs the operator's eyes before a re-run.
                let aftermath = if script.run_in_transaction {
                    "rolled back"
                } else {
                    "NOT rolled back -- it declares run_in_transaction = false,                      so any statement that already succeeded still stands"
                };
                let failure = HarvestError::Database(format!(
                    "migration `{}` failed ({aftermath}; {} applied before it): {error}",
                    script.name,
                    report.applied.len()
                ));
                // The migrations that DID apply are committed, and are the
                // caller's to report -- they never travel in the message alone.
                // The failing migration is named structurally too: when it is
                // non-transactional, "nothing applied" and "part of it applied
                // and nobody can say which" look identical without this.
                report.failed = Some(FailedMigration {
                    name: script.name.clone(),
                    rolled_back: script.run_in_transaction,
                });
                return Err(Box::new(PartialMigration {
                    report,
                    error: failure,
                }));
            }
        }
    }

    Ok(report)
}

/// Create the ledger table, tolerating a concurrent creator.
///
/// `CREATE TABLE IF NOT EXISTS` is **not** race-free in Postgres: two sessions
/// creating the same table at once can leave the loser with a catalog unique
/// violation (`pg_type_typname_nsp_index`) rather than the "already exists,
/// skipping" notice. Against a brand-new database that is exactly the moment
/// two migrators are most likely to collide — and failing there would drop the
/// loser out before it ever reaches the per-version row contention that makes
/// the rest of this function safe.
///
/// So a failure is not taken at face value: if the ledger exists afterwards,
/// someone else created it and there is nothing left to do.
async fn create_ledger(conn: &mut AsyncPgConnection) -> HarvestResult<()> {
    let Err(error) = conn.batch_execute(CREATE_LEDGER_SQL).await else {
        return Ok(());
    };
    if ledger_exists(conn).await.unwrap_or(false) {
        return Ok(());
    }
    Err(HarvestError::Database(format!(
        "cannot create {MIGRATION_LEDGER}: {error}"
    )))
}

/// Outcome of one migration's transaction.
enum Applied {
    /// This run applied it.
    Yes,
    /// A concurrent migrator got there first; nothing was applied here.
    Concurrently,
}

/// Transaction-body failure, split so "already applied" is never inferred from
/// a database error.
///
/// `AlreadyRecorded` is raised only by an explicit ledger lookup under the
/// table lock, never by matching `UniqueViolation` on the transaction's result:
/// a migration whose body legitimately hits a unique constraint would otherwise
/// be classified as "someone else already applied this" and silently skipped
/// while reporting success.
enum ApplyError {
    /// The ledger already carries this version — a concurrent migrator
    /// committed it before this transaction took the lock.
    AlreadyRecorded,
    /// Anything else: the migration's own SQL, the lock, the lookup, or the
    /// ledger insert.
    Db(DieselError),
}

impl From<DieselError> for ApplyError {
    fn from(error: DieselError) -> Self {
        Self::Db(error)
    }
}

async fn apply_one(
    conn: &mut AsyncPgConnection,
    script: &MigrationScript,
    lock_ledger: bool,
) -> Result<Applied, DieselError> {
    if script.run_in_transaction {
        apply_in_transaction(conn, script, lock_ledger).await
    } else {
        apply_without_transaction(conn, script).await
    }
}

/// Apply a migration whose `metadata.toml` says `run_in_transaction = false`.
///
/// Postgres rejects `CREATE INDEX CONCURRENTLY` — the reason that key exists —
/// inside a transaction block, so there is no transaction to put the ledger row
/// in either. Two consequences, both Diesel's as well:
///
/// * **The body runs first, the ledger row second.** Recording a version before
///   a statement that may fail would leave the ledger claiming a migration that
///   never ran, and no later run can repair that. The reverse order can only
///   lose the *record* of a migration that did run, which a re-run fixes.
/// * **A partial failure stays partial.** With no transaction, statements that
///   already succeeded are not rolled back. The version is not recorded, so the
///   next run replays the whole body from the start — safe only if that body is
///   idempotent (`IF NOT EXISTS`). Nothing here can verify that, and a Diesel
///   migration set is under no obligation to be, so a failure leaves an
///   operator to inspect what stands before re-running: exactly the position
///   `diesel migration run` leaves them in.
async fn apply_without_transaction(
    conn: &mut AsyncPgConnection,
    script: &MigrationScript,
) -> Result<Applied, DieselError> {
    conn.batch_execute(&script.sql).await?;

    let recorded = diesel::sql_query(format!(
        "INSERT INTO {MIGRATION_LEDGER} (version) VALUES ($1)"
    ))
    .bind::<Text, _>(script.version.as_str())
    .execute(conn)
    .await;

    match recorded {
        Ok(_) => Ok(Applied::Yes),
        // A unique violation here *usually* means a concurrent migrator
        // recorded this version while the body was running. Both ran the DDL —
        // unavoidable without a transaction to contend on, and why such a
        // migration must be idempotent.
        //
        // Only the row itself proves that, though. This module is explicitly
        // pointed at ledgers it did not create, and one may carry an extra
        // constraint or an insert trigger that raises `unique_violation`
        // without recording anything. Taking the error at face value would
        // report a migration as applied, exit 0, and leave a schema change no
        // ledger row covers — so the next run replays the body, which for a
        // non-transactional migration is precisely the case that cannot be
        // rolled back. Confirm the version is there before saying so.
        Err(error @ DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => {
            if version_is_recorded(conn, &script.version).await? {
                Ok(Applied::Concurrently)
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

/// Whether this role may take [`LEDGER_LOCK_MODE`] on the ledger.
///
/// Postgres grants `LOCK TABLE` by mode: `ACCESS SHARE` needs `SELECT`,
/// `ROW EXCLUSIVE` needs one of `INSERT`/`UPDATE`/`DELETE`/`TRUNCATE`, and
/// every stronger mode — [`LEDGER_LOCK_MODE`] among them — needs `UPDATE`,
/// `DELETE` or `TRUNCATE`.
///
/// Diesel's bookkeeping only ever reads the ledger and inserts into it, so a
/// least-privilege deployment can grant its migration role exactly `SELECT` and
/// `INSERT` on a ledger that role does not own. Taking the lock unconditionally
/// would deny such a role before any body ran, on a migration set
/// `diesel migration run` applies for it perfectly well — breaking the
/// compatibility this module exists to keep, and doing so at the one moment an
/// operator is mid-deploy.
///
/// So the lock is conditional on this probe, which is itself a plain `SELECT`.
/// Where the privilege is absent the run falls back to exactly Diesel's
/// behaviour — no ledger lock, no serialization between concurrent migrators —
/// which is what that role has today and is strictly better than refusing to
/// run at all.
async fn ledger_lock_is_permitted(conn: &mut AsyncPgConnection) -> HarvestResult<bool> {
    // `has_table_privilege` with a comma-separated list is true if *any* of the
    // listed privileges is held.
    let rows: Vec<LockPermitted> = diesel::sql_query(
        "SELECT has_table_privilege($1, 'UPDATE, DELETE, TRUNCATE') AS lock_permitted",
    )
    .bind::<Text, _>(MIGRATION_LEDGER)
    .load(conn)
    .await
    .map_err(database_error)?;
    // `into_iter().next()`, not `rows.first()`: `RunQueryDsl::first` is in
    // scope and would win method resolution on the `Vec` itself.
    Ok(rows
        .into_iter()
        .next()
        .is_some_and(|row| row.lock_permitted))
}

/// One `lock_permitted` flag.
#[derive(diesel::QueryableByName)]
struct LockPermitted {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    lock_permitted: bool,
}

/// Whether `version` is present in the ledger.
///
/// Both apply paths decide "already applied" with this rather than inferring it
/// from a unique violation. A violation is ambiguous evidence: the migration's
/// own SQL can raise one, and on a ledger this module did not create some other
/// constraint can raise one while recording nothing. A row either is there or
/// is not.
async fn version_is_recorded(
    conn: &mut AsyncPgConnection,
    version: &str,
) -> Result<bool, DieselError> {
    let found: Vec<LedgerVersion> = diesel::sql_query(format!(
        "SELECT version FROM {MIGRATION_LEDGER} WHERE version = $1"
    ))
    .bind::<Text, _>(version)
    .load(conn)
    .await?;
    Ok(!found.is_empty())
}

/// Apply a migration inside one transaction, in Diesel's order: **body first,
/// ledger row second**.
///
/// That order is not cosmetic. A migration body may legitimately consult
/// `__diesel_schema_migrations` — conditioning DDL on its own version being
/// absent, say. Recording the version first makes it visible to the body
/// through the transaction's own writes, so such a migration skips its DDL and
/// still commits the version: an incomplete schema, permanently marked applied,
/// from a file `diesel migration run` would have applied correctly. Whatever
/// serializes concurrent migrators must therefore not be the ledger row itself.
///
/// A `SHARE UPDATE EXCLUSIVE` lock on the ledger table does the serializing
/// instead. It conflicts with itself, so a second migrator waits here — before
/// any DDL — rather than half-way through the schema change. It is held to the
/// end of the transaction and needs no advisory key, keeping this path out of
/// the keyspace shared with the claim path, `mutex`, `admission_gate` and the
/// scheduler.
///
/// The mode is chosen for what it does *not* conflict with. `ACCESS SHARE`
/// means `status` and any other plain reader is unaffected. `ROW EXCLUSIVE` —
/// what an `INSERT` takes — means this lock never blocks a *different*
/// migrator's ledger insert, and that is what keeps the arrangement
/// deadlock-free rather than merely convenient.
///
/// Diesel's own order is body-then-insert with no ledger lock, so a
/// `diesel migration run` (or Autumn's startup path) racing here takes its
/// locks in the opposite order to this function: the schema object first, the
/// ledger second. A self-conflicting mode that also blocked `ROW EXCLUSIVE` —
/// `SHARE ROW EXCLUSIVE`, the obvious first choice — would close that cycle.
/// This transaction would hold the ledger while waiting on a table the other
/// migrator has locked for DDL, and that migrator would then wait on the ledger
/// to record the body it just finished. Postgres would break the deadlock by
/// aborting one of them, and this command could report a migration as failed
/// while the competing migrator was in the middle of applying it correctly.
/// `SHARE UPDATE EXCLUSIVE` lets that insert through, so the other migrator
/// always finishes and releases its DDL lock, and this transaction proceeds.
///
/// What that costs is honest: a migrator that does not take this lock is not
/// serialized by it, so racing a Diesel migrator can still mean both run the
/// same body and one fails on its own DDL. Diesel offers no protection against
/// that either — the guarantee here is over concurrent `harvest migrate` runs,
/// and it is not worth turning a survivable race against a foreign migrator
/// into a deadlock to widen it.
///
/// Holding the lock, the ledger is re-checked before the body runs: the
/// migrator that waited must not replay DDL the winner already committed. That
/// check is a plain `SELECT`, so "already applied" is decided deterministically
/// rather than inferred from a unique violation — which the body's own SQL
/// could equally have raised.
async fn apply_in_transaction(
    conn: &mut AsyncPgConnection,
    script: &MigrationScript,
    lock_ledger: bool,
) -> Result<Applied, DieselError> {
    let version = script.version.clone();
    let sql = script.sql.clone();

    let result: Result<(), ApplyError> = Box::pin(conn.transaction(async |tx| {
        if lock_ledger {
            diesel::sql_query(format!(
                "LOCK TABLE {MIGRATION_LEDGER} IN {LEDGER_LOCK_MODE} MODE"
            ))
            .execute(tx)
            .await?;
        }

        if version_is_recorded(tx, version.as_str()).await? {
            return Err(ApplyError::AlreadyRecorded);
        }

        tx.batch_execute(&sql).await?;

        diesel::sql_query(format!(
            "INSERT INTO {MIGRATION_LEDGER} (version) VALUES ($1)"
        ))
        .bind::<Text, _>(version.as_str())
        .execute(tx)
        .await?;
        Ok(())
    }))
    .await;

    match result {
        Ok(()) => Ok(Applied::Yes),
        Err(ApplyError::AlreadyRecorded) => Ok(Applied::Concurrently),
        Err(ApplyError::Db(error)) => Err(error),
    }
}

/// One `version` column value.
#[derive(diesel::QueryableByName)]
struct LedgerVersion {
    #[diesel(sql_type = Text)]
    version: String,
}

/// One `exists` flag.
#[derive(diesel::QueryableByName)]
struct TableExists {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    table_exists: bool,
}

async fn ledger_exists(conn: &mut AsyncPgConnection) -> HarvestResult<bool> {
    let rows: Vec<TableExists> = diesel::sql_query(format!(
        "SELECT to_regclass('{MIGRATION_LEDGER}') IS NOT NULL AS table_exists"
    ))
    .load(conn)
    .await
    .map_err(database_error)?;
    // `into_iter().next()`, not `rows.first()`: `RunQueryDsl::first` is in
    // scope and would win method resolution on the `Vec` itself.
    Ok(rows.into_iter().next().is_some_and(|row| row.table_exists))
}

async fn recorded_versions(conn: &mut AsyncPgConnection) -> HarvestResult<BTreeSet<String>> {
    let rows: Vec<LedgerVersion> =
        diesel::sql_query(format!("SELECT version FROM {MIGRATION_LEDGER}"))
            .load(conn)
            .await
            .map_err(database_error)?;
    Ok(rows.into_iter().map(|row| row.version).collect())
}

/// Split a supplied set against the ledger. Pure, so the classification is
/// unit-testable without a database.
fn build_plan(
    scripts: &[MigrationScript],
    recorded: &BTreeSet<String>,
    ledger_exists: bool,
) -> MigrationPlan {
    let mut ordered: Vec<&MigrationScript> = scripts.iter().collect();
    ordered.sort_by(|a, b| a.version.cmp(&b.version));

    let mut already_applied = Vec::new();
    let mut pending = Vec::new();
    for script in ordered {
        if recorded.contains(&script.version) {
            already_applied.push(script.name.clone());
        } else {
            pending.push(script.clone());
        }
    }

    let known: BTreeSet<&str> = scripts.iter().map(|s| s.version.as_str()).collect();
    let unrecognized = recorded
        .iter()
        .filter(|version| !known.contains(version.as_str()))
        .cloned()
        .collect();

    MigrationPlan {
        already_applied,
        pending,
        unrecognized,
        ledger_exists,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EMBEDDED_MIGRATION_SCRIPTS, MigrationScript, build_plan, embedded, from_directory,
        parse_run_in_transaction, validate_versions,
    };
    use std::collections::BTreeSet;

    fn script(name: &str) -> MigrationScript {
        MigrationScript::new(name, "SELECT 1;").expect("valid migration name")
    }

    fn recorded(versions: &[&str]) -> BTreeSet<String> {
        versions.iter().map(|v| (*v).to_string()).collect()
    }

    #[test]
    fn version_is_the_component_before_the_first_underscore() {
        let script = script("20260409000000_harvest_initial");
        assert_eq!(script.version, "20260409000000");
        assert_eq!(script.name, "20260409000000_harvest_initial");
    }

    #[test]
    fn a_name_without_an_underscore_is_its_own_version() {
        // Diesel's rule, mirrored: `migrations/20260409000000/up.sql` records
        // version `20260409000000`.
        assert_eq!(script("20260409000000").version, "20260409000000");
    }

    #[test]
    fn a_name_with_no_version_prefix_is_rejected() {
        let error = MigrationScript::new("_harvest_initial", "SELECT 1;")
            .expect_err("a leading underscore leaves an empty version");
        assert!(
            error.to_string().contains("no version prefix"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_multibyte_version_is_measured_in_characters_not_bytes() {
        // `VARCHAR(50)` counts characters. A 26-character prefix of two-byte
        // characters is 52 bytes and records fine; refusing it would reject a
        // migration `diesel migration run` applies.
        let version = "é".repeat(26);
        assert_eq!(version.len(), 52);
        let script = MigrationScript::new(format!("{version}_harvest_x"), "SELECT 1;")
            .expect("26 characters is inside VARCHAR(50)");
        assert_eq!(script.version.chars().count(), 26);
    }

    #[test]
    fn a_version_longer_than_the_ledger_column_is_rejected() {
        // VARCHAR(50) would truncate or error at INSERT time -- mid-apply, with
        // the schema change already queued. Refuse while it is still cheap.
        let long = "1".repeat(51);
        let error = MigrationScript::new(format!("{long}_harvest_x"), "SELECT 1;")
            .expect_err("51 characters exceeds VARCHAR(50)");
        assert!(
            error.to_string().contains("VARCHAR(50)"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn plan_splits_supplied_migrations_by_the_ledger() {
        let scripts = vec![script("20260101000000_a"), script("20260102000000_b")];
        let plan = build_plan(&scripts, &recorded(&["20260101000000"]), true);

        assert_eq!(plan.already_applied, vec!["20260101000000_a".to_string()]);
        assert_eq!(
            plan.pending.iter().map(|s| &s.name).collect::<Vec<_>>(),
            vec!["20260102000000_b"]
        );
        assert!(plan.unrecognized.is_empty());
        assert!(plan.has_pending());
    }

    #[test]
    fn plan_orders_pending_migrations_by_version_not_input_order() {
        // The caller concatenates several sets (core + plugin + application);
        // whatever order they hand them over in, application order is the
        // ledger's order.
        let scripts = vec![
            script("20260103000000_c"),
            script("20260101000000_a"),
            script("20260102000000_b"),
        ];
        let plan = build_plan(&scripts, &BTreeSet::new(), true);
        assert_eq!(
            plan.pending.iter().map(|s| &s.name).collect::<Vec<_>>(),
            vec!["20260101000000_a", "20260102000000_b", "20260103000000_c"]
        );
    }

    #[test]
    fn plan_reports_ledger_rows_no_supplied_set_accounts_for() {
        // The database is ahead of this binary: a newer deploy already migrated
        // it. Reported, never removed.
        let scripts = vec![script("20260101000000_a")];
        let plan = build_plan(
            &scripts,
            &recorded(&["20260101000000", "20260901000000"]),
            true,
        );
        assert_eq!(plan.unrecognized, vec!["20260901000000".to_string()]);
        assert!(!plan.has_pending());
    }

    #[test]
    fn plan_on_a_database_with_no_ledger_has_everything_pending() {
        let scripts = vec![script("20260101000000_a")];
        let plan = build_plan(&scripts, &BTreeSet::new(), false);
        assert!(!plan.ledger_exists);
        assert_eq!(plan.pending.len(), 1);
    }

    #[test]
    fn duplicate_versions_across_sets_are_refused() {
        // Two sets that independently chose one version: Diesel would record
        // one and silently never run the other.
        let scripts = vec![
            script("20260101000000_core"),
            script("20260101000000_plugin"),
        ];
        let error = validate_versions(&scripts).expect_err("duplicate version must be refused");
        let message = error.to_string();
        assert!(message.contains("20260101000000_core"), "{message}");
        assert!(message.contains("20260101000000_plugin"), "{message}");
    }

    #[test]
    fn distinct_versions_validate() {
        let scripts = vec![script("20260101000000_a"), script("20260102000000_b")];
        validate_versions(&scripts).expect("distinct versions are fine");
    }

    #[test]
    fn embedded_set_is_well_formed_and_matches_the_build_manifest() {
        let scripts = embedded();
        assert!(
            !scripts.is_empty(),
            "the embedded set must not be empty -- build.rs generates it from migrations/"
        );
        validate_versions(&scripts).expect("Harvest's own migrations must not collide");

        // The same directory listing `MIGRATIONS`/`full_migrations_sql()` are
        // built from, so a migration that reaches one reaches all three.
        let manifest: Vec<&str> = env!("HARVEST_MIGRATIONS_LIST").split(',').collect();
        assert_eq!(scripts.len(), manifest.len());
        for name in &manifest {
            assert!(
                scripts.iter().any(|s| s.name == *name),
                "migration {name} is in HARVEST_MIGRATIONS_LIST but not in the embedded scripts"
            );
        }

        // Harvest's own migrations are all transactional today, and the
        // embedded loader would have panicked above on a `metadata.toml` it
        // could not read -- so this also proves every metadata slot parses.
        for script in &scripts {
            assert!(
                script.run_in_transaction,
                "migration {} declares run_in_transaction = false; that is \
                 supported, but `full_migrations_sql()` applies the whole \
                 bundle in one batch, so the test fixtures need revisiting too",
                script.name
            );
        }

        // Sorted by version, and every body non-empty: a blank up.sql would
        // record a version without changing anything, which no later run can
        // repair.
        let mut sorted = scripts.clone();
        sorted.sort_by(|a, b| a.version.cmp(&b.version));
        assert_eq!(scripts, sorted, "embedded() must return version order");
        for script in &scripts {
            assert!(
                !script.sql.trim().is_empty(),
                "migration {} has an empty up.sql",
                script.name
            );
        }
    }

    #[test]
    fn embedded_scripts_carry_the_same_sql_as_the_concatenated_bundle() {
        // Both artifacts come from build.rs; this pins them to each other so a
        // future edit cannot leave the applier and the test-fixture bundle
        // disagreeing about what a migration contains.
        let bundle = crate::full_migrations_sql();
        for (name, sql, _metadata) in EMBEDDED_MIGRATION_SCRIPTS {
            assert!(
                bundle.contains(&format!("-- harvest-migration: {name}\n")),
                "migration {name} is missing from the concatenated bundle"
            );
            assert!(
                bundle.contains(sql),
                "migration {name}'s up.sql body differs from the bundle's copy"
            );
        }
    }

    #[test]
    fn from_directory_reads_harvests_own_migrations() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let from_disk = from_directory(&dir).expect("Harvest's migrations/ must be readable");
        assert_eq!(
            from_disk,
            embedded(),
            "reading migrations/ from disk must agree with the embedded set"
        );
    }

    #[test]
    fn from_directory_rejects_a_migration_with_no_up_sql() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("20260101000000_incomplete")).expect("mkdir");
        let error = from_directory(dir.path()).expect_err("a missing up.sql must be loud");
        assert!(
            error.to_string().contains("up.sql"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn from_directory_skips_files_and_hidden_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("20260101000000_real")).expect("mkdir");
        std::fs::write(
            dir.path().join("20260101000000_real").join("up.sql"),
            "SELECT 1;",
        )
        .expect("write up.sql");
        std::fs::write(dir.path().join("README.md"), "not a migration").expect("write file");
        std::fs::create_dir(dir.path().join(".hidden")).expect("mkdir hidden");

        let scripts = from_directory(dir.path()).expect("directory must be readable");
        assert_eq!(
            scripts.iter().map(|s| &s.name).collect::<Vec<_>>(),
            vec!["20260101000000_real"]
        );
    }

    // ── Diesel's per-migration `metadata.toml` ─────────────────────────────

    #[test]
    fn a_migration_without_metadata_runs_in_a_transaction() {
        // Diesel's default, and what every migration in this repository wants.
        assert!(script("20260101000000_a").run_in_transaction);
        assert!(parse_run_in_transaction("m", "").expect("empty metadata is fine"));
    }

    #[test]
    fn run_in_transaction_false_is_honoured() {
        // The `CREATE INDEX CONCURRENTLY` case: Postgres rejects that statement
        // inside a transaction block, so ignoring this key would fail a
        // migration that `diesel migration run` applies happily.
        let script = MigrationScript::with_metadata(
            "20260101000000_a",
            "CREATE INDEX CONCURRENTLY idx ON t (c);",
            "run_in_transaction = false\n",
        )
        .expect("metadata parses");
        assert!(!script.run_in_transaction);
    }

    #[test]
    fn metadata_accepts_what_diesel_accepts() {
        // Every one of these is valid TOML that Diesel reads as `false`.
        for metadata in [
            "# a comment\nrun_in_transaction = false\n",
            "  run_in_transaction   =   false  \n",
            "\"run_in_transaction\" = false\n",
            "'run_in_transaction' = false\n",
            "run_in_transaction = false # trailing comment\n",
            // Unknown keys are IGNORED, not refused -- serde's default and
            // Diesel's behaviour, so a future Diesel option does not make such
            // a migration unapplyable here.
            "some_future_key = 3\nrun_in_transaction = false\n",
        ] {
            assert!(
                !parse_run_in_transaction("m", metadata).expect("metadata parses"),
                "should have read false from: {metadata:?}"
            );
        }
    }

    #[test]
    fn a_commented_out_key_leaves_the_default_alone() {
        assert!(
            parse_run_in_transaction("m", "# run_in_transaction = false\n")
                .expect("metadata parses")
        );
    }

    #[test]
    fn a_key_inside_a_table_is_not_the_top_level_key() {
        // `[section]` puts what follows inside that table, so the top-level
        // key is absent and the default stands -- exactly what Diesel sees.
        assert!(
            parse_run_in_transaction("m", "[section]\nrun_in_transaction = false\n")
                .expect("valid TOML")
        );
    }

    #[test]
    fn metadata_diesel_refuses_is_refused_here_too() {
        // The five leaks the hand-rolled subset had, plus a non-boolean value.
        // Each is a file `diesel migration run` will not read; accepting any of
        // them applied a migration on metadata nobody agreed about.
        for metadata in [
            // Not a boolean.
            "run_in_transaction = yes\n",
            "run_in_transaction = 0\n",
            "run_in_transaction = \"false\"\n",
            // Duplicate key -- invalid TOML whether or not the values agree.
            "run_in_transaction = true\nrun_in_transaction = false\n",
            "run_in_transaction = false\nrun_in_transaction = false\n",
            // Not `key = value` at all.
            "this is not toml\n",
            "run_in_transaction = false\nthis is not toml\n",
            // An unknown key whose VALUE is not valid TOML: the parse fails
            // before serde gets to ignore the field.
            "future_option = not-valid-toml\n",
            // Unbalanced quotes around the key.
            "\"run_in_transaction = false\n",
            "run_in_transaction\" = false\n",
            // Non-ASCII whitespace: TOML allows space and tab only.
            "\u{00a0}run_in_transaction = false\n",
        ] {
            parse_run_in_transaction("m", metadata)
                .expect_err(&format!("should have refused: {metadata:?}"));
        }
    }

    #[test]
    fn a_refusal_names_the_migration() {
        let error = parse_run_in_transaction("20260101000000_a", "run_in_transaction = yes\n")
            .expect_err("not a boolean");
        assert!(
            error.to_string().contains("20260101000000_a"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn from_directory_reads_a_migrations_metadata() {
        let dir = tempfile::tempdir().expect("tempdir");
        let migration = dir.path().join("20260101000000_concurrent");
        std::fs::create_dir(&migration).expect("mkdir");
        std::fs::write(
            migration.join("up.sql"),
            "CREATE INDEX CONCURRENTLY idx ON t (c);",
        )
        .expect("write up.sql");
        std::fs::write(
            migration.join("metadata.toml"),
            "run_in_transaction = false\n",
        )
        .expect("write metadata.toml");

        let scripts = from_directory(dir.path()).expect("directory reads");
        assert_eq!(scripts.len(), 1);
        assert!(
            !scripts[0].run_in_transaction,
            "an --include-dir migration's metadata must survive the load"
        );
    }

    #[test]
    fn from_directory_refuses_metadata_it_cannot_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let migration = dir.path().join("20260101000000_bad_metadata");
        std::fs::create_dir(&migration).expect("mkdir");
        std::fs::write(migration.join("up.sql"), "SELECT 1;").expect("write up.sql");
        std::fs::write(
            migration.join("metadata.toml"),
            "run_in_transaction = maybe\n",
        )
        .expect("write metadata.toml");

        let error = from_directory(dir.path()).expect_err("unreadable metadata must be loud");
        assert!(
            error.to_string().contains("20260101000000_bad_metadata"),
            "the error must name the migration: {error}"
        );
    }

    #[test]
    fn from_directory_names_a_missing_directory() {
        let error = from_directory(std::path::Path::new("/nonexistent/harvest/migrations"))
            .expect_err("a missing directory must be reported, not silently empty");
        assert!(
            error
                .to_string()
                .contains("/nonexistent/harvest/migrations"),
            "unexpected error: {error}"
        );
    }
}
