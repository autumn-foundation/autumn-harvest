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

/// The only key Diesel's `metadata.toml` defines.
const RUN_IN_TRANSACTION_KEY: &str = "run_in_transaction";

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
        if version.len() > MAX_VERSION_LEN {
            return Err(HarvestError::Config(format!(
                "migration `{name}`: version `{version}` is {} characters, but \
                 {MIGRATION_LEDGER}.version is VARCHAR({MAX_VERSION_LEN})",
                version.len()
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
/// # Why not a TOML parser
///
/// Diesel's metadata file defines exactly one key, and pulling a TOML crate
/// into the engine core to read one boolean is not a trade worth making. This
/// reads that one key and **refuses everything else** — a table header, a line
/// that is not `<key> = <value>`, an unknown key, a non-boolean value, a
/// duplicate key — rather than guessing: guessing `false` runs DDL outside a
/// transaction the author never asked to leave, and guessing `true` fails the
/// `CONCURRENTLY` migration this exists to support. An absent file, or one that
/// only comments, keeps Diesel's default of `true`.
///
/// # Errors
///
/// [`HarvestError::Config`] naming the migration when the metadata contains a
/// table header, a line that is not `<key> = <value>`, a `run_in_transaction`
/// value that is not `true`/`false`, or the key more than once — a duplicate
/// key is invalid TOML whether or not the two values agree, so Diesel refuses
/// to parse the file at all.
///
/// Unknown keys are refused as well. Diesel's serde layer would ignore them,
/// but only after parsing the file as TOML — so an unknown key carrying an
/// invalid value fails there while a line-at-a-time reader would shrug. What
/// this accepts is therefore a strict subset of what Diesel accepts: blank
/// lines, `#` comments, and `run_in_transaction = true|false`. Every file it
/// reads, Diesel reads the same way; anything else is an error naming the line.
pub fn parse_run_in_transaction(name: &str, metadata: &str) -> HarvestResult<bool> {
    let mut found: Option<bool> = None;
    for raw in metadata.lines() {
        // `#` starts a comment. No key here can legitimately contain one, so
        // cutting at the first `#` cannot truncate a value we care about.
        let line = raw.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') {
            return Err(HarvestError::Config(format!(
                "migration `{name}`: {METADATA_FILE} contains a table section \
                 ({line}); only a top-level `{RUN_IN_TRANSACTION_KEY} = <bool>` \
                 is understood here -- apply this migration with the `diesel` CLI"
            )));
        }
        // A non-empty, non-comment line that is not `key = value` is not TOML
        // Diesel would accept either -- its parser rejects the whole file. It
        // is also how a multi-line construct (an array, an inline table
        // spanning lines) reaches this line-at-a-time reader, which is exactly
        // the shape this parser must not pretend to understand.
        let Some((key, value)) = line.split_once('=') else {
            return Err(HarvestError::Config(format!(
                "migration `{name}`: {METADATA_FILE} line `{line}` is not \
                 `<key> = <value>`; only a top-level \
                 `{RUN_IN_TRANSACTION_KEY} = <bool>` is understood here -- \
                 apply this migration with the `diesel` CLI"
            )));
        };
        // Accept the quoted spelling of the key; TOML allows `"key" = value`.
        let key = key.trim().trim_matches(['"', '\'']);
        if key != RUN_IN_TRANSACTION_KEY {
            // Refused, not skipped. Diesel's serde layer ignores unknown
            // fields, but its TOML parse runs FIRST -- so an unknown key with
            // an invalid value (`future_option = not-valid-toml`) fails there,
            // and skipping the line here would apply a migration on metadata
            // Diesel rejects. Validating such a value would mean implementing
            // TOML; refusing the key keeps what this accepts to a provable
            // subset of what Diesel accepts, which is the only side of that
            // trade that is safe to be wrong on.
            return Err(HarvestError::Config(format!(
                "migration `{name}`: {METADATA_FILE} sets `{key}`, which is not \
                 `{RUN_IN_TRANSACTION_KEY}` -- the only key understood here. \
                 Apply this migration with the `diesel` CLI"
            )));
        }
        let parsed = match value.trim() {
            "true" => true,
            "false" => false,
            other => {
                return Err(HarvestError::Config(format!(
                    "migration `{name}`: {METADATA_FILE} sets \
                     `{RUN_IN_TRANSACTION_KEY} = {other}`, which is not a boolean"
                )));
            }
        };
        // Any repeat, agreeing or not: TOML forbids a duplicate key outright,
        // so Diesel rejects the file rather than reading it. Accepting the
        // agreeing case would apply and record a migration whose metadata
        // `diesel migration run` refuses to parse at all.
        if found.is_some() {
            return Err(HarvestError::Config(format!(
                "migration `{name}`: {METADATA_FILE} sets \
                 `{RUN_IN_TRANSACTION_KEY}` more than once; duplicate keys are \
                 not valid TOML"
            )));
        }
        found = Some(parsed);
    }
    Ok(found.unwrap_or(true))
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
/// [`HarvestError::Config`] when the supplied set has a duplicate version;
/// [`HarvestError::Database`] when the connection fails, or when a migration
/// fails — naming the migration. That migration and its ledger row are rolled
/// back together; migrations applied earlier in the run stay applied, which is
/// exactly Diesel's behaviour and the reason migrations are ordered.
pub async fn apply(
    database_url: &str,
    scripts: &[MigrationScript],
) -> HarvestResult<MigrationReport> {
    validate_versions(scripts)?;
    let mut conn = connect(database_url).await?;
    apply_to_connection(&mut conn, scripts).await
}

/// [`apply`] against a connection the caller already holds.
///
/// # Concurrency
///
/// Each migration's ledger row is inserted **first** inside its transaction,
/// before the schema change runs. A second migrator racing on the same database
/// therefore blocks on that row rather than on the schema, and once the first
/// commits it sees a unique violation and skips the migration as already
/// applied ([`MigrationReport::applied_concurrently`]) instead of replaying DDL
/// that has already happened.
///
/// A `run_in_transaction = false` migration has no transaction to contend on,
/// so that protection does not apply to it: see [`apply_without_transaction`]
/// for what a race costs there.
///
/// # Errors
///
/// As [`apply`].
pub async fn apply_to_connection(
    conn: &mut AsyncPgConnection,
    scripts: &[MigrationScript],
) -> HarvestResult<MigrationReport> {
    validate_versions(scripts)?;

    conn.batch_execute(CREATE_LEDGER_SQL)
        .await
        .map_err(|e| HarvestError::Database(format!("cannot create {MIGRATION_LEDGER}: {e}")))?;

    let recorded = recorded_versions(conn).await?;
    let plan = build_plan(scripts, &recorded, true);

    let mut report = MigrationReport {
        applied: Vec::new(),
        already_applied: plan.already_applied,
        applied_concurrently: Vec::new(),
        unrecognized: plan.unrecognized,
    };

    for script in &plan.pending {
        match apply_one(conn, script).await {
            Ok(Applied::Yes) => report.applied.push(script.name.clone()),
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
                return Err(HarvestError::Database(format!(
                    "migration `{}` failed ({aftermath}; {} applied before it): {error}",
                    script.name,
                    report.applied.len()
                )));
            }
        }
    }

    Ok(report)
}

/// Outcome of one migration's transaction.
enum Applied {
    /// This run applied it.
    Yes,
    /// A concurrent migrator got there first; nothing was applied here.
    Concurrently,
}

/// Transaction-body failure, split so a unique violation raised by the **ledger
/// insert** is never confused with one raised by the migration's own SQL.
///
/// Matching `UniqueViolation` on the transaction's result alone would classify a
/// migration whose body legitimately hits a unique constraint as "someone else
/// already applied this" — silently skipping it and reporting success.
enum ApplyError {
    /// The ledger already carries this version: a concurrent migrator committed
    /// it while this transaction was open.
    AlreadyRecorded,
    /// Anything else — the migration's own SQL, or the insert failing for a
    /// reason other than the version already being there.
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
) -> Result<Applied, DieselError> {
    if script.run_in_transaction {
        apply_in_transaction(conn, script).await
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
///   next run replays the whole body — write these migrations idempotently
///   (`IF NOT EXISTS`), exactly as under `diesel migration run`.
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
        // A concurrent migrator recorded it while this body was running. Both
        // ran the DDL — unavoidable without a transaction to contend on, and
        // why such a migration must be idempotent.
        Err(DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => {
            Ok(Applied::Concurrently)
        }
        Err(error) => Err(error),
    }
}

async fn apply_in_transaction(
    conn: &mut AsyncPgConnection,
    script: &MigrationScript,
) -> Result<Applied, DieselError> {
    let version = script.version.clone();
    let sql = script.sql.clone();

    let result: Result<(), ApplyError> = Box::pin(conn.transaction(async |tx| {
        // Ledger row first: it is the row a concurrent migrator contends on,
        // so the loser waits here rather than half-way through the DDL.
        let recorded = diesel::sql_query(format!(
            "INSERT INTO {MIGRATION_LEDGER} (version) VALUES ($1)"
        ))
        .bind::<Text, _>(version.as_str())
        .execute(tx)
        .await;
        match recorded {
            Ok(_) => {}
            Err(DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => {
                return Err(ApplyError::AlreadyRecorded);
            }
            Err(error) => return Err(ApplyError::Db(error)),
        }
        tx.batch_execute(&sql).await?;
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
    fn metadata_tolerates_comments_whitespace_and_quoting() {
        for metadata in [
            "# a comment\nrun_in_transaction = false\n",
            "  run_in_transaction   =   false  \n",
            "\"run_in_transaction\" = false\n",
            "run_in_transaction = false # trailing comment\n",
        ] {
            assert!(
                !parse_run_in_transaction("m", metadata).expect("metadata parses"),
                "should have read false from: {metadata:?}"
            );
        }
    }

    #[test]
    fn a_commented_out_key_leaves_the_default_alone() {
        // Reading `false` out of a comment would run DDL outside a transaction
        // the author never asked to leave.
        assert!(
            parse_run_in_transaction("m", "# run_in_transaction = false\n")
                .expect("metadata parses")
        );
    }

    #[test]
    fn metadata_this_parser_cannot_read_is_refused_rather_than_guessed() {
        // Both directions of a wrong guess are bad -- `false` runs unprotected
        // DDL, `true` fails the CONCURRENTLY migration -- so refuse instead.
        for metadata in [
            "[section]\nrun_in_transaction = false\n",
            "run_in_transaction = yes\n",
            "run_in_transaction = 0\n",
            // Duplicate keys are invalid TOML whether or not they agree:
            // Diesel refuses to parse the file at all, so accepting the
            // agreeing case would apply a migration on metadata it rejects.
            "run_in_transaction = true\nrun_in_transaction = false\n",
            "run_in_transaction = false\nrun_in_transaction = false\n",
            // An unknown key: Diesel's serde layer would ignore it, but only
            // after parsing the file as TOML, so an invalid value fails there.
            // Refusing the key keeps what this accepts inside what Diesel
            // accepts without implementing TOML.
            "some_future_key = 3\nrun_in_transaction = false\n",
            "future_option = not-valid-toml\n",
            // Not `key = value` at all: Diesel's parser rejects the whole
            // file, so skipping the line would apply a migration on metadata
            // `diesel migration run` refuses to read.
            "this is not toml\n",
            "run_in_transaction = false\nthis is not toml\n",
        ] {
            parse_run_in_transaction("m", metadata)
                .expect_err(&format!("should have refused: {metadata:?}"));
        }
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
