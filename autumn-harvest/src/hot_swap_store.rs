//! Postgres registry for hot-swappable workflow modules (issue #967).
//!
//! **R&D spike, behind the `hot-code-swap` Cargo feature.** The storage half of
//! the spike; the runtime half is [`crate::hot_swap`], and the written
//! deliverable is `docs/rnd/hot-code-swap.md`.
//!
//! # Postgres-only, no new infrastructure
//!
//! The "module registry" a worker discovers, fetches and verifies modules from
//! is one table — `harvest_workflow_modules` — in the database the engine
//! already owns. No object store, no OCI registry, no sidecar. That is a
//! deliberate constraint from the issue, and it costs nothing here: a workflow
//! module is a small artefact, and a worker already holds a connection.
//!
//! # The lifecycle
//!
//! | Step | API | What enforces safety |
//! |------|-----|----------------------|
//! | discover | [`list_workflow_modules_for_build`] | — |
//! | fetch | [`fetch_workflow_module`] | — |
//! | verify | [`sync_build_into_registry`] | SHA-256 content check, then HMAC signature |
//! | load | [`sync_build_into_registry`] | [`ModuleRegistry::load_module`] compiles and binds |
//! | unload | [`ModuleRegistry::unload_build`] | `Arc` keeps in-flight invocations valid |
//! | retire | [`retire_build_modules`] | gated by `build_reachability(...).safe_to_retire` |
//!
//! Note what is *absent*: there is no "activate" step. Which module a new
//! execution lands on is decided entirely by the shipped build policy and its
//! percent ramp (issue #604), and which module an in-flight execution keeps is
//! decided by its recorded `assigned_build_id` (issue #171). A second switch in
//! this table would be a second source of truth for the same question, and the
//! two would eventually disagree.
//!
//! [`ModuleRegistry::load_module`]: crate::hot_swap::ModuleRegistry::load_module
//! [`ModuleRegistry::unload_build`]: crate::hot_swap::ModuleRegistry::unload_build

use chrono::{DateTime, Utc};
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::{HarvestError, HarvestResult, database_error};
use std::sync::Arc;

use crate::hot_swap::{
    MAX_WORKFLOW_MODULE_BYTES, ModuleDescriptor, ModuleRegistry, ModuleVerification,
    compute_module_hash, verify_module_bytes,
};

/// One `harvest_workflow_modules` row, payload included.
#[derive(Debug, Clone, diesel::QueryableByName)]
pub struct WorkflowModuleRow {
    /// The `BuildId` this module serves.
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub build_id: String,
    /// The workflow type name it implements.
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub workflow_name: String,
    /// Lowercase-hex SHA-256 recorded at publish time.
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub module_hash: String,
    /// The module payload.
    #[diesel(sql_type = diesel::sql_types::Binary)]
    pub module_bytes: Vec<u8>,
    /// Detached signature over `module_hash`, if the publisher supplied one.
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    pub signature: Option<String>,
    /// When the row was written.
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub published_at: DateTime<Utc>,
}

impl From<&WorkflowModuleRow> for ModuleDescriptor {
    fn from(row: &WorkflowModuleRow) -> Self {
        Self {
            build_id: row.build_id.clone(),
            workflow_name: row.workflow_name.clone(),
            module_hash: row.module_hash.clone(),
        }
    }
}

/// A row without its payload — what a listing returns, so enumerating a fleet's
/// modules never drags megabytes of WASM through the connection.
#[derive(Debug, Clone, diesel::QueryableByName)]
struct WorkflowModuleMetaRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    build_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    module_hash: String,
}

impl From<WorkflowModuleMetaRow> for ModuleDescriptor {
    fn from(row: WorkflowModuleMetaRow) -> Self {
        Self {
            build_id: row.build_id,
            workflow_name: row.workflow_name,
            module_hash: row.module_hash,
        }
    }
}

const MODULE_COLUMNS: &str =
    "build_id, workflow_name, module_hash, module_bytes, signature, published_at";
const META_COLUMNS: &str = "build_id, workflow_name, module_hash";

/// Publish `bytes` as `workflow_name`'s module under `build_id`.
///
/// # Immutability
///
/// A `(build_id, workflow_name)` pair binds to one module, permanently.
/// Publishing the *same* bytes again is idempotent; publishing *different* bytes
/// is refused with [`HarvestError::Config`].
///
/// This is not defensive strictness — it is the same invariant the engine
/// already relies on. An execution's `assigned_build_id` is fixed at start time
/// precisely so the code it will run cannot change underneath it. If a build id
/// could be rebound, that guarantee would evaporate: an in-flight execution
/// pinned to `wf-v1` would silently start running whatever `wf-v1` most recently
/// meant, which is exactly the non-determinism build routing exists to prevent.
/// Ship new code as a new build id; that is what the ramp is for.
///
/// # Errors
///
/// [`HarvestError::Config`] if the module is empty, exceeds
/// [`MAX_WORKFLOW_MODULE_BYTES`], or would rebind an existing build id;
/// [`HarvestError::Database`] on a connection or statement failure.
pub async fn publish_workflow_module(
    conn: &mut AsyncPgConnection,
    build_id: &str,
    workflow_name: &str,
    bytes: &[u8],
    signature: Option<&str>,
    signing_key: Option<&[u8]>,
) -> HarvestResult<ModuleDescriptor> {
    // Refuse oversized/empty payloads before spending a round trip on them.
    if bytes.is_empty() {
        return Err(HarvestError::Config(format!(
            "workflow module for `{workflow_name}` under build `{build_id}` is empty"
        )));
    }
    if bytes.len() > MAX_WORKFLOW_MODULE_BYTES {
        return Err(HarvestError::Config(format!(
            "workflow module for `{workflow_name}` under build `{build_id}` is {} bytes, \
             exceeding the maximum of {MAX_WORKFLOW_MODULE_BYTES} bytes",
            bytes.len()
        )));
    }

    // Verify the signature the publisher offers *now*, against the key this
    // deployment uses, rather than storing it unchecked and letting every
    // worker in the fleet discover it is wrong at sync time. A bad signature
    // stored here is a fleet-wide outage deferred by however long it takes the
    // next sync to run.
    if signing_key.is_some() {
        verify_module_bytes(build_id, workflow_name, bytes, None, signature, signing_key)?;
    }

    let module_hash = compute_module_hash(bytes);

    // `DO NOTHING` rather than `DO UPDATE`: the conflict case is exactly the
    // rebind we must refuse, and letting Postgres decide it keeps two concurrent
    // publishers from both believing they won. An empty RETURNING then means
    // "a row already existed", which the read below adjudicates.
    let inserted: Vec<WorkflowModuleMetaRow> = diesel::sql_query(format!(
        "INSERT INTO harvest_workflow_modules \
             (build_id, workflow_name, module_hash, module_bytes, signature) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (build_id, workflow_name) DO NOTHING \
         RETURNING {META_COLUMNS}"
    ))
    .bind::<diesel::sql_types::Text, _>(build_id)
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(&module_hash)
    .bind::<diesel::sql_types::Binary, _>(bytes)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(signature)
    .load(conn)
    .await
    .map_err(database_error)?;

    if let Some(row) = inserted.into_iter().next() {
        return Ok(row.into());
    }

    // Deliberately NOT `fetch_workflow_module`, which filters out retired rows:
    // the conflict we are adjudicating may well be with a *retired* row, and a
    // retired binding is exactly the one that must still refuse a rebind. Reading
    // through the filter here would report "concurrently retired" and invite the
    // caller to retry a publish that must never succeed.
    let existing = fetch_workflow_module_including_retired(conn, build_id, workflow_name)
        .await?
        .ok_or_else(|| {
            // The insert conflicted, so a row existed; if it is gone now a
            // concurrent retirement removed it. Surfacing that as a config
            // error (rather than retrying forever) keeps the operator in the
            // loop about a registry being mutated under them.
            HarvestError::Config(format!(
                "publish of `{workflow_name}` under build `{build_id}` conflicted with a row \
                 that was concurrently retired; retry the publish"
            ))
        })?;

    if existing.module_hash == module_hash {
        // Idempotent republish of identical bytes: the common case when a
        // worker re-seeds a build it already published. If the row was retired,
        // republishing the same bytes revives it — which is the whole reason
        // retirement is a tombstone rather than a delete.
        diesel::sql_query(
            "UPDATE harvest_workflow_modules SET retired_at = NULL \
             WHERE build_id = $1 AND workflow_name = $2 AND retired_at IS NOT NULL",
        )
        .bind::<diesel::sql_types::Text, _>(build_id)
        .bind::<diesel::sql_types::Text, _>(workflow_name)
        .execute(conn)
        .await
        .map_err(database_error)?;
        return Ok((&existing).into());
    }

    Err(HarvestError::Config(format!(
        "build `{build_id}` already binds workflow `{workflow_name}` to module \
         {} and a build id's module binding is immutable; publish the new code under a new \
         build id and ramp to it (attempted: {module_hash})",
        existing.module_hash
    )))
}

/// Fetch one module, payload included.
///
/// # Errors
///
/// [`HarvestError::Database`] on failure.
pub async fn fetch_workflow_module(
    conn: &mut AsyncPgConnection,
    build_id: &str,
    workflow_name: &str,
) -> HarvestResult<Option<WorkflowModuleRow>> {
    let rows: Vec<WorkflowModuleRow> = diesel::sql_query(format!(
        "SELECT {MODULE_COLUMNS} FROM harvest_workflow_modules \
         WHERE build_id = $1 AND workflow_name = $2 AND retired_at IS NULL"
    ))
    .bind::<diesel::sql_types::Text, _>(build_id)
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .load(conn)
    .await
    .map_err(database_error)?;
    Ok(rows.into_iter().next())
}

/// Fetch one module **including** a retired one.
///
/// Private, and used only by the publish path's conflict adjudication: a retired
/// row is invisible to every load path (that is what retirement is for) but is
/// still the binding that must refuse a rebind.
async fn fetch_workflow_module_including_retired(
    conn: &mut AsyncPgConnection,
    build_id: &str,
    workflow_name: &str,
) -> HarvestResult<Option<WorkflowModuleRow>> {
    let rows: Vec<WorkflowModuleRow> = diesel::sql_query(format!(
        "SELECT {MODULE_COLUMNS} FROM harvest_workflow_modules \
         WHERE build_id = $1 AND workflow_name = $2"
    ))
    .bind::<diesel::sql_types::Text, _>(build_id)
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .load(conn)
    .await
    .map_err(database_error)?;
    Ok(rows.into_iter().next())
}

/// List every module registered under `build_id`, without payloads.
///
/// # Errors
///
/// [`HarvestError::Database`] on failure.
pub async fn list_workflow_modules_for_build(
    conn: &mut AsyncPgConnection,
    build_id: &str,
) -> HarvestResult<Vec<ModuleDescriptor>> {
    let rows: Vec<WorkflowModuleMetaRow> = diesel::sql_query(format!(
        "SELECT {META_COLUMNS} FROM harvest_workflow_modules \
         WHERE build_id = $1 AND retired_at IS NULL ORDER BY workflow_name"
    ))
    .bind::<diesel::sql_types::Text, _>(build_id)
    .load(conn)
    .await
    .map_err(database_error)?;
    Ok(rows.into_iter().map(Into::into).collect())
}

/// List every registered module in the shard, without payloads.
///
/// # Errors
///
/// [`HarvestError::Database`] on failure.
pub async fn list_workflow_modules(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Vec<ModuleDescriptor>> {
    let rows: Vec<WorkflowModuleMetaRow> = diesel::sql_query(format!(
        "SELECT {META_COLUMNS} FROM harvest_workflow_modules \
         WHERE retired_at IS NULL ORDER BY build_id, workflow_name"
    ))
    .load(conn)
    .await
    .map_err(database_error)?;
    Ok(rows.into_iter().map(Into::into).collect())
}

/// Retire every module registered under `build_id`, returning how many rows were
/// retired.
///
/// Retirement, not rollback. Call it once
/// [`build_reachability`](crate::build_routing::build_reachability) reports
/// `safe_to_retire` for the build — i.e. no open executions and no pending tasks
/// still name it. Rolling *back* a bad deploy needs none of this: repoint the
/// ramp and new starts stop reaching the build immediately, while executions
/// already assigned to it finish on the code they started with.
///
/// # Soft, deliberately
///
/// This stamps `retired_at` rather than deleting the row, because the row **is**
/// the immutability guarantee. The primary key is what stops a build id being
/// re-pointed at different bytes, so a hard `DELETE` would quietly restore the
/// ability to publish `wf-v1` again with new code — and any execution still
/// parked on a long timer under `wf-v1` would resume on logic it never started
/// under. A retired row keeps the binding as a tombstone: republishing the
/// *same* bytes revives it, republishing *different* bytes is refused forever.
///
/// A retired module is invisible to [`sync_build_into_registry`] and to the
/// listings, so a worker will not load it — which is the operational effect
/// retirement is for.
///
/// # Errors
///
/// [`HarvestError::Database`] on failure.
pub async fn retire_build_modules(
    conn: &mut AsyncPgConnection,
    build_id: &str,
) -> HarvestResult<usize> {
    diesel::sql_query(
        "UPDATE harvest_workflow_modules SET retired_at = now() \
         WHERE build_id = $1 AND retired_at IS NULL",
    )
    .bind::<diesel::sql_types::Text, _>(build_id)
    .execute(conn)
    .await
    .map_err(database_error)
}

/// Discover, fetch, verify and load every module registered under `build_id`.
///
/// This is the whole worker-side lifecycle in one call: it is what a worker runs
/// at startup for its own build, and what it runs again — with no restart — when
/// an operator publishes a new build it must begin serving.
///
/// `signing_key` is the operator's module-signing key. When `Some`, an unsigned
/// or badly-signed module is refused; when `None`, signatures are not required
/// (content addressing still applies to every module either way).
///
/// # Fail-closed, and genuinely whole-build
///
/// Every module is fetched, verified and compiled first; only then are they all
/// bound, under one lock, by
/// [`ModuleRegistry::load_modules`](crate::hot_swap::ModuleRegistry::load_modules).
/// A build whose third module fails verification therefore leaves the first two
/// **unbound**.
///
/// That ordering is the whole point, and an earlier per-module loop got it
/// wrong: it bound each module as it verified, so a failing row left the worker
/// advertising a build it could only half-serve — claiming executions for the
/// workflows it had, and destroying every execution for the one it did not. A
/// half-synced build is worse than an unsynced one.
///
/// # Memory
///
/// Payloads are fetched **one at a time**, so peak host residency is one module
/// (≤ [`MAX_WORKFLOW_MODULE_BYTES`]) rather than the whole build. Selecting a
/// build's payloads in one query would let a single oversized or numerous set of
/// rows — which an attacker with `INSERT` chooses, not the publish path —
/// materialise unbounded bytes before any ceiling was consulted.
///
/// # Compilation
///
/// Cranelift compilation is CPU-bound and is neither fuel- nor epoch-bounded, so
/// it runs on [`tokio::task::spawn_blocking`] rather than occupying an async
/// worker thread — the same rule `wasm_store` established for guest invocation.
///
/// # Errors
///
/// [`HarvestError::Config`] if the build has no modules, if any module fails
/// verification or compilation, or if the build already binds a workflow name to
/// different bytes in this process; [`HarvestError::Database`] on a fetch
/// failure.
pub async fn sync_build_into_registry(
    conn: &mut AsyncPgConnection,
    registry: &Arc<ModuleRegistry>,
    build_id: &str,
    signing_key: Option<&[u8]>,
) -> HarvestResult<Vec<ModuleDescriptor>> {
    let names = list_workflow_modules_for_build(conn, build_id).await?;
    if names.is_empty() {
        // A silent `Ok(vec![])` here is how a typo'd build id, a not-yet-published
        // build, or — in a sharded deployment — a publish that landed on another
        // shard's database all look. Each of those then surfaces much later, as
        // executions failing one by one for want of a module. Fail where the
        // cause is still visible.
        return Err(HarvestError::Config(format!(
            "build `{build_id}` registers no workflow modules (in this shard's database);              nothing to load. Check the build id, and that the publish targeted this shard."
        )));
    }

    // Fetch + verify + compile everything before binding anything.
    let mut prepared: Vec<(String, Vec<u8>, Option<String>, String)> =
        Vec::with_capacity(names.len());
    for descriptor in &names {
        let row = fetch_workflow_module(conn, build_id, &descriptor.workflow_name)
            .await?
            .ok_or_else(|| {
                HarvestError::Config(format!(
                    "workflow module `{}` for build `{build_id}` vanished between listing and                      fetch; retry the sync",
                    descriptor.workflow_name
                ))
            })?;
        prepared.push((
            row.workflow_name,
            row.module_bytes,
            row.signature,
            row.module_hash,
        ));
    }

    let registry = Arc::clone(registry);
    let build = build_id.to_string();
    let key = signing_key.map(<[u8]>::to_vec);
    tokio::task::spawn_blocking(move || {
        let batch: Vec<(&str, &str, &[u8], ModuleVerification<'_>)> = prepared
            .iter()
            .map(|(workflow_name, bytes, signature, hash)| {
                let mut verification = ModuleVerification::none().with_expected_hash(hash);
                if let Some(signature) = signature.as_deref() {
                    verification = verification.with_signature(signature);
                }
                if let Some(key) = key.as_deref() {
                    verification = verification.with_signing_key(key);
                }
                (
                    build.as_str(),
                    workflow_name.as_str(),
                    bytes.as_slice(),
                    verification,
                )
            })
            .collect();
        registry.load_modules(&batch).map_err(|e| {
            HarvestError::Config(format!(
                "refusing to load the workflow modules for build `{build}`: {e}"
            ))
        })
    })
    .await
    .map_err(|join_err| {
        HarvestError::Config(format!(
            "workflow-module compilation task for build `{build_id}` failed to join: {join_err}"
        ))
    })?
}
