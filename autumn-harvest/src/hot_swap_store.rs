//! Postgres registry for hot-swappable workflow modules (issue #967).
//!
//! **R&D spike, behind the `hot-code-swap` Cargo feature.** The storage half of
//! the spike; the runtime half is [`crate::hot_swap`], and the written
//! deliverable is `docs/rnd/hot-code-swap.md`.
//!
//! # Postgres-only, no new infrastructure
//!
//! The "module registry" a worker discovers, fetches and verifies modules from
//! is one table, `harvest_workflow_modules`, in the database the engine
//! already owns. No object store, no OCI registry, no sidecar. That is a
//! deliberate constraint from the issue, and it costs nothing here. A workflow
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
//! Note what is *absent*: there is no "activate" step. The shipped build policy
//! and its percent ramp (issue #604) alone decide which module a new execution
//! lands on. The recorded `assigned_build_id` (issue #171) decides which module
//! an in-flight execution keeps. A second switch in this table would be a
//! second source of truth for the same question, and the two would eventually
//! disagree.
//!
//! [`ModuleRegistry::load_module`]: crate::hot_swap::ModuleRegistry::load_module
//! [`ModuleRegistry::unload_build`]: crate::hot_swap::ModuleRegistry::unload_build

use chrono::{DateTime, Utc};
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::{HarvestError, HarvestResult, database_error};
use std::sync::Arc;

use crate::hot_swap::{
    MAX_WORKFLOW_MODULE_BYTES, MAX_WORKFLOW_NAMES_PER_BUILD, ModuleDescriptor, ModuleRegistry,
    ModuleVerification, PreparedBinding, compute_module_hash, verify_module_bytes,
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
/// precisely so the code it will run cannot change underneath it. A rebindable
/// build id would void that guarantee. An in-flight execution pinned to `wf-v1`
/// would silently run whatever `wf-v1` most recently meant. That is exactly
/// the non-determinism build routing exists to prevent. Ship new code as a new
/// build id; that is what the ramp is for.
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
    // deployment uses. Stored unchecked, a bad signature reaches every worker
    // in the fleet at sync time. It is a fleet-wide outage, deferred until the
    // next sync runs.
    if signing_key.is_some() {
        verify_module_bytes(build_id, workflow_name, bytes, None, signature, signing_key)?;
    }

    let module_hash = compute_module_hash(bytes);

    // `DO NOTHING` rather than `DO UPDATE`: the conflict case is exactly the
    // rebind to refuse. Postgres decides it, so two concurrent publishers
    // cannot both believe they won. An empty RETURNING then means "a row
    // already existed", which the read below adjudicates.
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

    // Deliberately NOT `fetch_workflow_module`, which filters out retired rows.
    // The conflict here may well be with a *retired* row, and a retired binding
    // must still refuse a rebind. A read through the filter would report
    // "concurrently retired". The caller would then retry a publish that must
    // never succeed.
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
        // worker re-seeds a build it already published. Republishing the same
        // bytes revives a retired row. That is the whole reason retirement is
        // a tombstone rather than a delete.
        //
        // The SIGNATURE is written too (issue #967), and that is not cosmetic.
        // It is the only way to introduce or rotate signing for a build that
        // already exists. Clearing `retired_at` alone left the old (or NULL)
        // signature in place. A republish with a signature valid under a NEW
        // key returned success. Then `sync_build_into_registry(.., Some(new_key))`
        // rejected the very row it had just accepted. Key rotation would have
        // required a new build id, i.e. a deploy. This design exists to remove
        // precisely that coupling.
        //
        // Safe because the signature was verified against the caller's key
        // above, before any write: an unverifiable signature never reaches
        // here. The bytes are unchanged and the hash is re-derived from them.
        // So this rebinds the same content to a fresh attestation of the same
        // `(build_id, workflow_name, module_hash)` tuple. It cannot smuggle in
        // different code.
        //
        // `COALESCE`, not a bare assignment (issue #967). A publisher that
        // supplies no signature must not *erase* one. Mid-rollout, an older or
        // unsigned publisher could re-seed the same build and NULL a valid
        // attestation. Every worker that syncs with the signing key would then
        // reject the row as unsigned. A harmless duplicate publish would become
        // a fleet-wide refusal of a build that was correctly signed a moment
        // earlier. So an explicitly supplied (and therefore verified) signature
        // replaces the stored one, and `None` leaves it alone. Removing a
        // signature is deliberately not expressible here. It would be
        // indistinguishable from this accident, and retirement already exists
        // for withdrawing a module.
        diesel::sql_query(
            "UPDATE harvest_workflow_modules \
             SET retired_at = NULL, signature = COALESCE($3, signature) \
             WHERE build_id = $1 AND workflow_name = $2",
        )
        .bind::<diesel::sql_types::Text, _>(build_id)
        .bind::<diesel::sql_types::Text, _>(workflow_name)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(signature)
        .execute(conn)
        .await
        .map_err(database_error)?;
        return fetch_workflow_module_including_retired(conn, build_id, workflow_name)
            .await?
            .as_ref()
            .map(Into::into)
            .ok_or_else(|| {
                HarvestError::Config(format!(
                    "publish of `{workflow_name}` under build `{build_id}` was retired \
                     concurrently with its own republish; retry the publish"
                ))
            });
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
/// Private, and used only by the publish path's conflict adjudication. A
/// retired row is invisible to every load path, which is what retirement is
/// for. It is still the binding that must refuse a rebind.
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
/// still name it. Rolling *back* a bad deploy needs none of this. Repoint the
/// ramp, and new starts stop reaching the build immediately. Executions already
/// assigned to it finish on the code they started with.
///
/// # Soft, deliberately
///
/// This stamps `retired_at` rather than deleting the row, because the row **is**
/// the immutability guarantee. The primary key stops a build id being
/// re-pointed at different bytes. A hard `DELETE` would quietly allow a new
/// publish of `wf-v1` with new code. An execution still parked on a long timer
/// under `wf-v1` would then resume on logic it never started under. A retired
/// row keeps the binding as a tombstone. Republishing the *same* bytes revives
/// it; republishing *different* bytes is refused forever.
///
/// A retired module is invisible to [`sync_build_into_registry`] and to the
/// listings, so a worker will not load it. That is the operational effect
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
/// This is the whole worker-side lifecycle in one call. A worker runs it at
/// startup for its own build. It runs it again, with no restart, when an
/// operator publishes a new build it must begin serving.
///
/// `signing_key` is the operator's module-signing key. When `Some`, an unsigned
/// or badly-signed module is refused. When `None`, signatures are not required.
/// Content addressing still applies to every module either way.
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
/// wrong. It bound each module as it verified. A failing row then left the
/// worker advertising a build it could only half-serve. It claimed executions
/// for the workflows it had, and destroyed every execution for the one it did
/// not. A half-synced build is worse than an unsynced one.
///
/// # Memory
///
/// Each payload is fetched, compiled, and **dropped before the next is
/// fetched**. Peak residency in *source bytes* is then one module
/// (≤ [`MAX_WORKFLOW_MODULE_BYTES`]), not the whole build.
///
/// Fetching one row at a time is not on its own enough, and an earlier cut of
/// this function proved it. It read every payload into a `Vec` and only then
/// compiled, so the aggregate was resident anyway. The table's `CHECK` bounds a
/// single row at 32 MiB. Nothing bounds how many workflow names a build has,
/// and an attacker with `INSERT` chooses that, not the publish path. So a build
/// with enough valid rows could OOM the worker during sync.
///
/// The *compiled* artifacts are still held for the whole build, and that is
/// irreducible. Binding atomically means having every module in hand before
/// binding any (see above). Compiled code is materially smaller than the WASM
/// source it came from. Unlike the source, the worker keeps it resident
/// regardless.
///
/// # Compilation
///
/// Cranelift compilation is CPU-bound and is neither fuel- nor epoch-bounded.
/// So each module is compiled on [`tokio::task::spawn_blocking`] rather than
/// on an async worker thread. `wasm_store` set the same rule for guest
/// invocation.
///
/// # Errors
///
/// [`HarvestError::Config`] if the build has no modules, or if any module fails
/// verification or compilation. Also [`HarvestError::Config`] if the build
/// already binds a workflow name to different bytes in this process.
/// [`HarvestError::Database`] on a fetch failure.
pub async fn sync_build_into_registry(
    conn: &mut AsyncPgConnection,
    registry: &Arc<ModuleRegistry>,
    build_id: &str,
    signing_key: Option<&[u8]>,
) -> HarvestResult<Vec<ModuleDescriptor>> {
    let names = list_workflow_modules_for_build(conn, build_id).await?;
    if names.is_empty() {
        // A silent `Ok(vec![])` hides three causes: a typo'd build id, a
        // not-yet-published build, or a publish that landed on another shard's
        // database. Each of those then surfaces much later, as executions
        // failing one by one for want of a module. Fail where the cause is
        // still visible.
        return Err(HarvestError::Config(format!(
            "build `{build_id}` registers no workflow modules (in this shard's database);              nothing to load. Check the build id, and that the publish targeted this shard."
        )));
    }
    if names.len() > MAX_WORKFLOW_NAMES_PER_BUILD {
        // Refused before fetching or compiling a single module (issue #1345).
        // Source bytes are bounded by fetching one payload at a time. The
        // COMPILED artifacts stay resident for the whole batch until it
        // commits, since atomic binding needs every module in hand first.
        // Nothing before this check bounded how many of them accumulate.
        return Err(HarvestError::Config(format!(
            "build `{build_id}` registers {} workflow modules, over the \
             {MAX_WORKFLOW_NAMES_PER_BUILD}-name ceiling; refusing to sync it rather than risk \
             exhausting memory compiling all of them before the batch commits",
            names.len()
        )));
    }

    // Fetch, verify and compile ONE module at a time, then bind them all at once.
    // The obvious shape reads every payload into a `Vec`, then compiles. It
    // holds the whole build's source bytes resident simultaneously (issue #967).
    // The table's CHECK caps each row at 32 MiB, but nothing caps the number of
    // workflow names in a build. So the aggregate is unbounded, and a large
    // enough build OOMs the worker during sync. Here a payload is dropped as
    // soon as it is compiled. Peak residency in source bytes is one module
    // rather than the whole build.
    //
    // Atomicity is unaffected. `prepare_module` verifies and compiles without
    // binding, and `commit_prepared` binds the whole batch or none of it. So a
    // build whose third module fails still leaves the first two unbound. The
    // generation is read BEFORE the first prepare. Commit then catches an
    // `unload_build` that races the sync, rather than silently reviving a
    // retired build.
    let generation = registry.load_generation();
    let mut prepared: Vec<PreparedBinding> = Vec::with_capacity(names.len());
    for descriptor in &names {
        let row = fetch_workflow_module(conn, build_id, &descriptor.workflow_name)
            .await?
            .ok_or_else(|| {
                HarvestError::Config(format!(
                    "workflow module `{}` for build `{build_id}` vanished between listing and \
                     fetch; retry the sync",
                    descriptor.workflow_name
                ))
            })?;

        let registry = Arc::clone(registry);
        let build = build_id.to_string();
        let key = signing_key.map(<[u8]>::to_vec);
        // Cranelift compilation is CPU-bound and neither fuel- nor
        // epoch-bounded, so it runs off the async worker threads — the rule
        // `wasm_store` established for guest invocation.
        let one = tokio::task::spawn_blocking(move || {
            let mut verification = ModuleVerification::none().with_expected_hash(&row.module_hash);
            if let Some(signature) = row.signature.as_deref() {
                verification = verification.with_signature(signature);
            }
            if let Some(key) = key.as_deref() {
                verification = verification.with_signing_key(key);
            }
            registry
                .prepare_module(&build, &row.workflow_name, &row.module_bytes, &verification)
                .map_err(|e| {
                    HarvestError::Config(format!(
                        "refusing to load the workflow modules for build `{build}`: {e}"
                    ))
                })
            // `row` — and with it this module's source bytes — is dropped here,
            // before the next fetch.
        })
        .await
        .map_err(|join_err| {
            HarvestError::Config(format!(
                "workflow-module compilation task for build `{build_id}` failed to join: \
                 {join_err}"
            ))
        })??;
        prepared.push(one);
    }

    // Re-read the build's manifest and refuse to commit if it moved (issue
    // #967). The listing above is a snapshot. Publishing a NEW
    // `(build_id, workflow_name)` under an existing build is allowed by design.
    // The primary key only makes an existing name's bytes immutable. So the
    // sync could miss a module published mid-sync and still report success.
    // The worker would then lack a module for a workflow its build
    // compatibility admits. Every task for that workflow becomes a
    // capability-miss redelivery until someone syncs again. A retirement that
    // races the sync is the same hazard in the other direction.
    //
    // Failing is the right answer rather than quietly loading the subset. The
    // worker acts on the claim "this build is loaded" when it decides which
    // executions it can serve. The alternative is a worker confidently
    // half-serving a build. §8 argues that is worse than not serving it at all.
    //
    // BUT THIS DETECTS THE RACE, IT DOES NOT CLOSE IT. A publish landing after
    // this re-read and before `commit_prepared` below is still missed. The
    // window shrinks from "the whole fetch-and-compile pass", which is N
    // compilations long, to "one `spawn_blocking` dispatch". That is worth
    // having, since a publish realistically lands in the long window. But a
    // narrower race is not an absent one, and a second snapshot cannot make it
    // one.
    //
    // Nor would a REPEATABLE READ snapshot over the reads fix it. That makes the
    // semantics well-defined ("the build as of instant T"). It does not change
    // the operational fact: a worker that reports success still lacks a module
    // published after T. The race is not closable at this layer at all. A
    // build's membership is open-ended by design: publishing a NEW
    // `(build_id, workflow_name)` is allowed; only an existing name's bytes are
    // immutable. Closing it needs build-level sealing. `docs/rnd/hot-code-swap.md`
    // carries that forward as limitation 7, with the reason it is not done here.
    let after = list_workflow_modules_for_build(conn, build_id).await?;
    if after != names {
        return Err(HarvestError::Config(format!(
            "build `{build_id}`'s module set changed while it was being synced ({} modules at \
             the start, {} now); nothing was loaded. Retry the sync.",
            names.len(),
            after.len()
        )));
    }

    let registry = Arc::clone(registry);
    let build = build_id.to_string();
    tokio::task::spawn_blocking(move || {
        registry.commit_prepared(prepared, generation).map_err(|e| {
            HarvestError::Config(format!(
                "refusing to load the workflow modules for build `{build}`: {e}"
            ))
        })
    })
    .await
    .map_err(|join_err| {
        HarvestError::Config(format!(
            "workflow-module binding task for build `{build_id}` failed to join: {join_err}"
        ))
    })?
}
