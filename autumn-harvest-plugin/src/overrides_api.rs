//! `GET /admin/overrides`: every operator override in one list (issue #2388).
//!
//! The list has two parts. Schedule overrides (issues #2385 and #2386) live
//! on the shard of their schedule row, so the route reads every pool. The
//! run-default overrides (issue #2387) live on the default shard. A pool that
//! cannot be read is named, and the route still answers with the rest.
//!
//! `harvest overrides list` and the count on `GET /admin/config` read this
//! list.

use autumn_harvest::schedule_override::{OverrideSummary, list_overrides};
use autumn_web::AutumnError;
use autumn_web::reexports::axum;
use axum::extract::{Extension, Json};
use serde::Serialize;

use crate::api::{HarvestApiState, acquire_conn, map_error};
use crate::run_defaults_api::RunDefaultOverrideView;
use crate::shard_fanout::UnavailableShard;

/// One schedule override, with the shards that hold it.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleOverrideEntry {
    /// The shards whose pool holds the row. Two shards of one pool share it.
    pub shard_ids: Vec<i32>,
    /// The stored override.
    #[serde(flatten)]
    pub summary: OverrideSummary,
}

/// One run-default override.
#[derive(Debug, Clone, Serialize)]
pub struct RunDefaultOverrideEntry {
    /// The workflow type name. A DAG uses its own name.
    pub workflow_name: String,
    /// The stored override.
    #[serde(flatten)]
    pub stored: RunDefaultOverrideView,
}

/// The body of `GET /admin/overrides`.
#[derive(Debug, Clone, Serialize)]
pub struct OverridesView {
    /// Every schedule override, by kind and name.
    pub schedule_overrides: Vec<ScheduleOverrideEntry>,
    /// Every run-default override, by workflow name.
    pub run_default_overrides: Vec<RunDefaultOverrideEntry>,
    /// The shards that the route could not read. Their overrides are absent.
    pub unavailable_shards: Vec<UnavailableShard>,
}

/// The counts that `GET /admin/config` reports.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct OverrideCounts {
    /// The number of schedule overrides.
    pub schedule_overrides: usize,
    /// The number of run-default overrides.
    pub run_default_overrides: usize,
    /// The number of shards that could not be read, so a count may be low.
    pub unavailable_shards: usize,
}

impl OverridesView {
    /// The counts of this list.
    #[must_use]
    pub const fn counts(&self) -> OverrideCounts {
        OverrideCounts {
            schedule_overrides: self.schedule_overrides.len(),
            run_default_overrides: self.run_default_overrides.len(),
            unavailable_shards: self.unavailable_shards.len(),
        }
    }
}

/// Read every override.
///
/// Each pool is read once. A failed pool names each of its shards in
/// `unavailable_shards`.
///
/// # Errors
///
/// Returns an error only when the runtime has no storage pool.
pub async fn collect_overrides(api_state: &HarvestApiState) -> Result<OverridesView, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut view = OverridesView {
        schedule_overrides: Vec::new(),
        run_default_overrides: Vec::new(),
        unavailable_shards: Vec::new(),
    };
    for (shard_pool, shards) in pool.sharded_pool().pool_groups() {
        let shard_ids: Vec<i32> = shards.iter().map(|shard| shard.as_i32()).collect();
        let listed = match acquire_conn(shard_pool).await {
            Ok(mut conn) => list_overrides(&mut conn)
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        match listed {
            Ok(rows) => view
                .schedule_overrides
                .extend(rows.into_iter().map(|summary| ScheduleOverrideEntry {
                    shard_ids: shard_ids.clone(),
                    summary,
                })),
            Err(reason) => view
                .unavailable_shards
                .extend(shard_ids.iter().map(|&shard_id| UnavailableShard {
                    shard_id,
                    reason: reason.clone(),
                })),
        }
    }
    view.schedule_overrides.sort_by(|a, b| {
        (&a.summary.kind, &a.summary.name, &a.shard_ids).cmp(&(
            &b.summary.kind,
            &b.summary.name,
            &b.shard_ids,
        ))
    });

    let default_shard = pool.sharded_pool().default_shard().as_i32();
    let loaded = match acquire_conn(pool.default_pool()).await {
        Ok(mut conn) => autumn_harvest::run_defaults::load_run_default_overrides(&mut conn)
            .await
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    };
    match loaded {
        Ok(rows) => {
            view.run_default_overrides = rows
                .into_iter()
                .map(|stored| RunDefaultOverrideEntry {
                    workflow_name: stored.workflow_name.clone(),
                    stored: stored.into(),
                })
                .collect();
            view.run_default_overrides
                .sort_by(|a, b| a.workflow_name.cmp(&b.workflow_name));
        }
        Err(reason) => {
            if !view
                .unavailable_shards
                .iter()
                .any(|shard| shard.shard_id == default_shard)
            {
                view.unavailable_shards.push(UnavailableShard {
                    shard_id: default_shard,
                    reason,
                });
            }
        }
    }
    view.unavailable_shards.sort_by_key(|shard| shard.shard_id);
    Ok(view)
}

/// `GET /admin/overrides`.
pub async fn get_overrides(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<OverridesView>, AutumnError> {
    collect_overrides(&api_state).await.map(Json)
}
