//! Performance evidence generators (formerly `*/scripts/*_perf_repro.sh`).

use std::process::ExitCode;

use smop::prelude::*;

pub mod broken_session_scan;
pub mod build_reachability_fanout;
pub mod capability_labels_claim;
pub mod claim_batched_seek_and_refine;
pub mod classify_dhat_allocations;
pub mod concurrency_key_claim;
pub mod deep_backlog_ledger;
pub mod external_outbox_scan;
pub mod external_outbox_scan_matrix;
pub mod history_bloat;
pub mod history_ceiling_claim;
pub mod notify_stage_probe;
pub mod parent_close_cascade_unfinished_handlers;
pub mod pause_array_size_claim;
pub mod pool_recycle_ping;
pub mod queue_pause_claim;
pub mod quota_history_bytes;
pub mod quota_reconcile_candidate_scan;
pub mod schedule_to_close_claim;
pub mod sticky_routing_claim;
/// Helpers that several commands of this group share.
pub mod support;
pub mod usage_report_activity_lookback;
pub mod worker_session_claim;

/// Each subcommand of `cargo xtask perf`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Port of `autumn-harvest/scripts/broken_session_scan_perf_repro.sh`.
    BrokenSessionScan(broken_session_scan::Args),
    /// Port of `autumn-harvest/scripts/build_reachability_fanout_perf_repro.sh`.
    BuildReachabilityFanout(build_reachability_fanout::Args),
    /// Port of `autumn-harvest/scripts/capability_labels_claim_perf_repro.sh`.
    CapabilityLabelsClaim(capability_labels_claim::Args),
    /// Port of `autumn-harvest/scripts/claim_batched_seek_and_refine_perf_repro.sh`.
    ClaimBatchedSeekAndRefine(claim_batched_seek_and_refine::Args),
    /// Port of `autumn-harvest/scripts/concurrency_key_claim_perf_repro.sh`.
    ConcurrencyKeyClaim(concurrency_key_claim::Args),
    /// Port of `autumn-harvest/scripts/deep_backlog_ledger_repro.sh`.
    DeepBacklogLedger(deep_backlog_ledger::Args),
    /// Port of `autumn-harvest/scripts/external_outbox_scan_matrix.sh`.
    ExternalOutboxScanMatrix(external_outbox_scan_matrix::Args),
    /// Port of `autumn-harvest/scripts/external_outbox_scan_perf_repro.sh`.
    ExternalOutboxScan(external_outbox_scan::Args),
    /// Port of `autumn-harvest/scripts/history_ceiling_claim_perf_repro.sh`.
    HistoryCeilingClaim(history_ceiling_claim::Args),
    /// Port of `autumn-harvest/scripts/notify_stage_probe_perf_repro.sh`.
    NotifyStageProbe(notify_stage_probe::Args),
    /// Port of `autumn-harvest/scripts/parent_close_cascade_unfinished_handlers_perf_repro.sh`.
    ParentCloseCascadeUnfinishedHandlers(parent_close_cascade_unfinished_handlers::Args),
    /// Port of `autumn-harvest/scripts/pause_array_size_claim_perf_repro.sh`.
    PauseArraySizeClaim(pause_array_size_claim::Args),
    /// Port of `autumn-harvest/scripts/pool_recycle_ping_perf_repro.sh`.
    PoolRecyclePing(pool_recycle_ping::Args),
    /// Port of `autumn-harvest/scripts/queue_pause_claim_perf_repro.sh`.
    QueuePauseClaim(queue_pause_claim::Args),
    /// Port of `autumn-harvest/scripts/quota_history_bytes_perf_repro.sh`.
    QuotaHistoryBytes(quota_history_bytes::Args),
    /// Port of `autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh`.
    QuotaReconcileCandidateScan(quota_reconcile_candidate_scan::Args),
    /// Port of `autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh`.
    ScheduleToCloseClaim(schedule_to_close_claim::Args),
    /// Port of `autumn-harvest/scripts/sticky_routing_claim_perf_repro.sh`.
    StickyRoutingClaim(sticky_routing_claim::Args),
    /// Port of `autumn-harvest/scripts/usage_report_activity_lookback_perf_repro.sh`.
    UsageReportActivityLookback(usage_report_activity_lookback::Args),
    /// Port of `autumn-harvest/scripts/worker_session_claim_perf_repro.sh`.
    WorkerSessionClaim(worker_session_claim::Args),
    /// Port of `autumn-harvest-plugin/scripts/history_bloat_perf_repro.sh`.
    HistoryBloat(history_bloat::Args),
    /// Port of `autumn-harvest-sqlite/scripts/classify_dhat_allocations.py`.
    ClassifyDhatAllocations(classify_dhat_allocations::Args),
}

/// Runs one subcommand of `cargo xtask perf`.
pub fn run(command: Command) -> Result<ExitCode> {
    match command {
        Command::BrokenSessionScan(args) => broken_session_scan::run(args),
        Command::BuildReachabilityFanout(args) => build_reachability_fanout::run(args),
        Command::CapabilityLabelsClaim(args) => capability_labels_claim::run(args),
        Command::ClaimBatchedSeekAndRefine(args) => claim_batched_seek_and_refine::run(args),
        Command::ConcurrencyKeyClaim(args) => concurrency_key_claim::run(args),
        Command::DeepBacklogLedger(args) => deep_backlog_ledger::run(args),
        Command::ExternalOutboxScanMatrix(args) => external_outbox_scan_matrix::run(args),
        Command::ExternalOutboxScan(args) => external_outbox_scan::run(args),
        Command::HistoryCeilingClaim(args) => history_ceiling_claim::run(args),
        Command::NotifyStageProbe(args) => notify_stage_probe::run(args),
        Command::ParentCloseCascadeUnfinishedHandlers(args) => {
            parent_close_cascade_unfinished_handlers::run(args)
        }
        Command::PauseArraySizeClaim(args) => pause_array_size_claim::run(args),
        Command::PoolRecyclePing(args) => pool_recycle_ping::run(args),
        Command::QueuePauseClaim(args) => queue_pause_claim::run(args),
        Command::QuotaHistoryBytes(args) => quota_history_bytes::run(args),
        Command::QuotaReconcileCandidateScan(args) => quota_reconcile_candidate_scan::run(args),
        Command::ScheduleToCloseClaim(args) => schedule_to_close_claim::run(args),
        Command::StickyRoutingClaim(args) => sticky_routing_claim::run(args),
        Command::UsageReportActivityLookback(args) => usage_report_activity_lookback::run(args),
        Command::WorkerSessionClaim(args) => worker_session_claim::run(args),
        Command::HistoryBloat(args) => history_bloat::run(args),
        Command::ClassifyDhatAllocations(args) => classify_dhat_allocations::run(args),
    }
}
