//! Docs and code audits that gate CI (formerly `docs/audits/*.py`).

use std::process::ExitCode;

use smop::prelude::*;

pub mod action_sha_pin;
pub mod audit_catalog_coverage;
pub mod cli_flag_coverage;
pub mod comment_hygiene;
pub mod config_cli_drift;
pub mod corpus_link_check;
pub mod diesel_table_width;
pub mod doc_claim_drift;
pub mod doc_snippet_syntax;
pub mod ledger_census;
pub mod ledger_profiles;
pub mod libpq_keyword_sync;
pub mod mixed_version_contract;
pub mod openapi_response_coverage;
pub mod required_migration_versions_sync;
pub mod root_files;
pub mod shard_weight_drift;
pub mod standalone_chapter_sync;
pub mod tenant_header_validation_sync;
pub mod tracked_artifacts;
pub mod vantage_dashboard_contrast;
pub mod vantage_degraded_banner_focus;
pub mod vantage_flash_live_region;
pub mod vantage_label_association;
pub mod worker_image_contract;
pub mod workflow_yaml_parse;

/// Each subcommand of `cargo xtask audit`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Port of `docs/audits/action-sha-pin.py`.
    ActionShaPin(action_sha_pin::Args),
    /// Port of `docs/audits/audit-catalog-coverage.py`.
    AuditCatalogCoverage(audit_catalog_coverage::Args),
    /// Port of `docs/audits/cli-flag-coverage.py`.
    CliFlagCoverage(cli_flag_coverage::Args),
    /// Port of `docs/audits/comment-hygiene.py`.
    CommentHygiene(comment_hygiene::Args),
    /// Port of `docs/audits/config-cli-drift.py`.
    ConfigCliDrift(config_cli_drift::Args),
    /// Port of `docs/audits/corpus-link-check.py`.
    CorpusLinkCheck(corpus_link_check::Args),
    /// Port of `docs/audits/diesel-table-width.py`.
    DieselTableWidth(diesel_table_width::Args),
    /// Port of `docs/audits/doc-claim-drift.py`.
    DocClaimDrift(doc_claim_drift::Args),
    /// Port of `docs/audits/doc-snippet-syntax.py`.
    DocSnippetSyntax(doc_snippet_syntax::Args),
    /// Port of `docs/audits/ledger-census.py`.
    LedgerCensus(ledger_census::Args),
    /// Port of `docs/audits/ledger-profiles.py`.
    LedgerProfiles(ledger_profiles::Args),
    /// Port of `docs/audits/libpq-keyword-sync.py`.
    LibpqKeywordSync(libpq_keyword_sync::Args),
    /// Port of `docs/audits/mixed-version-contract.py`.
    MixedVersionContract(mixed_version_contract::Args),
    /// Port of `docs/audits/openapi-response-coverage.py`.
    OpenapiResponseCoverage(openapi_response_coverage::Args),
    /// Port of `docs/audits/required-migration-versions-sync.py`.
    RequiredMigrationVersionsSync(required_migration_versions_sync::Args),
    /// Port of `docs/audits/root-files.py`.
    RootFiles(root_files::Args),
    /// Port of `docs/audits/shard-weight-drift.py`.
    ShardWeightDrift(shard_weight_drift::Args),
    /// Port of `docs/audits/standalone-chapter-sync.py`.
    StandaloneChapterSync(standalone_chapter_sync::Args),
    /// Port of `docs/audits/tenant-header-validation-sync.py`.
    TenantHeaderValidationSync(tenant_header_validation_sync::Args),
    /// Port of `docs/audits/tracked-artifacts.py`.
    TrackedArtifacts(tracked_artifacts::Args),
    /// Port of `docs/audits/vantage-dashboard-contrast.py`.
    VantageDashboardContrast(vantage_dashboard_contrast::Args),
    /// Port of `docs/audits/vantage-degraded-banner-focus.py`.
    VantageDegradedBannerFocus(vantage_degraded_banner_focus::Args),
    /// Port of `docs/audits/vantage-flash-live-region.py`.
    VantageFlashLiveRegion(vantage_flash_live_region::Args),
    /// Port of `docs/audits/vantage-label-association.py`.
    VantageLabelAssociation(vantage_label_association::Args),
    /// Port of `docs/audits/worker-image-contract.py`.
    WorkerImageContract(worker_image_contract::Args),
    /// Port of `docs/audits/workflow-yaml-parse.py`.
    WorkflowYamlParse(workflow_yaml_parse::Args),
}

/// Runs one subcommand of `cargo xtask audit`.
pub fn run(command: Command) -> Result<ExitCode> {
    match command {
        Command::ActionShaPin(args) => action_sha_pin::run(args),
        Command::AuditCatalogCoverage(args) => audit_catalog_coverage::run(args),
        Command::CliFlagCoverage(args) => cli_flag_coverage::run(args),
        Command::CommentHygiene(args) => comment_hygiene::run(args),
        Command::ConfigCliDrift(args) => config_cli_drift::run(args),
        Command::CorpusLinkCheck(args) => corpus_link_check::run(args),
        Command::DieselTableWidth(args) => diesel_table_width::run(args),
        Command::DocClaimDrift(args) => doc_claim_drift::run(args),
        Command::DocSnippetSyntax(args) => doc_snippet_syntax::run(args),
        Command::LedgerCensus(args) => ledger_census::run(args),
        Command::LedgerProfiles(args) => ledger_profiles::run(args),
        Command::LibpqKeywordSync(args) => libpq_keyword_sync::run(args),
        Command::MixedVersionContract(args) => mixed_version_contract::run(args),
        Command::OpenapiResponseCoverage(args) => openapi_response_coverage::run(args),
        Command::RequiredMigrationVersionsSync(args) => required_migration_versions_sync::run(args),
        Command::RootFiles(args) => root_files::run(args),
        Command::ShardWeightDrift(args) => shard_weight_drift::run(args),
        Command::StandaloneChapterSync(args) => standalone_chapter_sync::run(args),
        Command::TenantHeaderValidationSync(args) => tenant_header_validation_sync::run(args),
        Command::TrackedArtifacts(args) => tracked_artifacts::run(args),
        Command::VantageDashboardContrast(args) => vantage_dashboard_contrast::run(args),
        Command::VantageDegradedBannerFocus(args) => vantage_degraded_banner_focus::run(args),
        Command::VantageFlashLiveRegion(args) => vantage_flash_live_region::run(args),
        Command::VantageLabelAssociation(args) => vantage_label_association::run(args),
        Command::WorkerImageContract(args) => worker_image_contract::run(args),
        Command::WorkflowYamlParse(args) => workflow_yaml_parse::run(args),
    }
}
