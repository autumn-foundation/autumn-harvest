//! Example and documentation guards (formerly `scripts/check-*.sh`).

use std::process::ExitCode;

use smop::prelude::*;

pub mod agent_adapter_deps;
pub mod agentd_runtime_artifacts_ignored;
pub mod broker_connector_cutover_example;
pub mod broker_connector_dead_letter_sink_example;
pub mod changelog_connector_migration_version;
pub mod child_idempotency_examples;
pub mod continue_as_new_checkpoint_example;
pub mod dags_schedules_examples;
pub mod default_run;
pub mod dev_database_url_placeholder_caveat;
pub mod dev_runtime_root_caveat;
pub mod durable_timers_examples;
pub mod formal_models;
pub mod formal_traces;
pub mod kani_proofs;
pub mod license_files;
pub mod no_powershell_env_syntax;
pub mod otel_workflow_semconv;
pub mod preflight_declared_deps_example;
pub mod readme_front_door_snippets;
pub mod readme_testing_dep_version;
pub mod readme_upgrade_guide_current;
pub mod search_attr_query_syntax;
pub mod signals_clean_approval_example_determinism;
pub mod sqlite_outcome_match_example;
pub mod underscore_input_snippets;
pub mod verus_proofs;
pub mod webhooks_feature_example;
pub mod worker_routing_examples;

/// Each subcommand of `cargo xtask check`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Port of `scripts/check-agent-adapter-deps.sh`.
    AgentAdapterDeps(agent_adapter_deps::Args),
    /// Port of `scripts/check-agentd-runtime-artifacts-ignored.sh`.
    AgentdRuntimeArtifactsIgnored(agentd_runtime_artifacts_ignored::Args),
    /// Port of `scripts/check-broker-connector-cutover-example.sh`.
    BrokerConnectorCutoverExample(broker_connector_cutover_example::Args),
    /// Port of `scripts/check-broker-connector-dead-letter-sink-example.sh`.
    BrokerConnectorDeadLetterSinkExample(broker_connector_dead_letter_sink_example::Args),
    /// Port of `scripts/check-changelog-connector-migration-version.sh`.
    ChangelogConnectorMigrationVersion(changelog_connector_migration_version::Args),
    /// Port of `scripts/check-child-idempotency-examples.sh`.
    ChildIdempotencyExamples(child_idempotency_examples::Args),
    /// Port of `scripts/check-continue-as-new-checkpoint-example.sh`.
    ContinueAsNewCheckpointExample(continue_as_new_checkpoint_example::Args),
    /// Port of `scripts/check-dags-schedules-examples.sh`.
    DagsSchedulesExamples(dags_schedules_examples::Args),
    /// Port of `scripts/check-default-run.sh`.
    DefaultRun(default_run::Args),
    /// Port of `scripts/check-dev-database-url-placeholder-caveat.sh`.
    DevDatabaseUrlPlaceholderCaveat(dev_database_url_placeholder_caveat::Args),
    /// Port of `scripts/check-dev-runtime-root-caveat.sh`.
    DevRuntimeRootCaveat(dev_runtime_root_caveat::Args),
    /// Port of `scripts/check-durable-timers-examples.sh`.
    DurableTimersExamples(durable_timers_examples::Args),
    /// Port of `scripts/check-formal-models.sh`.
    FormalModels(formal_models::Args),
    /// Port of `scripts/check-formal-traces.sh`.
    FormalTraces(formal_traces::Args),
    /// Port of `scripts/check-kani-proofs.sh`.
    KaniProofs(kani_proofs::Args),
    /// Port of `scripts/check-license-files.sh`.
    LicenseFiles(license_files::Args),
    /// Port of `scripts/check-no-powershell-env-syntax.sh`.
    NoPowershellEnvSyntax(no_powershell_env_syntax::Args),
    /// Port of `scripts/check-otel-workflow-semconv.sh`.
    OtelWorkflowSemconv(otel_workflow_semconv::Args),
    /// Port of `scripts/check-preflight-declared-deps-example.sh`.
    PreflightDeclaredDepsExample(preflight_declared_deps_example::Args),
    /// Port of `scripts/check-readme-front-door-snippets.sh`.
    ReadmeFrontDoorSnippets(readme_front_door_snippets::Args),
    /// Port of `scripts/check-readme-testing-dep-version.sh`.
    ReadmeTestingDepVersion(readme_testing_dep_version::Args),
    /// Port of `scripts/check-readme-upgrade-guide-current.sh`.
    ReadmeUpgradeGuideCurrent(readme_upgrade_guide_current::Args),
    /// Port of `scripts/check-search-attr-query-syntax.sh`.
    SearchAttrQuerySyntax(search_attr_query_syntax::Args),
    /// Port of `scripts/check-signals-clean-approval-example-determinism.sh`.
    SignalsCleanApprovalExampleDeterminism(signals_clean_approval_example_determinism::Args),
    /// Port of `scripts/check-sqlite-outcome-match-example.sh`.
    SqliteOutcomeMatchExample(sqlite_outcome_match_example::Args),
    /// Port of `scripts/check-underscore-input-snippets.sh`.
    UnderscoreInputSnippets(underscore_input_snippets::Args),
    /// Port of `scripts/check-verus-proofs.sh`.
    VerusProofs(verus_proofs::Args),
    /// Port of `scripts/check-webhooks-feature-example.sh`.
    WebhooksFeatureExample(webhooks_feature_example::Args),
    /// Port of `scripts/check-worker-routing-examples.sh`.
    WorkerRoutingExamples(worker_routing_examples::Args),
}

/// Runs one subcommand of `cargo xtask check`.
pub fn run(command: Command) -> Result<ExitCode> {
    match command {
        Command::AgentAdapterDeps(args) => agent_adapter_deps::run(args),
        Command::AgentdRuntimeArtifactsIgnored(args) => agentd_runtime_artifacts_ignored::run(args),
        Command::BrokerConnectorCutoverExample(args) => broker_connector_cutover_example::run(args),
        Command::BrokerConnectorDeadLetterSinkExample(args) => {
            broker_connector_dead_letter_sink_example::run(args)
        }
        Command::ChangelogConnectorMigrationVersion(args) => {
            changelog_connector_migration_version::run(args)
        }
        Command::ChildIdempotencyExamples(args) => child_idempotency_examples::run(args),
        Command::ContinueAsNewCheckpointExample(args) => {
            continue_as_new_checkpoint_example::run(args)
        }
        Command::DagsSchedulesExamples(args) => dags_schedules_examples::run(args),
        Command::DefaultRun(args) => default_run::run(args),
        Command::DevDatabaseUrlPlaceholderCaveat(args) => {
            dev_database_url_placeholder_caveat::run(args)
        }
        Command::DevRuntimeRootCaveat(args) => dev_runtime_root_caveat::run(args),
        Command::DurableTimersExamples(args) => durable_timers_examples::run(args),
        Command::FormalModels(args) => formal_models::run(args),
        Command::FormalTraces(args) => formal_traces::run(args),
        Command::KaniProofs(args) => kani_proofs::run(args),
        Command::LicenseFiles(args) => license_files::run(args),
        Command::NoPowershellEnvSyntax(args) => no_powershell_env_syntax::run(args),
        Command::OtelWorkflowSemconv(args) => otel_workflow_semconv::run(args),
        Command::PreflightDeclaredDepsExample(args) => preflight_declared_deps_example::run(args),
        Command::ReadmeFrontDoorSnippets(args) => readme_front_door_snippets::run(args),
        Command::ReadmeTestingDepVersion(args) => readme_testing_dep_version::run(args),
        Command::ReadmeUpgradeGuideCurrent(args) => readme_upgrade_guide_current::run(args),
        Command::SearchAttrQuerySyntax(args) => search_attr_query_syntax::run(args),
        Command::SignalsCleanApprovalExampleDeterminism(args) => {
            signals_clean_approval_example_determinism::run(args)
        }
        Command::SqliteOutcomeMatchExample(args) => sqlite_outcome_match_example::run(args),
        Command::UnderscoreInputSnippets(args) => underscore_input_snippets::run(args),
        Command::VerusProofs(args) => verus_proofs::run(args),
        Command::WebhooksFeatureExample(args) => webhooks_feature_example::run(args),
        Command::WorkerRoutingExamples(args) => worker_routing_examples::run(args),
    }
}
