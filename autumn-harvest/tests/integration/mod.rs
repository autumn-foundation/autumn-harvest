#![allow(dead_code)]

mod activity_failure_tests;
mod activity_outcome_metrics_tests;
mod admission_gate_tests;
mod alert_pack_docs;
mod audit_tests;
#[cfg(feature = "db")]
mod build_routing_tests;
mod cache_delta_load_tests;
mod cancellation_tests;
mod child_fanout_tests;
mod child_policy_tests;
mod circuit_breaker_wiring_tests;
mod completion_callback_tests;
mod concurrency_key_tests;
mod context_headers_tests;
mod cross_workflow_cancel_tests;
#[cfg(feature = "db")]
mod cross_workflow_signal_tests;
mod dag_builder;
mod dag_mapping_tests;
#[cfg(all(feature = "testing", feature = "unified-dag-execution"))]
mod dag_unified_tests;
mod dashboard_pack_docs;
mod debounce_tests;
mod delayed_start_tests;
mod det_check_tests;
mod event_batch_tests;
mod executor_span_tests;
#[cfg(feature = "testing")]
mod external_completion_tests;
mod fanout_tests;
mod force_fail_tests;
mod guardrail_catalog_tests;
mod havoc_reentrancy;
mod havoc_tests;
#[cfg(feature = "testing")]
mod idempotency_tests;
mod integration_e2e;
mod macros_activity;
mod macros_collect;
#[cfg(feature = "testing")]
mod macros_compile_fail;
mod macros_dag;
#[cfg(feature = "testing")]
mod macros_query_handlers;
mod macros_webhook;
mod macros_workflow;
mod metrics_coverage;
mod metrics_integration;
mod metrics_rs_adapter;
mod nd_block_tests;
mod pause_tests;
#[cfg(feature = "testing")]
mod payload_cap_tests;
#[cfg(feature = "db")]
mod payload_offload_db_tests;
#[cfg(feature = "testing")]
mod payload_offload_replay_tests;
mod poison_pill_tests;
mod priority_tests;
mod query_deadlock;
mod query_tests;
mod queue_fairness_tests;
mod redrive_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod replay_canary_tests;
mod replay_tests;
#[cfg(feature = "testing")]
mod replay_verifier_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod replayer_integration_tests;
#[cfg(feature = "testing")]
mod replayer_tests;
mod retry_now_tests;
mod saga_tests;
mod schedule_decisions;
mod schedule_runs_tests;
mod schedule_to_close_tests;
mod schedule_update_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod scheduled_time_tests;
mod scheduler_auto_pause_tests;
mod scheduler_bounded_runs_tests;
#[cfg(all(feature = "testing", feature = "db"))]
mod scheduler_carryover_tests;
mod scheduler_catchup_tests;
#[cfg(feature = "db")]
mod scheduler_ha_tests;
mod security;
mod sharding_unit;
mod signal_tests;
#[cfg(feature = "db")]
mod signal_with_start_tests;
mod sla_breach_tests;
mod slot_tuner_tests;
#[cfg(feature = "db")]
mod start_idempotency_tests;
mod sticky_routing_tests;
mod telemetry_span_tests;
#[cfg(feature = "db")]
mod throttle_tests;
#[cfg(feature = "db")]
mod transactional_activity_tests;
#[cfg(feature = "db")]
mod typed_stubs_tests;
mod updt_with_start_tests;
mod webhook_trigger_tests;
mod worker_session_tests;
#[cfg(feature = "db")]
mod workflow_handle_tests;
mod workflow_logger_tests;
mod workflow_mutation_tests;
mod workflow_retry_tests;
mod workflow_task_timeout_tests;
#[cfg(feature = "testing")]
mod workflow_test_env_tests;
