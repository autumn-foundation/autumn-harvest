//! Unit tests for the admission gate primitive (issue #377).
//!
//! These tests are pure — no database required — and run with
//! `cargo test -p autumn-harvest --no-default-features`.

use autumn_harvest::admission_gate::{AdmissionGate, AdmissionGateId, GateScope, check_admission};
use autumn_harvest::error::HarvestError;
use chrono::Utc;
use uuid::Uuid;

fn make_gate(scope: GateScope, reason: &str) -> AdmissionGate {
    AdmissionGate {
        id: AdmissionGateId(Uuid::new_v4()),
        scope,
        reason: reason.to_string(),
        message: None,
        created_by: "test-operator".to_string(),
        created_at: Utc::now(),
        expires_at: None,
    }
}

fn make_expiring_gate(
    scope: GateScope,
    reason: &str,
    expires: chrono::DateTime<Utc>,
) -> AdmissionGate {
    AdmissionGate {
        id: AdmissionGateId(Uuid::new_v4()),
        scope,
        reason: reason.to_string(),
        message: None,
        created_by: "test-operator".to_string(),
        created_at: Utc::now(),
        expires_at: Some(expires),
    }
}

// ── GateScope matching ───────────────────────────────────────────────────────

#[test]
fn fleet_scope_matches_any_workflow() {
    let gates = [make_gate(GateScope::Fleet, "incident in progress")];
    let result = check_admission(&gates, "any_workflow", "default", 0, None);
    assert!(result.is_some(), "Fleet gate must block any workflow");
}

#[test]
fn fleet_scope_matches_any_queue() {
    let gates = [make_gate(GateScope::Fleet, "incident in progress")];
    let result = check_admission(&gates, "some_workflow", "special-queue", 0, None);
    assert!(result.is_some(), "Fleet gate must block any queue");
}

#[test]
fn workflow_name_scope_matches_specific_workflow() {
    let gates = [make_gate(
        GateScope::WorkflowName("payment_flow".to_string()),
        "blocked",
    )];
    let result = check_admission(&gates, "payment_flow", "default", 0, None);
    assert!(
        result.is_some(),
        "WorkflowName gate must block the named workflow"
    );
}

#[test]
fn workflow_name_scope_does_not_match_other_workflows() {
    let gates = [make_gate(
        GateScope::WorkflowName("payment_flow".to_string()),
        "blocked",
    )];
    let result = check_admission(&gates, "onboarding", "default", 0, None);
    assert!(
        result.is_none(),
        "WorkflowName gate must not block other workflows"
    );
}

#[test]
fn queue_scope_matches_specific_queue() {
    let gates = [make_gate(
        GateScope::Queue("email-workers".to_string()),
        "outage",
    )];
    let result = check_admission(&gates, "send_emails", "email-workers", 0, None);
    assert!(
        result.is_some(),
        "Queue gate must block work on the named queue"
    );
}

#[test]
fn queue_scope_does_not_match_other_queues() {
    let gates = [make_gate(
        GateScope::Queue("email-workers".to_string()),
        "outage",
    )];
    let result = check_admission(&gates, "send_emails", "sms-workers", 0, None);
    assert!(result.is_none(), "Queue gate must not block other queues");
}

#[test]
fn shard_scope_matches_specific_shard() {
    let gates = [make_gate(GateScope::ShardId(3), "shard 3 overloaded")];
    let result = check_admission(&gates, "any_workflow", "default", 3, None);
    assert!(
        result.is_some(),
        "ShardId gate must block work on the named shard"
    );
}

#[test]
fn shard_scope_does_not_match_other_shards() {
    let gates = [make_gate(GateScope::ShardId(3), "shard 3 overloaded")];
    let result = check_admission(&gates, "any_workflow", "default", 1, None);
    assert!(result.is_none(), "ShardId gate must not block other shards");
}

#[test]
fn owner_scope_matches_specific_owner() {
    let gates = [make_gate(
        GateScope::Owner("team-billing".to_string()),
        "billing down",
    )];
    let result = check_admission(
        &gates,
        "billing_reconcile",
        "default",
        0,
        Some("team-billing"),
    );
    assert!(result.is_some(), "Owner gate must block the named owner");
}

#[test]
fn owner_scope_does_not_match_other_owners() {
    let gates = [make_gate(
        GateScope::Owner("team-billing".to_string()),
        "billing down",
    )];
    let result = check_admission(
        &gates,
        "send_email",
        "default",
        0,
        Some("team-notifications"),
    );
    assert!(result.is_none(), "Owner gate must not block other owners");
}

#[test]
fn owner_scope_does_not_match_no_owner() {
    let gates = [make_gate(
        GateScope::Owner("team-billing".to_string()),
        "billing down",
    )];
    let result = check_admission(&gates, "send_email", "default", 0, None);
    assert!(
        result.is_none(),
        "Owner gate must not block workflows with no owner"
    );
}

// ── Expiry ───────────────────────────────────────────────────────────────────

#[test]
fn expired_gate_does_not_block() {
    let past = Utc::now() - chrono::Duration::hours(1);
    let gates = [make_expiring_gate(
        GateScope::Fleet,
        "expired incident",
        past,
    )];
    let result = check_admission(&gates, "any_workflow", "default", 0, None);
    assert!(result.is_none(), "Expired gate must not block admissions");
}

#[test]
fn future_expiry_gate_blocks() {
    let future = Utc::now() + chrono::Duration::hours(1);
    let gates = [make_expiring_gate(
        GateScope::Fleet,
        "live incident",
        future,
    )];
    let result = check_admission(&gates, "any_workflow", "default", 0, None);
    assert!(result.is_some(), "Non-expired gate must block admissions");
}

#[test]
fn no_expiry_gate_blocks_indefinitely() {
    let gates = [make_gate(GateScope::Fleet, "no expiry")];
    let result = check_admission(&gates, "any_workflow", "default", 0, None);
    assert!(
        result.is_some(),
        "Gate with no expiry must block admissions"
    );
}

// ── Multiple gates ────────────────────────────────────────────────────────────

#[test]
fn no_gates_never_blocks() {
    let result = check_admission(&[], "any_workflow", "default", 0, None);
    assert!(
        result.is_none(),
        "Empty gate list must not block admissions"
    );
}

#[test]
fn first_matching_gate_is_returned() {
    let gate1 = make_gate(GateScope::Fleet, "first gate");
    let gate2 = make_gate(GateScope::Fleet, "second gate");
    let first_reason = gate1.reason.clone();
    let all_gates = [gate1, gate2];
    let result = check_admission(&all_gates, "wf", "q", 0, None);
    let matched = result.expect("must match");
    assert_eq!(
        matched.reason, first_reason,
        "First matching gate should be returned"
    );
}

#[test]
fn or_semantics_any_matching_gate_blocks() {
    // gate1 is scoped to queue-A (won't match queue-B), gate2 is fleet
    let gates = [
        make_gate(GateScope::Queue("queue-A".to_string()), "queue A down"),
        make_gate(GateScope::Fleet, "full incident"),
    ];
    let result = check_admission(&gates, "any_wf", "queue-B", 0, None);
    assert!(
        result.is_some(),
        "Fleet gate must still block even when queue-scoped gate misses"
    );
}

// ── Error type ───────────────────────────────────────────────────────────────

#[test]
fn admission_blocked_error_displays_correctly() {
    let gate_id = AdmissionGateId(Uuid::new_v4());
    let err = HarvestError::AdmissionBlocked {
        gate_id: gate_id.0,
        reason: "prod incident".to_string(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("admission blocked"),
        "Error display must mention 'admission blocked'"
    );
    assert!(
        msg.contains("prod incident"),
        "Error display must include the reason"
    );
}

#[test]
fn admission_blocked_is_distinct_from_already_exists() {
    use autumn_harvest::types::{ExecutionId, ShardId};
    let gate_id = Uuid::new_v4();
    let blocked = HarvestError::AdmissionBlocked {
        gate_id,
        reason: "incident".to_string(),
    };
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let exists = HarvestError::AlreadyExists {
        existing_exec_id: exec_id,
        existing_state: "RUNNING".to_string(),
    };
    assert_ne!(
        blocked.to_string(),
        exists.to_string(),
        "AdmissionBlocked must be a distinct error from AlreadyExists"
    );
}

// ── GateScope display ────────────────────────────────────────────────────────

#[test]
fn gate_scope_fleet_display() {
    let scope = GateScope::Fleet;
    assert_eq!(scope.to_string(), "fleet");
}

#[test]
fn gate_scope_workflow_name_display() {
    let scope = GateScope::WorkflowName("my_workflow".to_string());
    assert!(scope.to_string().contains("workflow_name"));
    assert!(scope.to_string().contains("my_workflow"));
}

#[test]
fn gate_scope_queue_display() {
    let scope = GateScope::Queue("email-workers".to_string());
    assert!(scope.to_string().contains("queue"));
    assert!(scope.to_string().contains("email-workers"));
}

#[test]
fn gate_scope_shard_id_display() {
    let scope = GateScope::ShardId(5);
    assert!(scope.to_string().contains("shard_id"));
    assert!(scope.to_string().contains('5'));
}

#[test]
fn gate_scope_owner_display() {
    let scope = GateScope::Owner("team-payments".to_string());
    assert!(scope.to_string().contains("owner"));
    assert!(scope.to_string().contains("team-payments"));
}

// ── AdmissionGateId ──────────────────────────────────────────────────────────

#[test]
fn gate_id_display_matches_inner_uuid() {
    let uuid = Uuid::new_v4();
    let id = AdmissionGateId(uuid);
    assert_eq!(id.to_string(), uuid.to_string());
}

// ── MAX_ACTIVE_GATES ─────────────────────────────────────────────────────────

#[test]
fn max_active_gates_constant_is_documented() {
    // Confirm the constant exists and has a reasonable bound. Read through a
    // runtime variable so clippy::assertions_on_constants doesn't fire.
    let cap = autumn_harvest::admission_gate::MAX_ACTIVE_GATES;
    assert!(
        cap >= 10,
        "MAX_ACTIVE_GATES must allow at least 10 simultaneous gates"
    );
    assert!(
        cap <= 1000,
        "MAX_ACTIVE_GATES must be at most 1000 to bound cardinality"
    );
}
