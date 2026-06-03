//! JSON Schema opt-in for workflow self-service triggering (issue #373).
//!
//! Demonstrates:
//! 1. Attaching a JSON Schema to a workflow via `with_input_schema_fn` (no
//!    extra dependencies — manual schema as `serde_json::Value`).
//! 2. Attaching a schema via `with_schemas::<I, O, E>()` using the `schema`
//!    feature, which derives the schema automatically from types that implement
//!    `schemars::JsonSchema`.
//! 3. The `RegisteredWorkflowRecord` response that `GET /workflows/registered`
//!    and `GET /workflows/registered/{name}/schema` return.
//! 4. How `WorkflowInfo::validate_input` rejects bad inputs at the API boundary
//!    with structured field-path errors — before the workflow ever reaches the
//!    task queue.
//!
//! Run with:
//!   cargo run --example schema_workflow --features schema

use autumn_harvest::info::RegisteredWorkflowRecord;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

// ── Domain types ─────────────────────────────────────────────────────────────

/// Onboarding workflow input.
///
/// When the `schema` feature is enabled, `#[derive(JsonSchema)]` generates
/// the JSON Schema automatically. Without the feature, you supply the schema
/// manually via `with_input_schema_fn`.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct OnboardInput {
    /// Unique identifier for the new user.
    pub user_id: i64,
    /// Verified email address.
    pub email: String,
    /// Optional referral code for attribution.
    pub referral_code: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct OnboardOutput {
    pub welcome_email_sent: bool,
}

// ── Workflows ────────────────────────────────────────────────────────────────

#[workflow(description = "Handles new-user onboarding from signup to first action")]
pub async fn onboarding(
    _ctx: &WorkflowContext,
    _input: OnboardInput,
) -> Result<OnboardOutput, String> {
    // Real implementation would dispatch activities here.
    Ok(OnboardOutput {
        welcome_email_sent: true,
    })
}

/// A minimal no-schema workflow for comparison.
#[workflow]
pub async fn no_schema_workflow(_ctx: &WorkflowContext, _input: ()) -> Result<(), String> {
    Ok(())
}

// ── Manual schema helper (no `schema` feature required) ──────────────────────

/// Returns a hand-written JSON Schema for `OnboardInput`.
///
/// Use this approach when you cannot add `schemars` to your dependency tree.
fn onboard_input_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "OnboardInput",
        "type": "object",
        "properties": {
            "user_id": {
                "type": "integer",
                "description": "Unique identifier for the new user"
            },
            "email": {
                "type": "string",
                "description": "Verified email address",
                "minLength": 5
            },
            "referral_code": {
                "type": "string",
                "description": "Optional referral code for attribution"
            }
        },
        "required": ["user_id", "email"]
    })
}

fn onboard_output_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "welcome_email_sent": {"type": "boolean"}
        },
        "required": ["welcome_email_sent"]
    })
}

fn onboard_error_schema() -> serde_json::Value {
    serde_json::json!({"type": "string"})
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() {
    println!("=== Harvest JSON Schema opt-in demo (issue #373) ===\n");

    // ── Option A: manual schema via with_input_schema_fn ─────────────────────
    println!("--- Option A: manual schema (no `schema` feature required) ---");

    let onboarding_info_manual = onboarding_info()
        .with_input_schema_fn(onboard_input_schema)
        .with_output_schema_fn(onboard_output_schema)
        .with_error_schema_fn(onboard_error_schema);

    let record_manual = RegisteredWorkflowRecord::from_info(&onboarding_info_manual);
    println!(
        "GET /workflows/registered/onboarding/schema (manual):\n{}",
        serde_json::to_string_pretty(&record_manual).unwrap()
    );

    // ── Option B: automatic schema via `schema` feature ──────────────────────
    #[cfg(feature = "schema")]
    {
        println!("\n--- Option B: automatic schema via `schema` feature ---");

        let onboarding_info_auto =
            onboarding_info().with_schemas::<OnboardInput, OnboardOutput, String>();

        let record_auto = RegisteredWorkflowRecord::from_info(&onboarding_info_auto);
        println!(
            "GET /workflows/registered/onboarding/schema (auto):\n{}",
            serde_json::to_string_pretty(&record_auto).unwrap()
        );
    }

    // ── GET /workflows/registered response ───────────────────────────────────
    println!("\n--- GET /workflows/registered ---");

    let registered: Vec<RegisteredWorkflowRecord> = {
        let mut records = vec![
            RegisteredWorkflowRecord::from_info(&onboarding_info_manual),
            RegisteredWorkflowRecord::from_info(&no_schema_workflow_info()),
        ];
        records.sort_by(|a, b| a.name.cmp(&b.name));
        records
    };
    println!("{}", serde_json::to_string_pretty(&registered).unwrap());

    // ── Input validation ──────────────────────────────────────────────────────
    println!("\n--- Server-side input validation ---");

    let schema_info = onboarding_info().with_input_schema_fn(onboard_input_schema);

    // Valid input: passes
    let valid = serde_json::json!({"user_id": 42, "email": "alice@example.com"});
    match schema_info.validate_input(&valid) {
        Ok(()) => println!("✓ Valid input accepted"),
        Err(v) => println!("✗ Unexpected rejection: {v:?}"),
    }

    // Missing required "email" field: rejected with field path
    let missing_email = serde_json::json!({"user_id": 42});
    match schema_info.validate_input(&missing_email) {
        Ok(()) => println!("✗ Should have been rejected"),
        Err(violations) => {
            println!("✓ Invalid input rejected (400 would be returned):");
            for v in &violations {
                println!(
                    "   field_path={}, message={}",
                    v.field_path.as_deref().unwrap_or("(root)"),
                    v.message
                );
            }
        }
    }

    // Wrong type for email: rejected with field path
    let wrong_type = serde_json::json!({"user_id": 42, "email": 12345});
    match schema_info.validate_input(&wrong_type) {
        Ok(()) => println!("✗ Should have been rejected"),
        Err(violations) => {
            println!("✓ Wrong-type input rejected:");
            for v in &violations {
                println!(
                    "   field_path={}, message={}",
                    v.field_path.as_deref().unwrap_or("(root)"),
                    v.message
                );
            }
        }
    }

    // No schema on no_schema_workflow — any input passes
    let any_input = serde_json::json!({"anything": "goes"});
    let no_schema_info = no_schema_workflow_info();
    match no_schema_info.validate_input(&any_input) {
        Ok(()) => println!("✓ No-schema workflow accepts any input (opt-in)"),
        Err(v) => println!("✗ Unexpected rejection: {v:?}"),
    }

    println!("\n=== Example complete ===");
    println!(
        "\nTo trigger the schema-aware workflow correctly:\n  \
         curl -X POST /api/harvest/workflows/onboarding/start \\\n  \
         -d '{{\"input\": {{\"user_id\": 1, \"email\": \"alice@example.com\"}}}}'\n\n\
         A deliberately wrong input:\n  \
         curl -X POST /api/harvest/workflows/onboarding/start \\\n  \
         -d '{{\"input\": {{\"user_id\": \"not_an_int\"}}}}'\n  \
         → 400 Bad Request with structured violations array"
    );
}
