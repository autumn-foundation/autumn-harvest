//! Schemas for activity payloads and side-effect values (issue #1994).
//!
//! An activity result and a side-effect value are read back on replay, the
//! same as a workflow payload. These tests prove three things. Activities and
//! side effects publish schemas. The contract carries them. The differ applies
//! the issue #794 ruleset to them.
//!
//! No database, no network: the gate is pure `serde_json` analysis.

// The fixtures are trivial handlers. `#[workflow]` reads every input binding,
// so an unused `_input` still trips the underscore lint.
#![allow(clippy::used_underscore_binding, clippy::unused_async)]

use autumn_harvest::prelude::*;
use autumn_harvest::schema_contract::{
    AcknowledgedBreakingChange, ActivitySchemaEntry, ChangeKind, SCHEMA_CONTRACT_VERSION,
    SchemaContractDiff, SchemaDelta, SchemaRole, SchemaSubject, SideEffectSchemaEntry, Verdict,
    WorkflowSchemaContract, WorkflowSchemaEntry, diff_schema_contracts, dropped_acknowledgements,
    subject_label, unacknowledged_breaking,
};
use autumn_harvest::{ActivityInfo, SideEffectInfo, WorkflowInfo};
use serde_json::{Value, json};

// ── fixtures ─────────────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ChargeInput {
    pub amount: i64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Receipt {
    pub id: String,
}

#[activity]
async fn charge(_ctx: &ActivityContext, input: ChargeInput) -> Result<Receipt, String> {
    Ok(Receipt {
        id: input.amount.to_string(),
    })
}

#[workflow]
async fn checkout(_ctx: &WorkflowContext, _input: ()) -> Result<(), String> {
    Ok(())
}

fn charge_input_schema() -> Value {
    json!({"type": "object", "properties": {"amount": {"type": "integer"}}, "required": ["amount"]})
}

fn receipt_schema() -> Value {
    json!({
        "type": "object",
        "description": "a doc comment",
        "properties": {"id": {"type": "string"}},
        "required": ["id"],
    })
}

fn variant_schema() -> Value {
    json!({"type": "string", "enum": ["a", "b"]})
}

/// A contract with one activity `charge` whose output schema is `output`.
fn activity_contract(output: Value) -> WorkflowSchemaContract {
    let mut c = WorkflowSchemaContract::from_entries("0.0.0-test", Vec::new());
    c.activities = vec![ActivitySchemaEntry {
        name: "charge".to_string(),
        input_schema: None,
        output_schema: Some(output),
    }];
    reparse(&c)
}

/// A contract with one side effect `checkout/pick` whose value is `value`.
fn side_effect_contract(workflow: &str, value: Value) -> WorkflowSchemaContract {
    let mut c = WorkflowSchemaContract::from_entries("0.0.0-test", Vec::new());
    c.side_effects = vec![SideEffectSchemaEntry {
        workflow: workflow.to_string(),
        id: "pick".to_string(),
        value_schema: Some(value),
    }];
    reparse(&c)
}

/// Round-trip through JSON, so every invariant `parse` derives is applied.
fn reparse(c: &WorkflowSchemaContract) -> WorkflowSchemaContract {
    WorkflowSchemaContract::parse(&c.to_json_pretty().unwrap()).unwrap()
}

fn only_breaking(diff: &SchemaContractDiff) -> Vec<&SchemaDelta> {
    diff.breaking().collect()
}

// ── AC 1: publishing ─────────────────────────────────────────────────────────

#[test]
fn an_activity_publishes_no_schema_by_default() {
    let info = charge_info();
    assert!(info.input_schema.is_none());
    assert!(info.output_schema.is_none());
}

#[test]
fn an_activity_publishes_input_and_output_schemas() {
    let info = charge_info()
        .with_input_schema_fn(charge_input_schema)
        .with_output_schema_fn(receipt_schema);
    assert_eq!((info.input_schema.expect("input"))(), charge_input_schema());
    assert_eq!((info.output_schema.expect("output"))(), receipt_schema());
}

#[test]
fn a_side_effect_publishes_a_value_schema_under_its_workflow() {
    let info = checkout_info()
        .side_effect("pick")
        .with_value_schema_fn(variant_schema);
    assert_eq!(info.workflow, "checkout");
    assert_eq!(info.id, "pick");
    assert_eq!((info.value_schema.expect("value"))(), variant_schema());
}

#[test]
fn the_contract_carries_activity_and_side_effect_schemas() {
    let workflows: Vec<WorkflowInfo> = vec![checkout_info()];
    let activities: Vec<ActivityInfo> = vec![
        charge_info().with_output_schema_fn(receipt_schema),
        charge_info()
            .with_input_schema_fn(charge_input_schema)
            .with_output_schema_fn(receipt_schema),
    ];
    let side_effects: Vec<SideEffectInfo> = vec![
        checkout_info()
            .side_effect("pick")
            .with_value_schema_fn(variant_schema),
        checkout_info().side_effect("roll"),
    ];
    let c = WorkflowSchemaContract::from_infos("0.0.0-test", &workflows)
        .with_activities(&activities)
        .with_side_effects(&side_effects);

    assert_eq!(c.activities.len(), 1, "duplicate names collapse last-wins");
    let charge = &c.activities[0];
    assert_eq!(charge.name, "charge");
    assert!(charge.input_schema.is_some(), "the last registration wins");
    assert!(
        charge
            .output_schema
            .as_ref()
            .unwrap()
            .get("description")
            .is_none(),
        "annotations are canonicalised away"
    );

    let ids: Vec<&str> = c.side_effects.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, ["pick", "roll"], "sorted by (workflow, id)");
    assert_eq!(c.side_effects[0].workflow, "checkout");

    assert_eq!(c.coverage.activities_total, 1);
    assert_eq!(c.coverage.with_activity_input_schema, 1);
    assert_eq!(c.coverage.with_activity_output_schema, 1);
    assert_eq!(c.coverage.side_effects_total, 2);
    assert_eq!(c.coverage.with_side_effect_value_schema, 1);
}

#[test]
fn the_contract_round_trips_with_the_new_sections() {
    let c = WorkflowSchemaContract::from_infos("0.0.0-test", &[checkout_info()])
        .with_activities(&[charge_info().with_output_schema_fn(receipt_schema)])
        .with_side_effects(&[checkout_info()
            .side_effect("pick")
            .with_value_schema_fn(variant_schema)]);
    let back = reparse(&c);
    assert_eq!(back, c);
    assert_eq!(back.contract_version, "2");
    assert_eq!(SCHEMA_CONTRACT_VERSION, "2");
}

/// A version 1 binary would skip the new sections in silence, so a version 1
/// label on version 2 content is refused.
#[test]
fn a_version_1_label_on_version_2_content_is_refused() {
    let c = activity_contract(receipt_schema());
    let mut v: Value = serde_json::from_str(&c.to_json_pretty().unwrap()).unwrap();
    v["contract_version"] = json!("1");
    let err = WorkflowSchemaContract::parse(&v.to_string())
        .expect_err("a v1 label must not carry activities");
    assert!(err.to_string().contains("contract_version"), "{err}");
}

#[test]
fn a_version_1_contract_still_parses() {
    let v1 = r#"{"version":"0.6.0","contract_version":"1","workflows":[{"name":"w"}]}"#;
    let c = WorkflowSchemaContract::parse(v1).expect("a v1 baseline must still parse");
    assert_eq!(c.activities, []);
    assert_eq!(c.side_effects, []);
}

#[test]
fn empty_sections_are_left_out_of_the_json() {
    let c = WorkflowSchemaContract::from_infos("0.0.0-test", &[checkout_info()])
        .with_activities(&[])
        .with_side_effects(&[]);
    let v: Value = serde_json::from_str(&c.to_json_pretty().unwrap()).unwrap();
    assert!(v.get("activities").is_none(), "{v}");
    assert!(v.get("side_effects").is_none(), "{v}");
}

#[test]
fn an_activity_without_a_schema_is_still_listed() {
    let c = WorkflowSchemaContract::from_entries("0.0.0-test", Vec::new())
        .with_activities(&[charge_info()]);
    assert_eq!(
        c.activities.len(),
        1,
        "coverage shows an unpublished activity"
    );
    assert_eq!(c.coverage.activities_total, 1);
    assert_eq!(c.coverage.with_activity_output_schema, 0);
}

// ── AC 2: the differ covers activities ───────────────────────────────────────

#[test]
fn removing_a_field_from_an_activity_output_is_breaking() {
    let base = activity_contract(receipt_schema());
    let cur = activity_contract(json!({"type": "object", "properties": {}}));
    let diff = diff_schema_contracts(&base, &cur);
    let breaking = only_breaking(&diff);
    assert_eq!(breaking.len(), 1, "{diff:#?}");
    let d = breaking[0];
    assert_eq!(d.subject, SchemaSubject::Activity);
    assert_eq!(d.workflow, "charge");
    assert_eq!(d.role, Some(SchemaRole::Output));
    assert_eq!(d.field_path, "/id");
    assert_eq!(d.change, ChangeKind::PropertyRemoved);
    assert_eq!(diff.exit_code(), 1);
}

#[test]
fn adding_a_required_field_to_an_activity_output_is_breaking() {
    let base = activity_contract(receipt_schema());
    let cur = activity_contract(json!({
        "type": "object",
        "properties": {"id": {"type": "string"}, "fee": {"type": "integer"}},
        "required": ["id", "fee"],
    }));
    let diff = diff_schema_contracts(&base, &cur);
    assert!(
        diff.breaking()
            .any(|d| d.change == ChangeKind::RequiredPropertyAdded
                && d.subject == SchemaSubject::Activity),
        "{diff:#?}"
    );
}

#[test]
fn an_optional_field_added_to_an_activity_output_is_compatible() {
    let base = activity_contract(receipt_schema());
    let cur = activity_contract(json!({
        "type": "object",
        "properties": {"id": {"type": "string"}, "note": {"type": ["string", "null"]}},
        "required": ["id"],
    }));
    let diff = diff_schema_contracts(&base, &cur);
    assert!(!diff.has_breaking(), "{diff:#?}");
    assert_eq!(diff.compatible_count, 1);
}

#[test]
fn an_activity_input_change_is_checked_too() {
    let mut base = WorkflowSchemaContract::from_entries("0.0.0-test", Vec::new());
    base.activities = vec![ActivitySchemaEntry {
        name: "charge".to_string(),
        input_schema: Some(json!({"type": "integer", "format": "int64"})),
        output_schema: None,
    }];
    let mut cur = base.clone();
    cur.activities[0].input_schema = Some(json!({"type": "integer", "format": "int32"}));
    let diff = diff_schema_contracts(&reparse(&base), &reparse(&cur));
    let d = only_breaking(&diff)[0];
    assert_eq!(d.role, Some(SchemaRole::Input));
    assert_eq!(d.change, ChangeKind::NumericFormatNarrowed);
}

#[test]
fn a_doc_comment_edit_on_an_activity_type_is_not_a_delta() {
    let mut edited = receipt_schema();
    edited["description"] = json!("a different doc comment");
    let diff = diff_schema_contracts(
        &activity_contract(receipt_schema()),
        &activity_contract(edited),
    );
    assert!(diff.deltas.is_empty(), "{diff:#?}");
}

#[test]
fn withdrawing_an_activity_schema_is_breaking() {
    let base = activity_contract(receipt_schema());
    let mut cur = base.clone();
    cur.activities[0].output_schema = None;
    let diff = diff_schema_contracts(&base, &reparse(&cur));
    let d = only_breaking(&diff)[0];
    assert_eq!(d.subject, SchemaSubject::Activity);
    assert_eq!(d.change, ChangeKind::SchemaRemoved);
}

#[test]
fn adding_an_activity_is_compatible() {
    let empty = WorkflowSchemaContract::from_entries("0.0.0-test", Vec::new());
    let added = diff_schema_contracts(&empty, &activity_contract(receipt_schema()));
    assert!(!added.has_breaking());
    assert_eq!(added.deltas[0].change, ChangeKind::ActivityAdded);
    assert_eq!(added.deltas[0].subject, SchemaSubject::Activity);
}

/// The generator list is not tied to the runtime registry. A silent removal
/// would let a later re-add change the output type with no check at all.
#[test]
fn removing_an_activity_is_breaking() {
    let empty = WorkflowSchemaContract::from_entries("0.0.0-test", Vec::new());
    let removed = diff_schema_contracts(&activity_contract(receipt_schema()), &empty);
    let d = &removed.deltas[0];
    assert_eq!(d.change, ChangeKind::ActivityRemoved);
    assert_eq!(d.subject, SchemaSubject::Activity);
    assert_eq!(d.workflow, "charge");
    assert_eq!(d.verdict, Verdict::Breaking);
}

#[test]
fn an_activity_and_a_workflow_with_one_name_are_diffed_apart() {
    let mut base = WorkflowSchemaContract::from_entries(
        "0.0.0-test",
        vec![WorkflowSchemaEntry {
            name: "charge".to_string(),
            description: None,
            input_schema: None,
            output_schema: Some(receipt_schema()),
            error_schema: None,
        }],
    );
    base.activities = activity_contract(receipt_schema()).activities;
    let base = reparse(&base);
    let mut cur = base.clone();
    cur.activities[0].output_schema = Some(json!({"type": "object", "properties": {}}));
    let diff = diff_schema_contracts(&base, &reparse(&cur));
    let breaking = only_breaking(&diff);
    assert_eq!(breaking.len(), 1, "{diff:#?}");
    assert_eq!(breaking[0].subject, SchemaSubject::Activity);
}

// ── side effects ─────────────────────────────────────────────────────────────

#[test]
fn removing_an_enum_value_from_a_side_effect_is_breaking() {
    let base = side_effect_contract("checkout", variant_schema());
    let cur = side_effect_contract("checkout", json!({"type": "string", "enum": ["a"]}));
    let diff = diff_schema_contracts(&base, &cur);
    let d = only_breaking(&diff)[0];
    assert_eq!(d.subject, SchemaSubject::SideEffect);
    assert_eq!(d.workflow, "checkout");
    assert_eq!(d.side_effect.as_deref(), Some("pick"));
    assert_eq!(d.role, Some(SchemaRole::Value));
    assert_eq!(d.change, ChangeKind::EnumValueRemoved);
}

#[test]
fn side_effects_are_keyed_by_workflow_and_id() {
    let mut base = side_effect_contract("checkout", variant_schema());
    base.side_effects.push(SideEffectSchemaEntry {
        workflow: "refund".to_string(),
        id: "pick".to_string(),
        value_schema: Some(json!({"type": "integer"})),
    });
    let base = reparse(&base);
    assert_eq!(
        base.side_effects.len(),
        2,
        "one id in two workflows is two entries"
    );
    let mut cur = base.clone();
    cur.side_effects[1].value_schema = Some(json!({"type": "string"}));
    let diff = diff_schema_contracts(&base, &reparse(&cur));
    let breaking = only_breaking(&diff);
    assert_eq!(breaking.len(), 1, "{diff:#?}");
    assert_eq!(breaking[0].subject, SchemaSubject::SideEffect);
    assert_eq!(breaking[0].workflow, "refund");
    assert_eq!(breaking[0].side_effect.as_deref(), Some("pick"));
}

#[test]
fn adding_a_side_effect_is_compatible() {
    let empty = WorkflowSchemaContract::from_entries("0.0.0-test", Vec::new());
    let with = side_effect_contract("checkout", variant_schema());
    let added = diff_schema_contracts(&empty, &with);
    assert_eq!(added.deltas[0].change, ChangeKind::SideEffectAdded);
    assert!(!added.has_breaking());
}

/// A side-effect declaration is not registered with the runtime. Its call site
/// can stay after the declaration goes, so a removal must not stop the check
/// in silence.
#[test]
fn removing_a_side_effect_declaration_is_breaking() {
    let empty = WorkflowSchemaContract::from_entries("0.0.0-test", Vec::new());
    let with = side_effect_contract("checkout", variant_schema());
    let removed = diff_schema_contracts(&with, &empty);
    let d = &removed.deltas[0];
    assert_eq!(d.change, ChangeKind::SideEffectRemoved);
    assert_eq!(d.subject, SchemaSubject::SideEffect);
    assert_eq!(d.side_effect.as_deref(), Some("pick"));
    assert_eq!(d.verdict, Verdict::Breaking);
}

/// A workflow removal is compatible (issue #520 gates it). Its side effects go
/// with it, so they must not each need an acknowledgement.
#[test]
fn removing_a_side_effect_with_its_workflow_is_compatible() {
    let mut with = side_effect_contract("checkout", variant_schema());
    with.workflows = WorkflowSchemaContract::from_infos("0.0.0-test", &[checkout_info()]).workflows;
    let with = reparse(&with);
    let empty = WorkflowSchemaContract::from_entries("0.0.0-test", Vec::new());
    let diff = diff_schema_contracts(&with, &empty);
    assert!(!diff.has_breaking(), "{diff:#?}");
    assert!(
        diff.deltas
            .iter()
            .any(|d| d.change == ChangeKind::SideEffectRemoved),
        "{diff:#?}"
    );
}

// ── identity, acknowledgement and output ─────────────────────────────────────

fn workflow_ack_for(d: &SchemaDelta) -> AcknowledgedBreakingChange {
    AcknowledgedBreakingChange {
        subject: SchemaSubject::Workflow,
        workflow: d.workflow.clone(),
        side_effect: None,
        role: d.role,
        field_path: d.field_path.clone(),
        change: d.change,
        reason: "drained".to_string(),
        recorded_in: None,
    }
}

#[test]
fn a_workflow_ack_does_not_cover_an_activity_of_the_same_name() {
    let base = activity_contract(receipt_schema());
    let cur = activity_contract(json!({"type": "object", "properties": {}}));
    let diff = diff_schema_contracts(&base, &cur);
    let mut head = cur;
    head.acknowledged_breaking_changes = diff.breaking().map(workflow_ack_for).collect();
    let missing = unacknowledged_breaking(&diff, &base, &head);
    assert_eq!(missing.len(), 1, "the subject is part of the identity");
}

#[test]
fn acknowledged_update_records_the_subject_and_the_side_effect_id() {
    let base = side_effect_contract("checkout", variant_schema());
    let cur = side_effect_contract("checkout", json!({"type": "string", "enum": ["a"]}));
    let next = base
        .acknowledged_update(&cur, "drained", None)
        .expect("a reasoned acknowledgement is accepted");
    let ack = &next.acknowledged_breaking_changes[0];
    assert_eq!(ack.subject, SchemaSubject::SideEffect);
    assert_eq!(ack.side_effect.as_deref(), Some("pick"));
    let diff = diff_schema_contracts(&base, &cur);
    assert_eq!(
        unacknowledged_breaking(&diff, &base, &next),
        Vec::<&SchemaDelta>::new()
    );
}

/// The side-effect id is part of the ack identity: an ack for `pick` does not
/// cover a break in `roll` of the same workflow.
#[test]
fn an_ack_for_one_side_effect_does_not_cover_another() {
    let mut base = side_effect_contract("checkout", variant_schema());
    base.side_effects.push(SideEffectSchemaEntry {
        workflow: "checkout".to_string(),
        id: "roll".to_string(),
        value_schema: Some(variant_schema()),
    });
    let base = reparse(&base);
    let mut cur = base.clone();
    let roll = cur
        .side_effects
        .iter_mut()
        .find(|s| s.id == "roll")
        .unwrap();
    roll.value_schema = Some(json!({"type": "string", "enum": ["a"]}));
    let cur = reparse(&cur);
    let diff = diff_schema_contracts(&base, &cur);
    let d = only_breaking(&diff)[0];
    assert_eq!(d.side_effect.as_deref(), Some("roll"));

    let mut ack = workflow_ack_for(d);
    ack.subject = SchemaSubject::SideEffect;
    ack.side_effect = Some("pick".to_string());
    let mut head = cur;
    head.acknowledged_breaking_changes = vec![ack];
    assert_eq!(unacknowledged_breaking(&diff, &base, &head).len(), 1);

    let mut retargeted = head.clone();
    retargeted.acknowledged_breaking_changes[0].side_effect = Some("roll".to_string());
    assert_eq!(dropped_acknowledgements(&head, &retargeted).len(), 1);
}

#[test]
fn retargeting_an_ack_from_a_workflow_to_an_activity_is_a_dropped_record() {
    let base_diff = diff_schema_contracts(
        &activity_contract(receipt_schema()),
        &activity_contract(json!({"type": "object", "properties": {}})),
    );
    let d = only_breaking(&base_diff)[0];
    let mut base = activity_contract(receipt_schema());
    base.acknowledged_breaking_changes = vec![workflow_ack_for(d)];
    let mut head = base.clone();
    head.acknowledged_breaking_changes[0].subject = SchemaSubject::Activity;
    assert_eq!(dropped_acknowledgements(&base, &head).len(), 1);
}

#[test]
fn a_workflow_delta_serialises_without_the_new_keys() {
    let wf = |schema: Value| {
        WorkflowSchemaContract::from_entries(
            "0.0.0-test",
            vec![WorkflowSchemaEntry {
                name: "w".to_string(),
                description: None,
                input_schema: Some(schema),
                output_schema: None,
                error_schema: None,
            }],
        )
    };
    let diff = diff_schema_contracts(
        &wf(json!({"type": "string"})),
        &wf(json!({"type": "integer"})),
    );
    let v = serde_json::to_value(&diff.deltas[0]).unwrap();
    assert!(v.get("subject").is_none(), "{v}");
    assert!(v.get("side_effect").is_none(), "{v}");

    let diff = diff_schema_contracts(
        &activity_contract(receipt_schema()),
        &activity_contract(json!({"type": "object", "properties": {}})),
    );
    let v = serde_json::to_value(&diff.deltas[0]).unwrap();
    assert_eq!(v["subject"], "activity", "{v}");
    assert!(v.get("side_effect").is_none(), "{v}");

    let diff = diff_schema_contracts(
        &side_effect_contract("checkout", variant_schema()),
        &side_effect_contract("checkout", json!({"type": "string", "enum": ["a"]})),
    );
    let v = serde_json::to_value(&diff.deltas[0]).unwrap();
    assert_eq!(
        v,
        json!({
            "subject": "side_effect",
            "workflow": "checkout",
            "side_effect": "pick",
            "role": "value",
            "field_path": "",
            "change": "enum_value_removed",
            "verdict": "breaking",
            "reason": v["reason"],
        }),
        "the side-effect wire format is pinned"
    );
}

#[test]
fn subject_labels_name_the_kind_of_item() {
    assert_eq!(
        subject_label(SchemaSubject::Workflow, "onboarding", None),
        "onboarding"
    );
    assert_eq!(
        subject_label(SchemaSubject::Activity, "charge", None),
        "activity:charge"
    );
    assert_eq!(
        subject_label(SchemaSubject::SideEffect, "checkout", Some("pick")),
        "checkout/side_effect:pick"
    );
    assert_eq!(Verdict::Breaking.as_str(), "breaking");
}
