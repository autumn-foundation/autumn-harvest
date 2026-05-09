/// Red-phase tests for issue #173: deterministic guardrail rule catalog foundation.
///
/// These tests define the expected shape of the catalog before any implementation
/// exists. They should fail on first run (red), then pass once guardrail.rs is
/// fully implemented (green).
use autumn_harvest::guardrail::{
    GuardrailFinding, GuardrailSuppression, GuardrailSuppressionError, RuleCategory, RuleEntry,
    Severity, catalog,
};
use std::collections::HashSet;

// ── Catalog coverage ─────────────────────────────────────────────────────────

#[test]
fn catalog_covers_all_required_categories() {
    let entries = catalog();
    let categories: HashSet<String> = entries
        .iter()
        .map(|e| format!("{:?}", e.category))
        .collect();

    let required = [
        "WallClock",
        "Randomness",
        "ProcessEnv",
        "SleepTimer",
        "BackgroundTask",
        "DirectIo",
        "ProcessGlobal",
    ];

    for cat in required {
        assert!(
            categories.contains(cat),
            "catalog is missing category: {cat}"
        );
    }
}

#[test]
fn catalog_rule_ids_are_unique() {
    let entries = catalog();
    let mut ids = HashSet::new();
    for entry in entries {
        assert!(
            ids.insert(entry.id),
            "duplicate rule id in catalog: {}",
            entry.id
        );
    }
}

#[test]
fn catalog_rule_ids_match_hvg_prefix_pattern() {
    for entry in catalog() {
        assert!(
            entry.id.starts_with("HVG"),
            "rule id '{}' does not start with 'HVG'",
            entry.id
        );
        let suffix = &entry.id["HVG".len()..];
        assert!(
            suffix.chars().all(|c| c.is_ascii_digit()),
            "rule id '{}' suffix is not all digits",
            entry.id
        );
        assert!(
            !suffix.is_empty(),
            "rule id '{}' has no numeric suffix",
            entry.id
        );
    }
}

#[test]
fn catalog_every_entry_has_nonempty_explanation_and_alternative() {
    for entry in catalog() {
        assert!(
            !entry.explanation.trim().is_empty(),
            "rule '{}' has empty explanation",
            entry.id
        );
        assert!(
            !entry.alternative.trim().is_empty(),
            "rule '{}' has empty alternative",
            entry.id
        );
    }
}

#[test]
fn catalog_has_at_least_one_hard_blocker_per_required_category() {
    let entries = catalog();

    // Every required category must have at least one HardBlocker
    let hard_blocker_categories: HashSet<String> = entries
        .iter()
        .filter(|e| matches!(e.severity, Severity::HardBlocker))
        .map(|e| format!("{:?}", e.category))
        .collect();

    let required = [
        "WallClock",
        "Randomness",
        "ProcessEnv",
        "SleepTimer",
        "BackgroundTask",
        "DirectIo",
        "ProcessGlobal",
    ];

    for cat in required {
        assert!(
            hard_blocker_categories.contains(cat),
            "category {cat} has no HardBlocker rule"
        );
    }
}

#[test]
fn catalog_alternative_guidance_mentions_harvest_concepts() {
    // At least one alternative per required category should mention a Harvest
    // concept (activity, timer, signal, version, ctx.) so guidance is actionable.
    let harvest_keywords = [
        "activity",
        "timer",
        "signal",
        "ctx.",
        "version",
        "WorkflowContext",
        "ActivityContext",
        "harvest",
    ];

    let entries = catalog();
    let mut found_harvest_guidance = false;
    for entry in entries {
        let alt_lower = entry.alternative.to_lowercase();
        if harvest_keywords.iter().any(|kw| alt_lower.contains(*kw)) {
            found_harvest_guidance = true;
        }
    }

    assert!(
        found_harvest_guidance,
        "no catalog entry references a Harvest concept in its alternative guidance"
    );
}

// ── RuleEntry field types ─────────────────────────────────────────────────────

#[test]
fn rule_entry_exposes_id_severity_category_explanation_alternative() {
    let entry: &RuleEntry = catalog().first().expect("catalog must be non-empty");
    // Just checking field access compiles and produces expected types.
    let _: &str = entry.id;
    let _: &Severity = &entry.severity;
    let _: &RuleCategory = &entry.category;
    let _: &str = entry.explanation;
    let _: &str = entry.alternative;
}

// ── GuardrailFinding ─────────────────────────────────────────────────────────

#[test]
fn guardrail_finding_fields_accessible() {
    let finding = GuardrailFinding {
        rule_id: "HVG001".to_string(),
        severity: Severity::HardBlocker,
        category: RuleCategory::WallClock,
        message: "Used std::time::Instant::now() in workflow body".to_string(),
        alternative: "Use ctx.current_time() or move time reads into an activity".to_string(),
        workflow_name: Some("checkout_workflow".to_string()),
        source_location: Some("src/workflows/checkout.rs:42".to_string()),
    };

    assert_eq!(finding.rule_id, "HVG001");
    assert!(matches!(finding.severity, Severity::HardBlocker));
    assert!(matches!(finding.category, RuleCategory::WallClock));
    assert!(finding.workflow_name.is_some());
    assert!(finding.source_location.is_some());
}

#[test]
fn guardrail_finding_optional_workflow_name_and_location() {
    let finding = GuardrailFinding {
        rule_id: "HVG002".to_string(),
        severity: Severity::HardBlocker,
        category: RuleCategory::Randomness,
        message: "rand::random() called in workflow body".to_string(),
        alternative: "Pass randomness as workflow input or record via a side-effect activity"
            .to_string(),
        workflow_name: None,
        source_location: None,
    };

    assert!(finding.workflow_name.is_none());
    assert!(finding.source_location.is_none());
}

#[test]
fn guardrail_finding_is_debug_and_clone() {
    let finding = GuardrailFinding {
        rule_id: "HVG001".to_string(),
        severity: Severity::HardBlocker,
        category: RuleCategory::WallClock,
        message: "test".to_string(),
        alternative: "use activity".to_string(),
        workflow_name: None,
        source_location: None,
    };
    let cloned = finding.clone();
    assert_eq!(finding.rule_id, cloned.rule_id);
    let _ = format!("{finding:?}");
}

// ── GuardrailSuppression ──────────────────────────────────────────────────────

#[test]
fn suppression_requires_nonempty_reason() {
    let result = GuardrailSuppression::new("HVG001", "");
    assert!(result.is_err(), "empty reason string should be rejected");
    match result {
        Err(GuardrailSuppressionError::EmptyReason) => {}
        other => panic!("expected EmptyReason, got {other:?}"),
    }
}

#[test]
fn suppression_accepts_nonempty_reason() {
    let suppression = GuardrailSuppression::new(
        "HVG001",
        "Seeded PRNG, determinism verified by replay test suite",
    )
    .expect("non-empty reason must succeed");
    assert_eq!(suppression.rule_id(), "HVG001");
    assert!(!suppression.reason().is_empty());
}

#[test]
fn suppression_whitespace_only_reason_is_rejected() {
    let result = GuardrailSuppression::new("HVG002", "   ");
    assert!(result.is_err(), "whitespace-only reason should be rejected");
}

#[test]
fn suppression_empty_rule_id_is_rejected() {
    let result = GuardrailSuppression::new("", "valid reason");
    assert!(result.is_err(), "empty rule ID should be rejected");
    match result {
        Err(GuardrailSuppressionError::EmptyRuleId) => {}
        other => panic!("expected EmptyRuleId, got {other:?}"),
    }
}

#[test]
fn suppression_whitespace_rule_id_is_rejected() {
    let result = GuardrailSuppression::new("   ", "valid reason");
    assert!(
        result.is_err(),
        "whitespace-only rule ID should be rejected"
    );
}

#[test]
fn suppression_exposes_rule_id_and_reason() {
    let s = GuardrailSuppression::new("HVG003", "CI fixture, not a real workflow").unwrap();
    assert_eq!(s.rule_id(), "HVG003");
    assert_eq!(s.reason(), "CI fixture, not a real workflow");
}

#[test]
fn suppression_is_debug_and_clone() {
    let s = GuardrailSuppression::new("HVG004", "reason").unwrap();
    let cloned = s.clone();
    assert_eq!(s.rule_id(), cloned.rule_id());
    let _ = format!("{s:?}");
}

// ── Severity and Category traits ──────────────────────────────────────────────

#[test]
fn severity_is_debug_clone_eq() {
    let a = Severity::HardBlocker;
    let b = Severity::Warning;
    let _ = format!("{a:?}");
    let _ = format!("{b:?}");
    assert_ne!(a, b);
    assert_eq!(a, a.clone());
}

#[test]
fn category_is_debug_clone_eq() {
    let cats = [
        RuleCategory::WallClock,
        RuleCategory::Randomness,
        RuleCategory::ProcessEnv,
        RuleCategory::SleepTimer,
        RuleCategory::BackgroundTask,
        RuleCategory::DirectIo,
        RuleCategory::ProcessGlobal,
    ];
    let mut seen = HashSet::new();
    for cat in &cats {
        let s = format!("{cat:?}");
        assert!(seen.insert(s.clone()), "duplicate category debug repr: {s}");
        let cloned = cat.clone();
        assert_eq!(cat, &cloned);
    }
}

// ── catalog lookup helper ─────────────────────────────────────────────────────

#[test]
fn catalog_lookup_by_id_returns_correct_entry() {
    use autumn_harvest::guardrail::rule_by_id;

    let entry = rule_by_id("HVG001").expect("HVG001 must exist in catalog");
    assert_eq!(entry.id, "HVG001");
    assert!(matches!(entry.category, RuleCategory::WallClock));
}

#[test]
fn catalog_lookup_missing_id_returns_none() {
    use autumn_harvest::guardrail::rule_by_id;

    assert!(rule_by_id("HVGZZZZ").is_none());
    assert!(rule_by_id("").is_none());
}

// ── Finding construction from catalog entry ───────────────────────────────────

#[test]
fn finding_from_rule_entry() {
    use autumn_harvest::guardrail::rule_by_id;

    let entry = rule_by_id("HVG002").unwrap();
    let finding = GuardrailFinding::from_rule(entry, "rand called here", None, None);
    assert_eq!(finding.rule_id, "HVG002");
    assert!(matches!(finding.category, RuleCategory::Randomness));
    assert!(!finding.message.is_empty());
    assert!(!finding.alternative.is_empty());
}
