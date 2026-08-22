//! Integration tests for the `harvest backup verify` subcommand (issue #943).
//!
//! Black-box tests over the crate's public backup-verify surface: the pure
//! guard/format/gate helpers. `run_backup_verify` itself needs a live database
//! and is not exercised here; its parts (guard, format, gate) are, and the DB
//! half is
//! covered by the core crate's `backup_verify_tests` integration suite; these
//! tests own the *operator-facing contract*: the scratch-DSN guard (AC4), the
//! exit-code mapping (`AC2d`), and the machine-readable report shape.

use autumn_harvest::backup_verify::{
    Finding, FindingClass, ReplaySummary, RestoreVerifyReport, ShardVerifyReport, VerifyStatus,
};
use autumn_harvest_cli::{
    BackupVerifyFormat, CliError, backup_verify_gate, backup_verify_json,
    format_backup_verify_text, parse_shard_targets, scratch_guard, scratch_guard_warning,
};

fn shard(findings: Vec<Finding>) -> ShardVerifyReport {
    ShardVerifyReport {
        shard_id: 0,
        dsn: "postgres://scratch/harvest_restore".to_string(),
        reachable: true,
        unreachable_reason: None,
        latest_event_at: None,
        non_terminal_executions: Some(3),
        replay: ReplaySummary {
            sampled: 3,
            clean: 3,
            divergent: 0,
            failed: 0,
            skipped_no_handler: 0,
            unreadable: 0,
        },
        findings,
    }
}

fn report(findings: Vec<Finding>) -> RestoreVerifyReport {
    RestoreVerifyReport::assemble(chrono::Utc::now(), vec![shard(findings)], Vec::new())
}

// ── AC4: the scratch guard ─────────────────────────────────────────────────

#[test]
fn a_dsn_matching_the_live_config_is_refused_without_the_ack() {
    let live = "postgres://app:secret@prod-db.internal:5432/harvest";
    let same = "postgres://reader@prod-db.internal:5432/harvest";
    let err = scratch_guard(&[same.to_string()], &[live.to_string()], false)
        .expect_err("a live-matching DSN must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("--i-know-this-is-scratch"),
        "the refusal must name the override flag: {msg}"
    );
    assert!(
        !msg.contains("secret"),
        "the refusal must not echo the live password: {msg}"
    );
}

#[test]
fn the_explicit_ack_overrides_the_live_dsn_guard() {
    let live = "postgres://app:secret@prod-db.internal:5432/harvest";
    let same = "postgres://reader@prod-db.internal:5432/harvest";
    scratch_guard(&[same.to_string()], &[live.to_string()], true)
        .expect("an explicit acknowledgement must be honoured");
}

#[test]
fn a_genuine_scratch_dsn_needs_no_ack() {
    let live = "postgres://app:secret@prod-db.internal:5432/harvest";
    let scratch = "postgres://postgres@127.0.0.1:5433/harvest_restore_drill";
    scratch_guard(&[scratch.to_string()], &[live.to_string()], false)
        .expect("a genuinely different target must not be refused");
}

#[test]
fn with_no_live_config_supplied_the_guard_is_inert_but_says_so() {
    scratch_guard(
        &["postgres://postgres@127.0.0.1:5432/anything".to_string()],
        &[],
        false,
    )
    .expect("nothing to compare against means nothing to refuse");
    // Inert is fine; SILENTLY inert is not -- an operator who believes the
    // safety net is engaged is the one who points this at production.
    let w = scratch_guard_warning(&[], false).expect("an unguarded run must warn");
    assert!(
        w.contains("--live-dsn"),
        "the warning must name the fix: {w}"
    );
}

#[test]
fn the_ack_override_warns_that_nothing_was_checked() {
    let w = scratch_guard_warning(
        &["postgres://postgres@127.0.0.1:5432/live".to_string()],
        true,
    )
    .expect("an acked run must warn that the guard did not run");
    assert!(
        w.contains("--i-know-this-is-scratch"),
        "the warning must name the flag that disabled the guard: {w}"
    );
}

#[test]
fn a_run_guarded_against_a_real_live_dsn_does_not_warn() {
    assert!(
        scratch_guard_warning(
            &["postgres://postgres@127.0.0.1:5432/live".to_string()],
            false
        )
        .is_none(),
        "a genuinely guarded run must be silent"
    );
}

#[test]
fn every_live_shard_dsn_is_guarded_against_not_just_the_first() {
    // A sharded fleet needs one --live-dsn per live shard. The guard must
    // compare against ALL of them, or naming shard 0 would let a --shard
    // pointed at live shard 1 through.
    let live = vec![
        "postgres://postgres@127.0.0.1:5432/live0".to_string(),
        "postgres://postgres@127.0.0.1:5432/live1".to_string(),
    ];
    let err = scratch_guard(
        &["1=postgres://postgres@127.0.0.1:5432/live1".to_string()],
        &live,
        false,
    )
    .expect_err("the second live shard must be guarded against too");
    assert!(matches!(err, CliError::InvalidInput(_)));
}

// ── AC3: multiple shard DSNs ───────────────────────────────────────────────

#[test]
fn shard_targets_default_to_shard_zero_when_unprefixed() {
    let targets =
        parse_shard_targets(&["postgres://h/a".to_string()]).expect("a bare DSN is shard 0");
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].shard_id, 0);
    assert_eq!(targets[0].dsn, "postgres://h/a");
}

#[test]
fn shard_targets_accept_an_explicit_shard_prefix() {
    let targets = parse_shard_targets(&[
        "0=postgres://h/a".to_string(),
        "3=postgres://h/b".to_string(),
    ])
    .expect("explicit prefixes parse");
    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0].shard_id, 0);
    assert_eq!(targets[1].shard_id, 3);
    assert_eq!(targets[1].dsn, "postgres://h/b");
}

#[test]
fn duplicate_shard_ids_are_rejected() {
    let err = parse_shard_targets(&[
        "1=postgres://h/a".to_string(),
        "1=postgres://h/b".to_string(),
    ])
    .expect_err("two DSNs for one shard is a configuration error");
    assert!(err.to_string().contains('1'), "{err}");
}

#[test]
fn an_empty_shard_list_is_rejected() {
    parse_shard_targets(&[]).expect_err("at least one shard DSN is required");
}

/// An `ExecutionId` carries its shard in the UUID's first two bytes as
/// `shard & 0xFFFF`, and `0xFFFF` is the reserved `ShardId::UNENCODED`
/// sentinel. A shard id outside `0..=0xFFFE` therefore does NOT survive the
/// round trip: `65536` truncates to `0`, so every target id read out of the
/// database the operator supplied under `65536=` decodes as shard `0`, is not
/// found in the supplied map, and is written off as "on an uninspected shard"
/// -- an advisory, exit 0, with the shard silently never checked.
///
/// Reject it at parse time with the same rule the shard router uses.
#[test]
fn unencodable_shard_ids_are_rejected() {
    for bad in ["65535", "65536", "70000"] {
        let spec = format!("{bad}=postgres://h/a");
        match parse_shard_targets(std::slice::from_ref(&spec)) {
            Ok(_) => {
                panic!("shard {bad} must be rejected: it cannot round-trip through an ExecutionId")
            }
            Err(e) => assert!(
                e.to_string().contains(bad),
                "the error must name the offending id: {e}"
            ),
        }
    }
}

/// The control: the boundary value is still accepted, so the guard rejects
/// only what genuinely cannot round-trip.
#[test]
fn the_largest_encodable_shard_id_is_accepted() {
    let targets = parse_shard_targets(&["65534=postgres://h/a".to_string()])
        .expect("0xFFFE is the largest encodable shard and must parse");
    assert_eq!(targets[0].shard_id, 65534);
}

// ── AC2(d): exit-code mapping ──────────────────────────────────────────────

#[test]
fn a_reclaimable_only_report_passes_the_gate() {
    let r = report(vec![Finding::new(
        FindingClass::DeadWorkerRunningTask,
        Some(0),
        4,
        vec!["t1".into()],
    )]);
    assert_eq!(r.status, VerifyStatus::ResumableWithReclaim);
    assert!(
        backup_verify_gate(&r).is_none(),
        "an expected post-restore artifact must not fail the drill"
    );
}

#[test]
fn an_incoherent_report_fails_the_gate() {
    let r = report(vec![Finding::new(
        FindingClass::DanglingTaskExecution,
        Some(0),
        1,
        vec!["t9".into()],
    )]);
    assert_eq!(r.status, VerifyStatus::Incoherent);
    let err = backup_verify_gate(&r).expect("incoherence must fail the gate");
    assert_eq!(err.exit_code(), 1);
}

#[test]
fn an_unavailable_report_exits_two_so_ci_can_tell_it_apart() {
    let unreachable = ShardVerifyReport::unreachable(
        1,
        "postgres://scratch/shard1".to_string(),
        "connect failed".to_string(),
    );
    let r = RestoreVerifyReport::assemble(
        chrono::Utc::now(),
        vec![shard(Vec::new()), unreachable],
        Vec::new(),
    );
    assert_eq!(r.status, VerifyStatus::Unavailable);
    let err = backup_verify_gate(&r).expect("an undetermined verdict must fail the gate");
    assert_eq!(
        err.exit_code(),
        2,
        "'could not determine' must be distinguishable from 'determined broken'"
    );
}

#[test]
fn a_clean_report_passes_the_gate() {
    let r = report(Vec::new());
    assert_eq!(r.status, VerifyStatus::Clean);
    assert!(backup_verify_gate(&r).is_none());
}

// ── AC2(d): the machine-readable report ────────────────────────────────────

#[test]
fn json_output_says_whether_replay_actually_ran() {
    // A JSON consumer must be able to tell "the data is coherent" from "your
    // workflow code still replays it" WITHOUT re-deriving the rule -- the
    // shipped CLI can never do the latter.
    let mut s = shard(Vec::new());
    s.replay = ReplaySummary {
        sampled: 4,
        clean: 0,
        divergent: 0,
        failed: 0,
        skipped_no_handler: 4,
        unreadable: 0,
    };
    let r = RestoreVerifyReport::assemble(chrono::Utc::now(), vec![s], Vec::new());
    let v: serde_json::Value =
        serde_json::from_str(&backup_verify_json(&r).expect("serialise")).expect("valid json");
    assert_eq!(
        v["replay_verified"],
        serde_json::json!(false),
        "an all-skipped run must not read as replay-verified: {v}"
    );
    assert_eq!(
        v["status"],
        serde_json::json!("clean"),
        "skipping replay is not itself a failure: {v}"
    );
}

#[test]
fn json_output_is_machine_readable_and_carries_the_verdict() {
    let r = report(vec![Finding::new(
        FindingClass::ChildTerminalRolledBack,
        Some(0),
        2,
        vec!["c1".into(), "c2".into()],
    )]);
    let json = backup_verify_json(&r).expect("serialise");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    assert_eq!(parsed["status"], "incoherent");
    assert!(parsed["shards"].is_array());
    let classes: Vec<&str> = parsed["shards"][0]["findings"]
        .as_array()
        .expect("findings array")
        .iter()
        .filter_map(|f| f["class"].as_str())
        .collect();
    assert!(
        classes.contains(&"child_terminal_rolled_back"),
        "the finding class must be machine-readable: {classes:?}"
    );
}

#[test]
fn text_output_names_the_verdict_the_class_and_what_heals_it() {
    let r = report(vec![Finding::new(
        FindingClass::DeadWorkerRunningTask,
        Some(0),
        4,
        vec!["t1".into()],
    )]);
    let text = format_backup_verify_text(&r);
    assert!(text.contains("resumable_with_reclaim"), "{text}");
    assert!(text.contains("dead_worker_running_task"), "{text}");
    assert!(
        text.contains("poison-pill"),
        "an operator must be told what heals it: {text}"
    );
}

#[test]
fn text_output_reports_replay_coverage_honestly() {
    let mut s = shard(Vec::new());
    s.replay = ReplaySummary {
        sampled: 5,
        clean: 0,
        divergent: 0,
        failed: 0,
        skipped_no_handler: 5,
        unreadable: 0,
    };
    let r = RestoreVerifyReport::assemble(chrono::Utc::now(), vec![s], Vec::new());
    let text = format_backup_verify_text(&r);
    // `contains("skipped")` alone is not falsifiable -- the verified branch
    // prints that word too. Pin the verdict token that only the unverified
    // branch emits, so a renderer that silently claims coverage fails here.
    assert!(
        text.contains("NOT VERIFIED"),
        "a report where nothing was replayable must say NOT VERIFIED: {text}"
    );
    assert!(
        !r.replay.verified(),
        "replay must not count as verified when every sample was skipped"
    );
}

#[test]
fn text_output_does_not_claim_not_verified_when_replay_really_ran() {
    let mut s = shard(Vec::new());
    s.replay = ReplaySummary {
        sampled: 5,
        clean: 5,
        divergent: 0,
        failed: 0,
        skipped_no_handler: 0,
        unreadable: 0,
    };
    let r = RestoreVerifyReport::assemble(chrono::Utc::now(), vec![s], Vec::new());
    let text = format_backup_verify_text(&r);
    assert!(
        !text.contains("NOT VERIFIED"),
        "genuine replay coverage must not be reported as unverified: {text}"
    );
    assert!(r.replay.verified(), "5 clean of 5 sampled is verified");
}

#[test]
fn the_default_output_format_is_text() {
    assert_eq!(BackupVerifyFormat::default(), BackupVerifyFormat::Text);
}

/// The regression the CLI smoke test caught: an unmigrated (empty) "restore"
/// makes every probe fail, and folding that into an advisory reported
/// `resumable_with_reclaim` + exit 0 — telling an operator to start workers
/// against a database with no harvest schema. Must be exit 2, and the message
/// must name the real cause (failed probes, not unreachable shards).
#[test]
fn a_report_whose_probes_never_ran_exits_two_and_says_why() {
    let r = report(vec![Finding::new(
        FindingClass::ProbeFailed,
        Some(0),
        9,
        vec!["relation \"harvest_task_queue\" does not exist".into()],
    )]);
    assert_eq!(
        r.status,
        VerifyStatus::Unavailable,
        "probes that could not run must never read as a pass"
    );
    let err = backup_verify_gate(&r).expect("must fail the gate");
    assert_eq!(err.exit_code(), 2);
    let msg = err.to_string();
    assert!(
        msg.contains("9 probe(s) could not run"),
        "the message must name the real cause: {msg}"
    );
    assert!(
        msg.contains("0 shard(s) unreachable"),
        "and must not blame shard reachability alone: {msg}"
    );
    let text = format_backup_verify_text(&r);
    assert!(text.contains("UNDETERMINED"), "{text}");
    assert!(text.contains("probe could not run"), "{text}");
}
