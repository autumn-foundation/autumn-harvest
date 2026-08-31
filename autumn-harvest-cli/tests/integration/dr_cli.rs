//! `harvest dr` — cross-region DR operator commands (issue #954).
//!
//! Argument-surface guards. These are deliberately about the *shape* of the
//! command rather than its effect, because the effect is proven against a live
//! Postgres in the core crate's `cross_region_dr_tests`. What can only be
//! checked here is that the destructive command is hard to run by accident: a
//! fence bump stops an entire fleet, and a fat-fingered one during a healthy
//! week is an outage.

use clap::Parser as _;

use autumn_harvest_cli::Cli;

/// `Commands` is deliberately private, so these assert through the parsed
/// command's `Debug` rendering rather than widening the CLI's public API for
/// a test's convenience. The safety guards below — which are the point — are
/// parse *failures* and need no field access at all.
fn parse(args: &[&str]) -> Result<String, clap::Error> {
    Cli::try_parse_from(args).map(|cli| format!("{cli:?}"))
}

#[test]
fn dr_status_needs_only_a_shard_dsn() {
    let rendered =
        parse(&["harvest", "dr", "status", "--shard", "postgres://h/s0"]).expect("status parses");
    assert!(rendered.contains("Status"), "{rendered}");
    assert!(rendered.contains("postgres://h/s0"), "{rendered}");
}

/// A fence bump stops every worker pinned to the old epoch — including the
/// ones in the region you are failing over *to*. It must not be a one-liner
/// anybody can run by reflex.
#[test]
fn fencing_refuses_to_run_without_an_explicit_confirmation() {
    let err = parse(&[
        "harvest",
        "dr",
        "fence",
        "--shard",
        "postgres://h/s0",
        "--reason",
        "failover",
    ])
    .expect_err("fencing without --i-understand-this-stops-the-fleet must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("i-understand-this-stops-the-fleet") || msg.contains("required"),
        "the rejection must name the missing confirmation flag: {msg}"
    );
}

/// A fence with no recorded reason is an unattributable outage three months
/// later. The column exists; make the CLI fill it.
#[test]
fn fencing_requires_a_recorded_reason() {
    let err = parse(&[
        "harvest",
        "dr",
        "fence",
        "--shard",
        "postgres://h/s0",
        "--i-understand-this-stops-the-fleet",
    ])
    .expect_err("fencing without --reason must be rejected");
    assert!(
        err.to_string().contains("reason") || err.to_string().contains("required"),
        "the rejection must name --reason: {err}"
    );
}

#[test]
fn a_fully_specified_fence_parses() {
    let rendered = parse(&[
        "harvest",
        "dr",
        "fence",
        "--shard",
        "0=postgres://h/s0",
        "--shard",
        "1=postgres://h/s1",
        "--reason",
        "failover to region B",
        "--i-understand-this-stops-the-fleet",
    ])
    .expect("a fully specified fence parses");
    // Fence-all-shards is the documented discipline, so the command must
    // accept a repeated --shard rather than one DSN at a time.
    assert!(rendered.contains("0=postgres://h/s0"), "{rendered}");
    assert!(rendered.contains("1=postgres://h/s1"), "{rendered}");
    assert!(rendered.contains("failover to region B"), "{rendered}");
}

/// `promote` is the step that advances sequences. It must exist as its own
/// verb: folding it into `fence` would let an operator fence without ever
/// advancing sequences, and the new primary's first append would then die on a
/// duplicate key.
#[test]
fn promote_is_its_own_verb() {
    let rendered =
        parse(&["harvest", "dr", "promote", "--shard", "postgres://h/s0"]).expect("promote parses");
    assert!(rendered.contains("Promote"), "{rendered}");
}
