//! `harvest partition` — the opt-in partitioned `harvest_events` layout
//! (issue #958).
//!
//! Argument-surface guards, deliberately about the *shape* of the command
//! rather than its effect: the effect is proven against a live Postgres in the
//! core crate's `event_partitioning_tests`. What can only be checked here is
//! that the two irreversible commands are hard to run by accident.
//!
//! Both matter. `enable` takes a brief `ACCESS EXCLUSIVE` lock on
//! `harvest_events` during which every append waits — and on a populated table
//! that window covers two index builds and a full-table constraint validation.
//! `disable` copies every surviving event row back into a plain table, which
//! rewrites the whole thing. Neither should be a one-liner run by reflex.

use clap::Parser as _;

use autumn_harvest_cli::Cli;

/// `Commands` is deliberately private, so these assert through the parsed
/// command's `Debug` rendering rather than widening the CLI's public API for a
/// test's convenience.
fn parse(args: &[&str]) -> Result<String, clap::Error> {
    Cli::try_parse_from(args).map(|cli| format!("{cli:?}"))
}

#[test]
fn partition_status_needs_only_a_shard_dsn() {
    let rendered = parse(&[
        "harvest",
        "partition",
        "status",
        "--shard",
        "postgres://h/s0",
    ])
    .expect("status parses");
    assert!(rendered.contains("Status"), "{rendered}");
    assert!(rendered.contains("postgres://h/s0"), "{rendered}");
}

/// The plan is read-only advice — it prints SQL and touches no database — so it
/// must not demand a shard DSN the operator may not have to hand yet.
#[test]
fn the_migration_plan_needs_no_database_at_all() {
    parse(&["harvest", "partition", "plan"]).expect("plan parses with no arguments");
}

#[test]
fn enabling_requires_acknowledging_the_lock_window() {
    let err = parse(&[
        "harvest",
        "partition",
        "enable",
        "--shard",
        "postgres://h/s0",
    ])
    .map(|rendered| {
        // Parsing succeeding is fine — the guard is a runtime refusal — but
        // the flag must exist and be off by default, or there is nothing to
        // refuse on.
        assert!(
            !rendered.contains("confirm: true"),
            "the lock-window acknowledgement must default to OFF: {rendered}"
        );
        rendered
    })
    .expect("enable parses; the confirmation is enforced at run time");
    assert!(err.contains("Enable"), "{err}");

    let confirmed = parse(&[
        "harvest",
        "partition",
        "enable",
        "--shard",
        "postgres://h/s0",
        "--i-understand-the-lock-window",
    ])
    .expect("the acknowledgement flag is accepted");
    assert!(
        confirmed.contains("confirm: true"),
        "the acknowledgement must actually register: {confirmed}"
    );
}

#[test]
fn reverting_requires_acknowledging_the_table_rewrite() {
    let rendered = parse(&[
        "harvest",
        "partition",
        "disable",
        "--shard",
        "postgres://h/s0",
    ])
    .expect("disable parses; the confirmation is enforced at run time");
    assert!(
        !rendered.contains("confirm: true"),
        "the rewrite acknowledgement must default to OFF: {rendered}"
    );

    let confirmed = parse(&[
        "harvest",
        "partition",
        "disable",
        "--shard",
        "postgres://h/s0",
        "--i-understand-this-rewrites-the-table",
    ])
    .expect("the acknowledgement flag is accepted");
    assert!(
        confirmed.contains("confirm: true"),
        "the acknowledgement must actually register: {confirmed}"
    );
}

/// The cohort width and the lookahead are the two settings that decide both
/// reclamation granularity and the live partition count, so both must be
/// reachable from the command an operator actually runs — not only from the
/// library defaults.
#[test]
fn the_cohort_width_and_lookahead_are_operator_settable() {
    let rendered = parse(&[
        "harvest",
        "partition",
        "enable",
        "--shard",
        "postgres://h/s0",
        "--cohort-width-secs",
        "604800",
        "--lookahead-cohorts",
        "2",
        "--i-understand-the-lock-window",
    ])
    .expect("enable accepts the sizing settings");
    assert!(rendered.contains("604800"), "{rendered}");
}
