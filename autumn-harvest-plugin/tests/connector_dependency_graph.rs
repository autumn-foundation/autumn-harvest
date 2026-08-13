//! AC1 guard: the core engine crate gains ZERO broker dependencies (issue #944).
//!
//! The whole reason the connector lives in `autumn-harvest-plugin` rather than
//! `autumn-harvest` is that the engine is Postgres-only. A broker client
//! leaking into the core dependency graph would silently break that invariant
//! for every embedder — including ones that never enable a connector feature.
//!
//! This test shells out to `cargo tree -p autumn-harvest` (with every core
//! feature on, so a feature-gated leak cannot hide) and fails if a broker or
//! cloud client ever appears.

use std::process::Command;

/// Crates that must never appear in `autumn-harvest`'s dependency graph.
///
/// Matched as whole crate names against `cargo tree --prefix none --format {lib}`
/// output, so a substring like `aws` inside an unrelated name cannot false-positive.
const FORBIDDEN: &[&str] = &[
    // Kafka
    "rdkafka",
    "rdkafka_sys",
    "kafka",
    // AWS / SQS
    "aws_sdk_sqs",
    "aws_config",
    "aws_smithy_runtime",
    "aws_smithy_client",
    "aws_types",
    // Other brokers a future adapter might reach for — the invariant is
    // "no broker client in core", not "no Kafka in core".
    "async_nats",
    "nats",
    "lapin",
    "rumqttc",
    "google_cloud_pubsub",
    "aws_sdk_kinesis",
];

fn cargo_tree(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO"))
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("`cargo tree` must be runnable from the plugin manifest dir");

    assert!(
        output.status.success(),
        "cargo tree {args:?} failed:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );

    String::from_utf8(output.stdout).expect("cargo tree output must be UTF-8")
}

/// Whole-line crate names from `--prefix none --format {lib}`, deduped-ish.
fn crate_names(tree: &str) -> Vec<&str> {
    tree.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        // `--format {lib}` renders the *lib target* name (underscored). Dedupe
        // markers like a trailing `(*)` are not emitted by this format, but be
        // defensive in case cargo changes.
        .map(|line| line.split_whitespace().next().unwrap_or(line))
        .collect()
}

#[test]
fn core_engine_has_no_broker_dependencies() {
    // `--all-features` is deliberate: a broker client sneaking in behind an
    // optional core feature is exactly the regression this guards against.
    let tree = cargo_tree(&[
        "tree",
        "-p",
        "autumn-harvest",
        "--all-features",
        "--edges",
        "normal,build",
        "--prefix",
        "none",
        "--format",
        "{lib}",
    ]);

    let names = crate_names(&tree);
    assert!(
        !names.is_empty(),
        "cargo tree produced no crate names; the guard would be vacuous"
    );

    let leaked: Vec<&str> = FORBIDDEN
        .iter()
        .copied()
        .filter(|forbidden| names.contains(forbidden))
        .collect();

    assert!(
        leaked.is_empty(),
        "issue #944 AC1 violated: broker client(s) {leaked:?} reached the core \
         `autumn-harvest` dependency graph. The engine must stay Postgres-only \
         — put the broker client behind a feature in `autumn-harvest-plugin` \
         and talk to the engine through a trait (the PayloadStore / \
         CompletionCallbackDeliverer / HistoryArchiver seam)."
    );
}

#[test]
fn the_guard_actually_sees_the_plugins_broker_clients() {
    // Falsifiability: prove the FORBIDDEN list and the parsing are real by
    // showing the *same* query finds rdkafka/aws-sdk-sqs when they genuinely
    // are in a graph. Without this, a typo'd crate name or a broken parse
    // would make `core_engine_has_no_broker_dependencies` pass vacuously
    // forever.
    let tree = cargo_tree(&[
        "tree",
        "-p",
        "autumn-harvest-plugin",
        "--features",
        "connectors,kafka,sqs",
        "--edges",
        "normal,build",
        "--prefix",
        "none",
        "--format",
        "{lib}",
    ]);

    let names = crate_names(&tree);
    for expected in ["rdkafka", "aws_sdk_sqs"] {
        assert!(
            names.contains(&expected),
            "expected `{expected}` in the plugin's kafka+sqs graph; if this \
             fails the negative guard above is not proving anything"
        );
        assert!(
            FORBIDDEN.contains(&expected),
            "`{expected}` must be in FORBIDDEN or the core guard cannot catch it"
        );
    }
}
