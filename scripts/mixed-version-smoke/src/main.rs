//! Mixed-version smoke worker (issue #1828).
//!
//! `scripts/run-mixed-version-smoke.sh` builds this binary twice: once from
//! the current tree and once from the previous release. Both binaries then
//! share one database. The contract that they prove is in
//! `docs/upgrading/README.md`.
//!
//! The `mv_smoke` workflow runs one activity, waits on a durable timer, and
//! runs a second activity. Each activity returns the label of the binary that
//! ran it. The script stops one version during the timer and starts the
//! other. The output then shows which version ran each step.
//!
//! With `MV_SMOKE_CODEC=1`, the worker installs a test payload codec. A codec
//! envelope that one version writes must then decode in the other.

mod compat;

use std::time::{Duration, Instant};

use autumn_harvest::diesel_async::pooled_connection::AsyncDieselConnectionManager;
use autumn_harvest::diesel_async::pooled_connection::deadpool::Pool;
use autumn_harvest::diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use autumn_harvest::payload_codec::{CodecError, PayloadCodec};
use autumn_harvest::prelude::*;
use autumn_harvest::start_or_load_workflow_execution;
use autumn_harvest_plugin::prelude::*;
use base64::Engine as _;
use diesel::sql_types::{Nullable, Text};
use serde_json::{Value, json};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The queue for the workflow and its activities.
const QUEUE: &str = "mixed-version-smoke";
/// The time between the two activities. The script stops a worker in it.
const GAP_SECS: u64 = 10;
/// The codec id. A change here is a change to the stored format.
const CODEC_ID: &str = "mv-smoke-xor";
/// The XOR mask of the test codec.
const CODEC_MASK: u8 = 0x5a;
/// The envelope key of a payload codec, as stored in the database.
const ENVELOPE_KEY: &str = "_harvest_codec_envelope";

#[workflow]
async fn mv_smoke(ctx: &WorkflowContext, input: Value) -> HarvestResult<Value> {
    let first = ctx
        .execute_activity_raw("mv_stamp", json!({ "step": 1 }), QUEUE)
        .await?;
    ctx.timer("gap", GAP_SECS).await?;
    let second = ctx
        .execute_activity_raw("mv_stamp", json!({ "step": 2 }), QUEUE)
        .await?;
    Ok(json!({
        "n": input["n"],
        "first": first["label"],
        "second": second["label"],
    }))
}

/// Return the label of the binary that runs this activity.
#[activity(start_to_close = "30s")]
async fn mv_stamp(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    let label = std::env::var("MV_SMOKE_LABEL").unwrap_or_else(|_| "unlabelled".to_owned());
    Ok(json!({ "label": label, "step": input["step"] }))
}

/// A reversible byte mask. It tests the envelope format, not secrecy.
struct XorCodec;

impl PayloadCodec for XorCodec {
    fn codec_id(&self) -> &'static str {
        CODEC_ID
    }

    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.iter().map(|byte| byte ^ CODEC_MASK).collect())
    }

    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        self.encode(encoded)
    }
}

/// Register the workload. `MV_SMOKE_CODEC=1` also installs the test codec.
fn builder() -> HarvestBuilder {
    let builder = HarvestBuilder::default()
        .workflows(workflows![mv_smoke])
        .activities(activities![mv_stamp])
        .worker(WorkerConfig::default().with_queues([QUEUE]));
    if std::env::var("MV_SMOKE_CODEC").as_deref() == Ok("1") {
        tracing::info!(codec = CODEC_ID, "payload codec installed");
        builder.payload_codec(XorCodec)
    } else {
        builder
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let url = std::env::var("DATABASE_URL").map_err(|_| "set DATABASE_URL")?;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["worker"] => worker(url).await,
        ["start", prefix, count] => start(&url, prefix, count.parse()?).await,
        ["wait-first", prefix, count, secs] => {
            wait_first(&url, prefix, count.parse()?, secs.parse()?).await
        }
        ["wait-done", prefix, count, secs] => {
            wait_done(&url, prefix, count.parse()?, secs.parse()?).await
        }
        ["check", prefix, count, first, second] => {
            check(&url, prefix, count.parse()?, first, second).await
        }
        _ => Err(USAGE.into()),
    }
}

const USAGE: &str = "usage: mixed-version-smoke <command>
  worker                                    run a worker until SIGINT or SIGTERM
  start      PREFIX COUNT                   start COUNT runs, ids PREFIX-0..
  wait-first PREFIX COUNT SECS              wait until each run ends its first step
  wait-done  PREFIX COUNT SECS              wait until each run completes
  check      PREFIX COUNT FIRST SECOND      check the labels of each step; `*` is any";

/// Run a worker and a scheduler until SIGINT or SIGTERM.
async fn worker(url: String) -> Result<(), BoxError> {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&url);
    let pool = Pool::builder(manager).max_size(8).build()?;
    let config = HarvestRuntimeConfig {
        mode: HarvestMode::External,
        worker_enabled: true,
        scheduler_enabled: true,
        database: HarvestDatabaseConfig { url: Some(url) },
        outbox: HarvestOutboxConfig {
            enabled: false,
            ..HarvestOutboxConfig::default()
        },
        ..HarvestRuntimeConfig::default()
    };
    let runner = HarvestRunner::start(
        builder().try_build()?,
        &config,
        HarvestRunnerResources::new(pool),
    )
    .await
    .map_err(|error| format!("the runner did not start: {error}"))?;
    tracing::info!("mixed-version-smoke worker ready");
    stop_signal().await?;
    runner.stop().await;
    tracing::info!("mixed-version-smoke worker stopped");
    Ok(())
}

async fn stop_signal() -> Result<(), BoxError> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = term.recv() => {}
    }
    Ok(())
}

/// Start `count` runs with the start path of this version.
async fn start(url: &str, prefix: &str, count: u32) -> Result<(), BoxError> {
    let mut conn = AsyncPgConnection::establish(url).await?;
    for n in 0..count {
        let id = format!("{prefix}-{n}");
        let input = json!({ "n": n });
        let params = compat::start_params("mv_smoke", &id, input, QUEUE);
        start_or_load_workflow_execution(&mut conn, params, None).await?;
    }
    println!("started {count} runs with prefix {prefix}");
    Ok(())
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct Run {
    #[diesel(sql_type = Text)]
    workflow_id: String,
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Nullable<Text>)]
    output: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    error: Option<String>,
}

/// The pattern that matches the ids of one prefix.
fn like(prefix: &str) -> String {
    format!("{prefix}-%")
}

/// Wait until each run has a completed activity, so it waits on its timer.
async fn wait_first(url: &str, prefix: &str, count: usize, secs: u64) -> Result<(), BoxError> {
    let query = "SELECT count(DISTINCT w.id) AS n
        FROM harvest_workflow_executions w
        JOIN harvest_events e ON e.workflow_exec_id = w.id
        WHERE w.workflow_id LIKE $1 AND e.event_data->>'type' = 'ActivityCompleted'";
    poll(url, secs, "first step", count, async |conn| {
        let row: Count = diesel::sql_query(query)
            .bind::<Text, _>(like(prefix))
            .get_result(conn)
            .await?;
        Ok(usize::try_from(row.n)?)
    })
    .await
}

/// Wait until each run completes. Fail at once on a failed run.
async fn wait_done(url: &str, prefix: &str, count: usize, secs: u64) -> Result<(), BoxError> {
    poll(url, secs, "completion", count, async |conn| {
        let runs = runs(conn, prefix).await?;
        if let Some(run) = runs
            .iter()
            .find(|run| !matches!(run.state.as_str(), "RUNNING" | "PENDING" | "COMPLETED"))
        {
            return Err(format!(
                "{} is {}: {}",
                run.workflow_id,
                run.state,
                run.error.as_deref().unwrap_or("no error text")
            )
            .into());
        }
        Ok(runs.iter().filter(|run| run.state == "COMPLETED").count())
    })
    .await
}

async fn runs(conn: &mut AsyncPgConnection, prefix: &str) -> Result<Vec<Run>, BoxError> {
    let query = "SELECT workflow_id, state, output::text AS output, error
        FROM harvest_workflow_executions
        WHERE workflow_id LIKE $1 ORDER BY workflow_id";
    Ok(diesel::sql_query(query)
        .bind::<Text, _>(like(prefix))
        .load(conn)
        .await?)
}

/// Poll `probe` once a second until it returns `want` or `secs` pass.
async fn poll<F>(
    url: &str,
    secs: u64,
    what: &str,
    want: usize,
    mut probe: F,
) -> Result<(), BoxError>
where
    F: AsyncFnMut(&mut AsyncPgConnection) -> Result<usize, BoxError>,
{
    let mut conn = AsyncPgConnection::establish(url).await?;
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let got = probe(&mut conn).await?;
        if got >= want {
            println!("{what}: {got}/{want}");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("{what}: {got}/{want} after {secs}s").into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Check that each run completed and that the expected versions ran it.
async fn check(
    url: &str,
    prefix: &str,
    count: usize,
    first: &str,
    second: &str,
) -> Result<(), BoxError> {
    let mut conn = AsyncPgConnection::establish(url).await?;
    let runs = runs(&mut conn, prefix).await?;
    if runs.len() != count {
        return Err(format!("{prefix}: {} runs, want {count}", runs.len()).into());
    }
    let mut pairs = std::collections::BTreeMap::<(String, String), usize>::new();
    for run in &runs {
        if run.state != "COMPLETED" {
            return Err(format!("{} is {}, want COMPLETED", run.workflow_id, run.state).into());
        }
        let stored = run
            .output
            .as_deref()
            .ok_or("a completed run has no output")?;
        let output = decode(serde_json::from_str(stored)?)?;
        let got = (label(&output, "first")?, label(&output, "second")?);
        for (want, have, step) in [(first, &got.0, "first"), (second, &got.1, "second")] {
            if want != "*" && want != have {
                return Err(format!(
                    "{}: {step} step ran on {have}, want {want}",
                    run.workflow_id
                )
                .into());
            }
        }
        *pairs.entry(got).or_default() += 1;
    }
    for ((a, b), n) in pairs {
        println!("{prefix}: {n} runs: first step on {a}, second step on {b}");
    }
    Ok(())
}

fn label(output: &Value, step: &str) -> Result<String, BoxError> {
    output[step]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("no {step} label in {output}").into())
}

/// Decode a stored payload. A plain value passes through.
///
/// This reads the envelope by hand, not through the engine. A change to the
/// stored envelope shape then fails here, as it fails for an older reader.
fn decode(stored: Value) -> Result<Value, BoxError> {
    let Some(envelope) = stored.get(ENVELOPE_KEY) else {
        return Ok(stored);
    };
    if envelope.as_i64() != Some(1) || stored["codec_id"] != CODEC_ID {
        return Err(format!("unexpected codec envelope: {stored}").into());
    }
    let data = stored["data"].as_str().ok_or("the envelope has no data")?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(data)?;
    Ok(serde_json::from_slice(&XorCodec.decode(&bytes)?)?)
}
