//! Prints the stage 2 gate measurements (issue #2257).
//!
//! ```sh
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/history_load_gate \
//!   cargo run -p history-load-gate --release
//! ```
//!
//! The binary applies the Harvest migrations, seeds the histories, times
//! the loads and prints Markdown tables. It deletes each seeded run, also
//! after a failed step. With `SIZES` and `RUNS` empty it opens no database
//! and only applies the gate. `README.md` lists the variables.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use autumn_harvest::migrate;
use autumn_harvest::types::ExecutionId;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use history_load_gate::cache_model::{self, Pattern};
use history_load_gate::disk_side::DiskSide;
use history_load_gate::gate::{self, Observed, Thresholds, Trip, Verdict};
use history_load_gate::sqlite_side::{SqliteReader, SqliteStore};
use history_load_gate::stats::{self, Summary};
use history_load_gate::{BoxError, pg_side, seed};

/// The queue of each seeded run. No worker polls it.
const QUEUE: &str = "history-load-gate";

/// The default entry cap of `WorkflowCache`.
const CACHE_ENTRIES: usize = 1_000;

/// The settings that the environment gives.
struct Config {
    url: Option<String>,
    sizes: Vec<usize>,
    reps: usize,
    payload_bytes: usize,
    runs: Vec<usize>,
    run_events: usize,
    concurrency: usize,
    model_runs: Vec<usize>,
    fleet: Observed,
}

impl Config {
    fn from_env() -> Result<Self, BoxError> {
        let mut sizes = env_list("SIZES", &[10_000, 50_000])?;
        sizes.sort_unstable();
        let config = Self {
            url: std::env::var("DATABASE_URL").ok(),
            sizes,
            reps: env_opt("REPS")?.unwrap_or(100),
            payload_bytes: env_opt("PAYLOAD_BYTES")?.unwrap_or(256),
            runs: env_list("RUNS", &[1_000, 10_000, 100_000])?,
            run_events: env_opt("RUN_EVENTS")?.unwrap_or(21),
            concurrency: env_opt("CONCURRENCY")?.unwrap_or(8),
            model_runs: env_list("MODEL_RUNS", &[500, 1_000, 1_250, 2_000, 10_000, 100_000])?,
            fleet: Observed::from_fleet(
                env_opt("FLEET_COLD_SHARE")?,
                env_opt("FLEET_COLD_P99_MS")?,
            )?,
        };
        if config.sizes.contains(&0) || config.run_events == 0 {
            return Err("a history needs at least one event".into());
        }
        if config.reps == 0 || config.concurrency == 0 {
            return Err("REPS and CONCURRENCY must be at least 1".into());
        }
        if config.url.is_none() && !(config.sizes.is_empty() && config.runs.is_empty()) {
            return Err("set DATABASE_URL, or set SIZES and RUNS empty".into());
        }
        Ok(config)
    }
}

/// Reads an optional value from the environment.
fn env_opt<T: std::str::FromStr>(name: &str) -> Result<Option<T>, BoxError>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(text) => Ok(Some(text.trim().parse()?)),
        Err(_) => Ok(None),
    }
}

/// Reads a comma-separated list of counts from the environment. An empty
/// value gives an empty list, which skips that section.
fn env_list(name: &str, default: &[usize]) -> Result<Vec<usize>, BoxError> {
    match std::env::var(name) {
        Ok(text) => Ok(text
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::parse)
            .collect::<Result<_, _>>()?),
        Err(_) => Ok(default.to_vec()),
    }
}

/// The load times of one history size.
struct SizeResult {
    events: usize,
    stored_bytes: usize,
    pg_read: Summary,
    pg_cold_path: Summary,
    sqlite_read: Summary,
}

/// The warm-up times of one live-run count.
struct WarmupResult {
    runs: usize,
    pg_serial: Duration,
    pg_parallel: Duration,
    sqlite_serial: Duration,
    sqlite_parallel: Duration,
    /// The first pass after a restart with the disk cache: a Postgres load
    /// and a disk write per run.
    disk_fill: Duration,
    /// The next pass with the disk cache: a disk read and an anchor check
    /// per run.
    disk_pass: Duration,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let config = Config::from_env()?;
    let mut sizes = Vec::new();
    let mut warmups = Vec::new();
    // The gate alone needs no database, so a fleet URL never gets migrated.
    if let Some(url) = &config.url
        && !(config.sizes.is_empty() && config.runs.is_empty())
    {
        let mut conn = AsyncPgConnection::establish(url).await?;
        migrate::apply_to_connection(&mut conn, &migrate::embedded()).await?;
        let dir = tempfile::tempdir()?;
        let mut sqlite = SqliteStore::open(&dir.path().join("history-cache.db"))?;
        for &events in &config.sizes {
            sizes.push(measure_size(&mut conn, &mut sqlite, &config, events).await?);
        }
        for &runs in &config.runs {
            warmups.push(measure_warmup(&mut conn, &mut sqlite, &config, url, runs).await?);
        }
    }

    print_sizes(&sizes, &config);
    print_warmups(&warmups, &config);
    print_cache_model(&config);
    print_verdict(&sizes, &config);
    Ok(())
}

/// Times `config.reps` loads of one history of `events` events.
#[allow(
    clippy::future_not_send,
    reason = "`main` awaits it on its own task, and the store is not `Sync`"
)]
async fn measure_size(
    conn: &mut AsyncPgConnection,
    sqlite: &mut SqliteStore,
    config: &Config,
    events: usize,
) -> Result<SizeResult, BoxError> {
    let history = seed::history(events, config.payload_bytes);
    let stored_bytes = history
        .iter()
        .map(|event| serde_json::to_string(event).map(|text| text.len()))
        .sum::<Result<usize, _>>()?;
    let pg_run = pg_side::seed(conn, &history, QUEUE).await?;
    let lite_run = match sqlite.seed(&history) {
        Ok(run) => run,
        Err(error) => {
            pg_side::delete_runs(conn, &[pg_run]).await?;
            return Err(error);
        }
    };
    let timed = time_size(conn, sqlite, config, events, pg_run, lite_run).await;
    // Delete the runs on success and on failure alike.
    pg_side::delete_runs(conn, &[pg_run]).await?;
    sqlite.delete_runs(&[lite_run])?;
    let (pg_read, pg_cold_path, sqlite_read) = timed?;
    Ok(SizeResult {
        events,
        stored_bytes,
        pg_read: stats::summarize(&pg_read).ok_or("no samples")?,
        pg_cold_path: stats::summarize(&pg_cold_path).ok_or("no samples")?,
        sqlite_read: stats::summarize(&sqlite_read).ok_or("no samples")?,
    })
}

/// The load samples of one size: one Postgres read, the two-read cold path
/// and one `SQLite` read.
type SizeSamples = (Vec<Duration>, Vec<Duration>, Vec<Duration>);

/// Times the loads of the seeded runs `pg_run` and `lite_run`.
#[allow(
    clippy::future_not_send,
    reason = "`main` awaits it on its own task, and the store is not `Sync`"
)]
async fn time_size(
    conn: &mut AsyncPgConnection,
    sqlite: &SqliteStore,
    config: &Config,
    events: usize,
    pg_run: ExecutionId,
    lite_run: ExecutionId,
) -> Result<SizeSamples, BoxError> {
    // One untimed load of each warms both caches.
    check_len(pg_side::load(conn, pg_run).await?.events.len(), events)?;
    check_len(sqlite.load(lite_run)?.len(), events)?;

    let mut pg_read = Vec::with_capacity(config.reps);
    let mut pg_cold_path = Vec::with_capacity(config.reps);
    let mut sqlite_read = Vec::with_capacity(config.reps);
    for _ in 0..config.reps {
        let started = Instant::now();
        let loaded = pg_side::load(conn, pg_run).await?;
        pg_read.push(started.elapsed());
        check_len(loaded.events.len(), events)?;
        drop(loaded);

        // The worker keeps the first history while it reads the second. Its
        // second read also sums the stored bytes, which this one skips.
        let started = Instant::now();
        let first = pg_side::load(conn, pg_run).await?;
        let second = pg_side::load(conn, pg_run).await?;
        pg_cold_path.push(started.elapsed());
        check_len(first.events.len() + second.events.len(), 2 * events)?;
        drop((first, second));

        let started = Instant::now();
        let loaded = sqlite.load(lite_run)?;
        sqlite_read.push(started.elapsed());
        check_len(loaded.len(), events)?;
    }
    Ok((pg_read, pg_cold_path, sqlite_read))
}

/// Times the cold load of `runs` live runs, as after an owner restart.
async fn measure_warmup(
    conn: &mut AsyncPgConnection,
    sqlite: &mut SqliteStore,
    config: &Config,
    url: &str,
    runs: usize,
) -> Result<WarmupResult, BoxError> {
    let history = seed::history(config.run_events, config.payload_bytes);
    let pg_template = pg_side::seed(conn, &history, QUEUE).await?;
    let mut pg_all = vec![pg_template];
    let mut lite_all = Vec::new();
    let timed = time_warmup(
        conn,
        sqlite,
        config,
        url,
        runs,
        &history,
        (&mut pg_all, &mut lite_all),
    )
    .await;
    // Delete the runs on success and on failure alike.
    pg_side::delete_runs(conn, &pg_all).await?;
    sqlite.delete_runs(&lite_all)?;
    timed
}

/// Seeds `runs` copies on both stores and times one pass over them. Each
/// seeded id goes into `seeded`, so the caller can delete it.
async fn time_warmup(
    conn: &mut AsyncPgConnection,
    sqlite: &mut SqliteStore,
    config: &Config,
    url: &str,
    runs: usize,
    history: &[autumn_harvest::WorkflowEvent],
    seeded: (&mut Vec<ExecutionId>, &mut Vec<ExecutionId>),
) -> Result<WarmupResult, BoxError> {
    let (pg_all, lite_all) = seeded;
    let pg_runs = Arc::new(pg_side::clone_runs(conn, pg_all[0], runs).await?);
    pg_all.extend(pg_runs.iter().copied());
    let lite_template = sqlite.seed(history)?;
    lite_all.push(lite_template);
    let lite_runs = Arc::new(sqlite.clone_runs(lite_template, runs)?);
    lite_all.extend(lite_runs.iter().copied());

    // A first pass warms the buffers of both stores. A restarted owner
    // process loses its own memory, not the page cache of the database.
    load_pg_serial(conn, &pg_runs, config.run_events).await?;
    load_sqlite_serial(sqlite, &lite_runs, config.run_events)?;

    let pg_serial = load_pg_serial(conn, &pg_runs, config.run_events).await?;
    let pg_parallel = load_pg_parallel(config, url, &pg_runs).await?;
    let sqlite_serial = load_sqlite_serial(sqlite, &lite_runs, config.run_events)?;
    let sqlite_parallel = load_sqlite_parallel(config, sqlite, &lite_runs)?;
    // Issue #2258: a fresh disk cache, as after a restart.
    let disk = Arc::new(DiskSide::open()?);
    let disk_fill = disk_pass(config, url, &pg_runs, &disk, DiskStep::Fill).await?;
    let disk_pass = disk_pass(config, url, &pg_runs, &disk, DiskStep::Read).await?;
    Ok(WarmupResult {
        runs,
        pg_serial,
        pg_parallel,
        sqlite_serial,
        sqlite_parallel,
        disk_fill,
        disk_pass,
    })
}

/// One pass of the disk cache warm-up.
#[derive(Clone, Copy)]
enum DiskStep {
    /// Load each run from Postgres and write it to the disk cache.
    Fill,
    /// Read each run from the disk cache and check its last event.
    Read,
}

/// Runs one disk cache pass over `ids` on `config.concurrency` connections.
/// Returns the wall time.
async fn disk_pass(
    config: &Config,
    url: &str,
    ids: &Arc<Vec<ExecutionId>>,
    disk: &Arc<DiskSide>,
    step: DiskStep,
) -> Result<Duration, BoxError> {
    let mut conns = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        conns.push(AsyncPgConnection::establish(url).await?);
    }
    let next = Arc::new(AtomicUsize::new(0));
    let events = config.run_events;
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(conns.len());
    for mut conn in conns {
        let ids = Arc::clone(ids);
        let next = Arc::clone(&next);
        let disk = Arc::clone(disk);
        tasks.push(tokio::spawn(async move {
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(&id) = ids.get(index) else {
                    return Ok::<(), BoxError>(());
                };
                match step {
                    DiskStep::Fill => {
                        let history = pg_side::load(&mut conn, id).await?.events;
                        check_len(history.len(), events)?;
                        let disk = Arc::clone(&disk);
                        tokio::task::spawn_blocking(move || disk.store(index, id, history))
                            .await??;
                    }
                    DiskStep::Read => check_len(disk.read(&mut conn, index, id).await?, events)?,
                }
            }
        }));
    }
    for task in tasks {
        task.await??;
    }
    Ok(started.elapsed())
}

/// Loads each run in `ids` on one connection. Returns the wall time.
async fn load_pg_serial(
    conn: &mut AsyncPgConnection,
    ids: &[ExecutionId],
    events: usize,
) -> Result<Duration, BoxError> {
    let started = Instant::now();
    for id in ids {
        check_len(pg_side::load(conn, *id).await?.events.len(), events)?;
    }
    Ok(started.elapsed())
}

/// Loads each run in `ids` on `config.concurrency` connections. Returns the
/// wall time.
async fn load_pg_parallel(
    config: &Config,
    url: &str,
    ids: &Arc<Vec<ExecutionId>>,
) -> Result<Duration, BoxError> {
    let mut conns = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        conns.push(AsyncPgConnection::establish(url).await?);
    }
    let next = Arc::new(AtomicUsize::new(0));
    let events = config.run_events;
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(conns.len());
    for mut conn in conns {
        let ids = Arc::clone(ids);
        let next = Arc::clone(&next);
        tasks.push(tokio::spawn(async move {
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(id) = ids.get(index) else {
                    return Ok::<(), BoxError>(());
                };
                check_len(pg_side::load(&mut conn, *id).await?.events.len(), events)?;
            }
        }));
    }
    for task in tasks {
        task.await??;
    }
    Ok(started.elapsed())
}

/// Loads each run in `ids` from `SQLite` on one connection. Returns the
/// wall time.
fn load_sqlite_serial(
    sqlite: &SqliteStore,
    ids: &[ExecutionId],
    events: usize,
) -> Result<Duration, BoxError> {
    let started = Instant::now();
    for id in ids {
        check_len(sqlite.load(*id)?.len(), events)?;
    }
    Ok(started.elapsed())
}

/// Loads each run in `ids` from `SQLite` on `config.concurrency` threads,
/// each with its own connection. Returns the wall time.
fn load_sqlite_parallel(
    config: &Config,
    sqlite: &SqliteStore,
    ids: &[ExecutionId],
) -> Result<Duration, BoxError> {
    let readers: Vec<SqliteReader> = (0..config.concurrency)
        .map(|_| sqlite.reader())
        .collect::<Result<_, _>>()?;
    let next = AtomicUsize::new(0);
    let events = config.run_events;
    let started = Instant::now();
    std::thread::scope(|scope| {
        let workers: Vec<_> = readers
            .into_iter()
            .map(|reader| {
                let next = &next;
                scope.spawn(move || -> Result<(), BoxError> {
                    while let Some(id) = ids.get(next.fetch_add(1, Ordering::Relaxed)) {
                        check_len(reader.load(*id)?.len(), events)?;
                    }
                    Ok(())
                })
            })
            .collect();
        for worker in workers {
            worker.join().map_err(|_| "a reader thread panicked")??;
        }
        Ok::<(), BoxError>(())
    })?;
    Ok(started.elapsed())
}

/// Fails when a load returned the wrong number of events.
fn check_len(got: usize, want: usize) -> Result<(), BoxError> {
    if got == want {
        Ok(())
    } else {
        Err(format!("a load returned {got} events, expected {want}").into())
    }
}

/// `duration` in milliseconds, with two decimals.
fn ms(duration: Duration) -> String {
    format!("{:.2}", duration.as_secs_f64() * 1_000.0)
}

/// `numerator / denominator`, with one decimal.
fn ratio(numerator: Duration, denominator: Duration) -> String {
    if denominator.is_zero() {
        return "n/a".to_owned();
    }
    format!(
        "{:.1}×",
        numerator.as_secs_f64() / denominator.as_secs_f64()
    )
}

fn print_sizes(sizes: &[SizeResult], config: &Config) {
    if sizes.is_empty() {
        return;
    }
    println!("## History load, one run\n");
    println!(
        "{} reps per size. Payload text {} bytes per event. Milliseconds, p50 / p99, \
         nearest rank.\n",
        config.reps, config.payload_bytes
    );
    println!(
        "| Events | JSON bytes | Postgres, 1 read | Postgres, cold path (2 reads) | SQLite, 1 read | Postgres / SQLite, p50 |"
    );
    println!("|---:|---:|---:|---:|---:|---:|");
    for size in sizes {
        println!(
            "| {} | {} | {} / {} | {} / {} | {} / {} | {} |",
            size.events,
            size.stored_bytes,
            ms(size.pg_read.p50),
            ms(size.pg_read.p99),
            ms(size.pg_cold_path.p50),
            ms(size.pg_cold_path.p99),
            ms(size.sqlite_read.p50),
            ms(size.sqlite_read.p99),
            ratio(size.pg_read.p50, size.sqlite_read.p50),
        );
    }
    println!();
}

fn print_warmups(warmups: &[WarmupResult], config: &Config) {
    if warmups.is_empty() {
        return;
    }
    let conns = config.concurrency;
    println!("## Owner-restart warm-up\n");
    println!(
        "Each live run holds {} events. The time is the wall time to load every run once, \
         on 1 connection and on {conns}.\n",
        config.run_events
    );
    println!(
        "| Live runs | Postgres, 1 conn | Postgres, {conns} conns | SQLite, 1 conn | SQLite, {conns} conns | Runs past the {CACHE_ENTRIES}-entry cache |"
    );
    println!("|---:|---:|---:|---:|---:|---:|");
    for warmup in warmups {
        println!(
            "| {} | {} ms | {} ms | {} ms | {} ms | {} |",
            warmup.runs,
            ms(warmup.pg_serial),
            ms(warmup.pg_parallel),
            ms(warmup.sqlite_serial),
            ms(warmup.sqlite_parallel),
            warmup.runs.saturating_sub(CACHE_ENTRIES),
        );
    }
    println!();
    println!("## Owner-restart warm-up with the disk cache (issue #2258)\n");
    println!(
        "{conns} connections and {conns} workers. The fill is the first decision of each run \
         after a restart: a Postgres load and a sealed disk write. The next pass is the next \
         decision of each run past the {CACHE_ENTRIES}-entry memory cache: a disk read and an \
         anchor check, against a Postgres load without the cache.\n"
    );
    println!(
        "| Live runs | Fill, disk cache | Next pass, Postgres | Next pass, disk cache | Postgres / disk |"
    );
    println!("|---:|---:|---:|---:|---:|");
    for warmup in warmups {
        println!(
            "| {} | {} ms | {} ms | {} ms | {} |",
            warmup.runs,
            ms(warmup.disk_fill),
            ms(warmup.pg_parallel),
            ms(warmup.disk_pass),
            ratio(warmup.pg_parallel, warmup.disk_pass),
        );
    }
    println!();
}

/// Decisions per row of the cache model.
const MODEL_DECISIONS: usize = 200_000;

fn print_cache_model(config: &Config) {
    if config.model_runs.is_empty() {
        return;
    }
    println!("## Cold share from the cache cap alone\n");
    println!(
        "The real `WorkflowCache` with {CACHE_ENTRIES} entries. {MODEL_DECISIONS} decisions \
         after one warm-up pass. No restart and no change of owner.\n"
    );
    println!("| Live runs per owner | Uniform | Round robin |");
    println!("|---:|---:|---:|");
    for &runs in &config.model_runs {
        let uniform =
            cache_model::cold_share(runs, CACHE_ENTRIES, MODEL_DECISIONS, Pattern::Uniform, 7);
        let round_robin =
            cache_model::cold_share(runs, CACHE_ENTRIES, MODEL_DECISIONS, Pattern::RoundRobin, 7);
        println!(
            "| {runs} | {:.1}% | {:.1}% |",
            uniform * 100.0,
            round_robin * 100.0
        );
    }
    println!();
}

fn print_verdict(sizes: &[SizeResult], config: &Config) {
    let thresholds = Thresholds::default();
    println!("## Gate\n");
    println!(
        "Thresholds: cold share above {:.0}% for a week, or cold p99 above {} ms. \
         Either one trips the gate.\n",
        thresholds.cold_share * 100.0,
        ms(thresholds.cold_p99)
    );
    // The bench times one history size at a time, so it shows which sizes
    // pass the p99 threshold. Only a fleet week gives the p99 itself. The
    // sizes are sorted, so the first match is the smallest.
    match sizes
        .iter()
        .find(|size| size.pg_cold_path.p99 > thresholds.cold_p99)
    {
        Some(size) => println!(
            "- Bench: the smallest measured size whose cold path passes the threshold at p99 \
             is {} events, at {} ms.",
            size.events,
            ms(size.pg_cold_path.p99)
        ),
        None if sizes.is_empty() => println!("- Bench: not run."),
        None => println!("- Bench: no measured size passes the p99 threshold."),
    }
    let shown = |value: Option<String>| value.unwrap_or_else(|| "not given".to_owned());
    println!(
        "- Fleet: cold share {}, cold p99 {}.",
        shown(
            config
                .fleet
                .cold_share
                .map(|s| format!("{:.1}%", s * 100.0))
        ),
        shown(config.fleet.cold_p99.map(|p| format!("{} ms", ms(p)))),
    );
    match gate::verdict(&thresholds, &config.fleet) {
        Verdict::Go(trips) => {
            for trip in trips {
                match trip {
                    Trip::ColdShare(share) => {
                        println!("- Trips: cold share {:.1}%.", share * 100.0);
                    }
                    Trip::ColdP99(p99) => println!("- Trips: cold p99 {} ms.", ms(p99)),
                }
            }
            println!("- Verdict: **go**.");
        }
        Verdict::NoGo => println!("- Verdict: **no-go**."),
        Verdict::Undecided => {
            println!("- Verdict: **undecided** until a fleet week gives both values.");
        }
    }
}
