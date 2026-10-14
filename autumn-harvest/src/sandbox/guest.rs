//! Both ends of one guest exchange.
//!
//! The guest agent calls [`serve_connection`] for each accepted connection.
//! The host backend calls [`ping`] and [`request_run`]. The code is generic
//! over the stream, so tests use an in-memory pipe and the microVM uses
//! vsock.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

use super::capture::TRUNCATION_MARKER;
use super::error::SandboxError;
use super::outcome::{MAX_SKIP_REASON_BYTES, SandboxOutcome, shown};
use super::protocol::{
    FrameError, GuestErrorKind, GuestErrorMessage, GuestMessage, HostMessage,
    MAX_CONTROL_FRAME_BYTES, MAX_FRAME_BYTES, OutcomeMessage, PROTOCOL_VERSION, ReadyMessage,
    RunMessage, StartMessage, read_message, write_message,
};
use super::request::ExecPlan;
use super::runtime::{BoxFuture, SandboxJob};

/// Something that runs a plan inside the guest.
pub trait PlanExecutor: Send + Sync {
    /// Run `plan` to an outcome.
    fn execute<'a>(
        &'a self,
        plan: &'a ExecPlan,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<SandboxOutcome, SandboxError>>;
}

/// The kernel duties of the guest agent before each run.
pub trait GuestKernel: Send + Sync {
    /// Mix 32 bytes of host entropy into the random pool, and force a
    /// reseed.
    ///
    /// # Errors
    ///
    /// Returns the error of the kernel call. The run stops.
    fn reseed(&self, entropy: &[u8; 32]) -> std::io::Result<()>;

    /// Set the wall clock to the host time, in milliseconds since the Unix
    /// epoch. A microVM restored from a snapshot starts with the clock of
    /// the snapshot.
    ///
    /// # Errors
    ///
    /// Returns the error of the kernel call. The run stops.
    fn set_clock(&self, unix_time_ms: u64) -> std::io::Result<()>;
}

/// A kernel that the agent does not manage, such as the host kernel in
/// development mode. Both duties do nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnmanagedKernel;

impl GuestKernel for UnmanagedKernel {
    fn reseed(&self, _entropy: &[u8; 32]) -> std::io::Result<()> {
        Ok(())
    }

    fn set_clock(&self, _unix_time_ms: u64) -> std::io::Result<()> {
        Ok(())
    }
}

/// What one connection carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Served {
    /// A ping. The guest keeps serving.
    Ping,
    /// A run. A single-use guest stops after it.
    Run,
}

/// The largest outcome frame that `plan` can produce.
///
/// Each kept stream byte can decode to three bytes of U+FFFD and escape to
/// six bytes of JSON. Base64 makes three bytes of a file into four. The
/// envelope and the other fields fit in 64 KiB.
#[must_use]
pub fn max_outcome_frame_bytes(plan: &ExecPlan) -> usize {
    let kept = usize::try_from(plan.limits.max_output_bytes).unwrap_or(usize::MAX);
    let stream = kept
        .saturating_mul(3)
        .saturating_add(TRUNCATION_MARKER.len())
        .saturating_mul(6);
    let files = usize::try_from(plan.max_output_files_total_bytes)
        .unwrap_or(usize::MAX)
        .div_ceil(3)
        .saturating_mul(4);
    // Each requested output appears once, with its escaped path, an
    // escaped skip reason and its JSON structure.
    let outputs = plan
        .outputs
        .iter()
        .map(|path| {
            path.len()
                .saturating_add(MAX_SKIP_REASON_BYTES)
                .saturating_mul(6)
                .saturating_add(OUTPUT_ITEM_BYTES)
        })
        .fold(0usize, usize::saturating_add);
    stream
        .saturating_mul(2)
        .saturating_add(files)
        .saturating_add(outputs)
        .saturating_add(FRAME_OVERHEAD_BYTES)
        .min(MAX_FRAME_BYTES)
}

/// Room for the JSON structure of one output entry.
const OUTPUT_ITEM_BYTES: usize = 64;

/// Map a failed write of a run frame to a sandbox error.
///
/// A run over the frame cap fails the same way on each attempt, so it is a
/// denial. Any other write failure is a fault of the connection.
fn run_write_error(err: &FrameError) -> SandboxError {
    match err {
        FrameError::TooLarge { declared, max } => SandboxError::Denied(format!(
            "the run takes {declared} bytes, over the {max}-byte guest frame; send smaller input"
        )),
        other => SandboxError::Unavailable(format!("could not send the run: {other}")),
    }
}

/// Room for the envelope, the run id, the paths and the other fields.
const FRAME_OVERHEAD_BYTES: usize = 64 * 1024;

/// Serve one connection: answer a ping, or run one plan.
///
/// A request that the guest cannot read gets an error frame with an empty
/// run id. So the host sees a protocol mismatch, not a lost connection. A
/// run counts as served even when its reply fails, so a single-use guest
/// never runs a second plan.
///
/// # Errors
///
/// Returns the [`FrameError`] of a request that cannot be read, or of a
/// ping reply that cannot be written. A failed run is not an error here:
/// the guest reports it to the host in a [`GuestMessage::Error`].
pub async fn serve_connection<S>(
    stream: &mut S,
    executor: &dyn PlanExecutor,
    kernel: &dyn GuestKernel,
) -> Result<Served, FrameError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let message: HostMessage = match read_message(stream, MAX_FRAME_BYTES).await {
        Ok(message) => message,
        Err(FrameError::Malformed(reason)) => {
            let reply = GuestMessage::Error(GuestErrorMessage {
                run_id: String::new(),
                kind: GuestErrorKind::InvalidPlan,
                message: format!(
                    "the guest cannot read the request; it speaks protocol version \
                     {PROTOCOL_VERSION}: {reason}"
                ),
            });
            let _ = write_message(stream, reply, MAX_CONTROL_FRAME_BYTES).await;
            return Err(FrameError::Malformed(reason));
        }
        Err(e) => return Err(e),
    };
    let run = match message {
        HostMessage::Ping => {
            write_message(stream, GuestMessage::Pong, MAX_CONTROL_FRAME_BYTES).await?;
            return Ok(Served::Ping);
        }
        HostMessage::Run(run) => run,
        HostMessage::Start(_) => {
            let reason = "a start message came before a run".to_owned();
            let reply = GuestMessage::Error(GuestErrorMessage {
                run_id: String::new(),
                kind: GuestErrorKind::InvalidPlan,
                message: reason.clone(),
            });
            let _ = write_message(stream, reply, MAX_CONTROL_FRAME_BYTES).await;
            return Err(FrameError::Malformed(reason));
        }
    };
    let reply = match run_plan(&run, stream, executor, kernel).await {
        Ok(outcome) => GuestMessage::Outcome(OutcomeMessage {
            run_id: run.run_id,
            outcome,
        }),
        Err((kind, message)) => GuestMessage::Error(GuestErrorMessage {
            run_id: run.run_id,
            kind,
            message,
        }),
    };
    if let Err(e) = write_message(stream, reply, MAX_FRAME_BYTES).await {
        tracing::warn!(error = %e, "the guest could not send the reply of a run");
    }
    Ok(Served::Run)
}

/// Check the run and reseed the random pool. Then wait for the start, set
/// the clock, and run the plan.
async fn run_plan<S>(
    run: &RunMessage,
    stream: &mut S,
    executor: &dyn PlanExecutor,
    kernel: &dyn GuestKernel,
) -> Result<SandboxOutcome, (GuestErrorKind, String)>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let entropy = parse_entropy(&run.entropy).map_err(|e| (GuestErrorKind::InvalidPlan, e))?;
    run.plan
        .validate()
        .map_err(|e| (GuestErrorKind::InvalidPlan, e.to_string()))?;
    // The code must not run on cloned random state, so a failed reseed
    // stops the run.
    kernel.reseed(&entropy).map_err(|e| {
        (
            GuestErrorKind::Internal,
            format!("could not reseed the random pool: {e}"),
        )
    })?;
    let start = await_start(stream, &run.run_id).await.map_err(|e| {
        (
            GuestErrorKind::Internal,
            format!("the host did not start the run: {e}"),
        )
    })?;
    kernel.set_clock(start.unix_time_ms).map_err(|e| {
        (
            GuestErrorKind::Internal,
            format!("could not set the clock: {e}"),
        )
    })?;
    let cancel = CancellationToken::new();
    executor
        .execute(&run.plan, &cancel)
        .await
        .map_err(|e| match e {
            SandboxError::Denied(message) => (GuestErrorKind::SpawnFailed, message),
            other => (GuestErrorKind::Internal, other.to_string()),
        })
}

/// Tell the host that the run is ready, and read its start message.
///
/// # Errors
///
/// Returns a message when the stream fails or the host sends another
/// message.
async fn await_start<S>(stream: &mut S, run_id: &str) -> Result<StartMessage, String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let ready = GuestMessage::Ready(ReadyMessage {
        run_id: run_id.to_owned(),
    });
    write_message(stream, ready, MAX_FRAME_BYTES)
        .await
        .map_err(|e| e.to_string())?;
    match read_message(stream, MAX_CONTROL_FRAME_BYTES).await {
        Ok(HostMessage::Start(start)) => Ok(start),
        Ok(_) => Err("the host sent another message in place of the start".into()),
        Err(e) => Err(e.to_string()),
    }
}

/// Parse 64 lowercase hex characters into 32 bytes.
///
/// # Errors
///
/// Returns a message for any other input.
pub fn parse_entropy(hex: &str) -> Result<[u8; 32], String> {
    let digits = hex.as_bytes();
    if digits.len() != 64 {
        return Err(format!(
            "entropy must be 64 hex digits, got {}",
            digits.len()
        ));
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        _ => Err("entropy must be lowercase hex".to_owned()),
    };
    let mut bytes = [0u8; 32];
    let (pairs, _) = digits.as_chunks::<2>();
    for (byte, [high, low]) in bytes.iter_mut().zip(pairs) {
        *byte = (nibble(*high)? << 4) | nibble(*low)?;
    }
    Ok(bytes)
}

/// Format 32 bytes as 64 lowercase hex characters.
#[must_use]
pub fn format_entropy(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// Ask the guest agent whether it is ready.
///
/// # Errors
///
/// Returns [`SandboxError::Unavailable`] when the guest does not answer with
/// a pong.
pub async fn ping<S>(stream: &mut S) -> Result<(), SandboxError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    write_message(stream, HostMessage::Ping, MAX_CONTROL_FRAME_BYTES)
        .await
        .map_err(|e| SandboxError::Unavailable(format!("could not ping the guest: {e}")))?;
    match read_message::<_, GuestMessage>(stream, MAX_CONTROL_FRAME_BYTES).await {
        Ok(GuestMessage::Pong) => Ok(()),
        Ok(other) => Err(SandboxError::Unavailable(format!(
            "the guest answered a ping with {other:?}"
        ))),
        Err(e) => Err(SandboxError::Unavailable(format!(
            "no pong from the guest: {e}"
        ))),
    }
}

/// Send `job` to the guest and read its outcome.
///
/// # Errors
///
/// - [`SandboxError::Unavailable`] when the stream fails or closes early, or
///   when the guest reports an internal error.
/// - [`SandboxError::Denied`] when the runtime command could not start.
/// - [`SandboxError::Protocol`] for a malformed or oversized frame, a wrong
///   run id, a wrong message type or a refused plan.
pub async fn request_run<S>(
    stream: &mut S,
    job: &SandboxJob,
) -> Result<SandboxOutcome, SandboxError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    send_run(stream, job).await?;
    read_outcome(stream, job).await
}

/// Send the run request of `job` to the guest, and start the run.
///
/// The host takes its clock for the start message only after the guest
/// answers that it decoded the request. So neither the transfer nor the
/// decode puts the guest clock behind.
///
/// # Errors
///
/// - [`SandboxError::Unavailable`] when the stream fails, or when the guest
///   reports an internal error.
/// - [`SandboxError::Denied`] when the request is too large.
/// - [`SandboxError::Protocol`] for a malformed or wrong answer, or a
///   refused plan.
pub async fn send_run<S>(stream: &mut S, job: &SandboxJob) -> Result<(), SandboxError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let run = HostMessage::Run(Box::new(RunMessage {
        run_id: job.run_id.clone(),
        entropy: format_entropy(&job.entropy),
        plan: job.plan.clone(),
    }));
    write_message(stream, run, MAX_FRAME_BYTES)
        .await
        .map_err(|e| run_write_error(&e))?;
    match read_reply(stream, job).await? {
        GuestMessage::Ready(ready) if ready.run_id == job.run_id => {}
        other => return Err(wrong_reply(other, job)),
    }
    let start = HostMessage::Start(StartMessage {
        unix_time_ms: unix_time_ms(),
    });
    write_message(stream, start, MAX_CONTROL_FRAME_BYTES)
        .await
        .map_err(|e| SandboxError::Unavailable(format!("could not start the run: {e}")))
}

/// Read the outcome of `job` from the guest.
///
/// # Errors
///
/// The same as [`request_run`], except the errors of the request write.
pub async fn read_outcome<S>(
    stream: &mut S,
    job: &SandboxJob,
) -> Result<SandboxOutcome, SandboxError>
where
    S: AsyncRead + Unpin + Send,
{
    match read_reply(stream, job).await? {
        GuestMessage::Outcome(message) if message.run_id == job.run_id => Ok(message.outcome),
        other => Err(wrong_reply(other, job)),
    }
}

/// Read one answer of the guest to `job`.
async fn read_reply<S>(stream: &mut S, job: &SandboxJob) -> Result<GuestMessage, SandboxError>
where
    S: AsyncRead + Unpin + Send,
{
    read_message(stream, max_outcome_frame_bytes(&job.plan))
        .await
        .map_err(|e| match e {
            FrameError::Closed | FrameError::Io(_) => SandboxError::Unavailable(format!(
                "the guest closed the connection without an answer: {e}"
            )),
            FrameError::TooLarge { .. } | FrameError::Malformed(_) => {
                SandboxError::Protocol(format!("bad guest frame: {e}"))
            }
        })
}

/// The error for a guest answer that is not the one the host waits for.
fn wrong_reply(reply: GuestMessage, job: &SandboxJob) -> SandboxError {
    match reply {
        GuestMessage::Ready(ReadyMessage { run_id })
        | GuestMessage::Outcome(OutcomeMessage { run_id, .. })
            if run_id != job.run_id =>
        {
            SandboxError::Protocol(format!(
                "the answer has run id {}, not {:?}",
                shown(&run_id),
                job.run_id
            ))
        }
        GuestMessage::Ready(_) => {
            SandboxError::Protocol("the guest sent a second ready message".into())
        }
        GuestMessage::Outcome(_) => {
            SandboxError::Protocol("the guest sent an outcome before the start".into())
        }
        GuestMessage::Error(error) if error.run_id.is_empty() => SandboxError::Protocol(format!(
            "the guest refused the request: {}",
            clean(&error.message)
        )),
        GuestMessage::Error(error) if error.run_id != job.run_id => {
            SandboxError::Protocol(format!(
                "the error has run id {}, not {:?}",
                shown(&error.run_id),
                job.run_id
            ))
        }
        GuestMessage::Error(error) => match error.kind {
            GuestErrorKind::InvalidPlan => SandboxError::Protocol(format!(
                "the guest refused the plan: {}",
                clean(&error.message)
            )),
            GuestErrorKind::SpawnFailed => SandboxError::Denied(clean(&error.message)),
            GuestErrorKind::Internal => {
                SandboxError::Unavailable(format!("the guest failed: {}", clean(&error.message)))
            }
        },
        GuestMessage::Pong => SandboxError::Protocol("the guest answered a run with a pong".into()),
    }
}

/// The longest guest error text that the host keeps, in bytes.
pub const MAX_GUEST_ERROR_BYTES: usize = 4096;

/// The most inodes that the guest `/work` tmpfs holds. It bounds the time
/// that the guest takes to remove a work tree.
pub const WORK_MAX_INODES: u32 = 65_536;

/// A guest error text for history: at most [`MAX_GUEST_ERROR_BYTES`], with
/// each NUL as U+FFFD. Postgres `jsonb` rejects `\u0000`.
fn clean(text: &str) -> String {
    let mut end = text.len().min(MAX_GUEST_ERROR_BYTES);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].replace('\0', "\u{fffd}")
}

/// The host wall clock, in milliseconds since the Unix epoch. A clock before
/// the epoch gives zero.
fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::sandbox::outcome::RunStatus;
    use crate::sandbox::outcome::tests::outcome;
    use crate::sandbox::policy::SandboxPolicy;
    use crate::sandbox::protocol::{envelope, write_frame};
    use crate::sandbox::request::SandboxRequest;

    /// Records each call, in order, and answers with a fixed result.
    struct Recorder {
        events: Arc<Mutex<Vec<String>>>,
        answer: Result<SandboxOutcome, SandboxError>,
    }

    impl PlanExecutor for Recorder {
        fn execute<'a>(
            &'a self,
            plan: &'a ExecPlan,
            _cancel: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<SandboxOutcome, SandboxError>> {
            self.events
                .lock()
                .unwrap()
                .push(format!("execute {}", plan.argv.join(" ")));
            Box::pin(std::future::ready(self.answer.clone()))
        }
    }

    fn job() -> SandboxJob {
        SandboxJob {
            run_id: "run-1".into(),
            entropy: [7; 32],
            plan: SandboxPolicy::default()
                .plan(&SandboxRequest::new("sh", "echo hi"))
                .unwrap(),
        }
    }

    /// Run the host side against a guest that serves with `answer`.
    async fn exchange(
        job: SandboxJob,
        answer: Result<SandboxOutcome, SandboxError>,
        reseed_ok: bool,
    ) -> (Result<SandboxOutcome, SandboxError>, Vec<String>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let executor = Recorder {
            events: Arc::clone(&events),
            answer,
        };
        let kernel = FakeKernel {
            events: Arc::clone(&events),
            reseed_ok,
        };
        let (mut host, mut guest) = tokio::io::duplex(1 << 20);
        let server =
            tokio::spawn(async move { serve_connection(&mut guest, &executor, &kernel).await });
        let result = request_run(&mut host, &job).await;
        let what = server.await.unwrap().unwrap();
        assert_eq!(what, Served::Run);
        let log = events.lock().unwrap().clone();
        (result, log)
    }

    #[tokio::test]
    async fn a_ping_gets_a_pong() {
        let (mut host, mut guest) = tokio::io::duplex(4096);
        let executor = Recorder {
            events: Arc::default(),
            answer: Err(SandboxError::Cancelled),
        };
        let server =
            tokio::spawn(
                async move { serve_connection(&mut guest, &executor, &UnmanagedKernel).await },
            );
        ping(&mut host).await.unwrap();
        assert_eq!(server.await.unwrap().unwrap(), Served::Ping);
    }

    /// A guest kernel that records its calls.
    struct FakeKernel {
        events: Arc<Mutex<Vec<String>>>,
        reseed_ok: bool,
    }

    impl GuestKernel for FakeKernel {
        fn reseed(&self, entropy: &[u8; 32]) -> std::io::Result<()> {
            self.events
                .lock()
                .unwrap()
                .push(format!("reseed {}", entropy[0]));
            if self.reseed_ok {
                Ok(())
            } else {
                Err(std::io::Error::other("no entropy device"))
            }
        }

        fn set_clock(&self, unix_time_ms: u64) -> std::io::Result<()> {
            let now = u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
            .unwrap();
            let near = now.abs_diff(unix_time_ms) < 60_000;
            self.events
                .lock()
                .unwrap()
                .push(format!("clock near={near}"));
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_run_sets_the_clock_after_the_reseed_and_before_the_code() {
        let mut answer = outcome(RunStatus::Exited { code: 0 });
        answer.stdout = "hi\n".into();
        let (result, log) = exchange(job(), Ok(answer.clone()), true).await;
        assert_eq!(result.unwrap(), answer);
        assert_eq!(log, ["reseed 7", "clock near=true", "execute sh main.sh"]);
    }

    /// A guest kernel that keeps the clock that it gets.
    #[derive(Default)]
    struct ClockKernel(Mutex<Option<u64>>);

    impl GuestKernel for ClockKernel {
        fn reseed(&self, _entropy: &[u8; 32]) -> std::io::Result<()> {
            Ok(())
        }

        fn set_clock(&self, unix_time_ms: u64) -> std::io::Result<()> {
            *self.0.lock().unwrap() = Some(unix_time_ms);
            Ok(())
        }
    }

    #[tokio::test]
    async fn the_clock_is_taken_after_the_request_transfer() {
        // A small pipe makes the request write wait until the guest reads.
        let (mut host, mut guest) = tokio::io::duplex(64);
        let kernel = Arc::new(ClockKernel::default());
        let guest_kernel = Arc::clone(&kernel);
        let sent = unix_time_ms();
        let server = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let executor = Recorder {
                events: Arc::default(),
                answer: Ok(outcome(RunStatus::Exited { code: 0 })),
            };
            serve_connection(&mut guest, &executor, guest_kernel.as_ref()).await
        });
        request_run(&mut host, &job()).await.unwrap();
        assert_eq!(server.await.unwrap().unwrap(), Served::Run);
        let clock = kernel.0.lock().unwrap().expect("the guest set its clock");
        assert!(
            clock >= sent + 500,
            "the guest clock is {} ms behind the host",
            (sent + 500).saturating_sub(clock)
        );
    }

    #[tokio::test]
    async fn a_failed_reseed_stops_the_run() {
        let (result, log) =
            exchange(job(), Ok(outcome(RunStatus::Exited { code: 0 })), false).await;
        assert!(
            matches!(result, Err(SandboxError::Unavailable(_))),
            "{result:?}"
        );
        assert_eq!(log, ["reseed 7"], "the code must not run without a reseed");
    }

    #[tokio::test]
    async fn a_plan_that_fails_the_guest_check_is_a_protocol_violation() {
        let mut bad = job();
        bad.plan.files[0].path = "../../etc/passwd".into();
        let (result, log) = exchange(bad, Ok(outcome(RunStatus::Exited { code: 0 })), true).await;
        assert!(
            matches!(result, Err(SandboxError::Protocol(_))),
            "{result:?}"
        );
        assert!(log.iter().all(|e| !e.starts_with("execute")));
    }

    #[tokio::test]
    async fn a_command_that_cannot_start_is_denied() {
        let (result, _) =
            exchange(job(), Err(SandboxError::Denied("no python3".into())), true).await;
        assert!(matches!(result, Err(SandboxError::Denied(ref m)) if m.contains("no python3")));
    }

    #[tokio::test]
    async fn an_internal_guest_error_is_unavailable() {
        let (result, _) =
            exchange(job(), Err(SandboxError::Unavailable("disk".into())), true).await;
        assert!(matches!(result, Err(SandboxError::Unavailable(_))));
    }

    /// A fake guest that takes the request, waits for the start, and
    /// answers with `reply`.
    async fn fake_guest_reply<T: serde::Serialize + Send + Sync + 'static>(
        reply: T,
    ) -> Result<SandboxOutcome, SandboxError> {
        let (mut host, mut guest) = tokio::io::duplex(1 << 20);
        let server = tokio::spawn(async move {
            let _: HostMessage = read_message(&mut guest, MAX_FRAME_BYTES).await.unwrap();
            let ready = GuestMessage::Ready(ReadyMessage {
                run_id: "run-1".into(),
            });
            write_message(&mut guest, ready, MAX_FRAME_BYTES)
                .await
                .unwrap();
            let start: HostMessage = read_message(&mut guest, MAX_CONTROL_FRAME_BYTES)
                .await
                .unwrap();
            assert!(matches!(start, HostMessage::Start(_)), "{start:?}");
            write_frame(&mut guest, &reply, MAX_FRAME_BYTES)
                .await
                .unwrap();
        });
        let result = request_run(&mut host, &job()).await;
        server.await.unwrap();
        result
    }

    #[tokio::test]
    async fn an_outcome_before_the_start_is_a_protocol_violation() {
        let (mut host, mut guest) = tokio::io::duplex(1 << 20);
        let server = tokio::spawn(async move {
            let _: HostMessage = read_message(&mut guest, MAX_FRAME_BYTES).await.unwrap();
            let early = GuestMessage::Outcome(OutcomeMessage {
                run_id: "run-1".into(),
                outcome: outcome(RunStatus::Exited { code: 0 }),
            });
            write_message(&mut guest, early, MAX_FRAME_BYTES)
                .await
                .unwrap();
        });
        let result = request_run(&mut host, &job()).await;
        server.await.unwrap();
        assert!(
            matches!(result, Err(SandboxError::Protocol(ref m)) if m.contains("before the start")),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn a_start_without_a_run_is_refused() {
        let (mut host, mut guest) = tokio::io::duplex(4096);
        let executor = Recorder {
            events: Arc::default(),
            answer: Ok(outcome(RunStatus::Exited { code: 0 })),
        };
        let server =
            tokio::spawn(
                async move { serve_connection(&mut guest, &executor, &UnmanagedKernel).await },
            );
        let start = HostMessage::Start(StartMessage { unix_time_ms: 0 });
        write_message(&mut host, start, MAX_CONTROL_FRAME_BYTES)
            .await
            .unwrap();
        let reply: GuestMessage = read_message(&mut host, MAX_CONTROL_FRAME_BYTES)
            .await
            .unwrap();
        assert!(
            matches!(reply, GuestMessage::Error(ref e) if e.run_id.is_empty()),
            "{reply:?}"
        );
        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn a_long_guest_error_is_cut_and_has_no_nul() {
        let reply = envelope(GuestMessage::Error(GuestErrorMessage {
            run_id: "run-1".into(),
            kind: GuestErrorKind::SpawnFailed,
            message: format!("bad\0{}", "x".repeat(100_000)),
        }));
        let err = fake_guest_reply(reply).await.unwrap_err().to_string();
        assert!(
            err.len() <= MAX_GUEST_ERROR_BYTES + 64,
            "{} bytes",
            err.len()
        );
        assert!(!err.contains('\0'));
    }

    #[tokio::test]
    async fn a_malformed_outcome_frame_gives_a_bounded_error() {
        let kind = format!("bad\0{}", "x".repeat(1 << 20));
        let reply = serde_json::json!({"version": 1, "message": {"type": kind}});
        let err = fake_guest_reply(reply).await.unwrap_err();
        assert!(matches!(err, SandboxError::Protocol(_)), "{err:?}");
        let err = err.to_string();
        assert!(
            err.len() <= MAX_GUEST_ERROR_BYTES + 64,
            "{} bytes",
            err.len()
        );
        assert!(!err.contains('\0'));
    }

    #[tokio::test]
    async fn an_unreadable_request_gets_an_error_frame() {
        let (mut host, mut guest) = tokio::io::duplex(4096);
        let executor = Recorder {
            events: Arc::default(),
            answer: Err(SandboxError::Cancelled),
        };
        let server =
            tokio::spawn(
                async move { serve_connection(&mut guest, &executor, &UnmanagedKernel).await },
            );
        let wrong = serde_json::json!({"version": 99, "message": {"type": "ping"}});
        write_frame(&mut host, &wrong, 1024).await.unwrap();
        let reply: GuestMessage = read_message(&mut host, MAX_CONTROL_FRAME_BYTES)
            .await
            .unwrap();
        assert!(
            matches!(reply, GuestMessage::Error(ref e) if e.run_id.is_empty() && e.message.contains("version")),
            "{reply:?}"
        );
        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn a_run_whose_reply_fails_still_counts_as_the_run() {
        let (mut host, mut guest) = tokio::io::duplex(1 << 20);
        let executor = Recorder {
            events: Arc::default(),
            answer: Ok(outcome(RunStatus::Exited { code: 0 })),
        };
        let run = HostMessage::Run(Box::new(RunMessage {
            run_id: "run-1".into(),
            entropy: "00".repeat(32),
            plan: job().plan,
        }));
        write_message(&mut host, run, MAX_FRAME_BYTES)
            .await
            .unwrap();
        // The host goes away before the reply.
        drop(host);
        let served = serve_connection(&mut guest, &executor, &UnmanagedKernel)
            .await
            .unwrap();
        assert_eq!(served, Served::Run);
    }

    #[tokio::test]
    async fn a_wrong_run_id_is_a_protocol_violation() {
        let reply = envelope(GuestMessage::Outcome(OutcomeMessage {
            run_id: "someone-else".into(),
            outcome: outcome(RunStatus::Exited { code: 0 }),
        }));
        let result = fake_guest_reply(reply).await;
        assert!(matches!(result, Err(SandboxError::Protocol(ref m)) if m.contains("run id")));
    }

    #[tokio::test]
    async fn a_pong_in_place_of_an_outcome_is_a_protocol_violation() {
        let result = fake_guest_reply(envelope(GuestMessage::Pong)).await;
        assert!(matches!(result, Err(SandboxError::Protocol(_))));
    }

    #[tokio::test]
    async fn garbage_from_the_guest_is_a_protocol_violation() {
        let result = fake_guest_reply(serde_json::json!({"version": 1, "message": "rm -rf"})).await;
        assert!(matches!(result, Err(SandboxError::Protocol(_))));
    }

    #[tokio::test]
    async fn an_oversized_answer_is_a_protocol_violation_without_reading_it() {
        use tokio::io::AsyncWriteExt as _;
        let (mut host, mut guest) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(async move {
            let _: HostMessage = read_message(&mut guest, MAX_FRAME_BYTES).await.unwrap();
            guest.write_all(&[0x7f, 0xff, 0xff, 0xff]).await.unwrap();
            guest
        });
        let result = request_run(&mut host, &job()).await;
        assert!(
            matches!(result, Err(SandboxError::Protocol(ref m)) if m.contains("exceeds")),
            "{result:?}"
        );
        drop(server.await.unwrap());
    }

    #[tokio::test]
    async fn a_guest_that_closes_early_is_unavailable() {
        let (mut host, guest) = tokio::io::duplex(1 << 20);
        drop(guest);
        let result = request_run(&mut host, &job()).await;
        assert!(
            matches!(result, Err(SandboxError::Unavailable(_))),
            "{result:?}"
        );
    }

    #[test]
    fn entropy_round_trips_through_hex() {
        let bytes: [u8; 32] = std::array::from_fn(|i| u8::try_from(i * 7).unwrap());
        let hex = format_entropy(&bytes);
        assert_eq!(hex.len(), 64);
        assert_eq!(parse_entropy(&hex).unwrap(), bytes);
        assert!(parse_entropy("00").is_err());
        assert!(parse_entropy(&"zz".repeat(32)).is_err());
        assert!(parse_entropy(&"AB".repeat(32)).is_err(), "lowercase only");
    }

    #[tokio::test]
    async fn bad_entropy_is_refused_by_the_guest() {
        let (mut host, mut guest) = tokio::io::duplex(1 << 20);
        let executor = Recorder {
            events: Arc::default(),
            answer: Ok(outcome(RunStatus::Exited { code: 0 })),
        };
        let server =
            tokio::spawn(
                async move { serve_connection(&mut guest, &executor, &UnmanagedKernel).await },
            );
        let job = job();
        let run = HostMessage::Run(Box::new(RunMessage {
            run_id: job.run_id,
            entropy: "not-hex".into(),
            plan: job.plan,
        }));
        write_message(&mut host, run, MAX_FRAME_BYTES)
            .await
            .unwrap();
        let reply: GuestMessage = read_message(&mut host, MAX_FRAME_BYTES).await.unwrap();
        assert!(matches!(
            reply,
            GuestMessage::Error(GuestErrorMessage {
                kind: GuestErrorKind::InvalidPlan,
                ..
            })
        ));
        server.await.unwrap().unwrap();
    }

    #[test]
    fn the_frame_bound_covers_the_largest_outcome() {
        let plan = SandboxPolicy::default()
            .plan(&SandboxRequest::new("sh", "").with_output("a"))
            .unwrap();
        let max = usize::try_from(plan.limits.max_output_bytes).unwrap();
        let mut worst = outcome(RunStatus::Exited { code: 0 });
        // A control character escapes to six bytes of JSON.
        worst.stdout = "\u{1}".repeat(max);
        worst.stderr = "\u{1}".repeat(max);
        worst.outputs.push(crate::sandbox::outcome::OutputFile {
            path: "a".into(),
            content: vec![0; usize::try_from(plan.max_output_files_total_bytes).unwrap()],
        });
        let message = envelope(GuestMessage::Outcome(OutcomeMessage {
            run_id: "r".repeat(64),
            outcome: worst,
        }));
        let size = serde_json::to_vec(&message).unwrap().len();
        assert!(max_outcome_frame_bytes(&plan) >= size, "{size}");
        assert!(max_outcome_frame_bytes(&plan) <= MAX_FRAME_BYTES);
    }

    #[test]
    fn the_frame_bound_covers_many_skipped_outputs_with_long_paths() {
        let mut sizes = crate::sandbox::policy::DEFAULT_SIZE_BOUNDS;
        sizes.max_outputs = 200;
        let mut tiny = crate::sandbox::policy::DEFAULT_LIMITS;
        tiny.max_output_bytes = 1;
        let policy = SandboxPolicy::default()
            .with_sizes(sizes)
            .with_limits(tiny, crate::sandbox::policy::MAX_LIMITS);
        let mut request = SandboxRequest::new("sh", "");
        let paths: Vec<String> = (0..200)
            .map(|i| format!("{i:03}/{}/{}", "\u{1}".repeat(250), "p".repeat(250)))
            .collect();
        for path in &paths {
            request = request.with_output(path.clone());
        }
        let plan = policy.plan(&request).unwrap();
        let mut skipped = outcome(RunStatus::Exited { code: 0 });
        for path in paths {
            skipped
                .skipped_outputs
                .push(crate::sandbox::outcome::SkippedOutput {
                    path,
                    reason: "\u{1}".repeat(crate::sandbox::outcome::MAX_SKIP_REASON_BYTES),
                });
        }
        let message = envelope(GuestMessage::Outcome(OutcomeMessage {
            run_id: "r".repeat(64),
            outcome: skipped,
        }));
        let size = serde_json::to_vec(&message).unwrap().len();
        assert!(max_outcome_frame_bytes(&plan) >= size, "{size}");
    }

    #[test]
    fn a_run_too_large_for_a_frame_is_a_denial_not_a_retry() {
        let err = run_write_error(&FrameError::TooLarge {
            declared: 1 << 30,
            max: MAX_FRAME_BYTES,
        });
        assert!(matches!(err, SandboxError::Denied(_)), "{err:?}");
        let err = run_write_error(&FrameError::Closed);
        assert!(matches!(err, SandboxError::Unavailable(_)), "{err:?}");
    }
}
