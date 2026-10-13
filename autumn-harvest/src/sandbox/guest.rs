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
use super::outcome::SandboxOutcome;
use super::protocol::{
    FrameError, GuestErrorKind, GuestErrorMessage, GuestMessage, HostMessage,
    MAX_CONTROL_FRAME_BYTES, MAX_FRAME_BYTES, OutcomeMessage, RunMessage, read_message,
    write_message,
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

/// Mixes host entropy into the guest random pool.
pub type ReseedFn = dyn Fn(&[u8; 32]) -> std::io::Result<()> + Send + Sync;

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
    stream
        .saturating_mul(2)
        .saturating_add(files)
        .saturating_add(FRAME_OVERHEAD_BYTES)
        .min(MAX_FRAME_BYTES)
}

/// Room for the envelope, the run id, the paths and the other fields.
const FRAME_OVERHEAD_BYTES: usize = 64 * 1024;

/// Serve one connection: answer a ping, or run one plan.
///
/// # Errors
///
/// Returns the [`FrameError`] of a request that cannot be read, or of a
/// reply that cannot be written. A failed run is not an error here: the
/// guest reports it to the host in a [`GuestMessage::Error`].
pub async fn serve_connection<S>(
    stream: &mut S,
    executor: &dyn PlanExecutor,
    reseed: &ReseedFn,
) -> Result<Served, FrameError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let message: HostMessage = read_message(stream, MAX_FRAME_BYTES).await?;
    let run = match message {
        HostMessage::Ping => {
            write_message(stream, GuestMessage::Pong, MAX_CONTROL_FRAME_BYTES).await?;
            return Ok(Served::Ping);
        }
        HostMessage::Run(run) => run,
    };
    let reply = match run_plan(&run, executor, reseed).await {
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
    write_message(stream, reply, MAX_FRAME_BYTES).await?;
    Ok(Served::Run)
}

/// Check the run, reseed the random pool, then run the plan.
async fn run_plan(
    run: &RunMessage,
    executor: &dyn PlanExecutor,
    reseed: &ReseedFn,
) -> Result<SandboxOutcome, (GuestErrorKind, String)> {
    let entropy = parse_entropy(&run.entropy).map_err(|e| (GuestErrorKind::InvalidPlan, e))?;
    run.plan
        .validate()
        .map_err(|e| (GuestErrorKind::InvalidPlan, e.to_string()))?;
    // The code must not run on cloned random state, so a failed reseed
    // stops the run.
    reseed(&entropy).map_err(|e| {
        (
            GuestErrorKind::Internal,
            format!("could not reseed the random pool: {e}"),
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

/// Parse 64 lowercase hex characters into 32 bytes.
///
/// # Errors
///
/// Returns a message for any other input.
pub fn parse_entropy(hex: &str) -> Result<[u8; 32], String> {
    let digits = hex.as_bytes();
    if digits.len() != 64 {
        return Err(format!("entropy must be 64 hex digits, got {}", digits.len()));
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        _ => Err("entropy must be lowercase hex".to_owned()),
    };
    let mut bytes = [0u8; 32];
    for (byte, pair) in bytes.iter_mut().zip(digits.chunks_exact(2)) {
        *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
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
        Err(e) => Err(SandboxError::Unavailable(format!("no pong from the guest: {e}"))),
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
pub async fn request_run<S>(stream: &mut S, job: &SandboxJob) -> Result<SandboxOutcome, SandboxError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let run = HostMessage::Run(RunMessage {
        run_id: job.run_id.clone(),
        entropy: format_entropy(&job.entropy),
        plan: job.plan.clone(),
    });
    write_message(stream, run, MAX_FRAME_BYTES)
        .await
        .map_err(|e| SandboxError::Unavailable(format!("could not send the run: {e}")))?;
    let reply: GuestMessage = read_message(stream, max_outcome_frame_bytes(&job.plan))
        .await
        .map_err(|e| match e {
            FrameError::Closed | FrameError::Io(_) => SandboxError::Unavailable(format!(
                "the guest closed the connection without an outcome: {e}"
            )),
            FrameError::TooLarge { .. } | FrameError::Malformed(_) => {
                SandboxError::Protocol(format!("bad outcome frame: {e}"))
            }
        })?;
    match reply {
        GuestMessage::Outcome(message) if message.run_id == job.run_id => Ok(message.outcome),
        GuestMessage::Outcome(message) => Err(SandboxError::Protocol(format!(
            "the outcome has run id {:?}, not {:?}",
            message.run_id, job.run_id
        ))),
        GuestMessage::Error(error) if error.run_id != job.run_id => Err(SandboxError::Protocol(
            format!("the error has run id {:?}, not {:?}", error.run_id, job.run_id),
        )),
        GuestMessage::Error(error) => Err(match error.kind {
            GuestErrorKind::InvalidPlan => SandboxError::Protocol(format!(
                "the guest refused the plan: {}",
                error.message
            )),
            GuestErrorKind::SpawnFailed => SandboxError::Denied(error.message),
            GuestErrorKind::Internal => SandboxError::Unavailable(format!(
                "the guest failed: {}",
                error.message
            )),
        }),
        GuestMessage::Pong => Err(SandboxError::Protocol(
            "the guest answered a run with a pong".into(),
        )),
    }
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
        let seen = Arc::clone(&events);
        let reseed = move |bytes: &[u8; 32]| {
            seen.lock().unwrap().push(format!("reseed {}", bytes[0]));
            if reseed_ok {
                Ok(())
            } else {
                Err(std::io::Error::other("no entropy device"))
            }
        };
        let (mut host, mut guest) = tokio::io::duplex(1 << 20);
        let server = tokio::spawn(async move {
            serve_connection(&mut guest, &executor, &reseed).await
        });
        let result = request_run(&mut host, &job).await;
        let served = server.await.unwrap().unwrap();
        assert_eq!(served, Served::Run);
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
        let server = tokio::spawn(async move {
            serve_connection(&mut guest, &executor, &|_: &[u8; 32]| Ok(())).await
        });
        ping(&mut host).await.unwrap();
        assert_eq!(server.await.unwrap().unwrap(), Served::Ping);
    }

    #[tokio::test]
    async fn a_run_returns_the_outcome_after_the_reseed() {
        let mut answer = outcome(RunStatus::Exited { code: 0 });
        answer.stdout = "hi\n".into();
        let (result, log) = exchange(job(), Ok(answer.clone()), true).await;
        assert_eq!(result.unwrap(), answer);
        assert_eq!(log, ["reseed 7", "execute sh main.sh"]);
    }

    #[tokio::test]
    async fn a_failed_reseed_stops_the_run() {
        let (result, log) = exchange(job(), Ok(outcome(RunStatus::Exited { code: 0 })), false).await;
        assert!(matches!(result, Err(SandboxError::Unavailable(_))), "{result:?}");
        assert_eq!(log, ["reseed 7"], "the code must not run without a reseed");
    }

    #[tokio::test]
    async fn a_plan_that_fails_the_guest_check_is_a_protocol_violation() {
        let mut bad = job();
        bad.plan.files[0].path = "../../etc/passwd".into();
        let (result, log) = exchange(bad, Ok(outcome(RunStatus::Exited { code: 0 })), true).await;
        assert!(matches!(result, Err(SandboxError::Protocol(_))), "{result:?}");
        assert!(log.iter().all(|e| !e.starts_with("execute")));
    }

    #[tokio::test]
    async fn a_command_that_cannot_start_is_denied() {
        let (result, _) = exchange(job(), Err(SandboxError::Denied("no python3".into())), true).await;
        assert!(matches!(result, Err(SandboxError::Denied(ref m)) if m.contains("no python3")));
    }

    #[tokio::test]
    async fn an_internal_guest_error_is_unavailable() {
        let (result, _) =
            exchange(job(), Err(SandboxError::Unavailable("disk".into())), true).await;
        assert!(matches!(result, Err(SandboxError::Unavailable(_))));
    }

    /// A fake guest that reads the request and answers with `reply`.
    async fn fake_guest_reply<T: serde::Serialize + Send + Sync + 'static>(
        reply: T,
    ) -> Result<SandboxOutcome, SandboxError> {
        let (mut host, mut guest) = tokio::io::duplex(1 << 20);
        let server = tokio::spawn(async move {
            let _: HostMessage = read_message(&mut guest, MAX_FRAME_BYTES).await.unwrap();
            write_frame(&mut guest, &reply, MAX_FRAME_BYTES).await.unwrap();
        });
        let result = request_run(&mut host, &job()).await;
        server.await.unwrap();
        result
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
        let (mut host, mut guest) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(async move {
            let _: HostMessage = read_message(&mut guest, MAX_FRAME_BYTES).await.unwrap();
            use tokio::io::AsyncWriteExt as _;
            guest.write_all(&[0x7f, 0xff, 0xff, 0xff]).await.unwrap();
            guest
        });
        let result = request_run(&mut host, &job()).await;
        assert!(matches!(result, Err(SandboxError::Protocol(ref m)) if m.contains("exceeds")), "{result:?}");
        drop(server.await.unwrap());
    }

    #[tokio::test]
    async fn a_guest_that_closes_early_is_unavailable() {
        let (mut host, guest) = tokio::io::duplex(1 << 20);
        drop(guest);
        let result = request_run(&mut host, &job()).await;
        assert!(matches!(result, Err(SandboxError::Unavailable(_))), "{result:?}");
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
        let server = tokio::spawn(async move {
            serve_connection(&mut guest, &executor, &|_: &[u8; 32]| Ok(())).await
        });
        let job = job();
        let run = HostMessage::Run(RunMessage {
            run_id: job.run_id,
            entropy: "not-hex".into(),
            plan: job.plan,
        });
        write_message(&mut host, run, MAX_FRAME_BYTES).await.unwrap();
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
}
