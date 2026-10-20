//! Durable streams: a durable route that answers with a resumable SSE stream.
//!
//! [`DurableCall::stream`](super::DurableCall::stream) runs the checks, the
//! key and the start of `send()`. It then streams each chunk that the run
//! stores with `publish_durable_progress`, and one final frame.
//!
//! The SSE id of a chunk is `<attempt>.<offset>`. The attempt is the place
//! of the run in its lineage: the run that the key gives, then each retry
//! attempt and each continue-as-new successor. A retry of the request with
//! `Last-Event-ID` resumes after that id. The id holds no execution id.
//!
//! See `docs/durable-routes.md` and `DESIGN-durable-streaming.md`.

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::{ExecutionId, HarvestResult};
use autumn_web::reexports::axum;
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::sse::Event;
use axum::response::{IntoResponse, Response};
use serde_json::{Map, Value, json};
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::Instant;

use super::{
    DEADLINE_HEADROOM, DurableCall, DurableOutcome, DurableOutcomeKind, KeyField, Loggable,
    Prepared, Problem, Started, check_key, failed_extra, problem_json, set_run_headers, time_left,
    within,
};
use crate::api::{
    DURABLE_STREAM_PAGE as PAGE, DURABLE_STREAM_SEND_TIMEOUT as SEND_TIMEOUT,
    PROGRESS_STREAM_KEEPALIVE as KEEPALIVE, SseSender,
};

/// The request header that carries the resume cursor.
pub const LAST_EVENT_ID_HEADER: &str = "last-event-id";

/// The default cap on live streams in one process.
pub const DEFAULT_MAX_STREAMS: usize = 32;

/// The stream budget when the handler sets none.
pub const DEFAULT_STREAM_FOR: Duration = Duration::from_secs(300);

/// The longest stream budget. The route clamps a longer budget to it.
pub const MAX_STREAM_FOR: Duration = Duration::from_secs(3600);

/// The longest wait between two reads of the run.
///
/// A wake comes only with a stored chunk. An end with no chunk, or a cancel
/// from outside, sends no wake, so the tick bounds that delay.
const TICK: Duration = Duration::from_secs(1);

/// The longest `LISTEN` connect. A slower connect falls back to polling.
const LISTEN_CONNECT: Duration = Duration::from_secs(5);

/// Frames that wait for the client in memory.
const FRAME_BUFFER: usize = 32;

/// The least time that a stream drains stored chunks, also with a zero
/// budget. A fast client then gets the backlog in one connection.
const DRAIN_FLOOR: Duration = Duration::from_secs(5);

/// The reconnect delay that a `pending` frame asks for.
const RESUME_RETRY: Duration = Duration::from_secs(1);

/// The answer for a malformed `Last-Event-ID`.
pub(super) const CURSOR_INVALID: Problem =
    Problem::new(StatusCode::BAD_REQUEST, "stream-cursor-invalid");

/// The place to resume a stream: after `offset` in the run at `attempt`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Cursor {
    attempt: usize,
    /// `None` resumes at the first chunk of the run.
    offset: Option<i64>,
}

/// Parse the `Last-Event-ID` fields. The error is a problem slug.
///
/// The grammar is `<attempt>` or `<attempt>.<offset>`, in decimal digits. An
/// absent or empty field starts at attempt 0. An offset from `i64::MAX` to
/// `u64::MAX` clamps to `i64::MAX`. A larger number is invalid.
pub(super) fn parse_cursor(field: &KeyField) -> Result<Cursor, &'static str> {
    let invalid = CURSOR_INVALID.slug;
    let raw = match field {
        KeyField::Absent => return Ok(Cursor::default()),
        KeyField::Many => return Err(invalid),
        KeyField::One(raw) => std::str::from_utf8(raw).map_err(|_| invalid)?.trim(),
    };
    if raw.is_empty() {
        return Ok(Cursor::default());
    }
    let (attempt, offset) = match raw.split_once('.') {
        Some((attempt, offset)) => (attempt, Some(offset)),
        None => (raw, None),
    };
    if !is_digits(attempt) || !offset.is_none_or(is_digits) {
        return Err(invalid);
    }
    let attempt = attempt.parse::<usize>().map_err(|_| invalid)?;
    let offset = match offset {
        None => None,
        Some(offset) => {
            let offset = offset.parse::<u64>().map_err(|_| invalid)?;
            Some(i64::try_from(offset).unwrap_or(i64::MAX))
        }
    };
    Ok(Cursor { attempt, offset })
}

/// `true` when `part` is one or more decimal digits.
///
/// `str::parse` takes a leading `+`, so the parser checks the digits first.
fn is_digits(part: &str) -> bool {
    !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit())
}

/// Why the stream moved to the next run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Cause {
    /// A workflow-level retry attempt.
    Retry,
    /// A continue-as-new successor.
    ContinuedAsNew,
}

impl Cause {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::ContinuedAsNew => "continued-as-new",
        }
    }
}

/// One frame of a durable stream.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Frame {
    /// A stored chunk.
    Progress {
        attempt: usize,
        offset: i64,
        chunk: Value,
    },
    /// The stream moved to the run at `attempt`.
    Attempt { attempt: usize, cause: Cause },
    /// The run completed. This holds the output.
    Result(Value),
    /// The run ended in another way. This holds the problem body.
    Problem(Value),
    /// The stream budget ended first. The client resumes.
    Pending,
}

impl Frame {
    /// The SSE id, for a frame that moves the cursor.
    fn id(&self) -> Option<String> {
        match self {
            Self::Progress {
                attempt, offset, ..
            } => Some(format!("{attempt}.{offset}")),
            Self::Attempt { attempt, .. } => Some(attempt.to_string()),
            Self::Result(_) | Self::Problem(_) | Self::Pending => None,
        }
    }

    fn event(&self) -> Event {
        let event = match self {
            Self::Progress { chunk, .. } => {
                Event::default().event("progress").data(chunk.to_string())
            }
            Self::Attempt { attempt, cause } => Event::default()
                .event("attempt")
                .data(json!({"attempt": attempt, "cause": cause.as_str()}).to_string()),
            Self::Result(output) => Event::default().event("result").data(output.to_string()),
            Self::Problem(body) => Event::default().event("problem").data(body.to_string()),
            Self::Pending => Event::default()
                .event("pending")
                .data(json!({"state": "pending"}).to_string())
                .retry(RESUME_RETRY),
        };
        match self.id() {
            Some(id) => event.id(id),
            None => event,
        }
    }
}

/// The final frame of `outcome`, or `None` while the run is pending.
fn final_frame(outcome: &DurableOutcome) -> Option<Frame> {
    let body = match &outcome.kind {
        DurableOutcomeKind::Pending => return None,
        DurableOutcomeKind::Completed => {
            return Some(Frame::Result(outcome.output.clone().unwrap_or(Value::Null)));
        }
        DurableOutcomeKind::Failed {
            error_type,
            details,
        } => problem_json(
            outcome.status,
            "failed",
            failed_extra(error_type.as_ref(), details.as_ref()),
        ),
        _ => problem_json(outcome.status, outcome.slug(), Map::new()),
    };
    Some(Frame::Problem(Value::Object(body)))
}

/// The `result-unavailable` problem frame.
fn unavailable_frame() -> Frame {
    Frame::Problem(Value::Object(problem_json(
        StatusCode::INTERNAL_SERVER_ERROR,
        "result-unavailable",
        Map::new(),
    )))
}

/// Where the frames go. Production sends SSE events. A unit test records.
pub(super) trait FrameSink: Send {
    /// Send one frame. `false` means that the client is gone or too slow.
    fn send(&mut self, frame: Frame) -> impl Future<Output = bool> + Send;
    /// `true` when the client is gone.
    fn is_closed(&self) -> bool;
}

/// The state of one run of the lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RunState {
    /// The run can still store chunks.
    Live,
    /// The run has ended. Its chunks and its successor are committed.
    Ended,
    /// The row is gone, for example after retention. Nothing can follow.
    Gone,
}

/// The reads of one stream. Production reads Postgres. A unit test reads a
/// fake lineage.
pub(super) trait StreamReads: Send {
    /// The state of the run.
    fn state(
        &mut self,
        exec_id: ExecutionId,
    ) -> impl Future<Output = HarvestResult<RunState>> + Send;
    /// The stored chunks above `after`, at most `limit`, in offset order.
    fn chunks(
        &mut self,
        exec_id: ExecutionId,
        after: Option<i64>,
        limit: i64,
    ) -> impl Future<Output = HarvestResult<Vec<(i64, Value)>>> + Send;
    /// The run that follows `exec_id`, and why.
    fn next_run(
        &mut self,
        exec_id: ExecutionId,
    ) -> impl Future<Output = HarvestResult<Option<(ExecutionId, Cause)>>> + Send;
    /// The final answer of the call, with no wait.
    fn outcome(&mut self) -> impl Future<Output = DurableOutcome> + Send;
    /// Wait for a wake of `exec_id`, or for at most `timeout`.
    fn wake(&mut self, exec_id: ExecutionId, timeout: Duration) -> impl Future<Output = ()> + Send;
}

/// How a stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamEnd {
    /// A `result` or `problem` frame went out.
    Final,
    /// The budget ended first. A `pending` frame went out.
    Pending,
    /// The client is gone.
    ClientGone,
}

/// Stream the lineage of `root` from `cursor` until it ends or `until`.
///
/// The walk takes at most `max_steps` steps from run to run. One loop serves
/// two phases. While it walks to the run at the cursor, it sends nothing.
/// After that, it drains each run and sends an `attempt` frame for each step.
pub(super) async fn produce(
    reads: &mut impl StreamReads,
    sink: &mut impl FrameSink,
    root: ExecutionId,
    cursor: Cursor,
    max_steps: usize,
    until: Instant,
) -> StreamEnd {
    let mut walk = Walk {
        current: root,
        attempt: 0,
        visited: HashSet::from([root]),
        max_steps,
    };
    let mut offset = cursor.offset.filter(|_| cursor.attempt == 0);
    let drain_until = until.max(Instant::now() + DRAIN_FLOOR);
    loop {
        if sink.is_closed() {
            return StreamEnd::ClientGone;
        }
        // A long lineage of ended runs steps with no pause, so each step
        // checks the budget too.
        if Instant::now() >= drain_until {
            return send_pending(sink).await;
        }
        let positioning = walk.attempt < cursor.attempt;
        // The state comes first. A terminal cycle commits its chunks and its
        // successor with its state, so the reads after an end see both.
        let Some(state) = budgeted(drain_until, reads.state(walk.current)).await else {
            return send_pending(sink).await;
        };
        let state = logged(state);
        let drained = match state {
            None => false,
            Some(RunState::Gone) => return finish(sink, unavailable_frame(), drain_until).await,
            Some(_) if positioning => true,
            Some(_) => match drain(reads, sink, &walk, &mut offset, drain_until).await {
                Drain::Done => true,
                Drain::Failed => false,
                Drain::Expired => return send_pending(sink).await,
                Drain::ClientGone => return StreamEnd::ClientGone,
            },
        };
        if drained && state == Some(RunState::Ended) {
            let Some(next) = budgeted(drain_until, reads.next_run(walk.current)).await else {
                return send_pending(sink).await;
            };
            match logged(next) {
                Some(Some((next, cause))) => {
                    if !walk.step(next) {
                        return finish(sink, unavailable_frame(), drain_until).await;
                    }
                    if walk.attempt == cursor.attempt {
                        offset = cursor.offset;
                    } else if !positioning {
                        offset = None;
                        let frame = Frame::Attempt {
                            attempt: walk.attempt,
                            cause,
                        };
                        match send_frame(sink, frame, drain_until).await {
                            Sent::Delivered => {}
                            Sent::Expired => return send_pending(sink).await,
                            Sent::ClientGone => return StreamEnd::ClientGone,
                        }
                    }
                    continue;
                }
                // The lineage ends here. A pending answer can change at the
                // next read, so read again.
                Some(None) => {
                    let Some(outcome) = budgeted(drain_until, reads.outcome()).await else {
                        return send_pending(sink).await;
                    };
                    if let Some(frame) = final_frame(&outcome) {
                        return finish(sink, frame, drain_until).await;
                    }
                }
                None => {}
            }
        }
        if let Some(end) = pause(reads, sink, walk.current, until).await {
            return end;
        }
    }
}

/// Run `read` until `until`. `None` means that the deadline came first.
///
/// Every read of the stream goes through here. A read that a busy pool
/// slows therefore cannot hold the slot past the budget.
async fn budgeted<T>(until: Instant, read: impl Future<Output = T>) -> Option<T> {
    tokio::time::timeout_at(until, read).await.ok()
}

/// The value of `read`, or `None` after it logs the error.
fn logged<T>(read: HarvestResult<T>) -> Option<T> {
    read.map_err(|error| {
        tracing::warn!(error = %Loggable(&error), "durable stream cannot read the run; reading again");
    })
    .ok()
}

/// The place of a stream in its lineage.
struct Walk {
    current: ExecutionId,
    attempt: usize,
    visited: HashSet<ExecutionId>,
    max_steps: usize,
}

impl Walk {
    /// Move to `next`. `false` means that the step cap or a cycle stops the
    /// walk.
    fn step(&mut self, next: ExecutionId) -> bool {
        if self.attempt >= self.max_steps {
            tracing::error!(
                max_steps = self.max_steps,
                "durable stream reached its step cap; the result is unavailable"
            );
            return false;
        }
        if !self.visited.insert(next) {
            tracing::error!(exec_id = %next, "durable stream found a cycle in a lineage");
            return false;
        }
        self.current = next;
        self.attempt += 1;
        true
    }
}

/// How one drain ended.
enum Drain {
    /// Every stored chunk above the cursor went out.
    Done,
    /// A read failed. The next tick reads again.
    Failed,
    /// The stream budget is spent. The client resumes at the cursor.
    Expired,
    /// The client is gone.
    ClientGone,
}

/// Send each stored chunk of the current run above `offset`, and move
/// `offset`.
///
/// The drain checks `until` before each frame. A slow client or a long
/// backlog therefore cannot hold the stream past its budget, or past
/// [`DRAIN_FLOOR`] for a shorter budget.
async fn drain(
    reads: &mut impl StreamReads,
    sink: &mut impl FrameSink,
    walk: &Walk,
    offset: &mut Option<i64>,
    until: Instant,
) -> Drain {
    loop {
        let Some(page) = budgeted(until, reads.chunks(walk.current, *offset, PAGE)).await else {
            return Drain::Expired;
        };
        let Some(page) = logged(page) else {
            return Drain::Failed;
        };
        let full = i64::try_from(page.len()).unwrap_or(i64::MAX) >= PAGE;
        for (chunk_offset, chunk) in page {
            let frame = Frame::Progress {
                attempt: walk.attempt,
                offset: chunk_offset,
                chunk,
            };
            match send_frame(sink, frame, until).await {
                Sent::Delivered => *offset = Some(chunk_offset),
                Sent::Expired => return Drain::Expired,
                Sent::ClientGone => return Drain::ClientGone,
            }
        }
        if !full {
            return Drain::Done;
        }
    }
}

/// Wait for a wake or one tick. `Some` ends the stream: the budget is spent,
/// or the client is gone.
async fn pause(
    reads: &mut impl StreamReads,
    sink: &mut impl FrameSink,
    exec_id: ExecutionId,
    until: Instant,
) -> Option<StreamEnd> {
    let remaining = until.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Some(send_pending(sink).await);
    }
    reads.wake(exec_id, remaining.min(TICK)).await;
    sink.is_closed().then_some(StreamEnd::ClientGone)
}

/// Send the `pending` frame at the end of the budget.
async fn send_pending(sink: &mut impl FrameSink) -> StreamEnd {
    if sink.send(Frame::Pending).await {
        StreamEnd::Pending
    } else {
        StreamEnd::ClientGone
    }
}

/// How one budgeted send ended.
enum Sent {
    Delivered,
    /// The deadline passed before the send. Nothing went out.
    Expired,
    ClientGone,
}

/// Send `frame` only while `until` is in the future.
///
/// Every frame but `pending` goes through here. A slow read or a slow send
/// before it therefore never leads to another slow send past the budget.
async fn send_frame(sink: &mut impl FrameSink, frame: Frame, until: Instant) -> Sent {
    if Instant::now() >= until {
        Sent::Expired
    } else if sink.send(frame).await {
        Sent::Delivered
    } else {
        Sent::ClientGone
    }
}

/// Send the final frame, or `pending` when the deadline has passed.
async fn finish(sink: &mut impl FrameSink, frame: Frame, until: Instant) -> StreamEnd {
    match send_frame(sink, frame, until).await {
        Sent::Delivered => StreamEnd::Final,
        Sent::Expired => send_pending(sink).await,
        Sent::ClientGone => StreamEnd::ClientGone,
    }
}

/// The stream budget: `requested`, clamped to [`MAX_STREAM_FOR`].
fn stream_budget(requested: Duration) -> Duration {
    requested.min(MAX_STREAM_FOR)
}

/// The answer of [`DurableCall::stream`](super::DurableCall::stream). It
/// implements [`IntoResponse`].
///
/// It is a plain answer before the stream, such as a problem or `202`
/// pending, or the SSE stream. [`Self::is_stream`] and
/// [`Self::plain_outcome`] tell them apart.
#[must_use]
#[derive(Debug)]
pub struct DurableStream {
    answer: Answer,
    execution_id: Option<ExecutionId>,
    replayed: bool,
}

#[derive(Debug)]
enum Answer {
    /// A plain answer before the stream: a problem or pending.
    Outcome(DurableOutcome),
    /// The SSE stream.
    Stream(Response),
}

impl DurableStream {
    const fn outcome(outcome: DurableOutcome) -> Self {
        Self {
            execution_id: outcome.execution_id,
            replayed: outcome.replayed,
            answer: Answer::Outcome(outcome),
        }
    }

    /// The run that the key started, when the call started or found one.
    #[must_use]
    pub const fn execution_id(&self) -> Option<ExecutionId> {
        self.execution_id
    }

    /// `true` when an earlier request started the run.
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }

    /// The span outcome of this answer.
    const fn slug(&self) -> &'static str {
        match &self.answer {
            Answer::Stream(_) => "stream",
            Answer::Outcome(outcome) => outcome.slug(),
        }
    }

    /// The HTTP status of the answer. A stream answers `200`.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match &self.answer {
            Answer::Outcome(outcome) => outcome.status(),
            Answer::Stream(response) => response.status(),
        }
    }

    /// `true` when the answer is the SSE stream.
    #[must_use]
    pub const fn is_stream(&self) -> bool {
        matches!(self.answer, Answer::Stream(_))
    }

    /// The plain answer before the stream, when there is one.
    #[must_use]
    pub const fn plain_outcome(&self) -> Option<&DurableOutcome> {
        match &self.answer {
            Answer::Outcome(outcome) => Some(outcome),
            Answer::Stream(_) => None,
        }
    }
}

impl IntoResponse for DurableStream {
    fn into_response(self) -> Response {
        match self.answer {
            Answer::Outcome(outcome) => outcome.into_response(),
            Answer::Stream(response) => response,
        }
    }
}

/// The SSE answer, with the headers of the run.
///
/// The stream holds data for one principal, so no cache may store it.
/// `x-accel-buffering: no` stops nginx from buffering the stream.
fn stream_response(
    frames: futures::channel::mpsc::Receiver<Result<Event, Infallible>>,
    replayed: bool,
    exposed_id: Option<ExecutionId>,
) -> Response {
    let mut response = axum::response::sse::Sse::new(frames)
        .keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(KEEPALIVE)
                .text("ping"),
        )
        .into_response();
    let headers = response.headers_mut();
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, private"),
    );
    headers.insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    set_run_headers(headers, replayed, exposed_id);
    response
}

/// Run [`DurableCall::stream`](super::DurableCall::stream) in its span.
pub(super) async fn start(call: DurableCall) -> DurableStream {
    use tracing::Instrument as _;
    let span = call.span();
    let answer = open(call).instrument(span.clone()).await;
    span.record("outcome", answer.slug());
    answer
}

/// Log once that a durable stream runs under Autumn's idempotency layer.
///
/// Each request under that layer answers `500`, so one log line is enough.
fn log_layer_refusal(workflow: &str) {
    static LOGGED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !LOGGED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::error!(
            workflow,
            cause = "a durable stream cannot run under Autumn's idempotency layer",
            "durable route setup error"
        );
    }
}

/// Check the call, start the run, and open the stream.
///
/// The checks run in this order: the idempotency layer, the key, the
/// cursor, then the checks of `send()`.
async fn open(mut call: DurableCall) -> DurableStream {
    let problem = |problem| DurableStream::outcome(DurableOutcome::problem(problem));
    // Autumn's layer collects the body to cache it. The client then gets a
    // live stream only at its end, and the layer caches a cut stream.
    if call.route.layer_active {
        log_layer_refusal(&call.workflow);
        return problem(Problem::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "misconfigured",
        ));
    }
    if let Err(slug) = check_key(&call.route.key) {
        return problem(Problem::new(StatusCode::BAD_REQUEST, slug));
    }
    let Ok(cursor) = parse_cursor(&call.route.cursor) else {
        return problem(CURSOR_INVALID);
    };
    let budget = stream_budget(call.stream_for);
    // A stream always answers `200`, so the success status does not apply.
    call.success = StatusCode::OK;
    let (start, prepared) = match call.prepare() {
        Ok(parts) => parts,
        Err(error) => return problem(error),
    };
    // The start and the first read end before Autumn's request deadline.
    // The stream body runs after the handler, so the deadline does not cut it.
    let deadline = prepared.request_timeout.map(|timeout| {
        Instant::now()
            + time_left(
                prepared.extracted_at.elapsed(),
                timeout,
                false,
                DEADLINE_HEADROOM / 2,
            )
    });
    let task = prepared.spawn_start(start);
    let started = match within(deadline, prepared.started(task)).await {
        Some(Ok(started)) => started,
        Some(Err(error)) => return problem(error),
        // The start can still commit. A retry then finds the claim.
        None => return DurableStream::outcome(DurableOutcome::pending(false)),
    };
    let first = within(
        deadline,
        Box::pin(prepared.answer_within(&started, Duration::ZERO)),
    )
    .await;
    let ended = match first.map(|outcome| (stream_start(&outcome.kind), outcome)) {
        Some((Start::Ended, _)) => true,
        Some((Start::Live, _)) | None => false,
        Some((Start::Plain, outcome)) => return DurableStream::outcome(outcome),
    };
    // Every stream takes a slot. A stream of an ended run reads every stored
    // chunk, so an uncapped count of them could load the database.
    let Ok(permit) = Arc::clone(&prepared.runtime.streams).try_acquire_owned() else {
        tracing::debug!("durable stream cap is full; answering pending");
        let (kind, status) = super::pending_outcome(false);
        return DurableStream::outcome(prepared.after_start(&started, kind, status, None));
    };
    let (tx, rx) = futures::channel::mpsc::channel(FRAME_BUFFER);
    let response = stream_response(
        rx,
        !started.created,
        prepared.expose_id.then_some(started.exec_id),
    );
    let answer = DurableStream {
        answer: Answer::Stream(response),
        execution_id: Some(started.exec_id),
        replayed: !started.created,
    };
    let until = Instant::now() + budget;
    spawn_producer(prepared, started, cursor, until, permit, !ended, tx);
    answer
}

/// How a stream starts, from the first answer about the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Start {
    /// The run is live, or the first read did not finish. The stream opens
    /// `LISTEN`.
    Live,
    /// The lineage ended. The stream reads stored rows only.
    Ended,
    /// The answer is final and holds no chunk to send: erased, or a problem
    /// such as `result-unavailable`. It is a plain answer, as for `send()`.
    Plain,
}

/// The [`Start`] of a stream whose first answer is `kind`.
const fn stream_start(kind: &DurableOutcomeKind) -> Start {
    match kind {
        DurableOutcomeKind::Pending => Start::Live,
        DurableOutcomeKind::Completed
        | DurableOutcomeKind::Failed { .. }
        | DurableOutcomeKind::Cancelled
        | DurableOutcomeKind::Terminated
        | DurableOutcomeKind::TimedOut => Start::Ended,
        DurableOutcomeKind::Erased | DurableOutcomeKind::Problem { .. } => Start::Plain,
    }
}

/// Spawn the task that feeds the stream. It owns the slot and the reads.
///
/// The task logs to the request's subscriber, in the request's span.
fn spawn_producer(
    prepared: Prepared,
    started: Started,
    cursor: Cursor,
    until: Instant,
    permit: OwnedSemaphorePermit,
    listen: bool,
    tx: SseSender,
) {
    use tracing::Instrument as _;
    use tracing::instrument::WithSubscriber as _;
    let root = started.exec_id;
    let max_steps = prepared.runtime.max_chain_steps;
    let task = async move {
        // A panic drops the slot with the task.
        let _permit = permit;
        let mut reads = PgReads {
            prepared,
            started,
            listen,
            listener: None,
        };
        let mut sink = SseSink(tx);
        let end = produce(&mut reads, &mut sink, root, cursor, max_steps, until).await;
        tracing::debug!(?end, "durable stream ended");
    };
    tokio::spawn(task.in_current_span().with_current_subscriber());
}

/// The SSE frame channel, as a [`FrameSink`].
struct SseSink(SseSender);

impl FrameSink for SseSink {
    async fn send(&mut self, frame: Frame) -> bool {
        use futures::SinkExt as _;
        matches!(
            tokio::time::timeout(SEND_TIMEOUT, self.0.send(Ok(frame.event()))).await,
            Ok(Ok(()))
        )
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// The Postgres reads of one stream.
struct PgReads {
    prepared: Prepared,
    started: Started,
    /// `false` for an ended lineage, which polls and opens no `LISTEN`. A
    /// failed connect also sets it to `false`, so the stream polls.
    listen: bool,
    /// The `LISTEN` connection of the current run.
    listener: Option<(ExecutionId, autumn_harvest::notify::DurableStreamListener)>,
}

impl PgReads {
    async fn conn(&self, exec_id: ExecutionId) -> HarvestResult<autumn_harvest::pool::PooledConn> {
        let pool = &self.prepared.runtime.pool;
        let shard = pool.sharded_pool().routed_shard_for_execution(exec_id);
        autumn_harvest::pool::acquire_within_pool_bound(pool.pool_for(shard)).await
    }
}

impl StreamReads for PgReads {
    async fn state(&mut self, exec_id: ExecutionId) -> HarvestResult<RunState> {
        use autumn_harvest::schema::harvest_workflow_executions as executions;
        use diesel::{ExpressionMethods as _, OptionalExtension as _, QueryDsl as _};
        use diesel_async::RunQueryDsl as _;
        let mut conn = self.conn(exec_id).await?;
        let state: Option<String> = executions::table
            .filter(executions::id.eq(exec_id.as_uuid()))
            .select(executions::state)
            .first(&mut conn)
            .await
            .optional()
            .map_err(|error| autumn_harvest::HarvestError::Database(error.to_string()))?;
        Ok(match state {
            None => RunState::Gone,
            Some(state) if crate::api::is_terminal_state(&state) => RunState::Ended,
            Some(_) => RunState::Live,
        })
    }

    async fn chunks(
        &mut self,
        exec_id: ExecutionId,
        after: Option<i64>,
        limit: i64,
    ) -> HarvestResult<Vec<(i64, Value)>> {
        let mut conn = self.conn(exec_id).await?;
        let rows =
            autumn_harvest::store::load_stream_chunks(&mut conn, exec_id, after, limit).await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.stream_offset, row.chunk))
            .collect())
    }

    async fn next_run(
        &mut self,
        exec_id: ExecutionId,
    ) -> HarvestResult<Option<(ExecutionId, Cause)>> {
        use autumn_harvest::schema::harvest_workflow_executions as executions;
        use diesel::{
            BoolExpressionMethods as _, ExpressionMethods as _, OptionalExtension as _,
            QueryDsl as _,
        };
        use diesel_async::RunQueryDsl as _;
        let id = exec_id.as_uuid();
        let mut conn = self.conn(exec_id).await?;
        // Both links have an index. Only one run can follow a run.
        let next: Option<(uuid::Uuid, Option<uuid::Uuid>)> = executions::table
            .filter(
                executions::retry_of_exec_id
                    .eq(id)
                    .or(executions::continued_from_exec_id.eq(id)),
            )
            // A reconciled migration seal is a stub, not a run.
            .filter(executions::migrated_run_terminal_at.is_null())
            .order((executions::started_at.asc(), executions::id.asc()))
            .select((executions::id, executions::retry_of_exec_id))
            .first(&mut conn)
            .await
            .optional()
            .map_err(|error| autumn_harvest::HarvestError::Database(error.to_string()))?;
        Ok(next.map(|(next, retry_of)| {
            let cause = if retry_of == Some(id) {
                Cause::Retry
            } else {
                Cause::ContinuedAsNew
            };
            (ExecutionId::from_uuid(next), cause)
        }))
    }

    async fn outcome(&mut self) -> DurableOutcome {
        Box::pin(self.prepared.answer_within(&self.started, Duration::ZERO)).await
    }

    async fn wake(&mut self, exec_id: ExecutionId, timeout: Duration) {
        use autumn_harvest::notify::{DurableStreamListener, DurableStreamWait};
        if self.listen && self.listener.as_ref().is_none_or(|(id, _)| *id != exec_id) {
            self.listener = None;
            if let Some(url) = &self.prepared.runtime.notify_url {
                let connect = DurableStreamListener::connect(url, exec_id.as_uuid());
                if let Ok(Ok(listener)) =
                    tokio::time::timeout(LISTEN_CONNECT.min(timeout), connect).await
                {
                    self.listener = Some((exec_id, listener));
                    // A chunk can commit before the `LISTEN` starts, so the
                    // stream reads the table once more before it waits.
                    return;
                }
            }
            // A connect retry on each tick would load a busy server. The
            // stream polls for the rest of its life.
            tracing::warn!("durable stream cannot open LISTEN; polling the table");
            self.listen = false;
        }
        match &self.listener {
            Some((_, listener)) => {
                if listener.wait_timeout(timeout).await == DurableStreamWait::ChannelClosed {
                    self.listener = None;
                    self.listen = false;
                    tokio::time::sleep(timeout).await;
                }
            }
            None => tokio::time::sleep(timeout).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::durable_route::{DurableRoute, IDEMPOTENCY_KEY_HEADER};
    use autumn_harvest::HarvestError;
    use autumn_web::AppState;
    use axum::extract::FromRequestParts as _;
    use axum::http::{Method, Request, header};

    const KEY: &str = "3f2b6c1e-7d4a-4f0e-9b1a-2c5d8e7f6a90";

    async fn extract(headers: &[(&str, &[u8])]) -> DurableRoute {
        let state = AppState::for_test();
        let mut builder = Request::builder().method(Method::POST).uri("/answers");
        for (name, value) in headers {
            builder = builder.header(*name, HeaderValue::from_bytes(value).expect("header value"));
        }
        let mut parts = builder.body(()).expect("request").into_parts().0;
        match DurableRoute::from_request_parts(&mut parts, &state).await {
            Ok(route) => route,
            Err(never) => match never {},
        }
    }

    fn one(value: &str) -> KeyField {
        KeyField::One(value.as_bytes().to_vec())
    }

    const fn at(attempt: usize, offset: Option<i64>) -> Cursor {
        Cursor { attempt, offset }
    }

    // SAC-01
    #[test]
    fn cursor_grammar_table() {
        let start = Ok(Cursor::default());
        assert_eq!(parse_cursor(&KeyField::Absent), start);
        assert_eq!(parse_cursor(&one("")), start);
        assert_eq!(parse_cursor(&one("  ")), start);
        assert_eq!(parse_cursor(&one("0")), Ok(at(0, None)));
        assert_eq!(parse_cursor(&one("3")), Ok(at(3, None)));
        assert_eq!(parse_cursor(&one("3.7")), Ok(at(3, Some(7))));
        assert_eq!(parse_cursor(&one(" 2.0 ")), Ok(at(2, Some(0))));
        assert_eq!(
            parse_cursor(&one("1.9223372036854775807")),
            Ok(at(1, Some(i64::MAX)))
        );
        assert_eq!(
            parse_cursor(&one("1.18446744073709551615")),
            Ok(at(1, Some(i64::MAX)))
        );
        for bad in [
            "x",
            "1.",
            ".1",
            "1.-1",
            "-1",
            "+1",
            "1.+2",
            "1.2.3",
            "1,2",
            "1 .2",
            "0x1",
            "99999999999999999999999",
            "1.99999999999999999999999",
        ] {
            assert_eq!(
                parse_cursor(&one(bad)),
                Err("stream-cursor-invalid"),
                "{bad:?}"
            );
        }
        assert_eq!(parse_cursor(&KeyField::Many), Err("stream-cursor-invalid"));
        assert_eq!(
            parse_cursor(&KeyField::One(vec![0xff])),
            Err("stream-cursor-invalid")
        );
    }

    // SAC-02
    #[test]
    fn frame_ids_round_trip_through_the_cursor() {
        let progress = Frame::Progress {
            attempt: 4,
            offset: 12,
            chunk: json!({"t": "a"}),
        };
        let attempt = Frame::Attempt {
            attempt: 2,
            cause: Cause::Retry,
        };
        let id = |frame: &Frame| one(&frame.id().expect("id"));
        assert_eq!(parse_cursor(&id(&progress)), Ok(at(4, Some(12))));
        assert_eq!(parse_cursor(&id(&attempt)), Ok(at(2, None)));
        for frame in [
            Frame::Result(json!(1)),
            Frame::Problem(json!({})),
            Frame::Pending,
        ] {
            assert_eq!(frame.id(), None, "{frame:?} keeps the cursor");
        }
    }

    // SAC-03
    #[tokio::test]
    async fn stream_refuses_the_idempotent_layer_before_any_io() {
        let mut route = extract(&[(IDEMPOTENCY_KEY_HEADER, KEY.as_bytes())]).await;
        route.layer_active = true;
        let answer = route.scope("u").call("w", 1).stream().await;
        assert!(!answer.is_stream());
        assert_eq!(answer.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            answer.plain_outcome().map(DurableOutcome::kind),
            Some(&DurableOutcomeKind::Problem {
                slug: "misconfigured"
            })
        );
    }

    // SAC-04
    #[tokio::test]
    async fn invalid_cursor_answers_400_before_any_io() {
        let answer = extract(&[
            (IDEMPOTENCY_KEY_HEADER, KEY.as_bytes()),
            (LAST_EVENT_ID_HEADER, b"x".as_slice()),
        ])
        .await
        .scope("u")
        .call("w", 1)
        .stream()
        .await;
        assert_eq!(answer.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            answer.plain_outcome().map(DurableOutcome::kind),
            Some(&DurableOutcomeKind::Problem {
                slug: "stream-cursor-invalid"
            })
        );
        // A key problem wins over a cursor problem.
        let answer = extract(&[(LAST_EVENT_ID_HEADER, b"x".as_slice())])
            .await
            .scope("u")
            .call("w", 1)
            .stream()
            .await;
        assert_eq!(
            answer.plain_outcome().map(DurableOutcome::kind),
            Some(&DurableOutcomeKind::Problem {
                slug: "idempotency-key-missing"
            })
        );
    }

    // SAC-05
    #[tokio::test]
    async fn stream_without_runtime_answers_503() {
        let answer = extract(&[
            (IDEMPOTENCY_KEY_HEADER, KEY.as_bytes()),
            (LAST_EVENT_ID_HEADER, b"0.3".as_slice()),
        ])
        .await
        .scope("u")
        .call("w", 1)
        .stream()
        .await;
        assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = answer.into_response();
        assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
    }

    // ── The producer, on a fake lineage ───────────────────────────────────────

    /// One run of a fake lineage.
    #[derive(Default)]
    struct FakeRun {
        ended: bool,
        chunks: Vec<(i64, Value)>,
        next: Option<(ExecutionId, Cause)>,
    }

    /// A change that lands at a wake.
    type Script = Box<dyn FnOnce(&mut HashMap<ExecutionId, FakeRun>) + Send>;

    /// Runs in memory, with a script for each wake.
    struct Fake {
        runs: HashMap<ExecutionId, FakeRun>,
        outcome: DurableOutcome,
        /// Reads that fail before the next read succeeds.
        failing_reads: usize,
        /// The error of a failing read.
        read_error: fn() -> HarvestError,
        /// A commit that lands after the next chunk read: the run ends and
        /// stores one more chunk.
        commit_after_chunks: Option<(ExecutionId, i64)>,
        scripts: std::collections::VecDeque<Script>,
        /// Outcome reads that answer pending before the final outcome.
        pending_outcomes: usize,
        /// The time that each state read takes.
        read_delay: Duration,
        wakes: usize,
        reads: usize,
    }

    impl Fake {
        fn new(outcome: DurableOutcome) -> Self {
            Self {
                runs: HashMap::new(),
                outcome,
                failing_reads: 0,
                read_error: || HarvestError::Database("passing".to_owned()),
                commit_after_chunks: None,
                scripts: std::collections::VecDeque::new(),
                pending_outcomes: 0,
                read_delay: Duration::ZERO,
                wakes: 0,
                reads: 0,
            }
        }

        /// Add a run with `chunks` offsets `0..chunks`.
        fn run(
            &mut self,
            ended: bool,
            chunks: i64,
            next: Option<(ExecutionId, Cause)>,
        ) -> ExecutionId {
            let exec_id = ExecutionId::new();
            self.runs.insert(
                exec_id,
                FakeRun {
                    ended,
                    chunks: (0..chunks).map(|o| (o, json!({"o": o}))).collect(),
                    next,
                },
            );
            exec_id
        }

        fn fail(&mut self) -> HarvestResult<()> {
            self.reads += 1;
            if self.failing_reads > 0 {
                self.failing_reads -= 1;
                return Err((self.read_error)());
            }
            Ok(())
        }
    }

    impl StreamReads for Fake {
        async fn state(&mut self, exec_id: ExecutionId) -> HarvestResult<RunState> {
            tokio::time::sleep(self.read_delay).await;
            self.fail().map(|()| match self.runs.get(&exec_id) {
                None => RunState::Gone,
                Some(run) if run.ended => RunState::Ended,
                Some(_) => RunState::Live,
            })
        }

        fn chunks(
            &mut self,
            exec_id: ExecutionId,
            after: Option<i64>,
            limit: i64,
        ) -> impl Future<Output = HarvestResult<Vec<(i64, Value)>>> + Send {
            std::future::ready(self.page(exec_id, after, limit))
        }

        async fn next_run(
            &mut self,
            exec_id: ExecutionId,
        ) -> HarvestResult<Option<(ExecutionId, Cause)>> {
            tokio::time::sleep(self.read_delay).await;
            self.fail().map(|()| self.runs[&exec_id].next)
        }

        fn outcome(&mut self) -> impl Future<Output = DurableOutcome> + Send {
            let outcome = if self.pending_outcomes > 0 {
                self.pending_outcomes -= 1;
                pending()
            } else {
                self.outcome.clone()
            };
            std::future::ready(outcome)
        }

        async fn wake(&mut self, _exec_id: ExecutionId, timeout: Duration) {
            self.wakes += 1;
            match self.scripts.pop_front() {
                Some(script) => script(&mut self.runs),
                None => tokio::time::sleep(timeout).await,
            }
        }
    }

    impl Fake {
        /// One page of chunks. A pending commit lands after the read.
        fn page(
            &mut self,
            exec_id: ExecutionId,
            after: Option<i64>,
            limit: i64,
        ) -> HarvestResult<Vec<(i64, Value)>> {
            self.fail()?;
            let page: Vec<(i64, Value)> = self.runs[&exec_id]
                .chunks
                .iter()
                .filter(|(o, _)| after.is_none_or(|a| *o > a))
                .take(usize::try_from(limit).expect("limit"))
                .cloned()
                .collect();
            if let Some((run, offset)) = self.commit_after_chunks.take() {
                let run = self.runs.get_mut(&run).expect("run");
                run.ended = true;
                run.chunks.push((offset, json!({"o": offset})));
            }
            Ok(page)
        }
    }

    /// Records frames. It closes after `close_after` frames. Each send takes
    /// `delay`, as for a slow client.
    #[derive(Default)]
    struct Recorder {
        frames: Vec<Frame>,
        close_after: Option<usize>,
        delay: Duration,
    }

    impl FrameSink for Recorder {
        async fn send(&mut self, frame: Frame) -> bool {
            if self.is_closed() {
                return false;
            }
            tokio::time::sleep(self.delay).await;
            self.frames.push(frame);
            true
        }

        fn is_closed(&self) -> bool {
            self.close_after
                .is_some_and(|limit| self.frames.len() >= limit)
        }
    }

    fn completed(output: Value) -> DurableOutcome {
        DurableOutcome {
            kind: DurableOutcomeKind::Completed,
            status: StatusCode::OK,
            execution_id: Some(ExecutionId::new()),
            replayed: false,
            expose_id: false,
            output: Some(output),
            retry_after: None,
        }
    }

    fn pending() -> DurableOutcome {
        DurableOutcome::pending(false)
    }

    fn progress(attempt: usize, offset: i64) -> Frame {
        Frame::Progress {
            attempt,
            offset,
            chunk: json!({"o": offset}),
        }
    }

    fn far() -> Instant {
        Instant::now() + Duration::from_secs(3600)
    }

    async fn run_fake(
        fake: &mut Fake,
        root: ExecutionId,
        cursor: Cursor,
    ) -> (StreamEnd, Vec<Frame>) {
        let mut sink = Recorder::default();
        let end = produce(fake, &mut sink, root, cursor, 1024, far()).await;
        (end, sink.frames)
    }

    // SAC-06
    #[tokio::test(start_paused = true)]
    async fn producer_streams_one_run_then_the_result() {
        let mut fake = Fake::new(completed(json!({"answer": 42})));
        let root = fake.run(true, 3, None);
        let (end, frames) = run_fake(&mut fake, root, Cursor::default()).await;
        assert_eq!(end, StreamEnd::Final);
        assert_eq!(
            frames,
            vec![
                progress(0, 0),
                progress(0, 1),
                progress(0, 2),
                Frame::Result(json!({"answer": 42})),
            ]
        );
    }

    // SAC-07
    #[tokio::test(start_paused = true)]
    async fn producer_resumes_after_the_cursor_with_no_gap_or_duplicate() {
        let mut fake = Fake::new(completed(json!(1)));
        let root = fake.run(true, 5, None);
        let (_, frames) = run_fake(&mut fake, root, at(0, Some(2))).await;
        assert_eq!(
            frames,
            vec![progress(0, 3), progress(0, 4), Frame::Result(json!(1))]
        );
        // A cursor at the run itself sends every chunk.
        let (_, frames) = run_fake(&mut fake, root, at(0, None)).await;
        assert_eq!(frames.len(), 6);
    }

    /// A lineage `a -(retry)-> b -(continue-as-new)-> c`, all ended.
    fn three_runs(fake: &mut Fake) -> (ExecutionId, ExecutionId, ExecutionId) {
        let c = fake.run(true, 1, None);
        let b = fake.run(true, 2, Some((c, Cause::ContinuedAsNew)));
        let a = fake.run(true, 2, Some((b, Cause::Retry)));
        (a, b, c)
    }

    /// Every frame of [`three_runs`], from the start.
    fn lineage_frames() -> Vec<Frame> {
        vec![
            progress(0, 0),
            progress(0, 1),
            Frame::Attempt {
                attempt: 1,
                cause: Cause::Retry,
            },
            progress(1, 0),
            progress(1, 1),
            Frame::Attempt {
                attempt: 2,
                cause: Cause::ContinuedAsNew,
            },
            progress(2, 0),
            Frame::Result(json!("done")),
        ]
    }

    // SAC-08
    #[tokio::test(start_paused = true)]
    async fn producer_follows_retries_and_continue_as_new_with_attempt_frames() {
        let mut fake = Fake::new(completed(json!("done")));
        let (a, _, _) = three_runs(&mut fake);
        let (end, frames) = run_fake(&mut fake, a, Cursor::default()).await;
        assert_eq!(end, StreamEnd::Final);
        assert_eq!(frames, lineage_frames());
        assert_eq!(
            frames,
            vec![
                progress(0, 0),
                progress(0, 1),
                Frame::Attempt {
                    attempt: 1,
                    cause: Cause::Retry
                },
                progress(1, 0),
                progress(1, 1),
                Frame::Attempt {
                    attempt: 2,
                    cause: Cause::ContinuedAsNew
                },
                progress(2, 0),
                Frame::Result(json!("done")),
            ]
        );
    }

    // SAC-09
    #[tokio::test(start_paused = true)]
    async fn producer_resumes_inside_a_later_attempt() {
        let mut fake = Fake::new(completed(json!("done")));
        let (a, _, _) = three_runs(&mut fake);
        let (_, frames) = run_fake(&mut fake, a, at(1, Some(0))).await;
        assert_eq!(
            frames,
            vec![
                progress(1, 1),
                Frame::Attempt {
                    attempt: 2,
                    cause: Cause::ContinuedAsNew
                },
                progress(2, 0),
                Frame::Result(json!("done")),
            ]
        );
        // A cursor at an attempt frame resends no attempt frame.
        let (_, frames) = run_fake(&mut fake, a, at(2, None)).await;
        assert_eq!(frames, vec![progress(2, 0), Frame::Result(json!("done"))]);
    }

    // SAC-10
    #[tokio::test(start_paused = true)]
    async fn producer_cursor_past_the_lineage_sends_only_the_end() {
        let mut fake = Fake::new(completed(json!("done")));
        let (a, _, _) = three_runs(&mut fake);
        let (end, frames) = run_fake(&mut fake, a, at(9, Some(4))).await;
        assert_eq!(end, StreamEnd::Final);
        assert_eq!(frames, vec![Frame::Result(json!("done"))]);
    }

    // SAC-11
    #[tokio::test(start_paused = true)]
    async fn the_end_follows_every_chunk_of_the_last_cycle() {
        let mut fake = Fake::new(completed(json!("done")));
        let root = fake.run(false, 1, None);
        // The last cycle commits its chunk with the terminal state, between
        // the chunk read and the next state read.
        fake.commit_after_chunks = Some((root, 1));
        let (end, frames) = run_fake(&mut fake, root, Cursor::default()).await;
        assert_eq!(end, StreamEnd::Final);
        assert_eq!(
            frames,
            vec![progress(0, 0), progress(0, 1), Frame::Result(json!("done"))]
        );
    }

    // SAC-12
    #[tokio::test(start_paused = true)]
    async fn producer_ends_with_pending_at_the_stream_budget() {
        let mut fake = Fake::new(pending());
        let root = fake.run(false, 2, None);
        let mut sink = Recorder::default();
        let until = Instant::now() + Duration::from_secs(12);
        let end = produce(&mut fake, &mut sink, root, Cursor::default(), 1024, until).await;
        assert_eq!(end, StreamEnd::Pending);
        assert_eq!(
            sink.frames,
            vec![progress(0, 0), progress(0, 1), Frame::Pending]
        );
        assert!(Instant::now() >= until, "the stream waited out its budget");
        assert!(fake.wakes >= 1);
        // A zero budget sends the stored chunks and never waits.
        let mut fake = Fake::new(pending());
        let root = fake.run(false, 1, None);
        let mut sink = Recorder::default();
        let end = produce(
            &mut fake,
            &mut sink,
            root,
            Cursor::default(),
            1024,
            Instant::now(),
        )
        .await;
        assert_eq!(end, StreamEnd::Pending);
        assert_eq!(sink.frames, vec![progress(0, 0), Frame::Pending]);
        assert_eq!(fake.wakes, 0);
    }

    // SAC-13
    #[tokio::test(start_paused = true)]
    async fn producer_stops_at_the_step_cap_with_result_unavailable() {
        let mut fake = Fake::new(completed(json!("done")));
        let (a, _, _) = three_runs(&mut fake);
        let mut sink = Recorder::default();
        let end = produce(&mut fake, &mut sink, a, Cursor::default(), 1, far()).await;
        assert_eq!(end, StreamEnd::Final);
        assert_eq!(sink.frames.last(), Some(&unavailable_frame()));
        assert!(
            !sink.frames.contains(&progress(2, 0)),
            "no frame past the cap"
        );
        // Two steps reach the end.
        let mut sink = Recorder::default();
        produce(&mut fake, &mut sink, a, Cursor::default(), 2, far()).await;
        assert_eq!(sink.frames.last(), Some(&Frame::Result(json!("done"))));
    }

    // SAC-13
    #[tokio::test(start_paused = true)]
    async fn producer_stops_at_a_cycle_with_result_unavailable() {
        let mut fake = Fake::new(completed(json!("done")));
        let a = fake.run(true, 0, None);
        let b = fake.run(true, 0, Some((a, Cause::ContinuedAsNew)));
        fake.runs.get_mut(&a).expect("a").next = Some((b, Cause::Retry));
        let (end, frames) = run_fake(&mut fake, a, Cursor::default()).await;
        assert_eq!(end, StreamEnd::Final);
        assert_eq!(frames.last(), Some(&unavailable_frame()));
        // A cycle in the walk to the cursor stops the same way.
        let (_, frames) = run_fake(&mut fake, a, at(5, None)).await;
        assert_eq!(frames, vec![unavailable_frame()]);
    }

    // SAC-14
    #[tokio::test(start_paused = true)]
    async fn producer_retries_a_failed_read_and_never_drops_a_chunk() {
        for failing in 1..=6 {
            let mut fake = Fake::new(completed(json!("done")));
            let (a, _, _) = three_runs(&mut fake);
            fake.failing_reads = failing;
            let (end, frames) = run_fake(&mut fake, a, Cursor::default()).await;
            assert_eq!(end, StreamEnd::Final, "{failing} failing reads");
            assert_eq!(frames, lineage_frames(), "{failing} failing reads");
            assert_eq!(fake.wakes, failing, "each failed read waits one tick");
        }
    }

    // SAC-15
    #[tokio::test(start_paused = true)]
    async fn producer_sends_the_problem_frame_for_a_failed_run() {
        let failed = DurableOutcome {
            kind: DurableOutcomeKind::Failed {
                error_type: Some("Refused".to_owned()),
                details: Some(json!({"why": "policy"})),
            },
            status: StatusCode::UNPROCESSABLE_ENTITY,
            execution_id: Some(ExecutionId::new()),
            replayed: true,
            expose_id: false,
            output: None,
            retry_after: None,
        };
        let mut fake = Fake::new(failed);
        let root = fake.run(true, 1, None);
        let (_, frames) = run_fake(&mut fake, root, Cursor::default()).await;
        let Some(Frame::Problem(body)) = frames.last() else {
            panic!("a problem frame ends the stream: {frames:?}");
        };
        assert_eq!(body["status"], 422);
        assert_eq!(body["type"], "urn:autumn-harvest:durable-route:failed");
        assert_eq!(body["error_type"], "Refused");
        assert_eq!(body["details"], json!({"why": "policy"}));
    }

    // SAC-16
    #[tokio::test(start_paused = true)]
    async fn producer_stops_when_the_client_is_gone() {
        let mut fake = Fake::new(pending());
        let root = fake.run(false, 10, None);
        let mut sink = Recorder {
            close_after: Some(3),
            ..Recorder::default()
        };
        let end = produce(&mut fake, &mut sink, root, Cursor::default(), 1024, far()).await;
        assert_eq!(end, StreamEnd::ClientGone);
        assert_eq!(sink.frames.len(), 3);
        assert_eq!(fake.wakes, 0, "a gone client is not waited for");
    }

    // SAC-17
    #[tokio::test(start_paused = true)]
    async fn producer_pages_large_streams() {
        let mut fake = Fake::new(completed(json!(0)));
        let root = fake.run(true, 600, None);
        let (_, frames) = run_fake(&mut fake, root, Cursor::default()).await;
        let offsets: Vec<i64> = frames
            .iter()
            .filter_map(|f| match f {
                Frame::Progress { offset, .. } => Some(*offset),
                _ => None,
            })
            .collect();
        assert_eq!(offsets, (0..600).collect::<Vec<_>>());
    }

    // SAC-18
    #[tokio::test(start_paused = true)]
    async fn producer_waits_only_while_the_run_is_live() {
        let mut fake = Fake::new(completed(json!("done")));
        let (a, _, _) = three_runs(&mut fake);
        run_fake(&mut fake, a, Cursor::default()).await;
        assert_eq!(fake.wakes, 0, "an ended lineage never waits");

        // A live run waits, then ends on a later wake.
        let mut fake = Fake::new(completed(json!("done")));
        let root = fake.run(false, 1, None);
        fake.scripts.push_back(Box::new(move |runs| {
            let run = runs.get_mut(&root).expect("root");
            run.chunks.push((1, json!({"o": 1})));
            run.ended = true;
        }));
        let (end, frames) = run_fake(&mut fake, root, Cursor::default()).await;
        assert_eq!(end, StreamEnd::Final);
        assert_eq!(fake.wakes, 1);
        assert_eq!(
            frames,
            vec![progress(0, 0), progress(0, 1), Frame::Result(json!("done"))]
        );
    }

    // SAC-19
    #[tokio::test]
    async fn final_frames_reuse_the_fixed_problem_text() {
        async fn body_of(outcome: DurableOutcome) -> Value {
            let bytes = axum::body::to_bytes(outcome.into_response().into_body(), usize::MAX)
                .await
                .expect("body");
            serde_json::from_slice(&bytes).expect("json")
        }
        let cases = [
            DurableOutcome {
                kind: DurableOutcomeKind::Failed {
                    error_type: Some("Refused".to_owned()),
                    details: None,
                },
                status: StatusCode::FORBIDDEN,
                execution_id: Some(ExecutionId::new()),
                replayed: false,
                expose_id: false,
                output: None,
                retry_after: None,
            },
            DurableOutcome {
                kind: DurableOutcomeKind::Erased,
                status: StatusCode::GONE,
                execution_id: Some(ExecutionId::new()),
                replayed: false,
                expose_id: false,
                output: None,
                retry_after: None,
            },
            DurableOutcome {
                kind: DurableOutcomeKind::Problem {
                    slug: "result-unavailable",
                },
                status: StatusCode::INTERNAL_SERVER_ERROR,
                execution_id: Some(ExecutionId::new()),
                replayed: false,
                expose_id: false,
                output: None,
                retry_after: None,
            },
        ];
        for outcome in cases {
            let frame = final_frame(&outcome).expect("final");
            let Frame::Problem(body) = frame else {
                panic!("a problem frame");
            };
            assert_eq!(body, body_of(outcome).await);
        }
        assert_eq!(
            final_frame(&completed(json!({"a": 1}))),
            Some(Frame::Result(json!({"a": 1})))
        );
        let mut empty = completed(Value::Null);
        empty.output = None;
        assert_eq!(final_frame(&empty), Some(Frame::Result(Value::Null)));
        assert_eq!(final_frame(&pending()), None);
        assert_eq!(unavailable_frame(), {
            let Some(frame) = final_frame(&DurableOutcome {
                kind: DurableOutcomeKind::Problem {
                    slug: "result-unavailable",
                },
                status: StatusCode::INTERNAL_SERVER_ERROR,
                execution_id: None,
                replayed: false,
                expose_id: false,
                output: None,
                retry_after: None,
            }) else {
                panic!("final");
            };
            frame
        });
    }

    // SAC-20
    #[test]
    fn stream_limits_have_defaults_and_bounds() {
        assert_eq!(DEFAULT_MAX_STREAMS, 32);
        assert_eq!(DEFAULT_STREAM_FOR, Duration::from_secs(300));
        assert_eq!(MAX_STREAM_FOR, Duration::from_secs(3600));
        assert_eq!(
            stream_budget(Duration::from_secs(10)),
            Duration::from_secs(10)
        );
        assert_eq!(stream_budget(Duration::from_secs(7200)), MAX_STREAM_FOR);
        assert_eq!(stream_budget(Duration::ZERO), Duration::ZERO);
    }

    // SAC-20
    #[tokio::test]
    async fn a_call_uses_the_default_stream_budget() {
        let call = extract(&[]).await.scope("u").call("w", 1);
        assert_eq!(call.stream_for, DEFAULT_STREAM_FOR);
        let call = call.stream_for(Duration::from_secs(9));
        assert_eq!(call.stream_for, Duration::from_secs(9));
    }

    // SAC-21
    #[tokio::test]
    async fn stream_response_headers_disable_proxy_buffering() {
        let exec_id = ExecutionId::new();
        let (_tx, rx) = futures::channel::mpsc::channel(1);
        let response = stream_response(rx, true, Some(exec_id));
        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers();
        assert_eq!(
            headers.get(header::CONTENT_TYPE).unwrap(),
            "text/event-stream"
        );
        assert_eq!(
            headers.get(header::CACHE_CONTROL).unwrap(),
            "no-cache, no-store, private"
        );
        assert_eq!(headers.get("x-accel-buffering").unwrap(), "no");
        assert_eq!(headers.get(super::super::REPLAYED_HEADER).unwrap(), "true");
        assert_eq!(
            headers.get(crate::api::HEADER_EXECUTION_ID).unwrap(),
            exec_id.to_string().as_str()
        );
        let (_tx, rx) = futures::channel::mpsc::channel(1);
        let response = stream_response(rx, false, None);
        assert!(
            response
                .headers()
                .get(super::super::REPLAYED_HEADER)
                .is_none()
        );
        assert!(
            response
                .headers()
                .get(crate::api::HEADER_EXECUTION_ID)
                .is_none()
        );
    }

    // SAC-45
    #[tokio::test(start_paused = true)]
    async fn the_budget_ends_a_slow_drain_with_pending() {
        let mut fake = Fake::new(completed(json!("done")));
        let root = fake.run(true, 600, None);
        let mut sink = Recorder {
            delay: Duration::from_secs(50),
            ..Recorder::default()
        };
        let until = Instant::now() + Duration::from_secs(120);
        let end = produce(&mut fake, &mut sink, root, Cursor::default(), 1024, until).await;
        assert_eq!(end, StreamEnd::Pending);
        // Three sends start before the budget ends. Then the drain stops.
        assert_eq!(
            sink.frames,
            vec![
                progress(0, 0),
                progress(0, 1),
                progress(0, 2),
                Frame::Pending
            ]
        );
        // The overrun is at most the send in flight and the pending frame.
        assert!(Instant::now() <= until + Duration::from_secs(100));
        // A drain that ends after the deadline sends no final frame.
        let mut fake = Fake::new(completed(json!("done")));
        let root = fake.run(true, 3, None);
        let mut sink = Recorder {
            delay: Duration::from_secs(50),
            ..Recorder::default()
        };
        let until = Instant::now() + Duration::from_secs(120);
        let end = produce(&mut fake, &mut sink, root, Cursor::default(), 1024, until).await;
        assert_eq!(end, StreamEnd::Pending);
        assert_eq!(
            sink.frames,
            vec![
                progress(0, 0),
                progress(0, 1),
                progress(0, 2),
                Frame::Pending
            ]
        );
        // A read that crosses the deadline is cut at the deadline.
        let mut fake = Fake::new(completed(json!("done")));
        let root = fake.run(true, 1, None);
        fake.read_delay = Duration::from_secs(60);
        let started = Instant::now();
        let mut sink = Recorder::default();
        let end = produce(&mut fake, &mut sink, root, Cursor::default(), 1024, started).await;
        assert_eq!(end, StreamEnd::Pending);
        assert_eq!(sink.frames, vec![Frame::Pending]);
        assert_eq!(
            started.elapsed(),
            DRAIN_FLOOR,
            "the read ends at the deadline"
        );
        // A slow read after the drain crosses the deadline. The final frame
        // then becomes `pending`.
        let mut fake = Fake::new(completed(json!("done")));
        let root = fake.run(true, 1, None);
        fake.read_delay = Duration::from_secs(4);
        let mut sink = Recorder::default();
        let until = Instant::now() + Duration::from_secs(6);
        let end = produce(&mut fake, &mut sink, root, Cursor::default(), 1024, until).await;
        assert_eq!(end, StreamEnd::Pending);
        assert_eq!(sink.frames, vec![progress(0, 0), Frame::Pending]);
    }

    // SAC-49
    #[tokio::test(start_paused = true)]
    async fn the_budget_ends_a_long_walk_with_pending() {
        // Fifty ended runs, each read in 1 s, with a zero budget.
        let mut fake = Fake::new(completed(json!("done")));
        let mut next = None;
        for _ in 0..50 {
            next = Some((fake.run(true, 1, next), Cause::ContinuedAsNew));
        }
        let (root, _) = next.expect("a root");
        fake.read_delay = Duration::from_secs(1);
        let started = Instant::now();
        let mut sink = Recorder::default();
        let end = produce(&mut fake, &mut sink, root, at(49, None), 1024, started).await;
        assert_eq!(end, StreamEnd::Pending);
        assert_eq!(sink.frames, vec![Frame::Pending]);
        // The walk stops at the drain floor, far from the end of the lineage.
        assert!(started.elapsed() <= DRAIN_FLOOR + Duration::from_secs(1));
        // The same budget ends a main-loop walk over ended runs.
        let mut sink = Recorder::default();
        let started = Instant::now();
        let end = produce(&mut fake, &mut sink, root, Cursor::default(), 1024, started).await;
        assert_eq!(end, StreamEnd::Pending);
        assert_eq!(sink.frames.last(), Some(&Frame::Pending));
        assert!(started.elapsed() <= DRAIN_FLOOR + Duration::from_secs(1));
    }

    // SAC-46
    #[tokio::test(start_paused = true)]
    async fn the_walk_to_the_cursor_handles_each_branch() {
        // A failed read while it walks waits one tick and reads again.
        let mut fake = Fake::new(completed(json!("done")));
        let (a, _, _) = three_runs(&mut fake);
        fake.failing_reads = 2;
        let (_, frames) = run_fake(&mut fake, a, at(2, None)).await;
        assert_eq!(frames, vec![progress(2, 0), Frame::Result(json!("done"))]);
        assert_eq!(fake.wakes, 2);
        // The step cap stops the walk to the cursor.
        let mut fake = Fake::new(completed(json!("done")));
        let (a, _, _) = three_runs(&mut fake);
        let mut sink = Recorder::default();
        produce(&mut fake, &mut sink, a, at(2, None), 1, far()).await;
        assert_eq!(sink.frames, vec![unavailable_frame()]);
        // A cursor ahead of a live last run waits for the next run.
        let mut fake = Fake::new(completed(json!("done")));
        let b = fake.run(true, 1, None);
        let a = fake.run(false, 1, None);
        fake.scripts.push_back(Box::new(move |runs| {
            let run = runs.get_mut(&a).expect("a");
            run.ended = true;
            run.next = Some((b, Cause::Retry));
        }));
        let (_, frames) = run_fake(&mut fake, a, at(1, None)).await;
        assert_eq!(frames, vec![progress(1, 0), Frame::Result(json!("done"))]);
        // The budget ends a walk that waits.
        let mut fake = Fake::new(pending());
        let a = fake.run(false, 1, None);
        let mut sink = Recorder::default();
        let until = Instant::now() + Duration::from_secs(3);
        let end = produce(&mut fake, &mut sink, a, at(4, None), 1024, until).await;
        assert_eq!(end, StreamEnd::Pending);
        assert_eq!(sink.frames, vec![Frame::Pending]);
        // A gone client ends the walk.
        let mut fake = Fake::new(completed(json!("done")));
        let (a, _, _) = three_runs(&mut fake);
        let mut sink = Recorder {
            close_after: Some(0),
            ..Recorder::default()
        };
        let end = produce(&mut fake, &mut sink, a, at(2, None), 1024, far()).await;
        assert_eq!(end, StreamEnd::ClientGone);
    }

    // SAC-47
    #[tokio::test(start_paused = true)]
    async fn the_main_loop_handles_each_branch() {
        // A pending outcome after the end reads again at the next tick.
        let mut fake = Fake::new(completed(json!("done")));
        let root = fake.run(true, 1, None);
        fake.pending_outcomes = 2;
        let (end, frames) = run_fake(&mut fake, root, Cursor::default()).await;
        assert_eq!(end, StreamEnd::Final);
        assert_eq!(frames, vec![progress(0, 0), Frame::Result(json!("done"))]);
        assert_eq!(fake.wakes, 2);
        // A gone client at an attempt frame ends the stream.
        let mut fake = Fake::new(completed(json!("done")));
        let (a, _, _) = three_runs(&mut fake);
        let mut sink = Recorder {
            close_after: Some(2),
            ..Recorder::default()
        };
        let end = produce(&mut fake, &mut sink, a, Cursor::default(), 1024, far()).await;
        assert_eq!(end, StreamEnd::ClientGone);
        assert_eq!(sink.frames, vec![progress(0, 0), progress(0, 1)]);
        // A gone client at the final frame ends the stream.
        let mut fake = Fake::new(completed(json!("done")));
        let root = fake.run(true, 1, None);
        let mut sink = Recorder {
            close_after: Some(1),
            ..Recorder::default()
        };
        let end = produce(&mut fake, &mut sink, root, Cursor::default(), 1024, far()).await;
        assert_eq!(end, StreamEnd::ClientGone);
        // A run whose row is gone ends with `result-unavailable`.
        let mut fake = Fake::new(completed(json!("done")));
        let root = ExecutionId::new();
        let (end, frames) = run_fake(&mut fake, root, Cursor::default()).await;
        assert_eq!(end, StreamEnd::Final);
        assert_eq!(frames, vec![unavailable_frame()]);
        // A page boundary: exactly one and two full pages.
        for count in [PAGE, 2 * PAGE] {
            let mut fake = Fake::new(completed(json!(0)));
            let root = fake.run(true, count, None);
            let (_, frames) = run_fake(&mut fake, root, Cursor::default()).await;
            assert_eq!(
                frames.len(),
                usize::try_from(count).expect("count") + 1,
                "{count} chunks"
            );
        }
    }

    // SAC-48
    #[tokio::test]
    async fn only_a_true_end_streams_without_listen() {
        assert_eq!(stream_start(&DurableOutcomeKind::Pending), Start::Live);
        for kind in [
            DurableOutcomeKind::Completed,
            DurableOutcomeKind::Failed {
                error_type: None,
                details: None,
            },
            DurableOutcomeKind::Cancelled,
            DurableOutcomeKind::Terminated,
            DurableOutcomeKind::TimedOut,
        ] {
            assert_eq!(stream_start(&kind), Start::Ended, "{kind:?}");
        }
        for kind in [
            DurableOutcomeKind::Erased,
            DurableOutcomeKind::Problem {
                slug: "result-unavailable",
            },
        ] {
            assert_eq!(stream_start(&kind), Start::Plain, "{kind:?}");
        }
        // The span outcome of an open stream is `stream`.
        let (_tx, rx) = futures::channel::mpsc::channel(1);
        let open = DurableStream {
            answer: Answer::Stream(stream_response(rx, false, None)),
            execution_id: Some(ExecutionId::new()),
            replayed: true,
        };
        assert_eq!(open.slug(), "stream");
        assert!(open.is_stream());
        assert!(open.replayed());
        assert!(open.execution_id().is_some());
        let plain = DurableStream::outcome(pending());
        assert_eq!(plain.slug(), "pending");
        assert_eq!(plain.execution_id(), None);
    }

    // SAC-44
    #[tokio::test(start_paused = true)]
    async fn stream_logs_hold_no_key_scope_input_or_chunk() {
        use crate::durable_route::tests::{Leaky, capture};
        let (captured, dispatch) = capture();
        let guard = tracing::dispatcher::set_default(&dispatch);
        let key = (IDEMPOTENCY_KEY_HEADER, b"secret-key-44".as_slice());
        let mut layered = extract(&[key]).await;
        layered.layer_active = true;
        let refused = layered
            .scope("secret-scope-44")
            .call("answer", json!({"note": "secret-input-44"}))
            .stream()
            .await;
        assert_eq!(refused.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let cursor = extract(&[key, (LAST_EVENT_ID_HEADER, b"secret-cursor".as_slice())])
            .await
            .scope("secret-scope-44")
            .call("answer", Leaky("secret-input-44"))
            .stream()
            .await;
        assert_eq!(cursor.status(), StatusCode::BAD_REQUEST);
        let leaky = extract(&[key])
            .await
            .scope("secret-scope-44")
            .call("answer", Leaky("secret-input-44"))
            .stream()
            .await;
        assert_eq!(leaky.status(), StatusCode::INTERNAL_SERVER_ERROR);
        // A codec error can quote payload bytes. A chunk can hold anything.
        let mut fake = Fake::new(completed(json!("secret-output-44")));
        let root = fake.run(true, 0, None);
        fake.runs.get_mut(&root).expect("root").chunks = vec![(0, json!("secret-chunk-44"))];
        fake.failing_reads = 2;
        fake.read_error = || HarvestError::Codec("secret-codec-44".to_owned());
        let (end, _) = run_fake(&mut fake, root, Cursor::default()).await;
        assert_eq!(end, StreamEnd::Final);
        drop(guard);
        let text = captured.text();
        assert!(text.contains("harvest.durable_route{"), "{text}");
        assert!(text.contains("workflow=answer"), "{text}");
        assert!(text.contains("outcome=\"misconfigured\""), "{text}");
        assert!(text.contains("outcome=\"stream-cursor-invalid\""), "{text}");
        assert!(text.contains("payload codec error"), "{text}");
        for secret in [
            "secret-key-44",
            "secret-scope-44",
            "secret-input-44",
            "secret-cursor",
            "secret-chunk-44",
            "secret-codec-44",
            "secret-output-44",
        ] {
            assert!(!text.contains(secret), "{secret} in {text}");
        }
    }

    // SAC-43
    #[tokio::test]
    async fn frames_render_the_documented_wire_form() {
        let frames = [
            progress(1, 7),
            Frame::Attempt {
                attempt: 2,
                cause: Cause::ContinuedAsNew,
            },
            Frame::Result(json!({"a": 1})),
            Frame::Problem(json!({"status": 500})),
            Frame::Pending,
        ];
        let events: Vec<Result<Event, Infallible>> = frames.iter().map(|f| Ok(f.event())).collect();
        let body = axum::response::sse::Sse::new(futures::stream::iter(events)).into_response();
        let bytes = axum::body::to_bytes(body.into_body(), usize::MAX)
            .await
            .expect("body");
        let text = String::from_utf8(bytes.to_vec()).expect("utf-8");
        for needle in [
            "event: progress\ndata: {\"o\":7}\nid: 1.7\n\n",
            "event: attempt\ndata: {\"attempt\":2,\"cause\":\"continued-as-new\"}\nid: 2\n\n",
            "event: result\ndata: {\"a\":1}\n\n",
            "event: problem\ndata: {\"status\":500}\n\n",
            "event: pending\ndata: {\"state\":\"pending\"}\nretry: 1000\n\n",
        ] {
            assert!(text.contains(needle), "{needle:?} not in {text:?}");
        }
        assert_eq!(Cause::Retry.as_str(), "retry");
    }
}
