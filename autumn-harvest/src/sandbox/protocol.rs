//! The wire protocol between the host and the guest agent.
//!
//! Each message is one frame: a 4-byte big-endian length, then that many
//! bytes of UTF-8 JSON. The reader checks the length against its cap before
//! it reads the body, and it never allocates the declared length up front.
//! A hostile guest therefore cannot make the host allocate more than the cap.
//!
//! Each JSON body is an [`Envelope`] with a protocol version. The host sends
//! [`HostMessage`] values, and the guest answers with [`GuestMessage`]
//! values. One connection carries one exchange.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::outcome::SandboxOutcome;
use super::request::ExecPlan;

/// The protocol version that this build speaks.
pub const PROTOCOL_VERSION: u32 = 1;

/// The largest frame that either side accepts, whatever its own cap.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// The largest frame of a ping or a pong.
pub const MAX_CONTROL_FRAME_BYTES: usize = 4096;

/// A versioned message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope<T> {
    /// The protocol version of the sender.
    pub version: u32,
    /// The message.
    pub message: T,
}

/// A message from the host to the guest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostMessage {
    /// Ask whether the guest agent is ready.
    Ping,
    /// Run a plan. Boxed, because a plan is large and a ping is empty.
    Run(Box<RunMessage>),
}

/// The body of [`HostMessage::Run`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunMessage {
    /// A fresh id for this run. The guest echoes it.
    pub run_id: String,
    /// 32 bytes of host entropy, as lowercase hex. The guest mixes them into
    /// its kernel random pool before the code runs, so two microVMs restored
    /// from one snapshot do not share random state.
    pub entropy: String,
    /// The host wall clock, in milliseconds since the Unix epoch. The guest
    /// sets its clock from it, because a microVM restored from a snapshot
    /// starts with the clock of the snapshot.
    pub unix_time_ms: u64,
    /// The plan to run.
    pub plan: ExecPlan,
}

/// A message from the guest to the host.
///
/// The host decodes it from untrusted bytes. A derived tagged decoder first
/// copies the whole message into a tree, which can cost many times the frame
/// size. The decoder here reads each field as it comes, in any order, and
/// skips an unknown field without a copy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GuestMessage {
    /// The guest agent is ready.
    Pong,
    /// The outcome of a run.
    Outcome(OutcomeMessage),
    /// The guest could not run the plan.
    Error(GuestErrorMessage),
}

/// The fields of a guest message.
#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum GuestField {
    Type,
    RunId,
    Outcome,
    Kind,
    Message,
    #[serde(other)]
    Other,
}

impl<'de> Deserialize<'de> for GuestMessage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(GuestMessageVisitor)
    }
}

/// Reads a guest message field by field.
struct GuestMessageVisitor;

/// The fields of a guest message, before the type picks the variant.
#[derive(Default)]
struct GuestFields {
    kind: Option<String>,
    run_id: Option<String>,
    outcome: Option<SandboxOutcome>,
    error_kind: Option<GuestErrorKind>,
    message: Option<String>,
}

impl<'de> serde::de::Visitor<'de> for GuestMessageVisitor {
    type Value = GuestMessage;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a guest message object")
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<GuestMessage, A::Error> {
        use serde::de::Error as _;
        let mut fields = GuestFields::default();
        while let Some(field) = map.next_key::<GuestField>()? {
            match field {
                GuestField::Type => put(&mut fields.kind, map.next_value()?, "type")?,
                GuestField::RunId => put(&mut fields.run_id, map.next_value()?, "run_id")?,
                GuestField::Outcome => put(&mut fields.outcome, map.next_value()?, "outcome")?,
                GuestField::Kind => put(&mut fields.error_kind, map.next_value()?, "kind")?,
                GuestField::Message => put(&mut fields.message, map.next_value()?, "message")?,
                GuestField::Other => {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }
        let GuestFields {
            kind,
            run_id,
            outcome,
            error_kind,
            message,
        } = fields;
        match (kind.as_deref(), run_id, outcome, error_kind, message) {
            (Some("pong"), None, None, None, None) => Ok(GuestMessage::Pong),
            (Some("outcome"), Some(run_id), Some(outcome), None, None) => {
                Ok(GuestMessage::Outcome(OutcomeMessage { run_id, outcome }))
            }
            (Some("error"), Some(run_id), None, Some(kind), Some(message)) => {
                Ok(GuestMessage::Error(GuestErrorMessage {
                    run_id,
                    kind,
                    message,
                }))
            }
            (None, ..) => Err(A::Error::missing_field("type")),
            (Some(kind @ ("pong" | "outcome" | "error")), ..) => Err(A::Error::custom(format!(
                "a guest {kind} message has the wrong fields"
            ))),
            (Some(other), ..) => Err(A::Error::unknown_variant(
                other,
                &["pong", "outcome", "error"],
            )),
        }
    }
}

/// Store the first value of a field. A second one is an error.
pub(crate) fn put<T, E: serde::de::Error>(
    slot: &mut Option<T>,
    value: T,
    name: &'static str,
) -> Result<(), E> {
    if slot.is_some() {
        return Err(E::duplicate_field(name));
    }
    *slot = Some(value);
    Ok(())
}

/// The body of [`GuestMessage::Outcome`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeMessage {
    /// The id from the run message.
    pub run_id: String,
    /// The outcome.
    pub outcome: SandboxOutcome,
}

/// Why the guest could not run a plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuestErrorKind {
    /// The plan failed the guest check.
    InvalidPlan,
    /// The command of the runtime could not start.
    SpawnFailed,
    /// The guest failed for another reason.
    Internal,
}

/// The body of [`GuestMessage::Error`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuestErrorMessage {
    /// The id from the run message.
    pub run_id: String,
    /// The class of the error.
    pub kind: GuestErrorKind,
    /// A description for the operator.
    pub message: String,
}

/// Why a frame could not be read or written.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// The peer closed the stream before a frame started.
    #[error("the peer closed the stream")]
    Closed,
    /// The declared length is over the cap. The reader read no body.
    #[error("frame of {declared} bytes exceeds the {max}-byte cap")]
    TooLarge {
        /// The length that the peer declared.
        declared: u64,
        /// The cap of the reader.
        max: usize,
    },
    /// The body is not the expected JSON, or the version does not match.
    #[error("malformed frame: {0}")]
    Malformed(String),
    /// The stream failed.
    #[error("frame i/o failed: {0}")]
    Io(#[from] std::io::Error),
}

/// Write `message` as one frame.
///
/// # Errors
///
/// Returns [`FrameError::TooLarge`] when the JSON is over `max` bytes, or
/// the I/O error of the write.
pub async fn write_frame<W, T>(writer: &mut W, message: &T, max: usize) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin + Send,
    T: Serialize + Sync,
{
    let body = serde_json::to_vec(message)
        .map_err(|e| FrameError::Malformed(format!("cannot encode the frame: {e}")))?;
    let cap = max.min(MAX_FRAME_BYTES);
    let declared = u32::try_from(body.len())
        .ok()
        .filter(|_| body.len() <= cap)
        .ok_or_else(|| FrameError::TooLarge {
            declared: u64::try_from(body.len()).unwrap_or(u64::MAX),
            max: cap,
        })?;
    // One buffer and one write, so a reader never sees a header alone.
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&declared.to_be_bytes());
    frame.extend_from_slice(&body);
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

/// Read one frame and decode it as `T`.
///
/// # Errors
///
/// - [`FrameError::Closed`]: a clean end of stream.
/// - [`FrameError::TooLarge`]: an oversized body. The reader reads none of
///   it.
/// - [`FrameError::Malformed`]: a body that is not valid.
/// - [`FrameError::Io`]: the stream failed.
pub async fn read_frame<R, T>(reader: &mut R, max: usize) -> Result<T, FrameError>
where
    R: AsyncRead + Unpin + Send,
    T: DeserializeOwned,
{
    let mut header = [0u8; 4];
    let first = reader.read(&mut header).await?;
    if first == 0 {
        return Err(FrameError::Closed);
    }
    reader.read_exact(&mut header[first..]).await?;
    let declared = u32::from_be_bytes(header);
    let cap = max.min(MAX_FRAME_BYTES);
    let length = usize::try_from(declared)
        .ok()
        .filter(|length| *length <= cap)
        .ok_or_else(|| FrameError::TooLarge {
            declared: u64::from(declared),
            max: cap,
        })?;
    // Grow the buffer as bytes arrive. A peer that declares a large frame
    // and sends less never gets the host to allocate the declared size.
    let mut body = Vec::new();
    reader
        .take(u64::from(declared))
        .read_to_end(&mut body)
        .await?;
    if body.len() != length {
        return Err(FrameError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "the frame declared {length} bytes and carried {}",
                body.len()
            ),
        )));
    }
    serde_json::from_slice(&body).map_err(|e| FrameError::Malformed(e.to_string()))
}

/// Wrap `message` in an envelope of this protocol version.
#[must_use]
pub const fn envelope<T>(message: T) -> Envelope<T> {
    Envelope {
        version: PROTOCOL_VERSION,
        message,
    }
}

/// Read one enveloped message and check its version.
///
/// # Errors
///
/// Returns the errors of [`read_frame`], and [`FrameError::Malformed`] for a
/// version that is not [`PROTOCOL_VERSION`].
pub async fn read_message<R, T>(reader: &mut R, max: usize) -> Result<T, FrameError>
where
    R: AsyncRead + Unpin + Send,
    T: DeserializeOwned,
{
    let envelope: Envelope<T> = read_frame(reader, max).await?;
    if envelope.version != PROTOCOL_VERSION {
        return Err(FrameError::Malformed(format!(
            "protocol version {} is not the supported version {PROTOCOL_VERSION}",
            envelope.version
        )));
    }
    Ok(envelope.message)
}

/// Write one enveloped message.
///
/// # Errors
///
/// Returns the errors of [`write_frame`].
pub async fn write_message<W, T>(writer: &mut W, message: T, max: usize) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin + Send,
    T: Serialize + Send + Sync,
{
    write_frame(writer, &envelope(message), max).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::sandbox::policy::SandboxPolicy;
    use crate::sandbox::request::SandboxRequest;

    async fn round_trip<T>(message: &T) -> T
    where
        T: Serialize + DeserializeOwned + Sync,
    {
        let mut buf = Vec::new();
        write_frame(&mut buf, message, MAX_FRAME_BYTES)
            .await
            .unwrap();
        read_frame(&mut buf.as_slice(), MAX_FRAME_BYTES)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_frame_is_a_big_endian_length_and_json() {
        let mut buf = Vec::new();
        write_frame(&mut buf, &serde_json::json!({"a": 1}), 1024)
            .await
            .unwrap();
        assert_eq!(&buf[..4], &[0, 0, 0, 7]);
        assert_eq!(&buf[4..], br#"{"a":1}"#);
    }

    #[tokio::test]
    async fn messages_round_trip() {
        let plan = SandboxPolicy::default()
            .plan(&SandboxRequest::new("sh", "echo hi"))
            .unwrap();
        let run = HostMessage::Run(Box::new(RunMessage {
            run_id: "r1".into(),
            entropy: "00".repeat(32),
            unix_time_ms: 1_700_000_000_000,
            plan,
        }));
        assert_eq!(round_trip(&envelope(run.clone())).await.message, run);
        let ping = envelope(HostMessage::Ping);
        assert_eq!(
            serde_json::to_value(&ping).unwrap(),
            serde_json::json!({"version": 1, "message": {"type": "ping"}})
        );
        let error = GuestMessage::Error(GuestErrorMessage {
            run_id: "r1".into(),
            kind: GuestErrorKind::SpawnFailed,
            message: "python3: not found".into(),
        });
        assert_eq!(round_trip(&error).await, error);
    }

    #[tokio::test]
    async fn an_oversized_declared_length_is_rejected_before_the_body() {
        // The peer declares 4 GiB - 1 and sends no body. The reader must fail
        // at once from the header alone.
        let frame = [0xff, 0xff, 0xff, 0xff];
        let err = read_frame::<_, serde_json::Value>(&mut frame.as_slice(), 1024)
            .await
            .expect_err("over the cap");
        assert!(matches!(
            err,
            FrameError::TooLarge {
                declared: 4_294_967_295,
                max: 1024
            }
        ));
    }

    #[tokio::test]
    async fn the_global_cap_applies_when_the_caller_cap_is_larger() {
        let declared = u32::try_from(MAX_FRAME_BYTES + 1).unwrap().to_be_bytes();
        let err = read_frame::<_, serde_json::Value>(&mut declared.as_slice(), usize::MAX)
            .await
            .expect_err("over the global cap");
        assert!(matches!(err, FrameError::TooLarge { .. }));
    }

    #[tokio::test]
    async fn a_writer_refuses_a_frame_over_its_cap() {
        let mut buf = Vec::new();
        let err = write_frame(&mut buf, &"x".repeat(100), 10)
            .await
            .expect_err("over the cap");
        assert!(matches!(err, FrameError::TooLarge { .. }));
        assert!(buf.is_empty(), "nothing is written");
    }

    #[tokio::test]
    async fn a_short_body_is_an_io_error() {
        let mut frame = vec![0, 0, 0, 10];
        frame.extend_from_slice(b"{}");
        let err = read_frame::<_, serde_json::Value>(&mut frame.as_slice(), 1024)
            .await
            .expect_err("truncated");
        assert!(matches!(err, FrameError::Io(_)), "{err:?}");
    }

    #[tokio::test]
    async fn an_empty_stream_is_closed() {
        let err = read_frame::<_, serde_json::Value>(&mut [].as_slice(), 1024)
            .await
            .expect_err("closed");
        assert!(matches!(err, FrameError::Closed));
    }

    #[tokio::test]
    async fn a_partial_header_is_an_io_error() {
        let err = read_frame::<_, serde_json::Value>(&mut [0u8, 0].as_slice(), 1024)
            .await
            .expect_err("partial header");
        assert!(matches!(err, FrameError::Io(_)), "{err:?}");
    }

    #[tokio::test]
    async fn invalid_json_is_malformed() {
        let mut frame = vec![0, 0, 0, 3];
        frame.extend_from_slice(b"{x}");
        let err = read_frame::<_, serde_json::Value>(&mut frame.as_slice(), 1024)
            .await
            .expect_err("bad json");
        assert!(matches!(err, FrameError::Malformed(_)));
    }

    #[tokio::test]
    async fn deeply_nested_json_is_malformed_not_a_stack_overflow() {
        let body = "[".repeat(100_000);
        let mut frame = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        frame.extend_from_slice(body.as_bytes());
        let err = read_frame::<_, serde_json::Value>(&mut frame.as_slice(), 1 << 20)
            .await
            .expect_err("recursion limit");
        assert!(matches!(err, FrameError::Malformed(_)));
    }

    #[tokio::test]
    async fn a_wrong_version_is_malformed() {
        let mut buf = Vec::new();
        let other = Envelope {
            version: PROTOCOL_VERSION + 1,
            message: HostMessage::Ping,
        };
        write_frame(&mut buf, &other, 1024).await.unwrap();
        let err = read_message::<_, HostMessage>(&mut buf.as_slice(), 1024)
            .await
            .expect_err("version");
        assert!(matches!(err, FrameError::Malformed(ref m) if m.contains("version")));
    }

    #[tokio::test]
    async fn an_unknown_message_type_is_malformed() {
        let mut buf = Vec::new();
        let raw = serde_json::json!({"version": 1, "message": {"type": "exec_shell"}});
        write_frame(&mut buf, &raw, 1024).await.unwrap();
        let err = read_message::<_, HostMessage>(&mut buf.as_slice(), 1024)
            .await
            .expect_err("unknown type");
        assert!(matches!(err, FrameError::Malformed(_)));
    }

    fn outcome_json() -> serde_json::Value {
        serde_json::to_value(crate::sandbox::outcome::tests::outcome(
            crate::sandbox::outcome::RunStatus::Exited { code: 0 },
        ))
        .unwrap()
    }

    #[test]
    fn a_guest_message_decodes_in_any_key_order_and_skips_unknown_fields() {
        let text = format!(
            r#"{{"outcome":{},"junk":[1,[2,{{"x":3}}]],"run_id":"r1","type":"outcome"}}"#,
            outcome_json()
        );
        let message: GuestMessage = serde_json::from_str(&text).unwrap();
        assert!(matches!(message, GuestMessage::Outcome(ref m) if m.run_id == "r1"));
        let error: GuestMessage = serde_json::from_str(
            r#"{"message":"no python3","kind":"spawn_failed","type":"error","run_id":"r1"}"#,
        )
        .unwrap();
        assert!(
            matches!(error, GuestMessage::Error(ref e) if e.kind == GuestErrorKind::SpawnFailed)
        );
        let pong: GuestMessage = serde_json::from_str(r#"{"type":"pong"}"#).unwrap();
        assert_eq!(pong, GuestMessage::Pong);
    }

    #[test]
    fn a_guest_message_with_the_wrong_fields_is_refused() {
        for text in [
            r#"{"type":"pong","run_id":"r1"}"#.to_owned(),
            r#"{"type":"outcome","run_id":"r1"}"#.to_owned(),
            r#"{"type":"error","run_id":"r1","kind":"internal"}"#.to_owned(),
            r#"{"type":"shell"}"#.to_owned(),
            r#"{"run_id":"r1"}"#.to_owned(),
            r#"{"type":"pong","type":"pong"}"#.to_owned(),
            format!(
                r#"{{"type":"error","run_id":"r1","kind":"internal","message":"m","outcome":{}}}"#,
                outcome_json()
            ),
        ] {
            assert!(
                serde_json::from_str::<GuestMessage>(&text).is_err(),
                "{text} must be refused"
            );
        }
    }

    #[test]
    fn the_run_status_decodes_in_the_key_order_of_jsonb() {
        use crate::sandbox::outcome::RunStatus;
        // Postgres `jsonb` sorts keys, so `code` comes before `kind`.
        let status: RunStatus = serde_json::from_str(r#"{"code":3,"kind":"exited"}"#).unwrap();
        assert_eq!(status, RunStatus::Exited { code: 3 });
        let status: RunStatus =
            serde_json::from_str(r#"{"kind":"signaled","later":[1,2],"signal":9}"#).unwrap();
        assert_eq!(status, RunStatus::Signaled { signal: 9 });
        let status: RunStatus = serde_json::from_str(r#"{"kind":"timed_out"}"#).unwrap();
        assert_eq!(status, RunStatus::TimedOut);
        for bad in [
            r#"{"kind":"exited"}"#,
            r#"{"kind":"timed_out","code":1}"#,
            r#"{"kind":"exploded"}"#,
            r#"{"code":1}"#,
        ] {
            assert!(serde_json::from_str::<RunStatus>(bad).is_err(), "{bad}");
        }
    }
}
