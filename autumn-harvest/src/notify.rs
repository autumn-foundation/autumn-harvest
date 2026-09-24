//! Postgres LISTEN/NOTIFY helpers for wake-on-enqueue.
//!
//! Instead of polling the task queue on a fixed interval, workers can subscribe
//! to a Postgres NOTIFY channel and wake immediately when a new task is enqueued.
//! This module provides the channel naming convention, the notification payload
//! type, and a [`QueueListener`] that wraps `tokio-postgres` for async LISTEN.

use std::time::Duration;

use diesel::sql_types::Text;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::error::{HarvestError, HarvestResult};

// ---------------------------------------------------------------------------
// Channel naming
// ---------------------------------------------------------------------------

/// Convert a queue name to its Postgres NOTIFY channel name.
///
/// The convention is `harvest_queue_{name}` with hyphens replaced by
/// underscores (Postgres identifiers cannot contain hyphens).
///
/// # Examples
///
/// ```
/// # use autumn_harvest::notify::queue_channel;
/// assert_eq!(queue_channel("email-queue"), "harvest_queue_email_queue");
/// ```
#[must_use]
pub fn queue_channel(queue_name: &str) -> String {
    format!("harvest_queue_{}", queue_name.replace('-', "_"))
}

/// Postgres NOTIFY channel used when workflow event history advances.
///
/// `store::append_events` sends this notification after inserting one or more
/// events, so embedders can LISTEN once and wake immediately when any workflow
/// changes state.
#[must_use]
pub const fn workflow_events_channel() -> &'static str {
    "harvest_events"
}

/// Postgres NOTIFY channel used for a single execution's ephemeral progress
/// stream (issue #791).
///
/// The convention is `harvest_progress_{exec_hex}` where `exec_hex` is the
/// execution UUID as 32 lowercase hex digits (no hyphens — Postgres identifiers
/// cannot contain them). The result is `17 + 32 = 49` characters, comfortably
/// within Postgres' 63-byte identifier limit.
///
/// Unlike [`workflow_events_channel`] (a single global channel fired on real
/// event appends), progress is per-execution so a subscriber LISTENs only to
/// the one run it is streaming and is never woken by unrelated workflows.
///
/// # Examples
///
/// ```
/// # use autumn_harvest::notify::workflow_progress_channel;
/// # use uuid::Uuid;
/// let id = Uuid::parse_str("0191c1a2-3b4c-7d5e-8f60-112233445566").unwrap();
/// assert_eq!(
///     workflow_progress_channel(id),
///     "harvest_progress_0191c1a23b4c7d5e8f60112233445566"
/// );
/// ```
#[must_use]
pub fn workflow_progress_channel(exec_id: Uuid) -> String {
    format!("harvest_progress_{}", exec_id.simple())
}

#[must_use]
fn quote_pg_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// NotifyPayload
// ---------------------------------------------------------------------------

/// Payload sent via Postgres NOTIFY when a task is enqueued.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NotifyPayload {
    /// The UUID of the newly enqueued task.
    pub task_id: Uuid,
}

/// Payload sent on [`workflow_events_channel`] after events are appended.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowEventNotifyPayload {
    /// Workflow execution whose history advanced.
    pub workflow_exec_id: Uuid,
    /// Number of events appended in this write.
    pub event_count: usize,
    /// Type name of the last appended event.
    pub last_event_type: String,
}

/// Payload sent on [`workflow_progress_channel`] for each published progress
/// chunk (issue #791).
///
/// This is the wire envelope carried in the Postgres `NOTIFY` payload. The
/// whole envelope must fit within Postgres' 8000-byte `NOTIFY` limit; the
/// `chunk` is size-capped by the context (see
/// [`PROGRESS_CHUNK_MAX_BYTES`](crate::context::PROGRESS_CHUNK_MAX_BYTES)) to
/// leave headroom for the envelope.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProgressNotifyPayload {
    /// Monotonic, epoch-prefixed sequence number (strictly increasing over the
    /// execution's lifetime, so a reconnecting subscriber can detect gaps).
    pub seq: u64,
    /// The published chunk (JSON; possibly a truncation marker if the original
    /// exceeded the size cap).
    pub chunk: serde_json::Value,
}

/// Outcome of waiting on a queue listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueWaitOutcome {
    /// A notification payload arrived before the timeout elapsed.
    Notification(NotifyPayload),
    /// No payload arrived before `poll_interval` elapsed.
    TimedOut,
    /// The listener channel closed because the underlying connection died.
    ChannelClosed,
}

/// Outcome of waiting on the workflow event listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowEventWaitOutcome {
    /// A workflow event notification arrived before the timeout elapsed.
    Notification(WorkflowEventNotifyPayload),
    /// No notification arrived before the caller's timeout elapsed.
    TimedOut,
    /// The LISTEN connection closed.
    ChannelClosed,
}

/// Outcome of waiting on a [`WorkflowProgressListener`] (issue #791).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressWaitOutcome {
    /// A progress chunk arrived before the timeout elapsed.
    Chunk(ProgressNotifyPayload),
    /// No chunk arrived before the caller's timeout elapsed (send a keepalive).
    TimedOut,
    /// The LISTEN connection closed (the SSE stream should end with an error).
    ChannelClosed,
}

// ---------------------------------------------------------------------------
// Send notification (via diesel connection)
// ---------------------------------------------------------------------------

/// Send a `NOTIFY` on the appropriate channel for the given queue.
///
/// This is typically called immediately after [`crate::queue::enqueue()`] to
/// wake any listening workers.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the `NOTIFY` SQL fails.
pub async fn notify_task_enqueued(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    task_id: Uuid,
) -> HarvestResult<()> {
    let channel = queue_channel(queue_name);
    let payload = serde_json::to_string(&NotifyPayload { task_id })
        .map_err(|e| HarvestError::Database(format!("failed to serialize notify payload: {e}")))?;

    // Chaos: drop this LISTEN/NOTIFY wake (issue #940 AC1(c)). The task row is
    // already committed; a dropped wake must still converge via the poll loop,
    // which is the source of truth (NOTIFY is only a latency optimization).
    if crate::chaos_drop_notify!(NOTIFY_TASK_ENQUEUED) {
        return Ok(());
    }

    diesel::sql_query("SELECT pg_notify($1, $2)")
        .bind::<Text, _>(&channel)
        .bind::<Text, _>(&payload)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

/// Send a `NOTIFY` on the appropriate channels for multiple queues in a single roundtrip.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the `NOTIFY` SQL fails.
pub async fn notify_tasks_enqueued(
    conn: &mut AsyncPgConnection,
    queue_names: &[String],
    task_id: Uuid,
) -> HarvestResult<()> {
    if queue_names.is_empty() {
        return Ok(());
    }

    let channels: Vec<String> = queue_names.iter().map(|q| queue_channel(q)).collect();
    let payload = serde_json::to_string(&NotifyPayload { task_id })
        .map_err(|e| HarvestError::Database(format!("failed to serialize notify payload: {e}")))?;

    diesel::sql_query("SELECT pg_notify(channel, $2) FROM unnest($1) AS channel")
        .bind::<diesel::sql_types::Array<Text>, _>(channels)
        .bind::<Text, _>(&payload)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

/// Send a notification that one or more workflow events were appended.
///
/// When called inside a transaction, Postgres delivers the notification only
/// after the transaction commits, so listeners wake after the execution row and
/// event rows are mutually visible.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if payload serialization or `pg_notify`
/// fails.
pub async fn notify_workflow_events_appended(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: Uuid,
    event_count: usize,
    last_event_type: &str,
) -> HarvestResult<()> {
    let payload = serde_json::to_string(&WorkflowEventNotifyPayload {
        workflow_exec_id,
        event_count,
        last_event_type: last_event_type.to_string(),
    })
    .map_err(|e| HarvestError::Database(format!("failed to serialize notify payload: {e}")))?;

    diesel::sql_query("SELECT pg_notify($1, $2)")
        .bind::<Text, _>(workflow_events_channel())
        .bind::<Text, _>(&payload)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

/// Fire a `NOTIFY` carrying one ephemeral progress chunk on the per-execution
/// progress channel (issue #791).
///
/// Delivered on commit when called inside a transaction — so a rolled-back
/// workflow-decision cycle's progress is discarded (never delivered) and the
/// retried cycle re-fires live, and a committed chunk is delivered exactly once
/// per commit. Best-effort by contract: callers should treat a failure as
/// non-fatal (the chunk is disposable).
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if payload serialization or `pg_notify`
/// fails.
pub async fn notify_workflow_progress(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: Uuid,
    seq: u64,
    chunk: &serde_json::Value,
) -> HarvestResult<()> {
    let channel = workflow_progress_channel(workflow_exec_id);
    let payload = serde_json::to_string(&ProgressNotifyPayload {
        seq,
        chunk: chunk.clone(),
    })
    .map_err(|e| HarvestError::Database(format!("failed to serialize progress payload: {e}")))?;

    diesel::sql_query("SELECT pg_notify($1, $2)")
        .bind::<Text, _>(&channel)
        .bind::<Text, _>(&payload)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Listener connections (TLS: issue #1717)
// ---------------------------------------------------------------------------

/// The transport a listener connection uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListenTransport {
    /// Plaintext, through `NoTls`.
    Plain,
    /// TLS, with a verified certificate chain and hostname.
    Tls,
}

/// Select the listener transport from the DSN's own `sslmode`.
///
/// Only `require` selects TLS, because `NoTls` cannot satisfy it. `disable`,
/// `prefer` and an absent `sslmode` stay plaintext. That is the behavior before
/// issue #1717, and it matches a pool built with `NoTls`. Thus a server with a
/// self-signed certificate does not break a `prefer` DSN.
fn listen_transport(config: &tokio_postgres::Config) -> ListenTransport {
    match config.get_ssl_mode() {
        tokio_postgres::config::SslMode::Require => ListenTransport::Tls,
        _ => ListenTransport::Plain,
    }
}

/// Render an error and each error in its `source()` chain.
///
/// `tokio_postgres` shows a TLS failure as "error performing TLS handshake".
/// The real cause is only in `source()`. A cause that the text already
/// contains is not added again.
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = cause.source();
    }
    out
}

/// The error for a listener connection that failed to open.
fn connect_error(error: &tokio_postgres::Error) -> HarvestError {
    HarvestError::Database(format!("pg connect failed: {}", error_chain(error)))
}

/// An open LISTEN connection.
struct ListenConnection {
    /// Client handle. The connection closes when it drops.
    client: tokio_postgres::Client,
    /// Notifications that the driver task forwards.
    rx: tokio::sync::mpsc::Receiver<tokio_postgres::Notification>,
    /// The task that drives the connection.
    driver: tokio::task::JoinHandle<()>,
}

/// Open a LISTEN connection with the transport that the DSN asks for.
///
/// `error_message` is the log message for a connection error after the open.
async fn open_listen_connection(
    database_url: &str,
    error_message: &'static str,
) -> HarvestResult<ListenConnection> {
    let config: tokio_postgres::Config = database_url.parse().map_err(|e| connect_error(&e))?;
    match listen_transport(&config) {
        ListenTransport::Plain => {
            let (client, connection) = config
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(|e| connect_error(&e))?;
            Ok(spawn_listen_driver(client, connection, error_message))
        }
        ListenTransport::Tls => open_tls_listen_connection(&config, error_message).await,
    }
}

/// Open a verified TLS LISTEN connection.
#[cfg(feature = "tls")]
async fn open_tls_listen_connection(
    config: &tokio_postgres::Config,
    error_message: &'static str,
) -> HarvestResult<ListenConnection> {
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_client_config()?);
    let (client, connection) = config.connect(tls).await.map_err(|e| connect_error(&e))?;
    Ok(spawn_listen_driver(client, connection, error_message))
}

/// Refuse `sslmode=require` when the crate has no TLS support.
#[cfg(not(feature = "tls"))]
#[allow(clippy::unused_async, reason = "the signature matches the `tls` build")]
async fn open_tls_listen_connection(
    _config: &tokio_postgres::Config,
    _error_message: &'static str,
) -> HarvestResult<ListenConnection> {
    Err(HarvestError::Config(
        "sslmode=require needs the `tls` feature of autumn-harvest".to_string(),
    ))
}

/// The rustls configuration for listener connections.
///
/// The trust store is read once per process. A failed read is not cached, so
/// a later connection tries again.
#[cfg(feature = "tls")]
fn tls_client_config() -> HarvestResult<rustls::ClientConfig> {
    static CONFIG: std::sync::OnceLock<rustls::ClientConfig> = std::sync::OnceLock::new();
    if let Some(config) = CONFIG.get() {
        return Ok(config.clone());
    }
    let built = build_tls_client_config()?;
    Ok(CONFIG.get_or_init(|| built).clone())
}

/// Build a rustls configuration that trusts the platform trust store.
///
/// The chain and the hostname are always verified, as in `harvest migrate`
/// (issue #1240). `SSL_CERT_FILE` or `SSL_CERT_DIR` can point at a private CA.
/// The `ring` provider is explicit, because `ClientConfig::builder()` panics
/// when no process-wide provider is installed.
#[cfg(feature = "tls")]
fn build_tls_client_config() -> HarvestResult<rustls::ClientConfig> {
    let native = rustls_native_certs::load_native_certs();
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(native.certs);
    if roots.is_empty() {
        return Err(HarvestError::Config(format!(
            "sslmode=require: the platform trust store has no usable certificates. \
             Install the ca-certificates package, or set SSL_CERT_FILE. \
             Loader errors: {:?}",
            native.errors
        )));
    }
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| HarvestError::Config(format!("rustls configuration failed: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(config)
}

/// Spawn the task that drives a LISTEN connection.
///
/// The task calls `poll_message()` to get each notification. The default
/// `Future` implementation of the connection discards them.
fn spawn_listen_driver<S, T>(
    client: tokio_postgres::Client,
    mut connection: tokio_postgres::Connection<S, T>,
    error_message: &'static str,
) -> ListenConnection
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(128);
    let driver = tokio::spawn(async move {
        use futures::future::poll_fn;

        loop {
            match poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                    // A send error means the listener dropped. Shut down.
                    if tx.send(n).await.is_err() {
                        break;
                    }
                }
                // Notices and other async messages are ignored.
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    tracing::error!(error = %error_chain(&e), "{error_message}");
                    break;
                }
                // The connection closed cleanly.
                None => break,
            }
        }
    });
    ListenConnection { client, rx, driver }
}

// ---------------------------------------------------------------------------
// QueueListener (using tokio-postgres)
// ---------------------------------------------------------------------------

/// Async listener for Postgres NOTIFY events on task queue channels.
///
/// Uses a dedicated `tokio-postgres` connection (separate from the diesel pool)
/// because `LISTEN` requires a long-lived connection that receives async
/// notifications. The connection is driven by a background task that forwards
/// notifications through an `mpsc` channel.
pub struct QueueListener {
    /// Client handle kept alive so the LISTEN connection stays open.
    _client: tokio_postgres::Client,
    /// Receiver for notifications forwarded by the connection driver task.
    rx: tokio::sync::mpsc::Receiver<tokio_postgres::Notification>,
    /// Background connection driver handle -- kept alive for the connection's lifetime.
    _connection_handle: tokio::task::JoinHandle<()>,
    /// Queue names this listener is subscribed to.
    queues: Vec<String>,
}

impl QueueListener {
    /// Connect to Postgres and subscribe to NOTIFY channels for the given queues.
    ///
    /// Spawns a background task that drives the connection and forwards
    /// [`Notification`]s through an internal channel. The connection stays
    /// alive as long as this `QueueListener` is held.
    ///
    /// `sslmode=require` in `database_url` selects verified TLS. Other modes
    /// connect in plaintext (issue #1717).
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the connection or LISTEN fails.
    /// Returns [`HarvestError::Config`] if TLS cannot be configured.
    pub async fn connect(database_url: &str, queues: &[String]) -> HarvestResult<Self> {
        let ListenConnection { client, rx, driver } =
            open_listen_connection(database_url, "postgres listener connection error").await?;

        // Subscribe to all queue channels.
        for queue in queues {
            let channel = queue_channel(queue);
            let quoted_channel = quote_pg_identifier(&channel);
            client
                .batch_execute(&format!("LISTEN {quoted_channel}"))
                .await
                .map_err(|e| {
                    HarvestError::Database(format!(
                        "LISTEN {quoted_channel} failed: {}",
                        error_chain(&e)
                    ))
                })?;
        }

        Ok(Self {
            _client: client,
            rx,
            _connection_handle: driver,
            queues: queues.to_vec(),
        })
    }

    /// Wait for a notification or timeout after `poll_interval`.
    ///
    /// Returns `Some(payload)` if a notification arrived, or `None` on timeout.
    /// Workers use this in a loop: wake on notification or fall back to polling.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload fails to
    /// deserialize.
    pub async fn wait_for_notification(
        &mut self,
        poll_interval: Duration,
    ) -> HarvestResult<Option<NotifyPayload>> {
        match self.wait_for_notification_outcome(poll_interval).await? {
            QueueWaitOutcome::Notification(payload) => Ok(Some(payload)),
            QueueWaitOutcome::TimedOut | QueueWaitOutcome::ChannelClosed => Ok(None),
        }
    }

    /// Wait for a notification and distinguish timeout from listener shutdown.
    ///
    /// This is useful for callers that need to reconnect after the underlying
    /// LISTEN connection dies instead of treating every wake miss as a normal
    /// timeout.
    pub async fn wait_for_notification_outcome(
        &mut self,
        poll_interval: Duration,
    ) -> HarvestResult<QueueWaitOutcome> {
        match tokio::time::timeout(poll_interval, self.rx.recv()).await {
            Ok(Some(notification)) => {
                let payload: NotifyPayload = serde_json::from_str(notification.payload())
                    .map_err(|e| HarvestError::Database(format!("bad notify payload: {e}")))?;
                Ok(QueueWaitOutcome::Notification(payload))
            }
            Ok(None) => Ok(QueueWaitOutcome::ChannelClosed),
            Err(_elapsed) => Ok(QueueWaitOutcome::TimedOut),
        }
    }

    /// The queue names this listener is subscribed to.
    #[must_use]
    pub fn queues(&self) -> &[String] {
        &self.queues
    }
}

/// Async listener for [`workflow_events_channel`] notifications.
pub struct WorkflowEventListener {
    /// Client handle kept alive so the LISTEN connection stays open.
    _client: tokio_postgres::Client,
    /// Receiver for notifications forwarded by the connection driver task.
    rx: tokio::sync::mpsc::Receiver<tokio_postgres::Notification>,
    /// Background connection driver handle kept alive for the connection's lifetime.
    _connection_handle: tokio::task::JoinHandle<()>,
}

impl WorkflowEventListener {
    /// Connect to Postgres and subscribe to the `harvest_events` channel.
    ///
    /// `sslmode=require` in `database_url` selects verified TLS. Other modes
    /// connect in plaintext (issue #1717).
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the connection or LISTEN fails.
    /// Returns [`HarvestError::Config`] if TLS cannot be configured.
    pub async fn connect(database_url: &str) -> HarvestResult<Self> {
        let ListenConnection { client, rx, driver } =
            open_listen_connection(database_url, "postgres workflow event listener error").await?;

        let channel = quote_pg_identifier(workflow_events_channel());
        client
            .batch_execute(&format!("LISTEN {channel}"))
            .await
            .map_err(|e| {
                HarvestError::Database(format!("LISTEN {channel} failed: {}", error_chain(&e)))
            })?;

        Ok(Self {
            _client: client,
            rx,
            _connection_handle: driver,
        })
    }

    /// Wait indefinitely for the next workflow event notification.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload is invalid.
    pub async fn wait_for_notification(&mut self) -> HarvestResult<WorkflowEventWaitOutcome> {
        match self.rx.recv().await {
            Some(notification) => {
                let payload: WorkflowEventNotifyPayload =
                    serde_json::from_str(notification.payload()).map_err(|e| {
                        HarvestError::Database(format!("bad workflow notify payload: {e}"))
                    })?;
                Ok(WorkflowEventWaitOutcome::Notification(payload))
            }
            None => Ok(WorkflowEventWaitOutcome::ChannelClosed),
        }
    }

    /// Wait for a notification up to `timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload is invalid.
    pub async fn wait_for_notification_timeout(
        &mut self,
        timeout: Duration,
    ) -> HarvestResult<WorkflowEventWaitOutcome> {
        match tokio::time::timeout(timeout, self.wait_for_notification()).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Ok(WorkflowEventWaitOutcome::TimedOut),
        }
    }
}

/// Async listener for a single execution's ephemeral progress stream (issue
/// #791).
///
/// Subscribes to the per-execution [`workflow_progress_channel`] and yields each
/// [`ProgressNotifyPayload`] the worker fires via [`notify_workflow_progress`].
/// Intended for the `GET /workflows/{id}/stream` SSE route: connect once per
/// streamed run, then poll [`wait_for_progress_timeout`](Self::wait_for_progress_timeout)
/// so the route can interleave keepalive ticks and terminal-state checks.
pub struct WorkflowProgressListener {
    /// Client handle kept alive so the LISTEN connection stays open.
    _client: tokio_postgres::Client,
    /// Receiver for notifications forwarded by the connection driver task.
    rx: tokio::sync::mpsc::Receiver<tokio_postgres::Notification>,
    /// Background connection driver handle kept alive for the connection's lifetime.
    _connection_handle: tokio::task::JoinHandle<()>,
}

impl WorkflowProgressListener {
    /// Connect to Postgres and subscribe to `exec_id`'s progress channel.
    ///
    /// `sslmode=require` in `database_url` selects verified TLS. Other modes
    /// connect in plaintext (issue #1717).
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the connection or LISTEN fails.
    /// Returns [`HarvestError::Config`] if TLS cannot be configured.
    pub async fn connect(database_url: &str, exec_id: Uuid) -> HarvestResult<Self> {
        let ListenConnection { client, rx, driver } =
            open_listen_connection(database_url, "postgres workflow progress listener error")
                .await?;

        let channel = quote_pg_identifier(&workflow_progress_channel(exec_id));
        client
            .batch_execute(&format!("LISTEN {channel}"))
            .await
            .map_err(|e| {
                HarvestError::Database(format!("LISTEN {channel} failed: {}", error_chain(&e)))
            })?;

        Ok(Self {
            _client: client,
            rx,
            _connection_handle: driver,
        })
    }

    /// Wait indefinitely for the next progress chunk.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload is invalid.
    pub async fn wait_for_progress(&mut self) -> HarvestResult<ProgressWaitOutcome> {
        match self.rx.recv().await {
            Some(notification) => {
                let payload: ProgressNotifyPayload = serde_json::from_str(notification.payload())
                    .map_err(|e| {
                    HarvestError::Database(format!("bad progress notify payload: {e}"))
                })?;
                Ok(ProgressWaitOutcome::Chunk(payload))
            }
            None => Ok(ProgressWaitOutcome::ChannelClosed),
        }
    }

    /// Wait for a progress chunk up to `timeout`.
    ///
    /// A [`ProgressWaitOutcome::TimedOut`] lets the SSE route send a keepalive
    /// and re-check the execution's terminal state before waiting again.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload is invalid.
    pub async fn wait_for_progress_timeout(
        &mut self,
        timeout: Duration,
    ) -> HarvestResult<ProgressWaitOutcome> {
        match tokio::time::timeout(timeout, self.wait_for_progress()).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Ok(ProgressWaitOutcome::TimedOut),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_name_for_queue() {
        assert_eq!(queue_channel("default"), "harvest_queue_default");
        assert_eq!(queue_channel("email-queue"), "harvest_queue_email_queue");
        assert_eq!(
            queue_channel("billing-high-priority"),
            "harvest_queue_billing_high_priority"
        );
    }

    #[test]
    fn notify_payload_roundtrips() {
        let original = NotifyPayload {
            task_id: Uuid::new_v4(),
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let deserialized: NotifyPayload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original.task_id, deserialized.task_id);
    }

    #[test]
    fn channel_name_no_hyphens_in_output() {
        let channel = queue_channel("a-b-c");
        assert!(
            !channel.contains('-'),
            "channel name must not contain hyphens: {channel}"
        );
    }

    #[test]
    fn quoted_identifier_escapes_embedded_quotes() {
        assert_eq!(
            quote_pg_identifier("harvest_queue_priority\"queue"),
            "\"harvest_queue_priority\"\"queue\""
        );
    }

    #[test]
    fn workflow_events_channel_is_stable() {
        assert_eq!(workflow_events_channel(), "harvest_events");
    }

    #[test]
    fn workflow_event_notify_payload_roundtrips() {
        let original = WorkflowEventNotifyPayload {
            workflow_exec_id: Uuid::new_v4(),
            event_count: 2,
            last_event_type: "WorkflowCompleted".to_string(),
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let deserialized: WorkflowEventNotifyPayload =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, deserialized);
    }

    // ── TLS for listener connections (issue #1717) ───────────────────────

    fn transport_for(dsn: &str) -> ListenTransport {
        let config: tokio_postgres::Config = dsn.parse().expect("test DSN parses");
        listen_transport(&config)
    }

    #[test]
    fn only_sslmode_require_selects_tls() {
        let base = "postgres://u:p@db.internal/harvest";
        assert_eq!(transport_for(base), ListenTransport::Plain);
        assert_eq!(
            transport_for(&format!("{base}?sslmode=disable")),
            ListenTransport::Plain
        );
        assert_eq!(
            transport_for(&format!("{base}?sslmode=prefer")),
            ListenTransport::Plain
        );
        assert_eq!(
            transport_for(&format!("{base}?sslmode=require")),
            ListenTransport::Tls
        );
    }

    #[test]
    fn keyword_dsn_with_sslmode_require_selects_tls() {
        assert_eq!(
            transport_for("host=db.internal dbname=harvest sslmode=require"),
            ListenTransport::Tls
        );
        assert_eq!(
            transport_for("host=db.internal dbname=harvest"),
            ListenTransport::Plain
        );
    }

    /// An `SSLRequest` message: length 8, then the code 80877103.
    #[cfg(feature = "tls")]
    const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

    /// Open a listener connection to a fake server, and record what arrives.
    ///
    /// The fake server reads the first message header. When `answer_tls` is
    /// true, it accepts TLS with `S` and also reads the next byte.
    async fn first_bytes_sent(sslmode: &str, answer_tls: bool) -> ([u8; 8], Option<u8>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let server = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake server");
        let port = server.local_addr().expect("fake server address").port();
        let accept = tokio::spawn(async move {
            let (mut socket, _) = server.accept().await.expect("accept");
            let mut header = [0_u8; 8];
            socket
                .read_exact(&mut header)
                .await
                .expect("message header");
            if !answer_tls {
                return (header, None);
            }
            socket.write_all(b"S").await.expect("accept TLS");
            let mut next = [0_u8; 1];
            let next = socket.read_exact(&mut next).await.ok().map(|_| next[0]);
            (header, next)
        });
        let url = format!("postgres://u@127.0.0.1:{port}/db?sslmode={sslmode}");
        let client = tokio::spawn(async move {
            open_listen_connection(&url, "test listener error")
                .await
                .map(|_| ())
        });
        let seen = tokio::time::timeout(Duration::from_secs(10), accept)
            .await
            .expect("fake server sees the client")
            .expect("fake server task");
        client.abort();
        seen
    }

    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn sslmode_require_starts_a_tls_handshake() {
        let (header, next) = first_bytes_sent("require", true).await;
        assert_eq!(header, SSL_REQUEST);
        // 0x16 is the TLS handshake record type, so this is a ClientHello.
        // A `NoTls` connector sends nothing after the server accepts TLS.
        assert_eq!(next, Some(0x16), "sslmode=require must send a ClientHello");
    }

    #[tokio::test]
    async fn sslmode_prefer_stays_plaintext() {
        let (header, _) = first_bytes_sent("prefer", false).await;
        // A startup message carries protocol version 3.0 after its length.
        assert_eq!(header[4..], [0, 3, 0, 0], "prefer must not send SSLRequest");
    }

    #[cfg(not(feature = "tls"))]
    #[tokio::test]
    async fn sslmode_require_without_the_tls_feature_is_a_config_error() {
        let result = open_listen_connection(
            "postgres://u@127.0.0.1:1/db?sslmode=require",
            "test listener error",
        )
        .await
        .map(|_| ());
        assert!(
            matches!(&result, Err(HarvestError::Config(m)) if m.contains("`tls` feature")),
            "{:?}",
            result.err()
        );
    }

    #[derive(Debug)]
    struct Layer(&'static str, Option<Box<Self>>);

    impl std::fmt::Display for Layer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Layer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|e| e as _)
        }
    }

    #[test]
    fn error_chain_names_every_cause() {
        let error = Layer(
            "error performing TLS handshake",
            Some(Box::new(Layer(
                "invalid peer certificate",
                Some(Box::new(Layer("UnknownIssuer", None))),
            ))),
        );
        assert_eq!(
            error_chain(&error),
            "error performing TLS handshake: invalid peer certificate: UnknownIssuer"
        );
    }

    // ── publish_progress channel (issue #791) ────────────────────────────

    #[test]
    fn workflow_progress_channel_naming() {
        let exec_id = Uuid::parse_str("0191c1a2-3b4c-7d5e-8f60-112233445566").expect("valid uuid");
        let channel = workflow_progress_channel(exec_id);
        assert_eq!(channel, "harvest_progress_0191c1a23b4c7d5e8f60112233445566");
        assert!(
            !channel.contains('-'),
            "progress channel must not contain hyphens: {channel}"
        );
        // Postgres identifiers are limited to NAMEDATALEN-1 = 63 bytes.
        assert!(
            channel.len() <= 63,
            "progress channel {} exceeds Postgres 63-byte identifier limit ({} bytes)",
            channel,
            channel.len()
        );
    }

    #[test]
    fn progress_notify_payload_roundtrips() {
        let original = ProgressNotifyPayload {
            seq: 0x0000_0005_00FF_FFFF,
            chunk: serde_json::json!({"phase": "mid", "pct": 50}),
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let deserialized: ProgressNotifyPayload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, deserialized);
    }

    #[test]
    fn progress_notify_payload_stays_within_pg_notify_limit() {
        // Postgres `pg_notify` payloads are hard-capped at 8000 bytes. The
        // context caps each chunk's serialized JSON at
        // `PROGRESS_CHUNK_MAX_BYTES` (7000) and this envelope wraps it as
        // `{"seq":..,"chunk":..}`. Pin the worst case directly: a max-`u64` seq
        // (20 decimal digits) plus a chunk serialized right at the cap must
        // still leave the whole envelope under the 8000-byte NOTIFY limit.
        let cap = crate::context::PROGRESS_CHUNK_MAX_BYTES;
        // A JSON string of (cap - 2) chars serializes (with its two quotes) to
        // exactly `cap` bytes — the largest chunk the context will forward.
        let max_chunk = serde_json::Value::String("x".repeat(cap - 2));
        assert_eq!(
            serde_json::to_vec(&max_chunk).unwrap().len(),
            cap,
            "test fixture: chunk must serialize to exactly the cap"
        );
        let payload = ProgressNotifyPayload {
            seq: u64::MAX,
            chunk: max_chunk,
        };
        let serialized = serde_json::to_string(&payload).expect("serialize");
        assert!(
            serialized.len() < 8000,
            "progress NOTIFY envelope must stay under the Postgres 8000-byte \
             pg_notify limit (max-u64 seq + max chunk), was {} bytes",
            serialized.len()
        );
    }
}
