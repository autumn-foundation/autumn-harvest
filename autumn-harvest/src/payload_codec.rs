//! Interfaces for transforming event payloads before they are persisted.
//!
//! **Why does this exist?**
//! By default, harvest workflow inputs, outputs, and parameters are stored in the database
//! as plain JSON. If a workflow processes sensitive data (like PII, secrets, or financial data),
//! storing it in plain text is a security risk. Payload codecs solve this by providing a hook
//! to intercept and transform these JSON values (typically encrypting or compressing them)
//! *before* they hit the database, and reversing the process when they are read back.
//!
//! ## Examples
//!
//! Implementing a simple rot13 "encryption" codec:
//!
//! ```rust
//! use autumn_harvest::payload_codec::{PayloadCodec, CodecError};
//!
//! struct Rot13Codec;
//!
//! impl PayloadCodec for Rot13Codec {
//!     fn codec_id(&self) -> &'static str { "rot13" }
//!     fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
//!         Ok(raw.iter().map(|&b| if b.is_ascii_alphabetic() {
//!             let base = if b.is_ascii_lowercase() { b'a' } else { b'A' };
//!             (b - base + 13) % 26 + base
//!         } else { b }).collect())
//!     }
//!     fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
//!         // rot13 is its own inverse
//!         self.encode(encoded)
//!     }
//! }
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine as _;
use serde_json::Value;

use crate::error::{HarvestError, HarvestResult};

/// Marker key for a payload field the read path could not decode (issue #608).
///
/// Sibling discriminator of `_harvest_codec_envelope`,
/// `_harvest_offload_envelope` (issue #524), and `_harvest_erased`
/// (issue #495).
pub const UNDECODABLE_MARKER_KEY: &str = "_harvest_undecodable";

/// Undecodable reason: the envelope names a codec that is not registered.
pub const UNDECODABLE_REASON_UNKNOWN_CODEC: &str = "unknown_codec";
/// Undecodable reason: the envelope's `data` field is not valid base64.
pub const UNDECODABLE_REASON_INVALID_BASE64: &str = "invalid_base64";
/// Undecodable reason: the codec's `decode` returned an error (bad key,
/// corrupt ciphertext). The codec's own error text is deliberately **not**
/// embedded — it could carry key material.
pub const UNDECODABLE_REASON_CODEC_ERROR: &str = "codec_error";
/// Undecodable reason: the decoded plaintext bytes are not valid JSON.
pub const UNDECODABLE_REASON_INVALID_JSON: &str = "invalid_json";

/// Builds the typed graceful-degrade marker for a payload field the read
/// path could not decode (issue #608):
/// `{"_harvest_undecodable": {"codec_id": <id>, "reason": <reason>}}`.
///
/// `reason` is one of the bounded `UNDECODABLE_REASON_*` strings — the marker
/// never echoes ciphertext or codec error text.
#[must_use]
pub fn undecodable_marker(codec_id: &str, reason: &str) -> Value {
    serde_json::json!({
        UNDECODABLE_MARKER_KEY: {
            "codec_id": codec_id,
            "reason": reason,
        }
    })
}

/// Outcome of a tolerant read-path decode walk (issue #608).
///
/// Counts only — never payload content — so it is safe to thread into audit
/// records and logs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LossyDecodeOutcome {
    /// Number of codec envelopes successfully decoded in place.
    pub decoded: usize,
    /// Number of envelopes replaced with an [`UNDECODABLE_MARKER_KEY`] marker.
    pub failed: usize,
}

impl LossyDecodeOutcome {
    /// Whether the walk decoded or marked at least one envelope.
    // No arithmetic: `merged` saturates, so `decoded + failed` could overflow
    // after a saturated merge — keep the predicate addition-free.
    #[must_use]
    pub const fn touched(&self) -> bool {
        self.decoded > 0 || self.failed > 0
    }

    /// Saturating merge for multi-field / multi-row accumulation.
    #[must_use]
    pub const fn merged(self, other: Self) -> Self {
        Self {
            decoded: self.decoded.saturating_add(other.decoded),
            failed: self.failed.saturating_add(other.failed),
        }
    }
}

/// The exact codec-envelope shape check shared by the strict
/// (`decode_payload`) and lossy (`decode_value_lossy`) read paths, so the two
/// can never disagree about what an envelope is: an object with **exactly**
/// three keys — `_harvest_codec_envelope == 1`, string `codec_id`, and string
/// `data`. Returns `(codec_id, base64_data)` for an envelope, `None` for
/// anything else (offload envelopes, erase tombstones, business data carrying
/// a `codec_id` field, malformed near-envelopes).
fn codec_envelope_parts(payload: &Value) -> Option<(&str, &str)> {
    let obj = payload.as_object()?;
    if obj.get("_harvest_codec_envelope").and_then(Value::as_i64) != Some(1) {
        return None;
    }
    let codec_id = obj.get("codec_id").and_then(Value::as_str)?;
    let encoded_b64 = obj.get("data").and_then(Value::as_str)?;
    if obj.len() != 3 {
        return None;
    }
    Some((codec_id, encoded_b64))
}

/// A trait for intercepting and transforming raw payload bytes.
///
/// Implementations of this trait are used by the [`PayloadCodecs`] registry
/// to encode and decode fields (such as inputs and outputs) before they are
/// written to or after they are read from the database.
pub trait PayloadCodec: Send + Sync {
    /// Returns a unique identifier for this codec.
    ///
    /// This ID is stored alongside encoded data to ensure that the correct codec
    /// is used when decoding. It must be unique across all registered codecs.
    fn codec_id(&self) -> &'static str;
    /// Encode raw payload bytes for persistence.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when encoding fails.
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError>;
    /// Decode persisted payload bytes back into raw bytes.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when decoding fails.
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError>;
}

/// Represents an error that occurred during encoding or decoding.
///
/// This error is returned by implementations of the [`PayloadCodec`] trait when a transformation fails.
/// For example, if a decryption codec fails due to a bad key, it should return this error.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::payload_codec::CodecError;
///
/// let error = CodecError("decryption failed: bad key".to_string());
/// assert_eq!(error.to_string(), "payload codec error: decryption failed: bad key");
/// ```
#[derive(Debug, thiserror::Error)]
#[error("payload codec error: {0}")]
pub struct CodecError(pub String);

/// A pass-through codec that performs no transformation.
///
/// **Why does this exist?**
/// By default, the system uses the `IdentityCodec`. This ensures that payloads are written
/// to the database exactly as they are passed to the workflow APIs (as raw JSON). It also
/// provides a safe fallback when no other codec is explicitly configured.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::payload_codec::{PayloadCodec, IdentityCodec};
///
/// let codec = IdentityCodec;
/// assert_eq!(codec.codec_id(), "identity");
///
/// let raw_data = b"hello world";
/// let encoded = codec.encode(raw_data).unwrap();
/// assert_eq!(encoded, raw_data); // Identity changes nothing!
/// ```
#[derive(Debug, Default)]
pub struct IdentityCodec;

impl PayloadCodec for IdentityCodec {
    fn codec_id(&self) -> &'static str {
        "identity"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.to_vec())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.to_vec())
    }
}

/// A registry of available payload codecs and the configured default.
///
/// **Why does this exist?**
/// When decoding a payload, the system needs to know which codec to use. Because
/// a workflow's history might span a long period, it may contain payloads encoded
/// with different codecs (e.g., if you migrated from "encryption-v1" to "encryption-v2").
/// The `PayloadCodecs` struct holds all known codecs so it can dynamically look up
/// the right one based on the `codec_id` stored alongside the data.
///
/// The `default` codec is the one used for encoding *new* payloads.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::payload_codec::{PayloadCodecs, IdentityCodec};
/// use std::sync::Arc;
///
/// let mut codecs = PayloadCodecs::default();
/// // The default is automatically IdentityCodec.
/// // But you can change the default for encoding new payloads:
/// codecs.set_default(Arc::new(IdentityCodec));
/// ```
#[derive(Clone)]
pub struct PayloadCodecs {
    default: Arc<dyn PayloadCodec>,
    codecs: BTreeMap<&'static str, Arc<dyn PayloadCodec>>,
}

impl Default for PayloadCodecs {
    fn default() -> Self {
        let identity: Arc<dyn PayloadCodec> = Arc::new(IdentityCodec);
        let mut codecs = BTreeMap::new();
        codecs.insert(identity.codec_id(), identity.clone());
        Self {
            default: identity,
            codecs,
        }
    }
}

impl PayloadCodecs {
    /// Registers a codec so it can be used for decoding.
    ///
    /// The codec will only be used if an incoming payload is marked with its `codec_id`.
    /// Registering a codec does not make it the default for new payloads (use [`PayloadCodecs::set_default`] for that).
    pub fn register(&mut self, codec: Arc<dyn PayloadCodec>) {
        self.codecs.insert(codec.codec_id(), codec);
    }

    /// Sets the default codec used for encoding new payloads.
    ///
    /// The codec will also be implicitly registered for decoding.
    /// You should typically call this when starting up your Harvest worker
    /// or server to ensure all newly written payloads use this codec.
    pub fn set_default(&mut self, codec: Arc<dyn PayloadCodec>) {
        self.register(codec.clone());
        self.default = codec;
    }

    /// Encode payload-bearing fields inside an event into codec envelopes.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError`] if event serialization or codec encoding fails.
    pub fn encode_event(&self, event: &crate::event::WorkflowEvent) -> HarvestResult<Value> {
        let mut value = serde_json::to_value(event)?;
        self.transform_event_data(&mut value, true)?;
        Ok(value)
    }

    /// Decode codec envelopes in a serialized event back to raw JSON payloads.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError`] if codec decoding or event deserialization fails.
    pub fn decode_event(&self, mut value: Value) -> HarvestResult<crate::event::WorkflowEvent> {
        self.transform_event_data(&mut value, false)?;
        Ok(serde_json::from_value(value)?)
    }

    fn transform_event_data(&self, root: &mut Value, encode: bool) -> HarvestResult<()> {
        let Some(data) = root.get_mut("data") else {
            return Ok(());
        };
        // `value` is the arbitrary `ctx.side_effect(...)` closure result on a
        // SideEffectRecorded event (issue #384). Pre-#384 custom side effects
        // were stored under MarkerRecorded.details and were codec-encoded; the
        // new field must be encoded too so a configured codec still encrypts /
        // compresses any secret or PII the closure captured. `value` is unique
        // to SideEffectRecorded among event variants, so no other event is
        // affected.
        // `last_completion_result` is a copy of a prior run's output frozen into the
        // WorkflowStarted event for scheduled carryover (issue #488). It must be encoded
        // too so a configured codec encrypts/compresses any secret or PII the prior
        // output carried; it is unique to WorkflowStarted among event variants.
        for key in crate::payload_store::PAYLOAD_FIELD_KEYS {
            if let Some(payload) = data.get_mut(key) {
                if encode {
                    *payload = self.encode_payload(payload)?;
                } else {
                    *payload = self.decode_payload(payload)?;
                }
            }
        }
        Ok(())
    }

    fn encode_payload(&self, payload: &Value) -> HarvestResult<Value> {
        if self.default.codec_id() == "identity" {
            return Ok(payload.clone());
        }
        let raw = serde_json::to_vec(payload)?;
        let encoded = self
            .default
            .encode(&raw)
            .map_err(|e| HarvestError::Config(e.to_string()))?;
        Ok(
            serde_json::json!({"_harvest_codec_envelope": 1, "codec_id": self.default.codec_id(), "data": base64::engine::general_purpose::STANDARD.encode(encoded)}),
        )
    }

    fn decode_payload(&self, payload: &Value) -> HarvestResult<Value> {
        let Some((codec_id, encoded_b64)) = codec_envelope_parts(payload) else {
            return Ok(payload.clone());
        };
        let codec = self
            .codecs
            .get(codec_id)
            .ok_or_else(|| HarvestError::UnknownPayloadCodec {
                id: codec_id.to_string(),
            })?;
        let encoded = base64::engine::general_purpose::STANDARD
            .decode(encoded_b64)
            .map_err(|e| HarvestError::Config(e.to_string()))?;
        let decoded = codec
            .decode(&encoded)
            .map_err(|e| HarvestError::Config(e.to_string()))?;
        Ok(serde_json::from_slice(&decoded)?)
    }

    /// Decode one already-shape-verified envelope, mapping every failure mode
    /// to the typed [`undecodable_marker`] instead of an error (issue #608).
    ///
    /// `Ok(plaintext)` on success, `Err(marker)` on failure — the caller
    /// substitutes whichever it gets, so a bad key / rotated-away codec can
    /// never fail the surrounding response.
    fn decode_envelope_lossy(&self, codec_id: &str, encoded_b64: &str) -> Result<Value, Value> {
        let Some(codec) = self.codecs.get(codec_id) else {
            return Err(undecodable_marker(
                codec_id,
                UNDECODABLE_REASON_UNKNOWN_CODEC,
            ));
        };
        let Ok(encoded) = base64::engine::general_purpose::STANDARD.decode(encoded_b64) else {
            return Err(undecodable_marker(
                codec_id,
                UNDECODABLE_REASON_INVALID_BASE64,
            ));
        };
        let Ok(decoded) = codec.decode(&encoded) else {
            return Err(undecodable_marker(codec_id, UNDECODABLE_REASON_CODEC_ERROR));
        };
        serde_json::from_slice(&decoded)
            .map_err(|_| undecodable_marker(codec_id, UNDECODABLE_REASON_INVALID_JSON))
    }

    /// Tolerantly decode every codec envelope found anywhere inside `value`,
    /// in place, for the operator read path (issue #608).
    ///
    /// Infallible: a per-envelope failure replaces that one field with an
    /// [`UNDECODABLE_MARKER_KEY`] marker (bounded `UNDECODABLE_REASON_*`
    /// reason, never ciphertext or codec error text) and the walk continues —
    /// one un-decryptable field never fails the surrounding response.
    ///
    /// Single-pass: a decoded result is **never** re-scanned, so
    /// envelope-shaped plaintext (business data, a frozen
    /// `last_completion_result` copy) is preserved verbatim as data. Offload
    /// envelopes (`_harvest_offload_envelope`), erase tombstones
    /// (`_harvest_erased`), and malformed near-envelopes pass through
    /// untouched, exactly as the strict `decode_event` path tolerates them.
    ///
    /// This is a read-path helper for in-memory response copies only — it
    /// must never be applied to values that are written back to storage.
    pub fn decode_value_lossy(&self, value: &mut Value) -> LossyDecodeOutcome {
        let mut outcome = LossyDecodeOutcome::default();
        self.decode_value_lossy_inner(value, &mut outcome);
        outcome
    }

    fn decode_value_lossy_inner(&self, value: &mut Value, outcome: &mut LossyDecodeOutcome) {
        let replacement = codec_envelope_parts(value)
            .map(|(codec_id, encoded_b64)| self.decode_envelope_lossy(codec_id, encoded_b64));
        if let Some(result) = replacement {
            match result {
                Ok(plaintext) => {
                    outcome.decoded = outcome.decoded.saturating_add(1);
                    *value = plaintext;
                }
                Err(marker) => {
                    outcome.failed = outcome.failed.saturating_add(1);
                    *value = marker;
                }
            }
            // Single-pass rule: never recurse into a decoded result.
            return;
        }
        match value {
            Value::Object(map) => {
                for child in map.values_mut() {
                    self.decode_value_lossy_inner(child, outcome);
                }
            }
            Value::Array(items) => {
                for child in items.iter_mut() {
                    self.decode_value_lossy_inner(child, outcome);
                }
            }
            _ => {}
        }
    }

    /// Tolerant decode for TEXT columns (execution / dead-letter `error`)
    /// that may carry a serialized codec envelope (issue #608).
    ///
    /// Returns `(Some(decoded), outcome)` only when `raw` parses as JSON and
    /// that JSON is **exactly** a codec envelope; `(None, default)` means
    /// "leave the original string untouched" (plain error text, or JSON that
    /// is not an envelope). A decoded plain-string payload is returned
    /// unwrapped (the original error was a plain string before encoding);
    /// any other decoded JSON is returned serialized. A decode failure
    /// returns the [`undecodable_marker`] serialized to a string, counted in
    /// the outcome as `failed`.
    #[must_use]
    pub fn decode_error_string_lossy(&self, raw: &str) -> (Option<String>, LossyDecodeOutcome) {
        let Ok(parsed) = serde_json::from_str::<Value>(raw) else {
            return (None, LossyDecodeOutcome::default());
        };
        let Some((codec_id, encoded_b64)) = codec_envelope_parts(&parsed) else {
            return (None, LossyDecodeOutcome::default());
        };
        match self.decode_envelope_lossy(codec_id, encoded_b64) {
            Ok(plaintext) => {
                let decoded = match plaintext {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                (
                    Some(decoded),
                    LossyDecodeOutcome {
                        decoded: 1,
                        failed: 0,
                    },
                )
            }
            Err(marker) => (
                Some(marker.to_string()),
                LossyDecodeOutcome {
                    decoded: 0,
                    failed: 1,
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct ReverseCodec;

    impl PayloadCodec for ReverseCodec {
        fn codec_id(&self) -> &'static str {
            "reverse"
        }
        fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
            let mut v = raw.to_vec();
            v.reverse();
            Ok(v)
        }
        fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
            let mut v = encoded.to_vec();
            v.reverse();
            Ok(v)
        }
    }

    #[test]
    fn encode_then_decode_round_trips_workflow_event_payloads() {
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(ReverseCodec));

        let event = crate::event::WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"user":"alice"}),
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        };

        let encoded = codecs.encode_event(&event).expect("encode");
        assert_eq!(encoded["data"]["input"]["_harvest_codec_envelope"], 1);
        assert_eq!(encoded["data"]["input"]["codec_id"], "reverse");

        let decoded = codecs.decode_event(encoded).expect("decode");
        match decoded {
            crate::event::WorkflowEvent::WorkflowStarted { input, .. } => {
                assert_eq!(input, serde_json::json!({"user":"alice"}));
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn encode_then_decode_round_trips_side_effect_recorded_value() {
        // issue #384: a custom side_effect closure result lands in
        // SideEffectRecorded.value and must be codec-encoded (encryption /
        // compression) just like the MarkerRecorded.details it replaced.
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(ReverseCodec));

        let event = crate::event::WorkflowEvent::SideEffectRecorded {
            kind: crate::event::SideEffectKind::Custom,
            name: Some("api_credential".to_string()),
            value: serde_json::json!({"token": "super-secret"}),
        };

        let encoded = codecs.encode_event(&event).expect("encode");
        // The value field is wrapped in a codec envelope (not stored raw)…
        assert_eq!(encoded["data"]["value"]["_harvest_codec_envelope"], 1);
        assert_eq!(encoded["data"]["value"]["codec_id"], "reverse");
        // …while the side-effect name (metadata) is left intact.
        assert_eq!(encoded["data"]["name"], "api_credential");

        let decoded = codecs.decode_event(encoded).expect("decode");
        match decoded {
            crate::event::WorkflowEvent::SideEffectRecorded { value, name, .. } => {
                assert_eq!(value, serde_json::json!({"token": "super-secret"}));
                assert_eq!(name.as_deref(), Some("api_credential"));
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn identity_codec_preserves_raw_payload_shape() {
        let codecs = PayloadCodecs::default();
        let event = crate::event::WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"a": 1}),
        };
        let encoded = codecs.encode_event(&event).expect("encode");
        assert_eq!(encoded["data"]["output"], serde_json::json!({"a": 1}));
    }

    #[test]
    fn legacy_object_with_codec_id_but_not_envelope_is_not_decoded() {
        let codecs = PayloadCodecs::default();
        let event = serde_json::json!({
            "type": "WorkflowCompleted",
            "data": {
                "output": {
                    "codec_id": "business-field",
                    "value": 1
                }
            }
        });

        let decoded = codecs.decode_event(event).expect("decode");
        match decoded {
            crate::event::WorkflowEvent::WorkflowCompleted { output } => {
                assert_eq!(output["codec_id"], "business-field");
                assert_eq!(output["value"], 1);
            }
            _ => panic!("unexpected"),
        }
    }

    #[test]
    fn decode_unknown_codec_id_fails() {
        let codecs = PayloadCodecs::default();
        let bad = serde_json::json!({
            "type": "WorkflowCompleted",
            "data": {
                "output": {
                    "_harvest_codec_envelope": 1,
                    "codec_id": "missing",
                    "data": "e30="
                }
            }
        });

        let err = codecs.decode_event(bad).expect_err("must fail");
        assert!(matches!(err, HarvestError::UnknownPayloadCodec { .. }));
    }

    // ── Tolerant read-path decode (issue #608) ────────────────────────────────
    //
    // `decode_value_lossy` / `decode_error_string_lossy` are the operator
    // read-path siblings of the strict `decode_event`: infallible, recursive,
    // per-field graceful degrade via the `_harvest_undecodable` marker.
    // (`base64::Engine` is already in scope via `use super::*`.)

    /// A codec whose `decode` always fails — exercises the graceful-degrade
    /// marker path (bad key / corrupt ciphertext at read time).
    #[derive(Debug)]
    struct FailingCodec;

    impl PayloadCodec for FailingCodec {
        fn codec_id(&self) -> &'static str {
            "failing"
        }
        fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
            Ok(raw.to_vec())
        }
        fn decode(&self, _encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
            Err(CodecError("simulated bad key".to_string()))
        }
    }

    /// Registry with `ReverseCodec` + `FailingCodec` registered (identity is
    /// always present); default stays identity so encoding elsewhere is inert.
    fn lossy_test_codecs() -> PayloadCodecs {
        let mut codecs = PayloadCodecs::default();
        codecs.register(Arc::new(ReverseCodec));
        codecs.register(Arc::new(FailingCodec));
        codecs
    }

    /// Builds a well-formed codec envelope for `plain` under `codec_id`,
    /// using `ReverseCodec`'s byte-reversal for the ciphertext.
    fn reverse_envelope(plain: &Value) -> Value {
        let mut bytes = serde_json::to_vec(plain).expect("serialize plain");
        bytes.reverse();
        serde_json::json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "reverse",
            "data": base64::engine::general_purpose::STANDARD.encode(bytes),
        })
    }

    /// An envelope naming a codec that is not registered anywhere.
    fn unknown_codec_envelope() -> Value {
        serde_json::json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "kms-rotated-away",
            "data": "e30=",
        })
    }

    #[test]
    fn decode_value_lossy_decodes_envelope_in_place() {
        let codecs = lossy_test_codecs();
        let plain = serde_json::json!({"user": "alice", "ssn": "s3cret-pii"});
        let mut value = serde_json::json!({
            "input": reverse_envelope(&plain),
            "meta": "untouched",
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value["input"], plain, "envelope must decode in place");
        assert_eq!(value["meta"], "untouched");
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 0
            }
        );
        assert!(outcome.touched());
    }

    #[test]
    fn decode_value_lossy_identity_envelope_round_trips() {
        // The identity codec is always registered, so an identity envelope
        // (written by a future/foreign path) must round-trip on read.
        let codecs = PayloadCodecs::default();
        let plain = serde_json::json!({"n": 42});
        let raw = serde_json::to_vec(&plain).unwrap();
        let mut value = serde_json::json!({
            "output": {
                "_harvest_codec_envelope": 1,
                "codec_id": "identity",
                "data": base64::engine::general_purpose::STANDARD.encode(raw),
            }
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value["output"], plain);
        assert_eq!(outcome.decoded, 1);
        assert_eq!(outcome.failed, 0);
    }

    #[test]
    fn decode_value_lossy_unknown_codec_id_yields_undecodable_marker_not_error() {
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({"input": unknown_codec_envelope()});

        // Infallible by signature: no Result to unwrap.
        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 0,
                failed: 1
            }
        );
        let marker = &value["input"][UNDECODABLE_MARKER_KEY];
        assert_eq!(marker["codec_id"], "kms-rotated-away");
        assert_eq!(marker["reason"], UNDECODABLE_REASON_UNKNOWN_CODEC);
    }

    #[test]
    fn decode_value_lossy_bad_base64_yields_marker() {
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "input": {
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
                "data": "!!!not-base64!!!",
            }
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(outcome.failed, 1);
        assert_eq!(outcome.decoded, 0);
        let marker = &value["input"][UNDECODABLE_MARKER_KEY];
        assert_eq!(marker["codec_id"], "reverse");
        assert_eq!(marker["reason"], UNDECODABLE_REASON_INVALID_BASE64);
    }

    #[test]
    fn decode_value_lossy_codec_decode_failure_yields_marker() {
        let codecs = lossy_test_codecs();
        let ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(b"ciphertext-bytes");
        let mut value = serde_json::json!({
            "input": {
                "_harvest_codec_envelope": 1,
                "codec_id": "failing",
                "data": ciphertext_b64,
            }
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(outcome.failed, 1);
        let marker = &value["input"][UNDECODABLE_MARKER_KEY];
        assert_eq!(marker["codec_id"], "failing");
        assert_eq!(marker["reason"], UNDECODABLE_REASON_CODEC_ERROR);
        // The marker must never echo the ciphertext (or the codec's own error
        // text, which could carry key material) back to the caller.
        let serialized = serde_json::to_string(&value).unwrap();
        assert!(
            !serialized.contains(&ciphertext_b64),
            "marker must not echo ciphertext: {serialized}"
        );
        assert!(
            !serialized.contains("simulated bad key"),
            "marker must not embed codec error text: {serialized}"
        );
    }

    #[test]
    fn decode_value_lossy_invalid_decoded_json_yields_marker() {
        // ReverseCodec decodes fine, but the plaintext bytes are not JSON.
        let codecs = lossy_test_codecs();
        let mut bytes = b"this is not json".to_vec();
        bytes.reverse();
        let mut value = serde_json::json!({
            "output": {
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
                "data": base64::engine::general_purpose::STANDARD.encode(bytes),
            }
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(outcome.failed, 1);
        let marker = &value["output"][UNDECODABLE_MARKER_KEY];
        assert_eq!(marker["reason"], UNDECODABLE_REASON_INVALID_JSON);
    }

    #[test]
    fn decode_value_lossy_non_envelope_values_pass_through_untouched() {
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "string": "plain",
            "number": 42,
            "bool": true,
            "null": null,
            "array": [1, {"nested": "obj"}, [2, 3]],
            "object": {"deep": {"deeper": {"leaf": "x"}}},
        });
        let pristine = value.clone();

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value, pristine, "non-envelope JSON must be byte-identical");
        assert_eq!(outcome, LossyDecodeOutcome::default());
        assert!(!outcome.touched());
    }

    #[test]
    fn decode_value_lossy_ignores_offload_envelope() {
        // The claim-check reference envelope (issue #524) has a different
        // discriminator key — it must pass through untouched.
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "output": {
                "_harvest_offload_envelope": 1,
                "store_id": "s3-main",
                "key": "blob/abc",
                "len": 2048,
                "checksum": "deadbeef",
            }
        });
        let pristine = value.clone();

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value, pristine);
        assert_eq!(outcome, LossyDecodeOutcome::default());
    }

    #[test]
    fn decode_value_lossy_ignores_erasure_tombstone() {
        // The PII-erasure tombstone (issue #495) must pass through untouched.
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({"input": {"_harvest_erased": true}});
        let pristine = value.clone();

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value, pristine);
        assert_eq!(outcome, LossyDecodeOutcome::default());
    }

    #[test]
    fn decode_value_lossy_ignores_non_envelope_object_with_codec_id_key() {
        // Same semantics as the strict decoder: business data that happens to
        // carry a `codec_id` field is not an envelope.
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "output": {"codec_id": "business-field", "value": 1}
        });
        let pristine = value.clone();

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value, pristine);
        assert_eq!(outcome, LossyDecodeOutcome::default());
    }

    #[test]
    fn decode_value_lossy_passes_through_malformed_envelope_variants() {
        // Near-envelopes must pass through untouched (same tolerance as the
        // strict `decode_payload`), never decode and never mark.
        let codecs = lossy_test_codecs();
        let malformed = [
            // Wrong version.
            serde_json::json!({
                "_harvest_codec_envelope": 2,
                "codec_id": "reverse",
                "data": "e30=",
            }),
            // Four keys.
            serde_json::json!({
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
                "data": "e30=",
                "extra": true,
            }),
            // Non-string data.
            serde_json::json!({
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
                "data": 42,
            }),
            // Non-string codec_id.
            serde_json::json!({
                "_harvest_codec_envelope": 1,
                "codec_id": 7,
                "data": "e30=",
            }),
            // Missing data field entirely (only 2 keys).
            serde_json::json!({
                "_harvest_codec_envelope": 1,
                "codec_id": "reverse",
            }),
        ];

        for near_envelope in malformed {
            let mut value = serde_json::json!({"input": near_envelope});
            let pristine = value.clone();
            let outcome = codecs.decode_value_lossy(&mut value);
            assert_eq!(value, pristine, "malformed variant must pass through");
            assert_eq!(outcome, LossyDecodeOutcome::default());
        }
    }

    #[test]
    fn decode_value_lossy_does_not_rescan_decoded_plaintext_for_envelopes() {
        // Single-pass rule: decoded plaintext that is itself envelope-shaped
        // (business data, or a frozen last_completion_result copy) must be
        // preserved verbatim — never decoded a second time, never marked.
        let codecs = lossy_test_codecs();
        let envelope_shaped_plaintext = unknown_codec_envelope();
        let mut value = serde_json::json!({
            "output": reverse_envelope(&envelope_shaped_plaintext),
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(
            value["output"], envelope_shaped_plaintext,
            "decoded plaintext must be preserved as data, not re-scanned"
        );
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 0
            }
        );
    }

    #[test]
    fn decode_value_lossy_recurses_into_arrays_and_nested_objects() {
        let codecs = lossy_test_codecs();
        let plain_a = serde_json::json!("alpha");
        let plain_b = serde_json::json!({"b": 2});
        let plain_c = serde_json::json!([3, 3, 3]);
        let mut value = serde_json::json!({
            "events": [
                {"data": {"input": reverse_envelope(&plain_a)}},
                {"data": {"output": reverse_envelope(&plain_b)}},
            ],
            "nested": {"deep": [reverse_envelope(&plain_c)]},
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value["events"][0]["data"]["input"], plain_a);
        assert_eq!(value["events"][1]["data"]["output"], plain_b);
        assert_eq!(value["nested"]["deep"][0], plain_c);
        assert_eq!(outcome.decoded, 3);
        assert_eq!(outcome.failed, 0);
    }

    #[test]
    fn decode_value_lossy_mixed_good_and_bad_envelopes_decodes_good_marks_bad() {
        // The per-field tolerance AC: one bad envelope never poisons its
        // siblings — the good field decodes, the bad one degrades to a marker.
        let codecs = lossy_test_codecs();
        let plain = serde_json::json!({"ok": true});
        let mut value = serde_json::json!({
            "good": reverse_envelope(&plain),
            "bad": unknown_codec_envelope(),
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(value["good"], plain);
        assert_eq!(
            value["bad"][UNDECODABLE_MARKER_KEY]["reason"],
            UNDECODABLE_REASON_UNKNOWN_CODEC
        );
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 1
            }
        );
    }

    #[test]
    fn decode_value_lossy_counts_decoded_and_failed_fields() {
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "a": reverse_envelope(&serde_json::json!(1)),
            "b": reverse_envelope(&serde_json::json!(2)),
            "c": reverse_envelope(&serde_json::json!(3)),
            "d": unknown_codec_envelope(),
            "e": unknown_codec_envelope(),
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 3,
                failed: 2
            }
        );
    }

    #[test]
    fn decode_error_string_lossy_decodes_stringified_envelope() {
        // TEXT error columns can carry a serialized envelope. A decoded
        // string plain payload is returned unwrapped (the original error was
        // a plain string before encoding).
        let codecs = lossy_test_codecs();
        let envelope = reverse_envelope(&serde_json::json!("boom"));
        let raw = serde_json::to_string(&envelope).unwrap();

        let (decoded, outcome) = codecs.decode_error_string_lossy(&raw);

        assert_eq!(decoded.as_deref(), Some("boom"));
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 0
            }
        );
    }

    #[test]
    fn decode_error_string_lossy_leaves_plain_error_text_untouched() {
        let codecs = lossy_test_codecs();

        let (decoded, outcome) =
            codecs.decode_error_string_lossy("activity failed: connection refused");

        assert_eq!(decoded, None, "plain error text must be left untouched");
        assert_eq!(outcome, LossyDecodeOutcome::default());
    }

    #[test]
    fn decode_error_string_lossy_leaves_non_envelope_json_untouched() {
        let codecs = lossy_test_codecs();

        let (decoded, outcome) =
            codecs.decode_error_string_lossy(r#"{"code": 500, "message": "downstream"}"#);

        assert_eq!(
            decoded, None,
            "JSON that is not exactly a codec envelope must be left untouched"
        );
        assert_eq!(outcome, LossyDecodeOutcome::default());
    }

    #[test]
    fn decode_error_string_lossy_failure_returns_serialized_marker() {
        // The fourth branch: the raw string IS exactly a serialized envelope,
        // but the codec's decode fails — the response copy gets the marker
        // serialized to a string, counted as `failed: 1`, and neither the
        // ciphertext nor the codec's own error text is echoed.
        let codecs = lossy_test_codecs();
        let ciphertext_b64 = base64::engine::general_purpose::STANDARD.encode(b"opaque-bytes");
        let raw = serde_json::to_string(&serde_json::json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "failing",
            "data": ciphertext_b64,
        }))
        .unwrap();

        let (rewritten, outcome) = codecs.decode_error_string_lossy(&raw);

        let rewritten = rewritten.expect("a failed decode must rewrite to the marker string");
        let marker: Value = serde_json::from_str(&rewritten).expect("marker string is JSON");
        assert_eq!(
            marker,
            undecodable_marker("failing", UNDECODABLE_REASON_CODEC_ERROR)
        );
        assert!(
            !rewritten.contains(&ciphertext_b64) && !rewritten.contains("simulated bad key"),
            "neither ciphertext nor codec error text may be echoed: {rewritten}"
        );
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 0,
                failed: 1
            }
        );
    }

    #[test]
    fn decode_error_string_lossy_serializes_non_string_decoded_json() {
        // The doc-comment contract: a decoded plain string comes back
        // unwrapped, but any other decoded JSON is returned serialized.
        let codecs = lossy_test_codecs();
        let envelope = reverse_envelope(&serde_json::json!({"code": 500}));
        let raw = serde_json::to_string(&envelope).unwrap();

        let (rewritten, outcome) = codecs.decode_error_string_lossy(&raw);

        assert_eq!(
            rewritten.as_deref(),
            Some(r#"{"code":500}"#),
            "non-string decoded JSON must be returned compact-serialized"
        );
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 0
            }
        );
    }

    #[test]
    fn decode_value_lossy_transforms_envelope_shaped_business_plaintext() {
        // Known limit (documented in docs/operations/read-path-decode.md):
        // envelopes are purely self-describing, so business data stored as
        // plaintext (identity write path) that is byte-for-byte a codec
        // envelope — at any nesting depth — is indistinguishable from a real
        // envelope and IS transformed by the walk: decoded when its codec_id
        // is registered, replaced with a marker when not.
        let codecs = lossy_test_codecs();
        let mut value = serde_json::json!({
            "batch": [
                {"business": reverse_envelope(&serde_json::json!({"n": 1}))},
                {"business": unknown_codec_envelope()},
            ]
        });

        let outcome = codecs.decode_value_lossy(&mut value);

        assert_eq!(
            value["batch"][0]["business"],
            serde_json::json!({"n": 1}),
            "registered codec_id ⇒ decoded even though it was business data"
        );
        assert_eq!(
            value["batch"][1]["business"],
            undecodable_marker("kms-rotated-away", UNDECODABLE_REASON_UNKNOWN_CODEC),
            "unregistered codec_id ⇒ the business object degrades to a marker"
        );
        assert_eq!(
            outcome,
            LossyDecodeOutcome {
                decoded: 1,
                failed: 1
            }
        );
    }

    #[test]
    fn undecodable_marker_shape_and_reasons_are_stable() {
        // Pins the marker key, the exact marker shape, and the four bounded
        // reason strings — the operator-facing degrade contract (issue #608).
        assert_eq!(UNDECODABLE_MARKER_KEY, "_harvest_undecodable");
        assert_eq!(UNDECODABLE_REASON_UNKNOWN_CODEC, "unknown_codec");
        assert_eq!(UNDECODABLE_REASON_INVALID_BASE64, "invalid_base64");
        assert_eq!(UNDECODABLE_REASON_CODEC_ERROR, "codec_error");
        assert_eq!(UNDECODABLE_REASON_INVALID_JSON, "invalid_json");

        let marker = undecodable_marker("kms-v1", UNDECODABLE_REASON_CODEC_ERROR);
        assert_eq!(
            marker,
            serde_json::json!({
                UNDECODABLE_MARKER_KEY: {
                    "codec_id": "kms-v1",
                    "reason": "codec_error",
                }
            })
        );
    }

    #[test]
    fn lossy_decode_outcome_merged_and_touched_semantics() {
        let zero = LossyDecodeOutcome::default();
        assert!(!zero.touched());

        let decoded_only = LossyDecodeOutcome {
            decoded: 2,
            failed: 0,
        };
        assert!(decoded_only.touched());

        let failed_only = LossyDecodeOutcome {
            decoded: 0,
            failed: 1,
        };
        assert!(failed_only.touched(), "a marked field is still a touch");

        let merged = decoded_only.merged(failed_only);
        assert_eq!(
            merged,
            LossyDecodeOutcome {
                decoded: 2,
                failed: 1
            }
        );
        assert_eq!(zero.merged(zero), zero);
    }
}
