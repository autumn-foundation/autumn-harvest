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
        let keys = ["input", "output", "payload", "details"];
        for key in keys {
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
        let Some(obj) = payload.as_object() else {
            return Ok(payload.clone());
        };
        let Some(envelope_version) = obj.get("_harvest_codec_envelope").and_then(Value::as_i64)
        else {
            return Ok(payload.clone());
        };
        if envelope_version != 1 {
            return Ok(payload.clone());
        }
        let Some(codec_id) = obj.get("codec_id").and_then(Value::as_str) else {
            return Ok(payload.clone());
        };
        let Some(encoded_b64) = obj.get("data").and_then(Value::as_str) else {
            return Ok(payload.clone());
        };
        if obj.len() != 3 {
            return Ok(payload.clone());
        }
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
}
