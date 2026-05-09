//! Payload Codecs allow encryption, compression, or custom encoding of workflow payloads before they are persisted.
//!
//! **Why does this exist?**
//! By default, `autumn-harvest` stores workflow inputs, outputs, and event payloads as plain JSON in the database.
//! This might not be desirable for sensitive data (like PII or credentials), or for large payloads that could benefit
//! from compression.
//!
//! The Payload Codec system intercepts data at the database boundary. Before an event is saved, its payload fields
//! (like `input`, `output`, `payload`, `details`) are passed through the default codec to be encoded (e.g., encrypted
//! into a byte array). When the event is read back from the database, it's passed through the corresponding codec to be
//! decoded back into its original JSON form.
//!
//! # Codec Envelopes
//! When a payload is encoded, it is wrapped in a "Codec Envelope" - a specific JSON structure that tells `autumn-harvest`
//! how to decode it later. The envelope looks like this:
//! ```json
//! {
//!   "_harvest_codec_envelope": 1,
//!   "codec_id": "my-aes-codec",
//!   "data": "<base64-encoded-bytes>"
//! }
//! ```
//! The `codec_id` must match the string returned by [`PayloadCodec::codec_id`] for the codec used to encode it.
//!
//! # Examples
//!
//! Setting up a custom codec registry:
//!
//! ```
//! # use std::sync::Arc;
//! # use autumn_harvest::payload_codec::{PayloadCodecs, PayloadCodec, CodecError};
//!
//! #[derive(Debug)]
//! struct Base64Codec;
//!
//! impl PayloadCodec for Base64Codec {
//!     fn codec_id(&self) -> &'static str { "base64" }
//!
//!     fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
//!         // Dummy implementation
//!         Ok(raw.to_vec())
//!     }
//!
//!     fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
//!         // Dummy implementation
//!         Ok(encoded.to_vec())
//!     }
//! }
//!
//! let mut codecs = PayloadCodecs::default();
//!
//! // Register the codec so it can decode historical payloads
//! codecs.register(Arc::new(Base64Codec));
//!
//! // Set it as the default so new payloads are encoded with it
//! codecs.set_default(Arc::new(Base64Codec));
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine as _;
use serde_json::Value;

use crate::error::{HarvestError, HarvestResult};

/// A trait defining how payloads are transformed before storage and after retrieval.
///
/// **Why does this exist?**
/// This trait provides the contract `autumn-harvest` uses to encode or decode arbitrary byte
/// payloads (such as workflow inputs and outputs). Implementing this trait allows developers
/// to inject custom encryption, compression, or specialized serialization logic directly into
/// the framework's persistence boundary.
///
/// # Examples
///
/// ```
/// use autumn_harvest::payload_codec::{PayloadCodec, CodecError};
///
/// #[derive(Debug)]
/// struct ReversingCodec;
///
/// impl PayloadCodec for ReversingCodec {
///     fn codec_id(&self) -> &'static str { "reversing-codec" }
///
///     fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
///         let mut v = raw.to_vec();
///         v.reverse();
///         Ok(v)
///     }
///
///     fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
///         let mut v = encoded.to_vec();
///         v.reverse(); // Reverse it back
///         Ok(v)
///     }
/// }
/// ```
pub trait PayloadCodec: Send + Sync {
    /// Returns the unique string identifier for this codec.
    ///
    /// This identifier is stored alongside the encoded data. When retrieving the payload,
    /// `autumn-harvest` uses this ID to lookup the correct [`PayloadCodec`] for decoding.
    fn codec_id(&self) -> &'static str;
    /// Encodes raw payload bytes into their transformed representation for persistence.
    ///
    /// # Errors
    ///
    /// Returns a [`CodecError`] if the transformation fails (e.g., due to an invalid encryption key).
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError>;
    /// Decodes transformed payload bytes back into their original, raw form.
    ///
    /// # Errors
    ///
    /// Returns a [`CodecError`] if the transformation fails (e.g., corrupted data).
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError>;
}

/// Represents an error that occurs during payload encoding or decoding.
///
/// **Why does this exist?**
/// Transforming payload bytes can fail for a variety of reasons (e.g. invalid encryption keys,
/// corrupted data, out-of-memory). `CodecError` provides a unified error type that codec
/// implementations return, which `autumn-harvest` then escalates as a system error.
///
/// # Examples
///
/// ```
/// use autumn_harvest::payload_codec::CodecError;
///
/// let error = CodecError("Invalid encryption key".to_string());
/// assert_eq!(error.to_string(), "payload codec error: Invalid encryption key");
/// ```
#[derive(Debug, thiserror::Error)]
#[error("payload codec error: {0}")]
pub struct CodecError(pub String);

/// The default codec that performs no transformation.
///
/// **Why does this exist?**
/// By default, `autumn-harvest` assumes payloads do not need encryption or compression.
/// The `IdentityCodec` acts as a pass-through: it takes raw JSON bytes and returns them exactly as they are.
///
/// When using the `IdentityCodec`, payloads are not wrapped in a codec envelope; they remain as standard JSON.
///
/// # Examples
///
/// ```
/// use autumn_harvest::payload_codec::{IdentityCodec, PayloadCodec};
///
/// let codec = IdentityCodec;
/// let raw_data = b"{\"key\": \"value\"}";
///
/// assert_eq!(codec.encode(raw_data).unwrap(), raw_data);
/// assert_eq!(codec.decode(raw_data).unwrap(), raw_data);
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

/// A registry of payload codecs used by the system.
///
/// **Why does this exist?**
/// A worker or system might need to understand multiple codecs at once (e.g., when migrating from
/// an old encryption scheme to a new one). `PayloadCodecs` maps a `codec_id` to its corresponding
/// [`PayloadCodec`] implementation, allowing the system to dynamically look up the correct decoder
/// for historical data.
///
/// It also maintains a "default" codec, which is used for encoding *new* payloads.
///
/// By default, this registry is populated with a single [`IdentityCodec`] set as the default.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use autumn_harvest::payload_codec::{PayloadCodecs, IdentityCodec};
///
/// let mut codecs = PayloadCodecs::default();
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
    /// Registers a codec so that the system can use it to decode historical payloads.
    ///
    /// **Why does this exist?**
    /// When you change your encryption keys or algorithm, older workflows in the database
    /// will still be encoded with the old codec. By registering the old codec alongside the new one,
    /// `autumn-harvest` can dynamically select the correct decoder based on the `codec_id`
    /// found in the payload's envelope.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use autumn_harvest::payload_codec::{PayloadCodecs, IdentityCodec};
    ///
    /// let mut codecs = PayloadCodecs::default();
    /// codecs.register(Arc::new(IdentityCodec)); // Now the system knows how to handle "identity"
    /// ```
    pub fn register(&mut self, codec: Arc<dyn PayloadCodec>) {
        self.codecs.insert(codec.codec_id(), codec);
    }

    /// Sets the primary codec used for encoding *new* payloads.
    ///
    /// **Why does this exist?**
    /// When a workflow event is saved to the database, its payloads need to be transformed.
    /// The default codec is the one applied to all outgoing data.
    ///
    /// Note: Calling `set_default` also automatically `register`s the codec so that data it encodes
    /// can be decoded later.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use autumn_harvest::payload_codec::{PayloadCodecs, IdentityCodec};
    ///
    /// let mut codecs = PayloadCodecs::default();
    /// codecs.set_default(Arc::new(IdentityCodec)); // All new events will use this codec
    /// ```
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
