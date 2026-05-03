use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine as _;
use serde_json::Value;

use crate::error::{HarvestError, HarvestResult};

pub trait PayloadCodec: Send + Sync {
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

#[derive(Debug, thiserror::Error)]
#[error("payload codec error: {0}")]
pub struct CodecError(pub String);

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
    pub fn register(&mut self, codec: Arc<dyn PayloadCodec>) {
        self.codecs.insert(codec.codec_id(), codec);
    }

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
