//! Property tests for the AES-256-GCM payload codec (issue #1825).
//!
//! Contract under test:
//! - `decode(encode(x)) == x` for arbitrary bytes, keys and key ids;
//! - the same holds for arbitrary JSON through `PayloadCodecs`;
//! - a flipped bit anywhere in the stored bytes fails decode.

use std::sync::Arc;

use autumn_harvest::aead_codec::{AeadCodec, DATA_KEY_BYTES, DataKey};
use autumn_harvest::payload_codec::{PayloadCodec, PayloadCodecs};
use proptest::prelude::*;
use serde_json::Value;

use super::prop_config::config;

fn key() -> impl Strategy<Value = DataKey> {
    proptest::array::uniform32(any::<u8>())
        .prop_map(|bytes: [u8; DATA_KEY_BYTES]| DataKey::from_bytes(&bytes).unwrap())
}

fn key_id() -> impl Strategy<Value = String> {
    "[A-Za-z0-9._:-]{1,64}"
}

fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<String>().prop_map(Value::from),
    ];
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::from),
            proptest::collection::btree_map(any::<String>(), inner, 0..4)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

proptest! {
    #![proptest_config(config())]

    /// AC3: decode(encode(x)) == x for arbitrary payload bytes.
    #[test]
    fn decode_inverts_encode_for_arbitrary_bytes(
        key in key(),
        key_id in key_id(),
        raw in proptest::collection::vec(any::<u8>(), 0..4096),
    ) {
        let codec = AeadCodec::new(&key_id, &key).unwrap();
        let stored = codec.encode(&raw).unwrap();
        prop_assert_eq!(codec.decode(&stored).unwrap(), raw);
    }

    /// AC3 at the registry level: an arbitrary JSON payload survives the
    /// envelope, base64 and AEAD layers unchanged.
    #[test]
    fn decode_inverts_encode_for_arbitrary_json(key in key(), payload in json_value()) {
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(AeadCodec::new("k1", &key).unwrap()));
        let stored = codecs.encode_payload(&payload).unwrap();
        prop_assert_ne!(&stored, &payload);
        prop_assert_eq!(codecs.decode_payload(&stored).unwrap(), payload);
    }

    /// AC1: tamper detection holds for every bit of every stored payload.
    #[test]
    fn a_flipped_bit_always_fails_decode(
        key in key(),
        raw in proptest::collection::vec(any::<u8>(), 0..256),
        position in any::<proptest::sample::Index>(),
        bit in 0u8..8,
    ) {
        let codec = AeadCodec::new("k1", &key).unwrap();
        let mut stored = codec.encode(&raw).unwrap();
        let index = position.index(stored.len());
        stored[index] ^= 1 << bit;
        prop_assert!(codec.decode(&stored).is_err());
    }
}
