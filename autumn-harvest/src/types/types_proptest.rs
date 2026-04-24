use proptest::prelude::*;
use crate::types::{ExecutionId, ShardId};

proptest! {
    #[test]
    fn test_execution_id_roundtrip_fuzz(shard_id in prop::num::i32::ANY) {
        let shard = ShardId::new(shard_id);
        let exec_id = ExecutionId::new_for_shard(shard);

        let masked = shard_id & 0xFFFF;
        let expected_shard = ShardId::new(masked);

        assert_eq!(exec_id.shard(), expected_shard);
    }
}
