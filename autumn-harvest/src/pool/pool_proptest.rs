use proptest::prelude::*;
use crate::pool::HarvestPoolConfig;

proptest! {
    #[test]
    fn test_pool_validate_fuzz(worker_pool in prop::num::usize::ANY, web_pool in prop::num::usize::ANY, max_total in prop::num::usize::ANY) {
        let config = HarvestPoolConfig {
            worker_pool_size: worker_pool,
            max_total_connections: max_total,
        };
        let _ = config.validate(web_pool);
    }
}
