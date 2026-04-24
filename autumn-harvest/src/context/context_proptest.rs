use proptest::prelude::*;
use crate::context::WorkflowContext;
use std::sync::Arc;
use std::thread;

proptest! {
    #[test]
    fn test_context_side_effect_concurrency(id in ".*") {
        let ctx = Arc::new(WorkflowContext::new_test());
        let mut handles = vec![];

        for _ in 0..10 {
            let ctx_clone = Arc::clone(&ctx);
            let id_clone = id.clone();
            handles.push(thread::spawn(move || {
                let _ = ctx_clone.side_effect(&id_clone, || 42);
            }));
        }

        for handle in handles {
            let _ = handle.join();
        }
    }
}
