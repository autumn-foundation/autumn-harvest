#[cfg(test)]
mod tests {
    use crate::query::QueryRegistry;
    use std::sync::{Arc, Mutex};
    use serde_json::json;

    #[test]
    fn test_query_registry_deadlock() {
        let registry = Arc::new(Mutex::new(QueryRegistry::new()));

        let reg_clone = Arc::clone(&registry);
        registry.lock().unwrap().register("deadlock", Arc::new(move || {
            // Re-entrant execute causes deadlock if the lock is held across the callback
            let _ = reg_clone.lock().unwrap().execute("other");
            json!("bar")
        }));

        registry.lock().unwrap().register("other", Arc::new(|| json!("foo")));

        // This will deadlock!
        // wait, I can't write a test that literally deadlocks or cargo test will hang forever.
        // I can run it in a separate thread with a timeout.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = registry.lock().unwrap().execute("deadlock");
            tx.send(()).unwrap();
        });

        // If it doesn't finish in 1 second, it deadlocked
        let result = rx.recv_timeout(std::time::Duration::from_secs(1));
        assert!(result.is_err(), "Expected a deadlock, but it completed!");
    }
}
