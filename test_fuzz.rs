use std::sync::{Arc, Mutex};
struct QueryRegistry {
    handlers: std::collections::HashMap<String, Arc<dyn Fn() -> String + Send + Sync>>
}
impl QueryRegistry {
    fn get(&self, name: &str) -> Option<Arc<dyn Fn() -> String + Send + Sync>> {
        self.handlers.get(name).cloned()
    }
}
struct Context {
    registry: Mutex<QueryRegistry>
}
impl Context {
    fn execute_query(&self, name: &str) -> Option<String> {
        let handler = self.registry.lock().unwrap().get(name);
        handler.map(|h| h())
    }
}
