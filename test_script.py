import sys

def main():
    content = ""
    with open("autumn-harvest/src/query.rs", "r") as f:
        content = f.read()

    old_block = """    /// Execute a registered query handler.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use autumn_harvest::query::QueryRegistry;
    /// use std::sync::Arc;
    /// use serde_json::json;
    ///
    /// let mut registry = QueryRegistry::new();
    /// registry.register("health", Arc::new(|| json!({ "status": "ok" })));
    ///
    /// let result = registry.execute("health").unwrap();
    /// assert_eq!(result, json!({ "status": "ok" }));
    ///
    /// let missing = registry.execute("unknown");
    /// assert!(missing.is_err());
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::NotFound`] if `name` is not registered.
    pub fn execute(&self, name: &str) -> HarvestResult<Value> {
        self.handlers
            .get(name)
            .map(|handler| handler())
            .ok_or_else(|| HarvestError::NotFound(format!("query handler '{name}'")))
    }"""

    content = content.replace(old_block, "")

    old_test = """    #[test]
    fn executes_registered_query() {
        let mut reg = QueryRegistry::new();
        reg.register("status", Arc::new(|| serde_json::json!({"ok": true})));

        let result = reg.execute("status").expect("query must be found");
        assert_eq!(result, serde_json::json!({"ok": true}));
    }"""

    new_test = """    #[test]
    fn executes_registered_query() {
        let mut reg = QueryRegistry::new();
        reg.register("status", Arc::new(|| serde_json::json!({"ok": true})));

        let handler = reg.get("status").expect("query must be found");
        let result = handler();
        assert_eq!(result, serde_json::json!({"ok": true}));
    }

    #[test]
    fn executes_registered_query_without_holding_borrow() {
        let reg = std::cell::RefCell::new(QueryRegistry::new());
        reg.borrow_mut().register("status", Arc::new(|| serde_json::json!({"ok": true})));

        let handler = reg.borrow().get("status").expect("query must be found");
        // The RefCell borrow is completely dropped here, allowing safe execution and re-entrancy.
        assert_eq!(handler(), serde_json::json!({"ok": true}));
    }"""

    content = content.replace(old_test, new_test)

    with open("autumn-harvest/src/query.rs", "w") as f:
        f.write(content)

if __name__ == "__main__":
    main()
