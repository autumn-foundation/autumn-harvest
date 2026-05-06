#[cfg(test)]
mod tests {
    use autumn_harvest::context::WorkflowContext;
    use autumn_harvest::types::ExecutionId;

    #[test]
    fn test_update_deadlock() {
        // use for_replay because testing feature might not be enabled
        let ctx = std::sync::Arc::new(WorkflowContext::for_replay(ExecutionId::new(), vec![]));

        // We want a validator that tries to access the update registry (by calling something on ctx).
        let ctx_clone = ctx.clone();
        ctx.register_update_handler(
            "deadlock_update",
            move |_| {
                // If the outer call is holding `update_registry.lock()`, this inner call will try to lock it again!
                // It will either panic (if Mutex is non-reentrant and catches it) or deadlock.
                ctx_clone.register_update_handler_no_validator("inner_update", |_| async {
                    Ok(serde_json::json!("ok"))
                });
                Ok(())
            },
            |_| async { Ok(serde_json::json!("ok")) },
        );

        // This will hold the lock while calling the validator.
        let _ = ctx.validate_update("deadlock_update", &serde_json::json!({}));
    }
}
