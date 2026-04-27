with open("autumn-harvest/tests/havoc_tests.rs", "r") as f:
    content = f.read()

# Since `store` module is gated by #[cfg(feature = "db")] in src/lib.rs, we need to apply the same #[cfg(feature = "db")] gate to the `store::events_to_insert_rows_from` import and test.
new_content = content.replace("use autumn_harvest::{\n    event::WorkflowEvent, policy::RetryPolicy, store::events_to_insert_rows_from,\n    types::ExecutionId,\n};", "use autumn_harvest::{event::WorkflowEvent, policy::RetryPolicy, types::ExecutionId};\n#[cfg(feature = \"db\")]\nuse autumn_harvest::store::events_to_insert_rows_from;")
new_content = new_content.replace("#[test]\nfn test_havoc_event_id_overflow() {", "#[cfg(feature = \"db\")]\n#[test]\nfn test_havoc_event_id_overflow() {")

with open("autumn-harvest/tests/havoc_tests.rs", "w") as f:
    f.write(new_content)
