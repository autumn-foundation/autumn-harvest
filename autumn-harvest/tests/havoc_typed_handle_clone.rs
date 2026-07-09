use autumn_harvest::handle_typed::TypedWorkflowHandle;

struct NotCloneable;

#[test]
fn test_clone() {
    fn assert_clone<T: Clone>() {}

    assert_clone::<TypedWorkflowHandle<NotCloneable>>();
}
