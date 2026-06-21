use autumn_harvest::handle_typed::TypedWorkflowHandle;
use std::rc::Rc;
use std::thread;

fn require_send<T: Send>(_: T) {}

fn main() {
    let mock_handle: TypedWorkflowHandle<Rc<()>> = unsafe { std::mem::MaybeUninit::uninit().assume_init() };
    require_send(mock_handle);
}
