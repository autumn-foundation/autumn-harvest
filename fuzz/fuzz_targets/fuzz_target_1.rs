#![no_main]
use libfuzzer_sys::fuzz_target;
use autumn_harvest::dlq::failure_signature;

fuzz_target!(|data: &str| {
    let _ = failure_signature(data);
});
