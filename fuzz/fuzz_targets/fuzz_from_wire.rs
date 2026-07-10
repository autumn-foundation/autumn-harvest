#![no_main]

use autumn_harvest::dlq::DlqGroupDimension;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let raw = String::from_utf8_lossy(data);
    let _ = DlqGroupDimension::from_wire(&raw);
});
