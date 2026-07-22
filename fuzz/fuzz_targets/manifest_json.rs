//! Fuzz the two JSON surfaces a reader parses from disk without any prior
//! trust: version manifests and the table HEAD. Malformed or truncated
//! bytes must produce a structured `Corruption` error, never a panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = h5i_db_core::manifest::VersionManifest::from_bytes(data, "fuzz");
    let _ = h5i_db_core::manifest::Head::from_bytes(data, "fuzz");
});
