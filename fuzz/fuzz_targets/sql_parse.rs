//! Fuzz the SQL text surface. `H5iSession::sql` hands user text to
//! DataFusion, whose first stage is `DFParser`; arbitrary statements must
//! yield parse errors, never panics. (Planning/execution need a live
//! database and are covered by the query test suite instead.)
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = datafusion_sql::parser::DFParser::parse_sql(text);
    }
});
