//! Compatibility facade for consumers constrained to the object_store 0.13
//! package version. The maintained 0.14 implementation preserves the API
//! used by h5i-db, DataFusion 54, and Parquet 58.

pub use object_store_new::*;
