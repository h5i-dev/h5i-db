//! Table specification: schema, time column, sort key, storage options.

use arrow::datatypes::{DataType, SchemaRef};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::util::{schema_from_b64, schema_to_b64};

/// Compression codec for Parquet segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    #[default]
    Zstd,
    Lz4,
    Snappy,
    Uncompressed,
}

/// Storage tuning options. These are tuning defaults, not format constants:
/// changing them affects only newly written segments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageOptions {
    /// Target uncompressed Arrow bytes per Parquet segment object.
    pub target_segment_bytes: u64,
    /// Target uncompressed bytes per Parquet row group.
    pub target_row_group_bytes: u64,
    pub codec: Codec,
    /// Columns to write Parquet split-block bloom filters for (opt-in).
    /// Aimed at high-cardinality entity columns (e.g. `symbol`) where the
    /// exact ≤128-value distinct-set pruning does not apply: a bloom answers
    /// `col = 'X'` at row-group granularity that min/max cannot. Empty by
    /// default, and when empty it is omitted from the serialized spec, so
    /// existing tables and their checksums are byte-for-byte unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bloom_filter_columns: Vec<String>,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            target_segment_bytes: 128 * 1024 * 1024,
            target_row_group_bytes: 32 * 1024 * 1024,
            codec: Codec::Zstd,
            bloom_filter_columns: Vec::new(),
        }
    }
}

/// Guard rails on the inline segment list (see DESIGN_CLAUDE.md §4).
pub const SEGMENT_COUNT_WARN: usize = 1024;
pub const SEGMENT_COUNT_HARD_DEFAULT: usize = 4096;

/// Options supplied at table creation.
#[derive(Debug, Clone, Default)]
pub struct TableOptions {
    /// Optional time index column; must be a timestamp or integer column.
    pub time_column: Option<String>,
    /// Sort key. If empty and `time_column` is set, defaults to the time
    /// column. `append` enforces this ordering.
    pub sort_key: Vec<String>,
    pub storage: StorageOptions,
    /// Hard cap on segments per manifest before writes demand compaction.
    pub max_segments_per_manifest: Option<usize>,
}

/// Persisted table specification (one JSON object per schema revision).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSpec {
    pub table_id: Uuid,
    pub name: String,
    /// Monotonic schema revision, bumped by schema-changing operations.
    pub schema_revision: u32,
    /// Arrow schema as base64 IPC (canonical, lossless).
    pub schema_ipc_b64: String,
    pub time_column: Option<String>,
    pub sort_key: Vec<String>,
    pub storage: StorageOptions,
    pub max_segments_per_manifest: usize,
    pub created_at_ns: i64,
    /// Checksum of the serialized spec minus this field (torn-write guard).
    #[serde(default)]
    pub checksum: String,
}

impl TableSpec {
    pub fn new(
        table_id: Uuid,
        name: &str,
        schema: &SchemaRef,
        options: &TableOptions,
    ) -> Result<Self> {
        let time_column = options.time_column.clone();
        if let Some(tc) = &time_column {
            let field = schema.field_with_name(tc).map_err(|_| {
                Error::invalid(format!("time column {tc:?} does not exist in the schema"))
            })?;
            match field.data_type() {
                DataType::Timestamp(_, _)
                | DataType::Int64
                | DataType::Int32
                | DataType::UInt64
                | DataType::UInt32
                | DataType::Date32
                | DataType::Date64 => {}
                other => {
                    return Err(Error::invalid(format!(
                        "time column {tc:?} must be a timestamp, date, or integer type, got {other}"
                    )))
                }
            }
            if field.is_nullable() {
                return Err(Error::invalid(format!(
                    "time column {tc:?} must be declared non-nullable"
                )));
            }
        }
        let mut sort_key = options.sort_key.clone();
        if sort_key.is_empty() {
            if let Some(tc) = &time_column {
                sort_key = vec![tc.clone()];
            }
        }
        for col in &sort_key {
            schema.field_with_name(col).map_err(|_| {
                Error::invalid(format!(
                    "sort key column {col:?} does not exist in the schema"
                ))
            })?;
        }
        if let (Some(tc), Some(first)) = (&time_column, sort_key.first()) {
            if first != tc {
                return Err(Error::invalid(format!(
                    "when a time column is declared it must be the first sort key \
                     (time column {tc:?}, sort key starts with {first:?})"
                )));
            }
        }
        for col in &options.storage.bloom_filter_columns {
            schema.field_with_name(col).map_err(|_| {
                Error::invalid(format!(
                    "bloom filter column {col:?} does not exist in the schema"
                ))
            })?;
        }
        let mut spec = Self {
            table_id,
            name: name.to_string(),
            schema_revision: 1,
            schema_ipc_b64: schema_to_b64(schema),
            time_column,
            sort_key,
            storage: options.storage.clone(),
            max_segments_per_manifest: options
                .max_segments_per_manifest
                .unwrap_or(SEGMENT_COUNT_HARD_DEFAULT),
            created_at_ns: crate::util::monotonic_commit_ts(None),
            checksum: String::new(),
        };
        spec.checksum = spec.compute_checksum()?;
        Ok(spec)
    }

    pub fn schema(&self) -> Result<SchemaRef> {
        schema_from_b64(&self.schema_ipc_b64)
    }

    pub fn compute_checksum(&self) -> Result<String> {
        let mut clone = self.clone();
        clone.checksum = String::new();
        Ok(crate::util::checksum_hex(&serde_json::to_vec(&clone)?))
    }

    pub fn verify_checksum(&self, object: &str) -> Result<()> {
        let expected = self.compute_checksum()?;
        if self.checksum != expected {
            return Err(Error::corruption(
                object,
                format!(
                    "spec checksum mismatch (stored {}, computed {})",
                    self.checksum, expected
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn ts_field(nullable: bool) -> Field {
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            nullable,
        )
    }

    fn base_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ts_field(false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn opts_with_time(tc: &str) -> TableOptions {
        TableOptions {
            time_column: Some(tc.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn new_defaults_sort_key_to_time_column() {
        let schema = base_schema();
        let spec =
            TableSpec::new(Uuid::new_v4(), "trades", &schema, &opts_with_time("ts")).unwrap();
        assert_eq!(spec.sort_key, vec!["ts".to_string()]);
        assert_eq!(spec.schema_revision, 1);
        assert_eq!(spec.name, "trades");
        assert_eq!(spec.max_segments_per_manifest, SEGMENT_COUNT_HARD_DEFAULT);
        // Schema round-trips losslessly through the stored IPC blob.
        assert_eq!(spec.schema().unwrap(), schema);
    }

    #[test]
    fn new_without_time_column_leaves_sort_key_empty() {
        let spec = TableSpec::new(
            Uuid::new_v4(),
            "t",
            &base_schema(),
            &TableOptions::default(),
        )
        .unwrap();
        assert!(spec.time_column.is_none());
        assert!(spec.sort_key.is_empty());
    }

    #[test]
    fn new_honors_explicit_max_segments() {
        let opts = TableOptions {
            time_column: Some("ts".into()),
            max_segments_per_manifest: Some(7),
            ..Default::default()
        };
        let spec = TableSpec::new(Uuid::new_v4(), "t", &base_schema(), &opts).unwrap();
        assert_eq!(spec.max_segments_per_manifest, 7);
    }

    #[test]
    fn time_column_must_exist() {
        let err = TableSpec::new(Uuid::new_v4(), "t", &base_schema(), &opts_with_time("nope"))
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
    }

    #[test]
    fn time_column_must_be_a_temporal_or_integer_type() {
        // "symbol" is Utf8 — not a valid time index.
        let err = TableSpec::new(
            Uuid::new_v4(),
            "t",
            &base_schema(),
            &opts_with_time("symbol"),
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
    }

    #[test]
    fn time_column_must_be_non_nullable() {
        let schema = Arc::new(Schema::new(vec![
            ts_field(true), // nullable time column
            Field::new("symbol", DataType::Utf8, false),
        ]));
        let err = TableSpec::new(Uuid::new_v4(), "t", &schema, &opts_with_time("ts")).unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
    }

    #[test]
    fn integer_time_column_is_accepted() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new("v", DataType::Float64, true),
        ]));
        let spec = TableSpec::new(Uuid::new_v4(), "t", &schema, &opts_with_time("seq")).unwrap();
        assert_eq!(spec.time_column.as_deref(), Some("seq"));
    }

    #[test]
    fn time_column_must_lead_the_sort_key() {
        let opts = TableOptions {
            time_column: Some("ts".into()),
            sort_key: vec!["symbol".into(), "ts".into()], // ts not first
            ..Default::default()
        };
        let err = TableSpec::new(Uuid::new_v4(), "t", &base_schema(), &opts).unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
    }

    #[test]
    fn multi_column_sort_key_with_time_first_is_ok() {
        let opts = TableOptions {
            time_column: Some("ts".into()),
            sort_key: vec!["ts".into(), "symbol".into()],
            ..Default::default()
        };
        let spec = TableSpec::new(Uuid::new_v4(), "t", &base_schema(), &opts).unwrap();
        assert_eq!(spec.sort_key, vec!["ts".to_string(), "symbol".to_string()]);
    }

    #[test]
    fn sort_key_columns_must_exist() {
        let opts = TableOptions {
            sort_key: vec!["ghost".into()],
            ..Default::default()
        };
        let err = TableSpec::new(Uuid::new_v4(), "t", &base_schema(), &opts).unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
    }

    #[test]
    fn bloom_filter_columns_must_exist() {
        let opts = TableOptions {
            time_column: Some("ts".into()),
            storage: StorageOptions {
                bloom_filter_columns: vec!["ghost".into()],
                ..Default::default()
            },
            ..Default::default()
        };
        let err = TableSpec::new(Uuid::new_v4(), "t", &base_schema(), &opts).unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
    }

    #[test]
    fn checksum_verifies_and_detects_tampering() {
        let mut spec =
            TableSpec::new(Uuid::new_v4(), "t", &base_schema(), &opts_with_time("ts")).unwrap();
        assert!(spec.verify_checksum("SPEC").is_ok());

        // Mutating a field without recomputing the checksum is corruption.
        spec.name = "renamed".into();
        let err = spec.verify_checksum("SPEC").unwrap_err();
        assert!(matches!(err, Error::Corruption { .. }));

        // Recomputing repairs it.
        spec.checksum = spec.compute_checksum().unwrap();
        assert!(spec.verify_checksum("SPEC").is_ok());
    }

    #[test]
    fn storage_defaults_omit_bloom_columns_from_json() {
        // Empty bloom set must not appear in the serialized spec (keeps golden
        // checksums byte-stable for existing tables).
        let spec =
            TableSpec::new(Uuid::new_v4(), "t", &base_schema(), &opts_with_time("ts")).unwrap();
        let json = serde_json::to_string(&spec).unwrap();
        assert!(!json.contains("bloom_filter_columns"), "json: {json}");
    }

    #[test]
    fn default_storage_options_are_sane() {
        let s = StorageOptions::default();
        assert_eq!(s.codec, Codec::Zstd);
        assert!(s.target_segment_bytes >= s.target_row_group_bytes);
        assert!(s.bloom_filter_columns.is_empty());
    }
}
