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
