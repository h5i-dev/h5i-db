//! Version-aware mergeable finance aggregate states over immutable segments.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, Float64Array, Int64Array, LargeStringArray, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use h5i_db_core::{Database, ReadAt, ResolvedTable, SegmentMeta};
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};

const FORMAT: u32 = 1;
const SEMANTICS_VERSION: u32 = 1;
const PREFIX: &str = "cache/aggregates/v1";
const MAX_CACHE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AggregateStateMode {
    #[default]
    Disabled,
    ReadOnly,
    ReadWrite,
}

/// Fixed-semantics OHLCV + VWAP state. Required columns must be non-null;
/// prices and volumes must be finite. This deliberately narrow contract is
/// safer than pretending to support arbitrary SQL aggregate equivalence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinanceAggregateSpec {
    pub timestamp: String,
    pub price: String,
    pub volume: String,
    pub group_by: Option<String>,
}

impl FinanceAggregateSpec {
    pub fn ohlcv(
        timestamp: impl Into<String>,
        price: impl Into<String>,
        volume: impl Into<String>,
    ) -> Self {
        Self {
            timestamp: timestamp.into(),
            price: price.into(),
            volume: volume.into(),
            group_by: None,
        }
    }

    pub fn grouped_by(mut self, column: impl Into<String>) -> Self {
        self.group_by = Some(column.into());
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AggregateStateMetrics {
    pub states_requested: usize,
    pub states_reused: usize,
    pub states_built: usize,
    pub segments_scanned: usize,
    pub rows_scanned: u64,
    pub bytes_scheduled: u64,
    pub corrupt_entries: usize,
    pub evictions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinanceAggregate {
    pub group: Option<String>,
    pub rows: u64,
    pub first_timestamp: i64,
    pub last_timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub vwap: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinanceAggregateResult {
    pub table: String,
    pub sequence: u64,
    pub groups: Vec<FinanceAggregate>,
    pub metrics: AggregateStateMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct PointKey {
    timestamp: i64,
    segment_checksum: String,
    row: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct GroupState {
    group: Option<String>,
    rows: u64,
    open_key: PointKey,
    close_key: PointKey,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    price_volume: f64,
}

impl GroupState {
    fn new(group: Option<String>, key: PointKey, price: f64, volume: f64) -> Self {
        Self {
            group,
            rows: 1,
            open_key: key.clone(),
            close_key: key,
            open: price,
            high: price,
            low: price,
            close: price,
            volume,
            price_volume: price * volume,
        }
    }

    fn add(&mut self, key: PointKey, price: f64, volume: f64) -> bool {
        self.rows += 1;
        if key < self.open_key {
            self.open_key = key.clone();
            self.open = price;
        }
        if key > self.close_key {
            self.close_key = key;
            self.close = price;
        }
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.volume += volume;
        self.price_volume += price * volume;
        self.volume.is_finite() && self.price_volume.is_finite()
    }

    fn merge(&mut self, other: GroupState) -> bool {
        self.rows += other.rows;
        if other.open_key < self.open_key {
            self.open_key = other.open_key;
            self.open = other.open;
        }
        if other.close_key > self.close_key {
            self.close_key = other.close_key;
            self.close = other.close;
        }
        self.high = self.high.max(other.high);
        self.low = self.low.min(other.low);
        self.volume += other.volume;
        self.price_volume += other.price_volume;
        self.volume.is_finite() && self.price_volume.is_finite()
    }

    fn finish(self) -> FinanceAggregate {
        FinanceAggregate {
            group: self.group,
            rows: self.rows,
            first_timestamp: self.open_key.timestamp,
            last_timestamp: self.close_key.timestamp,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            vwap: (self.volume != 0.0).then_some(self.price_volume / self.volume),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AggregateStateEntry {
    format: u32,
    segment_checksum: String,
    schema_revision: u32,
    aggregate_plan_hash: String,
    expression_semantics_version: u32,
    source_row_count: u64,
    groups: Vec<GroupState>,
    checksum: String,
}

impl AggregateStateEntry {
    fn seal(&mut self) -> h5i_db_core::Result<()> {
        self.checksum.clear();
        self.checksum = h5i_db_core::util::checksum_hex(&serde_json::to_vec(self)?);
        Ok(())
    }

    fn verify(&self, segment: &SegmentMeta, plan_hash: &str) -> h5i_db_core::Result<()> {
        if self.format != FORMAT
            || self.segment_checksum != segment.checksum
            || self.schema_revision != segment.schema_revision
            || self.aggregate_plan_hash != plan_hash
            || self.expression_semantics_version != SEMANTICS_VERSION
            || self.source_row_count != segment.rows
        {
            return Err(h5i_db_core::Error::corruption(
                segment.path.as_str(),
                "aggregate state key mismatch",
            ));
        }
        let mut unsigned = self.clone();
        let stored = std::mem::take(&mut unsigned.checksum);
        let actual = h5i_db_core::util::checksum_hex(&serde_json::to_vec(&unsigned)?);
        if stored != actual {
            return Err(h5i_db_core::Error::corruption(
                segment.path.as_str(),
                "aggregate state checksum mismatch",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut rows = 0u64;
        for group in &self.groups {
            let valid = group.rows > 0
                && seen.insert(group.group.clone())
                && group.open_key.segment_checksum == self.segment_checksum
                && group.close_key.segment_checksum == self.segment_checksum
                && group.open.is_finite()
                && group.high.is_finite()
                && group.low.is_finite()
                && group.close.is_finite()
                && group.volume.is_finite()
                && group.price_volume.is_finite()
                && group.low <= group.open
                && group.open <= group.high
                && group.low <= group.close
                && group.close <= group.high;
            if !valid {
                return Err(h5i_db_core::Error::corruption(
                    segment.path.as_str(),
                    "invalid aggregate state payload",
                ));
            }
            rows = rows.checked_add(group.rows).ok_or_else(|| {
                h5i_db_core::Error::corruption(
                    segment.path.as_str(),
                    "aggregate state row count overflow",
                )
            })?;
        }
        if rows != self.source_row_count {
            return Err(h5i_db_core::Error::corruption(
                segment.path.as_str(),
                "aggregate state row count mismatch",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateStateStore {
    db: Arc<Database>,
    mode: AggregateStateMode,
}

impl AggregateStateStore {
    pub fn new(db: Arc<Database>, mode: AggregateStateMode) -> Self {
        Self { db, mode }
    }

    pub async fn finance_rollup(
        &self,
        table: &str,
        at: ReadAt,
        spec: &FinanceAggregateSpec,
    ) -> h5i_db_core::Result<FinanceAggregateResult> {
        let resolved = self.db.resolve(table, at).await?;
        let plan_hash = validate_and_hash(&resolved, spec)?;
        let mut metrics = AggregateStateMetrics {
            states_requested: resolved.manifest.segments.len(),
            ..Default::default()
        };
        let mut merged = BTreeMap::<Option<String>, GroupState>::new();

        for segment in &resolved.manifest.segments {
            let path = state_path(segment, &plan_hash);
            let state = if self.mode != AggregateStateMode::Disabled {
                self.load(&path, segment, &plan_hash, &mut metrics).await
            } else {
                None
            };
            let state = match state {
                Some(state) => {
                    metrics.states_reused += 1;
                    state
                }
                None => {
                    let mut state = self.build(segment, &plan_hash, spec).await?;
                    metrics.states_built += 1;
                    metrics.segments_scanned += 1;
                    metrics.rows_scanned += segment.rows;
                    metrics.bytes_scheduled += segment.bytes;
                    if self.mode == AggregateStateMode::ReadWrite && state.seal().is_ok() {
                        if let Ok(bytes) = serde_json::to_vec(&state) {
                            if let Ok(true) = self
                                .db
                                .backend()
                                .put_if_absent(&path, bytes::Bytes::from(bytes))
                                .await
                            {
                                metrics.evictions += crate::sidecar::enforce_budget(
                                    self.db.backend(),
                                    PREFIX,
                                    MAX_CACHE_BYTES,
                                )
                                .await
                                .unwrap_or(0)
                            }
                        }
                    }
                    state
                }
            };
            for group in state.groups {
                match merged.get_mut(&group.group) {
                    Some(current) => {
                        if !current.merge(group) {
                            return Err(h5i_db_core::Error::invalid(
                                "finance aggregate overflow while merging states",
                            ));
                        }
                    }
                    None => {
                        merged.insert(group.group.clone(), group);
                    }
                }
            }
        }
        Ok(FinanceAggregateResult {
            table: resolved.entry.name,
            sequence: resolved.manifest.sequence,
            groups: merged.into_values().map(GroupState::finish).collect(),
            metrics,
        })
    }

    async fn load(
        &self,
        path: &ObjectPath,
        segment: &SegmentMeta,
        plan_hash: &str,
        metrics: &mut AggregateStateMetrics,
    ) -> Option<AggregateStateEntry> {
        let bytes = self.db.backend().get_opt(path).await.ok()??;
        if let Ok(entry) = serde_json::from_slice::<AggregateStateEntry>(&bytes) {
            if entry.verify(segment, plan_hash).is_ok() {
                return Some(entry);
            }
        }
        metrics.corrupt_entries += 1;
        if self.mode == AggregateStateMode::ReadWrite {
            let _ = self.db.backend().delete(path).await;
        }
        None
    }

    async fn build(
        &self,
        segment: &SegmentMeta,
        plan_hash: &str,
        spec: &FinanceAggregateSpec,
    ) -> h5i_db_core::Result<AggregateStateEntry> {
        let mut projection = vec![
            spec.timestamp.clone(),
            spec.price.clone(),
            spec.volume.clone(),
        ];
        if let Some(group) = &spec.group_by {
            projection.push(group.clone());
        }
        let batches =
            h5i_db_core::segment::read_segment(self.db.backend(), segment, Some(&projection), None)
                .await?;
        let mut groups = BTreeMap::<Option<String>, GroupState>::new();
        let mut row_offset = 0u64;
        for batch in batches {
            let timestamp = batch
                .column_by_name(&spec.timestamp)
                .expect("validated column");
            let price = batch.column_by_name(&spec.price).expect("validated column");
            let volume = batch
                .column_by_name(&spec.volume)
                .expect("validated column");
            let group = spec
                .group_by
                .as_ref()
                .map(|name| batch.column_by_name(name).expect("validated column"));
            for row in 0..batch.num_rows() {
                let timestamp = timestamp_value(timestamp.as_ref(), row)?;
                let price = f64_value(price.as_ref(), row)?;
                let volume = f64_value(volume.as_ref(), row)?;
                if !price.is_finite() || !volume.is_finite() || !(price * volume).is_finite() {
                    return Err(h5i_db_core::Error::invalid(
                        "finance aggregate price and volume must be finite",
                    ));
                }
                let group = group
                    .as_ref()
                    .map(|array| string_value(array.as_ref(), row))
                    .transpose()?;
                let key = PointKey {
                    timestamp,
                    segment_checksum: segment.checksum.clone(),
                    row: row_offset + row as u64,
                };
                match groups.get_mut(&group) {
                    Some(state) => {
                        if !state.add(key, price, volume) {
                            return Err(h5i_db_core::Error::invalid(
                                "finance aggregate overflow while building state",
                            ));
                        }
                    }
                    None => {
                        groups.insert(group.clone(), GroupState::new(group, key, price, volume));
                    }
                }
            }
            row_offset += batch.num_rows() as u64;
        }
        if row_offset != segment.rows {
            return Err(h5i_db_core::Error::corruption(
                segment.path.as_str(),
                format!(
                    "aggregate state source row mismatch: expected {}, read {row_offset}",
                    segment.rows
                ),
            ));
        }
        Ok(AggregateStateEntry {
            format: FORMAT,
            segment_checksum: segment.checksum.clone(),
            schema_revision: segment.schema_revision,
            aggregate_plan_hash: plan_hash.to_string(),
            expression_semantics_version: SEMANTICS_VERSION,
            source_row_count: segment.rows,
            groups: groups.into_values().collect(),
            checksum: String::new(),
        })
    }
}

fn validate_and_hash(
    resolved: &ResolvedTable,
    spec: &FinanceAggregateSpec,
) -> h5i_db_core::Result<String> {
    let schema = &resolved.schema;
    let timestamp = schema.field_with_name(&spec.timestamp)?;
    let price = schema.field_with_name(&spec.price)?;
    let volume = schema.field_with_name(&spec.volume)?;
    if timestamp.is_nullable() || price.is_nullable() || volume.is_nullable() {
        return Err(h5i_db_core::Error::invalid(
            "finance aggregate columns must be non-null",
        ));
    }
    if !matches!(
        timestamp.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) | DataType::Int64
    ) || *price.data_type() != DataType::Float64
        || !matches!(volume.data_type(), DataType::Float64 | DataType::Int64)
    {
        return Err(h5i_db_core::Error::invalid(
            "finance aggregate requires timestamp/int64 time, float64 price, and float64/int64 volume",
        ));
    }
    let group_type = if let Some(group) = &spec.group_by {
        let field = schema.field_with_name(group)?;
        if field.is_nullable() || !matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8)
        {
            return Err(h5i_db_core::Error::invalid(
                "finance aggregate group must be a non-null string",
            ));
        }
        format!("{:?}", field.data_type())
    } else {
        "none".into()
    };
    let canonical = serde_json::to_vec(&(
        SEMANTICS_VERSION,
        (&spec.timestamp, format!("{:?}", timestamp.data_type())),
        (&spec.price, format!("{:?}", price.data_type())),
        (&spec.volume, format!("{:?}", volume.data_type())),
        (spec.group_by.as_deref(), group_type),
    ))?;
    Ok(blake3::hash(&canonical).to_hex().to_string())
}

fn state_path(segment: &SegmentMeta, plan_hash: &str) -> ObjectPath {
    let key = format!(
        "{}:{}:{}:{}",
        segment.checksum, segment.schema_revision, plan_hash, SEMANTICS_VERSION
    );
    let digest = blake3::hash(key.as_bytes()).to_hex().to_string();
    ObjectPath::from(format!("{PREFIX}/{}/{digest}.json", &digest[..2]))
}

fn timestamp_value(array: &dyn Array, row: usize) -> h5i_db_core::Result<i64> {
    if array.is_null(row) {
        return Err(h5i_db_core::Error::invalid("null aggregate timestamp"));
    }
    if let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(array.value(row));
    }
    Err(h5i_db_core::Error::invalid(
        "unsupported aggregate timestamp representation",
    ))
}

fn f64_value(array: &dyn Array, row: usize) -> h5i_db_core::Result<f64> {
    if array.is_null(row) {
        return Err(h5i_db_core::Error::invalid("null aggregate numeric value"));
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        let value = array.value(row);
        let converted = value as f64;
        if converted as i64 != value {
            return Err(h5i_db_core::Error::invalid(
                "int64 aggregate volume is not exactly representable as float64",
            ));
        }
        return Ok(converted);
    }
    Err(h5i_db_core::Error::invalid(
        "unsupported aggregate numeric representation",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sealed-entry checksum verifies by re-serializing parsed JSON, so
    /// parse∘serialize must be the identity for f64. Full-mantissa values —
    /// like the random-walk sums real tick data produces — caught serde_json's
    /// default lossy float parse marking every state corrupt at bench scale;
    /// the `float_roundtrip` feature is the fix this test pins down.
    #[test]
    fn sealed_checksum_survives_json_round_trip_with_full_mantissa_floats() {
        // Deterministic xorshift so the test needs no rand dependency.
        let mut state = 0x9e3779b97f4a7c15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 / (1u64 << 53) as f64
        };
        let mut groups = Vec::new();
        let mut price = 100.0_f64;
        for index in 0..512 {
            price += next() - 0.5;
            let volume = (next() * 1e6).floor() + 1.0;
            let key = PointKey {
                timestamp: 1_750_000_000_000_000_000 + index,
                segment_checksum: "seg".into(),
                row: index as u64,
            };
            groups.push(GroupState {
                group: Some(format!("SYM{index:04}")),
                rows: 1,
                open_key: key.clone(),
                close_key: key,
                open: price,
                high: price,
                low: price,
                close: price,
                volume,
                price_volume: price * volume,
            });
        }
        let mut entry = AggregateStateEntry {
            format: FORMAT,
            segment_checksum: "seg".into(),
            schema_revision: 1,
            aggregate_plan_hash: "plan".into(),
            expression_semantics_version: SEMANTICS_VERSION,
            source_row_count: 512,
            groups,
            checksum: String::new(),
        };
        entry.seal().unwrap();

        let published = serde_json::to_vec(&entry).unwrap();
        let parsed: AggregateStateEntry = serde_json::from_slice(&published).unwrap();
        let mut unsigned = parsed.clone();
        let stored = std::mem::take(&mut unsigned.checksum);
        let recomputed =
            h5i_db_core::util::checksum_hex(&serde_json::to_vec(&unsigned).unwrap());
        assert_eq!(
            stored, recomputed,
            "JSON round-trip must reproduce the sealed bytes exactly"
        );
    }
}

fn string_value(array: &dyn Array, row: usize) -> h5i_db_core::Result<String> {
    if array.is_null(row) {
        return Err(h5i_db_core::Error::invalid("null aggregate group"));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(row).to_string());
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(array.value(row).to_string());
    }
    Err(h5i_db_core::Error::invalid(
        "unsupported aggregate group representation",
    ))
}
