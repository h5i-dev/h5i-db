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
    /// Neumaier compensation for `volume` / `price_volume`. In-memory only
    /// (`serde(skip)`): `settle()` folds it into the totals before the entry is
    /// sealed or finished, so the persisted payload, its checksum, and the
    /// merge across segments all stay bit-for-bit what they were — this only
    /// makes the *within-segment* sum over up to ~120k ticks accurate, keeping
    /// the warm (cached) rollup equal to the cold recompute. See
    /// [`crate::finance::neumaier_add`].
    #[serde(skip, default)]
    c_volume: f64,
    #[serde(skip, default)]
    c_price_volume: f64,
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
            c_volume: 0.0,
            c_price_volume: 0.0,
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
        crate::finance::neumaier_add(&mut self.volume, &mut self.c_volume, volume);
        crate::finance::neumaier_add(
            &mut self.price_volume,
            &mut self.c_price_volume,
            price * volume,
        );
        (self.volume + self.c_volume).is_finite()
            && (self.price_volume + self.c_price_volume).is_finite()
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
        crate::finance::neumaier_add(
            &mut self.volume,
            &mut self.c_volume,
            other.volume + other.c_volume,
        );
        crate::finance::neumaier_add(
            &mut self.price_volume,
            &mut self.c_price_volume,
            other.price_volume + other.c_price_volume,
        );
        (self.volume + self.c_volume).is_finite()
            && (self.price_volume + self.c_price_volume).is_finite()
    }

    /// Fold the compensation terms into the totals and zero them. Called before
    /// an entry is sealed/serialized so the persisted `volume`/`price_volume`
    /// carry the corrected value and the wire format never sees the comp
    /// fields.
    fn settle(&mut self) {
        self.volume += self.c_volume;
        self.c_volume = 0.0;
        self.price_volume += self.c_price_volume;
        self.c_price_volume = 0.0;
    }

    fn finish(mut self) -> FinanceAggregate {
        self.settle();
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
        // Fold Neumaier compensation into the stored totals before checksumming
        // so the persisted payload is the corrected, comp-free value.
        for group in &mut self.groups {
            group.settle();
        }
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
                c_volume: 0.0,
                c_price_volume: 0.0,
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
        let recomputed = h5i_db_core::util::checksum_hex(&serde_json::to_vec(&unsigned).unwrap());
        assert_eq!(
            stored, recomputed,
            "JSON round-trip must reproduce the sealed bytes exactly"
        );
    }

    fn key(timestamp: i64, row: u64) -> PointKey {
        PointKey {
            timestamp,
            segment_checksum: "seg".into(),
            row,
        }
    }

    fn segment_meta(rows: u64) -> SegmentMeta {
        SegmentMeta {
            id: uuid::Uuid::nil(),
            path: "tables/t/segments/s.parquet".into(),
            rows,
            bytes: 1,
            checksum: "seg".into(),
            time_range: None,
            sorted: true,
            schema_revision: 1,
            created_by_sequence: 1,
            columns: Default::default(),
        }
    }

    fn sealed_entry() -> AggregateStateEntry {
        let mut state = GroupState::new(Some("A".into()), key(10, 0), 5.0, 1.0);
        assert!(state.add(key(20, 1), 7.0, 2.0));
        let mut entry = AggregateStateEntry {
            format: FORMAT,
            segment_checksum: "seg".into(),
            schema_revision: 1,
            aggregate_plan_hash: "plan".into(),
            expression_semantics_version: SEMANTICS_VERSION,
            source_row_count: 2,
            groups: vec![state],
            checksum: String::new(),
        };
        entry.seal().unwrap();
        entry
    }

    #[test]
    fn open_and_close_follow_key_order_not_insertion_order() {
        // Rows arrive out of time order; open/close must track the total
        // (timestamp, checksum, row) key, not arrival.
        let mut state = GroupState::new(Some("A".into()), key(20, 5), 200.0, 1.0);
        assert!(state.add(key(10, 0), 100.0, 1.0)); // earlier → becomes open
        assert!(state.add(key(30, 9), 300.0, 1.0)); // later → becomes close
        assert!(state.add(key(10, 1), 150.0, 1.0)); // same ts, higher row → not open
        let done = state.finish();
        assert_eq!((done.open, done.close), (100.0, 300.0));
        assert_eq!((done.first_timestamp, done.last_timestamp), (10, 30));
        assert_eq!(done.high, 300.0);
        assert_eq!(done.low, 100.0);
    }

    #[test]
    fn merge_matches_sequential_accumulation() {
        let mut left = GroupState::new(None, key(10, 0), 1.0, 1.0);
        assert!(left.add(key(20, 1), 2.0, 2.0));
        let mut right = GroupState::new(None, key(5, 0), 9.0, 1.0);
        assert!(right.add(key(40, 3), 4.0, 1.0));

        let mut merged = left.clone();
        assert!(merged.merge(right));
        let done = merged.finish();
        assert_eq!(done.rows, 4);
        assert_eq!((done.open, done.close), (9.0, 4.0));
        assert_eq!((done.first_timestamp, done.last_timestamp), (5, 40));
        assert_eq!(done.volume, 5.0);
    }

    #[test]
    fn non_finite_accumulation_is_reported_not_cached() {
        let mut state = GroupState::new(None, key(1, 0), f64::MAX, 1.0);
        assert!(
            !state.add(key(2, 1), f64::MAX, 1.0),
            "overflow to infinity must fail the add"
        );
    }

    #[test]
    fn verify_accepts_the_sealed_entry_and_rejects_tampering() {
        let entry = sealed_entry();
        entry.verify(&segment_meta(2), "plan").expect("valid entry");

        // Key mismatches: wrong segment, wrong plan, wrong row count.
        assert!(entry.verify(&segment_meta(3), "plan").is_err());
        let mut other = segment_meta(2);
        other.checksum = "other".into();
        assert!(entry.verify(&other, "plan").is_err());
        assert!(entry.verify(&segment_meta(2), "other-plan").is_err());

        // Payload tampering breaks the checksum.
        let mut tampered = sealed_entry();
        tampered.groups[0].volume += 1.0;
        assert!(tampered.verify(&segment_meta(2), "plan").is_err());

        // Duplicate groups are an invalid payload even with a valid checksum.
        let mut duplicated = sealed_entry();
        let clone = duplicated.groups[0].clone();
        duplicated.groups.push(clone);
        duplicated.source_row_count = 4;
        duplicated.seal().unwrap();
        assert!(duplicated.verify(&segment_meta(4), "plan").is_err());

        // Group rows not summing to the segment row count.
        let mut short = sealed_entry();
        short.source_row_count = 5;
        short.seal().unwrap();
        assert!(short.verify(&segment_meta(5), "plan").is_err());
    }

    #[test]
    fn int64_values_must_be_exactly_representable_as_f64() {
        use arrow::array::{Float64Array, Int64Array};
        let exact = Int64Array::from(vec![1i64 << 53]);
        assert_eq!(f64_value(&exact, 0).unwrap(), (1i64 << 53) as f64);
        let inexact = Int64Array::from(vec![(1i64 << 53) + 1]);
        assert!(f64_value(&inexact, 0).is_err());
        let floats = Float64Array::from(vec![1.5]);
        assert_eq!(f64_value(&floats, 0).unwrap(), 1.5);
        // Unsupported representation.
        let strings = StringArray::from(vec!["x"]);
        assert!(f64_value(&strings, 0).is_err());
    }

    #[test]
    fn timestamp_and_group_value_extraction_rejects_unsupported_arrays() {
        use arrow::array::{Float64Array, Int64Array};
        let ts = TimestampNanosecondArray::from(vec![7i64]);
        assert_eq!(timestamp_value(&ts, 0).unwrap(), 7);
        let ints = Int64Array::from(vec![9i64]);
        assert_eq!(timestamp_value(&ints, 0).unwrap(), 9);
        let floats = Float64Array::from(vec![1.0]);
        assert!(timestamp_value(&floats, 0).is_err());

        let strings = StringArray::from(vec!["A"]);
        assert_eq!(string_value(&strings, 0).unwrap(), "A");
        let large = LargeStringArray::from(vec!["B"]);
        assert_eq!(string_value(&large, 0).unwrap(), "B");
        assert!(string_value(&ints, 0).is_err());
    }
}
