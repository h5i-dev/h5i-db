//! `h5i-db context`: everything an agent needs to know before its first query.
//!
//! Without this, orientation costs one round trip per table per fact —
//! `tables`, then `schema`, then `sample`, then `versions`, repeated across
//! the catalog. That is where an agent's first thirty seconds go, and the
//! answers are all already in the manifests, so one call can return them.
//!
//! Two properties make it usable as a cached preamble (an `AGENTS.md`
//! fragment, say):
//!
//! * **Deterministic by default.** The same database state produces the same
//!   bytes. Nothing clock-dependent appears unless `--stale-after` asks for
//!   it, which is the only flag that makes the output a function of *now*.
//! * **Budgeted.** `--budget` caps the answer in tokens, shedding detail in a
//!   fixed order and naming what it dropped, so a wide catalog degrades to a
//!   still-useful summary instead of blowing the context window.

use std::collections::BTreeMap;

use h5i_db_core::{Database, ReadAt, Result};
use serde_json::{Value, json};

/// Characters per token, for the `--budget` estimate. Deliberately crude: the
/// point is to bound the answer, not to model a tokenizer.
const CHARS_PER_TOKEN: usize = 4;

/// What gets shed, in order, when the budget is tight. Earlier entries are
/// dropped first because they are the most verbose per unit of usefulness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shed {
    /// Per-column type listings — recoverable with `h5i-db schema`.
    Columns,
    /// Snapshot names.
    Snapshots,
    /// Free-text commit notes.
    Notes,
    /// Whole tables, smallest row count first.
    Tables,
}

const SHED_ORDER: [Shed; 4] = [Shed::Columns, Shed::Snapshots, Shed::Notes, Shed::Tables];

impl Shed {
    fn label(self) -> &'static str {
        match self {
            Shed::Columns => "columns",
            Shed::Snapshots => "snapshots",
            Shed::Notes => "notes",
            Shed::Tables => "tables",
        }
    }
}

/// Build the situational-awareness document.
///
/// `stale_after_seconds` opts into freshness reporting; without it the output
/// carries no clock-dependent field.
pub async fn build(
    db: &Database,
    label: &str,
    budget_tokens: Option<usize>,
    stale_after_seconds: Option<u64>,
) -> Result<Value> {
    // Sorted by name so the document is byte-stable across runs.
    let mut entries = db.list_tables().await?;
    entries.sort_by(|a, b| a.name.cmp(&b.name));

    let now_ns = stale_after_seconds.map(|_| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    });

    let mut tables = Vec::with_capacity(entries.len());
    for entry in &entries {
        let resolved = db.resolve(&entry.name, ReadAt::Latest).await?;
        let m = &resolved.manifest;

        let columns: Vec<Value> = resolved
            .schema
            .fields()
            .iter()
            .map(|f| {
                json!({
                    "name": f.name(),
                    "type": f.data_type().to_string(),
                    "nullable": f.is_nullable(),
                })
            })
            .collect();

        // Pending plans are the single most important thing an agent can be
        // unaware of: work already staged and awaiting review.
        let plans = db.list_plans(&entry.name).await.unwrap_or_default();
        let pending: Vec<Value> = plans
            .iter()
            .map(|p| {
                json!({
                    "plan_id": p.plan_id,
                    "op": p.op.to_string(),
                    "base_version": p.base_version,
                    "expired": p.is_expired(),
                    "rows_affected": p.summary.rows_affected,
                })
            })
            .collect();

        let mut table = json!({
            "name": entry.name,
            "version": m.sequence,
            "rows": m.rows,
            "bytes": m.bytes,
            "segments": m.segments.len(),
            "schema_revision": m.schema_revision,
            "time_column": resolved.spec.time_column,
            "sort_key": resolved.spec.sort_key,
            "time_range": m.time_range,
            "columns": columns,
            "last_commit": {
                "op": m.op.to_string(),
                "at": chrono::DateTime::from_timestamp_nanos(m.committed_at_ns).to_rfc3339(),
                "note": m.note,
            },
            "data_policy": db.data_policy(&entry.name).await?.is_some(),
            "pending_plans": pending,
        });

        // Freshness is a function of the wall clock, so it appears only when
        // the caller asked for it — that is what keeps the default output
        // reproducible.
        if let (Some(now), Some(limit)) = (now_ns, stale_after_seconds) {
            let age_ns = now.saturating_sub(m.committed_at_ns).max(0);
            // Compare at nanosecond precision, not on the rounded seconds:
            // `--stale-after 0s` means "anything not committed this instant",
            // which a truncated age of 0 would silently answer "fresh".
            let limit_ns = (limit as i128) * 1_000_000_000;
            table["freshness"] = json!({
                "last_commit_age_seconds": age_ns / 1_000_000_000,
                "stale_after_seconds": limit,
                "stale": age_ns as i128 > limit_ns,
            });
        }
        tables.push(table);
    }

    let snapshots: Vec<String> = db
        .list_snapshots()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|s| s.name)
        .collect();

    let policy = db.policy().await?;
    // Name only the operations the policy actually gates: a list of things
    // that will fail is more actionable than the full boolean matrix.
    let plan_required: Vec<&str> = [
        ("append", policy.direct_append),
        ("write", policy.direct_write),
        ("replace", policy.direct_replace),
        ("delete", policy.direct_delete),
        ("restore", policy.direct_restore),
        ("compact", policy.direct_compact),
    ]
    .into_iter()
    .filter(|(_, allowed)| !*allowed)
    .map(|(op, _)| op)
    .collect();

    let mut doc = json!({
        "schema_version": SCHEMA_VERSION,
        "database": label,
        "tables": tables,
        "snapshots": snapshots,
        "plan_required_for": plan_required,
    });

    if let Some(budget) = budget_tokens {
        apply_budget(&mut doc, budget);
    }
    Ok(doc)
}

pub const SCHEMA_VERSION: u32 = 1;

fn estimate_tokens(doc: &Value) -> usize {
    serde_json::to_string(doc).map(|s| s.len()).unwrap_or(0) / CHARS_PER_TOKEN
}

/// Shed detail until the document fits, recording every drop.
///
/// Nothing is removed silently: whatever went missing is counted under
/// `omitted`, because a truncated answer that looks complete is worse than a
/// visibly partial one. Each entry is a fixed-size `{dropped, recover_with}` —
/// listing the dropped names instead would let the bookkeeping outgrow the
/// savings, which is how a *tighter* budget ends up producing a *larger*
/// document.
fn apply_budget(doc: &mut Value, budget: usize) {
    let mut omitted = BTreeMap::<String, Value>::new();
    for step in SHED_ORDER {
        // Measure the document as it would actually be emitted — the omission
        // record included. Checking the size without it lets a shed that just
        // met the budget be pushed back over it by its own bookkeeping.
        sync_omitted(doc, &omitted, budget);
        if estimate_tokens(doc) <= budget {
            break;
        }
        match step {
            Shed::Columns => {
                let mut dropped = 0usize;
                if let Some(tables) = doc["tables"].as_array_mut() {
                    for t in tables.iter_mut() {
                        if let Some(cols) = t["columns"].as_array() {
                            dropped += cols.len();
                        }
                        // Keep the count: an agent still learns the shape and
                        // knows exactly which command recovers the detail.
                        let n = t["columns"].as_array().map(|c| c.len()).unwrap_or(0);
                        t["column_count"] = json!(n);
                        t.as_object_mut().map(|o| o.remove("columns"));
                    }
                }
                if dropped > 0 {
                    omitted.insert(
                        step.label().into(),
                        json!({"dropped": dropped, "recover_with": "h5i-db schema <db> <table>"}),
                    );
                }
            }
            Shed::Snapshots => {
                let n = doc["snapshots"].as_array().map(|s| s.len()).unwrap_or(0);
                if n > 0 {
                    doc["snapshots"] = json!([]);
                    omitted.insert(
                        step.label().into(),
                        json!({"dropped": n, "recover_with": "h5i-db snapshot list <db>"}),
                    );
                }
            }
            Shed::Notes => {
                let mut dropped = 0usize;
                if let Some(tables) = doc["tables"].as_array_mut() {
                    for t in tables.iter_mut() {
                        if !t["last_commit"]["note"].is_null() {
                            t["last_commit"]["note"] = Value::Null;
                            dropped += 1;
                        }
                    }
                }
                if dropped > 0 {
                    omitted.insert(
                        step.label().into(),
                        json!({"dropped": dropped, "recover_with": "h5i-db versions <db> <table>"}),
                    );
                }
            }
            Shed::Tables => {
                // Last resort. Drop the smallest tables first: the big ones
                // are where the work is. One at a time, re-measuring the whole
                // document each round so we stop as soon as it fits.
                let mut dropped = 0usize;
                loop {
                    let remaining = doc["tables"].as_array().map(|t| t.len()).unwrap_or(0);
                    if remaining <= 1 {
                        break;
                    }
                    let mut probe = omitted.clone();
                    probe.insert(
                        step.label().into(),
                        json!({"dropped": dropped, "recover_with": "h5i-db tables <db>"}),
                    );
                    sync_omitted(doc, &probe, budget);
                    if estimate_tokens(doc) <= budget {
                        break;
                    }
                    if let Some(tables) = doc["tables"].as_array_mut() {
                        let idx = tables
                            .iter()
                            .enumerate()
                            .min_by_key(|(_, t)| t["rows"].as_u64().unwrap_or(0))
                            .map(|(i, _)| i)
                            .unwrap_or(0);
                        tables.remove(idx);
                    }
                    dropped += 1;
                }
                if dropped > 0 {
                    omitted.insert(
                        step.label().into(),
                        json!({
                            "dropped": dropped,
                            "recover_with": "h5i-db tables <db>",
                        }),
                    );
                }
            }
        }
    }
    sync_omitted(doc, &omitted, budget);
}

/// Reflect the omission record into the document, so every size measurement
/// sees exactly what will be emitted.
fn sync_omitted(doc: &mut Value, omitted: &BTreeMap<String, Value>, budget: usize) {
    if omitted.is_empty() {
        if let Some(obj) = doc.as_object_mut() {
            obj.remove("omitted");
            obj.remove("budget_tokens");
        }
    } else {
        doc["omitted"] = json!(omitted);
        doc["budget_tokens"] = json!(budget);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_with(tables: usize, cols: usize) -> Value {
        let tables: Vec<Value> = (0..tables)
            .map(|i| {
                json!({
                    "name": format!("table_{i}"),
                    "rows": (i as u64 + 1) * 100,
                    "columns": (0..cols).map(|c| json!({
                        "name": format!("column_number_{c}"),
                        "type": "Timestamp(Nanosecond, Some(\"UTC\"))",
                        "nullable": false,
                    })).collect::<Vec<_>>(),
                    "last_commit": {"op": "append", "at": "2026-07-01T00:00:00Z", "note": "a note"},
                })
            })
            .collect();
        json!({
            "tables": tables,
            "snapshots": ["one", "two"],
        })
    }

    #[test]
    fn a_document_within_budget_is_left_alone() {
        let mut doc = doc_with(1, 1);
        let before = doc.clone();
        apply_budget(&mut doc, 10_000);
        assert!(doc["tables"][0]["columns"].is_array(), "nothing to shed");
        assert_eq!(doc["tables"], before["tables"]);
        assert!(doc.get("omitted").is_none(), "no omissions to report");
    }

    #[test]
    fn columns_go_first_and_the_drop_is_named() {
        let mut doc = doc_with(4, 30);
        apply_budget(&mut doc, 200);
        assert!(
            doc["tables"][0]["columns"].is_null(),
            "columns are the first thing shed"
        );
        assert_eq!(doc["tables"][0]["column_count"], 30, "the shape survives");
        assert!(
            doc["omitted"]["columns"]["recover_with"]
                .as_str()
                .unwrap()
                .contains("schema"),
            "a dropped section must say how to get it back"
        );
    }

    #[test]
    fn a_tight_budget_drops_the_smallest_tables_and_names_them() {
        let mut doc = doc_with(6, 20);
        apply_budget(&mut doc, 40);
        let names: Vec<&str> = doc["tables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(!names.is_empty(), "never sheds everything");
        assert!(
            names.contains(&"table_5"),
            "the biggest table must survive: {names:?}"
        );
        let dropped = &doc["omitted"]["tables"];
        assert_eq!(
            dropped["dropped"].as_u64().unwrap() as usize + names.len(),
            6,
            "every dropped table is accounted for: {dropped}"
        );
    }

    #[test]
    fn shedding_is_monotone_in_the_budget() {
        // A tighter budget never yields a bigger document — the property that
        // makes --budget worth trusting.
        let mut prev = usize::MAX;
        for budget in [4000usize, 800, 300, 120, 40] {
            let mut doc = doc_with(5, 25);
            apply_budget(&mut doc, budget);
            let size = estimate_tokens(&doc);
            assert!(
                size <= prev,
                "budget {budget} produced {size} tokens, larger than the previous {prev}"
            );
            prev = size;
        }
    }
}
