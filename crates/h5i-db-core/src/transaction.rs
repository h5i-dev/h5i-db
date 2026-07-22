//! Multi-table atomic commits (ROADMAP §4.7, DESIGN §11.2).
//!
//! A `Transaction` stages writes to several tables and commits them with
//! **crash atomicity**: after a crash at any point, recovery leaves either
//! every table's HEAD advanced or none of them. The protocol:
//!
//! 1. Stage all segments and manifests for every table (nothing visible;
//!    staging leases protect the uploads from vacuum).
//! 2. Acquire every involved table's writer lock in deterministic
//!    (table-id) order — no deadlock between concurrent transactions.
//! 3. Revalidate every table's HEAD against the transaction's base; any
//!    mismatch aborts with `VersionConflict` before anything is published.
//! 4. Publish + fsync all manifests and their new segments.
//! 5. Write and fsync a **transaction journal** (`txn/<uuid>.json`) listing
//!    every target HEAD. The journal hitting disk is the commit point.
//! 6. Swap each HEAD in order; delete the journal.
//!
//! Crash before (5): no journal, no HEAD moved — the staged objects are
//! vacuum debris. Crash after (5): `Database::open` (read-write) finds the
//! journal and **rolls forward**, finishing the remaining HEAD swaps —
//! every manifest it needs was made durable in (4).
//!
//! What this does NOT give: a reader racing the swap loop can still observe
//! table A advanced and table B not yet (the window is microseconds, but it
//! exists). For coordinated reads, pin a snapshot — the guarantee here is
//! all-or-nothing *durability*, which is what cross-table ingest needs.

use std::collections::BTreeMap;

use arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::database::{CommitResult, Database, WriteOptions};
use crate::error::{Error, Result};
use crate::manifest::Head;

pub(crate) const TXN_PREFIX: &str = "txn";

pub(crate) fn txn_path(txn_id: Uuid) -> object_store::path::Path {
    object_store::path::Path::from(format!("{TXN_PREFIX}/{txn_id}.json"))
}

/// One table's target state in a journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TxnEntry {
    pub table_id: Uuid,
    pub table_name: String,
    /// Sequence the table must be at for the swap to apply (base).
    pub base_sequence: u64,
    /// The HEAD to install.
    pub new_head: Head,
}

/// The durable commit record. Its existence on disk IS the commit point:
/// recovery rolls the listed swaps forward, never back.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TxnJournal {
    pub txn_id: Uuid,
    pub created_at_ns: i64,
    pub entries: Vec<TxnEntry>,
    #[serde(default)]
    pub checksum: String,
}

impl TxnJournal {
    fn compute_checksum(&self) -> Result<String> {
        let mut clone = self.clone();
        clone.checksum = String::new();
        Ok(crate::util::checksum_hex(&serde_json::to_vec(&clone)?))
    }

    pub(crate) fn seal(mut self) -> Result<Self> {
        self.checksum = self.compute_checksum()?;
        Ok(self)
    }

    pub(crate) fn verify(&self, object: &str) -> Result<()> {
        if self.checksum != self.compute_checksum()? {
            return Err(Error::corruption(object, "txn journal checksum mismatch"));
        }
        Ok(())
    }
}

/// One staged per-table operation.
enum StagedOp {
    Append(Vec<RecordBatch>),
    Write(Vec<RecordBatch>),
}

/// A multi-table transaction builder. Obtain via [`Database::transaction`],
/// add operations, then [`Transaction::commit`].
pub struct Transaction<'a> {
    db: &'a Database,
    ops: BTreeMap<String, StagedOp>,
    opts: WriteOptions,
}

impl Database {
    /// Start a multi-table transaction. All operations commit atomically
    /// with respect to crashes (see module docs).
    pub fn transaction(&self) -> Transaction<'_> {
        Transaction {
            db: self,
            ops: BTreeMap::new(),
            opts: WriteOptions::default(),
        }
    }
}

impl<'a> Transaction<'a> {
    /// Strict ordered append to `table` (same validation as
    /// [`Database::append`]).
    pub fn append(&mut self, table: &str, batches: Vec<RecordBatch>) -> Result<&mut Self> {
        self.add(table, StagedOp::Append(batches))
    }

    /// Full-table replace of `table` (same validation as
    /// [`Database::write`]).
    pub fn write(&mut self, table: &str, batches: Vec<RecordBatch>) -> Result<&mut Self> {
        self.add(table, StagedOp::Write(batches))
    }

    /// Attach a note / user metadata applied to every table's manifest.
    pub fn with_options(&mut self, opts: WriteOptions) -> &mut Self {
        self.opts = opts;
        self
    }

    fn add(&mut self, table: &str, op: StagedOp) -> Result<&mut Self> {
        if self.ops.contains_key(table) {
            return Err(Error::invalid(format!(
                "transaction already contains an operation for table {table:?} \
                 (one operation per table)"
            )));
        }
        self.ops.insert(table.to_string(), op);
        Ok(self)
    }

    /// Stage everything, then run the journaled multi-head swap.
    pub async fn commit(self) -> Result<Vec<CommitResult>> {
        if self.ops.is_empty() {
            return Err(Error::invalid("empty transaction"));
        }
        let db = self.db;
        if db.is_read_only() {
            return Err(Error::ReadOnly {
                op: "transaction".into(),
            });
        }

        // ------------------------------------------------------------------
        // Stage phase: per table, validate + upload segments + build the
        // manifest — everything short of publishing it.
        // ------------------------------------------------------------------
        let mut staged: Vec<crate::database::StagedCommit> = Vec::new();
        for (table, op) in self.ops {
            let s = match op {
                StagedOp::Append(batches) => {
                    db.stage_append(&table, batches, &self.opts, false).await?
                }
                StagedOp::Write(batches) => db.stage_write(&table, batches, &self.opts).await?,
            };
            staged.push(s);
        }
        // Deterministic lock order.
        staged.sort_by_key(|s| s.entry.table_id);

        db.commit_staged_transaction(staged).await
    }
}

/// Recovery scan: called from `Database::open` on read-write opens. Rolls
/// forward any journal whose swaps did not all complete, then removes it.
pub(crate) async fn recover(db: &Database) -> Result<()> {
    let metas = db
        .backend()
        .list(&object_store::path::Path::from(TXN_PREFIX))
        .await?;
    for meta in metas {
        let bytes = db.backend().get(&meta.location).await?;
        let journal: TxnJournal = serde_json::from_slice(&bytes)
            .map_err(|e| Error::corruption(meta.location.as_ref(), format!("txn parse: {e}")))?;
        journal.verify(meta.location.as_ref())?;
        tracing::warn!(
            txn = %journal.txn_id,
            tables = journal.entries.len(),
            "found interrupted multi-table transaction; rolling forward"
        );
        for entry in &journal.entries {
            let current = db.backend().heads.read(entry.table_id).await?;
            let needs_swap = match &current {
                Some(state) => state.head.sequence < entry.new_head.sequence,
                // HEAD missing entirely: the table was dropped after the txn
                // (or never existed) — nothing to roll forward.
                None => false,
            };
            if needs_swap {
                let expected = current.as_ref().map(|s| s.tag.clone());
                // The manifest was fsynced before the journal, so the swap
                // is pure metadata. A concurrent writer having advanced the
                // head past base means the journal already applied there.
                db.backend()
                    .heads
                    .commit(
                        entry.table_id,
                        &entry.table_name,
                        expected.as_ref(),
                        &entry.new_head,
                        Box::pin(async { Ok(()) }),
                    )
                    .await?;
            }
        }
        db.backend().delete(&meta.location).await?;
    }
    Ok(())
}
