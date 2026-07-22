//! Mutation policy: whether each operation may commit directly or must go
//! through the reviewed plan/apply flow.
//!
//! The single-write-path invariant holds regardless: direct and planned
//! commits share the same segment writer and CAS commit; the policy only
//! controls whether the preview/approval step may be skipped. Defaults suit
//! local personal use (everything direct except vacuum's dry-run default);
//! agent/CI environments tighten it with `h5i-db policy set`.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::manifest::OpKind;
use crate::Backend;

const POLICY_FILE: &str = "POLICY";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct MutationPolicy {
    /// Allow `append`/`ingest --mode append` without a reviewed plan.
    pub direct_append: bool,
    /// Allow full-table `write` without a reviewed plan.
    pub direct_write: bool,
    /// Allow `replace_range` without a reviewed plan.
    pub direct_replace: bool,
    /// Allow `delete_range` without a reviewed plan.
    pub direct_delete: bool,
    /// Allow `restore` without a reviewed plan.
    pub direct_restore: bool,
    /// Allow `compact` (data-identical) without a reviewed plan.
    pub direct_compact: bool,
}

impl Default for MutationPolicy {
    fn default() -> Self {
        Self {
            direct_append: true,
            direct_write: true,
            direct_replace: true,
            direct_delete: true,
            direct_restore: true,
            direct_compact: true,
        }
    }
}

impl MutationPolicy {
    /// Check whether `op` may run directly; error tells the caller to plan.
    pub fn check_direct(&self, op: OpKind) -> Result<()> {
        let allowed = match op {
            OpKind::Create => true,
            OpKind::Write => self.direct_write,
            OpKind::Append => self.direct_append,
            OpKind::ReplaceRange => self.direct_replace,
            OpKind::DeleteRange => self.direct_delete,
            OpKind::Restore => self.direct_restore,
            OpKind::Compact => self.direct_compact,
        };
        if allowed {
            Ok(())
        } else {
            Err(Error::PolicyViolation { op: op.to_string() })
        }
    }

    pub fn set(&mut self, key: &str, value: bool) -> Result<()> {
        match key {
            "direct_append" => self.direct_append = value,
            "direct_write" => self.direct_write = value,
            "direct_replace" => self.direct_replace = value,
            "direct_delete" => self.direct_delete = value,
            "direct_restore" => self.direct_restore = value,
            "direct_compact" => self.direct_compact = value,
            other => {
                return Err(Error::invalid(format!(
                    "unknown policy key {other:?}; keys: direct_append, direct_write, \
                     direct_replace, direct_delete, direct_restore, direct_compact"
                )))
            }
        }
        Ok(())
    }
}

pub async fn load(backend: &Backend) -> Result<MutationPolicy> {
    match backend
        .get_opt(&object_store::path::Path::from(POLICY_FILE))
        .await?
    {
        None => Ok(MutationPolicy::default()),
        Some(bytes) => serde_json::from_slice(&bytes)
            .map_err(|e| Error::corruption(POLICY_FILE, format!("policy parse: {e}"))),
    }
}

pub async fn store(backend: &Backend, policy: &MutationPolicy) -> Result<()> {
    let path = object_store::path::Path::from(POLICY_FILE);
    backend
        .put(&path, serde_json::to_vec_pretty(policy)?.into())
        .await?;
    backend.sync_objects(&[path]).await
}
