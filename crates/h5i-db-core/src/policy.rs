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
            OpKind::Create | OpKind::EvolveSchema => true,
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

#[cfg(test)]
mod tests {
    use super::*;

    const MUTATING_OPS: [OpKind; 6] = [
        OpKind::Write,
        OpKind::Append,
        OpKind::ReplaceRange,
        OpKind::DeleteRange,
        OpKind::Restore,
        OpKind::Compact,
    ];

    #[test]
    fn default_policy_allows_everything() {
        let p = MutationPolicy::default();
        for op in MUTATING_OPS {
            assert!(p.check_direct(op).is_ok(), "default should allow {op}");
        }
        // Metadata-shape ops are always direct regardless of policy.
        assert!(p.check_direct(OpKind::Create).is_ok());
        assert!(p.check_direct(OpKind::EvolveSchema).is_ok());
    }

    #[test]
    fn create_and_evolve_are_always_direct_even_when_all_flags_off() {
        let p = MutationPolicy {
            direct_append: false,
            direct_write: false,
            direct_replace: false,
            direct_delete: false,
            direct_restore: false,
            direct_compact: false,
        };
        assert!(p.check_direct(OpKind::Create).is_ok());
        assert!(p.check_direct(OpKind::EvolveSchema).is_ok());
        // ...but the data-mutating ops are all forbidden.
        for op in MUTATING_OPS {
            let err = p.check_direct(op).unwrap_err();
            assert!(
                matches!(err, Error::PolicyViolation { .. }),
                "expected PolicyViolation for {op}, got {err:?}"
            );
        }
    }

    #[test]
    fn each_flag_gates_exactly_its_own_op() {
        // Turning off one flag forbids one op and leaves the rest allowed.
        let cases = [
            ("direct_append", OpKind::Append),
            ("direct_write", OpKind::Write),
            ("direct_replace", OpKind::ReplaceRange),
            ("direct_delete", OpKind::DeleteRange),
            ("direct_restore", OpKind::Restore),
            ("direct_compact", OpKind::Compact),
        ];
        for (key, gated) in cases {
            let mut p = MutationPolicy::default();
            p.set(key, false).unwrap();
            assert!(
                p.check_direct(gated).is_err(),
                "{key}=false should forbid {gated}"
            );
            for other in MUTATING_OPS.into_iter().filter(|o| *o != gated) {
                assert!(
                    p.check_direct(other).is_ok(),
                    "{key}=false should not affect {other}"
                );
            }
        }
    }

    #[test]
    fn policy_violation_names_the_op() {
        let mut p = MutationPolicy::default();
        p.set("direct_delete", false).unwrap();
        match p.check_direct(OpKind::DeleteRange) {
            Err(Error::PolicyViolation { op }) => assert_eq!(op, "delete_range"),
            other => panic!("expected PolicyViolation, got {other:?}"),
        }
    }

    #[test]
    fn set_rejects_unknown_key() {
        let mut p = MutationPolicy::default();
        let err = p.set("direct_frobnicate", false).unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }));
        // The error lists the valid keys to guide the caller.
        assert!(err.to_string().contains("direct_append"));
    }

    #[test]
    fn serde_round_trip_is_stable() {
        let mut p = MutationPolicy::default();
        p.set("direct_write", false).unwrap();
        p.set("direct_compact", false).unwrap();
        let json = serde_json::to_vec(&p).unwrap();
        let back: MutationPolicy = serde_json::from_slice(&json).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn missing_fields_fall_back_to_defaults() {
        // `#[serde(default)]` means an empty object deserializes to defaults.
        let back: MutationPolicy = serde_json::from_str("{}").unwrap();
        assert_eq!(back, MutationPolicy::default());
    }
}
