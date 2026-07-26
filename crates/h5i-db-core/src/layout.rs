//! On-disk / object-store layout.
//!
//! ```text
//! <root>/
//!   FORMAT                              # format + minimum reader version
//!   catalog/tables/<hash-of-name>.json  # name -> table UUID
//!   tables/<table-uuid>/
//!     HEAD                              # the ONLY mutable object per table
//!     spec/<revision>.json              # TableSpec revisions
//!     manifests/<seq zero-padded>.json  # one immutable manifest per version
//!     segments/<segment-uuid>.parquet   # immutable data
//!   snapshots/<hash-of-name>.json       # name -> {table uuid: version}
//!   forks/<hash-of-name>.json           # name -> {table uuid: pinned version}
//!   catalog/forks/<hash-of-fork>/<hash-of-name>.json   # fork-scoped catalog
//! ```
//!
//! User-supplied strings (table, snapshot, and fork names) are stored *inside*
//! the JSON objects and hashed for path components, so raw user input never
//! becomes a filesystem path.
//!
//! A fork owns no storage of its own: its tables are ordinary tables under
//! `tables/<uuid>/`, reached through the fork-scoped catalog instead of the
//! global one (see `fork.rs`). That is what keeps the commit, locking, and
//! vacuum paths fork-oblivious.

use object_store::path::Path as ObjPath;
use uuid::Uuid;

pub const FORMAT_FILE: &str = "FORMAT";
pub const CATALOG_PREFIX: &str = "catalog/tables";
pub const SNAPSHOT_PREFIX: &str = "snapshots";
pub const FORK_PREFIX: &str = "forks";
pub const FORK_CATALOG_PREFIX: &str = "catalog/forks";

/// Current database format version and the minimum reader that understands it.
///
/// `FORMAT_VERSION` stamps *manifests and HEADs*. Forks deliberately did not
/// change either — a fork's tables are ordinary tables — so this stays at 1 and
/// a v1 reader keeps reading everything a fork-capable writer produces inside a
/// fork-free database.
pub const FORMAT_VERSION: u32 = 1;
pub const MIN_READER_VERSION: u32 = 1;

/// Database-level reader capability of *this* binary, recorded in `FORMAT` as
/// `min_reader_version` when a feature needs it.
///
/// Separate from `FORMAT_VERSION` because the two answer different questions:
/// `FORMAT_VERSION` is "what shape are the objects", `READER_VERSION` is "what
/// must a reader understand about the database as a whole".
pub const READER_VERSION: u32 = 2;

/// `min_reader_version` a database is raised to the first time a fork is
/// created.
///
/// This is a **fence, not a migration**: no existing object is rewritten. It
/// exists because a fork is an extra GC root, and a reader that cannot see
/// `forks/` would happily raise a retention floor past a fork's pin and vacuum
/// away segments the fork still reads. Refusing to open is the only safe
/// response for such a reader, and the fence is sticky (dropping every fork
/// does not lower it) because the danger is the reader's blindness, not the
/// forks' presence.
pub const FORK_MIN_READER_VERSION: u32 = 2;

/// Manifest sequence numbers are zero-padded to 12 digits so lexicographic
/// object listing equals numeric ordering.
const SEQ_WIDTH: usize = 12;

pub fn hash_name(name: &str) -> String {
    blake3::hash(name.as_bytes()).to_hex().to_string()
}

pub fn format_path() -> ObjPath {
    ObjPath::from(FORMAT_FILE)
}

pub fn catalog_entry_path(table_name: &str) -> ObjPath {
    ObjPath::from(format!("{CATALOG_PREFIX}/{}.json", hash_name(table_name)))
}

pub fn table_prefix(table_id: Uuid) -> ObjPath {
    ObjPath::from(format!("tables/{table_id}"))
}

pub fn head_path(table_id: Uuid) -> ObjPath {
    ObjPath::from(format!("tables/{table_id}/HEAD"))
}

pub fn spec_path(table_id: Uuid, revision: u32) -> ObjPath {
    ObjPath::from(format!("tables/{table_id}/spec/{revision:08}.json"))
}

pub fn manifest_prefix(table_id: Uuid) -> ObjPath {
    ObjPath::from(format!("tables/{table_id}/manifests"))
}

pub fn manifest_path(table_id: Uuid, sequence: u64) -> ObjPath {
    ObjPath::from(format!(
        "tables/{table_id}/manifests/{sequence:0width$}.json",
        width = SEQ_WIDTH
    ))
}

/// Parse a manifest object path back into its sequence number.
pub fn manifest_sequence_from_path(path: &ObjPath) -> Option<u64> {
    let name = path.filename()?;
    let stem = name.strip_suffix(".json")?;
    stem.parse::<u64>().ok()
}

pub fn segment_prefix(table_id: Uuid) -> ObjPath {
    ObjPath::from(format!("tables/{table_id}/segments"))
}

/// Staging leases: uploaded-but-uncommitted segment sets, written before the
/// segments themselves so vacuum can always tell staging from debris.
pub fn staging_prefix(table_id: Uuid) -> ObjPath {
    ObjPath::from(format!("tables/{table_id}/staging"))
}

pub fn staging_lease_path(table_id: Uuid, writer_id: Uuid) -> ObjPath {
    ObjPath::from(format!("tables/{table_id}/staging/{writer_id}.json"))
}

pub fn segment_path(table_id: Uuid, segment_id: Uuid) -> ObjPath {
    ObjPath::from(format!("tables/{table_id}/segments/{segment_id}.parquet"))
}

pub fn snapshot_path(name: &str) -> ObjPath {
    ObjPath::from(format!("{SNAPSHOT_PREFIX}/{}.json", hash_name(name)))
}

pub fn fork_path(name: &str) -> ObjPath {
    ObjPath::from(format!("{FORK_PREFIX}/{}.json", hash_name(name)))
}

/// Prefix holding one fork's catalog entries. Hashing the fork name keeps the
/// same "user strings never become path components" rule as everywhere else.
pub fn fork_catalog_prefix(fork_name: &str) -> ObjPath {
    ObjPath::from(format!("{FORK_CATALOG_PREFIX}/{}", hash_name(fork_name)))
}

pub fn fork_catalog_entry_path(fork_name: &str, table_name: &str) -> ObjPath {
    ObjPath::from(format!(
        "{FORK_CATALOG_PREFIX}/{}/{}.json",
        hash_name(fork_name),
        hash_name(table_name)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_never_reach_paths() {
        let evil = "../../etc/passwd; DROP TABLE --";
        let p = catalog_entry_path(evil);
        assert!(p.as_ref().starts_with("catalog/tables/"));
        assert!(!p.as_ref().contains(".."));
        let s = snapshot_path(evil);
        assert!(s.as_ref().starts_with("snapshots/"));
        assert!(!s.as_ref().contains(".."));
        let f = fork_path(evil);
        assert!(f.as_ref().starts_with("forks/"));
        assert!(!f.as_ref().contains(".."));
        // Both components of a fork catalog path are hashed, so a hostile
        // fork name cannot escape into another fork's namespace either.
        let fc = fork_catalog_entry_path(evil, evil);
        assert!(fc.as_ref().starts_with("catalog/forks/"));
        assert!(!fc.as_ref().contains(".."));
    }

    #[test]
    fn fork_catalog_entries_live_under_their_fork_prefix() {
        let prefix = fork_catalog_prefix("agent-01").as_ref().to_string();
        let entry = fork_catalog_entry_path("agent-01", "trades");
        assert!(entry.as_ref().starts_with(&prefix));
        // Distinct forks never share a catalog prefix, so listing one fork's
        // tables can never surface another's.
        assert_ne!(prefix, fork_catalog_prefix("agent-02").as_ref().to_string());
        // The same table name in two forks maps to two different objects.
        assert_ne!(entry, fork_catalog_entry_path("agent-02", "trades"));
    }

    #[test]
    fn fork_namespace_is_disjoint_from_the_global_catalog() {
        // A fork's catalog objects must never be picked up by a listing of the
        // global table catalog (that listing drives vacuum and `tables`).
        let fork_entry = fork_catalog_entry_path("f", "trades");
        assert!(!fork_entry.as_ref().starts_with(&format!("{CATALOG_PREFIX}/")));
        assert!(fork_entry.as_ref().starts_with(FORK_CATALOG_PREFIX));
    }

    #[test]
    fn manifest_paths_round_trip_and_sort() {
        let id = Uuid::new_v4();
        let p0 = manifest_path(id, 0);
        let p10 = manifest_path(id, 10);
        let p9999 = manifest_path(id, 9999);
        assert_eq!(manifest_sequence_from_path(&p0), Some(0));
        assert_eq!(manifest_sequence_from_path(&p10), Some(10));
        assert_eq!(manifest_sequence_from_path(&p9999), Some(9999));
        // lexicographic == numeric thanks to zero padding
        assert!(p0.as_ref() < p10.as_ref());
        assert!(p10.as_ref() < p9999.as_ref());
    }

    #[test]
    fn manifest_sequence_rejects_non_manifest_paths() {
        // A segment path is not a manifest sequence.
        let seg = segment_path(Uuid::new_v4(), Uuid::new_v4());
        assert_eq!(manifest_sequence_from_path(&seg), None);
        // Non-numeric stem.
        let junk = ObjPath::from("tables/x/manifests/HEAD.json");
        assert_eq!(manifest_sequence_from_path(&junk), None);
        // Missing .json suffix.
        let no_ext = ObjPath::from("tables/x/manifests/000000000001");
        assert_eq!(manifest_sequence_from_path(&no_ext), None);
    }

    #[test]
    fn hashing_is_deterministic_and_name_sensitive() {
        assert_eq!(hash_name("trades"), hash_name("trades"));
        assert_ne!(hash_name("trades"), hash_name("quotes"));
        // Same name -> same catalog path (lookups are stable across runs).
        assert_eq!(catalog_entry_path("trades"), catalog_entry_path("trades"));
    }

    #[test]
    fn per_table_paths_are_nested_under_the_table_prefix() {
        let id = Uuid::new_v4();
        let prefix = table_prefix(id).as_ref().to_string();
        for p in [
            head_path(id),
            spec_path(id, 1),
            manifest_prefix(id),
            manifest_path(id, 1),
            segment_prefix(id),
            segment_path(id, Uuid::new_v4()),
            staging_prefix(id),
            staging_lease_path(id, Uuid::new_v4()),
        ] {
            assert!(
                p.as_ref().starts_with(&prefix),
                "{} should be under {prefix}",
                p.as_ref()
            );
        }
    }

    #[test]
    fn spec_revision_is_zero_padded() {
        let id = Uuid::new_v4();
        assert!(spec_path(id, 1).as_ref().ends_with("/spec/00000001.json"));
        // Padding preserves lexicographic == numeric ordering.
        assert!(spec_path(id, 2).as_ref() < spec_path(id, 10).as_ref());
    }

    #[test]
    fn segment_and_staging_paths_have_expected_extensions() {
        let id = Uuid::new_v4();
        assert!(
            segment_path(id, Uuid::new_v4())
                .as_ref()
                .ends_with(".parquet")
        );
        assert!(
            staging_lease_path(id, Uuid::new_v4())
                .as_ref()
                .ends_with(".json")
        );
    }
}
