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
//! ```
//!
//! User-supplied strings (table and snapshot names) are stored *inside* the
//! JSON objects and hashed for path components, so raw user input never
//! becomes a filesystem path.

use object_store::path::Path as ObjPath;
use uuid::Uuid;

pub const FORMAT_FILE: &str = "FORMAT";
pub const CATALOG_PREFIX: &str = "catalog/tables";
pub const SNAPSHOT_PREFIX: &str = "snapshots";

/// Current database format version and the minimum reader that understands it.
pub const FORMAT_VERSION: u32 = 1;
pub const MIN_READER_VERSION: u32 = 1;

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
}
