//! CLI lifecycle e2e tests: exercise the real binary across the table
//! lifecycle commands not covered by `cli.rs` — tables/schema/sample/versions,
//! snapshot create/list/delete + restore, rename, drop-table (incl. snapshot
//! pin refusal), delete-range, compact, and vacuum. Each test drives the
//! actual `h5i-db` binary and asserts the agent-facing JSON contract and exit
//! codes.

use std::path::Path;
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_h5i-db")
}

fn run(args: &[&str], cwd: &Path) -> Output {
    Command::new(bin())
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("spawn h5i-db")
}

fn ok_json(out: &Output) -> serde_json::Value {
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("stdout is JSON")
}

fn err_envelope(out: &Output) -> serde_json::Value {
    assert!(
        !out.status.success(),
        "expected failure, got success\nstdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    serde_json::from_slice(&out.stderr).expect("stderr is a JSON error envelope")
}

const CSV_V1: &str = "ts,symbol,price,size\n\
2026-07-01T09:30:00Z,AAPL,210.5,100\n\
2026-07-01T09:30:01Z,MSFT,455.2,50\n\
2026-07-01T09:30:02Z,AAPL,210.7,200\n";

// Strictly later timestamps so `append` (ordered) accepts them.
const CSV_V2: &str = "ts,symbol,price,size\n\
2026-07-01T09:30:03Z,AAPL,211.0,10\n\
2026-07-01T09:30:04Z,MSFT,456.0,20\n";

/// init + create-table(--like) + append one CSV. Returns nothing; panics on
/// any non-success exit.
fn bootstrap(cwd: &Path) {
    std::fs::write(cwd.join("v1.csv"), CSV_V1).unwrap();
    std::fs::write(cwd.join("v2.csv"), CSV_V2).unwrap();
    ok_json(&run(&["init", "m.db", "--format", "json"], cwd));
    ok_json(&run(
        &[
            "create-table",
            "m.db",
            "trades",
            "--like",
            "v1.csv",
            "--time-column",
            "ts",
            "--format",
            "json",
        ],
        cwd,
    ));
    ok_json(&run(
        &["ingest", "m.db", "trades", "v1.csv", "--format", "json"],
        cwd,
    ));
}

#[test]
fn tables_schema_sample_and_versions_report_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);

    // tables -> array with our one table, row count and time column.
    let tables = ok_json(&run(&["tables", "m.db", "--format", "json"], cwd));
    let arr = tables.as_array().expect("tables is an array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["table"], "trades");
    assert_eq!(arr[0]["rows"], 3);
    assert_eq!(arr[0]["time_column"], "ts");

    // schema -> declared fields, time column, sort key.
    let schema = ok_json(&run(&["schema", "m.db", "trades", "--format", "json"], cwd));
    assert_eq!(schema["table"], "trades");
    assert_eq!(schema["time_column"], "ts");
    let names: Vec<&str> = schema["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"ts") && names.contains(&"symbol") && names.contains(&"price"));

    // sample -n 2 -> exactly two rows, earliest first.
    let sample = ok_json(&run(
        &["sample", "m.db", "trades", "-n", "2", "--format", "json"],
        cwd,
    ));
    assert_eq!(sample.as_array().unwrap().len(), 2);

    // versions -> at least the create + the append, newest listing the append op.
    let versions = ok_json(&run(
        &["versions", "m.db", "trades", "--format", "json"],
        cwd,
    ));
    let vs = versions.as_array().unwrap();
    assert!(vs.len() >= 2, "expected >=2 versions, got {}", vs.len());
    let ops: Vec<&str> = vs.iter().map(|v| v["op"].as_str().unwrap()).collect();
    assert!(ops.contains(&"append"), "ops: {ops:?}");
}

#[test]
fn snapshot_create_list_restore_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);

    // Pin the current state.
    let snap = ok_json(&run(
        &[
            "snapshot",
            "create",
            "m.db",
            "before-append",
            "trades",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(snap["name"], "before-append");

    let list = ok_json(&run(&["snapshot", "list", "m.db", "--format", "json"], cwd));
    let names: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"before-append"), "names: {names:?}");

    // Append more data, then restore the earliest version -> a NEW head with
    // the old row count.
    ok_json(&run(
        &["ingest", "m.db", "trades", "v2.csv", "--format", "json"],
        cwd,
    ));
    let versions = ok_json(&run(
        &["versions", "m.db", "trades", "--format", "json"],
        cwd,
    ));
    let vs = versions.as_array().unwrap();
    let earliest = vs
        .iter()
        .map(|v| v["version"].as_u64().unwrap())
        .min()
        .unwrap();
    let latest = vs
        .iter()
        .map(|v| v["version"].as_u64().unwrap())
        .max()
        .unwrap();

    let restored = ok_json(&run(
        &[
            "restore",
            "m.db",
            "trades",
            &earliest.to_string(),
            "--format",
            "json",
        ],
        cwd,
    ));
    // Restore moves history forward: the new sequence exceeds the old head.
    assert!(
        restored["sequence"].as_u64().unwrap() > latest,
        "restore should append a new version"
    );
    assert_eq!(restored["op"], "restore");

    // The snapshot can be deleted.
    let deleted = ok_json(&run(
        &[
            "snapshot",
            "delete",
            "m.db",
            "before-append",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(deleted["deleted"], "before-append");
}

#[test]
fn drop_table_needs_confirmation_and_respects_snapshot_pins() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);

    // Without --yes: refused as a user error.
    let env = err_envelope(&run(
        &["drop-table", "m.db", "trades", "--format", "json"],
        cwd,
    ));
    assert_eq!(env["code"], "invalid_input");

    // Pin it, then drop --yes must refuse while the snapshot exists.
    ok_json(&run(
        &[
            "snapshot", "create", "m.db", "pin", "trades", "--format", "json",
        ],
        cwd,
    ));
    let pinned = run(
        &["drop-table", "m.db", "trades", "--yes", "--format", "json"],
        cwd,
    );
    assert!(
        !pinned.status.success(),
        "drop should be refused while a snapshot pins the table"
    );

    // Remove the pin, then the drop succeeds and the table is gone.
    ok_json(&run(
        &["snapshot", "delete", "m.db", "pin", "--format", "json"],
        cwd,
    ));
    let dropped = ok_json(&run(
        &["drop-table", "m.db", "trades", "--yes", "--format", "json"],
        cwd,
    ));
    assert_eq!(dropped["dropped"], "trades");
    let tables = ok_json(&run(&["tables", "m.db", "--format", "json"], cwd));
    assert!(tables.as_array().unwrap().is_empty());
}

#[test]
fn rename_table_moves_the_catalog_entry() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);

    let renamed = ok_json(&run(
        &["rename", "m.db", "trades", "ticks", "--format", "json"],
        cwd,
    ));
    assert_eq!(renamed["renamed"]["from"], "trades");
    assert_eq!(renamed["renamed"]["to"], "ticks");

    // New name resolves; old name is gone.
    let schema = ok_json(&run(&["schema", "m.db", "ticks", "--format", "json"], cwd));
    assert_eq!(schema["table"], "ticks");
    let missing = err_envelope(&run(&["schema", "m.db", "trades", "--format", "json"], cwd));
    assert_eq!(missing["code"], "table_not_found");
}

#[test]
fn delete_range_removes_rows_and_records_a_version() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);

    // Delete the single row at 09:30:01 (end is exclusive).
    let result = ok_json(&run(
        &[
            "delete-range",
            "m.db",
            "trades",
            "--start",
            "2026-07-01T09:30:01Z",
            "--end",
            "2026-07-01T09:30:02Z",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(result["op"], "delete_range");

    // The table now has two rows and a fresh count via SQL.
    let count = ok_json(&run(
        &[
            "query",
            "m.db",
            "SELECT COUNT(*) AS n FROM trades",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(count.as_array().unwrap()[0]["n"], 2);
}

#[test]
fn compact_and_vacuum_dry_run_then_apply() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    // A second append creates a second segment for compaction to merge.
    ok_json(&run(
        &["ingest", "m.db", "trades", "v2.csv", "--format", "json"],
        cwd,
    ));

    // compact -> a data-identical rewrite, recorded as a compact version.
    let compacted = ok_json(&run(
        &["compact", "m.db", "trades", "--format", "json"],
        cwd,
    ));
    assert_eq!(compacted["op"], "compact");
    // Row total is preserved by compaction.
    assert_eq!(compacted["rows_total"], 5);

    // vacuum dry run: reports candidates without deleting.
    let dry = ok_json(&run(
        &["vacuum", "m.db", "--grace-seconds", "0", "--format", "json"],
        cwd,
    ));
    assert_eq!(dry["dry_run"], true);
    assert_eq!(dry["deleted"], 0);

    // vacuum --apply: actually reclaims the now-unreferenced pre-compaction
    // segments left behind by the rewrite.
    let applied = ok_json(&run(
        &[
            "vacuum",
            "m.db",
            "--grace-seconds",
            "0",
            "--apply",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(applied["dry_run"], false);

    // The table still reads back all five rows after GC.
    let count = ok_json(&run(
        &[
            "query",
            "m.db",
            "SELECT COUNT(*) AS n FROM trades",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(count.as_array().unwrap()[0]["n"], 5);
}
