//! Research-mode e2e tests (ROADMAP Part VI): the arrival axis of the
//! point-in-time jail — `query --as-of`, which pins **every** table at a
//! decision point so a query can only read data already committed then.
//!
//! The tests drive the real binary because the guarantee being asserted is a
//! CLI-surface one: an agent handed `H5I_DB_AS_OF` in its environment must not
//! be able to read later commits from any table, and a bad decision point must
//! fail as a user error (exit 2), not an internal one.

use std::path::Path;
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_h5i-db")
}

fn run_env(args: &[&str], cwd: &Path, env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(bin());
    cmd.args(args).current_dir(cwd);
    // Never inherit a decision point from the developer's own shell.
    cmd.env_remove("H5I_DB_AS_OF");
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.output().expect("spawn h5i-db")
}

fn run(args: &[&str], cwd: &Path) -> Output {
    run_env(args, cwd, &[])
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

const TRADES_V1: &str = "ts,symbol,price\n\
2026-07-01T09:30:00Z,AAPL,100.0\n\
2026-07-01T09:30:01Z,AAPL,102.0\n";
const TRADES_V2: &str = "ts,symbol,price\n\
2026-07-01T09:30:02Z,AAPL,300.0\n";
const QUOTES_V1: &str = "ts,symbol,bid\n\
2026-07-01T09:30:00Z,AAPL,99.0\n";
const QUOTES_V2: &str = "ts,symbol,bid\n\
2026-07-01T09:30:02Z,AAPL,299.0\n";

/// Two tables, each with two commits: v1 is the "past", v2 is data that only
/// became available after the decision instant.
fn bootstrap(cwd: &Path) {
    std::fs::write(cwd.join("trades1.csv"), TRADES_V1).unwrap();
    std::fs::write(cwd.join("trades2.csv"), TRADES_V2).unwrap();
    std::fs::write(cwd.join("quotes1.csv"), QUOTES_V1).unwrap();
    std::fs::write(cwd.join("quotes2.csv"), QUOTES_V2).unwrap();
    ok_json(&run(&["init", "m.db", "--format", "json"], cwd));
    for (table, seed) in [("trades", "trades1.csv"), ("quotes", "quotes1.csv")] {
        ok_json(&run(
            &[
                "create-table",
                "m.db",
                table,
                "--like",
                seed,
                "--time-column",
                "ts",
                "--format",
                "json",
            ],
            cwd,
        ));
    }
    // v1 for both tables, then v2 for both.
    for (table, file) in [("trades", "trades1.csv"), ("quotes", "quotes1.csv")] {
        ok_json(&run(
            &["ingest", "m.db", table, file, "--format", "json"],
            cwd,
        ));
    }
    for (table, file) in [("trades", "trades2.csv"), ("quotes", "quotes2.csv")] {
        ok_json(&run(
            &["ingest", "m.db", table, file, "--format", "json"],
            cwd,
        ));
    }
}

fn scalar(v: &serde_json::Value, col: &str) -> f64 {
    v[0][col]
        .as_f64()
        .unwrap_or_else(|| panic!("no {col} in {v}"))
}

#[test]
fn as_of_pins_every_table_not_just_one() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    let sql = "SELECT (SELECT count(*) FROM trades) AS t, (SELECT count(*) FROM quotes) AS q";

    // Unpinned: head, i.e. both v2 commits are visible.
    let head = ok_json(&run(&["query", "m.db", sql, "--format", "json"], cwd));
    assert_eq!(scalar(&head, "t"), 3.0);
    assert_eq!(scalar(&head, "q"), 2.0);

    // Pinned at version 1: neither table's later commit is readable. A pin
    // that only covered the queried table would leave `quotes` at head.
    let pinned = ok_json(&run(
        &["query", "m.db", sql, "--as-of", "1", "--format", "json"],
        cwd,
    ));
    assert_eq!(scalar(&pinned, "t"), 2.0);
    assert_eq!(
        scalar(&pinned, "q"),
        1.0,
        "every table must be pinned, not just the first one"
    );
}

#[test]
fn env_var_pins_a_whole_session() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    let sql = "SELECT max(price) AS px FROM trades";

    // The 300.0 print only exists in the withheld commit.
    let head = ok_json(&run(&["query", "m.db", sql, "--format", "json"], cwd));
    assert_eq!(scalar(&head, "px"), 300.0);

    let jailed = ok_json(&run_env(
        &["query", "m.db", sql, "--format", "json"],
        cwd,
        &[("H5I_DB_AS_OF", "1")],
    ));
    assert_eq!(
        scalar(&jailed, "px"),
        102.0,
        "an agent handed H5I_DB_AS_OF must not see the later commit"
    );

    // An explicit flag still wins over the environment.
    let overridden = ok_json(&run_env(
        &["query", "m.db", sql, "--as-of", "2", "--format", "json"],
        cwd,
        &[("H5I_DB_AS_OF", "1")],
    ));
    assert_eq!(scalar(&overridden, "px"), 300.0);
}

#[test]
fn timestamp_and_snapshot_decision_points_agree_with_a_version_pin() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    let sql = "SELECT count(*) AS c FROM trades";

    // A snapshot taken now pins v2 for both tables.
    ok_json(&run(
        &["snapshot", "create", "m.db", "now", "--format", "json"],
        cwd,
    ));
    let by_snapshot = ok_json(&run(
        &["query", "m.db", sql, "--as-of", "now", "--format", "json"],
        cwd,
    ));
    assert_eq!(scalar(&by_snapshot, "c"), 3.0);

    // An availability timestamp after every commit resolves to the same head.
    let by_ts = ok_json(&run(
        &[
            "query",
            "m.db",
            sql,
            "--as-of",
            "2100-01-01T00:00:00Z",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(scalar(&by_ts, "c"), 3.0);

    // Past the i64-nanosecond epoch range (year 2262) the decision point is
    // unrepresentable; say so as a user error instead of silently wrapping.
    let envelope = err_envelope(&run(
        &[
            "query",
            "m.db",
            sql,
            "--as-of",
            "2999-01-01T00:00:00Z",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(envelope["code"], "invalid_input");

    // And a timestamp before any commit fails closed rather than reading head.
    let envelope = err_envelope(&run(
        &[
            "query",
            "m.db",
            sql,
            "--as-of",
            "1970-01-01T00:00:00Z",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(envelope["code"], "version_not_found");
}

#[test]
fn a_bad_decision_point_is_a_user_error_not_an_internal_one() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    let sql = "SELECT count(*) AS c FROM trades";

    // Unknown snapshot name: exit 2 (user error), with the snapshot code, not
    // a blanket internal error from session construction.
    let out = run(
        &["query", "m.db", sql, "--as-of", "nope", "--format", "json"],
        cwd,
    );
    let envelope = err_envelope(&out);
    assert_eq!(out.status.code(), Some(2), "envelope: {envelope}");
    assert_eq!(envelope["code"], "snapshot_not_found");

    // Version beyond the table's history: also a user error.
    let out = run(
        &["query", "m.db", sql, "--as-of", "99", "--format", "json"],
        cwd,
    );
    let envelope = err_envelope(&out);
    assert_eq!(out.status.code(), Some(2), "envelope: {envelope}");
    assert_eq!(envelope["code"], "version_not_found");
}
