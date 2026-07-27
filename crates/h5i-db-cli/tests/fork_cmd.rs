//! CLI e2e tests for forks (ROADMAP Part IX): the `fork` subcommand and the
//! global `--fork` scope, driven through the real binary.
//!
//! These assert the agent-facing contract rather than the engine's: what the
//! JSON says, which exit code comes back, and whether a failure carries the
//! next action an agent should take. The engine's guarantees are proven in
//! `h5i-db-core/tests/fork.rs`.

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

const CSV_V2: &str = "ts,symbol,price,size\n\
2026-07-01T09:30:03Z,AAPL,211.0,10\n\
2026-07-01T09:30:04Z,MSFT,456.0,20\n";

const CSV_V3: &str = "ts,symbol,price,size\n\
2026-07-01T09:30:05Z,AAPL,212.0,30\n";

fn bootstrap(cwd: &Path) {
    std::fs::write(cwd.join("v1.csv"), CSV_V1).unwrap();
    std::fs::write(cwd.join("v2.csv"), CSV_V2).unwrap();
    std::fs::write(cwd.join("v3.csv"), CSV_V3).unwrap();
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

fn count(cwd: &Path, extra: &[&str]) -> u64 {
    let mut args = vec!["query", "m.db", "select count(*) c from trades"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--format", "json"]);
    let v = ok_json(&run(&args, cwd));
    v[0]["c"].as_u64().expect("count is a number")
}

#[test]
fn create_list_and_show_report_the_forks_shape() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);

    let created = ok_json(&run(
        &[
            "fork",
            "create",
            "m.db",
            "agent-01",
            "--note",
            "hypothesis 1",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(created["name"], "agent-01");
    assert_eq!(created["note"], "hypothesis 1");
    assert_eq!(created["pins"].as_object().unwrap().len(), 1);

    let listed = ok_json(&run(&["fork", "list", "m.db", "--format", "json"], cwd));
    let arr = listed.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "agent-01");
    assert_eq!(arr[0]["tables_pinned"], 1);
    // A fresh fork owns nothing but already holds the base back.
    assert_eq!(arr[0]["bytes_own"], 0);
    assert!(arr[0]["bytes_pinned"].as_u64().unwrap() > 0);

    let shown = ok_json(&run(
        &["fork", "show", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));
    assert_eq!(shown["name"], "agent-01");
}

#[test]
fn the_fork_flag_scopes_reads_and_writes() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    ok_json(&run(
        &["fork", "create", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));

    ok_json(&run(
        &[
            "ingest", "m.db", "trades", "v2.csv", "--mode", "append", "--fork", "agent-01",
            "--format", "json",
        ],
        cwd,
    ));

    assert_eq!(count(cwd, &["--fork", "agent-01"]), 5);
    assert_eq!(count(cwd, &[]), 3, "the base must not see the fork's rows");

    // A table created in the fork is listed there and nowhere else.
    ok_json(&run(
        &[
            "create-table",
            "m.db",
            "features",
            "--like",
            "v1.csv",
            "--time-column",
            "ts",
            "--fork",
            "agent-01",
            "--format",
            "json",
        ],
        cwd,
    ));
    let in_fork = ok_json(&run(
        &["tables", "m.db", "--fork", "agent-01", "--format", "json"],
        cwd,
    ));
    let names: Vec<&str> = in_fork
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["table"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["features", "trades"]);

    let on_main = ok_json(&run(&["tables", "m.db", "--format", "json"], cwd));
    assert_eq!(on_main.as_array().unwrap().len(), 1);
}

#[test]
fn diff_then_promote_then_the_loser_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    for (fork, csv) in [("agent-a", "v2.csv"), ("agent-b", "v3.csv")] {
        ok_json(&run(
            &["fork", "create", "m.db", fork, "--format", "json"],
            cwd,
        ));
        ok_json(&run(
            &[
                "ingest", "m.db", "trades", csv, "--mode", "append", "--fork", fork, "--format",
                "json",
            ],
            cwd,
        ));
    }

    // diff is metadata-only and names the shared segments.
    let diff = ok_json(&run(
        &["fork", "diff", "m.db", "agent-a", "--format", "json"],
        cwd,
    ));
    let t = &diff["tables"][0];
    assert_eq!(t["table"], "trades");
    assert_eq!(t["kind"], "shadowed");
    assert_eq!(t["rows_base"], 3);
    assert_eq!(t["rows_fork"], 5);
    assert_eq!(t["segments_shared"], 1);
    assert_eq!(t["base_moved"], false);

    let promoted = ok_json(&run(
        &[
            "fork", "promote", "m.db", "agent-a", "--table", "trades", "--format", "json",
        ],
        cwd,
    ));
    assert_eq!(promoted["rows"], 5);
    // A local filesystem promote links; it must not rewrite Parquet.
    assert_eq!(promoted["bytes_copied"], 0);
    assert_eq!(count(cwd, &[]), 5);

    // Second promote loses, and says so in the agent contract.
    let out = run(
        &[
            "fork", "promote", "m.db", "agent-b", "--table", "trades", "--format", "json",
        ],
        cwd,
    );
    assert_eq!(out.status.code(), Some(3), "conflicts exit 3");
    let env = err_envelope(&out);
    assert_eq!(env["code"], "promote_conflict");
    assert_eq!(env["retryable"], false, "retrying a stale base cannot help");
    let actions = env["next_actions"].as_array().unwrap();
    assert!(!actions.is_empty(), "a conflict must offer a way forward");
    assert!(
        actions.iter().any(|a| a["cmd"]
            .as_str()
            .unwrap()
            .contains("fork drop m.db agent-b")),
        "discarding is the common resolution and must be offered: {actions:?}"
    );
    // The loser changed nothing.
    assert_eq!(count(cwd, &[]), 5);
}

#[test]
fn drop_removes_the_fork_and_frees_the_base() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    ok_json(&run(
        &["fork", "create", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));
    ok_json(&run(
        &[
            "ingest", "m.db", "trades", "v2.csv", "--mode", "append", "--fork", "agent-01",
            "--format", "json",
        ],
        cwd,
    ));

    // While it lives, the base table cannot be dropped out from under it.
    let env = err_envelope(&run(
        &["drop-table", "m.db", "trades", "--yes", "--format", "json"],
        cwd,
    ));
    assert!(
        env["message"].as_str().unwrap().contains("pinned by fork"),
        "{env}"
    );

    let dropped = ok_json(&run(
        &["fork", "drop", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));
    assert_eq!(dropped["dropped"], "agent-01");
    assert_eq!(dropped["tables_deleted"], 1);
    assert!(
        ok_json(&run(&["fork", "list", "m.db", "--format", "json"], cwd))
            .as_array()
            .unwrap()
            .is_empty()
    );
    // …and now the base is free again.
    ok_json(&run(
        &["drop-table", "m.db", "trades", "--yes", "--format", "json"],
        cwd,
    ));
}

#[test]
fn database_wide_commands_refuse_the_fork_flag() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    ok_json(&run(
        &["fork", "create", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));

    for args in [
        vec!["vacuum", "m.db", "--apply"],
        vec!["snapshot", "create", "m.db", "snap"],
        vec!["fork", "create", "m.db", "nested"],
    ] {
        let mut a = args.clone();
        a.extend_from_slice(&["--fork", "agent-01", "--format", "json"]);
        let env = err_envelope(&run(&a, cwd));
        let msg = env["message"].as_str().unwrap();
        assert!(
            msg.contains("agent-01") && msg.contains("base database"),
            "{args:?} should refuse inside a fork, got: {msg}"
        );
    }
}

#[test]
fn an_unknown_fork_suggests_the_closest_name() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    ok_json(&run(
        &["fork", "create", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));
    let env = err_envelope(&run(
        &["fork", "show", "m.db", "agent-02", "--format", "json"],
        cwd,
    ));
    assert_eq!(env["code"], "fork_not_found");
    assert_eq!(env["did_you_mean"], "agent-01");
    assert_eq!(env["exit_code"], serde_json::Value::Null);
}

#[test]
fn a_duplicate_fork_name_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    ok_json(&run(
        &["fork", "create", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));
    let env = err_envelope(&run(
        &["fork", "create", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));
    assert_eq!(env["code"], "fork_exists");
}

#[test]
fn an_as_of_before_all_history_fails_where_it_was_typed() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    let env = err_envelope(&run(
        &[
            "fork",
            "create",
            "m.db",
            "backtest",
            "--as-of",
            "2000-01-01T00:00:00Z",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(env["code"], "invalid_input");
    assert!(
        env["message"]
            .as_str()
            .unwrap()
            .contains("--as-of pins no version"),
        "{env}"
    );
}

#[test]
fn fork_metadata_is_carried_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    ok_json(&run(
        &[
            "fork",
            "create",
            "m.db",
            "agent-01",
            "--meta",
            r#"{"run_id":"r-42","hypothesis":3}"#,
            "--format",
            "json",
        ],
        cwd,
    ));
    let shown = ok_json(&run(
        &["fork", "show", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));
    assert_eq!(shown["user_meta"]["run_id"], "r-42");
    assert_eq!(shown["user_meta"]["hypothesis"], 3);

    // Non-object metadata is rejected rather than silently wrapped.
    let env = err_envelope(&run(
        &[
            "fork",
            "create",
            "m.db",
            "agent-02",
            "--meta",
            r#""just a string""#,
            "--format",
            "json",
        ],
        cwd,
    ));
    assert!(
        env["message"]
            .as_str()
            .unwrap()
            .contains("must be a JSON object"),
        "{env}"
    );
}

#[test]
fn promoting_a_fork_created_table_moves_it_to_main() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    ok_json(&run(
        &["fork", "create", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));
    ok_json(&run(
        &[
            "create-table",
            "m.db",
            "features",
            "--like",
            "v1.csv",
            "--time-column",
            "ts",
            "--fork",
            "agent-01",
            "--format",
            "json",
        ],
        cwd,
    ));
    ok_json(&run(
        &[
            "ingest", "m.db", "features", "v1.csv", "--fork", "agent-01", "--format", "json",
        ],
        cwd,
    ));

    let promoted = ok_json(&run(
        &[
            "fork", "promote", "m.db", "agent-01", "--table", "features", "--format", "json",
        ],
        cwd,
    ));
    assert_eq!(promoted["kind"], "created");
    assert_eq!(
        promoted["segments_linked"], 0,
        "a catalog move links nothing"
    );
    assert_eq!(promoted["bytes_copied"], 0);

    let on_main = ok_json(&run(&["tables", "m.db", "--format", "json"], cwd));
    let names: Vec<&str> = on_main
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["table"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["features", "trades"]);

    // Dropping the fork afterwards must not take the promoted table with it.
    ok_json(&run(
        &["fork", "drop", "m.db", "agent-01", "--format", "json"],
        cwd,
    ));
    let still = ok_json(&run(&["tables", "m.db", "--format", "json"], cwd));
    assert_eq!(still.as_array().unwrap().len(), 2);
}

// ---------------------------------------------------------------------------
// batch verbs (ROADMAP Part X, X-B1)
// ---------------------------------------------------------------------------

/// `--count` makes the wide fanout one command. The single-fork output shape
/// is deliberately unchanged by its presence: without the flag the command
/// still returns one object, not a list of one.
#[test]
fn fork_create_count_makes_a_numbered_fanout_and_drop_takes_many_names() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);

    let created = ok_json(&run(
        &[
            "fork", "create", "m.db", "sim", "--count", "5", "--format", "json",
        ],
        cwd,
    ));
    let created = created.as_array().expect("--count returns a list");
    assert_eq!(created.len(), 5);
    assert_eq!(created[0]["name"], "sim-0000");
    assert_eq!(created[4]["name"], "sim-0004");
    // Every fork of one base pins the same versions.
    for f in created {
        assert_eq!(f["pins"], created[0]["pins"]);
    }

    // Without --count the shape is a single object, exactly as before.
    let one = ok_json(&run(
        &["fork", "create", "m.db", "solo", "--format", "json"],
        cwd,
    ));
    assert_eq!(one["name"], "solo");
    assert!(one.as_array().is_none(), "one fork must not become a list");

    // Drop several at once.
    let dropped = ok_json(&run(
        &[
            "fork", "drop", "m.db", "sim-0000", "sim-0001", "sim-0002", "--format", "json",
        ],
        cwd,
    ));
    assert_eq!(
        dropped["dropped"],
        serde_json::json!(["sim-0000", "sim-0001", "sim-0002"])
    );
    let left = ok_json(&run(&["fork", "list", "m.db", "--format", "json"], cwd));
    let names: Vec<&str> = left
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["sim-0003", "sim-0004", "solo"]);
}

/// A name that is not there stops the batch and is named in the error, rather
/// than being skipped so the caller believes it deleted more than it did.
#[test]
fn fork_drop_reports_a_name_that_does_not_exist() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    bootstrap(cwd);
    ok_json(&run(
        &["fork", "create", "m.db", "real", "--format", "json"],
        cwd,
    ));

    let env = err_envelope(&run(
        &["fork", "drop", "m.db", "real", "ghost", "--format", "json"],
        cwd,
    ));
    assert!(env["message"].as_str().unwrap().contains("ghost"), "{env}");
    // The one before the failure really was dropped.
    assert!(
        ok_json(&run(&["fork", "list", "m.db", "--format", "json"], cwd))
            .as_array()
            .unwrap()
            .is_empty()
    );
}
