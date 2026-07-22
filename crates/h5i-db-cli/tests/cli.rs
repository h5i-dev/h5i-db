//! CLI contract tests: run the real binary and verify the agent-facing
//! contract — JSON output, machine-readable errors, stable exit codes.

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

fn stdout_json(out: &Output) -> serde_json::Value {
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("stdout is JSON")
}

fn stderr_envelope(out: &Output) -> serde_json::Value {
    serde_json::from_slice(&out.stderr).expect("stderr is a JSON error envelope")
}

const CSV: &str = "ts,symbol,price,size\n\
2026-07-01T09:30:00Z,AAPL,210.5,100\n\
2026-07-01T09:30:01Z,MSFT,455.2,50\n\
2026-07-01T09:30:02Z,AAPL,210.7,200\n";

#[test]
fn full_workflow_with_json_contract() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    std::fs::write(cwd.join("trades.csv"), CSV).unwrap();

    // init
    stdout_json(&run(&["init", "m.db", "--format", "json"], cwd));

    // create-table --like + ingest
    stdout_json(&run(
        &[
            "create-table",
            "m.db",
            "trades",
            "--like",
            "trades.csv",
            "--time-column",
            "ts",
            "--format",
            "json",
        ],
        cwd,
    ));
    let ingest = stdout_json(&run(
        &[
            "ingest",
            "m.db",
            "trades",
            "trades.csv",
            "--mode",
            "write",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(ingest["rows_total"], 3);
    assert_eq!(ingest["sequence"], 1);

    // query --format json
    let rows = stdout_json(&run(
        &[
            "query",
            "m.db",
            "SELECT symbol, count(*) AS n FROM trades GROUP BY symbol ORDER BY symbol",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(rows[0]["symbol"], "AAPL");
    assert_eq!(rows[0]["n"], 2);

    // schema introspection
    let schema = stdout_json(&run(&["schema", "m.db", "trades", "--format", "json"], cwd));
    assert_eq!(schema["time_column"], "ts");
    assert!(schema["fields"].as_array().unwrap().len() == 4);

    // versions
    let versions = stdout_json(&run(
        &["versions", "m.db", "trades", "--format", "json"],
        cwd,
    ));
    assert_eq!(versions.as_array().unwrap().len(), 2); // create + write

    // exit code 2 + envelope for user errors
    let out = run(
        &["query", "m.db", "SELECT * FROM nope", "--format", "json"],
        cwd,
    );
    assert_eq!(out.status.code(), Some(2));
    let env = stderr_envelope(&out);
    assert_eq!(env["code"], "invalid_input");
    assert_eq!(env["retryable"], false);

    // table_not_found has a hint
    let out = run(&["schema", "m.db", "nope"], cwd);
    assert_eq!(out.status.code(), Some(2));
    let env = stderr_envelope(&out);
    assert_eq!(env["code"], "table_not_found");
    assert!(env["hint"].as_str().unwrap().contains("tables"));
}

#[test]
fn plan_apply_flow_and_conflict_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    std::fs::write(cwd.join("trades.csv"), CSV).unwrap();
    stdout_json(&run(&["init", "m.db", "--format", "json"], cwd));
    stdout_json(&run(
        &[
            "create-table",
            "m.db",
            "trades",
            "--like",
            "trades.csv",
            "--time-column",
            "ts",
            "--format",
            "json",
        ],
        cwd,
    ));
    stdout_json(&run(
        &[
            "ingest",
            "m.db",
            "trades",
            "trades.csv",
            "--mode",
            "write",
            "--format",
            "json",
        ],
        cwd,
    ));

    // Plan a delete of the first second.
    let plan = stdout_json(&run(
        &[
            "delete-range",
            "m.db",
            "trades",
            "--start",
            "2026-07-01T09:30:00Z",
            "--end",
            "2026-07-01T09:30:01Z",
            "--plan",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(plan["summary"]["rows_affected"], 1);
    let plan_id = plan["plan_id"].as_str().unwrap().to_string();

    // Plans are listed.
    let plans = stdout_json(&run(
        &["plan", "list", "m.db", "trades", "--format", "json"],
        cwd,
    ));
    assert_eq!(plans.as_array().unwrap().len(), 1);

    // A concurrent commit moves the head…
    std::fs::write(
        cwd.join("more.csv"),
        "ts,symbol,price,size\n2026-07-01T09:31:00Z,AAPL,211.0,10\n",
    )
    .unwrap();
    stdout_json(&run(
        &["ingest", "m.db", "trades", "more.csv", "--format", "json"],
        cwd,
    ));

    // …so apply must fail with exit code 3 and a retryable conflict.
    let out = run(
        &[
            "plan", "apply", "m.db", "trades", &plan_id, "--format", "json",
        ],
        cwd,
    );
    assert_eq!(out.status.code(), Some(3), "conflict exit code");
    let env = stderr_envelope(&out);
    assert_eq!(env["code"], "version_conflict");
    assert_eq!(env["retryable"], true);

    // Discard cleans up.
    stdout_json(&run(
        &[
            "plan", "discard", "m.db", "trades", &plan_id, "--format", "json",
        ],
        cwd,
    ));
}

#[test]
fn read_only_and_limits() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    std::fs::write(cwd.join("trades.csv"), CSV).unwrap();
    stdout_json(&run(&["init", "m.db", "--format", "json"], cwd));
    stdout_json(&run(
        &[
            "create-table",
            "m.db",
            "trades",
            "--like",
            "trades.csv",
            "--time-column",
            "ts",
            "--format",
            "json",
        ],
        cwd,
    ));
    stdout_json(&run(
        &[
            "ingest",
            "m.db",
            "trades",
            "trades.csv",
            "--mode",
            "write",
            "--format",
            "json",
        ],
        cwd,
    ));

    // --max-rows truncates.
    let rows = stdout_json(&run(
        &[
            "query",
            "m.db",
            "SELECT * FROM trades ORDER BY ts",
            "--max-rows",
            "1",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(rows.as_array().unwrap().len(), 1);

    // jsonl: one object per line.
    let out = run(
        &["query", "m.db", "SELECT * FROM trades", "--format", "jsonl"],
        cwd,
    );
    assert!(out.status.success());
    let lines: Vec<_> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|l| l.to_string())
        .collect();
    assert_eq!(lines.len(), 3);
    for l in lines {
        let _: serde_json::Value = serde_json::from_str(&l).expect("jsonl line");
    }

    // csv with header.
    let out = run(
        &[
            "query",
            "m.db",
            "SELECT symbol FROM trades ORDER BY ts",
            "--format",
            "csv",
        ],
        cwd,
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.starts_with("symbol\n"), "{text}");
}

#[test]
fn policy_enforcement_through_cli() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    std::fs::write(cwd.join("trades.csv"), CSV).unwrap();
    stdout_json(&run(&["init", "m.db", "--format", "json"], cwd));
    stdout_json(&run(
        &[
            "create-table",
            "m.db",
            "trades",
            "--like",
            "trades.csv",
            "--time-column",
            "ts",
            "--format",
            "json",
        ],
        cwd,
    ));
    stdout_json(&run(
        &[
            "ingest",
            "m.db",
            "trades",
            "trades.csv",
            "--mode",
            "write",
            "--format",
            "json",
        ],
        cwd,
    ));

    // Default policy allows direct deletes.
    let pol = stdout_json(&run(&["policy", "show", "m.db", "--format", "json"], cwd));
    assert_eq!(pol["direct_delete"], true);

    // Tighten it.
    stdout_json(&run(
        &[
            "policy",
            "set",
            "m.db",
            "direct_delete=false",
            "--format",
            "json",
        ],
        cwd,
    ));

    // Direct delete now refused: exit 2 + policy_violation + actionable hint.
    let out = run(
        &[
            "delete-range",
            "m.db",
            "trades",
            "--start",
            "2026-07-01T09:30:00Z",
            "--end",
            "2026-07-01T09:30:01Z",
            "--format",
            "json",
        ],
        cwd,
    );
    assert_eq!(out.status.code(), Some(2));
    let env = stderr_envelope(&out);
    assert_eq!(env["code"], "policy_violation");
    assert!(env["hint"].as_str().unwrap().contains("--plan"));

    // The planned path still works end to end.
    let plan = stdout_json(&run(
        &[
            "delete-range",
            "m.db",
            "trades",
            "--start",
            "2026-07-01T09:30:00Z",
            "--end",
            "2026-07-01T09:30:01Z",
            "--plan",
            "--format",
            "json",
        ],
        cwd,
    ));
    let plan_id = plan["plan_id"].as_str().unwrap().to_string();
    let applied = stdout_json(&run(
        &[
            "plan", "apply", "m.db", "trades", &plan_id, "--format", "json",
        ],
        cwd,
    ));
    assert_eq!(applied["op"], "delete_range");

    // The audit trail records the reviewed path.
    let versions = stdout_json(&run(
        &["versions", "m.db", "trades", "--format", "json"],
        cwd,
    ));
    let last = versions.as_array().unwrap().last().unwrap();
    assert_eq!(last["op"], "delete_range");

    // Bad policy key is a user error.
    let out = run(&["policy", "set", "m.db", "nope=true"], cwd);
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn restore_verify_and_arrow_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = dir.path();
    std::fs::write(cwd.join("trades.csv"), CSV).unwrap();
    stdout_json(&run(&["init", "m.db", "--format", "json"], cwd));
    stdout_json(&run(
        &[
            "create-table",
            "m.db",
            "trades",
            "--like",
            "trades.csv",
            "--time-column",
            "ts",
            "--format",
            "json",
        ],
        cwd,
    ));
    stdout_json(&run(
        &[
            "ingest",
            "m.db",
            "trades",
            "trades.csv",
            "--mode",
            "write",
            "--format",
            "json",
        ],
        cwd,
    ));

    // Arrow IPC output round-trips through ingest (stdin).
    let out = run(
        &["query", "m.db", "SELECT * FROM trades", "--format", "arrow"],
        cwd,
    );
    assert!(out.status.success());
    assert!(!out.stdout.is_empty());
    std::fs::write(cwd.join("dump.arrow"), &out.stdout).unwrap();
    stdout_json(&run(
        &[
            "ingest",
            "m.db",
            "trades",
            "dump.arrow",
            "--input-format",
            "arrow",
            "--mode",
            "write",
            "--format",
            "json",
        ],
        cwd,
    ));

    // Restore v1, verify deep, timeout flag parses.
    let restored = stdout_json(&run(
        &["restore", "m.db", "trades", "1", "--format", "json"],
        cwd,
    ));
    assert_eq!(restored["op"], "restore");
    let verify = stdout_json(&run(
        &["verify", "m.db", "trades", "--deep", "--format", "json"],
        cwd,
    ));
    assert_eq!(verify["problems"].as_array().unwrap().len(), 0);
    let out = run(
        &[
            "query",
            "m.db",
            "SELECT count(*) FROM trades",
            "--timeout",
            "30s",
            "--format",
            "json",
        ],
        cwd,
    );
    assert!(out.status.success());
}
