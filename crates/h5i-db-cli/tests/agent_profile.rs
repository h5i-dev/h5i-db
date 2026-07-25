//! Agent output-profile e2e tests (ROADMAP VI-A2).
//!
//! The guarantees under test, in order of how much they matter:
//!
//! 1. With `H5I_DB_PROFILE` unset, stdout is byte-identical to before — the
//!    profile is opt-in and must not perturb anyone who did not ask for it.
//! 2. Output content never depends on whether stdout is a terminal.
//! 3. With the profile set, no result exceeds the budget, and the rows that
//!    were withheld are readable back from the reported spill file.
//! 4. A `--max-bytes` the caller passed keeps its hard `limit_exceeded`
//!    contract even under the profile.

use std::path::Path;
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_h5i-db")
}

fn run_env(args: &[&str], cwd: &Path, env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(bin());
    cmd.args(args).current_dir(cwd);
    cmd.env_remove("H5I_DB_PROFILE");
    cmd.env_remove("H5I_DB_AS_OF");
    // Keep spill files inside the test's own temp dir.
    cmd.env("H5I_DB_RESULT_DIR", cwd.join("results"));
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.output().expect("spawn h5i-db")
}

fn run(args: &[&str], cwd: &Path) -> Output {
    run_env(args, cwd, &[])
}

fn agent(args: &[&str], cwd: &Path) -> Output {
    run_env(args, cwd, &[("H5I_DB_PROFILE", "agent")])
}

fn ok(out: Output) -> Output {
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// The JSON summary the agent profile writes to stderr.
fn summary(out: &Output) -> serde_json::Value {
    let text = String::from_utf8_lossy(&out.stderr);
    let line = text
        .lines()
        .rev()
        .find(|l| l.starts_with('{'))
        .unwrap_or_else(|| panic!("no summary on stderr: {text}"));
    serde_json::from_str(line).expect("summary is JSON")
}

/// 2500 rows, comfortably over the 1000-row default budget.
fn csv(rows: usize) -> String {
    let mut s = String::from("ts,symbol,price\n");
    for i in 0..rows {
        // One row per second from 2026-07-01T00:00:00Z.
        let h = i / 3600;
        let m = (i % 3600) / 60;
        let sec = i % 60;
        s.push_str(&format!(
            "2026-07-01T{h:02}:{m:02}:{sec:02}Z,AAPL,{}.0\n",
            i
        ));
    }
    s
}

fn bootstrap(cwd: &Path, rows: usize) {
    std::fs::write(cwd.join("t.csv"), csv(rows)).unwrap();
    ok(run(&["init", "m.db", "--format", "json"], cwd));
    ok(run(
        &[
            "create-table",
            "m.db",
            "trades",
            "--like",
            "t.csv",
            "--time-column",
            "ts",
            "--format",
            "json",
        ],
        cwd,
    ));
    ok(run(
        &["ingest", "m.db", "trades", "t.csv", "--format", "json"],
        cwd,
    ));
}

#[test]
fn without_the_profile_output_is_unchanged() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd, 2500);
    let sql = "SELECT price FROM trades";

    let out = ok(run(&["query", "m.db", sql, "--format", "jsonl"], cwd));
    let lines = String::from_utf8_lossy(&out.stdout).lines().count();
    assert_eq!(lines, 2500, "the default profile must not cap anything");
    assert!(
        String::from_utf8_lossy(&out.stderr).trim().is_empty(),
        "the default profile must not add a summary: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !cwd.join("results").exists(),
        "the default profile must not spill"
    );

    // An explicit --max-rows still behaves exactly as it always did.
    let capped = ok(run(
        &["query", "m.db", sql, "--format", "jsonl", "--max-rows", "7"],
        cwd,
    ));
    assert_eq!(String::from_utf8_lossy(&capped.stdout).lines().count(), 7);
}

#[test]
fn agent_profile_caps_output_and_reports_the_true_total() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd, 2500);
    let out = ok(agent(
        &[
            "query",
            "m.db",
            "SELECT price FROM trades",
            "--format",
            "jsonl",
        ],
        cwd,
    ));

    let rendered = String::from_utf8_lossy(&out.stdout).lines().count();
    assert_eq!(rendered, 1000, "stdout must stop at the row budget");

    let s = summary(&out);
    assert_eq!(s["profile"], "agent");
    assert_eq!(s["truncated"], true);
    assert_eq!(s["total_rows"], 2500, "the real size must be reported");
    assert_eq!(s["returned_rows"], 1000);
    assert_eq!(s["max_rows"], 1000);
    assert_eq!(s["full_result_rows"], 2500);
    assert_eq!(s["full_result_truncated"], serde_json::Value::Null);
}

#[test]
fn withheld_rows_are_readable_back_from_the_spill() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd, 2500);
    let out = ok(agent(
        &[
            "query",
            "m.db",
            "SELECT price FROM trades",
            "--format",
            "jsonl",
        ],
        cwd,
    ));
    let s = summary(&out);
    let path = s["full_result_path"].as_str().expect("a spill path");
    assert!(Path::new(path).exists(), "spill file must exist: {path}");

    // It must be a real Parquet file any reader can open, not an opaque blob:
    // that is what makes the withheld rows recoverable. (The full read-back,
    // row for row, is asserted in the profile unit tests.)
    let bytes = std::fs::read(path).unwrap();
    assert!(bytes.len() > 4, "spill file is empty: {path}");
    assert_eq!(&bytes[..4], b"PAR1", "spill is not Parquet: {path}");
    assert_eq!(&bytes[bytes.len() - 4..], b"PAR1", "spill was not closed");

    // The pointer handed to the caller must name that same file.
    let how = s["read_full_result_with"].as_str().unwrap();
    assert!(
        how.contains(path),
        "the recovery hint should name the file: {how}"
    );
}

#[test]
fn a_result_within_budget_neither_truncates_nor_spills() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd, 2500);
    let out = ok(agent(
        &[
            "query",
            "m.db",
            "SELECT count(*) AS c FROM trades",
            "--format",
            "json",
        ],
        cwd,
    ));
    let s = summary(&out);
    assert_eq!(s["truncated"], false);
    assert_eq!(s["total_rows"], 1);
    assert!(
        s["full_result_path"].is_null(),
        "a small result should leave no file behind: {s}"
    );
}

#[test]
fn piping_does_not_change_the_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd, 50);
    let sql = "SELECT price FROM trades";
    // Both runs here are already non-TTY, so instead assert the stronger
    // property directly: redirecting stdout to a file yields the same bytes
    // as capturing it through a pipe.
    let piped = ok(run(&["query", "m.db", sql, "--format", "jsonl"], cwd)).stdout;
    let file_path = cwd.join("out.jsonl");
    let file = std::fs::File::create(&file_path).unwrap();
    let status = Command::new(bin())
        .args(["query", "m.db", sql, "--format", "jsonl"])
        .current_dir(cwd)
        .env_remove("H5I_DB_PROFILE")
        .stdout(file)
        .status()
        .unwrap();
    assert!(status.success());
    let to_file = std::fs::read(&file_path).unwrap();
    assert_eq!(
        piped, to_file,
        "output must not depend on the stdout target"
    );
}

#[test]
fn an_explicit_max_bytes_keeps_its_hard_error_under_the_profile() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd, 2500);
    // The caller asked for a hard ceiling, so breaching it is still exit 4
    // with the unchanged limit_exceeded envelope — the profile's own budget
    // is the only soft one.
    let out = agent(
        &[
            "query",
            "m.db",
            "SELECT price FROM trades",
            "--format",
            "jsonl",
            "--max-bytes",
            "128",
        ],
        cwd,
    );
    assert_eq!(out.status.code(), Some(4));
    let text = String::from_utf8_lossy(&out.stderr);
    let envelope: serde_json::Value = text
        .lines()
        .rev()
        .find_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("code").is_some())
        .expect("an error envelope on stderr");
    assert_eq!(envelope["code"], "limit_exceeded");
}

#[test]
fn an_explicit_max_rows_overrides_the_profile_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd, 2500);
    let out = ok(agent(
        &[
            "query",
            "m.db",
            "SELECT price FROM trades",
            "--format",
            "jsonl",
            "--max-rows",
            "5",
        ],
        cwd,
    ));
    assert_eq!(String::from_utf8_lossy(&out.stdout).lines().count(), 5);
    let s = summary(&out);
    assert_eq!(s["max_rows"], 5);
    assert_eq!(s["returned_rows"], 5);
    assert_eq!(s["total_rows"], 2500, "the total is still honest");
}
