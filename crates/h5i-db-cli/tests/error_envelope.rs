//! Error-envelope e2e tests (ROADMAP VI-A3): the machine-readable half of a
//! failure. `hint` explains a failure in prose; `next_actions` is the same
//! advice as commands an agent can run, and `did_you_mean` is a typo guess.
//!
//! The load-bearing test here is `suggested_action_actually_runs`: it takes
//! the command the binary emitted in its own error envelope and executes it,
//! so a suggestion that drifts out of date fails CI rather than an agent.

use std::path::Path;
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_h5i-db")
}

fn run(args: &[&str], cwd: &Path) -> Output {
    Command::new(bin())
        .args(args)
        .current_dir(cwd)
        .env_remove("H5I_DB_AS_OF")
        .output()
        .expect("spawn h5i-db")
}

fn ok(out: &Output) -> Output {
    assert!(
        out.status.success(),
        "expected success, got {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    out.clone()
}

fn envelope(out: &Output) -> serde_json::Value {
    assert!(
        !out.status.success(),
        "expected failure, got success\nstdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    serde_json::from_slice(&out.stderr).expect("stderr is a JSON error envelope")
}

const CSV: &str = "ts,symbol,price\n\
2026-07-01T09:30:00Z,AAPL,100.0\n\
2026-07-01T09:30:01Z,AAPL,102.0\n";
/// Overlaps the rows above, so a strict append must refuse it.
const CSV_OVERLAPPING: &str = "ts,symbol,price\n\
2026-07-01T09:30:00Z,AAPL,999.0\n";

fn bootstrap(cwd: &Path) {
    std::fs::write(cwd.join("t.csv"), CSV).unwrap();
    std::fs::write(cwd.join("overlap.csv"), CSV_OVERLAPPING).unwrap();
    ok(&run(&["init", "m.db", "--format", "json"], cwd));
    ok(&run(
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
    ok(&run(
        &["ingest", "m.db", "trades", "t.csv", "--format", "json"],
        cwd,
    ));
}

#[test]
fn envelope_is_versioned_and_keeps_the_v1_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    let e = envelope(&run(&["schema", "m.db", "nosuch", "--format", "json"], cwd));
    // v1 contract, unchanged.
    assert_eq!(e["code"], "table_not_found");
    assert!(e["message"].is_string());
    assert_eq!(e["retryable"], false);
    assert!(e["hint"].is_string(), "hint stays human-readable prose");
    // v2 additions.
    assert_eq!(e["schema_version"], 2);
    assert!(e["next_actions"].is_array());
}

#[test]
fn did_you_mean_catches_a_typo_and_stays_silent_on_a_real_miss() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);

    let typo = envelope(&run(&["schema", "m.db", "trade", "--format", "json"], cwd));
    assert_eq!(typo["did_you_mean"], "trades");

    // An unrelated name gets no guess: a confident wrong suggestion is worse
    // than none, because an agent will act on it.
    let miss = envelope(&run(
        &["schema", "m.db", "positions", "--format", "json"],
        cwd,
    ));
    assert!(
        miss["did_you_mean"].is_null(),
        "unrelated name should not be guessed: {miss}"
    );
    assert_eq!(miss["code"], "table_not_found");
}

#[test]
fn next_actions_carry_the_real_database_path_not_a_placeholder() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    let e = envelope(&run(&["schema", "m.db", "nosuch", "--format", "json"], cwd));
    let actions = e["next_actions"].as_array().unwrap();
    assert!(!actions.is_empty());
    for a in actions {
        let cmd = a["cmd"].as_str().unwrap();
        assert!(
            !cmd.contains("<db>"),
            "the placeholder must be substituted: {cmd}"
        );
        assert!(
            cmd.contains("m.db"),
            "action should name this database: {cmd}"
        );
        assert!(!a["why"].as_str().unwrap().is_empty());
    }
}

#[test]
fn out_of_order_append_offers_the_documented_escapes() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    // Appending rows that overlap existing ones is the canonical agent
    // mistake; the envelope must name both real escapes.
    let e = envelope(&run(
        &[
            "ingest",
            "m.db",
            "trades",
            "overlap.csv",
            "--format",
            "json",
        ],
        cwd,
    ));
    assert_eq!(e["code"], "sort_order_violation");
    let cmds: Vec<&str> = e["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["cmd"].as_str().unwrap())
        .collect();
    assert!(
        cmds.iter()
            .any(|c| c.contains("replace-range") && c.contains("--plan")),
        "the previewable range overwrite must be offered: {cmds:?}"
    );
    assert!(
        cmds.iter().any(|c| c.contains("--mode write")),
        "the full-restatement escape must be offered: {cmds:?}"
    );
}

#[test]
fn suggested_action_actually_runs() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    let e = envelope(&run(&["schema", "m.db", "nosuch", "--format", "json"], cwd));

    // Take the binary at its word: execute the command it suggested. This is
    // what stops next_actions from rotting into plausible-looking fiction.
    let suggested = e["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["cmd"].as_str().unwrap())
        .find(|c| c.contains("tables"))
        .expect("a table-not-found error should suggest listing tables");

    let mut parts = suggested.split_whitespace();
    assert_eq!(parts.next(), Some("h5i-db"), "suggestion: {suggested}");
    let args: Vec<&str> = parts.collect();
    let out = ok(&run(&args, cwd));
    let listed: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        listed[0]["table"], "trades",
        "the suggested command should have listed the real table: {listed}"
    );
}

#[test]
fn version_conflict_points_at_the_version_log() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    // Guard against a stale head: the table is at v1, so demanding v99 is a
    // conflict, and a conflict must tell the caller where to re-read.
    let out = run(
        &[
            "ingest",
            "m.db",
            "trades",
            "t.csv",
            "--expected-version",
            "99",
            "--format",
            "json",
        ],
        cwd,
    );
    let e = envelope(&out);
    assert_eq!(e["code"], "version_conflict");
    assert_eq!(out.status.code(), Some(3));
    let cmds: Vec<&str> = e["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["cmd"].as_str().unwrap())
        .collect();
    assert!(
        cmds.iter().any(|c| c.contains("versions m.db trades")),
        "a conflict should point at the version log: {cmds:?}"
    );
}

#[test]
fn an_idempotency_key_makes_a_retry_safe_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);

    // Re-running the *same* ingest is the shape of an agent retrying after an
    // ambiguous failure. Without a key the second attempt is rejected as out
    // of order; with one it returns the commit that already happened.
    let blind = run(
        &["ingest", "m.db", "trades", "t.csv", "--format", "json"],
        cwd,
    );
    assert_eq!(envelope(&blind)["code"], "sort_order_violation");

    let first: serde_json::Value = serde_json::from_slice(
        &ok(&run(
            &[
                "ingest",
                "m.db",
                "trades",
                "overlap.csv",
                "--mode",
                "write",
                "--idempotency-key",
                "load-2026-07-01",
                "--format",
                "json",
            ],
            cwd,
        ))
        .stdout,
    )
    .unwrap();

    let replay: serde_json::Value = serde_json::from_slice(
        &ok(&run(
            &[
                "ingest",
                "m.db",
                "trades",
                "overlap.csv",
                "--mode",
                "write",
                "--idempotency-key",
                "load-2026-07-01",
                "--format",
                "json",
            ],
            cwd,
        ))
        .stdout,
    )
    .unwrap();

    assert_eq!(
        replay["sequence"], first["sequence"],
        "the retry must return the original commit, not make a new one"
    );
    assert_eq!(replay["segments_added"], 0);
}
