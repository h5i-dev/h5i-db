//! `context` e2e tests (ROADMAP VI-A1, VI-B3).
//!
//! The command exists so an agent's first move is one call instead of a
//! tables → schema → sample → versions walk per table. What the tests pin
//! down is therefore: it really does answer all of those at once, it stays
//! byte-stable so it can be cached as a preamble, and it never hides a
//! pending plan or a policy gate the agent is about to trip over.

use std::path::Path;
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_h5i-db")
}

fn run(args: &[&str], cwd: &Path) -> Output {
    Command::new(bin())
        .args(args)
        .current_dir(cwd)
        .env_remove("H5I_DB_PROFILE")
        .env_remove("H5I_DB_AS_OF")
        .output()
        .expect("spawn h5i-db")
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

fn json(out: Output) -> serde_json::Value {
    serde_json::from_slice(&ok(out).stdout).expect("stdout is JSON")
}

const TRADES: &str = "ts,symbol,price\n\
2026-07-01T09:30:00Z,AAPL,100.0\n\
2026-07-01T09:30:01Z,AAPL,102.0\n\
2026-07-01T09:30:02Z,MSFT,400.0\n";
const QUOTES: &str = "ts,symbol,bid\n\
2026-07-01T09:30:00Z,AAPL,99.0\n";

fn bootstrap(cwd: &Path) {
    std::fs::write(cwd.join("trades.csv"), TRADES).unwrap();
    std::fs::write(cwd.join("quotes.csv"), QUOTES).unwrap();
    ok(run(&["init", "m.db", "--format", "json"], cwd));
    for (table, file) in [("trades", "trades.csv"), ("quotes", "quotes.csv")] {
        ok(run(
            &[
                "create-table",
                "m.db",
                table,
                "--like",
                file,
                "--time-column",
                "ts",
                "--format",
                "json",
            ],
            cwd,
        ));
        ok(run(
            &["ingest", "m.db", table, file, "--format", "json"],
            cwd,
        ));
    }
}

fn table<'a>(doc: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    doc["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == name)
        .unwrap_or_else(|| panic!("no table {name} in {doc}"))
}

#[test]
fn one_call_answers_what_the_discovery_walk_used_to() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    let doc = json(run(&["context", "m.db", "--format", "json"], cwd));

    assert_eq!(doc["schema_version"], 1);
    assert_eq!(doc["tables"].as_array().unwrap().len(), 2);

    let trades = table(&doc, "trades");
    // Everything `tables`, `schema` and `versions` would have said.
    assert_eq!(trades["rows"], 3);
    assert_eq!(trades["version"], 1);
    assert_eq!(trades["time_column"], "ts");
    assert!(trades["time_range"].is_array());
    assert_eq!(trades["last_commit"]["op"], "append");
    let cols: Vec<&str> = trades["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(cols, ["ts", "symbol", "price"]);
    assert_eq!(trades["data_policy"], false);
    assert!(trades["pending_plans"].as_array().unwrap().is_empty());
}

#[test]
fn output_is_byte_stable_so_it_can_be_cached() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    // Two runs against the same state must agree exactly — that is what makes
    // it safe to paste into an AGENTS.md preamble.
    let a = ok(run(&["context", "m.db", "--format", "json"], cwd)).stdout;
    let b = ok(run(&["context", "m.db", "--format", "json"], cwd)).stdout;
    assert_eq!(a, b, "context must not vary between identical runs");

    let doc: serde_json::Value = serde_json::from_slice(&a).unwrap();
    let names: Vec<&str> = doc["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "tables are ordered, not catalog-dependent");

    // No clock-dependent field unless asked for.
    assert!(
        doc["tables"][0]["freshness"].is_null(),
        "default output must be clock-free: {doc}"
    );
}

#[test]
fn stale_after_opts_into_freshness() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    // Just committed, so nothing is stale at an hour...
    let fresh = json(run(
        &["context", "m.db", "--stale-after", "1h", "--format", "json"],
        cwd,
    ));
    assert_eq!(table(&fresh, "trades")["freshness"]["stale"], false);

    // ...but everything is stale at zero seconds, which is what an ingest loop
    // that has quietly stopped would look like.
    let stale = json(run(
        &["context", "m.db", "--stale-after", "0s", "--format", "json"],
        cwd,
    ));
    assert_eq!(table(&stale, "trades")["freshness"]["stale"], true);
    assert!(
        table(&stale, "trades")["freshness"]["last_commit_age_seconds"]
            .as_i64()
            .unwrap()
            >= 0
    );
}

#[test]
fn a_staged_plan_and_a_policy_gate_are_both_surfaced() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    // Stage a mutation and close the direct-delete door.
    let plan = json(run(
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
    let plan_id = plan["plan_id"].as_str().unwrap();
    ok(run(
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

    let doc = json(run(&["context", "m.db", "--format", "json"], cwd));
    // Work already staged is the single most important thing an agent can be
    // unaware of.
    let pending = table(&doc, "trades")["pending_plans"].as_array().unwrap();
    assert_eq!(pending.len(), 1, "the staged plan must be visible: {doc}");
    assert_eq!(pending[0]["plan_id"], plan_id);
    assert_eq!(pending[0]["rows_affected"], 1);
    // And so is the gate it will hit if it tries to mutate directly.
    let gated: Vec<&str> = doc["plan_required_for"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(gated, ["delete"]);
}

#[test]
fn a_budget_sheds_detail_in_order_and_says_what_it_dropped() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    bootstrap(cwd);
    ok(run(
        &["snapshot", "create", "m.db", "pin", "--format", "json"],
        cwd,
    ));

    let full = json(run(&["context", "m.db", "--format", "json"], cwd));
    assert!(full["tables"][0]["columns"].is_array());
    assert!(full.get("omitted").is_none(), "nothing dropped: {full}");

    // A tight budget keeps the shape and names what it withheld.
    let small = json(run(
        &["context", "m.db", "--budget", "60", "--format", "json"],
        cwd,
    ));
    assert!(
        small["tables"][0]["columns"].is_null(),
        "columns are shed first: {small}"
    );
    assert!(small["tables"][0]["column_count"].is_number());
    assert!(
        small["omitted"]["columns"]["recover_with"]
            .as_str()
            .unwrap()
            .contains("schema"),
        "every omission must name its recovery command: {small}"
    );
    assert_eq!(small["budget_tokens"], 60);
    assert!(
        serde_json::to_string(&small).unwrap().len() < serde_json::to_string(&full).unwrap().len(),
        "a budgeted document must actually be smaller"
    );
}

#[test]
fn context_on_an_empty_database_still_answers() {
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path();
    ok(run(&["init", "m.db", "--format", "json"], cwd));
    let doc = json(run(&["context", "m.db", "--format", "json"], cwd));
    assert!(doc["tables"].as_array().unwrap().is_empty());
    assert!(doc["plan_required_for"].as_array().unwrap().is_empty());
}
