//! Docs-as-tests (ROADMAP VI-A4).
//!
//! Agents execute documentation literally. One stale example and they flip
//! into guess-mode, which is the expensive failure: not a wrong command, but a
//! loss of trust in every command. So the docs are checked against the binary
//! rather than against a reviewer's memory.
//!
//! Every `h5i-db …` invocation in the README and the skill is extracted and
//! validated: the subcommand must exist, and every long flag must appear in
//! that subcommand's help. Snippets are not blindly *executed* — most name a
//! `market.db` that does not exist here, and inventing one would test the
//! fixture rather than the docs — but `demo`, which is self-contained, is run
//! end to end.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_h5i-db")
}

fn repo_root() -> PathBuf {
    // crates/h5i-db-cli -> repo root
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root")
        .to_path_buf()
}

/// One documented invocation, with where it came from for the failure message.
#[derive(Debug)]
struct Invocation {
    source: String,
    line: usize,
    tokens: Vec<String>,
}

/// Pull `h5i-db …` command lines out of fenced code blocks.
fn extract(path: &Path) -> Vec<Invocation> {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let source = path
        .strip_prefix(repo_root())
        .unwrap_or(path)
        .display()
        .to_string();

    let mut out = Vec::new();
    let mut in_fence = false;
    let mut pending: Option<(usize, String)> = None;

    for (idx, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.starts_with("```") {
            in_fence = !in_fence;
            pending = None;
            continue;
        }
        if !in_fence {
            continue;
        }
        // Continuation lines end with a backslash.
        let (start_line, joined) = match pending.take() {
            Some((start, mut acc)) => {
                acc.push(' ');
                acc.push_str(line);
                (start, acc)
            }
            None => {
                let stripped = line.strip_prefix("$ ").unwrap_or(line);
                if !stripped.starts_with("h5i-db ") {
                    continue;
                }
                (idx + 1, stripped.to_string())
            }
        };
        if let Some(head) = joined.strip_suffix('\\') {
            pending = Some((start_line, head.trim_end().to_string()));
            continue;
        }
        // Drop a trailing `# comment`, then split on whitespace. Quoted SQL is
        // irrelevant here: only the flags are being validated.
        let cmd = joined.split('#').next().unwrap_or(&joined).trim();
        let tokens: Vec<String> = cmd.split_whitespace().map(str::to_string).collect();
        out.push(Invocation {
            source: source.clone(),
            line: start_line,
            tokens,
        });
    }
    out
}

/// The subcommand path, e.g. `["snapshot", "create"]`. Nested subcommands are
/// two words; everything else is one.
fn subcommand(tokens: &[String]) -> Vec<String> {
    const NESTED: [&str; 4] = ["snapshot", "plan", "policy", "data-policy"];
    let words: Vec<&String> = tokens
        .iter()
        .skip(1) // "h5i-db"
        .take_while(|t| !t.starts_with('-'))
        .collect();
    let Some(first) = words.first() else {
        return Vec::new();
    };
    if NESTED.contains(&first.as_str()) {
        words.iter().take(2).map(|s| s.to_string()).collect()
    } else {
        vec![first.to_string()]
    }
}

fn help_for(sub: &[String]) -> Option<String> {
    let mut cmd = Command::new(bin());
    for word in sub {
        cmd.arg(word);
    }
    let out = cmd.arg("--help").output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).to_string())
}

fn doc_files() -> Vec<PathBuf> {
    let root = repo_root();
    let mut files = vec![root.join("README.md")];
    let skills = root.join("skills");
    let mut stack = vec![skills];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "md") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

#[test]
fn every_documented_command_and_flag_exists() {
    let mut checked = 0usize;
    let mut failures = Vec::new();

    for file in doc_files() {
        for inv in extract(&file) {
            let sub = subcommand(&inv.tokens);
            if sub.is_empty() {
                continue;
            }
            let Some(help) = help_for(&sub) else {
                failures.push(format!(
                    "{}:{}: no such subcommand `{}`",
                    inv.source,
                    inv.line,
                    sub.join(" ")
                ));
                continue;
            };
            checked += 1;
            // Every long flag used must be documented by that subcommand.
            let flags: BTreeSet<&str> = inv
                .tokens
                .iter()
                .filter(|t| t.starts_with("--"))
                .map(|t| t.split('=').next().unwrap_or(t))
                .collect();
            for flag in flags {
                if !help.contains(flag) {
                    failures.push(format!(
                        "{}:{}: `{}` does not accept `{flag}`",
                        inv.source,
                        inv.line,
                        sub.join(" ")
                    ));
                }
            }
        }
    }

    assert!(
        checked >= 10,
        "extracted only {checked} commands — the doc scanner is probably broken, \
         which would make this test pass vacuously"
    );
    assert!(
        failures.is_empty(),
        "documentation has drifted from the binary:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn documented_environment_variables_are_real() {
    // The skill tells agents to export these; a renamed variable would be
    // silently ignored rather than erroring, so check them explicitly.
    let root = repo_root();
    let mut documented = BTreeSet::new();
    for file in doc_files() {
        let text = std::fs::read_to_string(&file).unwrap();
        for line in text.lines() {
            for token in line.split_whitespace() {
                if let Some(name) = token.strip_prefix("H5I_DB_") {
                    let name: String = name
                        .chars()
                        .take_while(|c| c.is_ascii_uppercase() || *c == '_')
                        .collect();
                    if !name.is_empty() {
                        documented.insert(format!("H5I_DB_{name}"));
                    }
                }
            }
        }
    }
    assert!(!documented.is_empty(), "no environment variables found");

    let sources = [
        "crates/h5i-db-cli/src/main.rs",
        "crates/h5i-db-cli/src/profile.rs",
    ]
    .iter()
    .map(|p| std::fs::read_to_string(root.join(p)).unwrap())
    .collect::<Vec<_>>()
    .join("\n");
    for var in &documented {
        assert!(
            sources.contains(var.as_str()),
            "{var} is documented but the binary never reads it"
        );
    }
}

#[test]
fn the_demo_runs_end_to_end_and_shows_real_leakage() {
    let tmp = tempfile::tempdir().unwrap();
    let out = Command::new(bin())
        .args(["demo", "--dir", tmp.path().to_str().unwrap()])
        .output()
        .expect("spawn demo");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "demo failed: {}\n{text}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The tour must actually reach its punchline, not just not crash.
    assert!(
        text.contains("leakage_detected: true"),
        "no leakage shown:\n{text}"
    );
    assert!(
        text.contains("vacuous: false"),
        "the demo must use a scenario the arrival axis can see — a vacuous \
         check would make the whole tour a lie:\n{text}"
    );
    // Both axes are demonstrated.
    assert!(
        text.contains("--decision-time"),
        "event-time axis missing:\n{text}"
    );
    assert!(
        text.contains("plan apply"),
        "the review gate is missing:\n{text}"
    );

    // And it left behind a database the follow-up commands can actually use.
    let db = tmp.path().join("demo.db");
    let context = Command::new(bin())
        .args(["context", db.to_str().unwrap(), "--format", "json"])
        .output()
        .unwrap();
    assert!(context.status.success());
    let doc: serde_json::Value = serde_json::from_slice(&context.stdout).unwrap();
    assert_eq!(doc["tables"][0]["name"], "trades");
    assert_eq!(doc["tables"][0]["version"], 2, "the restatement committed");
}
