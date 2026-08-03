//! End-to-end tests for the `h5i-nb` binary.
//!
//! The tests that need a kernel are ignored by default; run them the same way
//! as `tests/kernel.rs`:
//!
//! ```bash
//! H5I_TEST_KERNEL_JSON=… cargo test -p h5i-db-notebook --test cli -- --ignored --test-threads=1
//! ```

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::TempDir;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_h5i-nb")
}

/// The kernel name to write into test notebooks, and the search path that
/// makes it resolvable.
fn test_kernel() -> Option<(String, PathBuf)> {
    let json = PathBuf::from(std::env::var("H5I_TEST_KERNEL_JSON").ok()?);
    let name = json.parent()?.file_name()?.to_string_lossy().to_string();
    // …/share/jupyter/kernels/<name>/kernel.json  ->  …/share/jupyter
    let jupyter_path = json.parent()?.parent()?.parent()?.to_path_buf();
    Some((name, jupyter_path))
}

struct Nb {
    dir: TempDir,
    notebook: PathBuf,
    jupyter_path: Option<PathBuf>,
}

impl Nb {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let notebook = dir.path().join("test.ipynb");
        Nb {
            dir,
            notebook,
            jupyter_path: None,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut command = Command::new(binary());
        command.args(args).current_dir(self.dir.path());
        if let Some(path) = &self.jupyter_path {
            command.env("JUPYTER_PATH", path);
        }
        command.output().expect("failed to run h5i-nb")
    }

    fn path(&self) -> &str {
        self.notebook.to_str().unwrap()
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn code(output: &Output) -> i32 {
    output.status.code().unwrap_or(-1)
}

// ---------------------------------------------------------------------------
// no kernel needed
// ---------------------------------------------------------------------------

#[test]
fn kernel_list_succeeds_even_with_nothing_installed() {
    let nb = Nb::new();
    let output = nb.run(&["kernel", "list", "--format", "json"]);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    let parsed: serde_json::Value = serde_json::from_str(&stdout(&output)).unwrap();
    assert!(parsed["kernels"].is_array());
}

#[test]
fn a_missing_notebook_reports_a_machine_readable_error() {
    let nb = Nb::new();
    let output = nb.run(&["cells", "does-not-exist.ipynb", "--format", "json"]);
    assert_eq!(code(&output), 2, "{}", stderr(&output));
    let envelope: serde_json::Value = serde_json::from_str(stderr(&output).trim()).unwrap();
    assert_eq!(envelope["code"], "io");
    assert_eq!(envelope["retryable"], false);
    assert!(stdout(&output).is_empty(), "errors must not pollute stdout");
}

#[test]
fn asking_for_an_uninstalled_kernel_lists_what_exists() {
    let nb = Nb::new();
    let output = nb.run(&["new", nb.path(), "--kernel", "definitely-not-installed"]);
    assert_eq!(code(&output), 2);
    let text = stderr(&output);
    assert!(text.contains("kernel_not_found"), "{text}");
    assert!(text.contains("hint:"), "no actionable hint: {text}");
    assert!(!nb.notebook.exists(), "a failed create left a file behind");
}

#[test]
fn edit_commands_work_without_ever_starting_a_kernel() {
    let nb = Nb::new();
    // Written directly so the test does not depend on an installed kernel.
    std::fs::write(
        &nb.notebook,
        r#"{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}"#,
    )
    .unwrap();

    assert_eq!(
        code(&nb.run(&["edit", nb.path(), "insert", "--code", "first"])),
        0
    );
    assert_eq!(
        code(&nb.run(&[
            "edit",
            nb.path(),
            "insert",
            "--kind",
            "markdown",
            "--code",
            "# doc"
        ])),
        0
    );
    assert_eq!(
        code(&nb.run(&["edit", nb.path(), "insert", "--at", "0", "--code", "zeroth"])),
        0
    );

    let listing = nb.run(&["cells", nb.path(), "--format", "json"]);
    let parsed: serde_json::Value = serde_json::from_str(&stdout(&listing)).unwrap();
    let cells = parsed["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 3);
    assert_eq!(cells[0]["preview"], "zeroth");
    assert_eq!(cells[2]["type"], "markdown");

    assert_eq!(code(&nb.run(&["edit", nb.path(), "delete", "1"])), 0);
    assert_eq!(code(&nb.run(&["edit", nb.path(), "move", "0", "1"])), 0);

    let listing = nb.run(&["cells", nb.path(), "--format", "json"]);
    let parsed: serde_json::Value = serde_json::from_str(&stdout(&listing)).unwrap();
    let cells = parsed["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 2);
    assert_eq!(cells[1]["preview"], "zeroth");
}

#[test]
fn editing_a_cell_drops_outputs_that_no_longer_describe_it() {
    // Stale outputs attached to rewritten code are actively misleading.
    let nb = Nb::new();
    std::fs::write(
        &nb.notebook,
        r#"{"cells": [{"cell_type": "code", "execution_count": 1, "id": "a",
             "metadata": {}, "outputs": [{"output_type": "stream", "name": "stdout",
             "text": "stale"}], "source": "print('old')"}],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5}"#,
    )
    .unwrap();
    assert_eq!(
        code(&nb.run(&["edit", nb.path(), "set", "0", "--code", "print('new')"])),
        0
    );
    let listing = nb.run(&["cells", nb.path(), "--format", "json"]);
    let parsed: serde_json::Value = serde_json::from_str(&stdout(&listing)).unwrap();
    assert_eq!(parsed["cells"][0]["outputs"], "-");
    assert!(parsed["cells"][0]["execution_count"].is_null());
}

#[test]
fn an_out_of_range_cell_names_the_range_that_exists() {
    let nb = Nb::new();
    std::fs::write(
        &nb.notebook,
        r#"{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}"#,
    )
    .unwrap();
    let output = nb.run(&["output", nb.path(), "7"]);
    assert_eq!(code(&output), 2);
    assert!(
        stderr(&output).contains("cell_index_out_of_range")
            || stderr(&output).contains("cell_id_not_found"),
        "{}",
        stderr(&output)
    );
}

// ---------------------------------------------------------------------------
// kernel required
// ---------------------------------------------------------------------------

fn with_kernel() -> Nb {
    let (name, jupyter_path) = test_kernel().expect("set H5I_TEST_KERNEL_JSON");
    let mut nb = Nb::new();
    nb.jupyter_path = Some(jupyter_path);
    let output = nb.run(&["new", nb.path(), "--kernel", &name]);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    nb
}

fn stop(nb: &Nb) {
    let _ = nb.run(&["kernel", "stop", nb.path()]);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn state_persists_between_separate_cli_invocations() {
    // The claim the whole crate rests on. Each `exec` is its own process.
    let nb = with_kernel();
    let first = nb.run(&["exec", nb.path(), "--code", "import math; x = 41"]);
    assert_eq!(code(&first), 0, "{}", stderr(&first));

    let second = nb.run(&["exec", nb.path(), "--code", "x + math.floor(1.5)"]);
    assert_eq!(code(&second), 0, "{}", stderr(&second));
    assert!(stdout(&second).contains("42"), "{}", stdout(&second));
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn a_raised_cell_exits_non_zero_in_both_formats() {
    // An exit code that depends on --format would be a trap for any shell
    // pipeline or supervising agent.
    let nb = with_kernel();
    for format in ["table", "json"] {
        let output = nb.run(&["exec", nb.path(), "--code", "1/0", "--format", format]);
        assert_eq!(
            code(&output),
            2,
            "format {format} exited {}: {}",
            code(&output),
            stderr(&output)
        );
        assert!(
            stderr(&output).contains("cell_raised"),
            "format {format}: {}",
            stderr(&output)
        );
    }
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn a_traceback_reaches_stdout_and_the_notebook() {
    let nb = with_kernel();
    let output = nb.run(&["exec", nb.path(), "--code", "def f(): 1/0\nf()"]);
    assert_eq!(code(&output), 2);
    let text = stdout(&output);
    assert!(text.contains("ZeroDivisionError"), "{text}");

    // And it survived into the file, so `nb output` can retrieve it later.
    let saved = nb.run(&["output", nb.path(), "0"]);
    assert!(
        stdout(&saved).contains("ZeroDivisionError"),
        "{}",
        stdout(&saved)
    );
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn a_timeout_exits_four_and_leaves_the_session_usable() {
    let nb = with_kernel();
    let output = nb.run(&[
        "exec",
        nb.path(),
        "--code",
        "while True: pass",
        "--timeout",
        "3",
    ]);
    assert_eq!(code(&output), 4, "{}", stderr(&output));
    assert!(
        stderr(&output).contains("execute_timeout"),
        "{}",
        stderr(&output)
    );

    let after = nb.run(&["exec", nb.path(), "--code", "'recovered'"]);
    assert_eq!(code(&after), 0, "{}", stderr(&after));
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn restart_clears_state_and_from_clean_reruns_everything() {
    let nb = with_kernel();
    assert_eq!(code(&nb.run(&["exec", nb.path(), "--code", "y = 7"])), 0);
    assert_eq!(code(&nb.run(&["exec", nb.path(), "--code", "y"])), 0);

    assert_eq!(code(&nb.run(&["kernel", "restart", nb.path()])), 0);
    let after = nb.run(&["exec", nb.path(), "--code", "y"]);
    assert_eq!(code(&after), 2, "state survived a restart");

    // `run --from-clean` re-executes from a fresh kernel, so the cells that
    // define `y` run before the one that reads it and the run succeeds.
    let clean = nb.run(&["run", nb.path(), "--cells", "0-1", "--from-clean"]);
    assert_eq!(code(&clean), 0, "{}", stderr(&clean));
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn stopping_and_restarting_a_session_is_not_racy() {
    // A supervisor that is shutting down still has a connectable socket for a
    // moment; the client must recover instead of reporting a reset peer.
    let nb = with_kernel();
    for round in 0..3 {
        let output = nb.run(&["exec", nb.path(), "--code", &format!("{round}")]);
        assert_eq!(code(&output), 0, "round {round}: {}", stderr(&output));
        assert_eq!(code(&nb.run(&["kernel", "stop", nb.path()])), 0);
    }
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn stream_mode_does_not_print_the_output_twice() {
    let nb = with_kernel();
    let output = nb.run(&[
        "exec",
        nb.path(),
        "--code",
        "import sys\nprint('unique-marker'); sys.stdout.flush()",
        "--stream",
    ]);
    assert_eq!(code(&output), 0, "{}", stderr(&output));
    assert_eq!(
        stdout(&output).matches("unique-marker").count(),
        1,
        "output was duplicated: {}",
        stdout(&output)
    );
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn the_written_notebook_is_valid_nbformat() {
    let nb = with_kernel();
    assert_eq!(
        code(&nb.run(&["exec", nb.path(), "--code", "print('x')\n1+1"])),
        0
    );
    stop(&nb);

    // Round-tripping through the real library is the interop guarantee: a file
    // Jupyter rewrites differently would churn in every git diff.
    let python = std::env::var("H5I_TEST_KERNEL_JSON")
        .ok()
        .and_then(|p| {
            let prefix = Path::new(&p)
                .parent()?
                .parent()?
                .parent()?
                .parent()?
                .to_path_buf();
            Some(prefix.join("bin/python"))
        })
        .filter(|p| p.exists());
    let Some(python) = python else {
        eprintln!("skipping nbformat validation: no interpreter alongside the test kernel");
        return;
    };

    let script = format!(
        "import nbformat;\
         nb = nbformat.read({0:?}, as_version=4);\
         nbformat.validate(nb);\
         before = open({0:?},'rb').read();\
         nbformat.write(nb, {0:?});\
         assert before == open({0:?},'rb').read(), 'Jupyter rewrote the file differently';\
         print('ok')",
        nb.path()
    );
    let output = Command::new(python).arg("-c").arg(script).output().unwrap();
    assert!(
        output.status.success(),
        "nbformat rejected our file: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
