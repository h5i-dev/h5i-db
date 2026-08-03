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
use std::time::{Duration, Instant};

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

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(binary());
        command
            .args(args)
            .current_dir(self.dir.path())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(path) = &self.jupyter_path {
            command.env("JUPYTER_PATH", path);
        }
        command
    }

    fn spawn(&self, args: &[&str]) -> std::process::Child {
        self.command(args).spawn().expect("failed to spawn h5i-nb")
    }

    /// Run a command, killing it if it outlives `limit`.
    ///
    /// `None` means it had to be killed, which is how the tests below assert
    /// that a call answers at all: a hang is the failure being tested for, so
    /// blocking on it forever would turn a red test into a stuck suite.
    fn run_within(&self, args: &[&str], limit: Duration) -> Option<Output> {
        let mut child = self.spawn(args);
        let deadline = Instant::now() + limit;
        loop {
            match child.try_wait().expect("try_wait failed") {
                Some(_) => return Some(child.wait_with_output().expect("wait_with_output failed")),
                None if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                None => std::thread::sleep(Duration::from_millis(25)),
            }
        }
    }

    /// Write a notebook without going through `new`, which needs a kernelspec.
    fn write_bare_notebook(&self) {
        std::fs::write(
            &self.notebook,
            r#"{"cells": [], "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}}, "nbformat": 4, "nbformat_minor": 5}"#,
        )
        .unwrap();
    }

    /// Poll until a supervisor is serving this notebook.
    fn wait_until_running(&self, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            let output = self.run(&["kernel", "status", self.path()]);
            if !stdout(&output).contains("no session running") {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
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

#[test]
fn a_second_supervisor_refuses_to_take_over_a_live_session() {
    // Two supervisors on one notebook each hold it in memory and each rewrite
    // the whole file, so the second to save silently erases the first one's
    // cells. Ownership is claimed before the socket is touched, so the loser
    // must neither serve nor unlink anything.
    let nb = Nb::new();
    nb.write_bare_notebook();

    let mut first = nb.spawn(&["serve", nb.path()]);
    assert!(
        nb.wait_until_running(Duration::from_secs(10)),
        "the first supervisor never started serving"
    );

    let second = nb
        .run_within(&["serve", nb.path()], Duration::from_secs(10))
        .expect("the second supervisor hung instead of refusing");
    assert_eq!(code(&second), 3, "{}", stderr(&second));
    assert!(
        stderr(&second).contains("session_already_running"),
        "{}",
        stderr(&second)
    );

    // The loser must not have taken the winner's socket down on its way out.
    let status = nb.run(&["kernel", "status", nb.path()]);
    assert!(
        !stdout(&status).contains("no session running"),
        "the refused supervisor removed the live one's socket: {}",
        stdout(&status)
    );

    let _ = nb.run(&["kernel", "stop", nb.path()]);
    let _ = first.kill();
    let _ = first.wait();
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

// ---------------------------------------------------------------------------
// export, images, and the other N5 surfaces (no kernel needed)
// ---------------------------------------------------------------------------

/// A notebook written directly, so export tests never need a kernel.
fn notebook_with_outputs(nb: &Nb) {
    std::fs::write(
        &nb.notebook,
        r##"{"cells": [
             {"cell_type": "markdown", "id": "m", "metadata": {}, "source": "# Title"},
             {"cell_type": "code", "execution_count": 1, "id": "c1", "metadata": {},
              "outputs": [{"output_type": "stream", "name": "stdout", "text": "hello\n"}],
              "source": "print('hello')"},
             {"cell_type": "code", "execution_count": 2, "id": "c2", "metadata": {},
              "outputs": [{"output_type": "display_data", "metadata": {},
                           "data": {"image/png": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
                                    "text/plain": "<Figure>"}}],
              "source": "plot()"}
           ], "metadata": {"kernelspec": {"name": "python3", "language": "python", "display_name": "P"}},
           "nbformat": 4, "nbformat_minor": 5}"##,
    )
    .unwrap();
}

#[test]
fn export_writes_markdown_python_and_html() {
    let nb = Nb::new();
    notebook_with_outputs(&nb);

    let md = nb.run(&["export", nb.path(), "--to", "md"]);
    assert_eq!(code(&md), 0, "{}", stderr(&md));
    assert!(stdout(&md).contains("# Title"), "{}", stdout(&md));
    assert!(stdout(&md).contains("```python"), "{}", stdout(&md));

    let py = nb.run(&["export", nb.path(), "--to", "py"]);
    assert_eq!(code(&py), 0);
    assert!(stdout(&py).contains("# %%"), "{}", stdout(&py));

    let html = nb.run(&["export", nb.path(), "--to", "html"]);
    assert_eq!(code(&html), 0);
    assert!(
        stdout(&html).starts_with("<!doctype html>"),
        "{}",
        stdout(&html)
    );
    // Self-contained: the image is inlined, nothing is fetched.
    assert!(stdout(&html).contains("data:image/png;base64,"));
    assert!(!stdout(&html).contains("https://"));
}

#[test]
fn export_to_a_file_reports_what_it_wrote() {
    let nb = Nb::new();
    notebook_with_outputs(&nb);
    let out = nb.dir.path().join("export.html");
    let result = nb.run(&[
        "export",
        nb.path(),
        "--to",
        "html",
        "-o",
        out.to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert_eq!(code(&result), 0, "{}", stderr(&result));
    let parsed: serde_json::Value = serde_json::from_str(&stdout(&result)).unwrap();
    assert_eq!(parsed["format"], "html");
    assert!(out.exists());
    assert!(
        std::fs::read_to_string(&out)
            .unwrap()
            .contains("<!doctype html>")
    );
}

#[test]
fn an_unknown_export_format_lists_the_supported_ones() {
    let nb = Nb::new();
    notebook_with_outputs(&nb);
    let result = nb.run(&["export", nb.path(), "--to", "pdf"]);
    assert_eq!(code(&result), 2);
    assert!(
        stderr(&result).contains("md, py, html"),
        "{}",
        stderr(&result)
    );
}

#[test]
fn without_outputs_produces_a_clean_export() {
    let nb = Nb::new();
    notebook_with_outputs(&nb);
    let md = nb.run(&["export", nb.path(), "--to", "md", "--without-outputs"]);
    assert!(!stdout(&md).contains("hello\n```"), "{}", stdout(&md));
    assert!(stdout(&md).contains("print('hello')"), "{}", stdout(&md));
}

#[test]
fn nb_output_writes_images_beside_the_notebook_and_names_the_file() {
    let nb = Nb::new();
    notebook_with_outputs(&nb);
    let result = nb.run(&["output", nb.path(), "2"]);
    assert_eq!(code(&result), 0, "{}", stderr(&result));

    let expected = nb.dir.path().join("test_files/cell-c2-0.png");
    assert!(expected.exists(), "no file at {}", expected.display());
    // A real PNG, not the base64 text.
    let bytes = std::fs::read(&expected).unwrap();
    assert_eq!(&bytes[..4], b"\x89PNG");
    // The base64 never reaches the terminal.
    assert!(stdout(&result).contains("[saved"), "{}", stdout(&result));
    assert!(!stdout(&result).contains("iVBORw0"), "{}", stdout(&result));
    assert!(stdout(&result).len() < 400);
}

#[test]
fn a_text_only_notebook_creates_no_attachment_directory() {
    let nb = Nb::new();
    std::fs::write(
        &nb.notebook,
        r#"{"cells": [{"cell_type": "code", "execution_count": 1, "id": "a",
             "metadata": {}, "outputs": [{"output_type": "stream", "name": "stdout",
             "text": "just text"}], "source": "print(1)"}],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5}"#,
    )
    .unwrap();
    assert_eq!(code(&nb.run(&["output", nb.path(), "0"])), 0);
    assert!(
        !nb.dir.path().join("test_files").exists(),
        "an attachment directory was created for a notebook with no attachments"
    );
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn a_detached_cell_returns_at_once_and_still_records_its_output() {
    // The point of the supervisor owning the notebook: the client can leave
    // and the outputs still land.
    let nb = with_kernel();
    let started = std::time::Instant::now();
    let queued = nb.run(&[
        "exec",
        nb.path(),
        "--detach",
        "--code",
        "import time; time.sleep(4); print('late output')",
        "--format",
        "json",
    ]);
    assert_eq!(code(&queued), 0, "{}", stderr(&queued));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(3),
        "a detached exec waited for the cell: {:?}",
        started.elapsed()
    );
    let parsed: serde_json::Value = serde_json::from_str(&stdout(&queued)).unwrap();
    assert_eq!(parsed["detached"], true);
    assert_eq!(parsed["cell"], 0);

    // While it runs, a second exec is refused rather than left to hang.
    let busy = nb.run(&["exec", nb.path(), "--code", "1"]);
    assert_eq!(code(&busy), 3, "{}", stderr(&busy));
    assert!(stderr(&busy).contains("session_busy"), "{}", stderr(&busy));

    // Status still answers while the session is held.
    let status = nb.run(&["kernel", "status", nb.path(), "--format", "json"]);
    assert_eq!(code(&status), 0, "{}", stderr(&status));
    let parsed: serde_json::Value = serde_json::from_str(&stdout(&status)).unwrap();
    assert_eq!(parsed["busy"], true);
    assert!(
        parsed["notebook"].as_str().is_some_and(|p| !p.is_empty()),
        "a busy status must still say which notebook it is: {parsed}"
    );

    for _ in 0..60 {
        std::thread::sleep(std::time::Duration::from_millis(500));
        let saved = std::fs::read_to_string(&nb.notebook).unwrap();
        if saved.contains("late output") {
            stop(&nb);
            return;
        }
    }
    stop(&nb);
    panic!("the detached cell never recorded its output");
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn a_detached_cell_can_be_interrupted() {
    // Interrupting must not need the session lock, which the cell holds.
    let nb = with_kernel();
    assert_eq!(
        code(&nb.run(&[
            "exec",
            nb.path(),
            "--detach",
            "--code",
            "import time; time.sleep(120)"
        ])),
        0
    );
    std::thread::sleep(std::time::Duration::from_secs(3));

    let started = std::time::Instant::now();
    let interrupted = nb.run(&["kernel", "interrupt", nb.path()]);
    assert_eq!(code(&interrupted), 0, "{}", stderr(&interrupted));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "interrupt blocked behind the running cell: {:?}",
        started.elapsed()
    );

    for _ in 0..40 {
        std::thread::sleep(std::time::Duration::from_millis(500));
        if std::fs::read_to_string(&nb.notebook)
            .unwrap()
            .contains("KeyboardInterrupt")
        {
            stop(&nb);
            return;
        }
    }
    stop(&nb);
    panic!("the interrupt never reached the detached cell");
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn inspect_and_complete_reach_the_kernel() {
    let nb = with_kernel();
    assert_eq!(
        code(&nb.run(&["exec", nb.path(), "--code", "import json"])),
        0
    );

    let inspected = nb.run(&["inspect", nb.path(), "len"]);
    assert_eq!(code(&inspected), 0, "{}", stderr(&inspected));
    assert!(
        stdout(&inspected).contains("Docstring"),
        "{}",
        stdout(&inspected)
    );
    // Terminal colour is noise for anything reading this.
    assert!(
        !stdout(&inspected).contains('\u{1b}'),
        "ANSI escapes leaked"
    );

    let completed = nb.run(&["complete", nb.path(), "json.du"]);
    assert_eq!(code(&completed), 0, "{}", stderr(&completed));
    assert!(
        stdout(&completed).contains("dumps"),
        "{}",
        stdout(&completed)
    );
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn figures_are_written_beside_the_notebook_when_a_cell_produces_one() {
    let nb = with_kernel();
    let result = nb.run(&[
        "exec",
        nb.path(),
        "--code",
        "from IPython.display import display, Image\n\
         import base64\n\
         png = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==')\n\
         display(Image(data=png, format='png'))",
    ]);
    assert_eq!(code(&result), 0, "{}", stderr(&result));
    assert!(stdout(&result).contains("[saved"), "{}", stdout(&result));

    let files = nb.dir.path().join("test_files");
    let written: Vec<_> = std::fs::read_dir(&files)
        .unwrap_or_else(|e| panic!("no attachment directory at {}: {e}", files.display()))
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(written.len(), 1, "{written:?}");
    let bytes = std::fs::read(written[0].path()).unwrap();
    assert_eq!(&bytes[..4], b"\x89PNG", "not a decoded PNG");
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn an_edit_reaches_the_running_session_rather_than_the_file() {
    // The supervisor holds the notebook in memory for its whole lifetime and
    // rewrites the file after every cell, so an edit written straight to disk
    // is silently reverted by the next run.
    let nb = with_kernel();
    let first = nb.run(&["exec", nb.path(), "--code", "a = 1"]);
    assert_eq!(code(&first), 0, "{}", stderr(&first));

    let edit = nb.run(&["edit", nb.path(), "insert", "--code", "# keep me"]);
    assert_eq!(code(&edit), 0, "{}", stderr(&edit));

    // The next cell goes through the supervisor, which saves its own copy.
    let second = nb.run(&["exec", nb.path(), "--code", "a + 1"]);
    assert_eq!(code(&second), 0, "{}", stderr(&second));

    let cells = nb.run(&["cells", nb.path()]);
    assert!(
        stdout(&cells).contains("# keep me"),
        "the edit was clobbered by the session's copy: {}",
        stdout(&cells)
    );
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn status_and_interrupt_answer_while_an_attached_cell_runs() {
    // Serving connections one at a time made both of these queue behind the
    // cell they are meant to report on and stop, which is precisely when they
    // are worth having.
    let nb = with_kernel();
    let _ = nb.run(&["kernel", "start", nb.path()]);

    let mut cell = nb.spawn(&[
        "exec",
        nb.path(),
        "--code",
        "import time; time.sleep(60)",
        "--timeout",
        "0",
    ]);
    std::thread::sleep(Duration::from_secs(2));

    let status = nb
        .run_within(&["kernel", "status", nb.path()], Duration::from_secs(10))
        .expect("status hung behind the running cell");
    assert_eq!(code(&status), 0, "{}", stderr(&status));

    let interrupt = nb
        .run_within(&["kernel", "interrupt", nb.path()], Duration::from_secs(10))
        .expect("interrupt hung behind the running cell");
    assert_eq!(code(&interrupt), 0, "{}", stderr(&interrupt));

    // And the interrupt actually reached the cell.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut finished = false;
    while Instant::now() < deadline {
        if cell.try_wait().expect("try_wait failed").is_some() {
            finished = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if !finished {
        let _ = cell.kill();
    }
    let _ = cell.wait();
    assert!(finished, "the interrupt never stopped the cell");
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn stopping_a_session_does_not_wait_for_an_endless_cell() {
    // `kernel stop` used to need the session lock that the endless cell holds,
    // so the one command that could rescue the situation could never run.
    let nb = with_kernel();
    let queued = nb.run(&[
        "exec",
        nb.path(),
        "--code",
        "while True: pass",
        "--detach",
        "--timeout",
        "0",
    ]);
    assert_eq!(code(&queued), 0, "{}", stderr(&queued));
    std::thread::sleep(Duration::from_secs(2));

    let stopped = nb
        .run_within(&["kernel", "stop", nb.path()], Duration::from_secs(30))
        .expect("stop hung behind the endless cell");
    assert_eq!(code(&stopped), 0, "{}", stderr(&stopped));

    // And the supervisor really went away rather than merely answering.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut gone = false;
    while Instant::now() < deadline {
        let status = nb.run(&["kernel", "status", nb.path()]);
        if stdout(&status).contains("no session running") {
            gone = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(gone, "the supervisor answered the stop but never exited");
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn a_kernel_that_dies_mid_cell_is_reported_dead_rather_than_timed_out() {
    // Nothing watched the child process, so a kernel that vanished mid-cell
    // was only noticed when the timeout expired, and was then reported as a
    // retryable timeout: advice that sends an agent back to a corpse.
    let nb = with_kernel();
    let output = nb
        .run_within(
            &[
                "exec",
                nb.path(),
                "--code",
                "import os; os._exit(1)",
                "--timeout",
                "60",
            ],
            Duration::from_secs(30),
        )
        .expect("a dead kernel was never noticed");
    assert!(
        stderr(&output).contains("kernel_died"),
        "expected kernel_died, got: {}",
        stderr(&output)
    );
    stop(&nb);
}

#[test]
#[ignore = "requires an installed Jupyter kernel"]
fn interrupt_still_works_after_a_restart() {
    // Restarting replaced the kernel wholesale, including the interrupt handle
    // the session and the UI hold, so every later interrupt signalled a handle
    // nothing was listening to.
    let nb = with_kernel();
    let started = nb.run(&["kernel", "start", nb.path()]);
    assert_eq!(code(&started), 0, "{}", stderr(&started));
    let restarted = nb.run(&["kernel", "restart", nb.path()]);
    assert_eq!(code(&restarted), 0, "{}", stderr(&restarted));

    let mut cell = nb.spawn(&[
        "exec",
        nb.path(),
        "--code",
        "import time; time.sleep(60)",
        "--timeout",
        "0",
    ]);
    std::thread::sleep(Duration::from_secs(2));

    let interrupt = nb
        .run_within(&["kernel", "interrupt", nb.path()], Duration::from_secs(10))
        .expect("interrupt hung after a restart");
    assert_eq!(code(&interrupt), 0, "{}", stderr(&interrupt));

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut finished = false;
    while Instant::now() < deadline {
        if cell.try_wait().expect("try_wait failed").is_some() {
            finished = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if !finished {
        let _ = cell.kill();
    }
    let _ = cell.wait();
    assert!(
        finished,
        "the interrupt was lost: restarting orphaned the handle"
    );
    stop(&nb);
}
