//! Integration tests against a real Jupyter kernel.
//!
//! Ignored by default so `cargo test` passes on a machine with no kernel
//! installed. Run them with a kernel available:
//!
//! ```bash
//! uv venv .venv && uv pip install --python .venv/bin/python ipykernel
//! .venv/bin/python -m ipykernel install --prefix .venv --name h5i-test
//! H5I_TEST_KERNEL_JSON=.venv/share/jupyter/kernels/h5i-test/kernel.json \
//!     cargo test -p h5i-db-notebook --test kernel -- --ignored --test-threads=1
//! ```
//!
//! `--test-threads=1` is not incidental: each test starts a Python
//! interpreter, and running the suite concurrently is what makes a small
//! machine start swapping.

use std::time::Duration;

use h5i_db_notebook::document::{Output, StreamName};
use h5i_db_notebook::kernel::jupyter::StartOptions;
use h5i_db_notebook::kernel::{
    Completeness, ExecOptions, ExecStatus, JupyterKernel, Kernel, KernelStatus, NullSink,
    OutputEvent, spec,
};

/// The kernelspec under test: the pinned one when the environment names it,
/// otherwise whatever `python3` resolves to on this machine.
fn test_spec() -> spec::KernelSpecInfo {
    match std::env::var("H5I_TEST_KERNEL_JSON") {
        Ok(path) => spec::load(&path).unwrap_or_else(|e| panic!("H5I_TEST_KERNEL_JSON: {e}")),
        Err(_) => spec::find("python3")
            .expect("no python3 kernel found; set H5I_TEST_KERNEL_JSON or install ipykernel"),
    }
}

async fn start() -> JupyterKernel {
    JupyterKernel::start(test_spec(), StartOptions::default())
        .await
        .expect("kernel failed to start")
}

fn opts(secs: u64) -> ExecOptions {
    ExecOptions {
        timeout: Some(Duration::from_secs(secs)),
        ..Default::default()
    }
}

/// Run code, returning the outcome and discarding the streamed events.
async fn run(kernel: &mut JupyterKernel, code: &str) -> h5i_db_notebook::kernel::ExecOutcome {
    kernel
        .execute(code, &opts(30), &mut NullSink)
        .await
        .unwrap_or_else(|e| panic!("execute({code:?}) failed: {e}"))
}

fn text_result(outcome: &h5i_db_notebook::kernel::ExecOutcome) -> Option<String> {
    outcome.outputs.iter().find_map(|o| match o {
        Output::ExecuteResult(r) => r.data.text_plain().map(str::to_string),
        _ => None,
    })
}

fn stdout(outcome: &h5i_db_notebook::kernel::ExecOutcome) -> String {
    outcome
        .outputs
        .iter()
        .filter_map(|o| match o {
            Output::Stream(s) if s.name == StreamName::Stdout => Some(s.text.as_str()),
            _ => None,
        })
        .collect()
}

fn pid_alive(pid: u32) -> bool {
    // SAFETY: signal 0 performs the permission and existence check only.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn starts_idle_and_reports_kernel_info() {
    let kernel = start().await;
    assert_eq!(kernel.status(), KernelStatus::Idle);
    let info = kernel.kernel_info.as_ref().expect("no kernel_info_reply");
    assert_eq!(info.language_info.name, "python");
    assert!(!info.protocol_version.is_empty());
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn evaluates_an_expression_into_an_execute_result() {
    let mut kernel = start().await;
    let outcome = run(&mut kernel, "1 + 1").await;
    assert_eq!(outcome.status, ExecStatus::Ok);
    assert_eq!(text_result(&outcome).as_deref(), Some("2"));
    assert_eq!(outcome.execution_count, Some(1));
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn state_persists_between_executions() {
    // The entire premise of the crate: without this an agent may as well run
    // `python script.py`.
    let mut kernel = start().await;
    run(&mut kernel, "import math\nx = 41").await;
    let outcome = run(&mut kernel, "x + math.floor(1.5)").await;
    assert_eq!(text_result(&outcome).as_deref(), Some("42"));
    assert_eq!(
        outcome.execution_count,
        Some(2),
        "execution count did not advance"
    );
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn consecutive_prints_arrive_as_one_coalesced_stream() {
    let mut kernel = start().await;
    let outcome = run(&mut kernel, "for i in range(3):\n    print(i)").await;
    let streams = outcome
        .outputs
        .iter()
        .filter(|o| matches!(o, Output::Stream(_)))
        .count();
    assert_eq!(streams, 1, "expected one merged stream, got {streams}");
    assert_eq!(stdout(&outcome), "0\n1\n2\n");
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn stdout_and_stderr_stay_separate_and_ordered() {
    let mut kernel = start().await;
    let outcome = run(
        &mut kernel,
        "import sys\n\
         print('out1'); sys.stdout.flush()\n\
         print('err1', file=sys.stderr); sys.stderr.flush()\n\
         print('out2'); sys.stdout.flush()",
    )
    .await;
    let names: Vec<&str> = outcome
        .outputs
        .iter()
        .filter_map(|o| match o {
            Output::Stream(s) => Some(s.name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        names,
        ["stdout", "stderr", "stdout"],
        "stream interleaving was lost: {:?}",
        outcome.outputs
    );
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn an_exception_becomes_an_error_output_with_a_full_traceback() {
    let mut kernel = start().await;
    let outcome = run(&mut kernel, "def inner():\n    1 / 0\ninner()").await;
    assert_eq!(outcome.status, ExecStatus::Error);
    let error = outcome.error().expect("no error output");
    assert_eq!(error.ename, "ZeroDivisionError");
    assert!(
        error.traceback.len() > 1,
        "traceback was truncated to {:?}; agents need every frame",
        error.traceback
    );
    // State survives a raised exception: that is what makes cell-level
    // recovery better than re-running a script.
    let after = run(&mut kernel, "'still alive'").await;
    assert_eq!(after.status, ExecStatus::Ok);
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn display_data_can_be_updated_in_place() {
    let mut kernel = start().await;
    let outcome = run(
        &mut kernel,
        "from IPython.display import display, update_display\n\
         display('first', display_id='d1')\n\
         update_display('second', display_id='d1')",
    )
    .await;
    let displays: Vec<&str> = outcome
        .outputs
        .iter()
        .filter_map(|o| match o {
            Output::DisplayData(d) => d.data.text_plain(),
            _ => None,
        })
        .collect();
    assert_eq!(displays.len(), 1, "update appended instead of replacing");
    assert!(displays[0].contains("second"), "got {:?}", displays[0]);
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn clear_output_removes_earlier_output() {
    let mut kernel = start().await;
    let outcome = run(
        &mut kernel,
        "from IPython.display import clear_output\n\
         print('before')\n\
         import sys; sys.stdout.flush()\n\
         clear_output(wait=False)\n\
         print('after')",
    )
    .await;
    let text = stdout(&outcome);
    assert!(
        !text.contains("before"),
        "clear_output did not clear: {text:?}"
    );
    assert!(text.contains("after"), "got {text:?}");
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn events_stream_while_the_cell_is_still_running() {
    let mut kernel = start().await;
    let mut seen_before_finish = 0usize;
    let mut sink = |event: OutputEvent| {
        if matches!(event, OutputEvent::Output(_)) {
            seen_before_finish += 1;
        }
    };
    kernel
        .execute(
            "import sys, time\n\
             for i in range(3):\n    print(i); sys.stdout.flush(); time.sleep(0.05)",
            &opts(30),
            &mut sink,
        )
        .await
        .unwrap();
    assert!(
        seen_before_finish >= 3,
        "expected incremental output events, saw {seen_before_finish}"
    );
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn a_timeout_interrupts_the_cell_and_leaves_the_kernel_usable() {
    // The important half is the second assertion: a timeout that abandons a
    // busy kernel makes every later cell in the session hang.
    let mut kernel = start().await;
    let error = kernel
        .execute("while True:\n    pass", &opts(3), &mut NullSink)
        .await
        .expect_err("infinite loop should have timed out");
    assert_eq!(error.code(), "execute_timeout");
    assert!(error.retryable());

    let recovered = run(&mut kernel, "'recovered'").await;
    assert_eq!(recovered.status, ExecStatus::Ok);
    assert_eq!(kernel.status(), KernelStatus::Idle);
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn interrupt_stops_a_running_cell() {
    let mut kernel = start().await;
    let pid = kernel.pid();

    // `interrupt()` takes `&mut self`, so it cannot be called while `execute`
    // holds the borrow. A timer sending the same signal to the same process
    // group exercises the same path a TUI keypress would.
    let interrupter = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(750)).await;
        // SAFETY: signalling the group of a process we started.
        unsafe { libc::kill(-(pid as i32), libc::SIGINT) };
    });

    let started = std::time::Instant::now();
    let mut sink = NullSink;
    let outcome = kernel
        .execute("import time\ntime.sleep(60)", &opts(120), &mut sink)
        .await
        .expect("execute returned an error");
    interrupter.await.unwrap();

    assert!(
        started.elapsed() < Duration::from_secs(30),
        "cell was not interrupted; it ran for {:?}",
        started.elapsed()
    );
    assert_eq!(outcome.status, ExecStatus::Error);
    assert_eq!(
        outcome.error().map(|e| e.ename.as_str()),
        Some("KeyboardInterrupt")
    );

    // Interrupt preserves state, unlike restart.
    let after = run(&mut kernel, "'alive'").await;
    assert_eq!(after.status, ExecStatus::Ok);
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn restart_clears_state_and_keeps_the_handle_usable() {
    let mut kernel = start().await;
    run(&mut kernel, "sentinel = 'before restart'").await;
    let old_pid = kernel.pid();

    kernel.restart().await.expect("restart failed");
    assert_eq!(kernel.status(), KernelStatus::Idle);
    assert_ne!(kernel.pid(), old_pid, "restart reused the old process");

    let outcome = run(&mut kernel, "sentinel").await;
    assert_eq!(
        outcome.status,
        ExecStatus::Error,
        "state survived a restart, so it was not a real restart"
    );
    assert_eq!(outcome.error().map(|e| e.ename.as_str()), Some("NameError"));

    // And the old process is gone, not orphaned.
    for _ in 0..50 {
        if !pid_alive(old_pid) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !pid_alive(old_pid),
        "the pre-restart kernel is still running"
    );
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn shutdown_reaps_the_process() {
    let mut kernel = start().await;
    let pid = kernel.pid();
    assert!(pid_alive(pid));
    kernel.shutdown().await.expect("shutdown failed");
    for _ in 0..50 {
        if !pid_alive(pid) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!pid_alive(pid), "kernel survived shutdown");
    assert_eq!(kernel.status(), KernelStatus::Dead);
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn dropping_the_kernel_does_not_leak_the_process() {
    let pid = {
        let kernel = start().await;
        kernel.pid()
    };
    for _ in 0..50 {
        if !pid_alive(pid) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!pid_alive(pid), "dropping the kernel leaked process {pid}");
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn a_killed_kernel_is_reported_dead_rather_than_hanging() {
    let mut kernel = start().await;
    let pid = kernel.pid();
    // SAFETY: killing a process we started and own.
    unsafe { libc::kill(pid as i32, libc::SIGKILL) };

    for _ in 0..50 {
        if kernel.status() == KernelStatus::Dead {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(kernel.status(), KernelStatus::Dead);

    let error = kernel
        .execute("1", &opts(5), &mut NullSink)
        .await
        .expect_err("executing on a dead kernel should fail");
    assert_eq!(error.code(), "kernel_died");
    assert!(error.retryable(), "the session layer restarts on this");
    assert!(error.hint().is_some());
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn completion_returns_candidates_with_a_replacement_range() {
    let mut kernel = start().await;
    run(&mut kernel, "import json").await;
    let code = "json.dum";
    let completions = kernel.complete(code, code.len()).await.unwrap();
    assert!(
        completions.matches.iter().any(|m| m.ends_with("dumps")),
        "no dumps in {:?}",
        completions.matches
    );
    assert!(completions.cursor_end <= code.len());
    assert!(completions.cursor_start <= completions.cursor_end);
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn is_complete_distinguishes_a_finished_statement_from_an_open_block() {
    let mut kernel = start().await;
    assert_eq!(
        kernel.is_complete("1 + 1").await.unwrap(),
        Completeness::Complete
    );
    match kernel.is_complete("def f():").await.unwrap() {
        Completeness::Incomplete { .. } => {}
        other => panic!("expected Incomplete for an open block, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn inspect_finds_documentation_and_reports_misses() {
    let mut kernel = start().await;
    let found = kernel.inspect("len", 3, 0).await.unwrap();
    let bundle = found.expect("len should be introspectable");
    assert!(bundle.text_plain().is_some_and(|t| !t.is_empty()));

    let missing = kernel.inspect("zzz_not_a_name", 14, 0).await.unwrap();
    assert!(missing.is_none(), "expected no result for an unknown name");
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn reading_stdin_raises_by_default_rather_than_hanging() {
    // The default is `allow_stdin: false`, which makes the kernel raise
    // immediately. That is the honest outcome when nobody is at a keyboard:
    // loud, fast, and it names the problem in the traceback.
    let mut kernel = start().await;
    let outcome = run(&mut kernel, "value = input('name? ')").await;
    assert_eq!(outcome.status, ExecStatus::Error);
    assert_eq!(
        outcome.error().map(|e| e.ename.as_str()),
        Some("StdinNotImplementedError")
    );
    assert!(
        outcome.elapsed < Duration::from_secs(10),
        "{:?}",
        outcome.elapsed
    );
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn stdin_with_no_provider_answers_empty_instead_of_blocking() {
    // When a caller opts into stdin but supplies no way to answer, an
    // unanswered `input_request` would wedge the kernel until the timeout and
    // take the rest of the session with it. We reply empty so the cell
    // finishes; the value is documented to be empty, not real input.
    let mut kernel = start().await;
    let options = ExecOptions {
        timeout: Some(Duration::from_secs(15)),
        allow_stdin: true,
        ..Default::default()
    };
    let outcome = kernel
        .execute("value = input('name? ')\nvalue", &options, &mut NullSink)
        .await
        .expect("execute should return, not hang");
    assert_eq!(outcome.status, ExecStatus::Ok);
    assert_eq!(text_result(&outcome).as_deref(), Some("''"));
    assert!(
        outcome.elapsed < Duration::from_secs(10),
        "took {:?}, so it waited on the timeout instead of answering",
        outcome.elapsed
    );
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn rich_output_carries_every_mime_type_the_kernel_offered() {
    let mut kernel = start().await;
    let outcome = run(
        &mut kernel,
        "class Rich:\n\
         \x20   def _repr_html_(self): return '<b>rich</b>'\n\
         \x20   def __repr__(self): return 'plain rich'\n\
         Rich()",
    )
    .await;
    let result = outcome
        .outputs
        .iter()
        .find_map(|o| match o {
            Output::ExecuteResult(r) => Some(&r.data),
            _ => None,
        })
        .expect("no execute_result");
    assert_eq!(result.text_plain(), Some("plain rich"));
    assert_eq!(result.text_of("text/html"), Some("<b>rich</b>"));
}

#[tokio::test]
#[ignore = "requires an installed Jupyter kernel"]
async fn the_connection_file_is_written_where_jupyter_looks_for_it() {
    let kernel = start().await;
    let path = kernel.connection_file().to_path_buf();
    assert!(path.exists(), "no connection file at {}", path.display());
    let body = std::fs::read_to_string(&path).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed["signature_scheme"], "hmac-sha256");
    assert!(parsed["key"].as_str().is_some_and(|k| !k.is_empty()));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        // The file carries the HMAC key: readable by anyone means executable
        // by anyone.
        assert_eq!(mode & 0o077, 0, "connection file is group/world readable");
    }
}
