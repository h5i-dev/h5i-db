//! The supervisor process: owns one notebook's session, serves a unix socket.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, mpsc};

use crate::document::{Output, StreamName};
use crate::error::{Error, Result};
use crate::kernel::{Kernel, OutputEvent, OutputSink};
use crate::session::{CellResult, Session};
use crate::supervisor::protocol::{CellReport, Request, Response, SessionInfo, StreamEvent};

/// How long a supervisor sticks around with no client attached.
///
/// Long enough that an agent's think-time between cells never costs a kernel
/// restart, short enough that abandoned sessions do not accumulate.
pub const DEFAULT_IDLE_TTL: Duration = Duration::from_secs(3600);

/// How long to wait for a client's request line before hanging up.
///
/// A client writes its request immediately after connecting, so anything
/// slower is a connection that will never speak: without a bound it would hold
/// a task (and, before per-connection tasks, the whole accept loop) forever.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to wait for a running cell to release the session while shutting
/// down, before interrupting it.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// How long an edit waits for a running cell before reporting the session busy.
const EDIT_LOCK_WAIT: Duration = Duration::from_secs(5);

/// How often to test whether the session has been idle past its TTL.
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// How long a starting supervisor waits for a departing one to release the
/// notebook. Longer than the shutdown drain, so the ordinary stop-then-start
/// sequence hands over rather than colliding.
const LOCK_HANDOVER_WAIT: Duration = Duration::from_secs(15);

pub struct ServerOptions {
    pub socket: PathBuf,
    pub idle_ttl: Duration,
}

struct State {
    session: Session,
    last_activity: Instant,
    busy: bool,
}

/// Run the supervisor until it is told to shut down or idles out.
pub async fn serve(notebook: &Path, options: ServerOptions) -> Result<()> {
    let socket = options.socket.clone();

    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent).map_err(|e| Error::io(parent.display(), e))?;
        // The socket grants code execution in this kernel; nobody else on the
        // host should be able to reach it.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }

    // Ownership is claimed before anything is touched, because everything that
    // follows is destructive to a supervisor that already owns this notebook:
    // two supervisors would each hold the file in memory and each rewrite it
    // whole, so the second to save silently erases the first one's cells.
    // Held for the whole process lifetime, and released by the kernel on exit
    // however we die.
    let lock_path = crate::supervisor::lock_path(notebook);
    let _ownership = match SupervisorLock::try_acquire(&lock_path)? {
        Some(held) => held,
        None => {
            let already_running = || Error::SessionAlreadyRunning {
                path: notebook.display().to_string(),
            };
            // Somebody holds it. Which of two very different situations that
            // is shows in whether they are actually serving: a live socket
            // means this is a duplicate start, and saying so at once beats
            // waiting; no socket means the holder is on its way out and we are
            // its replacement, which is the ordinary `kernel stop` followed by
            // a fresh `exec`. Refusing that one would strand the client, which
            // is waiting for a socket that nobody is left to bind.
            if UnixStream::connect(&socket).await.is_ok() {
                return Err(already_running());
            }
            match SupervisorLock::acquire_within(&lock_path, LOCK_HANDOVER_WAIT).await? {
                Some(held) => held,
                None => return Err(already_running()),
            }
        }
    };

    let session = Session::open(notebook)?;

    // Safe now: holding the lock means no other supervisor is serving this
    // notebook, so whatever is at the socket path is a leftover from one that
    // died without cleaning up.
    let _ = std::fs::remove_file(&socket);

    let listener = UnixListener::bind(&socket).map_err(|e| Error::io(socket.display(), e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600));
    }

    // Taken before the session moves into the mutex: interrupting must never
    // need the lock that the cell being interrupted is holding.
    let interrupt = session.interrupt_handle();
    let notebook = notebook.to_path_buf();
    let state = Arc::new(Mutex::new(State {
        session,
        last_activity: Instant::now(),
        busy: false,
    }));
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

    // A ticker rather than a fresh sleep per iteration: a sleep created inside
    // the `select!` restarts on every connection, so a client polling more
    // often than the interval would keep the idle check from ever running and
    // an abandoned session would never exit.
    let mut idle_check = tokio::time::interval(IDLE_CHECK_INTERVAL);
    idle_check.tick().await;

    let result = loop {
        tokio::select! {
            incoming = listener.accept() => {
                match incoming {
                    Ok((stream, _)) => {
                        // One task per connection. Serving them inline would
                        // mean the lock-free Status and Interrupt paths queue
                        // behind whatever request is already being served, so
                        // a client could neither see nor stop a running cell:
                        // the two things worth doing while one runs. The
                        // session mutex, not the accept loop, is what keeps
                        // execution serialized.
                        tokio::spawn(handle_connection(
                            stream,
                            state.clone(),
                            shutdown_tx.clone(),
                            interrupt.clone(),
                            notebook.clone(),
                        ));
                    }
                    Err(error) => break Err(Error::io(socket.display(), error)),
                }
            }
            _ = shutdown_rx.recv() => break Ok(()),
            _ = idle_check.tick() => {
                // `try_lock`, never `lock`: a detached cell holds the session
                // for as long as it runs, and blocking here would stall the
                // accept loop, so status polling would hang exactly while
                // there is something worth polling for. A held lock also *is*
                // the busy signal, so failing to take it means not idle.
                let Ok(guard) = state.try_lock() else { continue };
                if !guard.busy && guard.last_activity.elapsed() > options.idle_ttl {
                    tracing::info!("idle for {:?}, exiting", options.idle_ttl);
                    break Ok(());
                }
            }
        }
    };

    // Unlink before draining. Shutting the kernel down takes a moment, and a
    // client that connects to a listener we are about to drop would get a
    // reset peer instead of simply starting a fresh supervisor.
    let _ = std::fs::remove_file(&socket);
    drop(listener);

    // A cell may still hold the session: a detached run, or one whose client
    // walked away. Wait briefly, then interrupt it. Waiting unconditionally
    // would let a single `while True` cell keep the supervisor (and its
    // kernel) alive forever after it was told to stop.
    let guard = match tokio::time::timeout(SHUTDOWN_GRACE, state.lock()).await {
        Ok(guard) => Some(guard),
        Err(_) => {
            interrupt.interrupt();
            tokio::time::timeout(SHUTDOWN_GRACE, state.lock())
                .await
                .ok()
        }
    };
    match guard {
        Some(mut guard) => {
            let _ = guard.session.shutdown().await;
            let _ = guard.session.save();
        }
        None => {
            // The cell will not yield the session, so its outputs cannot be
            // saved from here. Exiting anyway is still right: the kernel is a
            // child process group that dies with us, and every cell that did
            // finish was saved as it finished.
            tracing::warn!("shutting down with a cell still running; its outputs are lost");
        }
    }
    result
}

/// Exclusive claim on one notebook's supervisor role.
///
/// An advisory `flock` rather than a pid file: the kernel drops it when the
/// process dies however it dies, so a supervisor that is SIGKILLed or OOM-
/// killed leaves nothing stale behind. A pid file would need a liveness check,
/// and checking a recycled pid is exactly the mistake that lets one supervisor
/// declare another one dead.
struct SupervisorLock {
    #[allow(dead_code)]
    file: std::fs::File,
}

impl SupervisorLock {
    /// Claim the lock, waiting up to `limit` for the current holder to exit.
    ///
    /// Polled rather than blocking on `flock`, so the wait has a bound: a
    /// holder that is wedged rather than exiting must not turn every later
    /// start into a hang.
    async fn acquire_within(path: &Path, limit: Duration) -> Result<Option<Self>> {
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            if let Some(held) = Self::try_acquire(path)? {
                return Ok(Some(held));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Claim the lock, or return `None` if another process holds it.
    fn try_acquire(path: &Path) -> Result<Option<Self>> {
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::io::AsRawFd;

        // The lock lives in the session directory, which an explicit
        // `--socket` elsewhere would not have created.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| Error::io(parent.display(), e))?;
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(path)
            .map_err(|e| Error::io(path.display(), e))?;

        // SAFETY: `flock` on a descriptor we own. LOCK_NB makes it answer
        // rather than wait, which is what turns "somebody else is serving
        // this notebook" into a decision instead of a hang.
        let taken = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if taken != 0 {
            let error = std::io::Error::last_os_error();
            return match error.kind() {
                std::io::ErrorKind::WouldBlock => Ok(None),
                _ => Err(Error::io(path.display(), error)),
            };
        }

        // Recorded for whoever has to work out which process is holding a
        // session; nothing reads it back, because the lock itself is the
        // authority on liveness.
        use std::io::Write;
        let mut file = file;
        let _ = file.set_len(0);
        let _ = writeln!(file, "{}", std::process::id());
        let _ = file.flush();
        Ok(Some(SupervisorLock { file }))
    }
}

async fn handle_connection(
    stream: UnixStream,
    state: Arc<Mutex<State>>,
    shutdown: mpsc::Sender<()>,
    interrupt: crate::kernel::InterruptHandle,
    notebook: PathBuf,
) {
    let (read_half, write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    // All writes funnel through one task. `OutputSink::emit` is synchronous
    // and cannot await, so it hands frames to this channel instead: sending on
    // an unbounded channel never blocks, which keeps a slow or vanished client
    // from stalling the kernel.
    let (frames, mut rx) = mpsc::unbounded_channel::<Response>();
    let writer = tokio::spawn(async move {
        let mut out = write_half;
        while let Some(response) = rx.recv().await {
            let mut line = match serde_json::to_vec(&response) {
                Ok(line) => line,
                Err(_) => continue,
            };
            line.push(b'\n');
            // A disconnected client must not abort the cell: the notebook is
            // the authoritative record and the supervisor still has to finish
            // writing it.
            if out.write_all(&line).await.is_err() {
                break;
            }
        }
        let _ = out.flush().await;
        let _ = out.shutdown().await;
    });

    // A connection that never sends a request is a leak, not a client.
    match tokio::time::timeout(REQUEST_READ_TIMEOUT, lines.next_line()).await {
        Ok(Ok(Some(line))) => {
            let response = match serde_json::from_str::<Request>(&line) {
                Ok(request) => {
                    dispatch(request, &state, &frames, &shutdown, &interrupt, &notebook).await
                }
                Err(error) => {
                    Response::from_error(&Error::invalid(format!("malformed request: {error}")))
                }
            };
            let _ = frames.send(response);
        }
        Err(_) => {
            let _ = frames.send(Response::from_error(&Error::invalid(format!(
                "no request within {}s of connecting",
                REQUEST_READ_TIMEOUT.as_secs()
            ))));
        }
        // A client that hung up or failed mid-line has nothing to be told.
        Ok(_) => {}
    }

    drop(frames);
    let _ = writer.await;
}

async fn dispatch(
    request: Request,
    state: &Arc<Mutex<State>>,
    frames: &mpsc::UnboundedSender<Response>,
    shutdown: &mpsc::Sender<()>,
    interrupt: &crate::kernel::InterruptHandle,
    notebook: &Path,
) -> Response {
    // Status must answer while a cell is running, so it never takes the
    // session lock: a status call that blocks behind a twenty-minute cell is
    // useless precisely when it is most needed.
    if matches!(request, Request::Status) {
        return match state.try_lock() {
            Ok(guard) => Response::Status(describe(&guard, false)),
            Err(_) => {
                // A held lock means a cell is running, so answer from the file
                // rather than waiting for it. A status call that blocks behind
                // a twenty-minute cell is useless exactly when it is needed,
                // and one that answers with blanks is barely better.
                let document = crate::Notebook::read(notebook).ok();
                Response::Status(SessionInfo {
                    notebook: notebook.display().to_string(),
                    kernel_name: document
                        .as_ref()
                        .and_then(|d| d.kernel_name())
                        .unwrap_or("unknown")
                        .to_string(),
                    kernel_status: crate::kernel::KernelStatus::Busy,
                    cells: document.as_ref().map(|d| d.len()).unwrap_or(0),
                    pid: std::process::id(),
                    busy: true,
                    idle_seconds: 0,
                })
            }
        };
    }

    // Interrupt never takes the lock: the cell it is meant to stop is what
    // holds it. The handle was made for exactly this.
    if matches!(request, Request::Interrupt) {
        interrupt.interrupt();
        return Response::Ok;
    }

    // Nor does shutdown. "Stop this session" has to work while a cell is
    // running, or an unattended cell with no timeout could never be stopped at
    // all; the drain in `serve` interrupts whatever is still holding it.
    if matches!(request, Request::Shutdown) {
        let _ = shutdown.send(()).await;
        return Response::Ok;
    }

    // A detached run keeps the session for as long as the cell takes. Rather
    // than let a second `exec` block until it finishes, which reads as a hang,
    // say so: `session_busy` is retryable and carries exit code 3.
    let mut guard = match state.try_lock() {
        Ok(guard) => guard,
        Err(_) if matches!(request, Request::Exec { .. } | Request::Run { .. }) => {
            return Response::from_error(&Error::SessionBusy {
                path: notebook.display().to_string(),
            });
        }
        // An edit has to reach the session that owns the notebook, so it waits
        // for the running cell rather than being refused outright. Bounded,
        // because a cell with no timeout would otherwise turn `nb edit` into a
        // hang with nothing to show for it.
        Err(_) if matches!(request, Request::Edit { .. }) => {
            match tokio::time::timeout(EDIT_LOCK_WAIT, state.lock()).await {
                Ok(guard) => guard,
                Err(_) => {
                    return Response::from_error(&Error::SessionBusy {
                        path: notebook.display().to_string(),
                    });
                }
            }
        }
        Err(_) => state.lock().await,
    };
    guard.last_activity = Instant::now();
    guard.busy = true;

    // Detached execution returns as soon as the cell exists. The supervisor
    // owns the notebook, so the outputs are still recorded after the client
    // that asked for it has gone.
    if let Request::Exec {
        code,
        timeout_secs,
        detach: true,
    } = &request
    {
        apply_timeout(&mut guard.session, *timeout_secs);
        let index = match guard.session.append(code) {
            Ok(index) => index,
            Err(error) => {
                guard.busy = false;
                return Response::from_error(&error);
            }
        };
        let cell_id = guard
            .session
            .notebook()
            .get(index)
            .ok()
            .and_then(|c| c.id())
            .map(str::to_string);
        drop(guard);

        let background = state.clone();
        tokio::spawn(async move {
            let mut guard = background.lock().await;
            guard.busy = true;
            let mut sink = crate::kernel::NullSink;
            if let Err(error) = guard.session.run_cell(index, &mut sink).await {
                // Nobody is waiting on this call, so an error that only lives
                // in its return value is an error nobody can ever see: the
                // cell would sit there with no outputs, indistinguishable
                // from one still queued.
                tracing::warn!("detached cell {index} failed: {error}");
                guard.session.record_cell_error(index, &error);
            }
            guard.busy = false;
            guard.last_activity = Instant::now();
        });
        return Response::Detached { index, cell_id };
    }

    let response = run_request(request, &mut guard, frames, shutdown).await;

    guard.busy = false;
    guard.last_activity = Instant::now();
    response
}

async fn run_request(
    request: Request,
    state: &mut State,
    frames: &mpsc::UnboundedSender<Response>,
    shutdown: &mpsc::Sender<()>,
) -> Response {
    match request {
        // Handled before the lock is taken.
        Request::Status => Response::Status(describe(state, false)),

        Request::Exec {
            code,
            timeout_secs,
            detach,
        } => {
            // A detached request is handled before the lock is released in
            // `dispatch`; reaching here means it is a normal one.
            debug_assert!(!detach);
            apply_timeout(&mut state.session, timeout_secs);
            let mut sink = StreamSink(frames.clone());
            match state.session.exec(&code, &mut sink).await {
                Ok(result) => Response::Result {
                    cells: vec![report(result)],
                },
                Err(error) => Response::from_error(&error),
            }
        }

        Request::Run {
            indices,
            timeout_secs,
            stop_on_error,
        } => {
            apply_timeout(&mut state.session, timeout_secs);
            let mut sink = StreamSink(frames.clone());
            match state
                .session
                .run_cells(&indices, stop_on_error, &mut sink)
                .await
            {
                Ok(results) => Response::Result {
                    cells: results.into_iter().map(report).collect(),
                },
                Err(error) => Response::from_error(&error),
            }
        }

        Request::Restart { clear_outputs } => match state.session.restart(clear_outputs).await {
            Ok(()) => Response::Ok,
            Err(error) => Response::from_error(&error),
        },

        // Handled in `dispatch`, before the lock is taken.
        Request::Interrupt => Response::Ok,

        // Handled in `dispatch`, before the lock is taken.
        Request::Shutdown => {
            let _ = shutdown.send(()).await;
            Response::Ok
        }

        Request::Edit { action } => match state.session.apply_edit(&action) {
            Ok(message) => Response::Edited { message },
            Err(error) => Response::from_error(&error),
        },

        Request::Complete { code, cursor } => match kernel(state).await {
            Ok(kernel) => match kernel.complete(&code, cursor).await {
                Ok(completions) => Response::Completions {
                    matches: completions.matches,
                    cursor_start: completions.cursor_start,
                    cursor_end: completions.cursor_end,
                },
                Err(error) => Response::from_error(&error),
            },
            Err(error) => Response::from_error(&error),
        },

        Request::Inspect {
            code,
            cursor,
            detail,
        } => match kernel(state).await {
            Ok(kernel) => match kernel.inspect(&code, cursor, detail).await {
                Ok(Some(bundle)) => Response::Inspect {
                    found: true,
                    text: bundle.text_plain().map(str::to_string),
                },
                Ok(None) => Response::Inspect {
                    found: false,
                    text: None,
                },
                Err(error) => Response::from_error(&error),
            },
            Err(error) => Response::from_error(&error),
        },
    }
}

async fn kernel(state: &mut State) -> Result<&mut crate::kernel::JupyterKernel> {
    state.session.ensure_kernel().await?;
    state.session.kernel_mut()
}

fn apply_timeout(session: &mut Session, timeout_secs: Option<u64>) {
    if let Some(secs) = timeout_secs {
        // Zero means "no limit", matching the CLI flag.
        session.exec_options_mut().timeout = (secs > 0).then(|| Duration::from_secs(secs));
    }
}

fn describe(state: &State, busy_override: bool) -> SessionInfo {
    SessionInfo {
        notebook: state.session.path().display().to_string(),
        kernel_name: state.session.kernel_name(),
        kernel_status: state.session.kernel_status(),
        cells: state.session.notebook().len(),
        pid: std::process::id(),
        busy: state.busy || busy_override,
        idle_seconds: state.last_activity.elapsed().as_secs(),
    }
}

fn report(result: CellResult) -> CellReport {
    CellReport {
        index: result.index,
        cell_id: result.cell_id,
        status: result.status,
        execution_count: result.execution_count,
        elapsed_ms: result.elapsed.as_millis() as u64,
        outputs: result.outputs,
    }
}

/// Forwards output events to the client as they happen.
struct StreamSink(mpsc::UnboundedSender<Response>);

impl OutputSink for StreamSink {
    fn emit(&mut self, event: OutputEvent) {
        let response = match event {
            OutputEvent::Output(Output::Stream(stream)) => Response::Event(match stream.name {
                StreamName::Stdout => StreamEvent::Stdout { text: stream.text },
                StreamName::Stderr => StreamEvent::Stderr { text: stream.text },
            }),
            OutputEvent::Output(other) => Response::Event(StreamEvent::Output {
                mime_types: other
                    .data()
                    .map(|d| d.mime_types().map(str::to_string).collect())
                    .unwrap_or_else(|| vec![other.output_type().to_string()]),
            }),
            OutputEvent::ExecutionCount(count) => {
                Response::Event(StreamEvent::ExecutionCount { count })
            }
            OutputEvent::Status(status) => Response::Event(StreamEvent::KernelStatus { status }),
            _ => return,
        };
        // Unbounded send never blocks, and a dropped receiver just means the
        // client went away.
        let _ = self.0.send(response);
    }
}
