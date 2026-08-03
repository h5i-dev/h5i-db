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
    let session = Session::open(notebook)?;
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
    // A socket left by a crashed supervisor would make bind fail. The client
    // only starts us after failing to connect, so a leftover is stale.
    let _ = std::fs::remove_file(&socket);

    let listener = UnixListener::bind(&socket).map_err(|e| Error::io(socket.display(), e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600));
    }

    let state = Arc::new(Mutex::new(State {
        session,
        last_activity: Instant::now(),
        busy: false,
    }));
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

    let result = loop {
        tokio::select! {
            incoming = listener.accept() => {
                match incoming {
                    Ok((stream, _)) => {
                        // Handled one at a time: the session owns a single
                        // kernel, so concurrency here would only buy lock
                        // contention and interleaved output.
                        handle_connection(stream, state.clone(), shutdown_tx.clone()).await;
                    }
                    Err(error) => break Err(Error::io(socket.display(), error)),
                }
            }
            _ = shutdown_rx.recv() => break Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                let guard = state.lock().await;
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

    let mut guard = state.lock().await;
    let _ = guard.session.shutdown().await;
    let _ = guard.session.save();
    drop(guard);
    result
}

async fn handle_connection(
    stream: UnixStream,
    state: Arc<Mutex<State>>,
    shutdown: mpsc::Sender<()>,
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

    if let Ok(Some(line)) = lines.next_line().await {
        let response = match serde_json::from_str::<Request>(&line) {
            Ok(request) => dispatch(request, &state, &frames, &shutdown).await,
            Err(error) => {
                Response::from_error(&Error::invalid(format!("malformed request: {error}")))
            }
        };
        let _ = frames.send(response);
    }

    drop(frames);
    let _ = writer.await;
}

async fn dispatch(
    request: Request,
    state: &Arc<Mutex<State>>,
    frames: &mpsc::UnboundedSender<Response>,
    shutdown: &mpsc::Sender<()>,
) -> Response {
    // Status must answer while a cell is running, so it never takes the
    // session lock: a status call that blocks behind a twenty-minute cell is
    // useless precisely when it is most needed.
    if matches!(request, Request::Status) {
        return match state.try_lock() {
            Ok(guard) => Response::Status(describe(&guard, false)),
            Err(_) => {
                // Locked means a cell is running. Report that rather than wait.
                Response::Status(SessionInfo {
                    notebook: String::new(),
                    kernel_name: String::new(),
                    kernel_status: crate::kernel::KernelStatus::Busy,
                    cells: 0,
                    pid: std::process::id(),
                    busy: true,
                    idle_seconds: 0,
                })
            }
        };
    }

    let mut guard = state.lock().await;
    guard.last_activity = Instant::now();
    guard.busy = true;

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
            if detach {
                // Refused rather than silently run synchronously, which would
                // look to the caller like a hang.
                return Response::from_error(&Error::invalid(
                    "detached execution is not implemented yet; run without --detach",
                ));
            }
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

        Request::Interrupt => match state.session.interrupt().await {
            Ok(()) => Response::Ok,
            Err(error) => Response::from_error(&error),
        },

        Request::Restart { clear_outputs } => match state.session.restart(clear_outputs).await {
            Ok(()) => Response::Ok,
            Err(error) => Response::from_error(&error),
        },

        Request::Shutdown => {
            let _ = shutdown.send(()).await;
            Response::Ok
        }

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
