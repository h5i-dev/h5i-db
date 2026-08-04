//! Connect to a notebook's supervisor, starting one if none is listening.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::error::{Error, Result};
use crate::supervisor::protocol::{Request, Response};
use crate::supervisor::socket_path;

/// How long to wait for a freshly spawned supervisor to start listening.
const SPAWN_TIMEOUT: Duration = Duration::from_secs(20);

/// Subcommands that reach `serve` in *this* binary.
///
/// The supervisor is started by re-executing ourselves, so the argument that
/// gets there depends on how the command tree was mounted: the standalone
/// `h5i-nb` takes `serve`, while `h5i-db` takes `nb serve`. Guessing from the
/// executable's name would break the moment somebody renames or symlinks it,
/// so each entry point states its own prefix instead.
static COMMAND_PREFIX: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();

/// Declare how this process reaches the notebook commands. Called once, by
/// whichever entry point owns `main`.
pub fn set_command_prefix<I, S>(prefix: I)
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let _ = COMMAND_PREFIX.set(prefix.into_iter().map(Into::into).collect());
}

pub fn command_prefix() -> &'static [String] {
    COMMAND_PREFIX.get().map(Vec::as_slice).unwrap_or(&[])
}

pub struct SessionClient {
    notebook: PathBuf,
    socket: PathBuf,
}

impl SessionClient {
    pub fn new(notebook: impl AsRef<Path>) -> Self {
        let notebook = notebook.as_ref().to_path_buf();
        let socket = socket_path(&notebook);
        SessionClient { notebook, socket }
    }

    /// A client for a socket whose notebook is not known yet.
    ///
    /// `ls` walks the session directory, where the notebook path is something
    /// the supervisor reports rather than something the caller has. Only
    /// requests that never start a supervisor are meaningful here, since
    /// starting one needs a notebook to open.
    pub fn for_socket(socket: impl AsRef<Path>) -> Self {
        let socket = socket.as_ref().to_path_buf();
        SessionClient {
            notebook: socket.clone(),
            socket,
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Whether a supervisor is currently listening.
    pub async fn is_running(&self) -> bool {
        UnixStream::connect(&self.socket).await.is_ok()
    }

    /// Send a request, starting a supervisor first if needed.
    ///
    /// `on_event` sees streamed frames as they arrive; the terminal frame is
    /// returned. Any `Error` frame is converted back into a real error so the
    /// CLI's exit-code and hint handling is identical whether the failure
    /// happened locally or in the supervisor.
    pub async fn request(
        &self,
        request: &Request,
        on_event: impl FnMut(&Response),
    ) -> Result<Response> {
        self.send(request, on_event, StartPolicy::Start).await
    }

    /// Send a request only to a supervisor that is already running.
    ///
    /// Fails with [`Error::NoSession`] rather than starting one, for callers
    /// that have something useful to do without a session: an edit can be
    /// applied to the file directly, and starting a supervisor (which then
    /// idles for an hour) to avoid that would be a poor trade.
    pub async fn request_existing(
        &self,
        request: &Request,
        on_event: impl FnMut(&Response),
    ) -> Result<Response> {
        self.send(request, on_event, StartPolicy::Fail).await
    }

    async fn send(
        &self,
        request: &Request,
        mut on_event: impl FnMut(&Response),
        start: StartPolicy,
    ) -> Result<Response> {
        match self.attempt(request, &mut on_event, start).await {
            Ok(response) => Ok(response),
            Err(Attempt { error, delivered }) => {
                // A supervisor that is shutting down still has a connectable
                // socket for a moment, so a request can land on a listener
                // that is about to disappear. Retrying that is right, but only
                // while the supervisor cannot already have acted on it: a
                // request that was delivered and then lost its answer may have
                // appended and run a cell, and running it twice is worse than
                // reporting that the answer was lost.
                let safe = is_transport_failure(&error) && (!delivered || request.is_repeatable());
                if !safe {
                    return Err(error);
                }
                self.attempt(request, &mut on_event, start)
                    .await
                    .map_err(|attempt| attempt.error)
            }
        }
    }

    async fn attempt(
        &self,
        request: &Request,
        on_event: &mut impl FnMut(&Response),
        start: StartPolicy,
    ) -> std::result::Result<Response, Attempt> {
        let undelivered = |error: Error| Attempt {
            error,
            delivered: false,
        };
        let delivered = |error: Error| Attempt {
            error,
            delivered: true,
        };

        let stream = match UnixStream::connect(&self.socket).await {
            Ok(stream) => stream,
            Err(_) if start == StartPolicy::Fail => {
                return Err(undelivered(Error::NoSession {
                    path: self.notebook.display().to_string(),
                }));
            }
            Err(_) => {
                // The socket is missing or nobody is listening on it. A
                // supervisor that is starting claims ownership before it binds,
                // so at most one of several racing clients actually starts one.
                self.spawn_supervisor().await.map_err(undelivered)?;
                UnixStream::connect(&self.socket)
                    .await
                    .map_err(|e| undelivered(Error::io(self.socket.display(), e)))?
            }
        };

        let (read_half, mut write_half) = stream.into_split();
        let mut line = serde_json::to_vec(request).map_err(|e| undelivered(Error::internal(e)))?;
        line.push(b'\n');
        write_half
            .write_all(&line)
            .await
            .map_err(|e| undelivered(Error::io(self.socket.display(), e)))?;
        write_half
            .flush()
            .await
            .map_err(|e| undelivered(Error::io(self.socket.display(), e)))?;

        // Past this point the supervisor has the request and may act on it, so
        // every failure below is reported as delivered.
        let mut lines = BufReader::new(read_half).lines();
        let mut last = None;
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| delivered(Error::io(self.socket.display(), e)))?
        {
            if line.trim().is_empty() {
                continue;
            }
            let response: Response = serde_json::from_str(&line).map_err(|e| {
                delivered(Error::protocol(format!(
                    "unreadable frame from the session: {e}"
                )))
            })?;
            match response {
                Response::Event(_) => on_event(&response),
                terminal => last = Some(terminal),
            }
        }

        match last {
            Some(Response::Error {
                code,
                message,
                retryable,
                hint,
                exit_code,
            }) => Err(delivered(Error::Remote {
                code,
                message,
                retryable,
                hint,
                exit_code,
            })),
            Some(response) => Ok(response),
            None => Err(delivered(Error::protocol(
                "the session closed the connection without answering",
            ))),
        }
    }

    /// Start a detached supervisor for this notebook and wait for its socket.
    ///
    ///
    /// Detached deliberately: the supervisor has to outlive the CLI process
    /// that started it, which is the entire point. It is re-executed from our
    /// own binary so there is nothing extra to install or find on `PATH`.
    async fn spawn_supervisor(&self) -> Result<()> {
        if !self.notebook.exists() {
            return Err(Error::invalid(format!(
                "{} does not exist; create it with `h5i-nb new {}`",
                self.notebook.display(),
                self.notebook.display()
            )));
        }
        let exe = std::env::current_exe()
            .map_err(|e| Error::internal(format!("cannot locate our own binary: {e}")))?;

        // Canonical, not as typed. The supervisor outlives the shell that
        // started it and reports its own path to `ls` and `status`, where a
        // relative path names nothing in particular. Routing is unaffected:
        // the session id is already computed from the canonical form, which is
        // why a relative and an absolute path reach the same session.
        let notebook = self
            .notebook
            .canonicalize()
            .unwrap_or_else(|_| self.notebook.clone());

        let mut command = tokio::process::Command::new(exe);
        command
            .args(command_prefix())
            .arg("serve")
            .arg(&notebook)
            .arg("--socket")
            .arg(&self.socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(unix)]
        {
            // Its own session, so closing the terminal that ran the CLI does
            // not take the kernel with it.
            command.process_group(0);
        }
        command
            .spawn()
            .map_err(|e| Error::internal(format!("could not start the session process: {e}")))?;

        let deadline = Instant::now() + SPAWN_TIMEOUT;
        while Instant::now() < deadline {
            if UnixStream::connect(&self.socket).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(Error::KernelStartTimeout {
            name: self.notebook.display().to_string(),
            timeout_secs: SPAWN_TIMEOUT.as_secs(),
        })
    }

    /// Ask a running supervisor to stop. A session that is not running is
    /// already in the requested state, so this is not an error.
    pub async fn shutdown(&self) -> Result<bool> {
        if !self.is_running().await {
            return Ok(false);
        }
        self.request(&Request::Shutdown, |_| {}).await?;
        Ok(true)
    }
}

/// Whether a request may start a supervisor that is not running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartPolicy {
    Start,
    Fail,
}

/// A failed attempt, and whether the supervisor could have acted on it.
struct Attempt {
    error: Error,
    /// The request reached the supervisor's socket. Anything that fails after
    /// that point may have been executed, however the call ended here.
    delivered: bool,
}

/// Whether a failure looks like a socket whose owner has gone away.
///
/// Deliberately narrow: a real protocol or execution error must not be
/// retried, because re-running a cell that already ran would be worse than
/// reporting the failure.
fn is_transport_failure(error: &Error) -> bool {
    matches!(error, Error::Io { .. })
        || matches!(error, Error::Protocol { detail } if detail.contains("without answering"))
}
