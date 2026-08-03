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
        mut on_event: impl FnMut(&Response),
    ) -> Result<Response> {
        match self.attempt(request, &mut on_event).await {
            Err(error) if is_stale_socket(&error) => {
                // A supervisor that is shutting down still has a connectable
                // socket for a moment, so a request can land on a listener
                // that is about to disappear. That is transient: clear the
                // stale socket and start a fresh session once.
                let _ = std::fs::remove_file(&self.socket);
                self.attempt(request, &mut on_event).await
            }
            other => other,
        }
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

    async fn attempt(
        &self,
        request: &Request,
        on_event: &mut impl FnMut(&Response),
    ) -> Result<Response> {
        let stream = match UnixStream::connect(&self.socket).await {
            Ok(stream) => stream,
            Err(_) => {
                self.spawn_supervisor().await?;
                UnixStream::connect(&self.socket)
                    .await
                    .map_err(|e| Error::io(self.socket.display(), e))?
            }
        };

        let (read_half, mut write_half) = stream.into_split();
        let mut line = serde_json::to_vec(request).map_err(Error::internal)?;
        line.push(b'\n');
        write_half
            .write_all(&line)
            .await
            .map_err(|e| Error::io(self.socket.display(), e))?;
        write_half
            .flush()
            .await
            .map_err(|e| Error::io(self.socket.display(), e))?;

        let mut lines = BufReader::new(read_half).lines();
        let mut last = None;
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| Error::io(self.socket.display(), e))?
        {
            if line.trim().is_empty() {
                continue;
            }
            let response: Response = serde_json::from_str(&line)
                .map_err(|e| Error::protocol(format!("unreadable frame from the session: {e}")))?;
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
            }) => Err(Error::Remote {
                code,
                message,
                retryable,
                hint,
                exit_code,
            }),
            Some(response) => Ok(response),
            None => Err(Error::protocol(
                "the session closed the connection without answering",
            )),
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

        let mut command = tokio::process::Command::new(exe);
        command
            .arg("serve")
            .arg(&self.notebook)
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

fn is_stale_socket(error: &Error) -> bool {
    SessionClient::is_transport_failure(error)
}
