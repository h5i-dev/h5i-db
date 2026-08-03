//! A Jupyter kernel driven over ZeroMQ.
//!
//! We own the parts the transport crate leaves to the client: launching the
//! process, the startup handshake, correlating replies to requests, detecting
//! death, interrupting, restarting, and above all reaping. A notebook tool
//! that leaks kernel processes eats the machine, and that is the single most
//! common defect in hand-rolled clients, so process ownership is modelled
//! explicitly rather than left to `Drop` order.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jupyter_protocol::messaging::{
    CompleteRequest, ExecuteRequest, InputReply, InspectRequest, InterruptRequest,
    IsCompleteReplyStatus, IsCompleteRequest, KernelInfoRequest, ShutdownRequest,
};
use jupyter_protocol::{
    Channel, ConnectionInfo, ExecutionState, JupyterMessage, JupyterMessageContent, ReplyStatus,
    Transport,
};
use jupyter_zmq_client::{
    ClientIoPubConnection, DealerRecvConnection, DealerSendConnection,
    create_client_control_connection, create_client_iopub_connection,
    create_client_shell_connection_with_identity, create_client_stdin_connection_with_identity,
    peek_ports_with_listeners, peer_identity_for_session,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::document::{
    DisplayDataOutput, ErrorOutput, ExecuteResultOutput, MimeBundle, Output, StreamName,
    StreamOutput,
};
use crate::error::{Error, Result};
use crate::kernel::spec::{InterruptMode, KernelSpecInfo, runtime_dir};
use crate::kernel::{
    Completeness, Completions, ExecOptions, ExecOutcome, ExecStatus, InterruptHandle, Kernel,
    KernelStatus, OutputCollector, OutputEvent, OutputSink,
};

/// How long to wait for the first `kernel_info_reply` before giving up.
const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
/// Gap between `kernel_info_request` retries during the handshake.
const HANDSHAKE_RETRY: Duration = Duration::from_millis(250);
/// Default cap on the short request/reply calls (complete, inspect, …).
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Grace given to a kernel to exit cleanly before we escalate to signals.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// After an interrupt, how long to keep draining so the `KeyboardInterrupt`
/// traceback and the trailing `idle` still make it into the notebook.
const INTERRUPT_DRAIN: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct StartOptions {
    /// Working directory for the kernel process. Cells resolve relative paths
    /// against this, so it defaults to the notebook's directory, not ours.
    pub cwd: Option<PathBuf>,
    /// Extra environment on top of the kernelspec's own `env`.
    pub env: HashMap<String, String>,
    pub startup_timeout: Duration,
}

impl Default for StartOptions {
    fn default() -> Self {
        StartOptions {
            cwd: None,
            env: HashMap::new(),
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
        }
    }
}

// ---------------------------------------------------------------------------
// message routing
// ---------------------------------------------------------------------------

/// Routes incoming messages to whoever is waiting on their `parent_header`.
///
/// Every channel reader funnels through here, so a request's iopub traffic and
/// its shell reply arrive on one stream in the order the kernel sent them.
/// Messages with no registered parent are dropped rather than queued: they are
/// almost always another client's traffic on the shared iopub bus, and
/// buffering them without a consumer is an unbounded leak.
#[derive(Default)]
struct Router {
    pending: Mutex<HashMap<String, mpsc::UnboundedSender<JupyterMessage>>>,
}

impl Router {
    fn register(&self, msg_id: &str) -> mpsc::UnboundedReceiver<JupyterMessage> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending
            .lock()
            .expect("router mutex poisoned")
            .insert(msg_id.to_string(), tx);
        rx
    }

    fn unregister(&self, msg_id: &str) {
        self.pending
            .lock()
            .expect("router mutex poisoned")
            .remove(msg_id);
    }

    fn deliver(&self, message: JupyterMessage) {
        let Some(parent) = message.parent_header.as_ref().map(|h| h.msg_id.clone()) else {
            return;
        };
        let pending = self.pending.lock().expect("router mutex poisoned");
        if let Some(tx) = pending.get(&parent) {
            let _ = tx.send(message);
        }
    }
}

/// `KernelStatus` in a form the reader tasks can update without a lock.
#[derive(Default)]
struct SharedStatus(AtomicU8);

impl SharedStatus {
    fn get(&self) -> KernelStatus {
        match self.0.load(Ordering::Acquire) {
            0 => KernelStatus::Starting,
            1 => KernelStatus::Idle,
            2 => KernelStatus::Busy,
            3 => KernelStatus::Restarting,
            _ => KernelStatus::Dead,
        }
    }

    fn set(&self, status: KernelStatus) {
        self.0.store(
            match status {
                KernelStatus::Starting => 0,
                KernelStatus::Idle => 1,
                KernelStatus::Busy => 2,
                KernelStatus::Restarting => 3,
                KernelStatus::Dead => 4,
            },
            Ordering::Release,
        );
    }

    /// Apply a status seen on iopub, without resurrecting a dead kernel.
    fn observe(&self, state: &ExecutionState) {
        if self.get() == KernelStatus::Dead {
            return;
        }
        match state {
            ExecutionState::Idle => self.set(KernelStatus::Idle),
            ExecutionState::Busy => self.set(KernelStatus::Busy),
            ExecutionState::Starting => self.set(KernelStatus::Starting),
            ExecutionState::Restarting | ExecutionState::AutoRestarting => {
                self.set(KernelStatus::Restarting)
            }
            ExecutionState::Dead | ExecutionState::Terminating => self.set(KernelStatus::Dead),
            _ => {}
        }
    }
}

/// The kernel process, owned by a monitor task that reaps it.
struct Process {
    pid: u32,
    /// Fires with the exit status once the child is reaped.
    exited: watch::Receiver<Option<std::process::ExitStatus>>,
    /// Tail of the kernel's own stderr, for diagnosing a failed start.
    stderr: Arc<Mutex<String>>,
    monitor: JoinHandle<()>,
}

impl Process {
    fn has_exited(&self) -> bool {
        self.exited.borrow().is_some()
    }

    /// Signal the kernel's process group.
    ///
    /// The group exists because we spawn with `process_group(0)`; without that
    /// a negative pid would target *our own* group, so the two must stay
    /// together.
    fn signal_group(&self, sig: i32) {
        if self.has_exited() {
            return;
        }
        // SAFETY: `kill` on a pid we own; an already-reaped pid returns ESRCH
        // rather than hitting an unrelated process, because the monitor task
        // holds the child handle until it has been waited on.
        unsafe {
            libc::kill(-(self.pid as i32), sig);
        }
    }

    async fn wait_for_exit(&mut self, timeout: Duration) -> bool {
        if self.has_exited() {
            return true;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::timeout_at(deadline, self.exited.changed())
                .await
                .is_err()
            {
                return self.has_exited();
            }
            if self.has_exited() {
                return true;
            }
        }
    }

    fn stderr_tail(&self) -> String {
        self.stderr.lock().expect("stderr mutex poisoned").clone()
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // Last line of defence. A kernel outliving the client is a process
        // leak the user cannot see and will not clean up.
        self.signal_group(libc::SIGKILL);
        self.monitor.abort();
    }
}

// ---------------------------------------------------------------------------
// the kernel
// ---------------------------------------------------------------------------

pub struct JupyterKernel {
    spec: KernelSpecInfo,
    start_options: StartOptions,
    session_id: String,
    connection: ConnectionInfo,
    connection_file: PathBuf,
    process: Process,
    shell: DealerSendConnection,
    control: DealerSendConnection,
    stdin: DealerSendConnection,
    router: Arc<Router>,
    status: Arc<SharedStatus>,
    /// Set by anyone holding an [`InterruptHandle`]; acted on by `execute`,
    /// which is the only place that knows the kernel's interrupt mode.
    interrupt: InterruptHandle,
    readers: Vec<JoinHandle<()>>,
    /// Banner and language info from the handshake.
    pub kernel_info: Option<Box<jupyter_protocol::KernelInfoReply>>,
}

impl JupyterKernel {
    pub async fn start(spec: KernelSpecInfo, options: StartOptions) -> Result<Self> {
        let session_id = uuid::Uuid::new_v4().to_string();
        let key = uuid::Uuid::new_v4().to_string();

        // Hold the listeners until the moment we spawn, so the window in which
        // another process can steal a port is as small as it can be.
        let (ports, listeners) = peek_ports_with_listeners([127, 0, 0, 1].into(), 5)
            .await
            .map_err(|e| Error::KernelStartFailed {
                name: spec.name.clone(),
                detail: format!("could not reserve ports: {e}"),
            })?;

        let connection = ConnectionInfo {
            ip: "127.0.0.1".to_string(),
            transport: Transport::TCP,
            shell_port: ports[0],
            iopub_port: ports[1],
            stdin_port: ports[2],
            control_port: ports[3],
            hb_port: ports[4],
            key: key.clone(),
            signature_scheme: "hmac-sha256".to_string(),
            kernel_name: Some(spec.name.clone()),
        };

        let connection_file = write_connection_file(&connection, &session_id)?;
        let process = spawn_kernel(&spec, &options, &connection_file, listeners)?;

        let mut kernel = Self::connect(
            spec,
            options,
            session_id,
            connection,
            connection_file,
            process,
        )
        .await?;
        kernel.handshake().await?;
        Ok(kernel)
    }

    /// Wire up sockets and reader tasks against an already-running process.
    async fn connect(
        spec: KernelSpecInfo,
        start_options: StartOptions,
        session_id: String,
        connection: ConnectionInfo,
        connection_file: PathBuf,
        process: Process,
    ) -> Result<Self> {
        let identity = peer_identity_for_session(&session_id)
            .map_err(|e| Error::protocol(format!("peer identity: {e}")))?;

        let iopub = create_client_iopub_connection(&connection, "", &session_id)
            .await
            .map_err(|e| connect_failed(&spec.name, "iopub", e))?;
        let shell = create_client_shell_connection_with_identity(
            &connection,
            &session_id,
            identity.clone(),
        )
        .await
        .map_err(|e| connect_failed(&spec.name, "shell", e))?;
        let control = create_client_control_connection(&connection, &session_id)
            .await
            .map_err(|e| connect_failed(&spec.name, "control", e))?;
        let stdin =
            create_client_stdin_connection_with_identity(&connection, &session_id, identity)
                .await
                .map_err(|e| connect_failed(&spec.name, "stdin", e))?;

        let router = Arc::new(Router::default());
        let status = Arc::new(SharedStatus::default());
        status.set(KernelStatus::Starting);

        let (shell_tx, shell_rx) = shell.split();
        let (control_tx, control_rx) = control.split();
        let (stdin_tx, stdin_rx) = stdin.split();

        let readers = vec![
            spawn_iopub_reader(iopub, router.clone(), status.clone()),
            spawn_dealer_reader(shell_rx, router.clone()),
            spawn_dealer_reader(control_rx, router.clone()),
            spawn_dealer_reader(stdin_rx, router.clone()),
        ];

        Ok(JupyterKernel {
            spec,
            start_options,
            session_id,
            connection,
            connection_file,
            process,
            shell: shell_tx,
            control: control_tx,
            stdin: stdin_tx,
            router,
            status,
            interrupt: InterruptHandle::new(),
            readers,
            kernel_info: None,
        })
    }

    /// Poll `kernel_info_request` until the kernel answers.
    ///
    /// A single request is the classic hang: a kernel publishes nothing until
    /// its sockets are up, and ZeroMQ silently drops what we send before then.
    /// Retrying is the only reliable signal that the kernel is really ready.
    async fn handshake(&mut self) -> Result<()> {
        let deadline = Instant::now() + self.start_options.startup_timeout;
        loop {
            if self.process.has_exited() {
                return Err(Error::KernelStartFailed {
                    name: self.spec.name.clone(),
                    detail: describe_early_exit(&self.process),
                });
            }
            match self
                .request_reply(KernelInfoRequest {}, Channel::Shell, HANDSHAKE_RETRY)
                .await
            {
                Ok(message) => {
                    if let JupyterMessageContent::KernelInfoReply(info) = message.content {
                        self.kernel_info = Some(info);
                        self.settle_to_idle(Duration::from_secs(2)).await;
                        return Ok(());
                    }
                }
                Err(Error::ExecuteTimeout { .. }) => {}
                Err(e) => return Err(e),
            }
            if Instant::now() >= deadline {
                return Err(Error::KernelStartTimeout {
                    name: self.spec.name.clone(),
                    timeout_secs: self.start_options.startup_timeout.as_secs(),
                });
            }
        }
    }

    /// Wait out the `busy`/`idle` pair the handshake itself provoked.
    ///
    /// The `kernel_info_reply` arrives on shell while its matching iopub
    /// status messages are still in flight, so a caller that checks the status
    /// the instant `start()` returns can legitimately observe `Busy` for a
    /// kernel that is running nothing. Rather than expose that race, settle
    /// here: no user code has run yet, so once the reply is in hand the kernel
    /// is idle by definition and the only question is when iopub says so.
    async fn settle_to_idle(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.status.get() {
                KernelStatus::Idle | KernelStatus::Dead => return,
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        if self.status.get() != KernelStatus::Dead {
            self.status.set(KernelStatus::Idle);
        }
    }

    async fn send(&mut self, message: JupyterMessage, channel: Channel) -> Result<()> {
        let connection = match channel {
            Channel::Control => &mut self.control,
            Channel::Stdin => &mut self.stdin,
            _ => &mut self.shell,
        };
        connection
            .send(message)
            .await
            .map_err(|e| Error::protocol(format!("send on {channel:?}: {e}")))
    }

    /// Send a request and wait for its `*_reply`, ignoring the iopub traffic
    /// the same request also produces.
    async fn request_reply(
        &mut self,
        content: impl Into<JupyterMessageContent>,
        channel: Channel,
        timeout: Duration,
    ) -> Result<JupyterMessage> {
        let message = JupyterMessage::new(content, None);
        let msg_id = message.header.msg_id.clone();
        let mut rx = self.router.register(&msg_id);
        let guard = Unregister {
            router: self.router.clone(),
            msg_id: msg_id.clone(),
        };
        self.send(message, channel).await?;

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(message)) => {
                    if message.message_type().ends_with("_reply") {
                        drop(guard);
                        return Ok(message);
                    }
                }
                Ok(None) => {
                    return Err(Error::protocol("kernel connection closed while waiting"));
                }
                Err(_) => {
                    return Err(Error::ExecuteTimeout {
                        timeout_secs: timeout.as_secs().max(1),
                    });
                }
            }
        }
    }

    /// Where the connection file lives, so `jupyter console --existing <path>`
    /// can attach a second client to this same kernel.
    pub fn connection_file(&self) -> &std::path::Path {
        &self.connection_file
    }

    pub fn connection_info(&self) -> &ConnectionInfo {
        &self.connection
    }

    /// The kernel process id, for tests and diagnostics.
    pub fn pid(&self) -> u32 {
        self.process.pid
    }

    /// The session id this client signs its messages with.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    fn ensure_alive(&self) -> Result<()> {
        match self.status.get() {
            KernelStatus::Dead => Err(Error::KernelDied {
                detail: describe_exit(&self.process),
            }),
            _ => Ok(()),
        }
    }

    /// Tear down sockets, reader tasks, and the connection file.
    async fn teardown(&mut self) {
        for reader in self.readers.drain(..) {
            reader.abort();
        }
        let _ = std::fs::remove_file(&self.connection_file);
    }
}

/// Removes a router registration however the caller leaves the function.
struct Unregister {
    router: Arc<Router>,
    msg_id: String,
}

impl Drop for Unregister {
    fn drop(&mut self) {
        self.router.unregister(&self.msg_id);
    }
}

#[async_trait]
impl Kernel for JupyterKernel {
    fn spec(&self) -> &KernelSpecInfo {
        &self.spec
    }

    fn status(&self) -> KernelStatus {
        if self.process.has_exited() {
            self.status.set(KernelStatus::Dead);
        }
        self.status.get()
    }

    fn interrupt_handle(&self) -> InterruptHandle {
        self.interrupt.clone()
    }

    async fn execute(
        &mut self,
        code: &str,
        options: &ExecOptions,
        sink: &mut dyn OutputSink,
    ) -> Result<ExecOutcome> {
        self.ensure_alive()?;

        let request = ExecuteRequest {
            code: code.to_string(),
            silent: options.silent,
            store_history: options.store_history,
            user_expressions: None,
            allow_stdin: options.allow_stdin,
            stop_on_error: options.stop_on_error,
        };
        let message = JupyterMessage::new(request, None);
        let msg_id = message.header.msg_id.clone();
        let mut rx = self.router.register(&msg_id);
        let _guard = Unregister {
            router: self.router.clone(),
            msg_id,
        };

        let started = Instant::now();
        // A stale request from a previous cell must not interrupt this one.
        self.interrupt.reset();
        self.send(message, Channel::Shell).await?;

        let mut collector = OutputCollector::new();
        let mut execution_count: Option<i64> = None;
        let mut reply_status: Option<ReplyStatus> = None;
        let mut saw_idle = false;
        let mut interrupted_at: Option<Instant> = None;
        let mut interrupted_by_user = false;

        loop {
            // The kernel is done with a request when it has both sent the
            // reply and returned to idle. Waiting for only one of them either
            // truncates trailing output or hangs on kernels that reply late.
            if saw_idle && reply_status.is_some() {
                break;
            }

            let deadline = match interrupted_at {
                Some(at) => Some(tokio::time::Instant::from_std(at + INTERRUPT_DRAIN)),
                None => options
                    .timeout
                    .map(|t| tokio::time::Instant::from_std(started + t)),
            };

            // Three things can wake us: a message, the deadline, or somebody
            // holding an `InterruptHandle` pressing Ctrl-C. The interrupt
            // branch is disabled once we have already interrupted, so the
            // drain window is governed purely by the deadline.
            let wake = {
                let interrupt = self.interrupt.clone();
                tokio::select! {
                    biased;
                    message = rx.recv() => match message {
                        Some(message) => Wake::Message(Box::new(message)),
                        None => Wake::Closed,
                    },
                    _ = async {
                        match deadline {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => Wake::Deadline,
                    _ = interrupt.requested(), if interrupted_at.is_none() => Wake::Interrupted,
                }
            };

            let message = match wake {
                Wake::Message(message) => *message,
                Wake::Closed => {
                    return Err(Error::KernelDied {
                        detail: describe_exit(&self.process),
                    });
                }
                Wake::Deadline if interrupted_at.is_some() => {
                    // The interrupt did not settle the kernel in time.
                    return Err(if interrupted_by_user {
                        Error::Interrupted
                    } else {
                        Error::ExecuteTimeout {
                            timeout_secs: options.timeout.map(|t| t.as_secs()).unwrap_or(0).max(1),
                        }
                    });
                }
                Wake::Deadline => {
                    // Interrupt rather than abandon: leaving the kernel busy
                    // would block every later cell in the session, and the
                    // partial outputs already reached the sink.
                    let _ = self.interrupt().await;
                    interrupted_at = Some(Instant::now());
                    continue;
                }
                Wake::Interrupted => {
                    let _ = self.interrupt().await;
                    interrupted_at = Some(Instant::now());
                    interrupted_by_user = true;
                    continue;
                }
            };

            // Answered here rather than in the message folder because it is
            // the one inbound message that requires sending something back,
            // and an unanswered `input_request` hangs the kernel forever.
            if let JupyterMessageContent::InputRequest(request) = &message.content {
                sink.emit(OutputEvent::InputRequest {
                    prompt: request.prompt.clone(),
                    password: request.password,
                });
                let reply = JupyterMessage::new(empty_input_reply(), Some(&message));
                let _ = self.send(reply, Channel::Stdin).await;
                continue;
            }

            match self.handle_execution_message(message, &mut collector, sink, &mut execution_count)
            {
                Progress::Idle => saw_idle = true,
                Progress::Reply(status) => reply_status = Some(status),
                Progress::Continue => {}
            }
        }

        // An interrupt that settled cleanly still produced outputs (the
        // KeyboardInterrupt traceback), so it is reported as a normal error
        // outcome rather than as a failure of the call.
        if interrupted_at.is_some() && !interrupted_by_user {
            return Err(Error::ExecuteTimeout {
                timeout_secs: options.timeout.map(|t| t.as_secs()).unwrap_or(0).max(1),
            });
        }

        Ok(ExecOutcome {
            status: match reply_status {
                Some(ReplyStatus::Error) => ExecStatus::Error,
                Some(ReplyStatus::Aborted) => ExecStatus::Aborted,
                _ => ExecStatus::Ok,
            },
            execution_count,
            outputs: collector.into_outputs(),
            elapsed: started.elapsed(),
        })
    }

    async fn complete(&mut self, code: &str, cursor: usize) -> Result<Completions> {
        self.ensure_alive()?;
        let reply = self
            .request_reply(
                CompleteRequest {
                    code: code.to_string(),
                    cursor_pos: cursor,
                },
                Channel::Shell,
                DEFAULT_REQUEST_TIMEOUT,
            )
            .await?;
        let JupyterMessageContent::CompleteReply(reply) = reply.content else {
            return Ok(Completions::default());
        };
        let types = reply
            .metadata
            .get("_jupyter_types_experimental")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .map(|i| i.get("type").and_then(|t| t.as_str()).map(str::to_string))
                    .collect()
            })
            .unwrap_or_else(|| vec![None; reply.matches.len()]);
        Ok(Completions {
            matches: reply.matches,
            cursor_start: reply.cursor_start,
            cursor_end: reply.cursor_end,
            types,
        })
    }

    async fn inspect(
        &mut self,
        code: &str,
        cursor: usize,
        detail: u8,
    ) -> Result<Option<MimeBundle>> {
        self.ensure_alive()?;
        let reply = self
            .request_reply(
                InspectRequest {
                    code: code.to_string(),
                    cursor_pos: cursor,
                    detail_level: Some(if detail > 0 { 1 } else { 0 }),
                },
                Channel::Shell,
                DEFAULT_REQUEST_TIMEOUT,
            )
            .await?;
        let JupyterMessageContent::InspectReply(reply) = reply.content else {
            return Ok(None);
        };
        if !reply.found {
            return Ok(None);
        }
        Ok(Some(media_to_bundle(&reply.data)))
    }

    async fn is_complete(&mut self, code: &str) -> Result<Completeness> {
        self.ensure_alive()?;
        let reply = self
            .request_reply(
                IsCompleteRequest {
                    code: code.to_string(),
                },
                Channel::Shell,
                DEFAULT_REQUEST_TIMEOUT,
            )
            .await?;
        Ok(match reply.content {
            JupyterMessageContent::IsCompleteReply(reply) => match reply.status {
                IsCompleteReplyStatus::Complete => Completeness::Complete,
                IsCompleteReplyStatus::Incomplete => Completeness::Incomplete {
                    indent: reply.indent,
                },
                IsCompleteReplyStatus::Invalid => Completeness::Invalid,
                IsCompleteReplyStatus::Unknown => Completeness::Unknown,
            },
            _ => Completeness::Unknown,
        })
    }

    async fn interrupt(&mut self) -> Result<()> {
        match self.spec.interrupt_mode {
            InterruptMode::Signal => {
                self.process.signal_group(libc::SIGINT);
                Ok(())
            }
            InterruptMode::Message => self
                .request_reply(
                    InterruptRequest {},
                    Channel::Control,
                    DEFAULT_REQUEST_TIMEOUT,
                )
                .await
                .map(|_| ()),
        }
    }

    async fn restart(&mut self) -> Result<()> {
        self.status.set(KernelStatus::Restarting);
        self.stop_process(true).await;
        self.teardown().await;

        let spec = self.spec.clone();
        let options = self.start_options.clone();
        let fresh = JupyterKernel::start(spec, options).await?;

        // Move the new kernel's guts into `self` so callers keep their handle.
        let old = std::mem::replace(self, fresh);
        // `old` still owns a dead `Process`, whose Drop is now a no-op signal.
        drop(old);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.stop_process(false).await;
        self.teardown().await;
        self.status.set(KernelStatus::Dead);
        Ok(())
    }
}

impl JupyterKernel {
    /// Ask the kernel to exit, then escalate until it actually has.
    async fn stop_process(&mut self, restart: bool) {
        if self.process.has_exited() {
            return;
        }
        // Politely first: a clean shutdown lets the kernel flush and run
        // atexit handlers, which is where user data gets saved.
        let _ = self
            .send(
                JupyterMessage::new(ShutdownRequest { restart }, None),
                Channel::Control,
            )
            .await;
        if self.process.wait_for_exit(SHUTDOWN_GRACE).await {
            return;
        }
        self.process.signal_group(libc::SIGTERM);
        if self.process.wait_for_exit(Duration::from_secs(2)).await {
            return;
        }
        self.process.signal_group(libc::SIGKILL);
        self.process.wait_for_exit(Duration::from_secs(2)).await;
    }

    /// Fold one message into the execution state.
    fn handle_execution_message(
        &self,
        message: JupyterMessage,
        collector: &mut OutputCollector,
        sink: &mut dyn OutputSink,
        execution_count: &mut Option<i64>,
    ) -> Progress {
        let mut emit = |event: OutputEvent, collector: &mut OutputCollector| {
            collector.apply(&event);
            sink.emit(event);
        };

        match message.content {
            JupyterMessageContent::Status(status) => {
                self.status.observe(&status.execution_state);
                sink.emit(OutputEvent::Status(self.status.get()));
                return if matches!(status.execution_state, ExecutionState::Idle) {
                    Progress::Idle
                } else {
                    Progress::Continue
                };
            }
            JupyterMessageContent::ExecuteInput(input) => {
                let count = input.execution_count.0 as i64;
                *execution_count = Some(count);
                sink.emit(OutputEvent::ExecutionCount(count));
            }
            JupyterMessageContent::StreamContent(stream) => {
                let name = match stream.name {
                    jupyter_protocol::Stdio::Stdout => StreamName::Stdout,
                    jupyter_protocol::Stdio::Stderr => StreamName::Stderr,
                };
                emit(
                    OutputEvent::Output(Output::Stream(StreamOutput {
                        name,
                        text: stream.text,
                        extra: Default::default(),
                    })),
                    collector,
                );
            }
            JupyterMessageContent::DisplayData(display) => {
                let display_id = display
                    .transient
                    .as_ref()
                    .and_then(|t| t.display_id.clone());
                emit(
                    OutputEvent::Output(Output::DisplayData(DisplayDataOutput {
                        data: media_to_bundle(&display.data),
                        metadata: display.metadata,
                        extra: Default::default(),
                    })),
                    collector,
                );
                if let Some(id) = display_id {
                    collector.tag_display_id(&id);
                }
            }
            JupyterMessageContent::UpdateDisplayData(update) => {
                if let Some(id) = update.transient.display_id.clone() {
                    emit(
                        OutputEvent::Update {
                            display_id: id,
                            data: media_to_bundle(&update.data),
                        },
                        collector,
                    );
                }
            }
            JupyterMessageContent::ExecuteResult(result) => {
                let count = result.execution_count.0 as i64;
                *execution_count = Some(count);
                let display_id = result.transient.as_ref().and_then(|t| t.display_id.clone());
                emit(
                    OutputEvent::Output(Output::ExecuteResult(ExecuteResultOutput {
                        data: media_to_bundle(&result.data),
                        metadata: result.metadata,
                        execution_count: Some(count),
                        extra: Default::default(),
                    })),
                    collector,
                );
                if let Some(id) = display_id {
                    collector.tag_display_id(&id);
                }
            }
            JupyterMessageContent::ErrorOutput(error) => {
                emit(
                    OutputEvent::Output(Output::Error(ErrorOutput {
                        ename: error.ename,
                        evalue: error.evalue,
                        traceback: error.traceback,
                        extra: Default::default(),
                    })),
                    collector,
                );
            }
            JupyterMessageContent::ClearOutput(clear) => {
                emit(OutputEvent::Clear { wait: clear.wait }, collector);
            }
            JupyterMessageContent::ExecuteReply(reply) => {
                if reply.execution_count.0 > 0 {
                    *execution_count = Some(reply.execution_count.0 as i64);
                }
                return Progress::Reply(reply.status);
            }
            // comm traffic (ipywidgets) is accepted so widget-producing
            // libraries do not error, but nothing renders it in this build.
            _ => {}
        }
        Progress::Continue
    }
}

/// Why the execute loop woke up.
enum Wake {
    // Boxed: a JupyterMessage is an order of magnitude larger than the other
    // variants, and this enum is constructed on every message.
    Message(Box<JupyterMessage>),
    /// The kernel's channels closed under us.
    Closed,
    Deadline,
    /// Somebody holding an `InterruptHandle` asked to stop the cell.
    Interrupted,
}

/// What one message told us about where the request has got to.
enum Progress {
    /// The closing `idle` status: no more output is coming.
    Idle,
    /// The `execute_reply` landed, carrying the final status.
    Reply(ReplyStatus),
    Continue,
}

// ---------------------------------------------------------------------------
// launch helpers
// ---------------------------------------------------------------------------

fn connect_failed(kernel: &str, channel: &str, error: impl std::fmt::Display) -> Error {
    Error::KernelStartFailed {
        name: kernel.to_string(),
        detail: format!("could not connect the {channel} channel: {error}"),
    }
}

fn describe_exit(process: &Process) -> String {
    match *process.exited.borrow() {
        Some(status) => {
            let tail = process.stderr_tail();
            if tail.trim().is_empty() {
                format!(" ({status})")
            } else {
                format!(" ({status}); last output:\n{}", tail.trim_end())
            }
        }
        None => String::new(),
    }
}

fn describe_early_exit(process: &Process) -> String {
    let tail = process.stderr_tail();
    let status = process
        .exited
        .borrow()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "exited".to_string());
    if tail.trim().is_empty() {
        format!("the kernel process {status} before completing its handshake")
    } else {
        format!(
            "the kernel process {status} before completing its handshake:\n{}",
            tail.trim_end()
        )
    }
}

/// Write the connection file where Jupyter's own tooling looks for it, so
/// `jupyter console --existing` can attach to a kernel we started.
fn write_connection_file(connection: &ConnectionInfo, session_id: &str) -> Result<PathBuf> {
    let dir = runtime_dir();
    std::fs::create_dir_all(&dir).map_err(|e| Error::io(dir.display(), e))?;
    let path = dir.join(format!("kernel-{session_id}.json"));
    let body = serde_json::to_string_pretty(connection).map_err(Error::internal)?;
    std::fs::write(&path, body).map_err(|e| Error::io(path.display(), e))?;

    // The file carries the HMAC key: anyone who can read it can execute code
    // in this kernel.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(path)
}

fn spawn_kernel(
    spec: &KernelSpecInfo,
    options: &StartOptions,
    connection_file: &std::path::Path,
    listeners: Vec<tokio::net::TcpListener>,
) -> Result<Process> {
    let argv: Vec<String> = spec
        .argv
        .iter()
        .map(|arg| {
            if arg.contains("{connection_file}") {
                arg.replace("{connection_file}", &connection_file.to_string_lossy())
            } else {
                arg.clone()
            }
        })
        .collect();

    let mut command = tokio::process::Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .envs(&spec.env)
        .envs(&options.env)
        .kill_on_drop(false);

    if let Some(cwd) = &options.cwd {
        command.current_dir(cwd);
    }

    #[cfg(unix)]
    {
        // Its own process group, so a SIGINT aimed at the kernel cannot reach
        // us and a Ctrl-C in our TUI cannot reach the kernel by accident.
        // `Process::signal_group` depends on this.
        command.process_group(0);
    }

    // The kernel binds these ports as its first action; release them now.
    drop(listeners);

    let mut child = command.spawn().map_err(|e| Error::KernelStartFailed {
        name: spec.name.clone(),
        detail: format!("could not run {:?}: {e}", argv[0]),
    })?;

    let pid = child.id().ok_or_else(|| Error::KernelStartFailed {
        name: spec.name.clone(),
        detail: "the kernel process exited immediately".to_string(),
    })?;

    let stderr_tail = Arc::new(Mutex::new(String::new()));
    if let Some(stderr) = child.stderr.take() {
        spawn_tail_reader(stderr, stderr_tail.clone());
    }
    if let Some(stdout) = child.stdout.take() {
        spawn_tail_reader(stdout, stderr_tail.clone());
    }

    let (exit_tx, exit_rx) = watch::channel(None);
    let monitor = tokio::spawn(async move {
        let status = child.wait().await.ok();
        let _ = exit_tx.send(status);
    });

    Ok(Process {
        pid,
        exited: exit_rx,
        stderr: stderr_tail,
        monitor,
    })
}

/// Keep the last few KiB of a kernel's own output.
///
/// A kernel that dies during startup explains why on stderr and nowhere else,
/// so throwing this away turns every launch failure into "it did not work".
/// Bounded because a chatty kernel would otherwise grow it without limit.
fn spawn_tail_reader<R>(reader: R, sink: Arc<Mutex<String>>)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    const MAX_TAIL: usize = 8 * 1024;
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut reader = reader;
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let mut guard = sink.lock().expect("stderr mutex poisoned");
                    guard.push_str(&String::from_utf8_lossy(&buf[..n]));
                    if guard.len() > MAX_TAIL {
                        let cut = guard.len() - MAX_TAIL;
                        // Trim on a char boundary; the tail is diagnostic text.
                        let cut = (cut..guard.len())
                            .find(|i| guard.is_char_boundary(*i))
                            .unwrap_or(guard.len());
                        *guard = guard[cut..].to_string();
                    }
                }
            }
        }
    });
}

fn spawn_iopub_reader(
    mut iopub: ClientIoPubConnection,
    router: Arc<Router>,
    status: Arc<SharedStatus>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match iopub.read().await {
                Ok(message) => {
                    // Status is tracked from every status message, including
                    // ones parented to another client's request, because the
                    // kernel is equally busy either way.
                    if let JupyterMessageContent::Status(s) = &message.content {
                        status.observe(&s.execution_state);
                    }
                    router.deliver(message);
                }
                Err(error) => {
                    tracing::debug!(%error, "iopub reader stopped");
                    break;
                }
            }
        }
    })
}

fn spawn_dealer_reader(
    mut connection: DealerRecvConnection,
    router: Arc<Router>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match connection.read().await {
                Ok(message) => router.deliver(message),
                Err(error) => {
                    tracing::debug!(%error, "channel reader stopped");
                    break;
                }
            }
        }
    })
}

/// `Media` to our mime bundle.
///
/// The protocol crate models media as a typed enum, which serializes back to
/// the plain `{mime: payload}` map the notebook format uses.
pub(crate) fn media_to_bundle(media: &jupyter_protocol::Media) -> MimeBundle {
    match serde_json::to_value(media) {
        Ok(serde_json::Value::Object(map)) => MimeBundle(map),
        _ => MimeBundle::new(),
    }
}

/// Answer an `input_request` the caller cannot satisfy.
///
/// Sent so the kernel unblocks with an `EOFError` instead of waiting forever
/// for a human who is not there.
pub(crate) fn empty_input_reply() -> InputReply {
    InputReply {
        value: String::new(),
        status: ReplyStatus::Error,
        error: None,
    }
}
