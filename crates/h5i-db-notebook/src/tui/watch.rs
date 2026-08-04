//! Following a notebook that somebody else is driving.
//!
//! The editing UI ([`super::run`]) owns a session: it holds the notebook, runs
//! cells, and writes the file. A watcher owns nothing. It takes no lock,
//! starts no kernel, and never writes, which is what lets any number of them
//! sit beside an agent that is running cells through `nb exec` without any of
//! them being able to spoil the run.
//!
//! Changes arrive through the file rather than through a subscription, because
//! the file already is the broadcast channel: the supervisor rewrites the
//! whole notebook after every cell, and saves are a rename, so a reader can
//! never see a half-written document. Polling `(mtime, len)` a few times a
//! second is enough to keep up with cells that take seconds, and it needs no
//! protocol, no reconnect logic, and no list of subscribers in the supervisor.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crossterm::event::{Event as TermEvent, KeyCode, KeyEventKind, KeyModifiers, MouseEventKind};
use ratatui::Terminal;
use tokio::sync::mpsc;

use crate::document::Notebook;
use crate::error::{Error, Result};
use crate::kernel::KernelStatus;
use crate::supervisor::SessionClient;
use crate::supervisor::protocol::{Request, Response};
use crate::tui::app::{App, Command, Event};
use crate::tui::image::ImageProtocol;
use crate::tui::{Backend, TICK, enter, leave, present, spawn_terminal_reader};

/// How often the file is checked for changes.
///
/// Fast enough that a cell finishing feels immediate, slow enough that idling
/// costs a stat every quarter second.
const FILE_POLL: Duration = Duration::from_millis(250);

/// How often the supervisor is asked what the kernel is doing.
///
/// The file cannot answer this: a notebook mid-cell looks exactly like one
/// that is finished. `Status` answers without the session lock, so asking
/// while a cell runs is free.
const STATUS_POLL: Duration = Duration::from_secs(1);

/// Watch `path` until the reader quits.
pub async fn run(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref().to_path_buf();
    // Read once up front so an unreadable notebook fails here, with a message,
    // rather than as an empty pane that never explains itself.
    let notebook = Notebook::read(&path)?;

    let (command_tx, command_rx) = mpsc::unbounded_channel::<Command>();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event>();
    let watcher = tokio::spawn(watch_source(path.clone(), event_tx, command_rx));

    let mut app = App::watching(path, notebook);
    let protocol = ImageProtocol::detect();

    let (mut terminal, enhanced) = enter()?;
    let outcome = watch_loop(
        &mut terminal,
        &mut app,
        &command_tx,
        &mut event_rx,
        protocol,
    )
    .await;
    leave(&mut terminal, enhanced)?;

    let _ = command_tx.send(Command::Quit);
    let _ = watcher.await;
    outcome
}

async fn watch_loop(
    terminal: &mut Terminal<Backend>,
    app: &mut App,
    commands: &mpsc::UnboundedSender<Command>,
    events: &mut mpsc::UnboundedReceiver<Event>,
    protocol: ImageProtocol,
) -> Result<()> {
    let mut keys = spawn_terminal_reader();
    loop {
        present(terminal, app, protocol)?;

        tokio::select! {
            biased;
            event = events.recv() => {
                match event {
                    Some(event) => {
                        apply(app, event);
                        // Drain what is already queued so a burst costs one
                        // redraw rather than one each.
                        while let Ok(event) = events.try_recv() {
                            apply(app, event);
                        }
                    }
                    None => return Ok(()),
                }
            }
            key = keys.recv() => {
                match key {
                    None => return Ok(()),
                    Some(Err(error)) => return Err(Error::io("terminal", error)),
                    Some(Ok(TermEvent::Key(key))) if key.kind == KeyEventKind::Press => {
                        // Ctrl-C closes the pane rather than interrupting, the
                        // opposite of the editing UI, because the cell running
                        // here belongs to somebody else: a reflex keystroke
                        // must not stop an agent's work. `ii` is the
                        // deliberate two-key way to do that, and it is on the
                        // status bar.
                        if key.code == KeyCode::Char('c')
                            && key.modifiers == KeyModifiers::CONTROL
                        {
                            return Ok(());
                        }
                        for command in app.on_key(key) {
                            if matches!(command, Command::Quit) {
                                return Ok(());
                            }
                            let _ = commands.send(command);
                        }
                    }
                    Some(Ok(TermEvent::Mouse(mouse))) => match mouse.kind {
                        // Scrolling by hand means the reader has somewhere of
                        // their own to be.
                        MouseEventKind::ScrollDown => {
                            app.scroll = app.scroll.saturating_add(3);
                            app.following = false;
                        }
                        MouseEventKind::ScrollUp => {
                            app.scroll = app.scroll.saturating_sub(3);
                            app.following = false;
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
            _ = tokio::time::sleep(TICK) => {}
        }
    }
}

/// A watcher applies a new document differently from the editing UI: it has to
/// work out where the writer is before it can follow them.
fn apply(app: &mut App, event: Event) {
    match event {
        Event::Notebook(notebook) => app.on_watched_notebook(*notebook),
        other => app.on_event(other),
    }
}

/// Poll the file and the supervisor, and relay what a watcher may ask for.
async fn watch_source(
    path: PathBuf,
    events: mpsc::UnboundedSender<Event>,
    mut commands: mpsc::UnboundedReceiver<Command>,
) {
    let client = SessionClient::new(&path);
    let mut seen = Snapshot::of(&path);
    let mut file_tick = tokio::time::interval(FILE_POLL);
    let mut status_tick = tokio::time::interval(STATUS_POLL);

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    None | Some(Command::Quit) => return,
                    Some(Command::Interrupt) => {
                        // Through the supervisor's lock-free path, so it
                        // reaches a cell that is holding everything else.
                        match client.request_existing(&Request::Interrupt, |_| {}).await {
                            Ok(_) => {
                                let _ = events.send(Event::Message(
                                    "interrupt sent".to_string(),
                                ));
                            }
                            Err(error) => {
                                let _ = events.send(Event::Error(error.to_string()));
                            }
                        }
                    }
                    // A watcher raises nothing else, and a stray command must
                    // not be mistaken for permission to write.
                    Some(_) => {}
                }
            }
            _ = file_tick.tick() => {
                if let Some(next) = seen.reread(&path) {
                    match next {
                        Ok(notebook) => {
                            let _ = events.send(Event::Notebook(Box::new(notebook)));
                        }
                        // A partially written file cannot be observed (saves
                        // are a rename), so this is a genuinely broken
                        // notebook and worth saying out loud once, when it
                        // changes, rather than every poll.
                        Err(error) => {
                            let _ = events.send(Event::Error(error.to_string()));
                        }
                    }
                }
            }
            _ = status_tick.tick() => {
                let _ = events.send(Event::Status(kernel_status(&client).await));
            }
        }
    }
}

/// What the kernel behind this notebook is doing, if anything is.
async fn kernel_status(client: &SessionClient) -> KernelStatus {
    let answer = tokio::time::timeout(
        STATUS_POLL,
        client.request_existing(&Request::Status, |_| {}),
    )
    .await;
    match answer {
        Ok(Ok(Response::Status(info))) => info.kernel_status,
        // No session, or one that cannot answer in a second. Either way there
        // is no kernel to report on, and the file is still worth watching.
        _ => KernelStatus::Dead,
    }
}

/// The file as last read, and cheap enough to compare every quarter second.
struct Snapshot {
    stamp: Option<(SystemTime, u64)>,
    bytes: Vec<u8>,
}

impl Snapshot {
    fn of(path: &Path) -> Self {
        Snapshot {
            stamp: stamp(path),
            bytes: std::fs::read(path).unwrap_or_default(),
        }
    }

    /// The notebook if the file has changed since the last look.
    ///
    /// Two gates, because they answer different questions: the stamp says "is
    /// it worth reading", and the byte comparison says "did anything actually
    /// change". The second is meaningful because the writer is canonical, so
    /// identical state really does produce identical bytes; without it, a
    /// touch or a rewrite with the same content would move a following
    /// watcher's selection for no reason.
    fn reread(&mut self, path: &Path) -> Option<Result<Notebook>> {
        let stamp = stamp(path);
        if stamp == self.stamp {
            return None;
        }
        self.stamp = stamp;

        let bytes = std::fs::read(path).ok()?;
        if bytes == self.bytes {
            return None;
        }
        self.bytes = bytes;

        let text = String::from_utf8_lossy(&self.bytes);
        Some(Notebook::from_json_str(&text, &path.display().to_string()))
    }
}

fn stamp(path: &Path) -> Option<(SystemTime, u64)> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.modified().ok()?, meta.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::Cell;

    #[test]
    fn a_snapshot_reports_only_real_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nb.ipynb");
        let mut notebook = Notebook::new("python3", "Python 3", "python");
        notebook.push(Cell::new_code("x = 1"));
        notebook.write(&path).unwrap();

        let mut snapshot = Snapshot::of(&path);
        assert!(
            snapshot.reread(&path).is_none(),
            "an unchanged file looked changed"
        );

        // Rewriting identical content changes the stamp but not the notebook,
        // and must not be reported: it would move a following watcher.
        notebook.write(&path).unwrap();
        assert!(
            snapshot.reread(&path).is_none(),
            "an identical rewrite was reported as a change"
        );

        notebook.push(Cell::new_code("y = 2"));
        notebook.write(&path).unwrap();
        let reread = snapshot
            .reread(&path)
            .expect("a real change went unnoticed")
            .expect("the notebook should still parse");
        assert_eq!(reread.len(), 2);
    }

    #[test]
    fn a_broken_notebook_is_reported_rather_than_silently_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nb.ipynb");
        Notebook::new("python3", "Python 3", "python")
            .write(&path)
            .unwrap();
        let mut snapshot = Snapshot::of(&path);

        std::fs::write(&path, "{not json").unwrap();
        let outcome = snapshot.reread(&path).expect("the change went unnoticed");
        assert!(outcome.is_err(), "a broken file parsed");
    }
}
