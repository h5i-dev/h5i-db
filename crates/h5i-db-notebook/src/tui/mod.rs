//! The in-terminal notebook UI.
//!
//! Three pieces, kept apart on purpose: [`app`] holds the state and turns keys
//! into commands (pure, and tested without a terminal), [`render`] draws that
//! state (tested against ratatui's `TestBackend`), and [`runner`] owns the
//! session on its own task so a twenty-minute cell never freezes the screen.

pub mod app;
pub mod editor;
pub mod highlight;
pub mod image;
pub mod render;
pub mod runner;

use std::io::{self, Write};
use std::path::Path;
use std::time::Duration;

use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event as TermEvent, KeyCode, KeyEventKind,
    KeyModifiers, KeyboardEnhancementFlags, MouseEventKind, PopKeyboardEnhancementFlags,
    PushKeyboardEnhancementFlags,
};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::{execute, queue};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::kernel::InterruptHandle;
use crate::session::Session;
use crate::tui::app::{App, Command, Event};
use crate::tui::image::ImageProtocol;

/// How often the UI redraws when nothing has happened.
///
/// Only needed so a kernel status change with no accompanying key press still
/// reaches the screen promptly; everything else redraws on its own event.
const TICK: Duration = Duration::from_millis(120);

/// Open `path` in the terminal UI and run until the user quits.
pub async fn run(path: impl AsRef<Path>) -> Result<()> {
    let session = Session::open(path.as_ref())?;
    let notebook = session.notebook().clone();
    let interrupt = session.interrupt_handle();

    let (command_tx, command_rx) = mpsc::unbounded_channel::<Command>();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Event>();
    let runner = tokio::spawn(runner::run(session, command_rx, event_tx));

    let mut app = App::new(path.as_ref().to_path_buf(), notebook);
    let protocol = ImageProtocol::detect();

    let (mut terminal, enhanced) = enter()?;
    // The terminal must be restored whatever happens, so the loop's result is
    // captured and the teardown runs unconditionally.
    let outcome = event_loop(
        &mut terminal,
        &mut app,
        &command_tx,
        &mut event_rx,
        &interrupt,
        protocol,
    )
    .await;
    leave(&mut terminal, enhanced)?;

    let _ = command_tx.send(Command::Quit);
    let _ = runner.await;
    outcome
}

type Backend = CrosstermBackend<io::Stdout>;

fn enter() -> Result<(Terminal<Backend>, bool)> {
    enable_raw_mode().map_err(|e| Error::io("terminal", e))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)
        .map_err(|e| Error::io("terminal", e))?;

    // Without this, Shift+Enter and Ctrl+Enter arrive as a plain Enter: a
    // terminal cannot express them in the legacy encoding at all. Terminals
    // that implement the kitty keyboard protocol can, so ask for it and fall
    // back to the `e` / `E` bindings everywhere else.
    let enhanced = crossterm::terminal::supports_keyboard_enhancement().unwrap_or(false);
    if enhanced {
        let _ = execute!(
            stdout,
            PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
        );
    }
    let terminal =
        Terminal::new(CrosstermBackend::new(stdout)).map_err(|e| Error::io("terminal", e))?;
    Ok((terminal, enhanced))
}

fn leave(terminal: &mut Terminal<Backend>, enhanced: bool) -> Result<()> {
    if enhanced {
        let _ = execute!(terminal.backend_mut(), PopKeyboardEnhancementFlags);
    }
    disable_raw_mode().map_err(|e| Error::io("terminal", e))?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )
    .map_err(|e| Error::io("terminal", e))?;
    terminal.show_cursor().map_err(|e| Error::io("terminal", e))
}

async fn event_loop(
    terminal: &mut Terminal<Backend>,
    app: &mut App,
    commands: &mpsc::UnboundedSender<Command>,
    events: &mut mpsc::UnboundedReceiver<Event>,
    interrupt: &InterruptHandle,
    protocol: ImageProtocol,
) -> Result<()> {
    loop {
        let mut result = render::DrawResult::default();
        terminal
            .draw(|frame| {
                result = render::draw(frame, app);
            })
            .map_err(|e| Error::io("terminal", e))?;

        // Images are written over the frame ratatui just painted, because they
        // are escape sequences its buffer knows nothing about.
        if protocol.is_supported() && !result.images.is_empty() {
            let mut out = io::stdout();
            for pending in &result.images {
                let _ = image::draw_at(
                    &mut out,
                    protocol,
                    &pending.base64,
                    pending.area.x,
                    pending.area.y,
                    pending.area.height,
                    pending.area.width,
                );
            }
        }
        match result.cursor {
            Some((x, y)) => {
                terminal.show_cursor().ok();
                let _ = queue!(io::stdout(), crossterm::cursor::MoveTo(x, y));
                let _ = io::stdout().flush();
            }
            None => {
                terminal.hide_cursor().ok();
            }
        }

        tokio::select! {
            biased;
            event = events.recv() => {
                match event {
                    Some(event) => {
                        app.on_event(event);
                        // Drain what is already queued so a burst of output
                        // costs one redraw rather than one each.
                        while let Ok(event) = events.try_recv() {
                            app.on_event(event);
                        }
                    }
                    None => return Ok(()),
                }
            }
            key = read_terminal_event() => {
                match key? {
                    Some(TermEvent::Key(key)) if key.kind == KeyEventKind::Press => {
                        // Ctrl-C interrupts the cell rather than killing the
                        // UI. The notebook is the state; losing it to a reflex
                        // keystroke would be unforgivable.
                        if key.code == KeyCode::Char('c')
                            && key.modifiers == KeyModifiers::CONTROL
                        {
                            interrupt.interrupt();
                            app.message = "interrupting".to_string();
                            continue;
                        }
                        // `ii` in command mode means the same thing, and has
                        // to be caught here because the runner is busy.
                        let interrupting =
                            app.pending == Some('i') && key.code == KeyCode::Char('i');
                        for command in app.on_key(key) {
                            if matches!(command, Command::Quit) {
                                return Ok(());
                            }
                            let _ = commands.send(command);
                        }
                        if interrupting {
                            interrupt.interrupt();
                        }
                    }
                    Some(TermEvent::Mouse(mouse)) => match mouse.kind {
                        MouseEventKind::ScrollDown => app.scroll = app.scroll.saturating_add(3),
                        MouseEventKind::ScrollUp => app.scroll = app.scroll.saturating_sub(3),
                        _ => {}
                    },
                    _ => {}
                }
            }
            _ = tokio::time::sleep(TICK) => {}
        }
    }
}

/// Read one terminal event without blocking the async runtime.
///
/// crossterm's reader is synchronous, so it is polled on the blocking pool;
/// polling it inline would stall every other task, including the one carrying
/// output from the kernel.
async fn read_terminal_event() -> Result<Option<TermEvent>> {
    tokio::task::spawn_blocking(|| {
        if crossterm::event::poll(Duration::from_millis(50))? {
            crossterm::event::read().map(Some)
        } else {
            Ok(None)
        }
    })
    .await
    .map_err(|e| Error::internal(format!("terminal reader panicked: {e}")))?
    .map_err(|e| Error::io("terminal", e))
}
