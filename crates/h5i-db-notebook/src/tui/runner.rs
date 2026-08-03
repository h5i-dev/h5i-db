//! The task that owns the session while the UI draws.
//!
//! The UI never touches the session directly. It sends [`Command`]s and reads
//! [`Event`]s, which keeps the draw loop responsive while a cell runs: the
//! runner is blocked inside `execute` for as long as the cell takes, and the
//! UI is not.
//!
//! Interrupting is the one thing that cannot go through this queue, because
//! the queue is exactly what is blocked. The UI holds an
//! [`InterruptHandle`](crate::kernel::InterruptHandle) instead and calls it
//! directly.

use tokio::sync::mpsc;

use crate::document::{Cell, CellType};
use crate::error::Result;
use crate::kernel::{Kernel, OutputEvent, OutputSink};
use crate::session::Session;
use crate::tui::app::{Command, Event};

/// Own `session` and serve commands until told to quit.
pub async fn run(
    mut session: Session,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::UnboundedSender<Event>,
) {
    // The UI opens with whatever is on disk.
    let _ = events.send(Event::Notebook(Box::new(session.notebook().clone())));

    while let Some(command) = commands.recv().await {
        if matches!(command, Command::Quit) {
            break;
        }
        let outcome = handle(&mut session, command, &events).await;
        if let Err(error) = outcome {
            let _ = events.send(Event::Error(error.to_string()));
        }
        let _ = events.send(Event::Status(session.kernel_status()));
        let _ = events.send(Event::Notebook(Box::new(session.notebook().clone())));
    }

    // Best effort: the user is already gone, and a failure to shut down
    // cleanly must not stop the terminal from being restored.
    let _ = session.save();
    let _ = session.shutdown().await;
}

async fn handle(
    session: &mut Session,
    command: Command,
    events: &mpsc::UnboundedSender<Event>,
) -> Result<()> {
    match command {
        Command::Quit => Ok(()),

        Command::Run(index) => run_cell(session, index, events).await,

        Command::RunAll => {
            for index in session.code_cell_indices() {
                let errored = run_cell(session, index, events).await.is_err()
                    || session
                        .notebook()
                        .get(index)
                        .is_ok_and(|c| c.outputs().iter().any(|o| o.is_error()));
                if errored {
                    let _ = events.send(Event::Message(format!(
                        "stopped at cell {index}; press A again to continue past it"
                    )));
                    break;
                }
            }
            Ok(())
        }

        Command::SetSource(index, source) => {
            let notebook = session.notebook_mut();
            let cell = notebook.get_mut(index)?;
            cell.set_source(source);
            // Outputs describe code that no longer exists.
            cell.clear_outputs();
            session.save()
        }

        Command::Insert { at, kind } => {
            let cell = match kind {
                CellType::Markdown => Cell::new_markdown(""),
                CellType::Raw => Cell::new_raw(""),
                _ => Cell::new_code(""),
            };
            session.notebook_mut().insert(at, cell);
            session.save()
        }

        Command::Delete(index) => {
            session.notebook_mut().remove(index)?;
            session.save()
        }

        Command::Move { from, to } => {
            session.notebook_mut().move_cell(from, to)?;
            session.save()
        }

        Command::ChangeType(index, kind) => {
            let notebook = session.notebook_mut();
            let source = notebook.get(index)?.source().to_string();
            let replacement = match kind {
                CellType::Markdown => Cell::new_markdown(source),
                CellType::Raw => Cell::new_raw(source),
                _ => Cell::new_code(source),
            };
            notebook.cells[index] = replacement;
            session.save()
        }

        Command::ClearOutputs(index) => {
            session.notebook_mut().get_mut(index)?.clear_outputs();
            session.save()
        }

        Command::Restart { clear_outputs } => {
            let _ = events.send(Event::Message("restarting the kernel".to_string()));
            session.restart(clear_outputs).await?;
            let _ = events.send(Event::Message(
                "kernel restarted; all in-memory state is gone".to_string(),
            ));
            Ok(())
        }

        Command::Save => {
            session.save()?;
            let _ = events.send(Event::Message(format!(
                "saved {}",
                session.path().display()
            )));
            Ok(())
        }

        Command::Complete { code, cursor } => {
            session.ensure_kernel().await?;
            // Characters all the way down: the editor counts them, and so do
            // the kernel and the protocol.
            let completions = session.kernel_mut()?.complete(&code, cursor).await?;
            let _ = events.send(Event::Completions {
                matches: completions.matches,
                cursor_start: completions.cursor_start,
                cursor_end: completions.cursor_end,
            });
            Ok(())
        }

        Command::Inspect { code, cursor } => {
            session.ensure_kernel().await?;
            match session.kernel_mut()?.inspect(&code, cursor, 0).await? {
                Some(bundle) => {
                    let text = bundle.text_plain().unwrap_or("(no text form)").to_string();
                    let _ = events.send(Event::Inspection(crate::render::strip_ansi(&text)));
                }
                None => {
                    let _ = events.send(Event::Message("nothing to inspect here".to_string()));
                }
            }
            Ok(())
        }
    }
}

async fn run_cell(
    session: &mut Session,
    index: usize,
    events: &mpsc::UnboundedSender<Event>,
) -> Result<()> {
    let _ = events.send(Event::Started(index));
    let mut sink = ForwardSink(events.clone());
    let result = session.run_cell(index, &mut sink).await;
    match result {
        Ok(outcome) => {
            let _ = events.send(Event::Finished {
                index,
                errored: outcome.is_error(),
                elapsed_ms: outcome.elapsed.as_millis() as u64,
            });
            Ok(())
        }
        Err(error) => {
            // The outputs produced before the failure are already in the
            // notebook, because the session writes them before reporting.
            let _ = events.send(Event::Finished {
                index,
                errored: true,
                elapsed_ms: 0,
            });
            Err(error)
        }
    }
}

/// Forwards output events to the UI as they arrive.
struct ForwardSink(mpsc::UnboundedSender<Event>);

impl OutputSink for ForwardSink {
    fn emit(&mut self, event: OutputEvent) {
        let ui = match event {
            OutputEvent::Output(output) => Event::Output(Box::new(output)),
            OutputEvent::Status(status) => Event::Status(status),
            _ => return,
        };
        let _ = self.0.send(ui);
    }
}

// Cursor offsets are not converted here any more. The editor, the Jupyter
// protocol and both kernels all count characters, so the round trip through
// byte offsets that used to live here only introduced a mismatch: it sent
// ipykernel a cursor past the one it was asked about, and fed the character
// offsets that came back to a byte-indexed slice, which panicked whenever a
// cell held anything outside ASCII.
