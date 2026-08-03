//! The `h5i-nb` command line.
//!
//! Follows the agent contract in `DESIGN.md` §8: `--format table|json`,
//! machine-readable errors on stderr, stable exit codes, no prompts, no TTY
//! assumptions. Commands that only touch the file (`new`, `cells`, `output`,
//! `kernel list`) never start a kernel; the ones that run code go through the
//! session supervisor so state survives between invocations.

use std::io::{IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};

use crate::document::{Notebook, Output};
use crate::error::{Error, Result};
use crate::render::{DigestOptions, ImageWriter, digest, digest_with_images, summarize};
use crate::session::parse_cell_range;
use crate::supervisor::protocol::{
    CellReport, EditRequest, NewCellKind, Request, Response, StreamEvent,
};
use crate::supervisor::{SessionClient, server, socket_path};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    /// Human and agent readable text.
    Table,
    /// One JSON object on stdout.
    Json,
}

#[derive(Parser, Debug)]
#[command(
    name = "h5i-nb",
    about = "In-terminal Jupyter notebooks for h5i-db",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Output format.
    #[arg(long, global = true, value_enum, default_value = "table")]
    pub format: Format,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Create a new notebook.
    New {
        notebook: PathBuf,
        /// Kernel to record in the notebook's metadata.
        #[arg(long, default_value = "python3")]
        kernel: String,
        /// Database that this notebook's `%%sql` cells query by default.
        /// Recorded in the notebook, so it travels with the file.
        #[arg(long)]
        db: Option<String>,
    },

    /// Append a code cell and run it. Reads code from stdin when given `-`.
    Exec {
        notebook: PathBuf,
        /// Code to run, or `-` to read stdin.
        #[arg(long, default_value = "-")]
        code: String,
        /// Seconds before the cell is interrupted. 0 disables the limit.
        #[arg(long, default_value = "300")]
        timeout: u64,
        /// Print output as it arrives rather than only at the end.
        #[arg(long)]
        stream: bool,
        /// Return as soon as the cell is queued. Its outputs are still
        /// recorded, because the session process owns the notebook; poll with
        /// `h5i-nb cells` or `h5i-nb kernel status`.
        #[arg(long)]
        detach: bool,
        /// Do not elide long output.
        #[arg(long)]
        raw: bool,
    },

    /// Run cells that are already in the notebook.
    Run {
        notebook: PathBuf,
        /// Cells to run: `3`, `3-7`, `3-`, `1,4,9`, or `all`.
        #[arg(long, default_value = "all")]
        cells: String,
        #[arg(long, default_value = "300")]
        timeout: u64,
        /// Restart the kernel and clear outputs first, so the run proves the
        /// notebook reproduces from nothing.
        #[arg(long)]
        from_clean: bool,
        /// Keep going after a cell raises.
        #[arg(long)]
        keep_going: bool,
        #[arg(long)]
        raw: bool,
    },

    /// List cells with their type, execution count, and output shape.
    Cells { notebook: PathBuf },

    /// Print one cell's output.
    Output {
        notebook: PathBuf,
        /// Cell index or cell id.
        cell: String,
        /// Print the untruncated output.
        #[arg(long)]
        raw: bool,
        /// Write a binary output (an image) to this path instead of stdout.
        #[arg(long)]
        save: Option<PathBuf>,
        /// Which output of the cell, when it produced several.
        #[arg(long, default_value = "0")]
        index: usize,
    },

    /// Edit the notebook without running anything.
    Edit {
        notebook: PathBuf,
        #[command(subcommand)]
        action: EditAction,
    },

    /// Open the notebook in the terminal UI.
    View { notebook: PathBuf },

    /// Convert the notebook to markdown, a Python script, or standalone HTML.
    Export {
        notebook: PathBuf,
        /// Target format: md, py, or html.
        #[arg(long = "to", default_value = "md")]
        to: String,
        /// Write here instead of stdout.
        #[arg(long, short)]
        output: Option<PathBuf>,
        /// Leave outputs out, for a clean diff or a runnable script.
        #[arg(long)]
        without_outputs: bool,
    },

    /// Ask the kernel what a name is, the way `?` does in IPython.
    Inspect {
        notebook: PathBuf,
        /// Code to introspect; the cursor sits at its end unless --cursor says
        /// otherwise.
        code: String,
        /// Character offset to introspect at.
        #[arg(long)]
        cursor: Option<usize>,
    },

    /// Ask the kernel what completes at the cursor.
    Complete {
        notebook: PathBuf,
        code: String,
        #[arg(long)]
        cursor: Option<usize>,
    },

    /// Manage the kernel behind a notebook.
    Kernel {
        #[command(subcommand)]
        action: KernelAction,
    },

    /// Run a session supervisor in the foreground. Started automatically;
    /// you should not normally need to invoke it.
    #[command(hide = true)]
    Serve {
        notebook: PathBuf,
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Exit after this many seconds with no client attached.
        #[arg(long, default_value = "3600")]
        idle_ttl: u64,
    },
}

#[derive(Subcommand, Debug)]
pub enum EditAction {
    /// Replace a cell's source.
    Set {
        cell: String,
        #[arg(long, default_value = "-")]
        code: String,
    },
    /// Insert a new cell before `at` (or append when omitted).
    Insert {
        #[arg(long)]
        at: Option<usize>,
        #[arg(long, value_enum, default_value = "code")]
        kind: CellKind,
        #[arg(long, default_value = "-")]
        code: String,
    },
    /// Delete a cell.
    Delete { cell: String },
    /// Move a cell to a new position.
    Move { cell: String, to: usize },
    /// Drop every recorded output.
    ClearOutputs,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CellKind {
    Code,
    Markdown,
    Raw,
}

#[derive(Subcommand, Debug)]
pub enum KernelAction {
    /// List installed kernelspecs.
    List,
    /// Start the session (and its kernel) without running anything.
    Start { notebook: PathBuf },
    /// Show whether a session is running, and what its kernel is doing.
    Status { notebook: PathBuf },
    /// Interrupt the running cell, keeping kernel state.
    Interrupt { notebook: PathBuf },
    /// Restart the kernel. All in-memory state is lost.
    Restart {
        notebook: PathBuf,
        /// Also clear every recorded output.
        #[arg(long)]
        clear_outputs: bool,
    },
    /// Stop the session and its kernel.
    Stop { notebook: PathBuf },
}

/// Read cell source from an argument or stdin.
fn read_code(argument: &str) -> Result<String> {
    if argument != "-" {
        return Ok(argument.to_string());
    }
    if std::io::stdin().is_terminal() {
        return Err(Error::invalid(
            "no code given: pass --code \"…\" or pipe it on stdin",
        ));
    }
    let mut buffer = String::new();
    std::io::stdin()
        .read_to_string(&mut buffer)
        .map_err(|e| Error::io("stdin", e))?;
    if buffer.trim().is_empty() {
        return Err(Error::invalid("no code given: stdin was empty"));
    }
    Ok(buffer)
}

fn timeout_arg(seconds: u64) -> Option<u64> {
    Some(seconds)
}

pub async fn run(cli: Cli) -> Result<()> {
    run_command(cli.command, cli.format).await
}

/// Run one notebook command with a caller-supplied output format.
///
/// Separate from [`run`] so the command tree can be mounted inside another
/// CLI that already owns a global `--format`: two global flags with the same
/// name is a clap conflict, and duplicating the flag would let the two
/// disagree.
pub async fn run_command(command: Command, format: Format) -> Result<()> {
    let cli = Cli { command, format };
    match cli.command {
        Command::New {
            notebook,
            kernel,
            db,
        } => {
            let mut session = crate::Session::create(&notebook, &kernel)?;
            if let Some(db) = &db {
                crate::session::set_notebook_database(session.notebook_mut(), db);
                session.save()?;
            }
            emit(
                cli.format,
                serde_json::json!({
                    "notebook": notebook.display().to_string(),
                    "kernel": session.kernel_name(),
                    "database": db,
                }),
                || {
                    let mut line = format!(
                        "created {} with kernel {}",
                        notebook.display(),
                        session.kernel_name()
                    );
                    if let Some(db) = &db {
                        line.push_str(&format!(" and database {db}"));
                    }
                    line
                },
            )
        }

        Command::Exec {
            notebook,
            code,
            timeout,
            stream,
            raw,
            detach,
        } => {
            let code = read_code(&code)?;
            let client = SessionClient::new(&notebook);
            let request = Request::Exec {
                code,
                timeout_secs: timeout_arg(timeout),
                detach,
            };
            let response = client
                .request(&request, |event| print_event(event, stream))
                .await?;
            if let Response::Detached { index, cell_id } = response {
                return emit(
                    cli.format,
                    serde_json::json!({
                        "detached": true,
                        "cell": index,
                        "cell_id": cell_id,
                    }),
                    || {
                        format!(
                            "queued cell {index}; poll with `h5i-nb cells {}`",
                            notebook.display()
                        )
                    },
                );
            }
            // With --stream the body already reached the terminal as it was
            // produced; printing the digest too would duplicate every line.
            report_cells(cli.format, response, raw, stream, &notebook)
        }

        Command::Run {
            notebook,
            cells,
            timeout,
            from_clean,
            keep_going,
            raw,
        } => {
            let client = SessionClient::new(&notebook);
            if from_clean {
                client
                    .request(
                        &Request::Restart {
                            clear_outputs: true,
                        },
                        |_| {},
                    )
                    .await?;
            }
            // The range is resolved against the file so a bad range fails
            // before a kernel is started.
            let total = Notebook::read(&notebook)?.len();
            let indices = parse_cell_range(&cells, total)?;
            let response = client
                .request(
                    &Request::Run {
                        indices,
                        timeout_secs: timeout_arg(timeout),
                        stop_on_error: !keep_going,
                    },
                    |event| print_event(event, false),
                )
                .await?;
            report_cells(cli.format, response, raw, false, &notebook)
        }

        Command::Cells { notebook } => {
            let notebook = Notebook::read(&notebook)?;
            let rows: Vec<serde_json::Value> = notebook
                .cells
                .iter()
                .enumerate()
                .map(|(i, cell)| {
                    serde_json::json!({
                        "index": i,
                        "id": cell.id(),
                        "type": cell.cell_type_str(),
                        "execution_count": cell.as_code().and_then(|c| c.execution_count),
                        "lines": cell.source().lines().count(),
                        "outputs": summarize(cell.outputs()),
                        "preview": first_line(cell.source()),
                    })
                })
                .collect();
            emit(cli.format, serde_json::json!({ "cells": rows }), || {
                render_cell_table(&notebook)
            })
        }

        Command::Output {
            notebook,
            cell,
            raw,
            save,
            index,
        } => {
            let path = notebook;
            let notebook = Notebook::read(&path)?;
            let position = notebook.resolve(&cell)?;
            let outputs = notebook.get(position)?.outputs();
            if let Some(save_to) = save {
                return save_output(outputs, index, position, &save_to);
            }
            let options = DigestOptions {
                raw,
                ..Default::default()
            };
            let writer = ImageWriter::for_notebook(&path);
            let label = notebook
                .get(position)?
                .id()
                .map(str::to_string)
                .unwrap_or_else(|| position.to_string());
            let images = writer.write(outputs, &label);
            let text = digest_with_images(outputs, &options, &images);
            emit(
                cli.format,
                serde_json::json!({
                    "cell": position,
                    "outputs": outputs,
                    "saved": images
                        .iter()
                        .map(|p| p.as_ref().map(|p| p.display().to_string()))
                        .collect::<Vec<_>>(),
                }),
                || text.clone(),
            )
        }

        Command::Export {
            notebook,
            to,
            output,
            without_outputs,
        } => {
            let format = crate::export::Format::parse(&to)?;
            let document = Notebook::read(&notebook)?;
            let text = crate::export::export(
                &document,
                format,
                &crate::export::ExportOptions { without_outputs },
            );
            match output {
                Some(path) => {
                    std::fs::write(&path, &text).map_err(|e| Error::io(path.display(), e))?;
                    emit(
                        cli.format,
                        serde_json::json!({
                            "wrote": path.display().to_string(),
                            "bytes": text.len(),
                            "format": format.extension(),
                        }),
                        || format!("wrote {} ({} bytes)", path.display(), text.len()),
                    )
                }
                None => {
                    // Straight to stdout, unwrapped even under --format json:
                    // the point of exporting is the document itself, and
                    // wrapping it in JSON would mean unwrapping it again.
                    print!("{text}");
                    let _ = std::io::stdout().flush();
                    Ok(())
                }
            }
        }

        Command::Inspect {
            notebook,
            code,
            cursor,
        } => {
            let cursor = cursor.unwrap_or(code.chars().count());
            let client = SessionClient::new(&notebook);
            let response = client
                .request(
                    &Request::Inspect {
                        code,
                        cursor,
                        detail: 0,
                    },
                    |_| {},
                )
                .await?;
            let Response::Inspect { found, text } = response else {
                return Err(Error::protocol("expected an inspect reply"));
            };
            // IPython colours its introspection output. The escapes are pure
            // cost for anything reading rather than displaying it, and the
            // protocol carries the text faithfully so stripping belongs here.
            let text = text.map(|t| crate::render::strip_ansi(&t));
            emit(
                cli.format,
                serde_json::json!({ "found": found, "text": text }),
                || match &text {
                    Some(text) => text.clone(),
                    None => "nothing to inspect there".to_string(),
                },
            )
        }

        Command::Complete {
            notebook,
            code,
            cursor,
        } => {
            let cursor = cursor.unwrap_or(code.chars().count());
            let client = SessionClient::new(&notebook);
            let response = client
                .request(
                    &Request::Complete {
                        code: code.clone(),
                        cursor,
                    },
                    |_| {},
                )
                .await?;
            let Response::Completions {
                matches,
                cursor_start,
                cursor_end,
            } = response
            else {
                return Err(Error::protocol("expected a completion reply"));
            };
            emit(
                cli.format,
                serde_json::json!({
                    "matches": matches,
                    "cursor_start": cursor_start,
                    "cursor_end": cursor_end,
                }),
                || {
                    if matches.is_empty() {
                        "no completions".to_string()
                    } else {
                        matches.join("\n")
                    }
                },
            )
        }

        Command::Edit { notebook, action } => edit(cli.format, notebook, action).await,

        Command::View { notebook } => {
            if !std::io::stdout().is_terminal() {
                return Err(Error::invalid(
                    "`view` needs a terminal; use `cells`, `output`, or `exec` when piping",
                ));
            }
            // The UI owns the session in-process rather than going through the
            // supervisor: a human at a keyboard is one client, and routing
            // every keystroke's completion request through a socket would add
            // latency for nothing. A supervisor already holding this notebook
            // would fight it for the file, so it is stopped first.
            let client = SessionClient::new(&notebook);
            if client.is_running().await {
                client.shutdown().await?;
            }
            crate::tui::run(&notebook).await
        }

        Command::Kernel { action } => kernel(cli.format, action).await,

        Command::Serve {
            notebook,
            socket,
            idle_ttl,
        } => {
            let socket = socket.unwrap_or_else(|| socket_path(&notebook));
            server::serve(
                &notebook,
                server::ServerOptions {
                    socket,
                    idle_ttl: Duration::from_secs(idle_ttl),
                },
            )
            .await
        }
    }
}

fn first_line(source: &str) -> String {
    let line = source.lines().next().unwrap_or("").trim();
    if line.chars().count() > 60 {
        format!("{}…", line.chars().take(59).collect::<String>())
    } else {
        line.to_string()
    }
}

fn render_cell_table(notebook: &Notebook) -> String {
    let mut table = comfy_table::Table::new();
    table.set_header(["#", "id", "type", "exec", "outputs", "source"]);
    table.load_preset(comfy_table::presets::NOTHING);
    for (i, cell) in notebook.cells.iter().enumerate() {
        table.add_row([
            i.to_string(),
            cell.id().unwrap_or("-").to_string(),
            cell.cell_type_str().to_string(),
            cell.as_code()
                .and_then(|c| c.execution_count)
                .map(|c| c.to_string())
                .unwrap_or_else(|| "-".into()),
            summarize(cell.outputs()),
            first_line(cell.source()),
        ]);
    }
    table.to_string()
}

fn save_output(outputs: &[Output], index: usize, cell: usize, path: &PathBuf) -> Result<()> {
    let output = outputs.get(index).ok_or(Error::OutputIndexOutOfRange {
        index: cell,
        output: index,
        len: outputs.len(),
    })?;
    let bundle = output.data().ok_or_else(|| {
        Error::invalid(format!(
            "output {index} of cell {cell} is a {} and has no saveable payload",
            output.output_type()
        ))
    })?;
    // Prefer a binary payload: saving is what you do with an image.
    let binary = ["image/png", "image/jpeg", "application/pdf", "image/gif"]
        .into_iter()
        .find_map(|mime| bundle.text_of(mime).map(|data| (mime, data)));
    let bytes = match binary {
        Some((_, data)) => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(data.trim())
                .map_err(|e| Error::invalid(format!("output is not valid base64: {e}")))?
        }
        None => {
            let mime = bundle
                .richest(&["text/html", "text/plain", "text/markdown"])
                .ok_or_else(|| {
                    Error::invalid(format!(
                        "output {index} of cell {cell} has no saveable representation ({})",
                        bundle.mime_types().collect::<Vec<_>>().join(", ")
                    ))
                })?;
            bundle.text_of(mime).unwrap_or_default().as_bytes().to_vec()
        }
    };
    std::fs::write(path, &bytes).map_err(|e| Error::io(path.display(), e))?;
    println!("wrote {} bytes to {}", bytes.len(), path.display());
    Ok(())
}

async fn edit(format: Format, path: PathBuf, action: EditAction) -> Result<()> {
    // Code is read here, not in the supervisor: `--code -` means this
    // process's stdin, which the supervisor has no way to reach.
    let request = edit_request(action)?;

    // A running supervisor holds the notebook in memory and rewrites the whole
    // file after every cell, so an edit written straight to disk would be
    // silently reverted by the next run. Route it through the owner when there
    // is one, and touch the file directly only when there is not.
    let client = SessionClient::new(&path);
    let wire = Request::Edit {
        action: request.clone(),
    };
    let message = match client.request_existing(&wire, |_| {}).await {
        Ok(Response::Edited { message }) => message,
        Ok(other) => {
            return Err(Error::protocol(format!(
                "the session answered an edit with {other:?}"
            )));
        }
        // Nobody is holding the notebook, so the file is the only copy there
        // is and editing it directly is safe.
        Err(Error::NoSession { .. }) => apply_edit_to_file(&path, &request)?,
        Err(error) => return Err(error),
    };

    emit(
        format,
        serde_json::json!({"notebook": path.display().to_string(), "result": message}),
        || message.clone(),
    )
}

/// Apply an edit to the file, for a notebook nobody is holding.
fn apply_edit_to_file(path: &Path, action: &EditRequest) -> Result<String> {
    let mut notebook = Notebook::read(path)?;
    notebook.normalize();
    let message = crate::session::apply_edit(&mut notebook, action)?;
    notebook.write(path)?;
    Ok(message)
}

/// Resolve a command-line edit into its wire form, reading any piped code.
fn edit_request(action: EditAction) -> Result<EditRequest> {
    Ok(match action {
        EditAction::Set { cell, code } => EditRequest::Set {
            cell,
            source: read_code(&code)?,
        },
        EditAction::Insert { at, kind, code } => EditRequest::Insert {
            at,
            kind: match kind {
                CellKind::Code => NewCellKind::Code,
                CellKind::Markdown => NewCellKind::Markdown,
                CellKind::Raw => NewCellKind::Raw,
            },
            source: read_code(&code)?,
        },
        EditAction::Delete { cell } => EditRequest::Delete { cell },
        EditAction::Move { cell, to } => EditRequest::Move { cell, to },
        EditAction::ClearOutputs => EditRequest::ClearOutputs,
    })
}

async fn kernel(format: Format, action: KernelAction) -> Result<()> {
    match action {
        KernelAction::List => {
            let specs = crate::kernel::discover();
            let rows: Vec<serde_json::Value> = specs
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "name": s.name,
                        "display_name": s.display_name,
                        "language": s.language,
                        "path": s.path.display().to_string(),
                    })
                })
                .collect();
            emit(format, serde_json::json!({ "kernels": rows }), || {
                if specs.is_empty() {
                    "no kernels installed; try `python -m ipykernel install --user`".to_string()
                } else {
                    let mut table = comfy_table::Table::new();
                    table.load_preset(comfy_table::presets::NOTHING);
                    table.set_header(["name", "language", "display name"]);
                    for spec in &specs {
                        table.add_row([&spec.name, &spec.language, &spec.display_name]);
                    }
                    table.to_string()
                }
            })
        }

        KernelAction::Start { notebook } => {
            let client = SessionClient::new(&notebook);
            // Status also starts the supervisor, but the kernel itself is
            // lazy, so run a trivial cell-free request and then force it up.
            client.request(&Request::Status, |_| {}).await?;
            let response = client
                .request(
                    &Request::Complete {
                        code: String::new(),
                        cursor: 0,
                    },
                    |_| {},
                )
                .await;
            // A kernel that cannot start must surface here, not on first exec.
            response?;
            let status = client.request(&Request::Status, |_| {}).await?;
            report_status(format, status)
        }

        KernelAction::Status { notebook } => {
            let client = SessionClient::new(&notebook);
            if !client.is_running().await {
                return emit(
                    format,
                    serde_json::json!({"running": false, "notebook": notebook.display().to_string()}),
                    || format!("no session running for {}", notebook.display()),
                );
            }
            let status = client.request(&Request::Status, |_| {}).await?;
            report_status(format, status)
        }

        KernelAction::Interrupt { notebook } => {
            let client = SessionClient::new(&notebook);
            if !client.is_running().await {
                return Err(Error::NoSession {
                    path: notebook.display().to_string(),
                });
            }
            client.request(&Request::Interrupt, |_| {}).await?;
            emit(format, serde_json::json!({"interrupted": true}), || {
                "interrupted".to_string()
            })
        }

        KernelAction::Restart {
            notebook,
            clear_outputs,
        } => {
            let client = SessionClient::new(&notebook);
            client
                .request(&Request::Restart { clear_outputs }, |_| {})
                .await?;
            emit(format, serde_json::json!({"restarted": true}), || {
                "kernel restarted; all in-memory state is gone".to_string()
            })
        }

        KernelAction::Stop { notebook } => {
            let client = SessionClient::new(&notebook);
            let stopped = client.shutdown().await?;
            emit(format, serde_json::json!({"stopped": stopped}), || {
                if stopped {
                    "session stopped".to_string()
                } else {
                    "no session was running".to_string()
                }
            })
        }
    }
}

fn report_status(format: Format, response: Response) -> Result<()> {
    let Response::Status(info) = response else {
        return Err(Error::protocol("expected a status reply"));
    };
    emit(
        format,
        serde_json::to_value(&info).map_err(Error::internal)?,
        || {
            format!(
                "{} · kernel {} ({}) · {} cells · pid {} · idle {}s",
                info.notebook,
                info.kernel_name,
                info.kernel_status.as_str(),
                info.cells,
                info.pid,
                info.idle_seconds
            )
        },
    )
}

fn report_cells(
    format: Format,
    response: Response,
    raw: bool,
    body_already_streamed: bool,
    notebook: &Path,
) -> Result<()> {
    let Response::Result { cells } = response else {
        return Err(Error::protocol("expected a result reply"));
    };
    let options = DigestOptions {
        raw,
        ..Default::default()
    };

    // Figures are written next to the notebook and referred to by path. The
    // base64 never reaches the terminal either way; this is what lets the
    // reader actually look at the picture afterwards.
    let writer = ImageWriter::for_notebook(notebook);
    let images: Vec<Vec<Option<PathBuf>>> = cells
        .iter()
        .map(|cell| writer.write(&cell.outputs, &cell_label(cell)))
        .collect();

    if format == Format::Json {
        let saved: Vec<Vec<Option<String>>> = images
            .iter()
            .map(|cell| {
                cell.iter()
                    .map(|p| p.as_ref().map(|p| p.display().to_string()))
                    .collect()
            })
            .collect();
        emit(
            format,
            serde_json::json!({ "cells": cells, "saved": saved }),
            String::new,
        )?;
    } else {
        let mut out = String::new();
        for (cell, images) in cells.iter().zip(&images) {
            out.push_str(&render_cell_report(
                cell,
                &options,
                body_already_streamed,
                images,
            ));
        }
        print!("{out}");
        let _ = std::io::stdout().flush();
    }

    // Exit non-zero when a cell raised, so a shell `&&` chain stops and a
    // supervising agent sees the failure without parsing stdout. Checked for
    // both formats: an exit code that depends on --format would be a trap.
    if let Some(failed) = cells
        .iter()
        .find(|c| c.status == crate::kernel::ExecStatus::Error)
    {
        return Err(Error::CellRaised {
            index: failed.index,
            ename: failed
                .outputs
                .iter()
                .find_map(|o| match o {
                    Output::Error(e) => Some(e.ename.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "an exception".into()),
        });
    }
    Ok(())
}

/// A stable, filesystem-safe name for a cell, for attachment file names.
fn cell_label(cell: &CellReport) -> String {
    cell.cell_id
        .clone()
        .unwrap_or_else(|| cell.index.to_string())
}

fn render_cell_report(
    cell: &CellReport,
    options: &DigestOptions,
    header_only: bool,
    images: &[Option<PathBuf>],
) -> String {
    let body = if header_only {
        // Errors are the exception: a traceback goes to the notebook and to
        // the terminal digest, not to the incremental stream, so suppressing
        // it here would lose it entirely.
        digest(
            &cell
                .outputs
                .iter()
                .filter(|o| o.is_error())
                .cloned()
                .collect::<Vec<_>>(),
            options,
        )
    } else {
        digest_with_images(&cell.outputs, options, images)
    };
    let header = format!(
        "[{}] cell {} · {} · {}ms\n",
        cell.execution_count
            .map(|c| c.to_string())
            .unwrap_or_else(|| "-".into()),
        cell.index,
        cell.status.as_str(),
        cell.elapsed_ms
    );
    if body.trim().is_empty() {
        header
    } else if body.ends_with('\n') {
        format!("{header}{body}")
    } else {
        format!("{header}{body}\n")
    }
}

fn print_event(response: &Response, stream: bool) {
    if !stream {
        return;
    }
    if let Response::Event(event) = response {
        match event {
            StreamEvent::Stdout { text } => {
                print!("{text}");
                let _ = std::io::stdout().flush();
            }
            StreamEvent::Stderr { text } => {
                eprint!("{text}");
                let _ = std::io::stderr().flush();
            }
            _ => {}
        }
    }
}

fn emit(format: Format, json: serde_json::Value, text: impl FnOnce() -> String) -> Result<()> {
    match format {
        Format::Json => {
            let line = serde_json::to_string(&json).map_err(Error::internal)?;
            println!("{line}");
        }
        Format::Table => {
            let text = text();
            if !text.is_empty() {
                println!("{}", text.trim_end());
            }
        }
    }
    Ok(())
}

/// Write the error envelope and return the process exit code.
///
/// Same shape as the rest of the `h5i-db` CLI: agents parse this, so the
/// fields and the exit codes are part of the contract.
pub fn report_error(error: &Error, format: Format) -> i32 {
    let envelope = serde_json::json!({
        "code": error.code(),
        "message": error.to_string(),
        "retryable": error.retryable(),
        "hint": error.hint(),
    });
    match format {
        Format::Json => {
            eprintln!("{envelope}");
        }
        Format::Table => {
            eprintln!("error[{}]: {}", error.code(), error);
            if let Some(hint) = error.hint() {
                eprintln!("hint: {hint}");
            }
        }
    }
    error.exit_category().code()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn the_command_tree_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn exec_defaults_to_reading_stdin_with_a_five_minute_cap() {
        let cli = Cli::try_parse_from(["h5i-nb", "exec", "nb.ipynb"]).unwrap();
        match cli.command {
            Command::Exec { code, timeout, .. } => {
                assert_eq!(code, "-");
                assert_eq!(timeout, 300);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn run_defaults_to_every_cell_and_stops_on_error() {
        let cli = Cli::try_parse_from(["h5i-nb", "run", "nb.ipynb"]).unwrap();
        match cli.command {
            Command::Run {
                cells, keep_going, ..
            } => {
                assert_eq!(cells, "all");
                assert!(
                    !keep_going,
                    "runs must stop at the first failure by default"
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn format_is_global_and_defaults_to_table() {
        let cli = Cli::try_parse_from(["h5i-nb", "cells", "nb.ipynb", "--format", "json"]).unwrap();
        assert_eq!(cli.format, Format::Json);
        let cli = Cli::try_parse_from(["h5i-nb", "cells", "nb.ipynb"]).unwrap();
        assert_eq!(cli.format, Format::Table);
    }

    #[test]
    fn a_literal_code_argument_bypasses_stdin() {
        assert_eq!(read_code("1 + 1").unwrap(), "1 + 1");
    }

    #[test]
    fn long_source_previews_are_cut_to_one_line() {
        let preview = first_line("first line\nsecond line\n");
        assert_eq!(preview, "first line");
        let long = first_line(&"x".repeat(200));
        assert!(long.chars().count() <= 60, "{}", long.chars().count());
        assert!(long.ends_with('…'));
    }

    #[test]
    fn the_error_envelope_carries_the_contract() {
        let error = Error::KernelNotFound {
            name: "python3".into(),
            listed: String::new(),
        };
        assert_eq!(report_error(&error, Format::Json), 2);
        assert_eq!(
            report_error(
                &Error::KernelDied {
                    detail: String::new()
                },
                Format::Table
            ),
            5
        );
    }
}
