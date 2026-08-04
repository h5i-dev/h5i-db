//! Drawing the notebook.
//!
//! The whole notebook is flattened into one list of styled lines and then
//! windowed by a scroll offset. A notebook is a single vertical document, so a
//! flat line buffer is both the simplest model and the one that makes scrolling
//! behave the way a reader expects; per-cell widgets would each need their own
//! scroll state and could not scroll as one.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui_image::Image;

use crate::document::{CellType, Output};
use crate::kernel::KernelStatus;
use crate::render::{DigestOptions, collapse_carriage_returns, strip_ansi};
use crate::tui::app::{App, Mode, WATCH_HELP, help_rows};
use crate::tui::highlight::{Language, Token, highlight_line};
use crate::tui::image::Images;

/// An image this draw laid out, and whether it made it onto the screen.
///
/// Reported rather than acted on: the drawing already happened, and a caller
/// (or a test) that wants to know what the frame contains should not have to
/// re-derive it.
#[derive(Debug, Clone)]
pub struct PendingImage {
    pub base64: String,
    pub area: Rect,
    /// False when the terminal cannot draw it, or when the rows it needs are
    /// half off the screen and the protocol cannot be clipped. Either way the
    /// reader sees the labelled placeholder instead.
    pub drawn: bool,
}

/// What one draw produced, beyond the frame itself.
#[derive(Debug, Default)]
pub struct DrawResult {
    pub images: Vec<PendingImage>,
    /// Where to put the terminal cursor, when editing.
    pub cursor: Option<(u16, u16)>,
    /// Total content lines, so the caller can clamp scrolling.
    pub content_height: usize,
}

const GUTTER: usize = 7;

pub fn draw(frame: &mut Frame, app: &mut App, images: &mut Images) -> DrawResult {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(frame.area());

    // An overlay is decided before the cells are drawn rather than after,
    // because images now go into the frame as it is built. Clearing them
    // afterwards would be too late: the escape sequence would already be in
    // the buffer, and an overlay that does not happen to cover every row of a
    // plot would leave half of it showing through the panel.
    let mut suppressed = Images::disabled();
    let overlaid = app.inspection.is_some() || app.show_help;
    let for_cells = if overlaid { &mut suppressed } else { images };

    draw_header(frame, chunks[0], app);
    let result = draw_cells(frame, chunks[1], app, for_cells);
    draw_status(frame, chunks[2], app);

    if let Some(text) = app.inspection.clone() {
        draw_overlay(frame, frame.area(), "inspect  (any key closes)", &text);
    }
    if app.show_help {
        let rows = if app.read_only {
            WATCH_HELP.to_vec()
        } else {
            help_rows(app.enhanced_keys)
        };
        let text = rows
            .iter()
            .map(|(keys, what)| format!("{keys:<16}{what}"))
            .collect::<Vec<_>>()
            .join("\n");
        draw_overlay(frame, frame.area(), "keys  (any key closes)", &text);
    }
    if app.completions.is_some() {
        draw_completions(frame, chunks[1], app, result.cursor);
    }
    result
}

fn draw_header(frame: &mut Frame, area: Rect, app: &App) {
    let name = app
        .path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| app.path.display().to_string());
    let kernel = app.notebook.kernel_name().unwrap_or("no kernel");
    let (symbol, colour, label) = status_badge(app.status);
    let line = Line::from(vec![
        Span::styled(
            format!(" {name} "),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("· {} cells ", app.cell_count()),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(format!("· {kernel} "), Style::default().fg(Color::Cyan)),
        Span::styled(format!("{symbol} {label}"), Style::default().fg(colour)),
    ]);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(Color::Rgb(30, 30, 40))),
        area,
    );
}

/// Badge, colour, and label. The label is not `KernelStatus::as_str`: that is
/// the stable machine value the CLI emits, and "dead" reads as "it crashed"
/// where the TUI mostly means "not started yet".
fn status_badge(status: KernelStatus) -> (&'static str, Color, &'static str) {
    match status {
        KernelStatus::Idle => ("●", Color::Green, "idle"),
        KernelStatus::Busy => ("●", Color::Yellow, "busy"),
        KernelStatus::Starting => ("◐", Color::Yellow, "starting"),
        KernelStatus::Restarting => ("◐", Color::Yellow, "restarting"),
        KernelStatus::Dead => ("○", Color::DarkGray, "not running"),
    }
}

fn draw_status(frame: &mut Frame, area: Rect, app: &App) {
    let mode = match (app.read_only, app.mode) {
        // A watcher's badge outranks the mode: the first thing to know about a
        // pane is whether typing in it can change anything.
        (true, _) => ("WATCH", Color::Magenta),
        (false, Mode::Command) => ("COMMAND", Color::Blue),
        (false, Mode::Edit) => ("EDIT", Color::Green),
    };
    let pending = match app.pending {
        Some(c) => format!(" {c}…"),
        None => String::new(),
    };
    // The run hint names the key this terminal can actually send: on anything
    // without the kitty keyboard protocol, Shift+Enter arrives as a plain
    // Enter and opens the editor instead.
    let hint = match (app.read_only, app.mode, app.enhanced_keys) {
        (true, _, _) if app.following => "following  f unfollow  ii interrupt  ? keys  q quit",
        (true, _, _) => "f follow  j/k move  ii interrupt  ? keys  q quit",
        (false, Mode::Command, true) => "⏎ edit  ⇧⏎ run  a/b insert  dd delete  ? keys  q quit",
        (false, Mode::Command, false) => "⏎ edit  e run  a/b insert  dd delete  ? keys  q quit",
        (false, Mode::Edit, true) => "esc done  ⇧⏎ run  tab complete  ⇧tab dedent  ⌥i inspect",
        (false, Mode::Edit, false) => {
            "esc done, then e runs  tab complete  ⇧tab dedent  ⌥i inspect"
        }
    };
    let line = Line::from(vec![
        Span::styled(
            format!(" {} ", mode.0),
            Style::default()
                .bg(mode.1)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(
                " {}/{}{pending} ",
                app.selected + 1,
                app.cell_count().max(1)
            ),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(if app.message.is_empty() {
            hint.to_string()
        } else {
            app.message.clone()
        }),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

/// Build every line of the notebook, then render the visible window.
fn draw_cells(frame: &mut Frame, area: Rect, app: &mut App, images: &mut Images) -> DrawResult {
    let language_hint = app.language().map(str::to_string);
    let width = area.width.saturating_sub(GUTTER as u16 + 1) as usize;
    let mut lines: Vec<Line<'static>> = Vec::new();
    // Line number, payload, and the rows it was given, which depend on the
    // image's own shape and so cannot be recomputed from the line alone.
    let mut placements: Vec<(usize, String, u16)> = Vec::new();
    // Screen row of the cursor, once the scroll offset is known.
    let mut cursor_line: Option<(usize, usize)> = None;
    let mut selected_start = 0usize;
    let mut selected_end = 0usize;

    for index in 0..app.cell_count() {
        let selected = index == app.selected;
        if selected {
            selected_start = lines.len();
        }
        let Ok(cell) = app.notebook.get(index) else {
            continue;
        };
        let kind = cell.cell_type();
        let count = cell
            .as_code()
            .and_then(|c| c.execution_count)
            .map(|c| c.to_string())
            .unwrap_or_else(|| {
                if app.running == Some(index) {
                    "*".into()
                } else {
                    " ".into()
                }
            });

        let source = app.displayed_source(index);
        let language = Language::for_cell(&source, language_hint.as_deref());
        let bar = if selected { "┃" } else { "│" };
        let bar_style = Style::default().fg(if selected {
            Color::Cyan
        } else {
            Color::DarkGray
        });

        for (row, text) in source.split('\n').enumerate() {
            let label = if row == 0 {
                match kind {
                    CellType::Code => format!("[{count:>3}] "),
                    CellType::Markdown => "  md  ".to_string(),
                    CellType::Raw => " raw  ".to_string(),
                    CellType::Unknown => "  ?   ".to_string(),
                }
            } else {
                " ".repeat(GUTTER - 1)
            };
            let mut spans = vec![
                Span::styled(label, Style::default().fg(Color::DarkGray)),
                Span::styled(bar.to_string(), bar_style),
            ];
            spans.extend(style_source_line(text, kind, language));
            if selected
                && let Some(editor) = app.editor.as_ref()
                && editor.cursor().line == row
            {
                cursor_line = Some((lines.len(), editor.cursor().column));
            }
            lines.push(Line::from(spans));
        }

        for output in app.displayed_outputs(index) {
            render_output(output, width, &mut lines, &mut placements, images);
        }
        if selected {
            selected_end = lines.len();
        }
        lines.push(Line::from(""));
    }

    // Keep the selection (or the cursor) on screen.
    let height = (area.height as usize).max(1);
    let focus_start = cursor_line.map(|(l, _)| l).unwrap_or(selected_start);
    let focus_end = cursor_line
        .map(|(l, _)| l + 1)
        .unwrap_or(selected_end.max(focus_start + 1));
    // Showing the whole focus means scrolling no further down than its start
    // and no further up than its end minus a screen. A focus taller than the
    // screen cannot satisfy both, and asking for both is what made the view
    // flip between the two every frame: for that case, keep the scroll
    // anywhere inside the focus instead, which also leaves the wheel usable
    // within a long cell.
    let (lowest, highest) = if focus_end.saturating_sub(focus_start) <= height {
        (focus_end.saturating_sub(height), focus_start)
    } else {
        (focus_start, focus_end.saturating_sub(height))
    };
    app.scroll = app.scroll.clamp(lowest, highest);
    app.scroll = app.scroll.min(lines.len().saturating_sub(1));

    let visible: Vec<Line<'static>> = lines
        .iter()
        .skip(app.scroll)
        .take(height)
        .cloned()
        .collect();
    frame.render_widget(Paragraph::new(visible), area);

    let mut pending: Vec<PendingImage> = Vec::new();
    for (line, base64, rows) in placements {
        // Scrolled past the top, or not reached yet.
        let Some(row) = line.checked_sub(app.scroll) else {
            continue;
        };
        if row >= height {
            continue;
        }
        // The box the image was encoded for is the full one, even when the
        // bottom of it is off the screen. Encoding for the visible part
        // instead would re-encode every image on every scrolled row, and a
        // sixel encode is a decode plus a colour quantisation.
        let full = Rect {
            x: area.x + GUTTER as u16,
            y: area.y + row as u16,
            width: area.width.saturating_sub(GUTTER as u16),
            height: rows,
        };
        let visible = full.intersection(area);
        let drawn = match place(frame, images, &base64, full, visible) {
            true => true,
            false => {
                label_image(frame, &base64, visible);
                false
            }
        };
        pending.push(PendingImage {
            base64,
            area: full,
            drawn,
        });
    }

    let cursor = cursor_line.and_then(|(line, column)| {
        let row = line.checked_sub(app.scroll)?;
        (row < height).then(|| {
            (
                area.x + GUTTER as u16 + column.min(width) as u16,
                area.y + row as u16,
            )
        })
    });

    DrawResult {
        images: pending,
        cursor,
        content_height: lines.len(),
    }
}

/// Put one image on the frame, and say whether it went.
///
/// `full` is the box it was encoded for and `visible` the part of that box
/// still on screen. Kitty and half-blocks can be cut off mid-image and are let
/// through; sixel and iTerm2 draw a whole image or nothing, so a clipped one
/// is refused here rather than allowed to spill over the status bar.
fn place(frame: &mut Frame, images: &mut Images, base64: &str, full: Rect, visible: Rect) -> bool {
    if visible.width == 0 || visible.height == 0 {
        return false;
    }
    let Some(protocol) = images.protocol(base64, full) else {
        return false;
    };
    if protocol.needs_placeholder(visible).is_some() {
        return false;
    }
    frame.render_widget(Image::new(protocol).allow_clipping(true), visible);
    true
}

/// What a reader sees where an image could not be drawn.
fn label_image(frame: &mut Frame, base64: &str, visible: Rect) {
    if visible.width == 0 || visible.height == 0 {
        return;
    }
    let text = format!("[image/png · {} bytes]", base64.len());
    let line = Rect {
        height: 1,
        ..visible
    };
    frame.render_widget(
        Paragraph::new(text).style(Style::default().fg(Color::DarkGray)),
        line,
    );
}

fn style_source_line(text: &str, kind: CellType, language: Language) -> Vec<Span<'static>> {
    if kind != CellType::Code {
        return vec![Span::styled(
            text.to_string(),
            Style::default().fg(Color::Rgb(160, 190, 160)),
        )];
    }
    highlight_line(text, language)
        .into_iter()
        .map(|(start, end, token)| Span::styled(text[start..end].to_string(), token_style(token)))
        .collect()
}

fn token_style(token: Token) -> Style {
    match token {
        Token::Keyword => Style::default().fg(Color::Magenta),
        Token::Builtin => Style::default().fg(Color::Cyan),
        Token::String => Style::default().fg(Color::Green),
        Token::Comment => Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC),
        Token::Number => Style::default().fg(Color::Yellow),
        Token::Magic => Style::default()
            .fg(Color::Blue)
            .add_modifier(Modifier::BOLD),
        Token::Definition => Style::default().fg(Color::LightYellow),
        Token::Plain => Style::default(),
    }
}

/// Turn one output into lines, collecting any image for later drawing.
fn render_output(
    output: &Output,
    width: usize,
    lines: &mut Vec<Line<'static>>,
    placements: &mut Vec<(usize, String, u16)>,
    images: &mut Images,
) {
    let prefix = || Span::styled(" ".repeat(GUTTER), Style::default());
    let push = |text: String, style: Style, lines: &mut Vec<Line<'static>>| {
        lines.push(Line::from(vec![prefix(), Span::styled(text, style)]));
    };

    match output {
        Output::Stream(stream) => {
            let style = if stream.name == crate::document::StreamName::Stderr {
                Style::default().fg(Color::Red)
            } else {
                Style::default().fg(Color::Gray)
            };
            for line in collapse_carriage_returns(&stream.text).lines() {
                push(truncate(line, width), style, lines);
            }
        }
        Output::Error(error) => {
            let body = if error.traceback.is_empty() {
                vec![format!("{}: {}", error.ename, error.evalue)]
            } else {
                error.traceback.iter().map(|l| strip_ansi(l)).collect()
            };
            for line in body.iter().flat_map(|l| l.split('\n')) {
                push(
                    truncate(line, width),
                    Style::default().fg(Color::LightRed),
                    lines,
                );
            }
        }
        Output::ExecuteResult(_) | Output::DisplayData(_) => {
            let Some(bundle) = output.data() else { return };
            if let Some(base64) = bundle.text_of("image/png") {
                // How tall the image really is, so the rows reserved here and
                // the rows it is drawn into are the same rows: reserving a
                // fixed block would leave a gap under a wide plot and squash
                // a tall one.
                let rows = images.rows_for(base64, width as u16);
                placements.push((lines.len(), base64.to_string(), rows));
                // The rows are held open with blanks whether or not the image
                // can be drawn. The placeholder is written over them at draw
                // time, once it is known which it is.
                for _ in 0..rows {
                    push(String::new(), Style::default(), lines);
                }
                return;
            }
            let text = bundle
                .richest(&["text/plain", "text/markdown"])
                .and_then(|m| bundle.text_of(m))
                .map(strip_ansi)
                .unwrap_or_else(|| {
                    format!("[{}]", bundle.mime_types().collect::<Vec<_>>().join(", "))
                });
            for line in text.lines() {
                push(
                    truncate(line, width),
                    Style::default().fg(Color::White),
                    lines,
                );
            }
        }
        Output::Unknown(_) => push(
            format!("[{} output]", output.output_type()),
            Style::default().fg(Color::DarkGray),
            lines,
        ),
    }
}

/// Cut a line to the visible width, counting characters rather than bytes.
fn truncate(line: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if line.chars().count() <= width {
        return line.to_string();
    }
    let mut out: String = line.chars().take(width.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn draw_completions(frame: &mut Frame, area: Rect, app: &App, cursor: Option<(u16, u16)>) {
    let Some(popup) = app.completions.as_ref() else {
        return;
    };
    let height = (popup.matches.len().min(8) + 2) as u16;
    let width = popup
        .matches
        .iter()
        .map(|m| m.chars().count())
        .max()
        .unwrap_or(10)
        .clamp(12, 48) as u16
        + 2;
    let width = width.min(area.width);
    let height = height.min(area.height);
    if width == 0 || height == 0 {
        return;
    }

    // Anchored under the cursor, and flipped above it when there is no room
    // below, so the candidate list never covers the word being completed.
    let (anchor_x, anchor_y) = cursor.unwrap_or((area.x + GUTTER as u16, area.y));
    let x = anchor_x.min(area.x + area.width.saturating_sub(width));
    let below = anchor_y.saturating_add(1);
    let y = if below + height <= area.y + area.height {
        below
    } else {
        anchor_y.saturating_sub(height).max(area.y)
    };
    let popup_area = Rect {
        x,
        y,
        width,
        height,
    };
    let visible = popup.selected.saturating_sub(7);
    let lines: Vec<Line> = popup
        .matches
        .iter()
        .enumerate()
        .skip(visible)
        .take(8)
        .map(|(index, text)| {
            let style = if index == popup.selected {
                Style::default().bg(Color::Blue).fg(Color::White)
            } else {
                Style::default()
            };
            Line::from(Span::styled(text.clone(), style))
        })
        .collect();
    frame.render_widget(Clear, popup_area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" {} matches ", popup.matches.len())),
        ),
        popup_area,
    );
}

fn draw_overlay(frame: &mut Frame, area: Rect, title: &str, body: &str) {
    // Clamped to the area *after* applying the minimum: on a terminal narrower
    // than the minimum, an unclamped centring calculation underflows.
    let width = area
        .width
        .saturating_sub(area.width / 6)
        .max(20)
        .min(area.width);
    let height = area
        .height
        .saturating_sub(area.height / 4)
        .max(6)
        .min(area.height);
    if width == 0 || height == 0 {
        return;
    }
    let overlay = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    };
    frame.render_widget(Clear, overlay);
    frame.render_widget(
        Paragraph::new(body.to_string())
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!(" {title} ")),
            ),
        overlay,
    );
}

/// Options the TUI renders outputs with. Wider than the CLI's, because a
/// screen scrolls and a transcript does not.
pub fn tui_digest_options() -> DigestOptions {
    DigestOptions {
        head_lines: 200,
        tail_lines: 50,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{Cell, MimeBundle, Notebook, StreamName};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use serde_json::json;

    fn app_with(cells: Vec<Cell>) -> App {
        let mut notebook = Notebook::new("python3", "Python 3", "python");
        for cell in cells {
            notebook.push(cell);
        }
        App::new(std::path::PathBuf::from("t.ipynb"), notebook)
    }

    /// Render and return the screen as text.
    fn screen(app: &mut App, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| {
                // Disabled rather than detected: a test process has no
                // terminal to query, and one that did would have its stdin
                // read out from under the harness.
                draw(frame, app, &mut Images::disabled());
            })
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol().to_string())
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn a_cell_taller_than_the_screen_does_not_oscillate() {
        // Keeping both ends of the focus on screen is impossible when the
        // focus is taller than the screen, and asking for both made the view
        // flip between the top of the cell and the end of its outputs on
        // every frame, roughly twenty times a second.
        let mut app = app_with(vec![
            Cell::new_code("first"),
            Cell::new_code(
                (0..40)
                    .map(|i| format!("line {i}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
        ]);
        app.selected = 1;

        let first = screen(&mut app, 40, 12);
        let settled = app.scroll;
        let second = screen(&mut app, 40, 12);
        assert_eq!(
            app.scroll, settled,
            "the viewport moved with nothing to react to"
        );
        assert_eq!(first, second, "the same state drew two different screens");
    }

    #[test]
    fn a_tall_cell_can_be_scrolled_through() {
        // The clamp must keep the view inside the selected cell, not pin it
        // to one edge: a cell longer than the screen is unreadable otherwise.
        let mut app = app_with(vec![Cell::new_code(
            (0..40)
                .map(|i| format!("line {i}"))
                .collect::<Vec<_>>()
                .join("\n"),
        )]);
        app.selected = 0;
        screen(&mut app, 40, 12);
        let top = app.scroll;

        app.scroll += 5;
        screen(&mut app, 40, 12);
        assert_eq!(app.scroll, top + 5, "the scroll was snapped back");
    }

    #[test]
    fn a_terminal_without_modified_enter_is_told_so() {
        let mut app = app_with(vec![Cell::new_code("x = 1")]);
        app.enhanced_keys = false;

        // The status bar names the key that works here.
        let status = screen(&mut app, 100, 12);
        assert!(status.contains("e run"), "{status}");
        assert!(
            !status.contains("⇧⏎ run"),
            "the status bar promised a key this terminal cannot send: {status}"
        );

        // And the help says why, rather than listing it and letting the user
        // discover that Enter edits the cell instead.
        app.show_help = true;
        let help = screen(&mut app, 100, 30);
        assert!(
            help.contains("kitty"),
            "the explanation was clipped: {help}"
        );
        assert!(
            !help.contains("e, Shift+Enter"),
            "the help still advertised the key: {help}"
        );

        // A capable terminal keeps the full binding and gets no lecture.
        app.enhanced_keys = true;
        let help = screen(&mut app, 100, 30);
        assert!(help.contains("e, Shift+Enter"), "{help}");
        assert!(!help.contains("kitty"), "{help}");
    }

    #[test]
    fn the_header_names_the_notebook_and_the_kernel() {
        let mut app = app_with(vec![Cell::new_code("x = 1")]);
        let text = screen(&mut app, 80, 12);
        assert!(text.contains("t.ipynb"), "{text}");
        assert!(text.contains("python3"), "{text}");
        assert!(text.contains("1 cells"), "{text}");
    }

    #[test]
    fn cells_show_their_execution_count_and_source() {
        let mut notebook = Notebook::new("python3", "Python 3", "python");
        let mut cell = Cell::new_code("print('hi')");
        if let Some(code) = cell.as_code_mut() {
            code.execution_count = Some(7);
        }
        notebook.push(cell);
        let mut app = App::new(std::path::PathBuf::from("t.ipynb"), notebook);
        let text = screen(&mut app, 80, 12);
        assert!(text.contains("[  7]"), "{text}");
        assert!(text.contains("print('hi')"), "{text}");
    }

    #[test]
    fn a_running_cell_is_marked_with_a_star() {
        let mut app = app_with(vec![Cell::new_code("slow()")]);
        app.running = Some(0);
        let text = screen(&mut app, 80, 12);
        assert!(text.contains("[  *]"), "{text}");
    }

    #[test]
    fn markdown_and_raw_cells_are_labelled_not_numbered() {
        let mut app = app_with(vec![Cell::new_markdown("# title"), Cell::new_raw("raw")]);
        let text = screen(&mut app, 80, 12);
        assert!(text.contains("md"), "{text}");
        assert!(text.contains("raw"), "{text}");
    }

    #[test]
    fn stream_output_appears_under_its_cell() {
        let mut notebook = Notebook::new("python3", "Python 3", "python");
        let mut cell = Cell::new_code("print('out')");
        if let Some(code) = cell.as_code_mut() {
            code.outputs = vec![Output::stream(StreamName::Stdout, "out\n")];
        }
        notebook.push(cell);
        let mut app = App::new(std::path::PathBuf::from("t.ipynb"), notebook);
        let text = screen(&mut app, 80, 12);
        assert!(text.contains("out"), "{text}");
    }

    #[test]
    fn an_image_reserves_rows_and_falls_back_to_a_label() {
        let mut notebook = Notebook::new("python3", "Python 3", "python");
        let mut bundle = MimeBundle::new();
        bundle.insert("image/png", json!("iVBORw0KGgo="));
        let mut cell = Cell::new_code("plot()");
        if let Some(code) = cell.as_code_mut() {
            code.outputs = vec![Output::DisplayData(crate::document::DisplayDataOutput {
                data: bundle,
                metadata: Default::default(),
                extra: Default::default(),
            })];
        }
        notebook.push(cell);
        let mut app = App::new(std::path::PathBuf::from("t.ipynb"), notebook);

        let mut terminal = Terminal::new(TestBackend::new(80, 30)).unwrap();
        let mut result = DrawResult::default();
        terminal
            .draw(|frame| {
                result = draw(frame, &mut app, &mut Images::disabled());
            })
            .unwrap();
        assert_eq!(result.images.len(), 1);
        assert_eq!(result.images[0].base64, "iVBORw0KGgo=");
        assert_eq!(result.images[0].area.height, crate::tui::image::IMAGE_ROWS);
        assert!(
            !result.images[0].drawn,
            "a terminal that was never queried cannot have drawn anything"
        );

        // The rows are held open and the label sits in the first of them, so
        // an unshowable plot is still accounted for on screen.
        let text = screen(&mut app, 80, 30);
        assert!(text.contains("[image/png ·"), "{text}");
    }

    #[test]
    fn the_cursor_is_reported_while_editing() {
        let mut app = app_with(vec![Cell::new_code("hello")]);
        app.enter_edit_mode();
        app.editor.as_mut().unwrap().move_to_end();

        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let mut result = DrawResult::default();
        terminal
            .draw(|frame| {
                result = draw(frame, &mut app, &mut Images::disabled());
            })
            .unwrap();
        let (x, _) = result.cursor.expect("no cursor while editing");
        assert_eq!(x as usize, GUTTER + 5, "cursor should sit after 'hello'");
    }

    #[test]
    fn the_status_bar_shows_the_mode_and_position() {
        let mut app = app_with(vec![Cell::new_code("a"), Cell::new_code("b")]);
        app.selected = 1;
        let text = screen(&mut app, 80, 12);
        assert!(text.contains("COMMAND"), "{text}");
        assert!(text.contains("2/2"), "{text}");

        app.enter_edit_mode();
        let text = screen(&mut app, 80, 12);
        assert!(text.contains("EDIT"), "{text}");
    }

    #[test]
    fn help_overlays_the_notebook() {
        let mut app = app_with(vec![Cell::new_code("hidden_source_marker")]);
        app.show_help = true;
        let text = screen(&mut app, 80, 24);
        assert!(text.contains("run and advance"), "{text}");
    }

    #[test]
    fn the_completion_popup_marks_the_selection() {
        let mut app = app_with(vec![Cell::new_code("js")]);
        app.enter_edit_mode();
        app.on_event(crate::tui::app::Event::Completions {
            matches: vec!["json".into(), "jsonschema".into()],
            cursor_start: 0,
            cursor_end: 2,
        });
        let text = screen(&mut app, 80, 20);
        assert!(text.contains("json"), "{text}");
        assert!(text.contains("2 matches"), "{text}");
    }

    #[test]
    fn the_completion_popup_flips_above_the_cursor_near_the_bottom() {
        // Anchored below the cursor normally; a popup that runs off the
        // bottom of the screen shows nothing useful.
        let cells: Vec<Cell> = (0..20).map(|i| Cell::new_code(format!("c{i}"))).collect();
        let mut app = app_with(cells);
        app.selected = 19;
        app.enter_edit_mode();
        app.on_event(crate::tui::app::Event::Completions {
            matches: vec!["candidate_one".into(), "candidate_two".into()],
            cursor_start: 0,
            cursor_end: 1,
        });
        let text = screen(&mut app, 80, 12);
        assert!(text.contains("candidate_one"), "popup off screen: {text}");
    }

    #[test]
    fn a_long_notebook_scrolls_to_keep_the_selection_visible() {
        let cells: Vec<Cell> = (0..40)
            .map(|i| Cell::new_code(format!("marker_{i}")))
            .collect();
        let mut app = app_with(cells);
        app.selected = 39;
        let text = screen(&mut app, 80, 12);
        assert!(text.contains("marker_39"), "selection off screen: {text}");
        assert!(!text.contains("marker_0\n"), "did not scroll: {text}");
    }

    #[test]
    fn rendering_a_narrow_terminal_does_not_panic() {
        // Widths below the gutter used to underflow the content width.
        let mut app = app_with(vec![Cell::new_code("a very long line of source code")]);
        for width in [1u16, 3, 8, 20] {
            let _ = screen(&mut app, width, 6);
        }
    }

    #[test]
    fn overlays_survive_a_terminal_smaller_than_they_are() {
        // Found by running the real UI: centring an overlay wider than the
        // terminal underflowed and took the whole process down.
        let mut app = app_with(vec![Cell::new_code("x")]);
        for (width, height) in [(1u16, 1u16), (4, 3), (10, 4), (19, 5), (80, 24)] {
            app.show_help = true;
            let _ = screen(&mut app, width, height);
            app.show_help = false;
            app.inspection = Some("some documentation".to_string());
            let _ = screen(&mut app, width, height);
            app.inspection = None;
            app.on_event(crate::tui::app::Event::Completions {
                matches: vec!["candidate".into(); 12],
                cursor_start: 0,
                cursor_end: 1,
            });
            let _ = screen(&mut app, width, height);
            app.completions = None;
        }
    }

    #[test]
    fn multibyte_source_is_truncated_on_character_boundaries() {
        let mut app = app_with(vec![Cell::new_code("日本語".repeat(50))]);
        let _ = screen(&mut app, 30, 8);
    }
}
