//! UI state and key handling.
//!
//! Key handling is a pure function from (state, key) to a list of commands, so
//! every binding is testable without a terminal, a kernel, or a notebook file.
//! The runner task that actually executes those commands lives in
//! [`super::runner`].

use std::path::PathBuf;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::document::{CellType, Notebook, Output};
use crate::kernel::KernelStatus;
use crate::tui::editor::Editor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Keys act on cells: move, insert, delete, run.
    Command,
    /// Keys go into the cell's text.
    Edit,
}

/// Something for the runner task to do.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Run(usize),
    RunAll,
    /// Commit an edited cell's text.
    SetSource(usize, String),
    Insert {
        at: usize,
        kind: CellType,
    },
    Delete(usize),
    Move {
        from: usize,
        to: usize,
    },
    ChangeType(usize, CellType),
    ClearOutputs(usize),
    Restart {
        clear_outputs: bool,
    },
    Save,
    Complete {
        code: String,
        cursor: usize,
    },
    Inspect {
        code: String,
        cursor: usize,
    },
    Quit,
}

/// Something that happened, for the UI to reflect.
#[derive(Debug, Clone)]
pub enum Event {
    /// The authoritative notebook after a mutation.
    Notebook(Box<Notebook>),
    Started(usize),
    /// Streamed while a cell runs.
    Output(Box<Output>),
    Finished {
        index: usize,
        errored: bool,
        elapsed_ms: u64,
    },
    Status(KernelStatus),
    Completions {
        matches: Vec<String>,
        cursor_start: usize,
        cursor_end: usize,
    },
    Inspection(String),
    Message(String),
    Error(String),
}

#[derive(Debug, Clone)]
pub struct CompletionPopup {
    pub matches: Vec<String>,
    pub selected: usize,
    /// Character range in the cell that a chosen match replaces.
    pub cursor_start: usize,
    pub cursor_end: usize,
}

pub struct App {
    pub path: PathBuf,
    pub notebook: Notebook,
    pub selected: usize,
    pub mode: Mode,
    /// Present only while a cell is being edited.
    pub editor: Option<Editor>,
    pub scroll: usize,
    pub status: KernelStatus,
    pub message: String,
    /// Index of the cell currently running, if any.
    pub running: Option<usize>,
    /// Outputs streamed so far for the running cell, shown before it finishes.
    pub live_outputs: Vec<Output>,
    pub completions: Option<CompletionPopup>,
    pub inspection: Option<String>,
    /// First key of a two-key sequence (`dd`, `ii`, `00`).
    pub pending: Option<char>,
    pub show_help: bool,
    pub quit: bool,
    /// Set while an edit has not been committed to the runner.
    pub unsaved_edit: bool,
}

impl App {
    pub fn new(path: PathBuf, notebook: Notebook) -> Self {
        App {
            path,
            notebook,
            selected: 0,
            mode: Mode::Command,
            editor: None,
            scroll: 0,
            status: KernelStatus::Dead,
            message: String::new(),
            running: None,
            live_outputs: Vec::new(),
            completions: None,
            inspection: None,
            pending: None,
            show_help: false,
            quit: false,
            unsaved_edit: false,
        }
    }

    pub fn cell_count(&self) -> usize {
        self.notebook.len()
    }

    pub fn language(&self) -> Option<&str> {
        self.notebook.language()
    }

    fn clamp_selection(&mut self) {
        let last = self.cell_count().saturating_sub(1);
        self.selected = self.selected.min(last);
    }

    /// Apply an event from the runner.
    pub fn on_event(&mut self, event: Event) {
        match event {
            Event::Notebook(notebook) => {
                self.notebook = *notebook;
                self.clamp_selection();
            }
            Event::Started(index) => {
                self.running = Some(index);
                self.live_outputs.clear();
            }
            Event::Output(output) => self.live_outputs.push(*output),
            Event::Finished {
                index,
                errored,
                elapsed_ms,
            } => {
                self.running = None;
                self.live_outputs.clear();
                self.message = if errored {
                    format!("cell {index} raised after {elapsed_ms}ms")
                } else {
                    format!("cell {index} ran in {elapsed_ms}ms")
                };
            }
            Event::Status(status) => self.status = status,
            Event::Completions {
                matches,
                cursor_start,
                cursor_end,
            } => {
                self.completions = (!matches.is_empty()).then_some(CompletionPopup {
                    matches,
                    selected: 0,
                    cursor_start,
                    cursor_end,
                });
                if self.completions.is_none() {
                    self.message = "no completions".to_string();
                }
            }
            Event::Inspection(text) => self.inspection = Some(text),
            Event::Message(text) => self.message = text,
            Event::Error(text) => self.message = format!("error: {text}"),
        }
    }

    /// Translate a key press into commands, mutating local UI state.
    pub fn on_key(&mut self, key: KeyEvent) -> Vec<Command> {
        // Overlays swallow keys first, so Esc always means "close this".
        if self.show_help {
            self.show_help = false;
            return Vec::new();
        }
        if self.inspection.is_some() && self.mode == Mode::Command {
            self.inspection = None;
            return Vec::new();
        }
        if self.completions.is_some()
            && let Some(commands) = self.completion_key(key)
        {
            return commands;
        }
        match self.mode {
            Mode::Command => self.command_key(key),
            Mode::Edit => self.edit_key(key),
        }
    }

    /// Keys the completion popup consumes. `None` passes the key through.
    fn completion_key(&mut self, key: KeyEvent) -> Option<Vec<Command>> {
        let popup = self.completions.as_mut()?;
        match key.code {
            KeyCode::Esc => {
                self.completions = None;
                Some(Vec::new())
            }
            KeyCode::Up => {
                popup.selected = popup.selected.saturating_sub(1);
                Some(Vec::new())
            }
            KeyCode::Down => {
                popup.selected = (popup.selected + 1).min(popup.matches.len() - 1);
                Some(Vec::new())
            }
            KeyCode::Tab | KeyCode::Enter => {
                let choice = popup.matches[popup.selected].clone();
                let (start, end) = (popup.cursor_start, popup.cursor_end);
                self.completions = None;
                if let Some(editor) = self.editor.as_mut() {
                    editor.replace_range(start, end, &choice);
                    self.unsaved_edit = true;
                }
                Some(Vec::new())
            }
            _ => None,
        }
    }

    fn command_key(&mut self, key: KeyEvent) -> Vec<Command> {
        // Two-key sequences, resolved before anything else.
        if let Some(first) = self.pending.take() {
            return match (first, key.code) {
                ('d', KeyCode::Char('d')) => self.delete_selected(),
                ('i', KeyCode::Char('i')) => {
                    self.message = "interrupting".to_string();
                    // Interruption does not go through the runner queue: the
                    // runner is busy running the cell being interrupted.
                    Vec::new()
                }
                ('0', KeyCode::Char('0')) => vec![Command::Restart {
                    clear_outputs: false,
                }],
                _ => Vec::new(),
            };
        }

        match (key.code, key.modifiers) {
            (KeyCode::Char('q'), _) => vec![Command::Quit],
            (KeyCode::Char('?'), _) => {
                self.show_help = true;
                Vec::new()
            }
            (KeyCode::Char('d') | KeyCode::Char('i') | KeyCode::Char('0'), KeyModifiers::NONE) => {
                self.pending = match key.code {
                    KeyCode::Char(c) => Some(c),
                    _ => None,
                };
                Vec::new()
            }

            (KeyCode::Char('j') | KeyCode::Down, _) => {
                self.selected = (self.selected + 1).min(self.cell_count().saturating_sub(1));
                Vec::new()
            }
            (KeyCode::Char('k') | KeyCode::Up, _) => {
                self.selected = self.selected.saturating_sub(1);
                Vec::new()
            }
            (KeyCode::Char('g'), _) => {
                self.selected = 0;
                Vec::new()
            }
            (KeyCode::Char('G'), _) => {
                self.selected = self.cell_count().saturating_sub(1);
                Vec::new()
            }

            // Most terminals cannot send Shift+Enter or Ctrl+Enter at all:
            // without the kitty keyboard protocol they arrive as a plain
            // Enter. `e` and `E` are the bindings that always work, and the
            // modified Enters are honoured where the terminal can send them.
            (KeyCode::Enter, KeyModifiers::SHIFT) | (KeyCode::Char('e'), KeyModifiers::NONE) => {
                self.run_and_advance()
            }
            (KeyCode::Enter, KeyModifiers::CONTROL) | (KeyCode::Char('E'), _) => {
                self.run_in_place()
            }
            (KeyCode::Enter, _) => {
                self.enter_edit_mode();
                Vec::new()
            }

            (KeyCode::Char('a'), _) => {
                let at = self.selected;
                vec![Command::Insert {
                    at,
                    kind: CellType::Code,
                }]
            }
            (KeyCode::Char('b'), _) => {
                let at = self.selected + 1;
                self.selected = at;
                vec![Command::Insert {
                    at,
                    kind: CellType::Code,
                }]
            }
            (KeyCode::Char('m'), _) => vec![Command::ChangeType(self.selected, CellType::Markdown)],
            (KeyCode::Char('y'), _) => vec![Command::ChangeType(self.selected, CellType::Code)],
            (KeyCode::Char('r'), _) => vec![Command::ChangeType(self.selected, CellType::Raw)],
            (KeyCode::Char('o'), _) => vec![Command::ClearOutputs(self.selected)],

            (KeyCode::Char('J'), _) if self.selected + 1 < self.cell_count() => {
                let from = self.selected;
                self.selected += 1;
                vec![Command::Move { from, to: from + 1 }]
            }
            (KeyCode::Char('K'), _) if self.selected > 0 => {
                let from = self.selected;
                self.selected -= 1;
                vec![Command::Move { from, to: from - 1 }]
            }

            (KeyCode::Char('R'), _) => vec![Command::Restart {
                clear_outputs: true,
            }],
            (KeyCode::Char('A'), _) => vec![Command::RunAll],
            (KeyCode::Char('s'), _) => vec![Command::Save],
            _ => Vec::new(),
        }
    }

    fn edit_key(&mut self, key: KeyEvent) -> Vec<Command> {
        match (key.code, key.modifiers) {
            (KeyCode::Esc, _) => self.leave_edit_mode(),
            (KeyCode::Enter, KeyModifiers::SHIFT) => {
                let mut commands = self.leave_edit_mode();
                commands.extend(self.run_and_advance());
                commands
            }
            (KeyCode::Enter, KeyModifiers::CONTROL) => {
                let mut commands = self.leave_edit_mode();
                commands.extend(self.run_in_place());
                commands
            }
            // Shift+Tab reaches us as `BackTab` in every terminal that lacks
            // the kitty protocol and as a modified Tab in the ones that have
            // it. Both mean dedent: binding one of the two encodings to a
            // different action makes the key do different things depending on
            // the terminal, which is worse than either choice.
            (KeyCode::BackTab, _) | (KeyCode::Tab, KeyModifiers::SHIFT) => {
                if let Some(editor) = self.editor.as_mut() {
                    editor.dedent();
                    self.unsaved_edit = true;
                }
                Vec::new()
            }
            // Inspection needs a binding that survives every terminal, which
            // Shift+Tab does not.
            (KeyCode::Char('i'), KeyModifiers::ALT) => self.request_inspection(),
            (KeyCode::Tab, _) => {
                // Tab indents at the start of a line and completes otherwise,
                // which is what every notebook front end does.
                let editor = match self.editor.as_mut() {
                    Some(editor) => editor,
                    None => return Vec::new(),
                };
                if editor.word_before_cursor().is_empty() {
                    editor.indent();
                    self.unsaved_edit = true;
                    Vec::new()
                } else {
                    self.request_completion()
                }
            }
            _ => {
                let editor = match self.editor.as_mut() {
                    Some(editor) => editor,
                    None => return Vec::new(),
                };
                match key.code {
                    // Modified letters are chords, not text. Inserting the
                    // letter regardless of modifier is how a reflex Ctrl+S
                    // used to drop a stray `s` into the code.
                    KeyCode::Char(_)
                        if key
                            .modifiers
                            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) => {}
                    KeyCode::Char(c) => {
                        editor.insert_char(c);
                        self.unsaved_edit = true;
                    }
                    KeyCode::Enter => {
                        editor.insert_newline();
                        self.unsaved_edit = true;
                    }
                    KeyCode::Backspace => {
                        editor.backspace();
                        self.unsaved_edit = true;
                    }
                    KeyCode::Delete => {
                        editor.delete();
                        self.unsaved_edit = true;
                    }
                    KeyCode::Left => editor.move_left(),
                    KeyCode::Right => editor.move_right(),
                    KeyCode::Up => {
                        editor.move_up();
                    }
                    KeyCode::Down => {
                        editor.move_down();
                    }
                    KeyCode::Home => editor.move_home(),
                    KeyCode::End => editor.move_end(),
                    _ => {}
                }
                Vec::new()
            }
        }
    }

    pub fn enter_edit_mode(&mut self) {
        let Ok(cell) = self.notebook.get(self.selected) else {
            return;
        };
        self.editor = Some(Editor::new(cell.source()));
        self.mode = Mode::Edit;
        self.unsaved_edit = false;
        self.message = String::new();
    }

    /// Leave edit mode, committing the text only if it actually changed.
    ///
    /// Committing unconditionally would clear the cell's outputs on every
    /// Esc, because a source change invalidates them.
    pub fn leave_edit_mode(&mut self) -> Vec<Command> {
        self.completions = None;
        self.mode = Mode::Command;
        let Some(editor) = self.editor.take() else {
            return Vec::new();
        };
        let text = editor.text();
        let unchanged = self
            .notebook
            .get(self.selected)
            .is_ok_and(|c| c.source() == text);
        self.unsaved_edit = false;
        if unchanged {
            Vec::new()
        } else {
            vec![Command::SetSource(self.selected, text)]
        }
    }

    fn run_in_place(&mut self) -> Vec<Command> {
        vec![Command::Run(self.selected)]
    }

    fn run_and_advance(&mut self) -> Vec<Command> {
        let index = self.selected;
        let mut commands = vec![Command::Run(index)];
        if index + 1 >= self.cell_count() {
            // Running the last cell appends a new one, exactly like Jupyter,
            // so a session can keep going without reaching for a menu.
            commands.push(Command::Insert {
                at: index + 1,
                kind: CellType::Code,
            });
        }
        self.selected = index + 1;
        commands
    }

    fn request_completion(&mut self) -> Vec<Command> {
        let Some(editor) = self.editor.as_ref() else {
            return Vec::new();
        };
        vec![Command::Complete {
            code: editor.text(),
            cursor: editor.char_offset(),
        }]
    }

    fn request_inspection(&mut self) -> Vec<Command> {
        let Some(editor) = self.editor.as_ref() else {
            return Vec::new();
        };
        vec![Command::Inspect {
            code: editor.text(),
            cursor: editor.char_offset(),
        }]
    }

    fn delete_selected(&mut self) -> Vec<Command> {
        if self.cell_count() <= 1 {
            self.message = "the notebook needs at least one cell".to_string();
            return Vec::new();
        }
        let index = self.selected;
        if index + 1 >= self.cell_count() {
            self.selected = index.saturating_sub(1);
        }
        vec![Command::Delete(index)]
    }

    /// Source of the cell being displayed, which is the editor's buffer while
    /// editing so keystrokes appear immediately.
    pub fn displayed_source(&self, index: usize) -> String {
        if index == self.selected
            && let Some(editor) = self.editor.as_ref()
        {
            return editor.text();
        }
        self.notebook
            .get(index)
            .map(|c| c.source().to_string())
            .unwrap_or_default()
    }

    /// Outputs to draw for a cell: the live stream while it runs, otherwise
    /// what the notebook recorded.
    pub fn displayed_outputs(&self, index: usize) -> &[Output] {
        if self.running == Some(index) {
            return &self.live_outputs;
        }
        self.notebook.get(index).map(|c| c.outputs()).unwrap_or(&[])
    }
}

/// The key reference shown by `?`.
pub const HELP: &[(&str, &str)] = &[
    ("j / k, ↓ / ↑", "select cell"),
    ("g / G", "first / last cell"),
    ("Enter", "edit cell"),
    ("Esc", "back to command mode"),
    ("e, Shift+Enter", "run and advance"),
    ("E, Ctrl+Enter", "run in place"),
    ("A", "run every cell"),
    ("a / b", "insert cell above / below"),
    ("dd", "delete cell"),
    ("J / K", "move cell down / up"),
    ("m / y / r", "make markdown / code / raw"),
    ("o", "clear this cell's outputs"),
    ("ii", "interrupt the running cell"),
    ("00", "restart the kernel"),
    ("R", "restart and clear all outputs"),
    ("s", "save"),
    ("Tab", "indent, or complete"),
    ("Shift+Tab", "dedent"),
    ("Alt+i", "inspect what is under the cursor"),
    ("?", "this help"),
    ("q", "quit"),
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::Cell;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn with_modifiers(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    fn app(sources: &[&str]) -> App {
        let mut notebook = Notebook::new("python3", "Python 3", "python");
        for source in sources {
            notebook.push(Cell::new_code(*source));
        }
        App::new(PathBuf::from("test.ipynb"), notebook)
    }

    #[test]
    fn selection_moves_and_clamps_at_both_ends() {
        let mut app = app(&["a", "b", "c"]);
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.selected, 1);
        app.on_key(key(KeyCode::Char('G')));
        assert_eq!(app.selected, 2);
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.selected, 2, "should not run off the end");
        app.on_key(key(KeyCode::Char('g')));
        assert_eq!(app.selected, 0);
        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(app.selected, 0, "should not run off the start");
    }

    #[test]
    fn enter_edits_and_escape_commits_only_real_changes() {
        let mut app = app(&["x = 1"]);
        app.on_key(key(KeyCode::Enter));
        assert_eq!(app.mode, Mode::Edit);

        // Escaping without typing must not clear the cell's outputs, which is
        // what a SetSource would do.
        let commands = app.on_key(key(KeyCode::Esc));
        assert!(commands.is_empty(), "{commands:?}");

        app.on_key(key(KeyCode::Enter));
        app.on_key(key(KeyCode::Char('!')));
        let commands = app.on_key(key(KeyCode::Esc));
        assert_eq!(commands, vec![Command::SetSource(0, "!x = 1".to_string())]);
        assert_eq!(app.mode, Mode::Command);
    }

    #[test]
    fn shift_enter_runs_and_advances() {
        let mut app = app(&["a", "b"]);
        let commands = app.on_key(with_modifiers(KeyCode::Enter, KeyModifiers::SHIFT));
        assert_eq!(commands, vec![Command::Run(0)]);
        assert_eq!(app.selected, 1);
    }

    #[test]
    fn running_the_last_cell_appends_a_new_one() {
        // Otherwise a session dead-ends at the bottom of the notebook.
        let mut app = app(&["only"]);
        let commands = app.on_key(with_modifiers(KeyCode::Enter, KeyModifiers::SHIFT));
        assert_eq!(
            commands,
            vec![
                Command::Run(0),
                Command::Insert {
                    at: 1,
                    kind: CellType::Code
                }
            ]
        );
    }

    #[test]
    fn e_runs_and_advances_where_shift_enter_cannot_be_sent() {
        // The binding that works in every terminal, not just the handful that
        // implement the kitty keyboard protocol.
        let mut advancing = app(&["a", "b"]);
        assert_eq!(
            advancing.on_key(key(KeyCode::Char('e'))),
            vec![Command::Run(0)]
        );
        assert_eq!(advancing.selected, 1);

        let mut in_place = app(&["a", "b"]);
        assert_eq!(
            in_place.on_key(key(KeyCode::Char('E'))),
            vec![Command::Run(0)]
        );
        assert_eq!(in_place.selected, 0, "E should not move the selection");
    }

    #[test]
    fn ctrl_enter_runs_without_moving() {
        let mut app = app(&["a", "b"]);
        let commands = app.on_key(with_modifiers(KeyCode::Enter, KeyModifiers::CONTROL));
        assert_eq!(commands, vec![Command::Run(0)]);
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn shift_enter_from_edit_mode_commits_then_runs() {
        let mut app = app(&["x"]);
        app.on_key(key(KeyCode::Enter));
        app.on_key(key(KeyCode::Char('1')));
        let commands = app.on_key(with_modifiers(KeyCode::Enter, KeyModifiers::SHIFT));
        assert_eq!(
            commands,
            vec![
                Command::SetSource(0, "1x".to_string()),
                Command::Run(0),
                Command::Insert {
                    at: 1,
                    kind: CellType::Code
                }
            ],
            "the edit must be committed before the run sees it"
        );
        assert_eq!(app.mode, Mode::Command);
    }

    #[test]
    fn dd_deletes_but_never_empties_the_notebook() {
        let mut two = app(&["a", "b"]);
        two.on_key(key(KeyCode::Char('d')));
        assert_eq!(two.pending, Some('d'));
        let commands = two.on_key(key(KeyCode::Char('d')));
        assert_eq!(commands, vec![Command::Delete(0)]);

        let mut app = app(&["only"]);
        app.on_key(key(KeyCode::Char('d')));
        let commands = app.on_key(key(KeyCode::Char('d')));
        assert!(commands.is_empty(), "deleted the last cell");
        assert!(app.message.contains("at least one cell"));
    }

    #[test]
    fn a_broken_two_key_sequence_does_nothing() {
        let mut app = app(&["a", "b"]);
        app.on_key(key(KeyCode::Char('d')));
        let commands = app.on_key(key(KeyCode::Char('j')));
        assert!(
            commands.is_empty(),
            "d-then-j should not delete: {commands:?}"
        );
        assert_eq!(app.pending, None);
    }

    #[test]
    fn restart_bindings_are_distinct() {
        let mut app = app(&["a"]);
        app.on_key(key(KeyCode::Char('0')));
        assert_eq!(
            app.on_key(key(KeyCode::Char('0'))),
            vec![Command::Restart {
                clear_outputs: false
            }]
        );
        assert_eq!(
            app.on_key(key(KeyCode::Char('R'))),
            vec![Command::Restart {
                clear_outputs: true
            }]
        );
    }

    #[test]
    fn insert_above_and_below_target_the_right_slots() {
        let mut app = app(&["a", "b"]);
        app.selected = 1;
        assert_eq!(
            app.on_key(key(KeyCode::Char('a'))),
            vec![Command::Insert {
                at: 1,
                kind: CellType::Code
            }]
        );
        app.selected = 1;
        assert_eq!(
            app.on_key(key(KeyCode::Char('b'))),
            vec![Command::Insert {
                at: 2,
                kind: CellType::Code
            }]
        );
        assert_eq!(app.selected, 2, "insert below should follow the new cell");
    }

    #[test]
    fn moving_a_cell_keeps_the_selection_on_it() {
        let mut app = app(&["a", "b", "c"]);
        app.selected = 1;
        assert_eq!(
            app.on_key(key(KeyCode::Char('J'))),
            vec![Command::Move { from: 1, to: 2 }]
        );
        assert_eq!(app.selected, 2);
        assert!(
            app.on_key(key(KeyCode::Char('J'))).is_empty(),
            "cannot move the last cell further down"
        );
    }

    #[test]
    fn tab_indents_at_the_start_of_a_line_and_completes_after_a_word() {
        let mut indented = app(&["    "]);
        indented.on_key(key(KeyCode::Enter));
        indented.editor.as_mut().unwrap().move_to_end();
        let commands = indented.on_key(key(KeyCode::Tab));
        assert!(commands.is_empty(), "expected an indent, got {commands:?}");
        assert_eq!(indented.editor.as_ref().unwrap().text(), "        ");

        let mut app = app(&["json.du"]);
        app.on_key(key(KeyCode::Enter));
        app.editor.as_mut().unwrap().move_to_end();
        let commands = app.on_key(key(KeyCode::Tab));
        assert!(
            matches!(commands.as_slice(), [Command::Complete { .. }]),
            "{commands:?}"
        );
    }

    #[test]
    fn choosing_a_completion_rewrites_the_editor() {
        let mut app = app(&["import js"]);
        app.on_key(key(KeyCode::Enter));
        app.editor.as_mut().unwrap().move_to_end();
        app.on_event(Event::Completions {
            matches: vec!["json".into(), "jsonschema".into()],
            cursor_start: 7,
            cursor_end: 9,
        });
        assert!(app.completions.is_some());
        app.on_key(key(KeyCode::Down));
        app.on_key(key(KeyCode::Enter));
        assert_eq!(app.editor.as_ref().unwrap().text(), "import jsonschema");
        assert!(app.completions.is_none());
    }

    #[test]
    fn escape_closes_the_completion_popup_without_leaving_edit_mode() {
        let mut app = app(&["x"]);
        app.on_key(key(KeyCode::Enter));
        app.on_event(Event::Completions {
            matches: vec!["xyz".into()],
            cursor_start: 0,
            cursor_end: 1,
        });
        app.on_key(key(KeyCode::Esc));
        assert!(app.completions.is_none());
        assert_eq!(
            app.mode,
            Mode::Edit,
            "the popup should absorb the first Esc"
        );
    }

    #[test]
    fn no_completions_reports_rather_than_showing_an_empty_popup() {
        let mut app = app(&["x"]);
        app.on_event(Event::Completions {
            matches: Vec::new(),
            cursor_start: 0,
            cursor_end: 0,
        });
        assert!(app.completions.is_none());
        assert!(app.message.contains("no completions"));
    }

    #[test]
    fn help_and_inspection_overlays_close_on_any_key() {
        let mut app = app(&["x"]);
        app.on_key(key(KeyCode::Char('?')));
        assert!(app.show_help);
        app.on_key(key(KeyCode::Char('j')));
        assert!(!app.show_help);
        assert_eq!(
            app.selected, 0,
            "the key that closed help must not also act"
        );

        app.on_event(Event::Inspection("docs".into()));
        app.on_key(key(KeyCode::Esc));
        assert!(app.inspection.is_none());
    }

    #[test]
    fn live_output_is_shown_while_a_cell_runs_and_dropped_after() {
        let mut app = app(&["x"]);
        app.on_event(Event::Started(0));
        app.on_event(Event::Output(Box::new(Output::stream(
            crate::document::StreamName::Stdout,
            "working\n",
        ))));
        assert_eq!(app.displayed_outputs(0).len(), 1);
        app.on_event(Event::Finished {
            index: 0,
            errored: false,
            elapsed_ms: 12,
        });
        assert!(app.displayed_outputs(0).is_empty());
        assert!(app.message.contains("12ms"));
    }

    #[test]
    fn the_editor_buffer_is_what_gets_displayed_while_editing() {
        let mut app = app(&["original"]);
        app.on_key(key(KeyCode::Enter));
        app.on_key(key(KeyCode::Char('!')));
        assert_eq!(app.displayed_source(0), "!original");
        // Other cells still come from the notebook.
        assert_eq!(app.displayed_source(9), "");
    }

    #[test]
    fn a_replacement_notebook_clamps_a_now_invalid_selection() {
        let mut app = app(&["a", "b", "c"]);
        app.selected = 2;
        let mut smaller = Notebook::new("python3", "Python 3", "python");
        smaller.push(Cell::new_code("a"));
        app.on_event(Event::Notebook(Box::new(smaller)));
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn q_quits() {
        let mut app = app(&["x"]);
        assert_eq!(app.on_key(key(KeyCode::Char('q'))), vec![Command::Quit]);
    }
}
