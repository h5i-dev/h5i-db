//! A multi-line text buffer with a cursor.
//!
//! Deliberately free of any terminal or ratatui types so the editing rules are
//! testable as pure state transitions. Everything here counts in *characters*,
//! never bytes: a notebook full of `≥` and `日本語` is normal, and byte
//! indexing would put the cursor inside a codepoint.

/// Cursor position, in characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Cursor {
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone)]
pub struct Editor {
    lines: Vec<String>,
    cursor: Cursor,
    /// Column the cursor wants when moving vertically.
    ///
    /// Without it, moving down through a short line and back up would forget
    /// the original column, which every real editor remembers.
    goal_column: Option<usize>,
}

impl Default for Editor {
    fn default() -> Self {
        Editor::new("")
    }
}

impl Editor {
    pub fn new(text: &str) -> Self {
        let lines: Vec<String> = if text.is_empty() {
            vec![String::new()]
        } else {
            text.split('\n').map(str::to_string).collect()
        };
        Editor {
            lines,
            cursor: Cursor::default(),
            goal_column: None,
        }
    }

    pub fn text(&self) -> String {
        self.lines.join("\n")
    }

    pub fn lines(&self) -> &[String] {
        &self.lines
    }

    pub fn cursor(&self) -> Cursor {
        self.cursor
    }

    pub fn line_count(&self) -> usize {
        self.lines.len()
    }

    fn line_chars(&self, line: usize) -> usize {
        self.lines.get(line).map(|l| l.chars().count()).unwrap_or(0)
    }

    /// Byte offset of a character column, for slicing.
    fn byte_at(line: &str, column: usize) -> usize {
        line.char_indices()
            .nth(column)
            .map(|(i, _)| i)
            .unwrap_or(line.len())
    }

    /// Character offset of the cursor within the whole text, which is what the
    /// kernel's completion and inspection requests take.
    pub fn char_offset(&self) -> usize {
        let mut offset = 0;
        for line in self.lines.iter().take(self.cursor.line) {
            offset += line.chars().count() + 1; // + the newline
        }
        offset + self.cursor.column
    }

    pub fn set_cursor(&mut self, line: usize, column: usize) {
        self.cursor.line = line.min(self.lines.len().saturating_sub(1));
        self.cursor.column = column.min(self.line_chars(self.cursor.line));
        self.goal_column = None;
    }

    pub fn move_to_end(&mut self) {
        let last = self.lines.len().saturating_sub(1);
        self.set_cursor(last, self.line_chars(last));
    }

    // ---- editing --------------------------------------------------------

    pub fn insert_char(&mut self, c: char) {
        if c == '\n' {
            self.insert_newline();
            return;
        }
        let column = self.cursor.column;
        let line = &mut self.lines[self.cursor.line];
        let byte = Self::byte_at(line, column);
        line.insert(byte, c);
        self.cursor.column += 1;
        self.goal_column = None;
    }

    pub fn insert_str(&mut self, text: &str) {
        for c in text.chars() {
            self.insert_char(c);
        }
    }

    /// Split the line at the cursor, carrying the current indentation.
    ///
    /// Auto-indent is what makes a Python cell editable at all in a terminal;
    /// without it every block body has to be re-indented by hand.
    pub fn insert_newline(&mut self) {
        let column = self.cursor.column;
        let line = self.lines[self.cursor.line].clone();
        let byte = Self::byte_at(&line, column);
        let (before, after) = line.split_at(byte);

        let mut indent: String = before
            .chars()
            .take_while(|c| *c == ' ' || *c == '\t')
            .collect();
        // A line opening a block gets one more level. Four spaces, because
        // that is what the language this is mostly used for asks for.
        if before.trim_end().ends_with(':') {
            indent.push_str("    ");
        }

        self.lines[self.cursor.line] = before.to_string();
        let new_line = format!("{indent}{after}");
        self.cursor.line += 1;
        self.cursor.column = indent.chars().count();
        self.lines.insert(self.cursor.line, new_line);
        self.goal_column = None;
    }

    /// Delete the character before the cursor, joining lines at column 0.
    pub fn backspace(&mut self) {
        self.goal_column = None;
        if self.cursor.column > 0 {
            let column = self.cursor.column;
            let line = &mut self.lines[self.cursor.line];
            let start = Self::byte_at(line, column - 1);
            let end = Self::byte_at(line, column);
            line.replace_range(start..end, "");
            self.cursor.column -= 1;
            return;
        }
        if self.cursor.line == 0 {
            return;
        }
        let current = self.lines.remove(self.cursor.line);
        self.cursor.line -= 1;
        self.cursor.column = self.line_chars(self.cursor.line);
        self.lines[self.cursor.line].push_str(&current);
    }

    /// Delete the character under the cursor, joining the next line at the end.
    pub fn delete(&mut self) {
        self.goal_column = None;
        let width = self.line_chars(self.cursor.line);
        if self.cursor.column < width {
            let column = self.cursor.column;
            let line = &mut self.lines[self.cursor.line];
            let start = Self::byte_at(line, column);
            let end = Self::byte_at(line, column + 1);
            line.replace_range(start..end, "");
            return;
        }
        if self.cursor.line + 1 < self.lines.len() {
            let next = self.lines.remove(self.cursor.line + 1);
            self.lines[self.cursor.line].push_str(&next);
        }
    }

    /// Insert four spaces, or complete the indent to the next multiple of four.
    pub fn indent(&mut self) {
        let width = 4 - (self.cursor.column % 4);
        for _ in 0..width {
            self.insert_char(' ');
        }
    }

    /// Remove up to four spaces of indentation from the start of the line.
    ///
    /// Deliberately indifferent to where the cursor is: what gets removed is
    /// indentation, never the characters in front of the cursor. Taking the
    /// count from the leading whitespace and then deleting backwards from the
    /// cursor is how a dedent at the end of `    x = 1` used to eat `= 1`.
    pub fn dedent(&mut self) {
        let line = &mut self.lines[self.cursor.line];
        let remove = line.chars().take_while(|c| *c == ' ').count().min(4);
        if remove == 0 {
            return;
        }
        line.replace_range(0..remove, "");
        self.cursor.column = self.cursor.column.saturating_sub(remove);
    }

    /// Replace the range `[start, end)` in characters with `text`.
    ///
    /// Used to apply a completion, which is expressed as a replacement range
    /// by the protocol.
    pub fn replace_range(&mut self, start: usize, end: usize, text: &str) {
        let whole = self.text();
        let chars: Vec<char> = whole.chars().collect();
        let start = start.min(chars.len());
        let end = end.clamp(start, chars.len());
        let mut rebuilt: String = chars[..start].iter().collect();
        rebuilt.push_str(text);
        rebuilt.extend(&chars[end..]);

        let new_offset = start + text.chars().count();
        *self = Editor::new(&rebuilt);
        self.set_char_offset(new_offset);
    }

    fn set_char_offset(&mut self, offset: usize) {
        let mut remaining = offset;
        for (index, line) in self.lines.iter().enumerate() {
            let width = line.chars().count();
            if remaining <= width {
                self.cursor = Cursor {
                    line: index,
                    column: remaining,
                };
                return;
            }
            remaining -= width + 1;
        }
        self.move_to_end();
    }

    // ---- movement -------------------------------------------------------

    pub fn move_left(&mut self) {
        self.goal_column = None;
        if self.cursor.column > 0 {
            self.cursor.column -= 1;
        } else if self.cursor.line > 0 {
            self.cursor.line -= 1;
            self.cursor.column = self.line_chars(self.cursor.line);
        }
    }

    pub fn move_right(&mut self) {
        self.goal_column = None;
        if self.cursor.column < self.line_chars(self.cursor.line) {
            self.cursor.column += 1;
        } else if self.cursor.line + 1 < self.lines.len() {
            self.cursor.line += 1;
            self.cursor.column = 0;
        }
    }

    /// Move up a line. Returns false at the top, so the caller can decide
    /// whether that means "move to the previous cell".
    pub fn move_up(&mut self) -> bool {
        if self.cursor.line == 0 {
            return false;
        }
        let goal = *self.goal_column.get_or_insert(self.cursor.column);
        self.cursor.line -= 1;
        self.cursor.column = goal.min(self.line_chars(self.cursor.line));
        true
    }

    pub fn move_down(&mut self) -> bool {
        if self.cursor.line + 1 >= self.lines.len() {
            return false;
        }
        let goal = *self.goal_column.get_or_insert(self.cursor.column);
        self.cursor.line += 1;
        self.cursor.column = goal.min(self.line_chars(self.cursor.line));
        true
    }

    pub fn move_home(&mut self) {
        self.goal_column = None;
        // First press goes to the first non-blank, second to column zero:
        // in indented code the first non-blank is almost always what is meant.
        let line = &self.lines[self.cursor.line];
        let indent = line.chars().take_while(|c| c.is_whitespace()).count();
        self.cursor.column = if self.cursor.column == indent {
            0
        } else {
            indent
        };
    }

    pub fn move_end(&mut self) {
        self.goal_column = None;
        self.cursor.column = self.line_chars(self.cursor.line);
    }

    /// The identifier immediately before the cursor, for completion display.
    pub fn word_before_cursor(&self) -> String {
        let line = &self.lines[self.cursor.line];
        let chars: Vec<char> = line.chars().take(self.cursor.column).collect();
        chars
            .iter()
            .rev()
            .take_while(|c| c.is_alphanumeric() || **c == '_' || **c == '.')
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_editor_has_one_empty_line() {
        let editor = Editor::new("");
        assert_eq!(editor.line_count(), 1);
        assert_eq!(editor.text(), "");
    }

    #[test]
    fn typing_and_backspace_round_trip() {
        let mut editor = Editor::new("");
        editor.insert_str("hello");
        assert_eq!(editor.text(), "hello");
        editor.backspace();
        assert_eq!(editor.text(), "hell");
        assert_eq!(editor.cursor(), Cursor { line: 0, column: 4 });
    }

    #[test]
    fn newline_carries_the_indentation() {
        let mut editor = Editor::new("    x = 1");
        editor.move_to_end();
        editor.insert_newline();
        editor.insert_str("y = 2");
        assert_eq!(editor.text(), "    x = 1\n    y = 2");
    }

    #[test]
    fn a_line_ending_in_colon_opens_a_block() {
        let mut editor = Editor::new("def f():");
        editor.move_to_end();
        editor.insert_newline();
        editor.insert_str("return 1");
        assert_eq!(editor.text(), "def f():\n    return 1");
    }

    #[test]
    fn backspace_at_column_zero_joins_the_previous_line() {
        let mut editor = Editor::new("a\nb");
        editor.set_cursor(1, 0);
        editor.backspace();
        assert_eq!(editor.text(), "ab");
        assert_eq!(editor.cursor(), Cursor { line: 0, column: 1 });
    }

    #[test]
    fn delete_at_end_of_line_pulls_up_the_next() {
        let mut editor = Editor::new("a\nb");
        editor.set_cursor(0, 1);
        editor.delete();
        assert_eq!(editor.text(), "ab");
    }

    #[test]
    fn backspace_at_the_very_start_does_nothing() {
        let mut editor = Editor::new("abc");
        editor.backspace();
        assert_eq!(editor.text(), "abc");
    }

    #[test]
    fn multibyte_text_is_edited_by_character_not_byte() {
        // Byte indexing here would slice a codepoint in half and panic.
        let mut editor = Editor::new("日本語");
        editor.move_to_end();
        assert_eq!(editor.cursor().column, 3);
        editor.backspace();
        assert_eq!(editor.text(), "日本");
        editor.insert_char('語');
        assert_eq!(editor.text(), "日本語");
        editor.set_cursor(0, 1);
        editor.delete();
        assert_eq!(editor.text(), "日語");
    }

    #[test]
    fn vertical_movement_remembers_the_goal_column() {
        let mut editor = Editor::new("longer line\nx\nanother long line");
        editor.set_cursor(0, 9);
        editor.move_down();
        assert_eq!(editor.cursor().column, 1, "clamped to the short line");
        editor.move_down();
        assert_eq!(
            editor.cursor().column,
            9,
            "the original column should come back"
        );
    }

    #[test]
    fn movement_reports_when_it_cannot_go_further() {
        let mut editor = Editor::new("only line");
        assert!(!editor.move_up());
        assert!(!editor.move_down());
        let mut editor = Editor::new("a\nb");
        assert!(editor.move_down());
        assert!(!editor.move_down());
    }

    #[test]
    fn home_toggles_between_the_indent_and_column_zero() {
        let mut editor = Editor::new("    indented");
        editor.move_to_end();
        editor.move_home();
        assert_eq!(editor.cursor().column, 4);
        editor.move_home();
        assert_eq!(editor.cursor().column, 0);
        editor.move_home();
        assert_eq!(editor.cursor().column, 4);
    }

    #[test]
    fn indent_completes_to_the_next_stop() {
        let mut editor = Editor::new("ab");
        editor.move_to_end();
        editor.indent();
        assert_eq!(editor.text(), "ab  ", "should reach column 4, not add 4");
        editor.indent();
        assert_eq!(editor.text(), "ab      ");
    }

    #[test]
    fn dedent_removes_up_to_one_level() {
        let mut editor = Editor::new("        deep");
        editor.set_cursor(0, 8);
        editor.dedent();
        assert_eq!(editor.text(), "    deep");
        editor.dedent();
        assert_eq!(editor.text(), "deep");
        editor.dedent();
        assert_eq!(editor.text(), "deep", "nothing left to remove");
    }

    #[test]
    fn dedent_removes_indentation_not_the_code_before_the_cursor() {
        // The count of leading spaces used to be deleted from wherever the
        // cursor happened to be, so a dedent at the end of an indented line
        // silently ate the code instead of the indentation.
        let mut editor = Editor::new("    x = 1");
        editor.move_to_end();
        editor.dedent();
        assert_eq!(editor.text(), "x = 1");
        assert_eq!(editor.cursor().column, 5, "cursor should follow the text");

        // A line with nothing to remove must not lose a character either.
        let mut editor = Editor::new("x = 1");
        editor.move_to_end();
        editor.dedent();
        assert_eq!(editor.text(), "x = 1");
        assert_eq!(editor.cursor().column, 5);
    }

    #[test]
    fn char_offset_matches_the_flattened_text() {
        let editor = {
            let mut e = Editor::new("ab\ncde\nf");
            e.set_cursor(1, 2);
            e
        };
        // "ab\ncd|e\nf" -> a b \n c d = 5 characters before the cursor
        assert_eq!(editor.char_offset(), 5);
    }

    #[test]
    fn replacing_a_range_applies_a_completion() {
        let mut editor = Editor::new("import js");
        editor.move_to_end();
        // Replace "js" with "json", as a complete_reply would ask.
        editor.replace_range(7, 9, "json");
        assert_eq!(editor.text(), "import json");
        assert_eq!(
            editor.cursor(),
            Cursor {
                line: 0,
                column: 11
            }
        );
    }

    #[test]
    fn replacing_a_range_across_lines_keeps_the_cursor_sane() {
        let mut editor = Editor::new("a\nbb\nccc");
        editor.replace_range(2, 4, "X");
        assert_eq!(editor.text(), "a\nX\nccc");
        assert_eq!(editor.cursor(), Cursor { line: 1, column: 1 });
    }

    #[test]
    fn the_word_before_the_cursor_is_the_completion_prefix() {
        let mut editor = Editor::new("x = json.du");
        editor.move_to_end();
        assert_eq!(editor.word_before_cursor(), "json.du");
        editor.set_cursor(0, 1);
        assert_eq!(editor.word_before_cursor(), "x");
    }

    #[test]
    fn setting_a_cursor_out_of_range_clamps_instead_of_panicking() {
        let mut editor = Editor::new("a\nb");
        editor.set_cursor(99, 99);
        assert_eq!(editor.cursor(), Cursor { line: 1, column: 1 });
    }
}
