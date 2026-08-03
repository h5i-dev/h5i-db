//! Syntax highlighting for cell source.
//!
//! A hand-written lexer rather than tree-sitter. The roadmap originally called
//! for tree-sitter, but its grammars are C and would put a `cc` toolchain in
//! the build of a project whose distribution story is a single static binary,
//! and which already chose a pure-Rust ZeroMQ for that reason. Highlighting a
//! cell in a terminal needs keywords, strings, comments, and numbers told
//! apart; it does not need a parse tree, and being wrong inside an unterminated
//! string for one keystroke costs nothing.

/// What a run of characters is, for colouring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Token {
    Plain,
    Keyword,
    /// A built-in function or type name.
    Builtin,
    String,
    Comment,
    Number,
    /// The `%%sql` magic line.
    Magic,
    /// A function or class name at its definition site.
    Definition,
}

/// One coloured run: `(start_byte, end_byte, token)`.
pub type Span = (usize, usize, Token);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    Python,
    Sql,
    None,
}

impl Language {
    /// Pick the lexer for a cell, honouring the `%%sql` magic over the
    /// notebook's language.
    pub fn for_cell(source: &str, notebook_language: Option<&str>) -> Self {
        if source.starts_with("%%sql") {
            return Language::Sql;
        }
        match notebook_language {
            Some(l) if l.eq_ignore_ascii_case("python") => Language::Python,
            Some(l) if l.eq_ignore_ascii_case("sql") => Language::Sql,
            _ => Language::None,
        }
    }
}

/// Highlight one line. Lines are independent, which is why a string spanning
/// several lines only colours its first one; see the module note.
pub fn highlight_line(line: &str, language: Language) -> Vec<Span> {
    if line.starts_with("%%") {
        return vec![(0, line.len(), Token::Magic)];
    }
    match language {
        Language::Python => lex(line, PYTHON_KEYWORDS, PYTHON_BUILTINS, &["#"], true),
        Language::Sql => lex(line, SQL_KEYWORDS, SQL_FUNCTIONS, &["--"], false),
        Language::None => vec![(0, line.len(), Token::Plain)],
    }
}

fn lex(
    line: &str,
    keywords: &[&str],
    builtins: &[&str],
    comment_markers: &[&str],
    case_sensitive: bool,
) -> Vec<Span> {
    let bytes = line.as_bytes();
    let mut spans: Vec<Span> = Vec::new();
    let mut i = 0usize;
    // Set by `def`/`class`, so the next identifier is coloured as a definition.
    let mut expect_definition = false;

    while i < bytes.len() {
        // Comments run to the end of the line.
        if let Some(marker) = comment_markers.iter().find(|m| line[i..].starts_with(**m)) {
            let _ = marker;
            spans.push((i, line.len(), Token::Comment));
            break;
        }

        let c = bytes[i];
        if c == b'"' || c == b'\'' {
            let start = i;
            let quote = c;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    // Skip the escape and whatever it escapes, in bytes: an
                    // escaped multibyte character is still one `\` + one char.
                    // A backslash at end of line escapes nothing, and running
                    // past the buffer here would produce a span the renderer
                    // panics on when it slices.
                    i += 1;
                    if i >= bytes.len() {
                        break;
                    }
                    i += next_char_len(line, i);
                    continue;
                }
                if bytes[i] == quote {
                    i += 1;
                    break;
                }
                i += next_char_len(line, i);
            }
            spans.push((start, i.min(bytes.len()), Token::String));
            continue;
        }

        if c.is_ascii_digit() {
            let start = i;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.' || bytes[i] == b'_')
            {
                // An exponent sign belongs to the literal: `3.14e-2` is one
                // number, not a number minus a number.
                let exponent = matches!(bytes[i], b'e' | b'E')
                    && matches!(bytes.get(i + 1), Some(b'-') | Some(b'+'))
                    && bytes.get(i + 2).is_some_and(u8::is_ascii_digit);
                i += if exponent { 2 } else { 1 };
            }
            spans.push((start, i.min(bytes.len()), Token::Number));
            continue;
        }

        if is_word_byte(c) {
            let start = i;
            while i < bytes.len() && is_word_byte(bytes[i]) {
                i += 1;
            }
            let word = &line[start..i];
            let matches = |list: &[&str]| {
                if case_sensitive {
                    list.contains(&word)
                } else {
                    list.iter().any(|k| k.eq_ignore_ascii_case(word))
                }
            };
            let token = if expect_definition {
                expect_definition = false;
                Token::Definition
            } else if matches(keywords) {
                if word == "def" || word == "class" {
                    expect_definition = true;
                }
                Token::Keyword
            } else if matches(builtins) {
                Token::Builtin
            } else {
                Token::Plain
            };
            spans.push((start, i, token));
            continue;
        }

        // Everything else is punctuation and whitespace.
        let start = i;
        i += next_char_len(line, i);
        // Merge runs of plain text so the renderer emits fewer spans.
        match spans.last_mut() {
            Some(last) if last.2 == Token::Plain && last.1 == start => last.1 = i,
            _ => spans.push((start, i, Token::Plain)),
        }
    }
    spans
}

/// Length in bytes of the character starting at `index`.
fn next_char_len(line: &str, index: usize) -> usize {
    line[index..]
        .chars()
        .next()
        .map(char::len_utf8)
        .unwrap_or(1)
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

const PYTHON_KEYWORDS: &[&str] = &[
    "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue",
    "def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import",
    "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "while",
    "with", "yield", "match", "case",
];

const PYTHON_BUILTINS: &[&str] = &[
    "abs",
    "all",
    "any",
    "bool",
    "bytes",
    "dict",
    "dir",
    "enumerate",
    "filter",
    "float",
    "format",
    "frozenset",
    "getattr",
    "hasattr",
    "id",
    "int",
    "isinstance",
    "iter",
    "len",
    "list",
    "map",
    "max",
    "min",
    "next",
    "object",
    "open",
    "ord",
    "print",
    "range",
    "repr",
    "reversed",
    "round",
    "set",
    "setattr",
    "sorted",
    "str",
    "sum",
    "super",
    "tuple",
    "type",
    "zip",
    "self",
];

const SQL_KEYWORDS: &[&str] = &[
    "select",
    "from",
    "where",
    "group",
    "by",
    "order",
    "limit",
    "offset",
    "join",
    "left",
    "right",
    "inner",
    "outer",
    "full",
    "on",
    "having",
    "distinct",
    "as",
    "and",
    "or",
    "not",
    "null",
    "is",
    "in",
    "between",
    "like",
    "case",
    "when",
    "then",
    "else",
    "end",
    "with",
    "union",
    "all",
    "except",
    "intersect",
    "over",
    "partition",
    "asc",
    "desc",
    "cast",
    "interval",
    "create",
    "table",
    "insert",
    "into",
    "values",
    "update",
    "set",
    "delete",
    "using",
    "cross",
    "lateral",
];

const SQL_FUNCTIONS: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "abs",
    "round",
    "floor",
    "ceil",
    "coalesce",
    "nullif",
    "date_trunc",
    "now",
    "extract",
    "lag",
    "lead",
    "row_number",
    "rank",
    "dense_rank",
    "first_value",
    "last_value",
    "stddev",
    "variance",
    "percentile_cont",
    "array_agg",
    "length",
    "substr",
];

#[cfg(test)]
mod tests {
    use super::*;

    /// The tokens covering a line, as `(text, token)` pairs.
    fn tokens(line: &str, language: Language) -> Vec<(&str, Token)> {
        highlight_line(line, language)
            .into_iter()
            .map(|(start, end, token)| (&line[start..end], token))
            .collect()
    }

    /// Highlighting must never lose or duplicate a byte of the line.
    fn assert_covers(line: &str, language: Language) {
        let spans = highlight_line(line, language);
        let mut position = 0;
        for (start, end, _) in &spans {
            assert_eq!(*start, position, "gap or overlap in {line:?}: {spans:?}");
            assert!(end > start || start == end, "empty span in {line:?}");
            position = *end;
        }
        assert_eq!(position, line.len(), "spans did not cover {line:?}");
    }

    #[test]
    fn python_keywords_builtins_and_definitions() {
        let got = tokens("def load(path):", Language::Python);
        assert_eq!(got[0], ("def", Token::Keyword));
        assert_eq!(got[2], ("load", Token::Definition));
        assert!(tokens("print(len(x))", Language::Python).contains(&("print", Token::Builtin)));
    }

    #[test]
    fn strings_and_comments_are_recognised() {
        let got = tokens("x = 'abc'  # note", Language::Python);
        assert!(got.contains(&("'abc'", Token::String)), "{got:?}");
        assert!(got.contains(&("# note", Token::Comment)), "{got:?}");
    }

    #[test]
    fn a_hash_inside_a_string_is_not_a_comment() {
        let got = tokens("x = \"a # b\"", Language::Python);
        assert!(got.contains(&("\"a # b\"", Token::String)), "{got:?}");
        assert!(
            !got.iter().any(|(_, t)| *t == Token::Comment),
            "the # inside the string started a comment: {got:?}"
        );
    }

    #[test]
    fn escaped_quotes_do_not_end_a_string() {
        let got = tokens(r#"x = "a\"b" + y"#, Language::Python);
        assert!(got.contains(&(r#""a\"b""#, Token::String)), "{got:?}");
    }

    #[test]
    fn numbers_are_highlighted_including_floats_and_suffixes() {
        let got = tokens("x = 1_000 + 3.14e-2", Language::Python);
        assert!(got.contains(&("1_000", Token::Number)), "{got:?}");
        assert!(got.contains(&("3.14e-2", Token::Number)), "{got:?}");
    }

    #[test]
    fn sql_keywords_are_case_insensitive() {
        let upper = tokens("SELECT count(*) FROM trades", Language::Sql);
        let lower = tokens("select count(*) from trades", Language::Sql);
        assert!(upper.contains(&("SELECT", Token::Keyword)), "{upper:?}");
        assert!(lower.contains(&("select", Token::Keyword)), "{lower:?}");
        assert!(upper.contains(&("count", Token::Builtin)), "{upper:?}");
    }

    #[test]
    fn sql_comments_use_double_dash() {
        let got = tokens("SELECT 1 -- why", Language::Sql);
        assert!(got.contains(&("-- why", Token::Comment)), "{got:?}");
    }

    #[test]
    fn the_magic_line_is_its_own_token() {
        let got = tokens("%%sql --db market.db", Language::Sql);
        assert_eq!(got, vec![("%%sql --db market.db", Token::Magic)]);
    }

    #[test]
    fn a_sql_magic_selects_the_sql_lexer_whatever_the_notebook_says() {
        assert_eq!(
            Language::for_cell("%%sql\nSELECT 1", Some("python")),
            Language::Sql
        );
        assert_eq!(
            Language::for_cell("import x", Some("python")),
            Language::Python
        );
        assert_eq!(Language::for_cell("x", Some("julia")), Language::None);
        assert_eq!(Language::for_cell("x", None), Language::None);
    }

    #[test]
    fn spans_always_tile_the_line_exactly() {
        // The renderer slices the line by these spans, so a gap would drop
        // characters off the screen and an overlap would duplicate them.
        for line in [
            "",
            "x",
            "def f(a, b): return a + b  # add",
            "s = 'unterminated",
            "日本語 = 'テスト'  # コメント",
            "SELECT a, b FROM t WHERE x = 'q' -- c",
            "   ",
            "!@#$%^&*()",
            r#"x = "a\"#,
        ] {
            for language in [Language::Python, Language::Sql, Language::None] {
                assert_covers(line, language);
            }
        }
    }

    #[test]
    fn multibyte_lines_are_not_split_mid_character() {
        // Slicing a span boundary that fell inside a codepoint would panic.
        let line = "x = '日本語'  # コメント";
        for (start, end, _) in highlight_line(line, Language::Python) {
            assert!(line.is_char_boundary(start), "{start} is not a boundary");
            assert!(line.is_char_boundary(end), "{end} is not a boundary");
        }
    }

    #[test]
    fn an_unterminated_string_consumes_the_rest_of_the_line() {
        let got = tokens("x = 'abc", Language::Python);
        assert!(got.contains(&("'abc", Token::String)), "{got:?}");
    }
}
