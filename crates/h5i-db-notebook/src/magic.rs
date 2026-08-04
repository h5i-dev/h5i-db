//! Cell magics, resolved on the client rather than in the kernel.
//!
//! `%%sql` has to be handled here because it selects *which kernel runs the
//! cell*. IPython's own `%%sql` (from ipython-sql) is the opposite: a Python
//! kernel extension that opens its own connection, which means the query pays
//! for a Python interpreter and a driver round trip. Routing on the client
//! sends the statement straight into the in-process engine.
//!
//! The syntax deliberately matches IPython's, so a notebook written here still
//! reads correctly in JupyterLab even though JupyterLab would run the cell a
//! different way.

use crate::error::{Error, Result};

/// Which backend a cell is destined for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellRoute {
    /// Ordinary code for the notebook's Jupyter kernel.
    Kernel,
    /// A `%%sql` cell for the native engine.
    Sql(SqlMagic),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqlMagic {
    /// Override the notebook's database for this cell.
    pub database: Option<String>,
    /// Read the result into this variable in the Python kernel.
    pub into: Option<String>,
    /// Per-cell row cap.
    pub max_rows: Option<usize>,
    /// Query a fork rather than the base database.
    pub fork: Option<String>,
    /// Open the database read-write for this cell.
    ///
    /// Off by default: a notebook is usually read *about* a database, and a
    /// read-write open runs transaction recovery, which is a mutation the
    /// reader of a shared database did not ask for.
    pub write: bool,
}

/// Split a cell into its route and the body the target kernel should run.
pub fn route(source: &str) -> Result<(CellRoute, &str)> {
    let Some(rest) = source.strip_prefix("%%sql") else {
        return Ok((CellRoute::Kernel, source));
    };
    // `%%sqlfoo` is not a sql magic; the magic name has to end at the line.
    let (arguments, body) = match rest.split_once('\n') {
        Some((arguments, body)) => (arguments, body),
        None => (rest, ""),
    };
    if !arguments.is_empty() && !arguments.starts_with(char::is_whitespace) {
        return Ok((CellRoute::Kernel, source));
    }
    Ok((CellRoute::Sql(parse_sql_magic(arguments)?), body))
}

fn parse_sql_magic(arguments: &str) -> Result<SqlMagic> {
    let mut magic = SqlMagic::default();
    let mut tokens = split_arguments(arguments).into_iter();
    while let Some(token) = tokens.next() {
        let mut expect_value = |flag: &str| -> Result<String> {
            tokens.next().ok_or_else(|| {
                Error::invalid(format!(
                    "%%sql {flag} needs a value, for example `{flag} x`"
                ))
            })
        };
        match token.as_str() {
            "--db" | "--database" => magic.database = Some(expect_value("--db")?),
            "--into" => {
                let name = expect_value("--into")?;
                if !is_identifier(&name) {
                    return Err(Error::invalid(format!(
                        "%%sql --into {name:?} is not a valid Python identifier"
                    )));
                }
                magic.into = Some(name);
            }
            "--fork" => magic.fork = Some(expect_value("--fork")?),
            "--write" => magic.write = true,
            "--max-rows" => {
                let raw = expect_value("--max-rows")?;
                magic.max_rows = Some(raw.parse().map_err(|_| {
                    Error::invalid(format!("%%sql --max-rows {raw:?} is not a number"))
                })?);
            }
            other => {
                return Err(Error::invalid(format!(
                    "unknown %%sql option {other:?}; supported: --db, --fork, --into, \
                     --max-rows, --write"
                )));
            }
        }
    }
    Ok(magic)
}

/// Split on whitespace, honouring single and double quotes so a path with a
/// space in it survives.
fn split_arguments(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut has_token = false;

    for c in text.chars() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => current.push(c),
            None if c == '"' || c == '\'' => {
                quote = Some(c);
                has_token = true;
            }
            None if c.is_whitespace() => {
                if has_token {
                    out.push(std::mem::take(&mut current));
                    has_token = false;
                }
            }
            None => {
                current.push(c);
                has_token = true;
            }
        }
    }
    if has_token {
        out.push(current);
    }
    out
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_alphanumeric() || c == '_')
}

/// Python that loads an Arrow IPC file written by a `%%sql --into` cell.
///
/// A pyarrow `Table` is the guaranteed result and pandas is used only when it
/// is importable. The generated cell prints which one it produced, so the type
/// is never a guess: an agent that has to ask "is this a DataFrame?" has
/// already lost the benefit.
pub fn into_variable_code(name: &str, ipc_path: &str) -> String {
    format!(
        r#"import pyarrow.ipc as _h5i_ipc
with _h5i_ipc.open_file({path}) as _h5i_reader:
    _h5i_table = _h5i_reader.read_all()
try:
    import pandas as _h5i_pd  # noqa: F401
    {name} = _h5i_table.to_pandas()
    print(f"{name}: pandas.DataFrame {{{name}.shape[0]}} rows x {{{name}.shape[1]}} columns")
except ImportError:
    {name} = _h5i_table
    print(f"{name}: pyarrow.Table {{{name}.num_rows}} rows x {{{name}.num_columns}} columns")
del _h5i_table, _h5i_reader
"#,
        path = python_string_literal(ipc_path),
        name = name,
    )
}

/// A Python string literal, escaped rather than interpolated.
///
/// The path comes from a temp directory we chose, but building source code by
/// concatenation is how injection bugs start, so it is escaped anyway.
fn python_string_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_cell_routes_to_the_kernel_untouched() {
        let (route, body) = route("import pandas\nx = 1").unwrap();
        assert_eq!(route, CellRoute::Kernel);
        assert_eq!(body, "import pandas\nx = 1");
    }

    #[test]
    fn a_sql_cell_routes_to_the_engine_with_the_magic_line_removed() {
        let (route, body) = route("%%sql\nSELECT 1").unwrap();
        assert_eq!(route, CellRoute::Sql(SqlMagic::default()));
        assert_eq!(body, "SELECT 1");
    }

    #[test]
    fn options_are_parsed_off_the_magic_line() {
        let (route, body) =
            route("%%sql --db market.db --into df --max-rows 50 --fork bt-1\nSELECT *").unwrap();
        assert_eq!(
            route,
            CellRoute::Sql(SqlMagic {
                database: Some("market.db".into()),
                into: Some("df".into()),
                max_rows: Some(50),
                fork: Some("bt-1".into()),
                write: false,
            })
        );
        assert_eq!(body, "SELECT *");
    }

    #[test]
    fn write_is_opt_in_per_cell() {
        let (writing, body) = route("%%sql --write\nINSERT INTO t VALUES (1)").unwrap();
        match writing {
            CellRoute::Sql(magic) => assert!(magic.write),
            other => panic!("{other:?}"),
        }
        assert_eq!(body, "INSERT INTO t VALUES (1)");
        // Absent, a cell must not be able to write: that is the whole point.
        let (reading, _) = route("%%sql\nSELECT 1").unwrap();
        match reading {
            CellRoute::Sql(magic) => assert!(!magic.write),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn quoted_paths_survive_spaces() {
        let (route, _) = route("%%sql --db \"/tmp/my data/market.db\"\nSELECT 1").unwrap();
        match route {
            CellRoute::Sql(magic) => {
                assert_eq!(magic.database.as_deref(), Some("/tmp/my data/market.db"))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_magic_with_no_body_is_still_a_sql_cell() {
        let (route, body) = route("%%sql").unwrap();
        assert!(matches!(route, CellRoute::Sql(_)));
        assert_eq!(body, "");
    }

    #[test]
    fn a_word_starting_with_sql_is_not_a_magic() {
        // `%%sqlalchemy` belongs to whatever extension defines it, not to us.
        let (route, body) = route("%%sqlalchemy\nSELECT 1").unwrap();
        assert_eq!(route, CellRoute::Kernel);
        assert_eq!(body, "%%sqlalchemy\nSELECT 1");
    }

    #[test]
    fn a_magic_not_at_the_start_of_the_cell_is_ordinary_code() {
        let (route, _) = route("x = 1\n%%sql\nSELECT 1").unwrap();
        assert_eq!(route, CellRoute::Kernel);
    }

    #[test]
    fn unknown_options_are_rejected_with_the_supported_list() {
        let error = route("%%sql --explain\nSELECT 1").unwrap_err();
        assert_eq!(error.code(), "invalid_input");
        assert!(error.to_string().contains("--db"), "{error}");
    }

    #[test]
    fn a_flag_missing_its_value_says_so() {
        let error = route("%%sql --db\nSELECT 1").unwrap_err();
        assert!(error.to_string().contains("needs a value"), "{error}");
    }

    #[test]
    fn into_must_name_a_valid_python_identifier() {
        assert!(route("%%sql --into 2df\nSELECT 1").is_err());
        assert!(route("%%sql --into my-df\nSELECT 1").is_err());
        assert!(route("%%sql --into _ok2\nSELECT 1").is_ok());
    }

    #[test]
    fn generated_python_escapes_the_path() {
        let code = into_variable_code("df", "/tmp/a\"b\\c.arrow");
        assert!(code.contains(r#""/tmp/a\"b\\c.arrow""#), "{code}");
        assert!(code.contains("df = _h5i_table.to_pandas()"), "{code}");
    }

    #[test]
    fn argument_splitting_handles_the_awkward_cases() {
        assert_eq!(split_arguments(""), Vec::<String>::new());
        assert_eq!(split_arguments("   "), Vec::<String>::new());
        assert_eq!(split_arguments("a  b"), vec!["a", "b"]);
        assert_eq!(split_arguments("'a b' c"), vec!["a b", "c"]);
        // An empty quoted string is a real token, not nothing.
        assert_eq!(split_arguments("''"), vec![""]);
    }
}
