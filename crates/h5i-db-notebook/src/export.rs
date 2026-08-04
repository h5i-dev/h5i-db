//! Converting a notebook to something that is not a notebook.
//!
//! Three targets, each with a different reader in mind: **markdown** for a
//! human reading a diff or a README, **python** for re-running the work
//! without any notebook machinery, and **html** for a self-contained file
//! that can be attached to a message and opened anywhere.
//!
//! HTML is emitted as a single file with the images inlined as `data:` URIs.
//! A directory of side-car assets is the one thing that reliably does not
//! survive being sent to somebody, and the whole point of exporting to HTML
//! is that the recipient needs nothing installed.

use std::fmt::Write as _;

use crate::document::{CellType, MimeBundle, Notebook, Output, StreamName};
use crate::error::{Error, Result};
use crate::render::strip_ansi;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Markdown,
    Python,
    Html,
}

impl Format {
    pub fn extension(self) -> &'static str {
        match self {
            Format::Markdown => "md",
            Format::Python => "py",
            Format::Html => "html",
        }
    }

    pub fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "md" | "markdown" => Ok(Format::Markdown),
            "py" | "python" | "script" => Ok(Format::Python),
            "html" => Ok(Format::Html),
            other => Err(Error::invalid(format!(
                "unknown export format {other:?}; supported: md, py, html"
            ))),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExportOptions {
    /// Leave outputs out entirely, for a clean diff or a runnable script.
    pub without_outputs: bool,
}

pub fn export(notebook: &Notebook, format: Format, options: &ExportOptions) -> String {
    match format {
        Format::Markdown => markdown(notebook, options),
        Format::Python => python(notebook, options),
        Format::Html => html(notebook, options),
    }
}

// ---------------------------------------------------------------------------
// markdown
// ---------------------------------------------------------------------------

fn markdown(notebook: &Notebook, options: &ExportOptions) -> String {
    let language = notebook.language().unwrap_or("python").to_string();
    let mut out = String::new();
    for cell in &notebook.cells {
        match cell.cell_type() {
            CellType::Markdown => {
                out.push_str(cell.source());
                ensure_blank_line(&mut out);
            }
            CellType::Raw => {
                // Raw cells are by definition not markdown, so they are fenced
                // rather than pasted in and left to be interpreted.
                write_fence(&mut out, "", cell.source());
                out.push('\n');
            }
            _ => {
                let fence_language = if is_sql_cell(cell.source()) {
                    "sql"
                } else {
                    &language
                };
                let source = strip_magic(cell.source());
                if !source.trim().is_empty() {
                    write_fence(&mut out, fence_language, source);
                    out.push('\n');
                }
                if options.without_outputs {
                    continue;
                }
                for output in cell.outputs() {
                    markdown_output(&mut out, output);
                }
            }
        }
    }
    while out.ends_with("\n\n") {
        out.pop();
    }
    out
}

fn markdown_output(out: &mut String, output: &Output) {
    match output {
        Output::Stream(stream) => {
            let text = strip_ansi(&stream.text);
            if text.trim().is_empty() {
                return;
            }
            write_fence(out, "", text.trim_end());
            out.push('\n');
        }
        Output::Error(error) => {
            let body = if error.traceback.is_empty() {
                format!("{}: {}", error.ename, error.evalue)
            } else {
                error
                    .traceback
                    .iter()
                    .map(|l| strip_ansi(l))
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            write_fence(out, "", body.trim_end());
            out.push('\n');
        }
        Output::ExecuteResult(_) | Output::DisplayData(_) => {
            let Some(bundle) = output.data() else { return };
            if let Some(payload) = bundle.text_of("image/png") {
                // Inline, because a markdown file that points at files the
                // reader does not have is worse than no image at all.
                let _ = writeln!(
                    out,
                    "![output](data:image/png;base64,{})\n",
                    compact_base64(payload)
                );
                return;
            }
            if let Some(text) = bundle.text_of("text/markdown") {
                let _ = writeln!(out, "{}\n", text.trim_end());
                return;
            }
            if let Some(text) = bundle.text_plain() {
                let text = strip_ansi(text);
                if !text.trim().is_empty() {
                    write_fence(out, "", text.trim_end());
                    out.push('\n');
                }
            }
        }
        Output::Unknown(_) => {}
    }
}

fn ensure_blank_line(out: &mut String) {
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push('\n');
}

// ---------------------------------------------------------------------------
// python
// ---------------------------------------------------------------------------

/// A runnable script, in the `# %%` cell format PyCharm, VS Code, and
/// jupytext all understand, so the result round-trips back into a notebook.
fn python(notebook: &Notebook, options: &ExportOptions) -> String {
    let _ = options;
    let mut out = String::new();
    if let Some(kernel) = notebook.kernel_name() {
        let _ = writeln!(out, "# ---");
        let _ = writeln!(out, "# jupyter:");
        let _ = writeln!(out, "#   kernelspec:");
        let _ = writeln!(out, "#     name: {kernel}");
        let _ = writeln!(out, "# ---");
        out.push('\n');
    }
    for cell in &notebook.cells {
        match cell.cell_type() {
            CellType::Markdown => {
                let _ = writeln!(out, "# %% [markdown]");
                for line in cell.source().split('\n') {
                    let _ = writeln!(out, "# {line}");
                }
            }
            CellType::Raw => {
                let _ = writeln!(out, "# %% [raw]");
                for line in cell.source().split('\n') {
                    let _ = writeln!(out, "# {line}");
                }
            }
            _ => {
                let _ = writeln!(out, "# %%");
                // A `%%sql` cell is not Python. Commenting it out keeps the
                // script importable, and losing it silently would be worse
                // than a comment the reader has to act on.
                if is_sql_cell(cell.source()) {
                    let _ = writeln!(out, "# (h5i-db SQL cell, not runnable as Python)");
                    for line in cell.source().split('\n') {
                        let _ = writeln!(out, "# {line}");
                    }
                } else {
                    let mut strings = TripleQuoteScanner::default();
                    for line in cell.source().split('\n') {
                        // IPython line magics and shell escapes are not Python
                        // and would make the script a SyntaxError. Commenting
                        // them out is what jupytext does, and it keeps the
                        // script runnable while leaving the intent visible.
                        // Inside a triple-quoted string they are data, and
                        // commenting them there changes the string's value.
                        let inside_string = strings.inside();
                        strings.feed(line);
                        if !inside_string && is_ipython_line(line) {
                            let _ = writeln!(out, "# {line}");
                        } else {
                            let _ = writeln!(out, "{line}");
                        }
                    }
                }
            }
        }
        out.push('\n');
    }
    out
}

// ---------------------------------------------------------------------------
// html
// ---------------------------------------------------------------------------

fn html(notebook: &Notebook, options: &ExportOptions) -> String {
    let title = notebook
        .metadata
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("notebook");
    let mut out = String::new();
    let _ = writeln!(
        out,
        "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{}</title>\n<style>{STYLE}</style>\n</head>\n<body>\n<main>",
        escape(title)
    );

    for cell in &notebook.cells {
        match cell.cell_type() {
            CellType::Markdown => {
                // Rendered as preformatted text rather than parsed: a markdown
                // parser is a dependency and a class of injection bugs, and an
                // exported notebook is read for its outputs.
                let _ = writeln!(
                    out,
                    "<section class=\"md\"><pre>{}</pre></section>",
                    escape(cell.source())
                );
            }
            CellType::Raw => {
                let _ = writeln!(
                    out,
                    "<section class=\"raw\"><pre>{}</pre></section>",
                    escape(cell.source())
                );
            }
            _ => {
                let count = cell
                    .as_code()
                    .and_then(|c| c.execution_count)
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| " ".to_string());
                let _ = writeln!(out, "<section class=\"cell\">");
                let _ = writeln!(
                    out,
                    "<div class=\"in\"><span class=\"count\">[{}]</span><pre>{}</pre></div>",
                    escape(&count),
                    escape(cell.source())
                );
                if !options.without_outputs {
                    for output in cell.outputs() {
                        html_output(&mut out, output);
                    }
                }
                let _ = writeln!(out, "</section>");
            }
        }
    }
    let _ = writeln!(out, "</main>\n</body>\n</html>");
    out
}

fn html_output(out: &mut String, output: &Output) {
    match output {
        Output::Stream(stream) => {
            let class = match stream.name {
                StreamName::Stderr => "out stderr",
                StreamName::Stdout => "out",
            };
            let _ = writeln!(
                out,
                "<div class=\"{class}\"><pre>{}</pre></div>",
                escape(&strip_ansi(&stream.text))
            );
        }
        Output::Error(error) => {
            let body = if error.traceback.is_empty() {
                format!("{}: {}", error.ename, error.evalue)
            } else {
                error
                    .traceback
                    .iter()
                    .map(|l| strip_ansi(l))
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            let _ = writeln!(
                out,
                "<div class=\"out error\"><pre>{}</pre></div>",
                escape(&body)
            );
        }
        Output::ExecuteResult(_) | Output::DisplayData(_) => {
            let Some(bundle) = output.data() else { return };
            let _ = writeln!(out, "<div class=\"out\">{}</div>", html_bundle(bundle));
        }
        Output::Unknown(_) => {}
    }
}

fn html_bundle(bundle: &MimeBundle) -> String {
    if let Some(payload) = bundle.text_of("image/png") {
        return format!(
            "<img alt=\"output\" src=\"data:image/png;base64,{}\">",
            compact_base64(payload)
        );
    }
    if let Some(payload) = bundle.text_of("image/jpeg") {
        return format!(
            "<img alt=\"output\" src=\"data:image/jpeg;base64,{}\">",
            compact_base64(payload)
        );
    }
    // SVG and HTML from a kernel are markup we did not write. They are escaped
    // rather than embedded: an exported notebook is a document to read, and
    // pasting arbitrary kernel output into the DOM makes opening it a risk.
    if let Some(text) = bundle.text_of("text/html") {
        return format!("<pre class=\"html-source\">{}</pre>", escape(text));
    }
    if let Some(text) = bundle.text_of("image/svg+xml") {
        return format!("<pre class=\"html-source\">{}</pre>", escape(text));
    }
    match bundle.text_plain() {
        Some(text) => format!("<pre>{}</pre>", escape(&strip_ansi(text))),
        None => format!(
            "<pre>[{}]</pre>",
            escape(&bundle.mime_types().collect::<Vec<_>>().join(", "))
        ),
    }
}

/// Write `body` inside a fence long enough to contain it.
///
/// Markdown ends a fenced block at the first line whose backtick run is at
/// least as long as the opening fence, so a cell that prints ```` ``` ````
/// closes its own output block: everything after it renders as prose, and
/// every later fence in the document is inverted. CommonMark's answer is a
/// longer fence, so the fence is sized to the content.
fn write_fence(out: &mut String, info: &str, body: &str) {
    let longest = body
        .lines()
        .map(|line| {
            let trimmed = line.trim_start();
            if trimmed.starts_with("```") {
                trimmed.chars().take_while(|c| *c == '`').count()
            } else {
                0
            }
        })
        .max()
        .unwrap_or(0);
    let fence = "`".repeat(longest.max(2) + 1);
    let _ = writeln!(out, "{fence}{info}\n{body}\n{fence}");
}

/// Base64 with the line breaks nbformat may have stored removed.
fn compact_base64(payload: &str) -> String {
    payload.chars().filter(|c| !c.is_whitespace()).collect()
}

fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// Whether a line is IPython syntax rather than Python.
///
/// No Python *statement* can begin with `%` or `!`. A line inside a
/// triple-quoted string can, which is why callers track that separately: it is
/// text, and commenting it out would change what the program contains.
fn is_ipython_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with('%') || trimmed.starts_with('!')
}

/// Tracks whether a line falls inside a triple-quoted Python string.
///
/// Deliberately not a Python parser: it follows `"""` and `'''` and the
/// escapes and single-quoted strings that could hide one, which is what it
/// takes to answer "is this line code or text". Anything subtler than that
/// (a `#` comment containing `"""`, say) errs towards leaving the line alone,
/// because printing a line unchanged is always safe and commenting one out
/// is not.
#[derive(Default)]
struct TripleQuoteScanner {
    open: Option<&'static str>,
}

impl TripleQuoteScanner {
    fn inside(&self) -> bool {
        self.open.is_some()
    }

    fn feed(&mut self, line: &str) {
        let bytes = line.as_bytes();
        let mut i = 0;
        // Only tracked while outside a triple-quoted string: a `'` inside one
        // is just a character.
        let mut short_quote: Option<u8> = None;
        while i < bytes.len() {
            let rest = &bytes[i..];
            if let Some(delimiter) = self.open {
                if rest.starts_with(delimiter.as_bytes()) {
                    self.open = None;
                    i += 3;
                    continue;
                }
                // A backslash inside a string escapes whatever follows.
                i += if rest[0] == b'\\' { 2 } else { 1 };
                continue;
            }
            match short_quote {
                Some(quote) => {
                    if rest[0] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if rest[0] == quote {
                        short_quote = None;
                    }
                    i += 1;
                }
                None => {
                    if rest.starts_with(b"\"\"\"") {
                        self.open = Some("\"\"\"");
                        i += 3;
                        continue;
                    }
                    if rest.starts_with(b"'''") {
                        self.open = Some("'''");
                        i += 3;
                        continue;
                    }
                    // A comment cannot open a string, and the rest of the line
                    // is not code.
                    if rest[0] == b'#' {
                        return;
                    }
                    if rest[0] == b'"' || rest[0] == b'\'' {
                        short_quote = Some(rest[0]);
                    }
                    i += 1;
                }
            }
        }
    }
}

/// Whether this cell is one the SQL kernel would run.
///
/// Asked of [`magic::route`] rather than tested with `starts_with("%%sql")`,
/// so export agrees with what actually executes: `%%sqlalchemy` is somebody
/// else's magic and runs as Python, and treating it as ours commented a
/// working cell out of the exported script.
fn is_sql_cell(source: &str) -> bool {
    matches!(
        crate::magic::route(source),
        Ok((crate::magic::CellRoute::Sql(_), _))
    )
}

/// Drop a leading `%%sql` line so exported source reads as the SQL it is.
fn strip_magic(source: &str) -> &str {
    match crate::magic::route(source) {
        Ok((crate::magic::CellRoute::Sql(_), body)) => body,
        _ => source,
    }
}

const STYLE: &str = "\
:root{color-scheme:light dark}\
body{margin:0;padding:2rem 1rem;font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace}\
main{max-width:60rem;margin:0 auto}\
pre{margin:0;white-space:pre-wrap;overflow-wrap:anywhere}\
.cell{margin:0 0 1.5rem}\
.in{display:flex;gap:.75rem;padding:.6rem .8rem;border-left:3px solid #6b8afd;background:color-mix(in srgb,currentColor 4%,transparent)}\
.count{color:#6b8afd;flex:0 0 auto}\
.out{padding:.4rem .8rem .4rem 3.1rem}\
.out.stderr pre,.out.error pre{color:#c1443b}\
.md{margin:0 0 1.25rem;font-family:ui-sans-serif,system-ui,sans-serif}\
.raw{margin:0 0 1.25rem;opacity:.7}\
img{max-width:100%;height:auto}\
.html-source{opacity:.75}";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{Cell, DisplayDataOutput, ErrorOutput, ExecuteResultOutput};
    use serde_json::json;

    fn notebook(cells: Vec<Cell>) -> Notebook {
        let mut notebook = Notebook::new("python3", "Python 3", "python");
        for cell in cells {
            notebook.push(cell);
        }
        notebook
    }

    fn code_with(source: &str, outputs: Vec<Output>) -> Cell {
        let mut cell = Cell::new_code(source);
        if let Some(code) = cell.as_code_mut() {
            code.outputs = outputs;
            code.execution_count = Some(1);
        }
        cell
    }

    #[test]
    fn a_fence_grows_past_backticks_in_the_content() {
        // A cell that prints ``` used to close its own output block, so
        // everything after it rendered as prose and every later fence in the
        // document was inverted.
        let mut nb = Notebook::new("python3", "Python 3", "python");
        let mut cell = Cell::new_code("print('```')");
        if let Some(code) = cell.as_code_mut() {
            code.outputs
                .push(Output::Stream(crate::document::StreamOutput {
                    name: StreamName::Stdout,
                    text: "```\nnot a fence\n```\n".to_string(),
                    extra: Default::default(),
                }));
        }
        nb.push(cell);
        let text = markdown(&nb, &ExportOptions::default());

        // The output's own backticks must sit inside a longer fence.
        assert!(text.contains("````\n```\nnot a fence\n```\n````"), "{text}");
    }

    #[test]
    fn a_cell_whose_source_holds_a_fence_is_still_fenced_correctly() {
        let mut nb = Notebook::new("python3", "Python 3", "python");
        nb.push(Cell::new_code("doc = \"\"\"\n```\n\"\"\""));
        let text = markdown(&nb, &ExportOptions::default());
        let opening = text.lines().next().unwrap();
        assert!(
            opening.starts_with("````"),
            "the fence did not grow: {text}"
        );
    }

    #[test]
    fn only_our_own_sql_magic_is_treated_as_sql() {
        // `%%sqlalchemy` belongs to some other extension and runs as Python,
        // so exporting it as a commented-out SQL cell deleted working code
        // from the script.
        let mut nb = Notebook::new("python3", "Python 3", "python");
        nb.push(Cell::new_code("%%sqlalchemy\nengine.execute(q)"));
        let script = python(&nb, &ExportOptions::default());
        assert!(
            script.contains("engine.execute(q)"),
            "the cell was lost: {script}"
        );
        assert!(
            !script.contains("not runnable as Python"),
            "a Python cell was labelled SQL: {script}"
        );

        let text = markdown(&nb, &ExportOptions::default());
        assert!(
            text.contains("%%sqlalchemy"),
            "the magic line was stripped: {text}"
        );
    }

    #[test]
    fn magic_lines_inside_a_string_are_left_alone() {
        // Commenting one out changes what the string contains, so the script
        // no longer does what the notebook did.
        let mut nb = Notebook::new("python3", "Python 3", "python");
        nb.push(Cell::new_code(
            "template = \"\"\"\n% total\n!important\n\"\"\"\n%time run()",
        ));
        let script = python(&nb, &ExportOptions::default());
        assert!(
            script.contains("\n% total\n"),
            "string was edited: {script}"
        );
        assert!(
            script.contains("\n!important\n"),
            "string was edited: {script}"
        );
        // The real magic, outside the string, still gets commented out.
        assert!(script.contains("# %time run()"), "{script}");
    }

    #[test]
    fn formats_parse_from_their_common_names() {
        assert_eq!(Format::parse("md").unwrap(), Format::Markdown);
        assert_eq!(Format::parse("markdown").unwrap(), Format::Markdown);
        assert_eq!(Format::parse("PY").unwrap(), Format::Python);
        assert_eq!(Format::parse("html").unwrap(), Format::Html);
        let error = Format::parse("pdf").unwrap_err();
        assert!(error.to_string().contains("md, py, html"), "{error}");
    }

    #[test]
    fn markdown_fences_code_and_output_separately() {
        let nb = notebook(vec![
            Cell::new_markdown("# Title"),
            code_with(
                "print('hi')",
                vec![Output::stream(StreamName::Stdout, "hi\n")],
            ),
        ]);
        let out = markdown(&nb, &ExportOptions::default());
        assert!(out.contains("# Title"), "{out}");
        assert!(out.contains("```python\nprint('hi')\n```"), "{out}");
        assert!(out.contains("```\nhi\n```"), "{out}");
    }

    #[test]
    fn a_sql_cell_exports_as_sql_without_its_magic_line() {
        let nb = notebook(vec![code_with("%%sql\nSELECT 1", Vec::new())]);
        let out = markdown(&nb, &ExportOptions::default());
        assert!(out.contains("```sql\nSELECT 1\n```"), "{out}");
        assert!(!out.contains("%%sql"), "the magic line leaked: {out}");
    }

    #[test]
    fn without_outputs_drops_them_from_every_format() {
        let nb = notebook(vec![code_with(
            "x",
            vec![Output::stream(StreamName::Stdout, "secret\n")],
        )]);
        let options = ExportOptions {
            without_outputs: true,
        };
        for format in [Format::Markdown, Format::Html] {
            let out = export(&nb, format, &options);
            assert!(!out.contains("secret"), "{format:?}: {out}");
        }
    }

    #[test]
    fn python_export_uses_the_cell_format_other_tools_read() {
        let nb = notebook(vec![
            Cell::new_markdown("Some prose\nover two lines"),
            code_with("x = 1", Vec::new()),
        ]);
        let out = python(&nb, &ExportOptions::default());
        assert!(out.contains("# %% [markdown]"), "{out}");
        assert!(out.contains("# Some prose"), "{out}");
        assert!(out.contains("# over two lines"), "{out}");
        assert!(out.contains("# %%\nx = 1"), "{out}");
        assert!(out.contains("name: python3"), "{out}");
    }

    #[test]
    fn a_sql_cell_is_commented_out_rather_than_dropped_from_a_script() {
        // Emitting it as bare Python would make the script a syntax error;
        // dropping it would silently lose work.
        let nb = notebook(vec![code_with("%%sql\nSELECT 1", Vec::new())]);
        let out = python(&nb, &ExportOptions::default());
        assert!(out.contains("# %%sql"), "{out}");
        assert!(out.contains("not runnable as Python"), "{out}");
        for line in out.lines().filter(|l| !l.trim().is_empty()) {
            assert!(
                line.starts_with('#') || line.starts_with("x"),
                "uncommented SQL leaked into the script: {line:?}"
            );
        }
    }

    #[test]
    fn ipython_magics_are_commented_out_of_a_python_export() {
        // `%matplotlib inline` is a SyntaxError in a plain script, so a export
        // that emitted it verbatim would not run at all.
        let nb = notebook(vec![code_with(
            "%matplotlib inline\n!pip install x\nimport matplotlib\nx = 100 % 7",
            Vec::new(),
        )]);
        let out = python(&nb, &ExportOptions::default());
        assert!(out.contains("# %matplotlib inline"), "{out}");
        assert!(out.contains("# !pip install x"), "{out}");
        assert!(out.contains("\nimport matplotlib"), "{out}");
        // A modulo is not a magic.
        assert!(out.contains("\nx = 100 % 7"), "{out}");
    }

    #[test]
    fn html_is_one_self_contained_file_with_the_image_inlined() {
        let mut bundle = MimeBundle::new();
        bundle.insert("image/png", json!("iVBORw0KGgo="));
        let nb = notebook(vec![code_with(
            "plot()",
            vec![Output::DisplayData(DisplayDataOutput {
                data: bundle,
                metadata: Default::default(),
                extra: Default::default(),
            })],
        )]);
        let out = html(&nb, &ExportOptions::default());
        assert!(out.starts_with("<!doctype html>"), "{out}");
        assert!(
            out.contains("src=\"data:image/png;base64,iVBORw0KGgo=\""),
            "{out}"
        );
        // Nothing may be fetched from elsewhere; the file has to open offline.
        assert!(!out.contains("http://"), "{out}");
        assert!(!out.contains("https://"), "{out}");
        assert!(!out.contains("<script"), "{out}");
    }

    #[test]
    fn html_escapes_source_and_output() {
        let nb = notebook(vec![code_with(
            "print('<script>alert(1)</script>')",
            vec![Output::stream(StreamName::Stdout, "<img onerror=x>\n")],
        )]);
        let out = html(&nb, &ExportOptions::default());
        assert!(
            !out.contains("<script>alert"),
            "source was not escaped: {out}"
        );
        assert!(
            !out.contains("<img onerror"),
            "output was not escaped: {out}"
        );
        assert!(out.contains("&lt;script&gt;alert(1)"), "{out}");
    }

    #[test]
    fn kernel_html_output_is_shown_as_source_not_embedded() {
        // A kernel can emit arbitrary markup; embedding it would make opening
        // an exported notebook a way to run somebody else's HTML.
        let mut bundle = MimeBundle::new();
        bundle.insert("text/html", json!("<b onclick='steal()'>table</b>"));
        let nb = notebook(vec![code_with(
            "df",
            vec![Output::ExecuteResult(ExecuteResultOutput {
                data: bundle,
                metadata: Default::default(),
                execution_count: Some(1),
                extra: Default::default(),
            })],
        )]);
        let out = html(&nb, &ExportOptions::default());
        assert!(!out.contains("onclick='steal()'"), "{out}");
        assert!(out.contains("&lt;b onclick="), "{out}");
    }

    #[test]
    fn tracebacks_survive_export_with_their_ansi_stripped() {
        let nb = notebook(vec![code_with(
            "1/0",
            vec![Output::Error(ErrorOutput {
                ename: "ZeroDivisionError".into(),
                evalue: "division by zero".into(),
                traceback: vec![
                    "\u{1b}[0;31mZeroDivisionError\u{1b}[0m".into(),
                    "  line 1".into(),
                ],
                extra: Default::default(),
            })],
        )]);
        for format in [Format::Markdown, Format::Html] {
            let out = export(&nb, format, &ExportOptions::default());
            assert!(out.contains("ZeroDivisionError"), "{format:?}: {out}");
            assert!(!out.contains('\u{1b}'), "{format:?} kept ANSI escapes");
        }
    }

    #[test]
    fn an_empty_notebook_exports_to_something_valid() {
        let nb = notebook(Vec::new());
        assert_eq!(markdown(&nb, &ExportOptions::default()), "");
        let out = html(&nb, &ExportOptions::default());
        assert!(out.starts_with("<!doctype html>") && out.trim_end().ends_with("</html>"));
        assert!(python(&nb, &ExportOptions::default()).contains("name: python3"));
    }

    #[test]
    fn base64_stored_across_lines_is_rejoined_for_the_data_uri() {
        // nbformat may split a payload; a newline inside a data: URI breaks it.
        let mut bundle = MimeBundle::new();
        bundle.insert("image/png", json!("iVBO\nRw0K"));
        let nb = notebook(vec![code_with(
            "plot()",
            vec![Output::DisplayData(DisplayDataOutput {
                data: bundle,
                metadata: Default::default(),
                extra: Default::default(),
            })],
        )]);
        let out = html(&nb, &ExportOptions::default());
        assert!(out.contains("base64,iVBORw0K\""), "{out}");
    }
}
