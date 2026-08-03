//! Rendering outputs for an agent reading a terminal.
//!
//! The premise of the crate is that a notebook costs an agent fewer tokens
//! than `python script.py` did, so the default must be a budgeted digest, not
//! a transcript. Three rules do most of the work:
//!
//! - long text keeps its head and tail and elides the middle, because the
//!   interesting parts of a 10,000-line dump are at the ends;
//! - `\r` progress redraws collapse to their final frame, because a tqdm bar
//!   otherwise costs hundreds of near-identical lines;
//! - images are never inlined as base64, only referenced by path.
//!
//! The untruncated output always stays in the `.ipynb` and is retrievable with
//! `nb output --raw`, which is deliberately the same summarise-then-rehydrate
//! contract as `h5i capture run` / `h5i recall object`.

use crate::document::{MimeBundle, Output};

#[derive(Debug, Clone)]
pub struct DigestOptions {
    /// Lines kept from the start of a long text output.
    pub head_lines: usize,
    /// Lines kept from the end.
    pub tail_lines: usize,
    /// Hard cap on a single line before it is truncated mid-line.
    pub max_line_width: usize,
    /// Emit the full text with no elision.
    pub raw: bool,
}

impl Default for DigestOptions {
    fn default() -> Self {
        DigestOptions {
            head_lines: 40,
            tail_lines: 20,
            max_line_width: 2000,
            raw: false,
        }
    }
}

/// Mime types worth showing as text, best first.
const TEXT_PRIORITY: &[&str] = &[
    "text/plain",
    "text/markdown",
    "application/vnd.h5i.table+text",
];

/// Render one cell's outputs for a terminal.
pub fn digest(outputs: &[Output], options: &DigestOptions) -> String {
    let mut parts = Vec::new();
    for output in outputs {
        match output {
            Output::Stream(stream) => {
                let text = collapse_carriage_returns(&stream.text);
                let body = elide(&text, options);
                if body.trim().is_empty() {
                    continue;
                }
                if stream.name == crate::document::StreamName::Stderr {
                    parts.push(prefix_lines(&body, "stderr: "));
                } else {
                    parts.push(body);
                }
            }
            Output::ExecuteResult(result) => {
                parts.push(render_bundle(&result.data, options));
            }
            Output::DisplayData(display) => {
                parts.push(render_bundle(&display.data, options));
            }
            Output::Error(error) => {
                // Tracebacks are never elided: every frame matters when the
                // reader has to work out which call raised.
                let traceback = error
                    .traceback
                    .iter()
                    .map(|line| strip_ansi(line))
                    .collect::<Vec<_>>()
                    .join("\n");
                if traceback.trim().is_empty() {
                    parts.push(format!("{}: {}", error.ename, error.evalue));
                } else {
                    parts.push(traceback);
                }
            }
            Output::Unknown(map) => {
                parts.push(format!("[{} output]", output_type_of(map)));
            }
        }
    }
    parts.retain(|p| !p.trim().is_empty());
    parts.join("\n")
}

fn output_type_of(map: &serde_json::Map<String, serde_json::Value>) -> String {
    map.get("output_type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string()
}

fn render_bundle(bundle: &MimeBundle, options: &DigestOptions) -> String {
    if let Some(mime) = bundle.richest(TEXT_PRIORITY)
        && let Some(text) = bundle.text_of(mime)
    {
        return elide(&strip_ansi(text), options);
    }
    // No text representation: say what is there rather than dumping base64.
    let mimes: Vec<&str> = bundle.mime_types().collect();
    if mimes.is_empty() {
        return String::new();
    }
    format!("[{}]", mimes.join(", "))
}

/// Keep only the last frame of each `\r`-overwritten line.
///
/// Progress bars redraw by returning to the start of the line, so the raw
/// stream holds every intermediate frame. Only the final one carries
/// information, and dropping the rest routinely cuts the output by 99%.
pub fn collapse_carriage_returns(text: &str) -> String {
    if !text.contains('\r') {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    for (i, line) in text.split_inclusive('\n').enumerate() {
        let _ = i;
        let (body, newline) = match line.strip_suffix('\n') {
            Some(body) => (body, "\n"),
            None => (line, ""),
        };
        // `\r\n` line endings are not progress redraws.
        let body = body.strip_suffix('\r').unwrap_or(body);
        let final_frame = body.rsplit('\r').next().unwrap_or(body);
        out.push_str(final_frame);
        out.push_str(newline);
    }
    out
}

/// Head/tail elision with a count of what was dropped.
fn elide(text: &str, options: &DigestOptions) -> String {
    let text = truncate_long_lines(text, options);
    if options.raw {
        return text;
    }
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let budget = options.head_lines + options.tail_lines;
    if lines.len() <= budget + 1 {
        return text;
    }
    let dropped = lines.len() - budget;
    let mut out = String::new();
    for line in &lines[..options.head_lines] {
        out.push_str(line);
    }
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(&format!("… {dropped} lines elided …\n"));
    for line in &lines[lines.len() - options.tail_lines..] {
        out.push_str(line);
    }
    out
}

fn truncate_long_lines(text: &str, options: &DigestOptions) -> String {
    if options.raw {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let (body, newline) = match line.strip_suffix('\n') {
            Some(body) => (body, "\n"),
            None => (line, ""),
        };
        if body.chars().count() > options.max_line_width {
            let kept: String = body.chars().take(options.max_line_width).collect();
            let dropped = body.chars().count() - options.max_line_width;
            out.push_str(&kept);
            out.push_str(&format!("… (+{dropped} chars)"));
        } else {
            out.push_str(body);
        }
        out.push_str(newline);
    }
    out
}

/// Remove ANSI escape sequences.
///
/// IPython colours its tracebacks, and the escape bytes are pure cost for a
/// consumer that is reading rather than displaying.
pub fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            out.push(c);
            continue;
        }
        match chars.peek() {
            // CSI: ends at the first byte in 0x40..=0x7e.
            Some('[') => {
                chars.next();
                for c in chars.by_ref() {
                    if ('\u{40}'..='\u{7e}').contains(&c) {
                        break;
                    }
                }
            }
            // OSC: ends at BEL or ST.
            Some(']') => {
                chars.next();
                while let Some(c) = chars.next() {
                    if c == '\u{7}' {
                        break;
                    }
                    if c == '\u{1b}' && chars.peek() == Some(&'\\') {
                        chars.next();
                        break;
                    }
                }
            }
            // Two-character escape.
            Some(_) => {
                chars.next();
            }
            None => {}
        }
    }
    out
}

fn prefix_lines(text: &str, prefix: &str) -> String {
    text.split_inclusive('\n')
        .map(|line| format!("{prefix}{line}"))
        .collect::<String>()
}

/// One-line summary of what a cell produced, for `nb cells`.
pub fn summarize(outputs: &[Output]) -> String {
    if outputs.is_empty() {
        return "-".to_string();
    }
    let mut parts = Vec::new();
    for output in outputs {
        match output {
            Output::Stream(s) => {
                let lines = s.text.lines().count().max(1);
                parts.push(format!("{} {lines}L", s.name.as_str()));
            }
            Output::Error(e) => parts.push(e.ename.clone()),
            Output::ExecuteResult(r) => parts.push(describe_bundle("result", &r.data)),
            Output::DisplayData(d) => parts.push(describe_bundle("display", &d.data)),
            Output::Unknown(_) => parts.push(output.output_type().to_string()),
        }
    }
    parts.join(", ")
}

fn describe_bundle(label: &str, bundle: &MimeBundle) -> String {
    let interesting: Vec<&str> = bundle.mime_types().filter(|m| *m != "text/plain").collect();
    if interesting.is_empty() {
        label.to_string()
    } else {
        format!("{label}({})", interesting.join("+"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{ErrorOutput, ExecuteResultOutput, StreamName, StreamOutput};
    use serde_json::json;

    fn stream(text: &str) -> Output {
        Output::Stream(StreamOutput {
            name: StreamName::Stdout,
            text: text.to_string(),
            extra: Default::default(),
        })
    }

    #[test]
    fn short_output_passes_through_untouched() {
        let out = digest(&[stream("hello\nworld\n")], &DigestOptions::default());
        assert_eq!(out, "hello\nworld\n");
    }

    #[test]
    fn long_output_keeps_both_ends_and_reports_the_gap() {
        let text: String = (0..1000).map(|i| format!("line {i}\n")).collect();
        let options = DigestOptions {
            head_lines: 3,
            tail_lines: 2,
            ..Default::default()
        };
        let out = digest(&[stream(&text)], &options);
        assert!(out.starts_with("line 0\nline 1\nline 2\n"), "{out}");
        assert!(out.ends_with("line 998\nline 999\n"), "{out}");
        assert!(out.contains("… 995 lines elided …"), "{out}");
        assert!(out.lines().count() < 10);
    }

    #[test]
    fn raw_mode_elides_nothing() {
        let text: String = (0..1000).map(|i| format!("line {i}\n")).collect();
        let options = DigestOptions {
            raw: true,
            ..Default::default()
        };
        let out = digest(&[stream(&text)], &options);
        assert_eq!(out.lines().count(), 1000);
        assert!(!out.contains("elided"));
    }

    #[test]
    fn progress_bar_redraws_collapse_to_the_final_frame() {
        // What a tqdm bar actually looks like on the wire.
        let text = "  0%|          | 0/100\r 50%|#####     | 50/100\r100%|##########| 100/100\n";
        let out = collapse_carriage_returns(text);
        assert_eq!(out, "100%|##########| 100/100\n");
    }

    #[test]
    fn crlf_line_endings_are_not_mistaken_for_redraws() {
        assert_eq!(collapse_carriage_returns("a\r\nb\r\n"), "a\nb\n");
    }

    #[test]
    fn tracebacks_are_never_elided() {
        let traceback: Vec<String> = (0..500).map(|i| format!("  frame {i}")).collect();
        let output = Output::Error(ErrorOutput {
            ename: "ValueError".into(),
            evalue: "bad".into(),
            traceback,
            extra: Default::default(),
        });
        let options = DigestOptions {
            head_lines: 2,
            tail_lines: 1,
            ..Default::default()
        };
        let out = digest(&[output], &options);
        assert!(!out.contains("elided"), "a traceback was truncated");
        assert!(out.contains("frame 0") && out.contains("frame 499"));
    }

    #[test]
    fn ansi_colour_is_stripped_from_tracebacks() {
        let output = Output::Error(ErrorOutput {
            ename: "E".into(),
            evalue: "v".into(),
            traceback: vec!["\u{1b}[0;31mValueError\u{1b}[0m: bad".into()],
            extra: Default::default(),
        });
        let out = digest(&[output], &DigestOptions::default());
        assert_eq!(out, "ValueError: bad");
        assert!(!out.contains('\u{1b}'));
    }

    #[test]
    fn strip_ansi_handles_osc_and_two_char_escapes() {
        assert_eq!(strip_ansi("a\u{1b}]0;title\u{7}b"), "ab");
        assert_eq!(strip_ansi("a\u{1b}]0;t\u{1b}\\b"), "ab");
        assert_eq!(strip_ansi("a\u{1b}=b"), "ab");
        assert_eq!(strip_ansi("plain"), "plain");
    }

    #[test]
    fn images_are_named_not_inlined() {
        // A 5 MB base64 PNG must never reach the terminal.
        let mut bundle = MimeBundle::new();
        bundle.insert("image/png", json!("A".repeat(5_000_000)));
        let output = Output::DisplayData(crate::document::DisplayDataOutput {
            data: bundle,
            metadata: Default::default(),
            extra: Default::default(),
        });
        let out = digest(&[output], &DigestOptions::default());
        assert_eq!(out, "[image/png]");
        assert!(out.len() < 100);
    }

    #[test]
    fn a_figure_with_a_text_repr_prefers_the_text() {
        let mut bundle = MimeBundle::new();
        bundle.insert("image/png", json!("AAAA"));
        bundle.insert("text/plain", json!("<Figure size 640x480>"));
        let output = Output::DisplayData(crate::document::DisplayDataOutput {
            data: bundle,
            metadata: Default::default(),
            extra: Default::default(),
        });
        let out = digest(&[output], &DigestOptions::default());
        assert_eq!(out, "<Figure size 640x480>");
    }

    #[test]
    fn very_long_single_lines_are_cut_with_a_count() {
        let text = format!("{}\n", "x".repeat(10_000));
        let options = DigestOptions {
            max_line_width: 20,
            ..Default::default()
        };
        let out = digest(&[stream(&text)], &options);
        assert!(out.contains("… (+9980 chars)"), "{out}");
        assert!(out.len() < 200);
    }

    #[test]
    fn stderr_is_labelled_so_warnings_are_distinguishable() {
        let output = Output::Stream(StreamOutput {
            name: StreamName::Stderr,
            text: "warning: careful\n".into(),
            extra: Default::default(),
        });
        let out = digest(&[output], &DigestOptions::default());
        assert_eq!(out, "stderr: warning: careful\n");
    }

    #[test]
    fn summaries_are_one_line_and_name_the_shape() {
        let mut bundle = MimeBundle::new();
        bundle.insert("text/plain", json!("x"));
        bundle.insert("text/html", json!("<table>"));
        let outputs = vec![
            stream("a\nb\nc\n"),
            Output::ExecuteResult(ExecuteResultOutput {
                data: bundle,
                metadata: Default::default(),
                execution_count: Some(1),
                extra: Default::default(),
            }),
        ];
        let summary = summarize(&outputs);
        assert_eq!(summary, "stdout 3L, result(text/html)");
        assert!(!summary.contains('\n'));
        assert_eq!(summarize(&[]), "-");
    }
}
