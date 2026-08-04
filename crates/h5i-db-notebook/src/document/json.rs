//! Canonical nbformat JSON: byte-for-byte what Jupyter's own writer produces.
//!
//! Jupyter serializes notebooks with
//! `json.dumps(nb, indent=1, sort_keys=True, separators=(",", ": "),
//! ensure_ascii=False)` plus a trailing newline, after splitting multiline
//! strings into line arrays. Matching that exactly is not cosmetic: in a repo
//! that versions everything, a notebook that reshuffles its whole JSON body
//! every time a human opens it in JupyterLab produces a useless diff and an
//! unreviewable history.
//!
//! The split/join pass runs over the whole `Value` tree the way nbformat's own
//! `split_lines` / `rejoin_lines` do, so no individual field type has to know
//! that its on-disk form differs from its in-memory form.

use serde::Serialize;
use serde_json::{Map, Value};

/// Mime types whose payload is a real JSON value rather than text.
///
/// Mirrors nbformat's `_is_json_mime`: note that the `+json` suffix only
/// counts under `application/`, so a hypothetical `text/foo+json` is treated
/// as text, exactly as upstream treats it.
fn is_json_mime(mime: &str) -> bool {
    mime == "application/json" || (mime.starts_with("application/") && mime.ends_with("+json"))
}

/// Mime types nbformat splits into line arrays when writing.
///
/// Deliberately narrow, and *not* the complement of [`is_json_mime`]: base64
/// payloads like `image/png` stay as one string. Splitting them would still
/// reload correctly, but the file would no longer match what Jupyter writes,
/// which is the whole point of this module.
fn splits_on_write(mime: &str) -> bool {
    mime.starts_with("text/") || matches!(mime, "application/javascript" | "image/svg+xml")
}

/// Split a string the way Python's `str.splitlines(keepends=True)` does, which
/// is what nbformat uses. Note this is *not* `split_inclusive('\n')`: Python
/// also breaks on `\r`, `\r\n`, and a handful of unicode line separators, and
/// a notebook written on Windows will contain `\r\n`.
fn python_splitlines(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let (is_break, width) = match bytes[i] {
            b'\r' => {
                if bytes.get(i + 1) == Some(&b'\n') {
                    (true, 2)
                } else {
                    (true, 1)
                }
            }
            // 0x1c-0x1e are the file, group and record separators, which
            // Python's `splitlines` breaks on and nbformat therefore splits a
            // source at. Missing them meant re-serialising such a source as
            // one line, so the file churned on every save.
            b'\n' | 0x0b | 0x0c | 0x1c | 0x1d | 0x1e => (true, 1),
            0xc2 if bytes.get(i + 1) == Some(&0x85) => (true, 2), // U+0085 NEL
            0xe2 if bytes.get(i + 1) == Some(&0x80)
                && matches!(bytes.get(i + 2), Some(0xa8) | Some(0xa9)) =>
            {
                (true, 3) // U+2028 LINE SEPARATOR, U+2029 PARAGRAPH SEPARATOR
            }
            _ => (false, 1),
        };
        i += width;
        if is_break {
            out.push(s[start..i].to_string());
            start = i;
        }
    }
    if start < s.len() {
        out.push(s[start..].to_string());
    }
    out
}

fn split_value(v: &mut Value) {
    if let Value::String(s) = v {
        let lines = python_splitlines(s);
        *v = Value::Array(lines.into_iter().map(Value::String).collect());
    }
}

fn join_value(v: &mut Value) {
    if let Value::Array(items) = v
        && items.iter().all(Value::is_string)
    {
        let joined: String = items.iter().filter_map(Value::as_str).collect();
        *v = Value::String(joined);
    }
}

/// `{filename: {mime: payload}}` as carried by markdown cell attachments.
fn each_attachment_bundle(cell: &mut Value, mut f: impl FnMut(&mut Value)) {
    if let Some(Value::Object(attachments)) = cell.get_mut("attachments") {
        for (_, bundle) in attachments.iter_mut() {
            f(bundle);
        }
    }
}

fn cell_is_code(cell: &Value) -> bool {
    cell.get("cell_type").and_then(Value::as_str) == Some("code")
}

fn output_type(output: &Value) -> &str {
    output
        .get("output_type")
        .and_then(Value::as_str)
        .unwrap_or("")
}

/// On-disk line arrays to in-memory strings (nbformat's `rejoin_lines`).
///
/// Permissive on purpose: any non-JSON mime holding a list of strings is
/// joined, even one [`splits_on_write`] would never have split. Files in the
/// wild contain such payloads and Jupyter joins them too.
pub fn rejoin_lines(nb: &mut Value) {
    let Some(cells) = nb.get_mut("cells").and_then(Value::as_array_mut) else {
        return;
    };
    for cell in cells {
        if let Some(source) = cell.get_mut("source") {
            join_value(source);
        }
        each_attachment_bundle(cell, |bundle| {
            if let Value::Object(map) = bundle {
                for (mime, payload) in map.iter_mut() {
                    if !is_json_mime(mime) {
                        join_value(payload);
                    }
                }
            }
        });
        if !cell_is_code(cell) {
            continue;
        }
        let Some(outputs) = cell.get_mut("outputs").and_then(Value::as_array_mut) else {
            continue;
        };
        for output in outputs {
            match output_type(output) {
                "execute_result" | "display_data" => {
                    if let Some(Value::Object(data)) = output.get_mut("data") {
                        for (mime, payload) in data.iter_mut() {
                            if !is_json_mime(mime) {
                                join_value(payload);
                            }
                        }
                    }
                }
                "" => {}
                _ => {
                    if let Some(text) = output.get_mut("text") {
                        join_value(text);
                    }
                }
            }
        }
    }
}

/// In-memory strings to on-disk line arrays (nbformat's `split_lines`).
pub fn split_lines(nb: &mut Value) {
    let Some(cells) = nb.get_mut("cells").and_then(Value::as_array_mut) else {
        return;
    };
    for cell in cells {
        if let Some(source) = cell.get_mut("source") {
            split_value(source);
        }
        each_attachment_bundle(cell, |bundle| {
            if let Value::Object(map) = bundle {
                for (mime, payload) in map.iter_mut() {
                    if splits_on_write(mime) {
                        split_value(payload);
                    }
                }
            }
        });
        if !cell_is_code(cell) {
            continue;
        }
        let Some(outputs) = cell.get_mut("outputs").and_then(Value::as_array_mut) else {
            continue;
        };
        for output in outputs {
            match output_type(output) {
                "execute_result" | "display_data" => {
                    if let Some(Value::Object(data)) = output.get_mut("data") {
                        for (mime, payload) in data.iter_mut() {
                            if splits_on_write(mime) {
                                split_value(payload);
                            }
                        }
                    }
                }
                "stream" => {
                    if let Some(text) = output.get_mut("text") {
                        split_value(text);
                    }
                }
                _ => {}
            }
        }
    }
}

/// Drop the values nbformat treats as transient and refuses to persist.
///
/// Upstream calls this on both read and write; we call it only on write, so
/// an in-memory document still reflects the file it came from, and the file
/// we produce still matches what Jupyter would have produced.
pub fn strip_transient(nb: &mut Value) {
    if let Some(Value::Object(meta)) = nb.get_mut("metadata") {
        meta.remove("orig_nbformat");
        meta.remove("orig_nbformat_minor");
        meta.remove("signature");
    }
    if let Some(cells) = nb.get_mut("cells").and_then(Value::as_array_mut) {
        for cell in cells {
            if let Some(Value::Object(meta)) = cell.get_mut("metadata") {
                meta.remove("trusted");
            }
        }
    }
}

/// Recursively reorder every object's keys, matching Python's `sort_keys=True`.
///
/// Python sorts by unicode code point and Rust's `str: Ord` compares UTF-8
/// bytes, which induce the same order, so a plain sort is faithful.
pub fn sort_keys(v: &mut Value) {
    match v {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = std::mem::take(map).into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (_, val) in entries.iter_mut() {
                sort_keys(val);
            }
            *map = entries.into_iter().collect::<Map<String, Value>>();
        }
        Value::Array(items) => items.iter_mut().for_each(sort_keys),
        _ => {}
    }
}

/// Format an `f64` the way Python's `repr` (and therefore `json.dumps`) does.
///
/// Rust and Python both emit the shortest string that round-trips, but they
/// disagree on presentation: Python switches to exponential notation when the
/// decimal exponent leaves `[-4, 15]` and pads the exponent to two digits
/// (`1e+300`, `1.5e-08`), where Rust writes `1e300` and `1.5e-8`. Notebooks
/// carry floats in `application/json` payloads and in metadata, so without
/// this the byte-identity guarantee quietly holds only for notebooks that
/// happen to contain no very large or very small numbers.
pub fn format_f64_python(v: f64) -> String {
    if !v.is_finite() {
        // Python's json emits bare `Infinity`/`NaN`, which is not valid JSON.
        // serde_json cannot hold either in a `Number` (they parse to null), so
        // this is unreachable from a parsed document and exists only so a
        // hand-built value cannot produce invalid output.
        return if v.is_nan() {
            "NaN".to_string()
        } else if v > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }

    // `{:e}` gives the shortest round-trip digit count, but breaks exact ties
    // away from zero where Python's dtoa breaks them to even: the double
    // 3236844372569.78125 is exactly between two 17-digit decimals, and Rust
    // picks …7813 where Python picks …7812. Rust's *fixed-precision* mode is
    // correctly rounded half-to-even, so re-rendering at the shortest length
    // keeps the length and adopts Python's tie-break. Re-rounding cannot carry
    // into an extra digit, because the two forms differ only when the value
    // sits exactly on the midpoint, and rounding down from a midpoint never
    // carries.
    let shortest = format!("{v:e}");
    let digit_count = shortest
        .split_once('e')
        .map(|(m, _)| m.chars().filter(char::is_ascii_digit).count())
        .unwrap_or(1);
    let sci = format!("{:.*e}", digit_count.saturating_sub(1), v);
    let (mantissa, exp) = sci.split_once('e').expect("{:e} always emits an exponent");
    let exp: i32 = exp.parse().expect("{:e} emits a valid exponent");
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", mantissa),
    };
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();

    if (-4..=15).contains(&exp) {
        let n = digits.len() as i32;
        let body = if exp >= 0 {
            if exp + 1 >= n {
                // Integral value: pad with zeros, then Python's mandatory `.0`.
                format!("{}{}.0", digits, "0".repeat((exp + 1 - n).max(0) as usize))
            } else {
                let split = (exp + 1) as usize;
                format!("{}.{}", &digits[..split], &digits[split..])
            }
        } else {
            format!("0.{}{}", "0".repeat((-exp - 1) as usize), digits)
        };
        format!("{sign}{body}")
    } else {
        format!(
            "{sign}{mantissa}e{}{:02}",
            if exp < 0 { '-' } else { '+' },
            exp.abs()
        )
    }
}

/// `PrettyFormatter` with Python's indent, plus Python's float presentation.
struct NbFormatter<'a> {
    inner: serde_json::ser::PrettyFormatter<'a>,
}

impl serde_json::ser::Formatter for NbFormatter<'_> {
    fn write_f32<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        value: f32,
    ) -> std::io::Result<()> {
        self.write_f64(writer, value as f64)
    }

    fn write_f64<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        value: f64,
    ) -> std::io::Result<()> {
        writer.write_all(format_f64_python(value).as_bytes())
    }

    // Everything structural is PrettyFormatter's job.
    fn begin_array<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.inner.begin_array(w)
    }
    fn end_array<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.inner.end_array(w)
    }
    fn begin_array_value<W: ?Sized + std::io::Write>(
        &mut self,
        w: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        self.inner.begin_array_value(w, first)
    }
    fn end_array_value<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.inner.end_array_value(w)
    }
    fn begin_object<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.inner.begin_object(w)
    }
    fn end_object<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.inner.end_object(w)
    }
    fn begin_object_key<W: ?Sized + std::io::Write>(
        &mut self,
        w: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        self.inner.begin_object_key(w, first)
    }
    fn begin_object_value<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.inner.begin_object_value(w)
    }
    fn end_object_value<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.inner.end_object_value(w)
    }
}

/// Serialize a `Value` with Python's `indent=1, separators=(",", ": ")`.
fn write_canonical(v: &Value) -> String {
    let formatter = NbFormatter {
        inner: serde_json::ser::PrettyFormatter::with_indent(b" "),
    };
    let mut buf = Vec::with_capacity(4096);
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
    v.serialize(&mut ser)
        .expect("serializing a Value into a Vec cannot fail");
    // Safe: serde_json only emits UTF-8.
    let mut s = String::from_utf8(buf).expect("serde_json emits UTF-8");
    s.push('\n');
    s
}

/// Render a notebook `Value` exactly as Jupyter's writer would.
pub fn to_canonical_string(mut nb: Value) -> String {
    strip_transient(&mut nb);
    split_lines(&mut nb);
    sort_keys(&mut nb);
    write_canonical(&nb)
}

/// Parse notebook JSON into the in-memory shape (line arrays joined).
pub fn parse_to_value(text: &str) -> serde_json::Result<Value> {
    let mut v: Value = serde_json::from_str(text)?;
    rejoin_lines(&mut v);
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn splitlines_matches_python_semantics() {
        assert_eq!(python_splitlines(""), Vec::<String>::new());
        assert_eq!(python_splitlines("a"), vec!["a"]);
        assert_eq!(python_splitlines("a\n"), vec!["a\n"]);
        assert_eq!(python_splitlines("a\nb"), vec!["a\n", "b"]);
        assert_eq!(python_splitlines("a\nb\n"), vec!["a\n", "b\n"]);
        assert_eq!(python_splitlines("\n"), vec!["\n"]);
        assert_eq!(python_splitlines("a\r\nb"), vec!["a\r\n", "b"]);
        assert_eq!(python_splitlines("a\rb"), vec!["a\r", "b"]);
    }

    #[test]
    fn split_then_join_is_the_identity_on_sources() {
        for src in ["", "a", "a\n", "a\nb", "a\r\nb\n", "x\n\n\ny"] {
            let mut v = json!({"cells": [{"cell_type": "code", "source": src, "outputs": []}]});
            split_lines(&mut v);
            rejoin_lines(&mut v);
            assert_eq!(
                v["cells"][0]["source"],
                json!(src),
                "round trip failed on {src:?}"
            );
        }
    }

    #[test]
    fn json_mime_payloads_are_never_split() {
        let mut v = json!({"cells": [{
            "cell_type": "code",
            "source": "",
            "outputs": [{
                "output_type": "display_data",
                "data": {
                    "application/json": {"a": [1, 2]},
                    "application/vnd.vega.v5+json": {"b": 1},
                    "text/plain": "one\ntwo"
                }
            }]
        }]});
        split_lines(&mut v);
        let data = &v["cells"][0]["outputs"][0]["data"];
        assert!(data["application/json"].is_object());
        assert!(data["application/vnd.vega.v5+json"].is_object());
        assert_eq!(data["text/plain"], json!(["one\n", "two"]));
    }

    #[test]
    fn base64_payloads_stay_on_one_line() {
        // nbformat splits `text/*` plus javascript and svg, and nothing else.
        // Splitting `image/png` would still reload, but the file would stop
        // matching what Jupyter writes.
        let mut v = json!({"cells": [{
            "cell_type": "code",
            "source": "",
            "outputs": [{
                "output_type": "display_data",
                "data": {
                    "image/png": "iVBORw0KGgo=",
                    "image/svg+xml": "<svg>\n</svg>",
                    "application/javascript": "a\nb",
                    "text/html": "<b>\n</b>"
                }
            }]
        }]});
        split_lines(&mut v);
        let data = &v["cells"][0]["outputs"][0]["data"];
        assert_eq!(data["image/png"], json!("iVBORw0KGgo="));
        assert_eq!(data["image/svg+xml"], json!(["<svg>\n", "</svg>"]));
        assert_eq!(data["application/javascript"], json!(["a\n", "b"]));
        assert_eq!(data["text/html"], json!(["<b>\n", "</b>"]));
    }

    #[test]
    fn markdown_attachments_are_split_and_rejoined() {
        let mut v = json!({"cells": [{
            "cell_type": "markdown",
            "source": "![](attachment:a.png)",
            "attachments": {"a.png": {"image/png": "AAAA", "text/plain": "x\ny"}}
        }]});
        split_lines(&mut v);
        let bundle = &v["cells"][0]["attachments"]["a.png"];
        assert_eq!(bundle["image/png"], json!("AAAA"));
        assert_eq!(bundle["text/plain"], json!(["x\n", "y"]));
        rejoin_lines(&mut v);
        assert_eq!(
            v["cells"][0]["attachments"]["a.png"]["text/plain"],
            json!("x\ny")
        );
    }

    #[test]
    fn transient_values_are_dropped_on_write() {
        let v = json!({
            "cells": [{"cell_type": "code", "source": "", "outputs": [],
                       "metadata": {"trusted": true, "tags": ["keep"]}}],
            "metadata": {"signature": "sha", "orig_nbformat": 4, "kernelspec": {"name": "python3"}},
            "nbformat": 4, "nbformat_minor": 5
        });
        let out = to_canonical_string(v);
        assert!(!out.contains("signature"), "{out}");
        assert!(!out.contains("orig_nbformat\""), "{out}");
        assert!(!out.contains("trusted"), "{out}");
        assert!(out.contains("keep") && out.contains("kernelspec"), "{out}");
    }

    #[test]
    fn outputs_on_non_code_cells_are_left_alone() {
        // nbformat gates output handling on `cell_type == "code"`, so a stray
        // outputs array on a markdown cell must survive untouched.
        let mut v = json!({"cells": [{
            "cell_type": "markdown",
            "source": "x",
            "outputs": [{"output_type": "stream", "name": "stdout", "text": "a\nb"}]
        }]});
        split_lines(&mut v);
        assert_eq!(v["cells"][0]["outputs"][0]["text"], json!("a\nb"));
    }

    #[test]
    fn canonical_output_matches_pythons_formatting() {
        let v = json!({"b": 1, "a": {"d": [], "c": [1, 2]}});
        // json.dumps(v, indent=1, sort_keys=True, separators=(",", ": "))
        let expected =
            "{\n \"a\": {\n  \"c\": [\n   1,\n   2\n  ],\n  \"d\": []\n },\n \"b\": 1\n}\n";
        assert_eq!(to_canonical_string(v), expected);
    }

    #[test]
    fn non_ascii_is_not_escaped() {
        let v = json!({"s": "日本語"});
        assert!(to_canonical_string(v).contains("日本語"));
    }

    #[test]
    fn floats_render_the_way_python_repr_does() {
        // Fixed-notation band, including both boundaries.
        assert_eq!(format_f64_python(1.0), "1.0");
        assert_eq!(format_f64_python(0.0), "0.0");
        assert_eq!(format_f64_python(-0.0), "-0.0");
        assert_eq!(format_f64_python(0.1), "0.1");
        assert_eq!(format_f64_python(1.0 / 3.0), "0.3333333333333333");
        assert_eq!(format_f64_python(0.0001), "0.0001");
        assert_eq!(format_f64_python(1e15), "1000000000000000.0");
        assert_eq!(format_f64_python(-2.5), "-2.5");
        assert_eq!(format_f64_python(1234.5678), "1234.5678");

        // Exponential band: two-digit minimum, explicit sign.
        assert_eq!(format_f64_python(1e16), "1e+16");
        assert_eq!(format_f64_python(1e-5), "1e-05");
        assert_eq!(format_f64_python(1.5e-8), "1.5e-08");
        assert_eq!(format_f64_python(1e300), "1e+300");
        assert_eq!(format_f64_python(-1.5e-8), "-1.5e-08");
        assert_eq!(format_f64_python(5e-324), "5e-324");
        assert_eq!(format_f64_python(f64::MAX), "1.7976931348623157e+308");
        assert_eq!(
            format_f64_python(1.2345678901234568e17),
            "1.2345678901234568e+17"
        );
    }

    #[test]
    fn exact_midpoints_break_to_even_like_python() {
        // 3236844372569.78125 sits exactly between two 17-digit decimals.
        // Rust's shortest formatter rounds away from zero (…7813); Python's
        // dtoa rounds to even (…7812), and the file has to match Python.
        assert_eq!(
            format_f64_python(f64::from_bits(0x42878d17ac12ce40)),
            "3236844372569.7812"
        );
    }

    #[test]
    fn every_formatted_float_parses_back_to_the_same_bits() {
        // Presentation must never cost round-trip fidelity.
        let mut v = 1.0e-320_f64;
        while v < f64::MAX / 4.0 {
            for probe in [v, -v, v * 1.5, v * 7.0 / 3.0] {
                let s = format_f64_python(probe);
                let back: f64 = s
                    .parse()
                    .unwrap_or_else(|e| panic!("{s} did not reparse: {e}"));
                assert_eq!(back.to_bits(), probe.to_bits(), "{s} lost bits");
            }
            v *= 3.7;
        }
    }

    #[test]
    fn floats_inside_a_notebook_value_use_python_formatting() {
        let v = json!({"a": 1e300, "b": 1.5e-8, "c": 1.0});
        assert_eq!(
            to_canonical_string(v),
            "{\n \"a\": 1e+300,\n \"b\": 1.5e-08,\n \"c\": 1.0\n}\n"
        );
    }
}
