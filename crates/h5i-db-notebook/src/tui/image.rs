//! Inline images, where the terminal can show them.
//!
//! Only the protocols that pass base64 straight through are implemented:
//! kitty's graphics protocol (kitty, ghostty, WezTerm) and iTerm2's inline
//! image escape (iTerm2, WezTerm). Both take the PNG bytes as-is.
//!
//! Sixel and unicode half-blocks are deliberately absent: both require
//! decoding the PNG to pixels, which means an image-decoding dependency and a
//! resampler, for terminals that are increasingly rare. A terminal we cannot
//! draw in gets a labelled placeholder and the output stays reachable through
//! `h5i-nb output --save`, so nothing is lost but the convenience.

use std::io::Write;

/// Which inline-image escape this terminal understands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageProtocol {
    Kitty,
    ITerm2,
    None,
}

impl ImageProtocol {
    /// Sniff the terminal from its environment.
    ///
    /// Environment sniffing rather than a terminal query: a query needs a
    /// response read within a timeout, and getting that wrong on a terminal
    /// that stays silent hangs the UI before it has drawn anything.
    pub fn detect() -> Self {
        Self::from_env(|name| std::env::var(name).ok())
    }

    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Self {
        // An explicit override always wins, so a user in an unrecognised
        // terminal is not stuck with placeholders.
        if let Some(value) = get("H5I_NB_IMAGES") {
            return match value.to_ascii_lowercase().as_str() {
                "kitty" => ImageProtocol::Kitty,
                "iterm" | "iterm2" => ImageProtocol::ITerm2,
                _ => ImageProtocol::None,
            };
        }
        if get("KITTY_WINDOW_ID").is_some() {
            return ImageProtocol::Kitty;
        }
        let term_program = get("TERM_PROGRAM").unwrap_or_default();
        if term_program == "iTerm.app" {
            return ImageProtocol::ITerm2;
        }
        if term_program == "WezTerm" {
            return ImageProtocol::ITerm2;
        }
        if term_program == "ghostty" {
            return ImageProtocol::Kitty;
        }
        match get("TERM").unwrap_or_default().as_str() {
            t if t.contains("kitty") => ImageProtocol::Kitty,
            _ => ImageProtocol::None,
        }
    }

    pub fn is_supported(self) -> bool {
        self != ImageProtocol::None
    }
}

/// Rows an inline image is drawn into.
///
/// Fixed rather than derived from the image: knowing the real aspect ratio
/// would mean decoding the PNG, and both protocols will scale to fit a row
/// count we give them.
pub const IMAGE_ROWS: u16 = 15;

/// The escape sequence that draws `base64_png` at the cursor.
///
/// Returns `None` when the terminal cannot show it, so the caller falls back
/// to the placeholder it already drew.
pub fn escape_sequence(
    protocol: ImageProtocol,
    base64_png: &str,
    rows: u16,
    columns: u16,
) -> Option<String> {
    // Whitespace is legal inside a notebook's base64 payload but not inside
    // these escapes.
    let payload: String = base64_png.chars().filter(|c| !c.is_whitespace()).collect();
    if payload.is_empty() {
        return None;
    }
    match protocol {
        ImageProtocol::None => None,
        ImageProtocol::ITerm2 => Some(format!(
            "\u{1b}]1337;File=inline=1;preserveAspectRatio=1;width={columns};height={rows}:{payload}\u{7}"
        )),
        ImageProtocol::Kitty => {
            // a=T transmits and displays; f=100 says the payload is PNG;
            // c/r give the cell box to scale into. The payload is chunked
            // because terminals cap the length of a single escape.
            let mut out = String::with_capacity(payload.len() + 256);
            let chunks: Vec<&str> = chunk(&payload, 4096);
            for (index, chunk) in chunks.iter().enumerate() {
                let more = u8::from(index + 1 < chunks.len());
                if index == 0 {
                    out.push_str(&format!(
                        "\u{1b}_Ga=T,f=100,c={columns},r={rows},m={more};{chunk}\u{1b}\\"
                    ));
                } else {
                    out.push_str(&format!("\u{1b}_Gm={more};{chunk}\u{1b}\\"));
                }
            }
            Some(out)
        }
    }
}

fn chunk(text: &str, size: usize) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let end = (start + size).min(text.len());
        out.push(&text[start..end]);
        start = end;
    }
    out
}

/// Draw an image at a screen position, leaving the cursor where it was.
///
/// ratatui owns the screen, so this runs after the frame is drawn: the cursor
/// is saved, moved into the cell ratatui reserved, the escape is written, and
/// the cursor is restored.
pub fn draw_at(
    out: &mut impl Write,
    protocol: ImageProtocol,
    base64_png: &str,
    column: u16,
    row: u16,
    rows: u16,
    columns: u16,
) -> std::io::Result<()> {
    let Some(sequence) = escape_sequence(protocol, base64_png, rows, columns) else {
        return Ok(());
    };
    // Save cursor, move (1-based), draw, restore.
    write!(out, "\u{1b}7\u{1b}[{};{}H", row + 1, column + 1)?;
    out.write_all(sequence.as_bytes())?;
    write!(out, "\u{1b}8")?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    #[test]
    fn terminals_are_detected_from_their_own_markers() {
        assert_eq!(
            ImageProtocol::from_env(env(&[("KITTY_WINDOW_ID", "1")])),
            ImageProtocol::Kitty
        );
        assert_eq!(
            ImageProtocol::from_env(env(&[("TERM_PROGRAM", "iTerm.app")])),
            ImageProtocol::ITerm2
        );
        assert_eq!(
            ImageProtocol::from_env(env(&[("TERM_PROGRAM", "WezTerm")])),
            ImageProtocol::ITerm2
        );
        assert_eq!(
            ImageProtocol::from_env(env(&[("TERM", "xterm-kitty")])),
            ImageProtocol::Kitty
        );
        assert_eq!(
            ImageProtocol::from_env(env(&[("TERM", "xterm-256color")])),
            ImageProtocol::None
        );
        assert_eq!(ImageProtocol::from_env(env(&[])), ImageProtocol::None);
    }

    #[test]
    fn an_explicit_override_beats_detection() {
        assert_eq!(
            ImageProtocol::from_env(env(&[
                ("H5I_NB_IMAGES", "iterm2"),
                ("KITTY_WINDOW_ID", "1")
            ])),
            ImageProtocol::ITerm2
        );
        assert_eq!(
            ImageProtocol::from_env(env(&[("H5I_NB_IMAGES", "off"), ("KITTY_WINDOW_ID", "1")])),
            ImageProtocol::None
        );
    }

    #[test]
    fn kitty_payloads_are_chunked_and_terminated() {
        let payload = "A".repeat(10_000);
        let sequence = escape_sequence(ImageProtocol::Kitty, &payload, 15, 40).unwrap();
        assert!(sequence.starts_with("\u{1b}_Ga=T,f=100,c=40,r=15,m=1;"));
        assert!(sequence.ends_with("\u{1b}\\"));
        // 10000 / 4096 = 3 chunks.
        assert_eq!(sequence.matches("\u{1b}_G").count(), 3);
        // Every chunk but the last says more is coming.
        assert_eq!(sequence.matches("m=1;").count(), 2);
        assert_eq!(sequence.matches("m=0;").count(), 1);
    }

    #[test]
    fn a_short_kitty_payload_is_a_single_chunk() {
        let sequence = escape_sequence(ImageProtocol::Kitty, "iVBORw0KGgo=", 15, 40).unwrap();
        assert_eq!(sequence.matches("\u{1b}_G").count(), 1);
        assert!(sequence.contains("m=0;iVBORw0KGgo="));
    }

    #[test]
    fn iterm_sequences_carry_the_size_and_terminate_with_bel() {
        let sequence = escape_sequence(ImageProtocol::ITerm2, "iVBOR", 15, 40).unwrap();
        assert!(sequence.starts_with("\u{1b}]1337;File=inline=1"));
        assert!(sequence.contains("width=40;height=15"));
        assert!(sequence.ends_with("\u{7}"));
    }

    #[test]
    fn base64_newlines_are_stripped_before_being_sent() {
        // nbformat stores base64 payloads split across lines; an embedded
        // newline would terminate the escape early and dump the rest as text.
        let sequence = escape_sequence(ImageProtocol::Kitty, "iVBO\nRw0K\n", 15, 40).unwrap();
        assert!(!sequence.contains('\n'));
        assert!(sequence.contains("iVBORw0K"));
    }

    #[test]
    fn nothing_is_emitted_without_support_or_payload() {
        assert!(escape_sequence(ImageProtocol::None, "iVBOR", 15, 40).is_none());
        assert!(escape_sequence(ImageProtocol::Kitty, "   \n ", 15, 40).is_none());
    }

    #[test]
    fn drawing_saves_and_restores_the_cursor() {
        let mut buffer = Vec::new();
        draw_at(&mut buffer, ImageProtocol::Kitty, "iVBOR", 10, 4, 15, 40).unwrap();
        let text = String::from_utf8(buffer).unwrap();
        assert!(text.starts_with("\u{1b}7"), "cursor was not saved");
        assert!(
            text.contains("\u{1b}[5;11H"),
            "wrong 1-based position: {text:?}"
        );
        assert!(text.ends_with("\u{1b}8"), "cursor was not restored");
    }

    #[test]
    fn drawing_into_an_unsupported_terminal_writes_nothing() {
        let mut buffer = Vec::new();
        draw_at(&mut buffer, ImageProtocol::None, "iVBOR", 0, 0, 15, 40).unwrap();
        assert!(buffer.is_empty());
    }
}
