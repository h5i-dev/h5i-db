//! Inline images, where the terminal can show them.
//!
//! Two things have to be true before a PNG in a notebook reaches the screen,
//! and guessing at either one is what kept plots invisible for so long.
//!
//! **Which protocol the terminal speaks.** Reading `TERM_PROGRAM` and
//! `KITTY_WINDOW_ID` looks like it answers this, and under WSL it cannot: the
//! terminal runs on the Windows side and its environment does not cross into
//! Linux unless `WSLENV` says so, which it almost never does. Every terminal
//! then looks like a bare `xterm-256color` and every image becomes a
//! placeholder. So ask the terminal instead — [`Picker::from_query_stdio`]
//! writes the kitty, iTerm2 and device-attribute queries and reads the
//! answers, which is also how euporie gets this right.
//!
//! **What the pixels are.** Sixel needs the PNG decoded and quantised, and
//! every protocol needs the real aspect ratio if the plot is not to be drawn
//! into a box of the wrong shape. `ratatui-image` owns both, so this module is
//! a cache in front of it: PNG dimensions by digest (a header read, not a
//! decode) to size the reserved rows, and encoded protocols by digest and box
//! so a scroll or a keystroke does not re-encode every plot on screen.

use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant};

use ratatui::layout::{Rect, Size};
use ratatui_image::Resize;
use ratatui_image::picker::{Picker, ProtocolType};
use ratatui_image::protocol::Protocol;

/// Rows an image is drawn into when its real shape is unknown.
///
/// Only reached when the PNG header will not parse, or when no terminal query
/// was possible: with a picker the row count comes from the image itself.
pub const IMAGE_ROWS: u16 = 15;

/// The tallest an image may be drawn, so one plot cannot fill the screen and
/// push the cell that produced it out of view.
pub const MAX_IMAGE_ROWS: u16 = 24;

/// How long the terminal gets to prove it answers a query at all.
///
/// A round trip is milliseconds even over ssh; this is long enough that a slow
/// link is not mistaken for a silent terminal, and short enough that starting
/// up in front of one that never answers is not a visible pause.
const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// How many encoded protocols to keep before starting over.
///
/// Each entry holds an encoded frame, which for sixel is a few hundred
/// kilobytes, so this is a memory bound rather than a hit-rate tuning knob.
/// Dropping the lot is fine: the next frame re-encodes only what is on screen.
const CACHE_LIMIT: usize = 16;

/// Decoded images, ready to draw, and the terminal's answer about how.
pub struct Images {
    /// `None` when the terminal was never asked (tests) or refused to answer.
    picker: Option<Picker>,
    /// PNG dimensions by payload digest. Cheap, and never invalidated.
    sizes: HashMap<[u8; 16], (u32, u32)>,
    /// Encoded protocols by payload digest and the box they were encoded for.
    encoded: HashMap<([u8; 16], u16, u16), Protocol>,
}

impl Images {
    /// Ask the terminal what it can draw.
    ///
    /// Must be called after entering the alternate screen and before anything
    /// starts reading keys: the query writes to stdout and reads the reply off
    /// stdin, and a reader thread would eat that reply.
    pub fn detect() -> Self {
        Self::with_override(std::env::var("H5I_NB_IMAGES").ok())
    }

    /// A picker that draws nothing, for tests and for `H5I_NB_IMAGES=off`.
    pub fn disabled() -> Self {
        Images {
            picker: None,
            sizes: HashMap::new(),
            encoded: HashMap::new(),
        }
    }

    fn with_override(setting: Option<String>) -> Self {
        let forced = match setting.as_deref().map(str::trim) {
            // Turning images off is worth honouring without a query at all:
            // somebody setting this has a terminal that misbehaves, and the
            // query itself is part of what they are turning off.
            Some("off" | "none" | "0" | "false" | "no") => return Self::disabled(),
            Some("kitty") => Some(ProtocolType::Kitty),
            Some("iterm" | "iterm2") => Some(ProtocolType::Iterm2),
            Some("sixel") => Some(ProtocolType::Sixel),
            Some("halfblocks" | "blocks" | "unicode") => Some(ProtocolType::Halfblocks),
            _ => None,
        };
        // The query is still worth running even when the protocol is forced,
        // because it is also how the cell size in pixels is discovered, and
        // without that an image cannot be scaled to a box of character cells.
        //
        // But only where it is safe to run, and that has to be established
        // first. `from_query_stdio` reads the replies on a thread it spawns
        // and does not own: the thread blocks in `stdin().read()` until a
        // device status report arrives, and when the outer timeout fires the
        // thread is abandoned rather than stopped. On a terminal that never
        // answers it stays parked on stdin forever and eats every keystroke
        // after it, so the notebook paints perfectly and then ignores every
        // key including `q`. Sending our own status report first, and reading
        // the reply with a timed `poll` that cannot block, tells us whether
        // that thread will ever come back.
        let mut picker = if answers_a_status_report(PROBE_TIMEOUT) {
            Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks())
        } else {
            // Half-blocks need no query and no pixel size, so a silent
            // terminal still gets a recognisable plot rather than a label.
            Picker::halfblocks()
        };
        if let Some(protocol) = forced {
            picker.set_protocol_type(protocol);
        }
        Images {
            picker: Some(picker),
            sizes: HashMap::new(),
            encoded: HashMap::new(),
        }
    }

    /// Whether anything will be drawn at all.
    pub fn is_supported(&self) -> bool {
        self.picker.is_some()
    }

    /// The protocol in use, for the status line and for tests.
    pub fn protocol_type(&self) -> Option<ProtocolType> {
        self.picker.as_ref().map(|p| p.protocol_type())
    }

    /// How many rows an image should be given, at `columns` wide.
    ///
    /// Derived from the PNG's own dimensions and the terminal's cell size, so
    /// a wide plot gets a short box and a tall one a taller box, and neither
    /// is stretched to fit. Falls back to [`IMAGE_ROWS`] when the header will
    /// not parse or the terminal never answered.
    pub fn rows_for(&mut self, base64_png: &str, columns: u16) -> u16 {
        let columns = columns.max(1);
        let Some(font) = self.picker.as_ref().map(|p| p.font_size()) else {
            return IMAGE_ROWS.min(MAX_IMAGE_ROWS);
        };
        let Some((width_px, height_px)) = self.dimensions(base64_png) else {
            return IMAGE_ROWS.min(MAX_IMAGE_ROWS);
        };
        let (font_w, font_h) = (font.width.max(1) as u32, font.height.max(1) as u32);
        (rows_for_pixels(width_px, height_px, columns, font_w, font_h)) as u16
    }

    /// The encoded image for `area`, or `None` if it cannot be drawn.
    ///
    /// Encoding is where the cost is (a decode, a resample, and for sixel a
    /// colour quantisation), so the result is kept until the box changes.
    pub fn protocol(&mut self, base64_png: &str, area: Rect) -> Option<&Protocol> {
        self.picker.as_ref()?;
        if area.width == 0 || area.height == 0 {
            return None;
        }
        let key = (digest(base64_png), area.width, area.height);
        if !self.encoded.contains_key(&key) {
            // Bounded rather than evicted by age: what is on screen is
            // re-encoded on the next frame anyway, and a scroll through a
            // notebook of plots would otherwise grow without limit.
            if self.encoded.len() >= CACHE_LIMIT {
                self.encoded.clear();
            }
            let image = decode(base64_png)?;
            let picker = self.picker.as_ref()?;
            let protocol = picker
                .new_protocol(
                    image,
                    Size::new(area.width, area.height),
                    // Fit rather than Scale: an image smaller than its box is
                    // left alone instead of being blown up into a blur.
                    Resize::Fit(None),
                )
                .ok()?;
            self.encoded.insert(key, protocol);
        }
        self.encoded.get(&key)
    }

    /// PNG dimensions, read from the header rather than by decoding.
    fn dimensions(&mut self, base64_png: &str) -> Option<(u32, u32)> {
        let key = digest(base64_png);
        if let Some(size) = self.sizes.get(&key) {
            return Some(*size);
        }
        let bytes = decode_base64(base64_png)?;
        let size = image::ImageReader::new(std::io::Cursor::new(bytes))
            .with_guessed_format()
            .ok()?
            .into_dimensions()
            .ok()?;
        self.sizes.insert(key, size);
        Some(size)
    }
}

impl Default for Images {
    fn default() -> Self {
        Self::disabled()
    }
}

impl std::fmt::Debug for Images {
    // `Protocol` holds encoded frames and is not `Debug`, and printing them
    // would be useless anyway.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Images")
            .field("protocol", &self.protocol_type())
            .field("cached", &self.encoded.len())
            .finish()
    }
}

/// Does this terminal answer a device status report within `timeout`?
///
/// The one question every terminal since the VT100 can answer, asked here only
/// to find out whether asking anything is safe: see [`Images::with_override`]
/// for why a terminal that stays silent must never be queried again.
///
/// Read with `poll` rather than a plain read so that a silent terminal costs
/// the timeout and nothing more. The caller must already be in raw mode, or
/// the reply sits in the line buffer until the user presses Enter.
fn answers_a_status_report(timeout: Duration) -> bool {
    let mut out = std::io::stdout();
    if out.write_all(b"\x1b[5n").is_err() || out.flush().is_err() {
        return false;
    }

    let deadline = Instant::now() + timeout;
    let mut seen: Vec<u8> = Vec::with_capacity(64);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return false;
        }
        let mut fd = libc::pollfd {
            fd: libc::STDIN_FILENO,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one initialised pollfd, and a count that matches it.
        let ready = unsafe { libc::poll(&mut fd, 1, left.as_millis() as libc::c_int) };
        if ready <= 0 {
            return false;
        }
        let mut buffer = [0u8; 64];
        // SAFETY: reading into a buffer we own, for at most its own length.
        let read =
            unsafe { libc::read(libc::STDIN_FILENO, buffer.as_mut_ptr().cast(), buffer.len()) };
        // Nothing more is coming: stdin is closed, or not a terminal at all.
        if read <= 0 {
            return false;
        }
        seen.extend_from_slice(&buffer[..read as usize]);
        if is_status_report(&seen) {
            return true;
        }
        // A terminal answering something other than what was asked, at length.
        // Whatever it is doing, it is not the conversation we need.
        if seen.len() > 512 {
            return false;
        }
    }
}

/// `CSI 0 n` (ready) or `CSI 3 n` (not ready), the two legal answers.
fn is_status_report(bytes: &[u8]) -> bool {
    bytes.windows(4).any(|w| w == b"\x1b[0n" || w == b"\x1b[3n")
}

/// How many rows an image of `width_px` x `height_px` will actually occupy.
///
/// Two cases, and getting only the first of them right is what leaves a band
/// of blank rows under every small plot. An image narrower than the space it
/// is offered is drawn at its own size, because [`Resize::Fit`] does not
/// upscale — blowing a 200px figure up to fill the pane would only blur it. A
/// wider one is scaled down until it fits, and its height shrinks with it. So
/// the answer is the smaller of "its natural height" and "its height once the
/// width fits", which is also exactly what `Fit` will do.
fn rows_for_pixels(width_px: u32, height_px: u32, columns: u16, font_w: u32, font_h: u32) -> u64 {
    let natural_rows = (height_px as u64).div_ceil(font_h as u64);
    let fitted_rows = (height_px as u64 * columns as u64 * font_w as u64)
        .div_ceil((width_px as u64 * font_h as u64).max(1));
    natural_rows
        .min(fitted_rows)
        .clamp(1, MAX_IMAGE_ROWS as u64)
}

/// A short, stable key for a base64 payload.
///
/// The payload itself would work as a key and can be megabytes; hashing keeps
/// the cache's own memory proportional to the number of images, not their size.
fn digest(base64_png: &str) -> [u8; 16] {
    let hash = blake3::hash(base64_png.as_bytes());
    let mut key = [0u8; 16];
    key.copy_from_slice(&hash.as_bytes()[..16]);
    key
}

/// nbformat stores base64 wrapped across lines, which no decoder accepts.
fn decode_base64(base64_png: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let payload: String = base64_png.chars().filter(|c| !c.is_whitespace()).collect();
    if payload.is_empty() {
        return None;
    }
    base64::engine::general_purpose::STANDARD
        .decode(payload.as_bytes())
        .ok()
}

fn decode(base64_png: &str) -> Option<image::DynamicImage> {
    let bytes = decode_base64(base64_png)?;
    image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .ok()?
        .decode()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 4x2 PNG, so dimensions and aspect ratio are known exactly.
    fn png_4x2() -> String {
        use base64::Engine as _;
        let mut bytes = Vec::new();
        let image = image::RgbaImage::from_pixel(4, 2, image::Rgba([255, 0, 0, 255]));
        image::DynamicImage::ImageRgba8(image)
            .write_to(
                &mut std::io::Cursor::new(&mut bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn a_disabled_picker_draws_nothing_and_still_reserves_rows() {
        let mut images = Images::disabled();
        assert!(!images.is_supported());
        assert!(
            images
                .protocol(&png_4x2(), Rect::new(0, 0, 40, 15))
                .is_none()
        );
        // The rows are still reserved, so the layout does not jump between a
        // terminal that can draw and one that cannot.
        assert_eq!(images.rows_for(&png_4x2(), 40), IMAGE_ROWS);
    }

    #[test]
    fn turning_images_off_skips_the_terminal_query_entirely() {
        // Without this the query would run in a test process with no terminal,
        // which is slow at best and reads somebody else's stdin at worst.
        for setting in ["off", "none", "0", "false", "no"] {
            let images = Images::with_override(Some(setting.to_string()));
            assert!(!images.is_supported(), "{setting} left images on");
        }
    }

    #[test]
    fn dimensions_come_from_the_png_header() {
        let mut images = Images::disabled();
        assert_eq!(images.dimensions(&png_4x2()), Some((4, 2)));
        // Second look is served from the cache, and agrees.
        assert_eq!(images.dimensions(&png_4x2()), Some((4, 2)));
    }

    #[test]
    fn base64_split_across_lines_still_decodes() {
        // nbformat wraps long payloads; an unfiltered decode rejects them and
        // every plot in a real notebook would fall back to a placeholder.
        let payload = png_4x2();
        let wrapped = payload
            .as_bytes()
            .chunks(16)
            .map(|c| String::from_utf8_lossy(c).into_owned())
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(decode_base64(&wrapped), decode_base64(&payload));
        assert!(decode(&wrapped).is_some());
    }

    #[test]
    fn nothing_decodes_from_an_empty_or_broken_payload() {
        assert!(decode_base64("   \n ").is_none());
        assert!(decode("bm90IGEgcG5n").is_none());
        let mut images = Images::disabled();
        assert!(images.dimensions("bm90IGEgcG5n").is_none());
    }

    #[test]
    fn a_wide_image_is_scaled_down_and_keeps_its_shape() {
        // 640x480 in a 93-column pane of 8x16 cells: 80x30 cells at its own
        // size, too tall, so it is scaled until the height fits and the rows
        // reserved are the rows it will take.
        assert_eq!(rows_for_pixels(640, 480, 93, 8, 16), MAX_IMAGE_ROWS as u64);
        // 800x100 is 100 cells wide with only 50 to give it, so it is halved:
        // 400x50 pixels, which is 4 rows rather than the 7 its full height
        // would have needed.
        assert_eq!(rows_for_pixels(800, 100, 50, 8, 16), 4);
    }

    #[test]
    fn a_small_image_reserves_only_the_rows_it_will_use() {
        // 200x120 at 8x16 is 25x8 cells, and it is drawn at that size because
        // `Resize::Fit` does not upscale. Reserving the full width's worth of
        // rows (28, before clamping) would leave sixteen blank rows under
        // every small plot, which is what the naive aspect-ratio sum does.
        assert_eq!(rows_for_pixels(200, 120, 93, 8, 16), 8);
    }

    #[test]
    fn no_image_is_allowed_to_fill_the_screen_or_vanish() {
        // 100x1000 is 50 rows at its own size and has the width to stay
        // there, so only the cap stops it from burying the cell above it.
        assert_eq!(
            rows_for_pixels(100, 1000, 40, 10, 20),
            MAX_IMAGE_ROWS as u64
        );
        // A tall sliver narrow enough to fit is left at its own height, since
        // the cap is the only thing that shrinks an image that already fits.
        assert_eq!(rows_for_pixels(4, 400, 40, 10, 20), 20);
        // And a single-pixel-tall one still gets a row rather than none.
        assert_eq!(rows_for_pixels(4000, 1, 40, 10, 20), 1);
    }

    #[test]
    fn only_a_real_status_report_counts_as_an_answer() {
        assert!(is_status_report(b"\x1b[0n"));
        assert!(is_status_report(b"\x1b[3n"));
        // Buried in whatever else the terminal had to say.
        assert!(is_status_report(b"\x1b[?62;4;22c\x1b[0n"));
        assert!(!is_status_report(b""));
        assert!(!is_status_report(b"\x1b[0"));
        // The query we sent, echoed back by a terminal with echo still on, is
        // not an answer: treating it as one would let the query that hangs
        // run against a terminal that never replies.
        assert!(!is_status_report(b"\x1b[5n"));
    }

    #[test]
    fn a_digest_separates_payloads_and_survives_the_same_one_twice() {
        assert_eq!(digest("aaa"), digest("aaa"));
        assert_ne!(digest("aaa"), digest("aab"));
    }
}
