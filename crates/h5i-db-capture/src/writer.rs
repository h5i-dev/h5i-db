//! Hourly lz4 NDJSON files, rolled by arrival time.
//!
//! Layout, chosen so a partial capture is still navigable and an hour is a
//! unit you can copy, delete or re-ingest on its own:
//!
//! ```text
//! <root>/<venue>/<YYYY-MM-DD>/<HH>.ndjson.lz4
//! <root>/<venue>/<YYYY-MM-DD>/<HH>.001.ndjson.lz4   (second run in that hour)
//! ```
//!
//! # One lz4 frame per file, and why there is a part number
//!
//! `lz4_flex`'s decoder stops at the first frame's end mark: a file holding
//! two concatenated frames reads back as only its first frame, silently. So a
//! recorder restarted inside an hour it has already written cannot append,
//! and must not truncate either. It opens the next part instead, and a reader
//! globs `<HH>*.ndjson.lz4` to get the hour whole.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use lz4_flex::frame::FrameEncoder;

/// Writes archive lines to the file for the hour each line arrived in.
pub struct CaptureWriter {
    root: PathBuf,
    venue: String,
    flush_after: Duration,
    open: Option<OpenHour>,
    lines: u64,
}

struct OpenHour {
    hour: i64,
    path: PathBuf,
    /// `Option` only so `close` can move the encoder out to finish it.
    encoder: Option<FrameEncoder<BufWriter<File>>>,
    last_flush: Instant,
    unflushed: u64,
}

impl CaptureWriter {
    /// `flush_after` bounds how much an ungraceful death costs.
    ///
    /// Bytes sit in the lz4 block buffer and then in the `BufWriter` until
    /// something pushes them out, and `kill -9` gets no chance to. Flushing on
    /// an interval puts every completed block on disk, so a killed recorder
    /// leaves a frame that is missing only its end mark: a tolerant reader
    /// still recovers every complete block, which is nearly everything.
    pub fn new(root: impl Into<PathBuf>, venue: impl Into<String>, flush_after: Duration) -> Self {
        Self {
            root: root.into(),
            venue: venue.into(),
            flush_after,
            open: None,
            lines: 0,
        }
    }

    /// Lines written since the writer was created.
    pub fn lines(&self) -> u64 {
        self.lines
    }

    /// The file currently being written, if any.
    pub fn current_path(&self) -> Option<&Path> {
        self.open.as_ref().map(|open| open.path.as_path())
    }

    /// Write one line, rolling the file if `received_at` is in a new hour.
    ///
    /// Rolling on *arrival* time rather than venue time keeps the file name a
    /// true statement about the file: everything in `14.ndjson.lz4` was seen
    /// between 14:00 and 15:00 UTC, whatever the payloads claim.
    pub fn write_line(&mut self, received_at: u64, line: &str) -> io::Result<()> {
        let hour = (received_at / 1_000_000_000) as i64 / 3600;
        if self.open.as_ref().is_some_and(|open| open.hour != hour) {
            self.close()?;
        }
        if self.open.is_none() {
            self.open = Some(self.open_hour(hour)?);
        }
        let flush_after = self.flush_after;
        let open = self
            .open
            .as_mut()
            .expect("an hour was just opened if it was missing");
        let encoder = open
            .encoder
            .as_mut()
            .expect("the encoder is only absent inside close");
        encoder.write_all(line.as_bytes())?;
        encoder.write_all(b"\n")?;
        open.unflushed += 1;
        self.lines += 1;
        if open.last_flush.elapsed() >= flush_after {
            self.flush()?;
        }
        Ok(())
    }

    /// Push completed blocks down to the file.
    ///
    /// Called on the flush interval and from the shutdown path. A flush with
    /// nothing pending is skipped, so a quiet market costs no syscalls.
    pub fn flush(&mut self) -> io::Result<()> {
        let Some(open) = self.open.as_mut() else {
            return Ok(());
        };
        if open.unflushed == 0 {
            open.last_flush = Instant::now();
            return Ok(());
        }
        let encoder = open
            .encoder
            .as_mut()
            .expect("the encoder is only absent inside close");
        // Two flushes: the first ends the lz4 block, the second moves the
        // resulting bytes out of the BufWriter and into the file.
        encoder.flush()?;
        encoder.get_mut().flush()?;
        open.last_flush = Instant::now();
        open.unflushed = 0;
        Ok(())
    }

    /// Write the frame's end mark, then flush and fsync the file.
    ///
    /// The end mark is what makes the file decodable at all, and the fsync is
    /// why shutdown is worth handling rather than leaving to the OS: without
    /// it a machine that loses power just after SIGINT still loses the tail
    /// the operator watched being written.
    pub fn close(&mut self) -> io::Result<()> {
        let Some(mut open) = self.open.take() else {
            return Ok(());
        };
        let encoder = open
            .encoder
            .take()
            .expect("the encoder is only absent inside close");
        let mut sink = encoder.finish().map_err(io::Error::other)?;
        sink.flush()?;
        sink.into_inner()
            .map_err(|error| io::Error::other(error.to_string()))?
            .sync_all()
    }

    fn open_hour(&self, hour: i64) -> io::Result<OpenHour> {
        let stamp = chrono::DateTime::from_timestamp(hour * 3600, 0)
            .ok_or_else(|| io::Error::other(format!("hour {hour} is not a representable time")))?;
        let dir = self
            .root
            .join(&self.venue)
            .join(stamp.format("%Y-%m-%d").to_string());
        std::fs::create_dir_all(&dir)?;
        let hour_name = stamp.format("%H").to_string();
        // `create_new` rather than a stat-then-open: two recorders pointed at
        // one directory should collide loudly here rather than interleave two
        // frames into one unreadable file.
        for part in 0..=999u32 {
            let path = dir.join(match part {
                0 => format!("{hour_name}.ndjson.lz4"),
                // Zero padded so lexical order stays chronological order.
                _ => format!("{hour_name}.{part:03}.ndjson.lz4"),
            });
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(file) => {
                    return Ok(OpenHour {
                        hour,
                        path,
                        encoder: Some(FrameEncoder::new(BufWriter::new(file))),
                        last_flush: Instant::now(),
                        unflushed: 0,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::other(format!(
            "{} already has 1000 parts for hour {hour_name}",
            dir.display()
        )))
    }
}

impl Drop for CaptureWriter {
    fn drop(&mut self) {
        // A panic elsewhere should still leave a decodable file. Errors here
        // have nowhere to go, so the explicit `close` on the shutdown path is
        // what reports them; this is only the backstop.
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn decompress(path: &Path) -> String {
        let bytes = std::fs::read(path).unwrap();
        let mut out = String::new();
        lz4_flex::frame::FrameDecoder::new(&bytes[..])
            .read_to_string(&mut out)
            .unwrap();
        out
    }

    #[test]
    fn lines_land_in_the_hour_they_arrived_in() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = CaptureWriter::new(dir.path(), "kalshi", Duration::from_secs(3600));
        // 2025-01-01T00:00:02Z and 2025-01-01T01:00:02Z.
        writer.write_line(1_735_689_602_000_000_000, "a").unwrap();
        writer.write_line(1_735_693_202_000_000_000, "b").unwrap();
        writer.close().unwrap();

        let day = dir.path().join("kalshi").join("2025-01-01");
        assert_eq!(decompress(&day.join("00.ndjson.lz4")), "a\n");
        assert_eq!(decompress(&day.join("01.ndjson.lz4")), "b\n");
        assert_eq!(writer.lines(), 2);
    }

    #[test]
    fn flushing_mid_file_leaves_one_frame_that_still_reads_whole() {
        // A zero flush interval flushes after every line. If flushing ever
        // starts a second frame instead of a second block, the decoder stops
        // at the first line and this test says so.
        let dir = tempfile::tempdir().unwrap();
        let mut writer = CaptureWriter::new(dir.path(), "polymarket", Duration::from_secs(0));
        for i in 0..4 {
            writer
                .write_line(1_735_689_602_000_000_000, &format!("line{i}"))
                .unwrap();
        }
        writer.close().unwrap();
        let path = dir.path().join("polymarket/2025-01-01/00.ndjson.lz4");
        assert_eq!(decompress(&path), "line0\nline1\nline2\nline3\n");
    }

    #[test]
    fn a_restart_inside_the_same_hour_opens_the_next_part() {
        // Appending would produce a two-frame file whose second frame the
        // decoder skips without complaint, which is the worst kind of loss.
        let dir = tempfile::tempdir().unwrap();
        for line in ["first", "second"] {
            let mut writer = CaptureWriter::new(dir.path(), "kalshi", Duration::from_secs(3600));
            writer.write_line(1_735_689_602_000_000_000, line).unwrap();
            writer.close().unwrap();
        }
        let day = dir.path().join("kalshi/2025-01-01");
        assert_eq!(decompress(&day.join("00.ndjson.lz4")), "first\n");
        assert_eq!(decompress(&day.join("00.001.ndjson.lz4")), "second\n");
    }

    #[test]
    fn a_killed_capture_still_yields_its_flushed_blocks() {
        // Simulates kill -9: flush, then never close, so the frame has no end
        // mark. The claim being tested is that the loss is bounded by the
        // flush interval rather than being the whole file.
        let dir = tempfile::tempdir().unwrap();
        let path = {
            let mut writer = CaptureWriter::new(dir.path(), "kalshi", Duration::from_secs(0));
            writer
                .write_line(1_735_689_602_000_000_000, "kept")
                .unwrap();
            let path = writer.current_path().unwrap().to_path_buf();
            // Leak rather than drop: `Drop` would close the frame properly.
            std::mem::forget(writer);
            path
        };
        let bytes = std::fs::read(&path).unwrap();
        let mut out = Vec::new();
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&bytes[..]);
        // The read errors at the missing end mark, after handing back the
        // block that was flushed.
        let _ = decoder.read_to_end(&mut out);
        assert_eq!(String::from_utf8(out).unwrap(), "kept\n");
    }
}
