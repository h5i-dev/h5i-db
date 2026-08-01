"""Hourly lz4 NDJSON files, rolled by arrival time.

Layout, chosen so a partial capture is still navigable and an hour is a unit
you can copy, delete or re-ingest on its own:

```text
<root>/<venue>/<YYYY-MM-DD>/<HH>.ndjson.lz4
<root>/<venue>/<YYYY-MM-DD>/<HH>.001.ndjson.lz4   (second run in that hour)
```

# One lz4 frame per file, and why there is a part number

`lz4.frame.decompress` and `LZ4FrameDecompressor` both stop at the first
frame's end mark: a file holding two concatenated frames reads back as only its
first frame, with no error at all. So a recorder restarted inside an hour it
has already written cannot append, and must not truncate either. It opens the
next part instead, and a reader globs `<HH>*.ndjson.lz4` to get the hour whole.

This is also why the periodic flush uses `compress_flush(end_frame=False)` and
never `LZ4FrameCompressor.flush()`, which ends the frame. A flush that ended
the frame would make every flush interval the start of a new, invisible file.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, BinaryIO, Optional

from ._deps import require

__all__ = ["CaptureWriter", "read_capture", "read_capture_text", "read_hour"]

_NANOS_PER_SECOND = 1_000_000_000
_SECONDS_PER_HOUR = 3600
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

#: How many parts one hour may have before the recorder gives up. A restart
#: loop is the usual cause, and failing loudly beats filling a directory.
_MAX_PARTS = 1000


def _lz4() -> Any:
    return require("lz4.frame", why="write and read lz4-compressed captures")


@dataclass
class _OpenHour:
    """The file currently being written, plus the state to finish it."""

    hour: int
    path: Path
    handle: BinaryIO
    context: Any
    last_flush: float
    unflushed: int = 0


class CaptureWriter:
    """Writes archive lines to the file for the hour each line arrived in.

    `flush_after` bounds how much an ungraceful death costs. Bytes sit in the
    lz4 block buffer and then in the file object's buffer until something
    pushes them out, and `kill -9` gets no chance to. Flushing on an interval
    puts every completed block on disk, so a killed recorder leaves a frame
    that is missing only its end mark: :func:`read_capture` still recovers
    every complete block, which is nearly everything.
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        venue: str,
        flush_after: float = 5.0,
    ) -> None:
        self._root = Path(root)
        self._venue = venue
        self._flush_after = float(flush_after)
        self._open: Optional[_OpenHour] = None
        self._lines = 0

    @property
    def lines(self) -> int:
        """Lines written since the writer was created."""
        return self._lines

    @property
    def current_path(self) -> Optional[Path]:
        """The file currently being written, if any."""
        return self._open.path if self._open is not None else None

    def write_line(self, received_at: int, line: str) -> None:
        """Write one line, rolling the file if `received_at` is in a new hour.

        Rolling on *arrival* time rather than venue time keeps the file name a
        true statement about the file: everything in `14.ndjson.lz4` was seen
        between 14:00 and 15:00 UTC, whatever the payloads claim.
        """
        hour = int(received_at) // _NANOS_PER_SECOND // _SECONDS_PER_HOUR
        if self._open is not None and self._open.hour != hour:
            self.close()
        if self._open is None:
            self._open = self._open_hour(hour)
        open_hour = self._open
        lz4 = _lz4()
        payload = line.encode("utf-8") + b"\n"
        open_hour.handle.write(lz4.compress_chunk(open_hour.context, payload))
        open_hour.unflushed += 1
        self._lines += 1
        if time.monotonic() - open_hour.last_flush >= self._flush_after:
            self.flush()

    def flush(self) -> None:
        """Push completed blocks down to the file.

        Called on the flush interval and from the shutdown path. A flush with
        nothing pending is skipped, so a quiet market costs no syscalls.
        """
        open_hour = self._open
        if open_hour is None:
            return
        if open_hour.unflushed == 0:
            open_hour.last_flush = time.monotonic()
            return
        lz4 = _lz4()
        # `end_frame=False` is the whole point: it ends the current lz4 block
        # without writing the frame's end mark, so the file stays one frame.
        open_hour.handle.write(lz4.compress_flush(open_hour.context, end_frame=False))
        open_hour.handle.flush()
        open_hour.last_flush = time.monotonic()
        open_hour.unflushed = 0

    def close(self) -> None:
        """Write the frame's end mark, then flush and fsync the file.

        The end mark is what makes the file decodable by a strict reader at
        all, and the fsync is why shutdown is worth handling rather than
        leaving to the OS: without it a machine that loses power just after
        SIGINT still loses the tail the operator watched being written.

        Idempotent, so the shutdown path and the fallback in `__del__` can both
        call it.
        """
        open_hour = self._open
        if open_hour is None:
            return
        self._open = None
        lz4 = _lz4()
        try:
            open_hour.handle.write(lz4.compress_flush(open_hour.context, end_frame=True))
            open_hour.handle.flush()
            os.fsync(open_hour.handle.fileno())
        finally:
            open_hour.handle.close()

    def __enter__(self) -> "CaptureWriter":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        # A crash elsewhere should still leave a decodable file. Errors here
        # have nowhere to go, so the explicit `close` on the shutdown path is
        # what reports them; this is only the backstop.
        try:
            self.close()
        except Exception:  # pragma: no cover - interpreter teardown
            pass

    def _open_hour(self, hour: int) -> _OpenHour:
        stamp = _EPOCH + timedelta(seconds=hour * _SECONDS_PER_HOUR)
        directory = self._root / self._venue / stamp.strftime("%Y-%m-%d")
        directory.mkdir(parents=True, exist_ok=True)
        hour_name = stamp.strftime("%H")
        lz4 = _lz4()
        for part in range(_MAX_PARTS):
            name = (
                f"{hour_name}.ndjson.lz4"
                if part == 0
                # Zero padded so lexical order stays chronological order.
                else f"{hour_name}.{part:03d}.ndjson.lz4"
            )
            path = directory / name
            try:
                # O_EXCL rather than a stat-then-open: two recorders pointed at
                # one directory should collide loudly here rather than
                # interleave two frames into one silently truncated file.
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            except FileExistsError:
                continue
            handle = os.fdopen(descriptor, "wb")
            context = lz4.create_compression_context()
            # Independent blocks, no checksums: the same frame options the Rust
            # writer used, and independence means a block damaged by a partial
            # write does not make the blocks after it unreadable too.
            handle.write(
                lz4.compress_begin(
                    context,
                    block_linked=False,
                    block_checksum=False,
                    content_checksum=False,
                    auto_flush=0,
                )
            )
            return _OpenHour(
                hour=hour,
                path=path,
                handle=handle,
                context=context,
                last_flush=time.monotonic(),
            )
        raise OSError(
            f"{directory} already has {_MAX_PARTS} parts for hour {hour_name}"
        )


def read_capture_text(
    path: str | os.PathLike[str], *, tolerant: bool = True
) -> str:
    """Decompress one capture file to its NDJSON text.

    `tolerant` is on by default because the interesting file is the one a
    `kill -9` left behind: it has every flushed block and no end mark, and a
    strict decode throws away the blocks along with the complaint. A truncated
    trailing line is dropped rather than returned half-read, since a reader
    that sees it will only fail to parse it.
    """
    lz4 = _lz4()
    data = Path(path).read_bytes()
    decompressor = lz4.LZ4FrameDecompressor()
    out = bytearray()
    # Fed in chunks so a failure part way through keeps what came before it.
    for start in range(0, len(data), 1 << 16):
        try:
            out += decompressor.decompress(data[start : start + (1 << 16)])
        except RuntimeError:
            if not tolerant:
                raise
            break
    if not decompressor.eof and not tolerant:
        raise ValueError(
            f"{path} has no lz4 end mark: it was written by a recorder that "
            "was killed. Read it with tolerant=True to recover the flushed "
            "blocks."
        )
    return bytes(out).decode("utf-8", "ignore" if tolerant else "strict")


def read_capture(
    path: str | os.PathLike[str], *, tolerant: bool = True
) -> list[dict[str, Any]]:
    """One capture file as parsed envelopes, in the order they were written."""
    text = read_capture_text(path, tolerant=tolerant)
    lines = text.split("\n")
    if lines and lines[-1] != "":
        # No trailing newline means the last line is half a line.
        lines.pop()
    return [json.loads(line) for line in lines if line]


def _part_number(path: Path) -> int:
    """The part index of `HH.ndjson.lz4` (0) or `HH.NNN.ndjson.lz4` (NNN)."""
    pieces = path.name.split(".")
    if len(pieces) >= 4 and pieces[1].isdigit():
        return int(pieces[1])
    return 0


def read_hour(
    directory: str | os.PathLike[str], hour: str, *, tolerant: bool = True
) -> list[dict[str, Any]]:
    """Every part of one hour, concatenated in part order.

    The part files exist because a restart cannot append, so reading an hour
    means reading all of them; a caller who globs `<HH>.ndjson.lz4` alone
    silently drops everything the restarted recorder wrote.

    Sorted by part *number*, not by name. Part zero has no suffix at all, so
    `sorted()` puts `00.001.ndjson.lz4` before `00.ndjson.lz4` and hands back an
    hour whose first file is its second run.
    """
    parts = sorted(Path(directory).glob(f"{hour}*.ndjson.lz4"), key=_part_number)
    out: list[dict[str, Any]] = []
    for part in parts:
        out.extend(read_capture(part, tolerant=tolerant))
    return out
